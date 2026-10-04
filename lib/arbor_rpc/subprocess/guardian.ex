defmodule Arbor.RPC.Subprocess.Guardian do
  @moduledoc false
  use GenServer

  alias Arbor.RPC.Subprocess.{NativeBackend, NativeProtocol, Receipt, WriteAdmission}

  def start(owner, actor, generation, command, opts, limits) do
    GenServer.start(__MODULE__, {owner, actor, generation, command, opts, limits},
      timeout: limits.startup_timeout + limits.cleanup_timeout + 250
    )
  end

  def close(pid, generation, timeout), do: call(pid, generation, :close, timeout)
  def proof(pid, generation), do: call(pid, generation, :proof, 500)
  def receipt(pid, generation, timeout), do: call(pid, generation, :receipt, timeout)

  def write(pid, generation, data, timeout) do
    case call(pid, generation, {:write, data, now() + timeout, nil}, timeout) do
      {:uncertain, result} -> result
      result -> result
    end
  end

  def write_admitted(pid, generation, data, deadline, context) do
    timeout = if deadline == :infinity, do: 1100, else: min(1100, max(0, deadline - now()))
    call(pid, generation, {:write, data, deadline, context}, timeout)
  end

  def ack(pid, generation, sequence),
    do: GenServer.cast(pid, {generation, self(), {:ack, sequence}})

  defp call(pid, generation, request, timeout) do
    GenServer.call(pid, {generation, request}, timeout)
  catch
    :exit, {:timeout, _} -> {:error, :guardian_timeout}
    :exit, _ -> {:error, :cleanup_status_unavailable}
  end

  @impl true
  def init({owner, actor, generation, command, opts, limits}) do
    Process.flag(:trap_exit, true)
    Process.put({__MODULE__, :generation}, generation)
    Process.put({__MODULE__, :owner}, owner)
    owner_monitor = Process.monitor(owner)
    actor_monitor = Process.monitor(actor)

    with {:ok, port} <- NativeBackend.open(command, opts, limits),
         {:ok, token, proof} <-
           startup(port, limits.startup_timeout, opts, owner_monitor, actor_monitor) do
      Process.put({__MODULE__, :actor}, actor)
      Process.send_after(self(), :heartbeat, 1000)

      {:ok,
       %{
         owner: owner,
         actor: actor,
         generation: generation,
         owner_monitor: owner_monitor,
         actor_monitor: actor_monitor,
         port: port,
         token: token,
         proof: proof,
         limits: limits,
         chunk: nil,
         sequence: 0,
         write_sequence: 0,
         write: nil,
         receipt: nil,
         close_waiters: [],
         closing: false,
         explicit: false,
         vendor_status: nil,
         stdout_eof: false,
         helper_eof: false,
         helper_status: nil,
         terminal_sent: false,
         completion_timer: nil
       }, {:continue, :credit}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  defp startup(port, timeout, opts, owner_monitor, actor_monitor) do
    receive do
      {^port, {:data, packet}} ->
        case NativeProtocol.decode(packet, nil) do
          {:ok, {:started, token, child, pgid, group?, 0}} ->
            if group? == Keyword.get(opts, :process_group, false) and
                 (not group? or pgid == child),
               do: {:ok, token, %{pid: child, group: group?}},
               else: startup_error(port, :invalid_native_group_proof)

          {:ok, {:started, _token, _child, _pgid, _group, error}} ->
            startup_error(port, {:native_exec_failed, error})

          {:error, reason} ->
            startup_error(port, reason)
        end

      {^port, {:exit_status, status}} ->
        startup_error(port, {:native_helper_startup_exit, status})

      {:DOWN, monitor, :process, _pid, _reason}
      when monitor == owner_monitor or monitor == actor_monitor ->
        startup_error(port, :owner_or_actor_down_during_startup)
    after
      timeout + 100 -> startup_error(port, :native_helper_startup_timeout)
    end
  end

  defp startup_error(port, reason) do
    NativeBackend.close(port)
    {:error, reason}
  end

  @impl true
  def handle_continue(:credit, state) do
    case command(state, :credit) do
      :ok -> {:noreply, state}
      {:error, reason} -> {:noreply, fail(state, reason)}
    end
  end

  @impl true
  def handle_call(_request, {caller, _tag}, state) when node(caller) != node(),
    do: {:reply, {:error, :remote_caller_not_supported}, state}

  def handle_call({generation, _request}, _from, %{generation: current} = state)
      when generation != current,
      do: {:reply, {:error, :stale_generation}, state}

  def handle_call({_generation, :receipt}, _from, state) do
    result = if state.receipt, do: {:ok, state.receipt}, else: {:error, :cleanup_pending}
    {:reply, result, state}
  end

  def handle_call({_generation, :proof}, _from, state), do: {:reply, {:ok, state.proof}, state}

  def handle_call({_generation, :close}, from, state) do
    state = %{state | explicit: true}

    if state.receipt do
      command(state, :quit)
      {:reply, Receipt.result(state.receipt), state}
    else
      if length(state.close_waiters) >= state.limits.max_waiters do
        {:reply, {:error, :waiter_limit}, state}
      else
        {:noreply, begin_cleanup(%{state | close_waiters: [from | state.close_waiters]})}
      end
    end
  end

  def handle_call({_generation, {:write, data, deadline, context}}, {caller, _tag} = from, state) do
    cond do
      caller != state.actor ->
        {:reply, {:error, :invalid_write_owner}, state}

      state.closing or state.write != nil or state.vendor_status != nil ->
        WriteAdmission.complete(context, {:error, :closed})
        {:reply, {:error, :closed}, state}

      true ->
        sequence = state.write_sequence + 1
        packet = NativeProtocol.command(:write, state.token, sequence, IO.iodata_to_binary(data))

        # Materialization and scheduling both spend the original caller budget.
        # A queued Guardian request may never initiate a late physical write.
        if not WriteAdmission.current?(context, deadline) do
          WriteAdmission.complete(context, {:error, :timeout})
          {:reply, {:error, :timeout}, state}
        else
          case NativeBackend.command(state.port, packet) do
            :ok ->
              wait = if deadline == :infinity, do: 1000, else: min(1000, max(0, deadline - now()))
              timer = Process.send_after(self(), {:write_timeout, sequence}, wait)

              {:noreply,
               %{state | write_sequence: sequence, write: {sequence, from, timer, context}}}

            error ->
              WriteAdmission.complete(context, error)
              {:reply, error, state}
          end
        end
    end
  end

  @impl true
  def handle_cast({generation, actor, {:ack, sequence}}, state)
      when generation == state.generation and actor == state.actor and sequence == state.chunk and
             is_integer(sequence) and sequence > 0 do
    state = %{state | chunk: nil}

    state =
      case command(state, :credit, sequence) do
        :ok -> state
        {:error, reason} -> fail(state, reason)
      end

    {:noreply, terminal(state)}
  end

  def handle_cast(_message, state), do: {:noreply, state}

  @impl true
  def handle_info({port, {:data, packet}}, %{port: port} = state) do
    state =
      case NativeProtocol.decode(packet, state.token) do
        {:ok, event} -> event(event, state)
        {:error, reason} -> fail(state, reason)
      end

    {:noreply, terminal(state)}
  end

  def handle_info({port, :eof}, %{port: port} = state) do
    state = %{state | helper_eof: true}
    {:noreply, terminal(if(state.receipt, do: state, else: fail(state, :helper_receipt_lost)))}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    receipt = if state.receipt, do: %{state.receipt | helper_status: status}, else: nil
    {:noreply, %{state | helper_status: status, receipt: receipt}}
  end

  def handle_info({:EXIT, port, reason}, %{port: port} = state) do
    state = if state.receipt, do: state, else: fail(state, {:helper_port_exit, reason})
    {:noreply, terminal(state)}
  end

  def handle_info(:heartbeat, state) do
    state =
      if state.receipt == nil and not state.closing do
        case command(state, :heartbeat) do
          :ok -> state
          {:error, reason} -> fail(state, reason)
        end
      else
        state
      end

    Process.send_after(self(), :heartbeat, 1000)
    {:noreply, terminal(state)}
  end

  def handle_info({:write_timeout, sequence}, %{write: {sequence, from, _timer, context}} = state) do
    WriteAdmission.uncertain(context, {:error, :write_admission_timeout})
    GenServer.reply(from, {:uncertain, {:error, :write_admission_timeout}})
    {:noreply, begin_cleanup(%{state | write: nil})}
  end

  def handle_info(:cleanup_timeout, %{receipt: nil} = state),
    do: {:noreply, terminal(fail(state, :cleanup_timeout))}

  def handle_info(:receipt_expired, state), do: {:stop, :normal, state}

  def handle_info(:final_output_timeout, %{terminal_sent: true} = state), do: {:noreply, state}

  def handle_info(:final_output_timeout, state) do
    send_event(state, {:closed, {:final_output_unconfirmed, state.vendor_status}})
    command(state, :quit)
    {:noreply, %{state | terminal_sent: true}}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state)
      when monitor == state.actor_monitor or monitor == state.owner_monitor do
    {:noreply, begin_cleanup(%{state | explicit: true})}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp event({:data, sequence, bytes}, state)
       when state.chunk == nil and sequence == state.sequence + 1 do
    send_event(state, {:chunk, sequence, bytes})
    %{state | chunk: sequence, sequence: sequence}
  end

  defp event({:vendor_exit, status}, %{vendor_status: nil} = state) do
    timer = Process.send_after(self(), :final_output_timeout, state.limits.closed_retention)
    begin_cleanup(%{state | vendor_status: status, completion_timer: timer})
  end

  defp event(:stdout_eof, state), do: %{state | stdout_eof: true}

  defp event(
         {:write_result, sequence, result},
         %{write: {sequence, from, timer, context}} = state
       ) do
    Process.cancel_timer(timer)
    WriteAdmission.complete(context, result)
    GenServer.reply(from, result)
    %{state | write: nil}
  end

  defp event({:cleanup, direct?, scope?, status, _signals, child}, state)
       when child == state.proof.pid and state.receipt == nil do
    receipt = %Receipt{
      generation: state.generation,
      direct_child: if(direct?, do: :reaped, else: :unconfirmed),
      targeted_group:
        if(state.proof.group,
          do: if(scope?, do: :absent, else: :unconfirmed),
          else: :not_requested
        ),
      vendor_status: if(status >= 0, do: status),
      helper_status: state.helper_status,
      completed_at: System.monotonic_time(:millisecond)
    }

    retain_receipt(state, receipt)
  end

  defp event({:cleanup, _direct, _scope, _status, _signals, _child}, %{receipt: receipt} = state)
       when not is_nil(receipt),
       do: state

  defp event(_invalid_or_duplicate, state), do: fail(state, :invalid_helper_event)

  defp begin_cleanup(%{closing: true} = state), do: state

  defp begin_cleanup(state) do
    Process.send_after(self(), :cleanup_timeout, state.limits.cleanup_timeout + 100)

    case command(state, :close) do
      :ok -> %{state | closing: true}
      {:error, reason} -> fail(%{state | closing: true}, reason)
    end
  end

  defp fail(%{receipt: receipt} = state, _reason) when not is_nil(receipt), do: state

  defp fail(state, reason) do
    NativeBackend.close(state.port)

    receipt =
      Receipt.unconfirmed(state.generation, state.proof.group, reason, state.helper_status)

    retain_receipt(state, %{receipt | vendor_status: state.vendor_status})
  end

  defp retain_receipt(state, receipt) do
    state = finish_write(state, {:error, :closed})
    Enum.each(state.close_waiters, &GenServer.reply(&1, Receipt.result(receipt)))
    Process.send_after(self(), :receipt_expired, state.limits.receipt_retention)
    if state.explicit, do: command(state, :quit)
    %{state | receipt: receipt, close_waiters: [], closing: true}
  end

  defp finish_write(%{write: nil} = state, _result), do: state

  defp finish_write(%{write: {_sequence, from, timer, context}} = state, result) do
    Process.cancel_timer(timer)
    WriteAdmission.uncertain(context, result)
    GenServer.reply(from, {:uncertain, result})
    %{state | write: nil}
  end

  defp terminal(%{terminal_sent: true} = state), do: state

  defp terminal(state) do
    cond do
      state.receipt != nil and Receipt.result(state.receipt) != :ok ->
        reason =
          if state.vendor_status, do: {:exit_status, state.vendor_status}, else: :helper_failure

        send_event(state, {:closed, reason})
        %{state | terminal_sent: true}

      state.vendor_status != nil and state.stdout_eof and state.chunk == nil and
          state.receipt != nil ->
        if state.completion_timer, do: Process.cancel_timer(state.completion_timer)
        send_event(state, {:closed, {:exit_status, state.vendor_status}})
        command(state, :quit)
        %{state | terminal_sent: true}

      true ->
        state
    end
  end

  defp command(state, type, sequence \\ 0),
    do: NativeBackend.command(state.port, NativeProtocol.command(type, state.token, sequence))

  defp send_event(state, event),
    do: send(state.actor, {:arbor_rpc_native, self(), state.generation, event})

  @impl true
  def terminate(_reason, state) do
    NativeBackend.close(state.port)
    :ok
  end

  defp now, do: System.monotonic_time(:millisecond)
end
