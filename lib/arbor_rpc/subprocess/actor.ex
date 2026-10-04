defmodule Arbor.RPC.Subprocess.Actor do
  @moduledoc false
  use GenServer
  alias Arbor.RPC.{Framing, PortEnvironment}
  alias Arbor.RPC.Subprocess.{Cleanup, Command}

  @defaults [
    max_frame_bytes: 1_048_576,
    max_queue_bytes: 4_194_304,
    max_queue_frames: 1024,
    max_waiters: 128,
    max_input_chunk_bytes: 4_194_304,
    max_mailbox_messages: 1024,
    max_write_bytes: 1_048_576,
    cleanup_timeout: 500,
    term_grace: 100,
    closed_retention: 5000
  ]

  @impl true
  def init({owner, generation, command, opts}) do
    Process.flag(:trap_exit, true)
    owner_monitor = Process.monitor(owner)

    with {:ok, limits} <- limits(opts),
         :ok <- boolean_options(opts),
         {:ok, command} <- Command.resolve(command, opts),
         {:ok, port} <- open_port(command, opts),
         {:ok, proof, guardian} <- establish_cleanup(port, opts, limits) do
      {:ok,
       %{
         owner: owner,
         owner_monitor: owner_monitor,
         generation: generation,
         port: port,
         proof: proof,
         guardian: guardian,
         limits: limits,
         decoder: Framing.new(max_frame_bytes: limits.max_frame_bytes),
         queue: :queue.new(),
         count: 0,
         bytes: 0,
         inflight: %{},
         waiters: :queue.new(),
         waiter_count: 0,
         consumer: nil,
         consumer_monitor: nil,
         window: 0,
         closed: nil,
         terminal_sent: false,
         cleanup_result: :ok
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  defp limits(opts) do
    invalid =
      Enum.find(@defaults, fn {key, default} ->
        value = Keyword.get(opts, key, default)
        not (is_integer(value) and value > 0)
      end)

    if invalid do
      {:error, {:invalid_limit, elem(invalid, 0)}}
    else
      limits = Map.new(@defaults, fn {key, default} -> {key, Keyword.get(opts, key, default)} end)

      if limits.cleanup_timeout >= limits.term_grace + 50,
        do: {:ok, limits},
        else: {:error, :invalid_cleanup_budget}
    end
  end

  defp boolean_options(opts) do
    case Enum.find(
           [:process_group, :stderr_to_stdout],
           &(not is_boolean(Keyword.get(opts, &1, false)))
         ) do
      nil -> :ok
      key -> {:error, {:invalid_option, key}}
    end
  end

  defp open_port(command, opts) do
    options = [
      :binary,
      :exit_status,
      :eof,
      :use_stdio,
      :hide,
      args: command.args,
      cd: String.to_charlist(command.cd),
      env: PortEnvironment.to_port(command.env),
      busy_limits_port: {4096, 8192},
      busy_limits_msgq: {4096, 8192}
    ]

    options =
      if Keyword.get(opts, :stderr_to_stdout, false),
        do: [:stderr_to_stdout | options],
        else: options

    # Retain the Port and its owned PID metadata after EOF, even if the child
    # exits before init establishes cleanup proof. EOF itself is not proof of
    # child death; exit_status drives natural-exit cleanup and terminal delivery.
    {:ok, Port.open({:spawn_executable, String.to_charlist(command.executable)}, options)}
  catch
    :error, reason -> {:error, {:port_open_failed, reason}}
  end

  defp establish_cleanup(port, opts, limits) do
    budget = Map.to_list(limits)

    case Cleanup.proof(port, Keyword.get(opts, :process_group, false), limits.cleanup_timeout) do
      {:ok, proof} ->
        actor = self()
        guardian = spawn(fn -> guardian(actor, proof, budget) end)
        {:ok, proof, guardian}

      {:error, reason, proof} ->
        case Cleanup.run(proof, port, budget) do
          :ok -> {:error, reason}
          {:error, _cleanup_reason} = error -> {:error, {:cleanup_failed, reason, error}}
        end
    end
  end

  # The guardian is not linked to the actor or opening owner. If either kills
  # the actor while it owns the Port, the OS child still receives cleanup.
  defp guardian(actor, proof, budget) do
    monitor = Process.monitor(actor)

    receive do
      {:cleaned, ^actor} -> Process.demonitor(monitor, [:flush])
      {:DOWN, ^monitor, :process, ^actor, _reason} -> Cleanup.run(proof, nil, budget)
    end
  end

  @impl true
  def handle_call(_request, {caller, _tag}, state) when node(caller) != node(),
    do: {:reply, {:error, :remote_caller_not_supported}, state}

  def handle_call({generation, _request}, _from, %{generation: current} = state)
      when generation != current do
    {:reply, {:error, :stale_generation}, state}
  end

  def handle_call({_generation, :stats}, _from, state) do
    {:reply,
     %{
       frames: state.count,
       bytes: state.bytes,
       queued: :queue.len(state.queue),
       inflight: map_size(state.inflight),
       waiters: state.waiter_count,
       closed: state.closed != nil
     }, state}
  end

  def handle_call({_generation, :connected?}, _from, state),
    do: {:reply, state.closed == nil, state}

  def handle_call({_generation, :os_pid}, _from, state),
    do: {:reply, state.proof.pid, state}

  def handle_call({_generation, :close}, _from, state) do
    state = state |> close(:closed) |> terminal_now()
    state = reply_all_waiters(state, {:closed, state.closed, Framing.remainder(state.decoder)})
    {:stop, :normal, state.cleanup_result, state}
  end

  def handle_call({_generation, {:write, _data}}, _from, %{closed: closed} = state)
      when closed != nil do
    {:reply, {:error, :closed}, state}
  end

  def handle_call({_generation, {:write, data}}, _from, state) do
    result = write(state.port, data, state.limits.max_write_bytes)
    state = if result == {:error, :port_closed}, do: close(state, :port_closed), else: state
    reply(result, state)
  end

  def handle_call(
        {_generation, {:next, _deadline, _acceptance_deadline, _immediate}},
        _from,
        %{consumer: consumer} = state
      )
      when consumer != nil do
    {:reply, {:error, :subscribed}, state}
  end

  def handle_call({_generation, {:next, deadline, acceptance_deadline, immediate}}, from, state) do
    cond do
      acceptance_deadline != :infinity and now() >= acceptance_deadline ->
        {:reply, {:error, :timeout}, state}

      deadline != :infinity and not immediate and now() >= deadline ->
        {:reply, {:error, :timeout}, state}

      state.closed != nil and :queue.is_empty(state.queue) ->
        result = {:closed, state.closed, Framing.remainder(state.decoder)}
        {:stop, :normal, result, reply_all_waiters(%{state | terminal_sent: true}, result)}

      immediate and not buffered_before?(state.queue, deadline) ->
        {:reply, {:error, :timeout}, state}

      not :queue.is_empty(state.queue) ->
        {{:value, {frame, _received_at}}, queue} = :queue.out(state.queue)

        {:reply, {:ok, frame},
         %{state | queue: queue, count: state.count - 1, bytes: state.bytes - byte_size(frame)}}

      state.waiter_count >= state.limits.max_waiters ->
        {:reply, {:error, :waiter_limit}, state}

      true ->
        token = make_ref()
        monitor = Process.monitor(elem(from, 0))

        timer =
          if deadline == :infinity,
            do: nil,
            else: Process.send_after(self(), {:waiter_timeout, token}, max(0, deadline - now()))

        waiter = %{from: from, token: token, monitor: monitor, deadline: deadline, timer: timer}

        state = %{
          state
          | waiters: :queue.in(waiter, state.waiters),
            waiter_count: state.waiter_count + 1
        }

        noreply(state)
    end
  end

  def handle_call({_generation, {:subscribe, consumer, window}}, _from, state) do
    cond do
      not is_pid(consumer) or node(consumer) != node() or not Process.alive?(consumer) ->
        {:reply, {:error, :invalid_consumer}, state}

      not is_integer(window) or window <= 0 or window > state.limits.max_queue_frames ->
        {:reply, {:error, :invalid_window}, state}

      state.consumer != nil ->
        {:reply, {:error, :already_subscribed}, state}

      state.waiter_count > 0 ->
        {:reply, {:error, :readers_waiting}, state}

      true ->
        state = %{
          state
          | consumer: consumer,
            consumer_monitor: Process.monitor(consumer),
            window: window
        }

        reply(:ok, state)
    end
  end

  def handle_call({_generation, {:ack, token}}, {caller, _tag}, state) do
    cond do
      caller != state.consumer ->
        {:reply, {:error, :not_consumer}, state}

      not Map.has_key?(state.inflight, token) ->
        {:reply, {:error, :invalid_ack}, state}

      true ->
        {bytes, inflight} = Map.pop(state.inflight, token)
        state = %{state | inflight: inflight, count: state.count - 1, bytes: state.bytes - bytes}
        reply(:ok, state)
    end
  end

  defp write(port, data, limit) do
    case data_size(data) do
      {:ok, bytes} when bytes > limit ->
        {:error, :write_too_large}

      {:ok, _bytes} ->
        if Port.command(port, data, [:nosuspend]), do: :ok, else: {:error, :backpressure}

      {:error, _reason} = error ->
        error
    end
  catch
    :error, :badarg -> {:error, :port_closed}
  end

  defp data_size(data) do
    {:ok, :erlang.iolist_size(data)}
  catch
    :error, :badarg -> {:error, :invalid_iodata}
  end

  @impl true
  def handle_info({port, {:data, data}}, %{port: port, closed: nil} = state) do
    {:message_queue_len, mailbox} = Process.info(self(), :message_queue_len)

    result =
      cond do
        mailbox > state.limits.max_mailbox_messages -> {:error, :mailbox_pressure}
        byte_size(data) > state.limits.max_input_chunk_bytes -> {:error, :input_chunk_too_large}
        true -> Framing.reduce(state.decoder, data, state, &enqueue/2)
      end

    case result do
      {:ok, state, decoder} ->
        noreply(%{state | decoder: decoder})

      {:error, reason, state, decoder} ->
        noreply(%{state | decoder: decoder} |> close(reason))

      {:error, reason} ->
        noreply(close(state, reason))
    end
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state),
    do: noreply(close(state, {:exit_status, status}, :natural))

  def handle_info({:EXIT, port, reason}, %{port: port} = state),
    do: noreply(close(state, {:port_exit, reason}, :natural))

  def handle_info({:waiter_timeout, token}, state) do
    noreply(remove_waiters(state, &(&1.token == token), {:error, :timeout}))
  end

  def handle_info(:closed_timeout, state) do
    reason = if state.count > 0, do: {:drain_timeout, state.closed}, else: state.closed
    state = %{state | closed: reason} |> terminal_now()
    result = {:closed, reason, Framing.remainder(state.decoder)}
    {:stop, :normal, reply_all_waiters(state, result)}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{owner_monitor: monitor} = state) do
    state = state |> close(:owner_down) |> terminal_now()

    {:stop, :normal,
     reply_all_waiters(state, {:closed, state.closed, Framing.remainder(state.decoder)})}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{consumer_monitor: monitor} = state) do
    state = close(state, :consumer_down)
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state),
    do: noreply(remove_waiters(state, &(&1.monitor == monitor), nil))

  def handle_info(_message, state), do: {:noreply, state}

  defp enqueue(frame, state) do
    bytes = byte_size(frame)

    cond do
      state.count + 1 > state.limits.max_queue_frames ->
        {:error, :queue_frame_limit}

      state.bytes + bytes > state.limits.max_queue_bytes ->
        {:error, :queue_byte_limit}

      true ->
        {:ok,
         %{
           state
           | queue: :queue.in({frame, now()}, state.queue),
             count: state.count + 1,
             bytes: state.bytes + bytes
         }}
    end
  end

  defp deliver(%{terminal_sent: true} = state), do: state

  defp deliver(%{consumer: consumer} = state) when consumer != nil do
    if map_size(state.inflight) < state.window and not :queue.is_empty(state.queue) do
      {{:value, {frame, _received_at}}, queue} = :queue.out(state.queue)
      token = make_ref()
      send(consumer, {:arbor_rpc, state.generation, {:frame, token, frame}})
      deliver(%{state | queue: queue, inflight: Map.put(state.inflight, token, byte_size(frame))})
    else
      if state.closed != nil and state.count == 0 and not state.terminal_sent do
        send(
          consumer,
          {:arbor_rpc, state.generation,
           {:closed, state.closed, Framing.remainder(state.decoder)}}
        )

        %{state | terminal_sent: true}
      else
        state
      end
    end
  end

  defp deliver(state) do
    case :queue.out(state.waiters) do
      {:empty, _queue} ->
        state

      {{:value, waiter}, waiters} ->
        if expired?(waiter) or not Process.alive?(elem(waiter.from, 0)) do
          release(waiter, {:error, :timeout})
          deliver(%{state | waiters: waiters, waiter_count: state.waiter_count - 1})
        else
          deliver_waiter(state, waiter, waiters)
        end
    end
  end

  defp deliver_waiter(state, waiter, waiters) do
    case :queue.out(state.queue) do
      {{:value, {frame, _received_at}}, queue} ->
        release(waiter, {:ok, frame})

        deliver(%{
          state
          | queue: queue,
            count: state.count - 1,
            bytes: state.bytes - byte_size(frame),
            waiters: waiters,
            waiter_count: state.waiter_count - 1
        })

      {:empty, _queue} when state.closed != nil ->
        result = {:closed, state.closed, Framing.remainder(state.decoder)}
        release(waiter, result)

        reply_all_waiters(
          %{state | waiters: waiters, waiter_count: state.waiter_count - 1, terminal_sent: true},
          result
        )

      {:empty, _queue} ->
        state
    end
  end

  # Timeout zero still allows an already-buffered frame to be drained. Other
  # finite waiters expire before they can consume frames arriving past deadline.
  defp expired?(%{deadline: :infinity}), do: false
  defp expired?(%{deadline: deadline}), do: now() >= deadline

  defp buffered_before?(queue, deadline) do
    case :queue.peek(queue) do
      {:value, {_frame, received_at}} -> received_at <= deadline
      :empty -> false
    end
  end

  defp reply(result, state) do
    state = deliver(state)
    if state.terminal_sent, do: {:stop, :normal, result, state}, else: {:reply, result, state}
  end

  defp noreply(state) do
    state = deliver(state)
    if state.terminal_sent, do: {:stop, :normal, state}, else: {:noreply, state}
  end

  defp remove_waiters(state, predicate, reply) do
    {remove, retain} = state.waiters |> :queue.to_list() |> Enum.split_with(predicate)
    Enum.each(remove, &release(&1, reply))
    %{state | waiters: :queue.from_list(retain), waiter_count: length(retain)}
  end

  defp reply_all_waiters(state, reply), do: remove_waiters(state, fn _ -> true end, reply)

  defp release(waiter, reply) do
    if waiter.timer, do: Process.cancel_timer(waiter.timer)
    Process.demonitor(waiter.monitor, [:flush])
    if reply, do: GenServer.reply(waiter.from, reply)
  end

  defp close(state, reason, kind \\ :requested)
  defp close(%{closed: closed} = state, _reason, _kind) when closed != nil, do: state

  defp close(state, reason, kind) do
    result =
      if kind == :natural,
        do: Cleanup.after_exit(state.proof, state.port, Map.to_list(state.limits)),
        else: Cleanup.run(state.proof, state.port, Map.to_list(state.limits))

    if result == :ok, do: send(state.guardian, {:cleaned, self()})
    reason = if result == :ok, do: reason, else: {:cleanup_failed, reason, result}
    Process.send_after(self(), :closed_timeout, state.limits.closed_retention)
    %{state | closed: reason, cleanup_result: result}
  end

  defp terminal_now(%{consumer: consumer, terminal_sent: false} = state)
       when consumer != nil do
    send(
      consumer,
      {:arbor_rpc, state.generation, {:closed, state.closed, Framing.remainder(state.decoder)}}
    )

    %{state | terminal_sent: true}
  end

  defp terminal_now(state), do: state

  @impl true
  def terminate(_reason, state) do
    close(state, :actor_stopped)
    :ok
  end

  defp now, do: System.monotonic_time(:millisecond)
end
