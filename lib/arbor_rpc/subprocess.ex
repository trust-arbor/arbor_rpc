defmodule Arbor.RPC.Subprocess do
  @moduledoc """
  An owned child process with bounded newline delivery and finite cleanup.

  A stable actor frames output; a guardian owns the native helper Port and monitors
  the lifetime owner, which defaults
  to the process that called `open/2`. An explicit live local `:owner` PID lets
  a temporary connection task open the child on behalf of a long-lived client.
  Passing the opaque handle to a handshake task does not transfer ownership of
  the child. `identity/1` is a fresh generation reference, suitable for ignoring
  events from a previous connection. The actor has no protocol or logging policy.

  `Arbor.RPC.FramedStream` provides pull delivery or acknowledged push delivery.
  The configured frame count and bytes include queued and unacknowledged frames.
  An oversized frame, input chunk or queue closes the child with an explicit
  error. One native raw-data credit bounds data delivered to the actor before
  framed admission; terminal/control packets have separately reserved capacity.
  Supported writes additionally reserve count and byte credit before retaining
  detached payloads in the Actor-owned ledger. A coalesced wake carries no
  payload to the Actor mailbox. These managed bounds do not include kernel
  buffers, Port-driver allocation, caller-owned inputs or bytes an application
  retains after acknowledging. One bounded native command packet and the
  helper's bounded stdin queue are separate from pending ledger credit.
  A consumer must acknowledge after processing rather than forwarding to an
  unbounded mailbox.

  Options include `:owner`, `:cd`, `:env`, `:environment_policy` (default `:isolated`),
  `:process_group` (default `false`), `:stderr_to_stdout` (default `false`),
  `:max_frame_bytes` (1 MiB), `:max_queue_bytes` (4 MiB), `:max_queue_frames`
  (1024), `:max_waiters` (128), `:max_input_chunk_bytes` (4 MiB),
  `:max_mailbox_messages` (1024), `:max_write_bytes` (1 MiB),
  `:max_pending_writes` (64), `:max_pending_write_bytes` (4 MiB, including
  reservation metadata),
  `:cleanup_timeout` (500 ms), `:term_grace` (100 ms) and `:closed_retention`
  (5 seconds). Cleanup requires at least 50 ms beyond TERM grace for KILL.
  Explicit close stops the actor. Natural exit or pressure failure drains
  admitted frames until terminal delivery or `:closed_retention`. The guardian
  retains a typed cleanup receipt for `:receipt_retention` (5 seconds), then
  stops; receipt loss or expiry returns an explicit unavailable error.

  The source-built native parent retains its actual child unreaped until all
  future signalling is disabled. It signals only that owned child or its
  verified private group, then reaps and polls group existence without signals.
  Group absence is an observation of the targeted group, not containment of an
  arbitrary descendant tree. Escaped descendants are outside this contract.
  Actual vendor status, final output/EOF and helper status remain distinct.
  Helper/guardian loss cannot confirm cleanup. Numeric PID diagnostics are not
  signalling authority; retired unmanaged cleanup is unsupported.

  The draft requires a build-time C compiler on supported Unix platforms, no
  runtime compiler or NIF. Missing helper and unsupported platform errors have
  no numeric signalling fallback. Installed/release lookup uses `:code.priv_dir`.
  Linux/macOS lifecycle and pressure qualification, Windows handle/Job support,
  helper hard death and uninterruptible child exit remain release gates.
  """

  alias Arbor.RPC.Subprocess.{Actor, Guardian, Receipt, WriteAdmission}

  @enforce_keys [:pid, :guardian, :generation, :cleanup_timeout, :max_write_bytes]
  defstruct [:pid, :guardian, :generation, :cleanup_timeout, :max_write_bytes]

  @opaque t :: %__MODULE__{
            pid: pid(),
            guardian: pid(),
            generation: reference(),
            cleanup_timeout: pos_integer(),
            max_write_bytes: pos_integer()
          }

  @spec open([binary()], keyword()) :: {:ok, t()} | {:error, term()}
  def open(command, opts \\ []) do
    owner = Keyword.get(opts, :owner, self())
    generation = make_ref()

    if is_pid(owner) and node(owner) == node() and Process.alive?(owner) do
      start(owner, generation, command, opts)
    else
      {:error, :invalid_owner}
    end
  end

  defp start(owner, generation, command, opts) do
    case GenServer.start(Actor, {owner, generation, command, opts}, timeout: startup_budget(opts)) do
      {:ok, pid} ->
        case Process.info(pid, :dictionary) do
          {:dictionary, dictionary} ->
            case List.keyfind(dictionary, {Actor, :guardian}, 0) do
              {{Actor, :guardian}, guardian} when is_pid(guardian) ->
                {:ok,
                 %__MODULE__{
                   pid: pid,
                   guardian: guardian,
                   generation: generation,
                   cleanup_timeout: Keyword.get(opts, :cleanup_timeout, 500),
                   max_write_bytes: Keyword.get(opts, :max_write_bytes, 1_048_576)
                 }}

              _ ->
                {:error, :closed_during_startup}
            end

          nil ->
            {:error, :closed_during_startup}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp startup_budget(opts) do
    Enum.reduce([{:startup_timeout, 1000}, {:cleanup_timeout, 500}], 1000, fn {key, default},
                                                                              total ->
      value = Keyword.get(opts, key, default)
      total + if(is_integer(value) and value in 1..60_000, do: value, else: default)
    end)
  end

  @doc """
  Captures a finite command's output as original bytes and its exit status.

  The capturing process owns the child, including when it dies before the
  command finishes. `:timeout` is one monotonic read deadline (5,000 ms by
  default), and `:max_output_bytes` bounds the complete output (1 MiB by
  default). Newlines, CR, invalid UTF-8 and a final unfinished line are preserved.
  Nonzero exit status returns `{:ok, output, status}` for caller interpretation.

  Output or queue pressure, deadline expiry and cleanup failure return explicit
  errors; truncated output is never reported as success. Startup and cleanup
  use the existing subprocess budgets. Cleanup follows the read deadline and
  can add its finite budget plus actor-call scheduling allowance. Kernel and Port-driver allocation remain outside managed queue counters. Child environment, working
  directory, stderr redirection and group policy are the same as `open/2`.
  `:owner` is always the capturing caller for this finite operation.

  Byte overflow reports `:output_too_large`; count/mailbox pressure retains
  `:queue_frame_limit` / `:mailbox_pressure`. Managed frame count remains bounded
  even when output consists of many empty lines. Cleanup failure takes precedence
  and includes the capture result's reason in `{:cleanup_failed, reason, cleanup}`.
  If close is unconfirmed, capture force-stops only its own freshly opened
  actor so its guardian attempts bounded cleanup, retaining the explicit error.
  """
  @spec capture([binary()], keyword()) :: {:ok, binary(), non_neg_integer()} | {:error, term()}
  def capture(command, opts \\ []), do: Arbor.RPC.Subprocess.Capture.run(command, opts)

  @doc "A unique, opaque identity for this child generation."
  @spec identity(t()) :: reference()
  def identity(%__MODULE__{generation: generation}), do: generation

  @doc """
  Admits a bounded write without suspending on a busy native stdin queue.

  Count/byte exhaustion or finite ledger contention returns `{:error, :backpressure}`.
  The per-write byte cap uses the Actor's immutable configuration; admitted
  iodata is detached before owner retention. The default original call deadline
  is `cleanup_timeout + 1000` ms and covers validation, admission, queueing and
  native submission. An expired or dead producer cannot initiate a new write.
  A result queued while a caller is suspended past that cutoff returns timeout.

  Caller timeout/death releases work proven not to have started. A started or
  uncertain physical write stays charged until a conclusive native ACK or
  proven pre-command rejection, or until the Actor's ETS owner dies. `:ok`
  confirms bounded native helper admission, not consumption by the child;
  accepted bytes can reach stdin after the caller returns. Actor death releases
  BEAM retention and does not certify OS cleanup: use `cleanup_receipt/1`.
  Raw Erlang messages and direct calls to the private Actor are outside this
  supported admission boundary. Input traversal/materialization and ETS work
  are synchronous cooperative work, not a hard CPU/RSS scheduling guarantee.
  """
  @spec write(t(), iodata()) :: :ok | {:error, term()}
  def write(%__MODULE__{} = handle, data), do: call(handle, {:write, data})

  @doc """
  Closes the child within its finite budget; repeated close is safe.

  A close timeout retains `{:error, :timeout}` and force-stops only the matching
  local actor generation, letting its guardian attempt bounded cleanup. Actor
  death does not confirm OS cleanup. Stale or forged handles cannot signal a
  different process. An available typed guardian receipt, rather than Actor death, confirms cleanup.
  """
  @spec close(t() | nil) :: :ok | {:error, term()}
  def close(nil), do: :ok

  def close(%__MODULE__{} = handle) do
    case validate_actor(handle) do
      :ok -> close_actor(handle)
      {:error, :closed} -> close_guardian(handle)
      error -> error
    end
  end

  defp close_actor(handle) do
    case call(handle, :close) do
      {:error, :timeout} = error ->
        if validate_actor(handle) == :ok, do: Process.exit(handle.pid, :kill)
        error

      {:error, :closed} ->
        close_guardian(handle)

      result ->
        result
    end
  end

  @doc "Returns the retained typed cleanup receipt, or an explicit pending/unavailable error."
  @spec cleanup_receipt(t()) :: {:ok, Receipt.t()} | {:error, term()}
  def cleanup_receipt(%__MODULE__{} = handle) do
    with :ok <- validate_guardian(handle) do
      Guardian.receipt(handle.guardian, handle.generation, handle.cleanup_timeout + 200)
    end
  end

  defp close_guardian(handle) do
    with :ok <- validate_guardian(handle) do
      Guardian.close(handle.guardian, handle.generation, handle.cleanup_timeout + 200)
    end
  end

  defp validate_guardian(%__MODULE__{guardian: pid, generation: generation}) when is_pid(pid) do
    if node(pid) == node() do
      case Process.info(pid, [:initial_call, :dictionary]) do
        nil ->
          {:error, :cleanup_status_unavailable}

        [initial_call: {:proc_lib, :init_p, 5}, dictionary: dictionary] ->
          if List.keyfind(dictionary, :"$initial_call", 0) ==
               {:"$initial_call", {Guardian, :init, 1}} and
               List.keyfind(dictionary, {Guardian, :generation}, 0) ==
                 {{Guardian, :generation}, generation},
             do: :ok,
             else: {:error, :invalid_guardian_identity}

        _ ->
          {:error, :invalid_guardian_identity}
      end
    else
      {:error, :remote_handle_not_supported}
    end
  end

  defp validate_guardian(_handle), do: {:error, :invalid_guardian_identity}

  defp validate_actor(%__MODULE__{pid: pid, generation: generation}) when is_pid(pid) do
    if node(pid) == node() do
      case Process.info(pid, [:initial_call, :dictionary]) do
        nil ->
          {:error, :closed}

        [initial_call: {:proc_lib, :init_p, 5}, dictionary: dictionary] ->
          validate_actor_dictionary(dictionary, generation)

        _other_process ->
          {:error, :invalid_handle}
      end
    else
      {:error, :remote_handle_not_supported}
    end
  end

  defp validate_actor(_handle), do: {:error, :invalid_handle}

  defp validate_actor_dictionary(dictionary, generation) do
    case List.keyfind(dictionary, :"$initial_call", 0) do
      {:"$initial_call", {Actor, :init, 1}} ->
        expected = {{Actor, :generation}, generation}

        if List.keyfind(dictionary, {Actor, :generation}, 0) == expected,
          do: :ok,
          else: {:error, :stale_generation}

      _ ->
        {:error, :invalid_handle}
    end
  end

  @spec connected?(t()) :: boolean()
  def connected?(handle), do: call(handle, :connected?) == true

  @doc "The owned child's OS PID for diagnostics, or nil after the actor stops."
  @spec os_pid(t()) :: pos_integer() | nil
  def os_pid(handle) do
    case call(handle, :os_pid) do
      pid when is_integer(pid) and pid > 0 -> pid
      _ -> nil
    end
  end

  @doc "The stable process a protocol wrapper may monitor for connection failure."
  @spec linked_processes(t()) :: [pid()]
  def linked_processes(%__MODULE__{pid: pid}), do: [pid]

  @doc "Queue diagnostics; excludes asynchronous Port driver messages."
  @spec stats(t()) :: map() | {:error, term()}
  def stats(handle), do: call(handle, :stats)

  @doc false
  @spec call(t(), term(), timeout() | nil) :: term()
  def call(handle, request, timeout \\ nil)

  def call(%__MODULE__{cleanup_timeout: budget} = handle, {:write, data}, timeout) do
    timeout = if is_nil(timeout), do: budget + 1000, else: timeout

    deadline =
      if timeout == :infinity, do: :infinity, else: System.monotonic_time(:millisecond) + timeout

    with :ok <- validate_actor(handle),
         {:ok, table} <- write_table(handle) do
      WriteAdmission.submit(table, handle.pid, handle.generation, data, deadline)
    end
  end

  def call(
        %__MODULE__{pid: pid, generation: generation, cleanup_timeout: budget},
        request,
        timeout
      ) do
    timeout = if is_nil(timeout), do: budget + 1000, else: timeout

    if node(pid) == node(),
      do: GenServer.call(pid, {generation, request}, timeout),
      else: {:error, :remote_handle_not_supported}
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
    :exit, _ -> {:error, :closed}
  end

  defp write_table(handle) do
    case Process.info(handle.pid, :dictionary) do
      {:dictionary, dictionary} ->
        case List.keyfind(dictionary, {Actor, :writes}, 0) do
          {{Actor, :writes}, table} -> {:ok, table}
          _ -> {:error, :closed}
        end

      nil ->
        {:error, :closed}
    end
  end
end
