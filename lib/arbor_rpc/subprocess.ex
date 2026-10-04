defmodule Arbor.RPC.Subprocess do
  @moduledoc """
  An owned child process with bounded newline delivery and finite cleanup.

  A stable actor owns the Port and monitors the lifetime owner, which defaults
  to the process that called `open/2`. An explicit live local `:owner` PID lets
  a temporary connection task open the child on behalf of a long-lived client.
  Passing the opaque handle to a handshake task does not transfer ownership of
  the child. `identity/1` is a fresh generation reference, suitable for ignoring
  events from a previous connection. The actor has no protocol or logging policy.

  `Arbor.RPC.FramedStream` provides pull delivery or acknowledged push delivery.
  The configured frame count and bytes include queued and unacknowledged frames.
  An oversized frame, input chunk or queue closes the child with an explicit
  error. Credit bounds the subscriber's frame messages. OTP Port input has no
  read-credit API: driver messages may transiently accumulate in the actor's
  mailbox before its high-water check runs. These counters are not a hard bound
  on that raw driver mailbox or on the bytes retained by an application after
  acknowledging a frame. A consumer must acknowledge after processing a frame,
  rather than after forwarding it to another unbounded mailbox.

  Options include `:owner`, `:cd`, `:env`, `:environment_policy` (default `:isolated`),
  `:process_group` (default `false`), `:stderr_to_stdout` (default `false`),
  `:max_frame_bytes` (1 MiB), `:max_queue_bytes` (4 MiB), `:max_queue_frames`
  (1024), `:max_waiters` (128), `:max_input_chunk_bytes` (4 MiB),
  `:max_mailbox_messages` (1024), `:max_write_bytes` (1 MiB),
  `:cleanup_timeout` (500 ms), `:term_grace` (100 ms) and `:closed_retention`
  (5 seconds). Cleanup requires at least 50 ms beyond TERM grace for KILL.
  Explicit close stops the actor. On natural exit or pressure failure, the
  actor stops after terminal delivery; unused buffered frames expire after
  `:closed_retention` with an explicit drain-timeout closure. Group cleanup is
  accepted on Unix only after verifying the owned child is its group leader.
  The Port retains PID metadata after EOF; actual `exit_status`, rather than
  EOF alone, proves direct-child death. A group leader that exits before group
  ownership can be measured is rejected with `:child_not_process_group_leader`.
  No command replay or unverified group signalling occurs.
  Windows tree cleanup requires platform qualification before a release.
  """

  alias Arbor.RPC.Subprocess.Actor

  @enforce_keys [:pid, :generation, :cleanup_timeout, :max_write_bytes]
  defstruct [:pid, :generation, :cleanup_timeout, :max_write_bytes]

  @opaque t :: %__MODULE__{
            pid: pid(),
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
    case GenServer.start(Actor, {owner, generation, command, opts}) do
      {:ok, pid} ->
        {:ok,
         %__MODULE__{
           pid: pid,
           generation: generation,
           cleanup_timeout: Keyword.get(opts, :cleanup_timeout, 500),
           max_write_bytes: Keyword.get(opts, :max_write_bytes, 1_048_576)
         }}

      {:error, reason} ->
        {:error, reason}
    end
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
  can add its finite budget plus actor-call scheduling allowance. The usual
  raw Port-driver mailbox limitation still applies. Child environment, working
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

  @doc "Writes bytes without suspension when the Port output queue is busy."
  @spec write(t(), iodata()) :: :ok | {:error, term()}
  def write(%__MODULE__{max_write_bytes: limit} = handle, data) do
    case admit_write(data, limit) do
      :ok -> call(handle, {:write, data})
      {:error, _reason} = error -> error
    end
  end

  defp admit_write(data, limit) do
    if :erlang.iolist_size(data) <= limit, do: :ok, else: {:error, :write_too_large}
  catch
    :error, :badarg -> {:error, :invalid_iodata}
  end

  @doc """
  Closes the child within its finite budget; repeated close is safe.

  A close timeout retains `{:error, :timeout}` and force-stops only the matching
  local actor generation, letting its guardian attempt bounded cleanup. Actor
  death does not confirm OS cleanup. Stale or forged handles cannot signal a
  different process. The raw guardian ownership-proof limitations still apply.
  """
  @spec close(t() | nil) :: :ok | {:error, term()}
  def close(nil), do: :ok

  def close(%__MODULE__{} = handle) do
    case validate_actor(handle) do
      :ok -> close_actor(handle)
      {:error, :closed} -> :ok
      error -> error
    end
  end

  defp close_actor(handle) do
    case call(handle, :close) do
      {:error, :timeout} = error ->
        if validate_actor(handle) == :ok, do: Process.exit(handle.pid, :kill)
        error

      {:error, :closed} ->
        :ok

      result ->
        result
    end
  end

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
  def call(
        %__MODULE__{pid: pid, generation: generation, cleanup_timeout: budget},
        request,
        timeout \\ nil
      ) do
    timeout = if is_nil(timeout), do: budget + 1000, else: timeout

    if node(pid) == node(),
      do: GenServer.call(pid, {generation, request}, timeout),
      else: {:error, :remote_handle_not_supported}
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
    :exit, _ -> {:error, :closed}
  end
end
