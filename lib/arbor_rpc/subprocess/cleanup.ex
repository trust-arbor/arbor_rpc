defmodule Arbor.RPC.Subprocess.Cleanup do
  @moduledoc false
  alias Arbor.RPC.PortEnvironment

  # This proof is constructed only from the port we opened. A caller cannot
  # supply a signal target. A negative group target requires a measured PGID
  # equal to that owned child PID; never use the VM's process group.
  def proof(port, process_group, timeout) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} when pid > 1 ->
        proof = %{pid: pid, group: false}

        case {process_group, :os.type()} do
          {false, _} -> {:ok, proof}
          {true, {:unix, _}} -> verify_group(proof, timeout)
          {true, _} -> {:error, :process_group_unsupported, proof}
        end

      _ ->
        {:error, :child_pid_unavailable, nil}
    end
  rescue
    ArgumentError -> {:error, :child_pid_unavailable, nil}
  end

  defp verify_group(%{pid: pid} = proof, timeout) do
    with {:ok, output, 0} <- utility("ps", ["-o", "pgid=", "-p", Integer.to_string(pid)], timeout),
         {^pid, ""} <- Integer.parse(String.trim(output)),
         true <- pid != String.to_integer(System.pid()) do
      {:ok, %{proof | group: true}}
    else
      _ -> {:error, :child_not_process_group_leader, proof}
    end
  end

  def run(nil, port, _opts), do: close_port(port)

  def run(proof, port, opts) do
    deadline = now() + Keyword.fetch!(opts, :cleanup_timeout)
    # Stop future driver input before the TERM grace period. Cleanup still
    # uses the owned PID/group proof captured while the Port was open.
    close_port(port)

    case :os.type() do
      {:unix, _} ->
        # Reserve a separate KILL allowance. A failed/exhausted liveness probe
        # is unknown, never evidence that the child has exited.
        reserve = 50
        signal(proof, "TERM", max(0, remaining(deadline) - reserve))
        grace_deadline = min(deadline - reserve, now() + Keyword.fetch!(opts, :term_grace))

        case wait_until_gone(proof, grace_deadline) do
          :gone -> :ok
          _alive_or_unknown -> kill(proof, deadline)
        end

      {:win32, _} ->
        case utility(
               "taskkill",
               ["/PID", Integer.to_string(proof.pid), "/T", "/F"],
               remaining(deadline)
             ) do
          {:ok, _, 0} -> :ok
          result -> {:error, {:cleanup_failed, result}}
        end
    end
  end

  # Natural exit of the leader still needs group cleanup. A non-group target
  # is already reaped, so avoid signalling a PID that may subsequently be reused.
  def after_exit(%{group: true} = proof, port, opts), do: run(proof, port, opts)
  def after_exit(_proof, port, _opts), do: close_port(port)

  defp kill(proof, deadline) do
    case signal(proof, "KILL", remaining(deadline)) do
      {:ok, _, 0} ->
        :ok

      {:ok, _, _status} ->
        if status(proof, remaining(deadline)) == :gone, do: :ok, else: {:error, :cleanup_failed}

      {:error, :timeout} ->
        {:error, :cleanup_timeout}

      {:error, reason} ->
        {:error, {:cleanup_failed, reason}}
    end
  end

  defp status(proof, timeout) do
    classify_probe(signal(proof, "0", timeout))
  end

  @doc false
  def classify_probe({:ok, _output, 0}), do: :alive

  def classify_probe({:ok, output, status}) when is_binary(output) do
    # Both fixed Unix kill utilities report ESRCH in this form under LC_ALL=C.
    # EPERM and utility/argument errors are unknown, never proof of exit.
    if String.contains?(output, "No such process"),
      do: :gone,
      else: {:error, {:liveness_unconfirmed, status}}
  end

  def classify_probe(error), do: error

  defp signal(_proof, _signal, timeout) when timeout <= 0, do: {:error, :timeout}

  defp signal(%{pid: pid, group: group}, signal, timeout) do
    target = if group, do: "-#{pid}", else: Integer.to_string(pid)
    utility("kill", ["-#{signal}", "--", target], timeout)
  end

  defp wait_until_gone(proof, deadline) do
    if remaining(deadline) > 0 do
      case status(proof, remaining(deadline)) do
        :alive ->
          receive do
          after
            min(10, remaining(deadline)) -> wait_until_gone(proof, deadline)
          end

        result ->
          result
      end
    else
      {:error, :timeout}
    end
  end

  # Cleanup utilities have bounded output, a monotonic deadline and their own
  # port cleanup. System.cmd in a timed Task would leave the utility alive when
  # only its BEAM caller is killed. Unix utilities use fixed system locations.
  defp utility(name, args, timeout) when timeout > 0 do
    executable =
      case :os.type() do
        {:unix, _} -> Enum.find(["/bin/#{name}", "/usr/bin/#{name}"], &File.regular?/1)
        _ -> System.find_executable(name)
      end

    if executable do
      port =
        Port.open(
          {:spawn_executable, String.to_charlist(executable)},
          [
            :binary,
            :exit_status,
            :stderr_to_stdout,
            :hide,
            args: args,
            env:
              [] |> PortEnvironment.base() |> Map.put("LC_ALL", "C") |> PortEnvironment.to_port()
          ]
        )

      result = collect(port, now() + timeout, "")
      close_port(port)
      result
    else
      {:error, :utility_unavailable}
    end
  catch
    :error, reason -> {:error, reason}
  end

  defp utility(_name, _args, _timeout), do: {:error, :timeout}

  defp collect(port, deadline, output) do
    receive do
      {^port, {:data, data}} when byte_size(output) + byte_size(data) <= 4096 ->
        collect(port, deadline, output <> data)

      {^port, {:data, _data}} ->
        {:error, :utility_output_limit}

      {^port, {:exit_status, status}} ->
        {:ok, output, status}
    after
      remaining(deadline) -> {:error, :timeout}
    end
  end

  defp close_port(nil), do: :ok

  defp close_port(port) do
    Port.close(port)
    :ok
  catch
    :error, _ -> :ok
  end

  defp now, do: System.monotonic_time(:millisecond)
  defp remaining(deadline), do: max(0, deadline - now())
end
