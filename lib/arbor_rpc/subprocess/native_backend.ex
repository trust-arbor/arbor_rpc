defmodule Arbor.RPC.Subprocess.NativeBackend do
  @moduledoc false
  @behaviour Arbor.RPC.Subprocess.Backend

  alias Arbor.RPC.PortEnvironment

  @doc false
  def helper_path do
    case :code.priv_dir(:arbor_rpc) do
      directory when is_list(directory) ->
        {:ok, Path.join(List.to_string(directory), "native/arbor_rpc_subprocess")}

      _ ->
        {:error, :native_helper_unavailable}
    end
  end

  @impl true
  def open(command, opts, limits) do
    with {:unix, platform} when platform in [:darwin, :linux] <- :os.type(),
         {:ok, executable} <- helper_path(),
         true <- File.regular?(executable) do
      args = [
        "1",
        bool(opts, :process_group),
        bool(opts, :stderr_to_stdout),
        Integer.to_string(limits.term_grace),
        Integer.to_string(limits.cleanup_timeout),
        "5000",
        Integer.to_string(limits.closed_retention),
        Integer.to_string(limits.max_write_bytes),
        Integer.to_string(limits.startup_timeout),
        "--",
        command.executable | command.args
      ]

      port =
        Port.open({:spawn_executable, String.to_charlist(executable)}, [
          :binary,
          :exit_status,
          :eof,
          :use_stdio,
          :hide,
          {:packet, 4},
          args: args,
          cd: String.to_charlist(command.cd),
          env: PortEnvironment.to_port(command.env),
          busy_limits_port: {4096, 8192},
          busy_limits_msgq: {4096, 8192}
        ])

      {:ok, port}
    else
      false -> {:error, :native_helper_unavailable}
      {:error, _reason} = error -> error
      platform -> {:error, {:unsupported_subprocess_backend, platform}}
    end
  catch
    :error, reason -> {:error, {:native_helper_open_failed, reason}}
  end

  @impl true
  def command(port, packet) do
    if Port.command(port, packet, [:nosuspend]), do: :ok, else: {:error, :backpressure}
  catch
    :error, :badarg -> {:error, :helper_closed}
  end

  @impl true
  def close(port) do
    Port.close(port)
    :ok
  catch
    :error, :badarg -> :ok
  end

  defp bool(opts, key), do: if(Keyword.get(opts, key, false), do: "1", else: "0")
end
