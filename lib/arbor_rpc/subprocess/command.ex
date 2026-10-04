defmodule Arbor.RPC.Subprocess.Command do
  @moduledoc """
  Resolves a child command against its own working directory and environment.

  No host PATH fallback is attempted when an explicit child PATH is absent or
  unset. Environment policy belongs to the caller; this module applies the
  shared isolation policy and explicit overrides without protocol defaults.
  """

  import Bitwise
  alias Arbor.RPC.PortEnvironment

  @type t :: %{
          executable: binary(),
          args: [binary()],
          cd: binary(),
          env: PortEnvironment.normalized()
        }

  @spec resolve([binary()], keyword(), map(), PortEnvironment.os_type()) ::
          {:ok, t()} | {:error, term()}
  def resolve(command, opts, host_env \\ System.get_env(), os_type \\ :os.type()) do
    with :ok <- PortEnvironment.validate_policy(opts),
         :ok <- validate_command(command),
         {:ok, cd} <- working_directory(opts),
         env <-
           Map.merge(
             PortEnvironment.base(opts, host_env, os_type),
             PortEnvironment.normalize(Keyword.get(opts, :env, []))
           ),
         path <- PortEnvironment.child_path(opts, host_env, os_type),
         {:ok, executable} <- executable(hd(command), cd, path, env, os_type) do
      {:ok, %{executable: executable, args: tl(command), cd: cd, env: env}}
    end
  end

  defp validate_command([exe | args]) when is_binary(exe) and exe != "" do
    if Enum.all?(args, &is_binary/1) and
         Enum.all?([exe | args], &(not String.contains?(&1, <<0>>))),
       do: :ok,
       else: {:error, :invalid_command}
  end

  defp validate_command(_), do: {:error, :invalid_command}

  defp working_directory(opts) do
    case Keyword.get(opts, :cd, File.cwd!()) do
      cd when is_binary(cd) ->
        expanded = Path.expand(cd)
        if File.dir?(expanded), do: {:ok, expanded}, else: {:error, {:invalid_cd, cd}}

      other ->
        {:error, {:invalid_cd, other}}
    end
  end

  defp executable(name, cd, path, env, os_type) do
    candidates =
      if Path.type(name) == :absolute or String.contains?(name, ["/", "\\"]) do
        [Path.expand(name, cd)]
      else
        separator = if match?({:win32, _}, os_type), do: ";", else: ":"

        if is_binary(path),
          do: Enum.map(String.split(path, separator), &Path.expand(Path.join(&1, name), cd)),
          else: []
      end

    candidates = windows_extensions(candidates, env, os_type)

    case Enum.find(candidates, &executable_file?(&1, os_type)) do
      nil -> {:error, {:executable_not_found, name}}
      found -> {:ok, found}
    end
  end

  defp windows_extensions(paths, env, {:win32, _}) do
    extensions = Map.get(env, "PATHEXT", ".COM;.EXE;.BAT;.CMD")
    extensions = if is_binary(extensions), do: String.split(extensions, ";"), else: []

    Enum.flat_map(paths, fn path ->
      if Path.extname(path) == "", do: [path | Enum.map(extensions, &(path <> &1))], else: [path]
    end)
  end

  defp windows_extensions(paths, _env, _os_type), do: paths

  defp executable_file?(path, {:win32, _}), do: File.regular?(path)

  defp executable_file?(path, _unix) do
    case File.stat(path) do
      {:ok, %{type: :regular, mode: mode}} -> (mode &&& 0o111) != 0
      _ -> false
    end
  end
end
