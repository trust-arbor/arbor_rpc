defmodule ArchiveConsumer do
  @moduledoc false

  def probe do
    {:ok, _} = Application.ensure_all_started(:archive_consumer)
    expected_version = System.fetch_env!("ARCHIVE_EXPECTED_VERSION")

    for app <- [:arbor_rpc] do
      ^expected_version = app |> Application.spec(:vsn) |> to_string()
    end

    # This is run both from an installed Mix application and an assembled
    # release. Executables below are absolute; a runtime compiler is unavailable.
    System.put_env("PATH", "/no-runtime-compiler")
    System.put_env("CC", "/compiler-must-not-run")
    nil = System.find_executable("cc")

    priv = :arbor_rpc |> :code.priv_dir() |> List.to_string()
    true = File.regular?(Path.join(priv, "native/arbor_rpc_subprocess"))

    {:ok, "installed-final\r\nlast", 7} =
      Arbor.RPC.Subprocess.capture([
        "/bin/sh",
        "-c",
        "printf 'installed-final\\r\\nlast'; exit 7"
      ])

    {:ok, child} = Arbor.RPC.Subprocess.open(["/bin/sleep", "30"], process_group: true)
    :ok = Arbor.RPC.Subprocess.close(child)

    {:ok, %{direct_child: :reaped, targeted_group: :absent}} =
      Arbor.RPC.Subprocess.cleanup_receipt(child)

    :ok = Arbor.RPC.Subprocess.close(child)
    apps = Application.started_applications() |> Enum.map(&elem(&1, 0))
    true = Enum.all?([:arbor_rpc], &(&1 in apps))

    record_installation()

    IO.puts(
      "Archive apps, installed helper, exact bytes/status and retained cleanup receipt pass"
    )
  end

  defp record_installation do
    if path = System.get_env("ARCHIVE_INSTALL_REPORT") do
      apps = [:arbor_rpc]
      helper = Path.join(to_string(:code.priv_dir(:arbor_rpc)), "native/arbor_rpc_subprocess")

      digest = fn path ->
        path |> File.read!() |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
      end

      packages =
        Map.new(apps, fn app ->
          app_file = Path.join([to_string(:code.lib_dir(app)), "ebin", "#{app}.app"])
          modules = Application.spec(app, :modules)

          code =
            Map.new(modules, fn module ->
              {Atom.to_string(module), digest.(to_string(:code.which(module)))}
            end)

          {Atom.to_string(app),
           %{
             version: to_string(Application.spec(app, :vsn)),
             app_sha256: digest.(app_file),
             beam_sha256: code
           }}
        end)

      File.write!(path, Jason.encode!(%{helper_sha256: digest.(helper), packages: packages}))
    end
  end
end
