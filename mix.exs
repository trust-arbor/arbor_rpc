defmodule Mix.Tasks.Compile.ArborRpcNative do
  use Mix.Task.Compiler
  @recursive true

  # ARBOR_RPC_NATIVE, or `config :arbor_rpc, native_helper: ...`, selects:
  #   auto (default): build when a C compiler is available, otherwise warn
  #   required: a missing compiler or failed build stops compilation
  #   skip: never build; Arbor.RPC.Subprocess reports the helper unavailable
  @policies ~w(auto required skip)

  @impl true
  def run(args) do
    case {policy(), :os.type()} do
      {:skip, _os} ->
        clean()
        {:noop, []}

      {policy, {:unix, platform}} when platform in [:darwin, :linux] ->
        compile(args, policy)

      {:required, platform} ->
        Mix.raise("Arbor RPC native subprocess helper is unsupported on #{inspect(platform)}")

      {:auto, platform} ->
        Mix.shell().info(
          "Arbor RPC native subprocess helper is unavailable on #{inspect(platform)}; framing remains available"
        )

        {:noop, []}
    end
  end

  @impl true
  def manifests, do: [manifest()]

  @impl true
  def clean do
    File.rm(target())
    File.rm(manifest())
    :ok
  end

  defp policy do
    value =
      case System.get_env("ARBOR_RPC_NATIVE") do
        empty when empty in [nil, ""] -> Application.get_env(:arbor_rpc, :native_helper, :auto)
        value -> value
      end

    if to_string(value) in @policies do
      String.to_atom(to_string(value))
    else
      Mix.raise(
        "ARBOR_RPC_NATIVE / :native_helper must be auto, required or skip, got: #{inspect(value)}"
      )
    end
  end

  defp compile(args, policy) do
    # The stamp's prefix is what a compiler-free rebuild can still verify.
    origin =
      digest(File.read!(source())) <> " " <> digest(:erlang.system_info(:system_architecture))

    compiler =
      case System.get_env("CC") do
        empty when empty in [nil, ""] -> "cc"
        compiler -> compiler
      end

    if compiler =~ ~r/\s/,
      do:
        Mix.raise("CC must name one compiler executable, without arguments: #{inspect(compiler)}")

    case System.find_executable(compiler) do
      nil ->
        # Without a compiler, keep only a helper built from this source for this
        # architecture; anything older could speak another helper protocol.
        if File.regular?(target()) and built_from?(origin) do
          {:noop, []}
        else
          clean()
          unavailable(policy, "no C compiler was found (CC=#{inspect(compiler)})")
        end

      executable ->
        {version, _status} = System.cmd(executable, ["--version"], stderr_to_stdout: true)
        stamp = origin <> " " <> digest({executable, version, flags()})

        if "--force" in args or not File.regular?(target()) or
             File.read(manifest()) != {:ok, stamp},
           do: build(executable, stamp, policy),
           else: {:noop, []}
    end
  end

  defp build(executable, stamp, policy) do
    target = target()
    prepare_priv()
    temporary = target <> ".building"
    Mix.shell().info("Compiling Arbor RPC native subprocess helper")

    case System.cmd(executable, flags() ++ [source(), "-o", temporary], stderr_to_stdout: true) do
      {output, 0} ->
        if output != "", do: Mix.shell().info(output)
        File.chmod!(temporary, 0o755)
        File.rename!(temporary, target)
        File.mkdir_p!(Path.dirname(manifest()))
        File.write!(manifest(), stamp)
        {:ok, []}

      {output, status} ->
        File.rm(temporary)
        clean()
        unavailable(policy, "the C compiler exited with status #{status}:\n#{output}")
    end
  end

  defp built_from?(origin) do
    case File.read(manifest()) do
      {:ok, stamp} -> String.starts_with?(stamp, origin <> " ")
      _ -> false
    end
  end

  defp digest(term) when is_binary(term), do: :crypto.hash(:sha256, term) |> Base.encode16()
  defp digest(term), do: term |> :erlang.term_to_binary() |> digest()

  # Older releases wrote the helper into the source priv/native and linked
  # _build to it. Remove those generated files, then a link left dangling.
  defp prepare_priv do
    legacy = Path.expand("priv/native", __DIR__)

    for name <-
          ~w(arbor_rpc_subprocess arbor_rpc_subprocess.sha256 arbor_rpc_subprocess.building),
        do: File.rm(Path.join(legacy, name))

    _ = File.rmdir(legacy)
    _ = File.rmdir(Path.dirname(legacy))
    priv = Path.join(Mix.Project.app_path(), "priv")

    with {:ok, _link} <- File.read_link(priv),
         false <- File.dir?(priv),
         do: File.rm!(priv)

    File.mkdir_p!(Path.dirname(target()))
  end

  defp unavailable(:required, reason),
    do: Mix.raise("Arbor RPC native subprocess helper is required but was not built: #{reason}")

  defp unavailable(:auto, reason) do
    Mix.shell().info([
      :yellow,
      """
      warning: Arbor RPC native subprocess helper was not built: #{reason}
        Arbor.RPC.Subprocess, and stdio transports built on it, will return
        {:error, :native_helper_unavailable}. JSON-RPC and framing are unaffected.
        Install a C17 compiler (or set CC), then run `mix deps.compile arbor_rpc --force`.
        Set ARBOR_RPC_NATIVE=skip to silence this, or ARBOR_RPC_NATIVE=required to fail.
      """,
      :reset
    ])

    {:noop, []}
  end

  # Warnings fail the build only in this repository: dependencies compile in :prod.
  defp flags do
    base = ~w(-std=c17 -O2 -Wall -Wextra -pedantic)
    if Mix.env() in [:dev, :test], do: base ++ ["-Werror"], else: base
  end

  defp source, do: Path.expand("c_src/subprocess_helper.c", __DIR__)
  defp target, do: Path.join(Mix.Project.app_path(), "priv/native/arbor_rpc_subprocess")
  defp manifest, do: Path.join(Mix.Project.manifest_path(), "compile.arbor_rpc_native")
end

defmodule Arbor.RPC.MixProject do
  use Mix.Project
  @version "1.0.0-rc.4"
  def project do
    [
      app: :arbor_rpc,
      name: "ArborRPC",
      version: @version,
      elixir: "~> 1.17",
      elixirc_paths: paths(Mix.env()),
      compilers: [:arbor_rpc_native] ++ Mix.compilers(),
      deps: deps(),
      description: "ArborRPC: shared JSON-RPC, framing and environment mechanics.",
      package: [
        licenses: ["MIT"],
        links: %{"GitHub" => "https://github.com/trust-arbor/arbor_rpc"},
        files:
          ~w(lib c_src docs mix.exs .formatter.exs README.md usage-rules.md LICENSE CHANGELOG.md)
      ],
      source_url: "https://github.com/trust-arbor/arbor_rpc",
      docs: [
        main: "readme",
        extras: [
          "README.md",
          {"usage-rules.md", title: "Agent usage rules"},
          "docs/QUICKSTART.md",
          "docs/TROUBLESHOOTING.md",
          "docs/DEVELOPMENT.md",
          "CHANGELOG.md"
        ],
        source_ref: "v#{@version}",
        source_url_pattern:
          "https://github.com/trust-arbor/arbor_rpc/blob/v#{@version}/%{path}#L%{line}"
      ]
    ]
  end

  def application, do: [extra_applications: [:logger, :crypto]]
  defp paths(:test), do: ["lib", "dev", "test/support"]
  defp paths(:dev), do: ["lib", "dev"]
  defp paths(_), do: ["lib"]

  defp deps do
    [
      external_dep(:jason, "~> 1.4"),
      external_dep(:ex_doc, "~> 0.40", only: :dev, runtime: false)
    ]
  end

  defp external_dep(app, version, opts \\ []) do
    case System.get_env("ARBOR_V2_DEPS") do
      nil ->
        {app, version, opts}

      directory ->
        {app, version,
         Keyword.merge(opts,
           path: Path.join(directory, to_string(app)),
           override: true
         )}
    end
  end
end
