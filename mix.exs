defmodule Mix.Tasks.Compile.ArborRpcNative do
  use Mix.Task.Compiler
  @recursive true

  def run(args) do
    case :os.type() do
      {:unix, platform} when platform in [:darwin, :linux] ->
        result = compile(args)
        # Mix builds the app structure before custom compilers. A source
        # archive initially has no generated priv directory; refresh after
        # building it so installed code/release lookup sees the helper.
        Mix.Project.build_structure()
        result

      platform ->
        Mix.shell().info(
          "Arbor RPC native subprocess helper is unavailable on #{inspect(platform)}; framing remains available"
        )

        {:noop, []}
    end
  end

  defp compile(args) do
    source = Path.expand("c_src/subprocess_helper.c", __DIR__)
    target = Path.expand("priv/native/arbor_rpc_subprocess", __DIR__)
    manifest = target <> ".sha256"
    compiler = System.get_env("CC") || "cc"
    flags = ~w(-std=c17 -O2 -Wall -Wextra -Werror -pedantic)

    digest =
      :crypto.hash(
        :sha256,
        :erlang.term_to_binary(
          {File.read!(source), compiler, flags, :erlang.system_info(:system_architecture)}
        )
      )
      |> Base.encode16()

    if "--force" in args or not File.regular?(target) or File.read(manifest) != {:ok, digest} do
      executable =
        System.find_executable(compiler) ||
          Mix.raise(
            "Arbor RPC source-build backend requires a C compiler; set CC to a compiler executable"
          )

      File.mkdir_p!(Path.dirname(target))
      temporary = target <> ".building"

      case System.cmd(executable, flags ++ [source, "-o", temporary], stderr_to_stdout: true) do
        {_output, 0} ->
          File.chmod!(temporary, 0o755)
          File.rename!(temporary, target)
          File.write!(manifest, digest)
          {:ok, []}

        {output, status} ->
          File.rm(temporary)
          Mix.raise("Arbor RPC native helper build failed (#{status}): #{output}")
      end
    else
      {:noop, []}
    end
  end
end

defmodule Arbor.RPC.MixProject do
  use Mix.Project
  @version "2.0.0-rc.1"
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
        files: ~w(lib c_src docs mix.exs .formatter.exs README.md LICENSE CHANGELOG.md)
      ],
      source_url: "https://github.com/trust-arbor/arbor_rpc",
      docs: [
        main: "readme",
        extras: [
          "README.md",
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
