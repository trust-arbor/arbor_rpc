defmodule ArborRpc.MixProject do
  use Mix.Project
  @version "2.0.0-dev"
  def project do
    [
      app: :arbor_rpc,
      version: @version,
      elixir: "~> 1.17",
      elixirc_paths: paths(Mix.env()),
      deps: deps(),
      description: "Shared JSON-RPC, framing and environment mechanics for Arbor protocols.",
      package: [
        licenses: ["MIT"],
        links: %{"GitHub" => "https://github.com/trust-arbor/arbor_acp"},
        files: ~w(lib mix.exs .formatter.exs README.md LICENSE CHANGELOG.md)
      ],
      source_url: "https://github.com/trust-arbor/arbor_acp",
      docs: [name: "ArborRPC", main: "readme", extras: ["README.md", "CHANGELOG.md"]]
    ]
  end

  def application, do: [extra_applications: [:logger]]
  defp paths(:test), do: ["lib", "dev", "test/support"]
  defp paths(:dev), do: ["lib", "dev"]
  defp paths(_), do: ["lib"]

  defp deps do
    [
      external_dep(:jason, "~> 1.4")
    ]
  end

  defp external_dep(app, version) do
    case System.get_env("ARBOR_V2_DEPS") do
      nil -> {app, version}
      directory -> {app, path: Path.join(directory, to_string(app)), override: true}
    end
  end
end
