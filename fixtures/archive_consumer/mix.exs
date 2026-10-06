defmodule ArchiveConsumer.MixProject do
  use Mix.Project

  def project do
    [
      app: :archive_consumer,
      version: "0.1.0",
      elixir: "~> 1.17",
      deps: deps(),
      releases: [archive_consumer: [include_erts: true]]
    ]
  end

  def application do
    [extra_applications: [:logger, :arbor_rpc]]
  end

  defp deps do
    packages =
      for package <- [:arbor_rpc] do
        {package, path: "packages/#{package}", override: true}
      end

    # Offline local qualification can reuse external source copies. CI leaves
    # this unset and resolves the archives' external dependencies through Hex.
    external =
      case System.get_env("ARCHIVE_CONSUMER_EXTERNAL_DEPS") do
        nil ->
          []

        root ->
          for app <- [:jason],
              do: {app, path: Path.join(root, to_string(app)), override: true}
      end

    packages ++ external
  end
end
