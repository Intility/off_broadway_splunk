defmodule OffBroadway.Splunk.MixProject do
  use Mix.Project

  @version "1.2.2"
  @description "Splunk producer for Broadway data processing pipelines"
  @source_url "https://github.com/Intility/off_broadway_splunk"

  def project do
    [
      app: :off_broadway_splunk,
      version: @version,
      elixir: "~> 1.13",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      description: @description,
      deps: deps(),
      package: [
        maintainers: ["Rolf Håvard Blindheim <rolf.havard.blindheim@intility.no>"],
        licenses: ["Apache-2.0"],
        links: %{GitHub: @source_url}
      ],
      docs: [
        main: "readme",
        source_ref: "v#{@version}",
        source_url: @source_url,
        extras: [
          "README.md",
          "LICENSE"
        ]
      ],
      test_coverage: [
        [tool: ExCoveralls],
        summary: [threshold: 80]
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: extra_applications(Mix.env())
    ]
  end

  def extra_applications(env) when env in [:dev, :test], do: [:logger, :hackney]
  def extra_applications(_), do: [:logger]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 1.0"},
      {:decimal, "~> 2.0"},
      {:excoveralls, "~> 0.15.0", only: :test},
      {:ex_doc, "~> 0.29", only: [:dev, :test], runtime: false},
      {:exconstructor, "~> 1.2"},
      {:hackney, "~> 1.18", optional: true},
      {:jason, ">= 1.0.0"},
      {:mix_test_watch, "~> 1.1", only: :dev},
      {:nimble_options, "~> 0.4 or ~> 0.5 or ~> 1.0"},
      {:telemetry, "~> 1.1 or ~> 1.2"},
      {:tesla, "~> 1.4 or ~> 1.5"}
    ]
  end
end
