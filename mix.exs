defmodule OffBroadway.Splunk.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://gitlab.intility.com/soc/off_broadway_splunk"

  def project do
    [
      app: :off_broadway_splunk,
      version: @version,
      elixir: "~> 1.13",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: [
        maintainers: ["Rolf Håvard Blindheim <rolf.havard.blindheim@intility.no>"],
        licenses: ["Apache-2.0"],
        links: %{Gitlab: @source_url}
      ],
      docs: [
        main: "readme",
        source_ref: "v#{@version}",
        extras: [
          "README.md",
          "LICENSE"
        ]
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: extra_applications(Mix.env()),
      mod: {OffBroadway.Splunk.Application, []}
    ]
  end

  def extra_applications(env) when env in [:dev, :test], do: [:logger, :hackney]
  def extra_applications(_), do: [:logger]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 1.0"},
      {:decimal, "~> 2.0"},
      {:exconstructor, "~> 1.2"},
      {:tesla, "~> 1.4"},
      {:jason, ">= 1.0.0"},
      {:hackney, "~> 1.18", optional: true},
      {:ex_doc, "~> 0.28.4", only: [:dev, :test], runtime: false},
      {:junit_formatter, "~> 3.3", only: :test},
      {:mox, "~> 1.0", only: :test}
    ]
  end
end
