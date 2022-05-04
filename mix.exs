defmodule OffBroadwaySplunk.MixProject do
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
        maintainers: ["Rolf Håvard Blindheim"],
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
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 1.0"},
      {:mox, "~> 1.0", only: :test},
      {:ex_doc, "~> 0.28.4", only: :dev, runtime: false}
    ]
  end
end
