defmodule MergeIntoPolyfill.MixProject do
  use Mix.Project

  def project do
    [
      app: :merge_into_polyfill,
      version: "0.1.0-rc-0",
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      description: "DSL and polyfill to express MERGE queries using ecto (postgres adapter only)",
      package: package(),
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp aliases do
    [
      test: "do ecto.create, ecto.migrate, test"
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:ecto, "~> 3.10"},
      {:ecto, ">= 3.10.1"},
      {:ecto_sql, ">= 3.10.0"},
      {:postgrex, "> 0.0.1"},
      {:jason, ">= 1.0.0"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/ZennerIoT/merge_into_polyfill"}
    ]
  end
end
