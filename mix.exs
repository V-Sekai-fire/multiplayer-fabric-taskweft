defmodule Taskweft.MixProject do
  use Mix.Project

  def project do
    [
      app: :taskweft,
      version: "0.1.0",
      elixir: "~> 1.17",
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_env: fn -> %{"FINE_INCLUDE_DIR" => Fine.include_dir()} end,
      deps: deps()
    ]
  end

  def cli do
    [preferred_envs: [propcheck: :test]]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:fine, "~> 0.1"},
      {:elixir_make, "~> 0.9"},
      {:exqlite, "~> 0.23"},
      {:jason, "~> 1.4"},
      {:propcheck, "~> 1.4", only: [:test, :dev], runtime: false}
    ]
  end
end
