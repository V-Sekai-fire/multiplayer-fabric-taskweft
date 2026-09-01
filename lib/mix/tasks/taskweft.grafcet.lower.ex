# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Mix.Tasks.Taskweft.Grafcet.Lower do
  @moduledoc """
  Lower a directory of compact IEC 60848 GRAFCET JSON-LD files
  (aligned with Project-AGRAFE) into taskweft HTN JSON.

      mix taskweft.grafcet.lower --in <dir> --out <dir>

  Reads every `*.grafcet.jsonld` under `--in`, lowers each via
  `Taskweft.Grafcet.lower/1`, writes to `--out/<stem>.htn.jsonld`.
  """
  use Mix.Task

  alias Taskweft.Grafcet

  @shortdoc "Lower compact GRAFCET personas to HTN JSON"

  @impl true
  def run(argv) do
    {opts, _} = OptionParser.parse!(argv, strict: [in: :string, out: :string])
    in_dir = Keyword.fetch!(opts, :in)
    out_dir = Keyword.fetch!(opts, :out)
    File.mkdir_p!(out_dir)

    files = Path.wildcard(Path.join(in_dir, "*.grafcet.jsonld"))

    if files == [] do
      Mix.raise("no *.grafcet.jsonld files under #{in_dir}")
    end

    for f <- files do
      stem = Path.basename(f, ".grafcet.jsonld")
      htn = f |> File.read!() |> Jason.decode!() |> Grafcet.lower()
      out = Path.join(out_dir, "#{stem}.htn.jsonld")
      File.write!(out, Jason.encode_to_iodata!(htn, pretty: true))
      Mix.shell().info("#{stem}: #{length(Map.keys(htn["actions"]))} actions -> #{out}")
    end
  end
end
