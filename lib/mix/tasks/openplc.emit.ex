# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Mix.Tasks.Openplc.Emit do
  @moduledoc """
  Emit PLCopen XML (FBD-only per RFD 2145) from a compact GRAFCET
  document (RFD 2143). Output is one `<pou>` element the operator
  can hand to `openplc-cli compile` (see `mix openplc.compile`).

      mix openplc.emit <grafcet.jsonld> [--out <path>]

  Default `--out` is `build/openplc/<sfc-name>.plcopen.xml`.
  """
  use Mix.Task
  @shortdoc "Emit PLCopen FBD XML from compact GRAFCET (RFD 2145 stage 1)"

  alias Taskweft.OpenPLC.PLCopen

  @impl true
  def run(argv) do
    {opts, [path]} = OptionParser.parse!(argv, strict: [out: :string])

    grafcet = path |> File.read!() |> Jason.decode!()
    name = grafcet["sfc"] || Path.basename(path, ".grafcet.jsonld")

    out =
      Keyword.get(opts, :out) ||
        Path.join("build/openplc", "#{name}.plcopen.xml")

    File.mkdir_p!(Path.dirname(out))
    File.write!(out, PLCopen.emit(grafcet))
    Mix.shell().info("PLCopen FBD -> #{out}")
  end
end
