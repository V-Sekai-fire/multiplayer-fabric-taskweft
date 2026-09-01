# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Mix.Tasks.Openplc.Compile do
  @moduledoc """
  Compile a PLCopen XML file (from `mix openplc.emit`) using the
  operator's installed `openplc-cli` binary from OpenPLC Editor v4.
  The Editor is GPL-3.0 as a tool; STruC++ ships a GCC-style runtime
  library exception so the compiled output carries no GPL obligation.
  RFD 2145 records the aggregation argument.

      mix openplc.compile <path.plcopen.xml> \\
          [--target riscv64|native] [--out <path>] [--cli <path>]

  `--target riscv64` produces a RISC-V shared object for Godot Sandbox
  (RFD 2149). Default target is `native`.
  """
  use Mix.Task
  @shortdoc "Compile PLCopen XML to .so/.riscv via operator-installed openplc-cli"

  @impl true
  def run(argv) do
    {opts, [xml_path]} =
      OptionParser.parse!(argv,
        strict: [target: :string, out: :string, cli: :string]
      )

    target = Keyword.get(opts, :target, "native")
    ext = if target == "riscv64", do: ".riscv", else: ".so"

    default_out =
      Path.rootname(xml_path)
      |> Path.basename()
      |> then(&Path.join(Path.dirname(xml_path), &1 <> ext))

    out = Keyword.get(opts, :out, default_out)

    cli =
      Keyword.get(opts, :cli) ||
        System.get_env("OPENPLC_CLI") ||
        System.find_executable("openplc-cli") ||
        Mix.raise("""
        openplc-cli not found. Install OpenPLC Editor v4 from
        https://github.com/Autonomy-Logic/openplc-editor and put its
        CLI on PATH, or pass --cli /path/to/openplc-cli.
        """)

    File.mkdir_p!(Path.dirname(out))

    args = ["compile", xml_path, "--target", target, "--output", out]
    Mix.shell().info("$ #{cli} #{Enum.join(args, " ")}")

    {output, code} = System.cmd(cli, args, stderr_to_stdout: true)
    Mix.shell().info(output)

    if code != 0 do
      Mix.raise("openplc-cli exited #{code}. See RFD 2145 for the compile boundary.")
    end

    Mix.shell().info("OK -> #{out}")
  end
end
