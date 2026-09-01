# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.OpenPLC.PLCopenTest do
  @moduledoc """
  RFD 2145 stage-1 emitter smoke test. Body is FBD only. SFC, ST, LD
  are all blocklisted; the emitter must never produce those tags.
  Every consumer downstream (OpenPLC v4, glTF Interactivity, VRChat
  Udon, UE Blueprint, Resonite ProtoFlux, Godot Sandbox) speaks
  FBD-shape, so this is the one output form.
  """
  use ExUnit.Case, async: true

  alias Taskweft.OpenPLC.PLCopen

  @fixture Path.expand("../../fixtures/grafcet/weftspun-build.grafcet.jsonld",
                       __DIR__)

  test "weftspun-build compact GRAFCET emits an FBD-only POU" do
    g = @fixture |> File.read!() |> Jason.decode!()
    xml = PLCopen.emit(g)

    assert xml =~ ~s(<pou name="weftspun_hexagonal_buildout" pouType="program">)

    # SFC is blocklisted — no step-graph elements anywhere
    refute xml =~ "<SFC>",         "SFC body found; SFC is blocklisted per BLOCKLIST"
    refute xml =~ "<step ",        "step element found; SFC is blocklisted"
    refute xml =~ "<transition ",  "transition element found; SFC is blocklisted"
    refute xml =~ "initialStep=",  "initialStep attr found; SFC is blocklisted"

    # ST, LD, IL likewise stay out
    refute xml =~ "<ST>",   "ST body found; ST is blocklisted"
    refute xml =~ "<LD>",   "LD body found; LD is blocklisted"
    refute xml =~ "<IL>",   "IL body found; IL is deprecated"
    refute xml =~ "CDATA"
    refute xml =~ ":="

    # FBD body wraps the whole POU
    assert xml =~ "<body>\n    <FBD>", "FBD is the only body language"

    # Every done_* is declared
    for v <- ~w(lake ingest embedder render slat phenotype oracle) do
      assert xml =~ ~s(<variable name="done_#{v}">), "missing done_#{v}"
    end

    # Every step gets an SR_L flip-flop and a companion step_<n>_Q boolean
    for step <- ~w(lake ingest embedder render slat phenotype oracle) do
      assert xml =~ ~s(instanceName="step_#{step}"), "missing SR_L for step_#{step}"
      assert xml =~ ~s(<variable name="step_#{step}_Q">)
    end

    # Every action emits a MOVE block
    for step <- ~w(lake ingest embedder render slat phenotype oracle) do
      assert xml =~ ~s(instanceName="do_#{step}"), "missing MOVE for do_#{step}"
    end

    # Transition guards are AND blocks; time-delayed transitions bring TON
    assert xml =~ ~s(typeName="AND")
    assert xml =~ ~s(typeName="TON")
    assert xml =~ "T#1h"
    assert xml =~ "T#2h"
    assert xml =~ "T#3h"

    # First-scan gate for the initial step
    assert xml =~ ~s(instanceName="latch_first_scan")
    assert xml =~ ~s(<variable name="first_scan_done">)
  end

  test "emitted XML is well-formed under xmllint if available" do
    g = @fixture |> File.read!() |> Jason.decode!()
    xml = "<project>#{PLCopen.emit(g)}</project>"

    tmp = Path.join(System.tmp_dir!(), "taskweft_plcopen_fbd_test.xml")
    File.write!(tmp, xml)

    case System.find_executable("xmllint") do
      nil -> :ok
      xmllint ->
        {output, code} = System.cmd(xmllint, ["--noout", tmp], stderr_to_stdout: true)
        assert code == 0,
               "PLCopen FBD XML rejected by xmllint (exit #{code}):\n#{output}"
    end
  end

  test "OR-divergence raises with a pointer at RFD 2147 staging" do
    or_fixture = Path.expand("../../fixtures/grafcet/blocks_get_or.grafcet.jsonld",
                             __DIR__)
    g = or_fixture |> File.read!() |> Jason.decode!()

    assert_raise RuntimeError, ~r/staged; see RFD 214[37]/, fn ->
      PLCopen.emit(g)
    end
  end
end
