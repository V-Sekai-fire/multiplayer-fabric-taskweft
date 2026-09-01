# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.OpenPLC.GDScriptTest do
  @moduledoc """
  RFD 2150 stage 1 (FBD -> GDScript reader half). Smokes that the
  emitter produces GDScript with the shape godot-sandbox's
  SafeGDScript compiler accepts, from the same compact GRAFCET input
  the FBD emitter reads.
  """
  use ExUnit.Case, async: true

  alias Taskweft.OpenPLC.GDScript

  @fixture Path.expand("../../fixtures/grafcet/weftspun-build.grafcet.jsonld",
                       __DIR__)

  test "weftspun-build emits a GDScript state machine" do
    g = @fixture |> File.read!() |> Jason.decode!()
    src = GDScript.emit(g)

    # Godot boilerplate
    assert src =~ "extends Node"
    assert src =~ "func _process(_delta: float) -> void:"

    # Every V becomes a done_ bool
    for v <- ~w(lake ingest embedder render slat phenotype oracle) do
      assert src =~ "var done_#{v}: bool = false", "missing done_#{v}"
    end

    # Every step gets an activity mirror
    for step <- ~w(__init lake ingest embedder render slat phenotype oracle) do
      assert src =~ "var step_#{step}_active: bool = false"
    end

    # First-scan latch fires the initial step exactly once
    assert src =~ "if _first_scan:"
    assert src =~ "step___init_active = true"
    assert src =~ "_first_scan = false"

    # Each step's action assigns done_<name> when active
    assert src =~ "if step_lake_active:"
    assert src =~ "done_lake = true"

    # No ST/LD/SFC XML tokens — this is plain GDScript body, not FBD XML
    refute src =~ "<step "
    refute src =~ "<transition"
    refute src =~ "<FBD>"
    refute src =~ "typeName=\"SR_L\""
  end
end
