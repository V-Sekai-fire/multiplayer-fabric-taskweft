# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.FullCycleTest do
  use ExUnit.Case, async: true

  @tag :red
  test "full GEPA cycle: ASI → reflect → evolve → bootstrap stores successful trace" do
    replan = %{"recovered" => true, "fail_step" => 1, "failed_action" => "a_gather", "new_plan" => [["a_rest"]]}
    instructions = ["gather copper", "rest when tired"]

    {:ok, asi} = Taskweft.GEPA.ASI.serialize(replan)
    score = if asi["recovered"], do: 1.0, else: -1.0
    {:ok, _critique} = Taskweft.GEPA.Reflect.reflect(score, asi)
    {:ok, evolved} = Taskweft.GEPA.Optimizer.evolve(instructions, score)

    :ok = Taskweft.GEPA.Bootstrap.store(%{"instructions" => evolved, "score" => score})
    traces = Taskweft.GEPA.Bootstrap.get()
    assert Enum.any?(traces, &(&1["score"] == 1.0))
  end
end
