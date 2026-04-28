# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.OptimizerTest do
  use ExUnit.Case, async: true

  @tag :red
  test "evolve/2 returns a non-empty instruction list given instructions and a score" do
    instructions = ["fight chickens near bank", "rest when hp low"]
    assert {:ok, evolved} = Taskweft.GEPA.Optimizer.evolve(instructions, -1.0)
    assert is_list(evolved) and length(evolved) > 0
  end
end
