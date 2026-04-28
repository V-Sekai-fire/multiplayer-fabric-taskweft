# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.OptimizerTest do
  use ExUnit.Case, async: false
  import Mox

  setup :verify_on_exit!

  @tag :red
  test "evolve/2 returns a non-empty instruction list given instructions and a score" do
    stub(Taskweft.GEPA.InstructorMock, :chat_completion, fn _params, _config ->
      {:ok, nil,
       %{"instructions" => ["fight chickens near bank", "rest when hp drops below 30%"]}}
    end)

    stub(Taskweft.GEPA.InstructorMock, :reask_messages, fn _raw, _params, _config -> [] end)

    instructions = ["fight chickens near bank", "rest when hp low"]
    assert {:ok, evolved} = Taskweft.GEPA.Optimizer.evolve(instructions, -1.0)
    assert is_list(evolved) and length(evolved) > 0
  end
end
