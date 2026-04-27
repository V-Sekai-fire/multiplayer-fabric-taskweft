# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.ASITest do
  use ExUnit.Case, async: true

  @tag :red
  test "serialize/1 extracts failed_action and reason from a replan result map" do
    replan_result = %{
      "recovered" => false,
      "fail_step" => 1,
      "failed_action" => "a_fight",
      "new_plan" => []
    }

    assert {:ok, asi} = Taskweft.GEPA.ASI.serialize(replan_result)
    assert Map.has_key?(asi, "failed_action")
    assert Map.has_key?(asi, "reason")
  end
end
