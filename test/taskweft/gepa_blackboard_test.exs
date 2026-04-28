# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.BlackboardTest do
  use ExUnit.Case, async: false

  @tag :red
  test "put/2 stores a value and get/1 retrieves it exactly" do
    :ok = Taskweft.GEPA.Blackboard.put(:hp, 80)
    assert Taskweft.GEPA.Blackboard.get(:hp) == 80
  end

  @tag :red
  test "search/1 returns keys ranked by HRR similarity" do
    :ok = Taskweft.GEPA.Blackboard.put(:fight_target, "chickens")
    :ok = Taskweft.GEPA.Blackboard.put(:zone, "bank")
    results = Taskweft.GEPA.Blackboard.search("fight enemy target")
    assert is_list(results)
    assert length(results) >= 1
    [{top_key, top_sim} | _] = results
    assert is_atom(top_key) or is_binary(top_key)
    assert is_float(top_sim)
  end
end
