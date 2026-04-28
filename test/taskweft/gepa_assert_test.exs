# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.AssertTest do
  use ExUnit.Case, async: true

  @tag :red
  test "assert_/2 returns :ok when predicate passes" do
    assert :ok = Taskweft.GEPA.Assert.assert_(:hp_positive, fn -> true end)
  end

  @tag :red
  test "assert_/2 returns {:backtrack, label} when predicate fails" do
    assert {:backtrack, :hp_positive} =
             Taskweft.GEPA.Assert.assert_(:hp_positive, fn -> false end)
  end
end
