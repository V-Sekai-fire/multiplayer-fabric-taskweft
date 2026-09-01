# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.Grafcet.StaticTest do
  @moduledoc """
  End-to-end test of the Lean-produced analyser through the C NIF:
  Elixir binary in, JSON binary out with `reachable` and
  `concurrent_pairs`. Landed by RFD 2144.
  """
  use ExUnit.Case, async: false

  alias Taskweft.Grafcet.Static

  @blocks_get_or ~s({
    "S": [
      ["^"],
      ["find", "", "V.found:=1", "1h"],
      ["|>", "pickup_from_table", "unstack"],
      ["pickup_from_table", "X.find", "V.picked:=1", "1h"],
      ["unstack", "X.find", "V.unstacked:=1", "1h"],
      ["|<"],
      ["mark_done", "", "V.done:=1", "1h"]
    ]
  })

  test "analyses the blocks_get_or fixture (RFD 2143 stage 1)" do
    {:ok, reply} = Static.analyse(@blocks_get_or)
    result = Jason.decode!(reply)

    reachable = MapSet.new(result["reachable"])
    for step <- ~w(find pickup_from_table unstack mark_done) do
      assert step in reachable, "expected #{step} to be reachable"
    end

    pairs = MapSet.new(Enum.map(result["concurrent_pairs"], &MapSet.new/1))
    assert MapSet.new(["pickup_from_table", "unstack"]) in pairs,
           "expected the two OR branches to be flagged as a concurrent pair"
  end

  test "empty S produces an empty reachable set and no concurrent pairs" do
    {:ok, reply} = Static.analyse(~s({"S": []}))
    result = Jason.decode!(reply)
    assert result["reachable"] == []
    assert result["concurrent_pairs"] == []
  end
end
