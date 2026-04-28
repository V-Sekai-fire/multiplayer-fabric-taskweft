# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.BootstrapTest do
  use ExUnit.Case, async: false

  @tag :red
  test "store/1 accepts a trace and get/0 returns it" do
    trace = %{"plan" => [["a_fight", "hero"]], "score" => 1.0}
    :ok = Taskweft.GEPA.Bootstrap.store(trace)
    traces = Taskweft.GEPA.Bootstrap.get()
    assert Enum.any?(traces, &(&1["score"] == 1.0))
  end

  @tag :red
  test "similar/1 returns traces ranked by HRR similarity to query" do
    :ok =
      Taskweft.GEPA.Bootstrap.store(%{
        "plan" => [["a_fight"]],
        "score" => 1.0,
        "context" => "fight chickens"
      })

    :ok =
      Taskweft.GEPA.Bootstrap.store(%{
        "plan" => [["a_gather"]],
        "score" => 0.5,
        "context" => "gather copper"
      })

    results = Taskweft.GEPA.Bootstrap.similar("fight monster combat", 1)
    assert length(results) == 1
  end
end
