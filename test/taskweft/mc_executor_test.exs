# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.MCExecutorTest do
  use ExUnit.Case, async: true

  @tag :red
  test "execute/4 returns ok with trace JSON for a valid domain and plan" do
    domain = File.read!(Path.join([__DIR__, "../../priv/plans/domains/blocks_world.jsonld"]))
    {:ok, plan_json} = Taskweft.plan(domain)
    assert {:ok, trace_json} = Taskweft.MCExecutor.execute(domain, plan_json, "[]", 0)
    trace = Jason.decode!(trace_json)
    assert is_integer(trace["completed"])
  end
end
