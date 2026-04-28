# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.ToolTest do
  use ExUnit.Case, async: true

  @tag :red
  test "call/2 invokes the registered tool and returns its result" do
    Taskweft.GEPA.ToolRegistry.register(:ping, fn _args -> {:ok, "pong"} end)
    assert {:ok, "pong"} = Taskweft.GEPA.ToolRegistry.call(:ping, %{})
  end

  @tag :red
  test "call/2 returns error for unknown tool" do
    assert {:error, :unknown_tool} = Taskweft.GEPA.ToolRegistry.call(:nonexistent, %{})
  end
end
