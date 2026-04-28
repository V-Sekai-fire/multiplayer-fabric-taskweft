# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.ToolContextTest do
  use ExUnit.Case, async: false

  @tag :red
  test "tool_context/0 returns a string listing registered tool names" do
    Taskweft.GEPA.ToolRegistry.register(:test_tool_ctx, fn _ -> {:ok, "ok"} end,
      description: "a test tool"
    )
    context = Taskweft.GEPA.ToolContext.build()
    assert is_binary(context)
    assert String.contains?(context, "test_tool_ctx")
  end
end
