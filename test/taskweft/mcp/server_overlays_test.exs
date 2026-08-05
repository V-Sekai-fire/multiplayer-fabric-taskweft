# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.MCP.ServerOverlaysTest do
  use ExUnit.Case, async: true

  # ExMCP's Builder maps an unknown param type to %{type: "string"} with
  # no warning. `{:list, :string}` is such a type, and it looked right
  # in the source while it published a string schema. Only `{:array, _}`
  # gives an array, thus this test asserts the published shape.
  defp tool(name) do
    {:ok, tools, _cursor, _state} = Taskweft.MCP.Server.handle_list_tools(%{}, %{})
    Enum.find(tools, fn t -> (t[:name] || t["name"]) == name end)
  end

  defp schema_of(tool_name) do
    tool = tool(tool_name)
    tool[:input_schema] || tool[:inputSchema]
  end

  for tool_name <- ["plan", "validate"] do
    test "#{tool_name} publishes overlays as an array of strings" do
      schema = schema_of(unquote(tool_name))
      props = schema[:properties] || schema["properties"]
      overlays = props[:overlays] || props["overlays"]

      assert overlays[:type] == "array"
      assert overlays[:items] == %{type: "string"}
    end

    test "#{tool_name} requires domain_dsl and not overlays" do
      schema = schema_of(unquote(tool_name))
      required = schema[:required] || schema["required"] || []

      assert "domain_dsl" in required
      refute "overlays" in required
    end
  end
end
