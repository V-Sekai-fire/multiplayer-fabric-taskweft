# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.ToolContext do
  @moduledoc """
  Builds a prompt context string listing available ExMCP tools.
  Injected into Reflect and Optimizer prompts so the LLM knows what actions the agent can take.
  """

  alias ExMCP.Server.Tools.Registry
  alias Taskweft.GEPA.ToolRegistry

  @spec build() :: String.t()
  def build do
    case Process.whereis(ToolRegistry) do
      nil ->
        ""
      _ ->
        Registry.list_tools(ToolRegistry)
        |> Enum.map_join("\n", fn t ->
          "- #{t.name}: #{Map.get(t, :description, "")}"
        end)
    end
  end
end
