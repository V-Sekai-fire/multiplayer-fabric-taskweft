# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.ToolRegistry do
  @moduledoc """
  Tool-Calling Controller — thin wrapper over ExMCP.Server.Tools.Registry.

  Tools are registered with a name atom and a 1-arity handler. The underlying
  ExMCP registry handles dispatch, schema validation, and unknown-tool errors.
  """

  alias ExMCP.Server.Tools.Registry

  def start_link(opts \\ []) do
    Registry.start_link(Keyword.put_new(opts, :name, __MODULE__))
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  @spec register(atom(), (map() -> {:ok, term()} | {:error, term()}), keyword()) :: :ok
  def register(name, fun, opts \\ []) when is_atom(name) and is_function(fun, 1) do
    ensure_started()

    tool_def = %{
      name: to_string(name),
      description: Keyword.get(opts, :description, to_string(name)),
      inputSchema: Keyword.get(opts, :input_schema, %{type: "object", properties: %{}})
    }

    Registry.register_tool(__MODULE__, tool_def, fn args, _state -> fun.(args) end)
  end

  @spec call(atom(), map()) :: {:ok, term()} | {:error, term()}
  def call(name, args) do
    ensure_started()

    case Registry.call_tool(__MODULE__, to_string(name), args, nil) do
      {:ok, result, _state} -> {:ok, result}
      {:ok, result} -> {:ok, result}
      {:error, "Unknown tool: " <> _} -> {:error, :unknown_tool}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_started do
    case Process.whereis(__MODULE__) do
      nil -> start_link([])
      _ -> :ok
    end
  end
end
