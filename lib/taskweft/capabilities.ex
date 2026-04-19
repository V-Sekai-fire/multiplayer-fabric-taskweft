defmodule Taskweft.Capabilities do
  @moduledoc """
  Extended ReBAC engine: DFS capability traversal and EntityCapabilities API.

  Port of `plan/ipyhop/capabilities.py` `ReBACEngine` and `EntityCapabilities`.
  Adds DFS `can/4` (returns authorized + path) on top of the stateless
  expression algebra already in `Taskweft.ReBAC`.
  """

  alias Taskweft.NIF

  @doc """
  DFS traversal: can `subj` reach `capability` via any relationship path?

  Terminal edges are HAS_CAPABILITY, CONTROLS, OWNS. Intermediate hops
  follow any edge type. Returns `{:ok, %{"authorized" => bool, "path" => [string]}}`.
  """
  def can(graph_json, subj, capability, max_depth \\ 10) do
    json = NIF.rebac_can(graph_json, subj, capability, max_depth)
    Jason.decode(json)
  end

  @doc """
  All capability objects reachable from `entity` via HAS_CAPABILITY edges.
  Returns `{:ok, [capability_string]}`.
  """
  def get_entity_capabilities(graph_json, entity) do
    {:ok, NIF.rebac_get_entity_capabilities(graph_json, entity)}
  end

  @doc """
  All entities that hold HAS_CAPABILITY to `capability`.
  Returns `{:ok, [entity_string]}`.
  """
  def get_entities_with_capability(graph_json, capability) do
    {:ok, NIF.rebac_get_entities_with_capability(graph_json, capability)}
  end

  @doc """
  True if entity has `capability` (direct or via group membership).
  """
  def has_capability?(graph_json, entity, capability, max_depth \\ 10) do
    case can(graph_json, entity, capability, max_depth) do
      {:ok, %{"authorized" => auth}} -> auth
      _ -> false
    end
  end

  @doc """
  True if entity has any of the listed capabilities.
  """
  def has_any_capability?(graph_json, entity, capabilities, max_depth \\ 10) do
    Enum.any?(capabilities, &has_capability?(graph_json, entity, &1, max_depth))
  end

  @doc """
  True if entity has all of the listed capabilities.
  """
  def has_all_capabilities?(graph_json, entity, capabilities, max_depth \\ 10) do
    Enum.all?(capabilities, &has_capability?(graph_json, entity, &1, max_depth))
  end
end
