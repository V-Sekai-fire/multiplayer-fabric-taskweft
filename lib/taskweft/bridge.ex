defmodule Taskweft.Bridge do
  @moduledoc """
  Plan-memory bridge: converts planner state/results into memory fact content.

  Wraps the C++ `tw_bridge.hpp` via Fine NIF. Provides helpers to:

  - Format variable bindings as memory content strings.
  - Extract entity names from a PDDL-style state dict.
  - Convert a plan result to storable `{content, category, tags}` facts.
  - Convert all state bindings to `{content, category, tags}` facts.
  """

  alias Taskweft.NIF

  @doc """
  Format a variable binding triple as a content string: `"var arg val"`.
  """
  def binding_content(var, arg, val) do
    NIF.bridge_binding_content(var, arg, val)
  end

  @doc """
  Extract entity names (inner dict keys) from a PDDL-style state JSON.

  Skips private (`_`-prefixed), `__name__`, and `rigid` variables.
  Returns a list of entity name strings.
  """
  def extract_entities(state_json) do
    NIF.bridge_extract_entities(state_json)
  end

  @doc """
  Convert a plan result to a JSON array of `{content, category, tags}` facts.

  - `plan_json` — JSON array of `[action, arg...]` steps.
  - `domain` — domain name string used as the `tags` value.
  - `entities_json` — JSON array of entity name strings (from `extract_entities/1`).

  Returns a JSON string array (summary + up to 20 per-step facts).
  """
  def plan_contents(plan_json, domain, entities_json) do
    NIF.bridge_plan_contents(plan_json, domain, entities_json)
  end

  @doc """
  Convert all state (var, arg, val) triples to a JSON array of memory facts.

  - `state_json` — PDDL-style state dict.
  - `domain` — domain name string used as the `tags` value.
  - `category` — category string for each fact (e.g. `"state"`).

  Returns a JSON string array of `{content, category, tags}` objects.
  """
  def state_bindings(state_json, domain, category \\ "state") do
    NIF.bridge_state_bindings(state_json, domain, category)
  end
end
