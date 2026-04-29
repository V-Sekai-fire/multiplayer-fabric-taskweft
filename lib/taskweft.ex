defmodule Taskweft do
  @moduledoc """
  Elixir bindings for the Taskweft HTN planner (C++20, Fine NIF).

  All JSON strings use the taskweft JSON-LD domain format.
  See the standalone/ C++ headers for the schema reference.
  """

  alias Taskweft.NIF
  alias Taskweft.ReBAC

  def plan(domain_json) do
    {:ok, NIF.plan(domain_json)}
  rescue
    e -> {:error, Exception.message(e)}
  end

  def replan(domain_json, plan_json, fail_step \\ -1) do
    {:ok, NIF.replan(domain_json, plan_json, fail_step)}
  rescue
    e -> {:error, Exception.message(e)}
  end

  def check_temporal(domain_json, plan_json, origin_iso \\ "PT0S") do
    {:ok, NIF.check_temporal(domain_json, plan_json, origin_iso)}
  rescue
    e -> {:error, Exception.message(e)}
  end

  def rebac_check(graph_json, subj, expr_json, obj, fuel \\ 8) do
    ReBAC.check(graph_json, subj, expr_json, obj, fuel)
  end

  def rebac_expand(graph_json, rel, obj, fuel \\ 8) do
    ReBAC.expand(graph_json, rel, obj, fuel)
  end

  def bridge_extract_entities(state_json) do
    NIF.bridge_extract_entities(state_json)
  end

  def bridge_plan_contents(plan_json, domain, entities_json) do
    NIF.bridge_plan_contents(plan_json, domain, entities_json)
  end
end
