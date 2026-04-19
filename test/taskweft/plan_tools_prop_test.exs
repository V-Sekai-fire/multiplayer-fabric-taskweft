defmodule Taskweft.PlanToolsPropTest do
  use ExUnit.Case, async: true
  use PropCheck

  alias Taskweft.PlanTools

  @domain File.read!(
    Path.join([__DIR__, "../../priv/plans/domains/blocks_world.jsonld"])
  )

  def mode_gen, do: oneof([exactly("plan"), exactly("simulate"), exactly("replan")])
  def name_gen, do: let(chars <- non_empty(list(range(?a, ?z))), do: List.to_string(chars))

  # --- handle/1 ---

  property "handle: unknown mode returns error" do
    forall mode <- name_gen() do
      params = %{"mode" => "unknown_#{mode}", "domain" => @domain}
      match?({:error, _}, PlanTools.handle(params))
    end
  end

  property "handle: missing domain returns error" do
    forall mode <- mode_gen() do
      params = %{"mode" => mode}
      match?({:error, _}, PlanTools.handle(params))
    end
  end

  property "handle: plan mode returns ok or error (never crashes)" do
    forall _i <- range(0, 4) do
      params = %{"mode" => "plan", "domain" => @domain}
      result = PlanTools.handle(params)
      match?({:ok, _}, result) or match?({:error, _}, result)
    end
  end

  property "handle: simulate mode with empty plan returns ok or error" do
    forall _i <- range(0, 4) do
      params = %{"mode" => "simulate", "domain" => @domain, "plan" => "[]"}
      result = PlanTools.handle(params)
      match?({:ok, _}, result) or match?({:error, _}, result)
    end
  end

  property "handle: replan mode with empty plan returns ok or error" do
    forall _i <- range(0, 4) do
      params = %{"mode" => "replan", "domain" => @domain, "plan" => "[]"}
      result = PlanTools.handle(params)
      match?({:ok, _}, result) or match?({:error, _}, result)
    end
  end

  # --- serialize_state/1 ---

  property "serialize_state: returns ok with JSON string" do
    forall {k, v} <- {name_gen(), name_gen()} do
      {:ok, json} = PlanTools.serialize_state(%{k => v})
      is_binary(json) and json =~ k
    end
  end

  property "serialize_state: private keys are excluded" do
    forall {k, v} <- {name_gen(), name_gen()} do
      state = %{"_#{k}" => "private", k => v}
      {:ok, json} = PlanTools.serialize_state(state)
      not (json =~ "_#{k}") and json =~ k
    end
  end

  property "serialize_state: empty map returns empty JSON object" do
    {:ok, json} = PlanTools.serialize_state(%{})
    json == "{}"
  end

  # --- coerce_keys/1 ---

  property "coerce_keys: non-tuple keys pass through unchanged" do
    forall {k, v} <- {name_gen(), name_gen()} do
      {:ok, result} = PlanTools.coerce_keys(%{k => v})
      result[k] == v
    end
  end

  property "coerce_keys: empty map returns empty map" do
    {:ok, result} = PlanTools.coerce_keys(%{})
    result == %{}
  end
end
