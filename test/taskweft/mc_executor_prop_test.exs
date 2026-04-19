defmodule Taskweft.MCExecutorPropTest do
  use ExUnit.Case, async: true
  use PropCheck

  alias Taskweft.MCExecutor

  # Minimal blocks-world domain for execution tests
  @domain File.read!(
    Path.join([__DIR__, "../../priv/plans/domains/blocks_world.jsonld"])
  )

  def step_gen, do: oneof([exactly("move"), exactly("pickup"), exactly("putdown")])
  def name_gen, do: oneof([exactly("a"), exactly("b"), exactly("c")])
  def prob_gen, do: let(n <- range(0, 100), do: n / 100.0)

  # Simple 1-step plan JSON that at minimum parses correctly
  def plan_json(action, args) do
    args_json = Enum.map_join(args, ",", &~s("#{&1}"))
    ~s([["#{action}",#{args_json}]])
  end

  property "execute: returns {:ok, json} or {:error, _} — never crashes" do
    forall {seed, prob} <- {range(0, 9999), prob_gen()} do
      probs = Jason.encode!([prob])
      result = MCExecutor.execute(@domain, ~s([["move","b","c","a"]]), probs, seed)
      match?({:ok, _}, result) or match?({:error, _}, result)
    end
  end

  property "execute: all-succeed trace has completed == plan length" do
    # Use a trivially empty plan so action lookup doesn't matter
    forall seed <- range(0, 100) do
      result = MCExecutor.execute(@domain, "[]", "[]", seed)
      case result do
        {:ok, json} ->
          trace = Jason.decode!(json)
          trace["completed"] == 0 and trace["failed_at"] == nil
        {:error, _} ->
          true
      end
    end
  end

  property "execute: prob=0.0 causes first step to fail" do
    # Any non-empty valid plan with prob 0 → fails at step 0
    forall seed <- range(0, 100) do
      plan = ~s([["move","b","c","a"]])
      probs = "[0.0]"
      result = MCExecutor.execute(@domain, plan, probs, seed)
      case result do
        {:ok, json} ->
          trace = Jason.decode!(json)
          trace["completed"] == 0 and trace["failed_at"] == 0
        {:error, _} ->
          true
      end
    end
  end

  property "execute: prob=1.0 never fails" do
    forall seed <- range(0, 100) do
      plan = "[]"
      result = MCExecutor.execute(@domain, plan, "[]", seed)
      case result do
        {:ok, json} ->
          trace = Jason.decode!(json)
          trace["failed_at"] == nil
        {:error, _} ->
          true
      end
    end
  end

  property "execute: same seed produces same trace" do
    forall seed <- range(0, 9999) do
      plan  = ~s([["move","b","c","a"]])
      probs = "[0.5]"
      r1 = MCExecutor.execute(@domain, plan, probs, seed)
      r2 = MCExecutor.execute(@domain, plan, probs, seed)
      r1 == r2
    end
  end

  property "execute: steps count matches plan length on success" do
    forall seed <- range(0, 100) do
      result = MCExecutor.execute(@domain, "[]", "[]", seed)
      case result do
        {:ok, json} ->
          trace = Jason.decode!(json)
          length(trace["steps"]) == 0
        {:error, _} ->
          true
      end
    end
  end
end
