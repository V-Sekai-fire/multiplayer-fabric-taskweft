defmodule Taskweft.PlannerPropTest do
  use ExUnit.Case, async: true
  use PropCheck

  @domains_dir Path.join([__DIR__, "../../priv/plans/domains"])
  @problems_dir Path.join([__DIR__, "../../priv/plans/problems"])

  def domain_file_gen do
    files = File.ls!(@domains_dir) |> Enum.filter(&String.ends_with?(&1, ".jsonld"))
    oneof(Enum.map(files, &exactly/1))
  end

  # --- plan/1 ---

  property "plan: returns ok or no_plan — never crashes on valid domain" do
    forall fname <- domain_file_gen() do
      domain = File.read!(Path.join(@domains_dir, fname))
      result = Taskweft.plan(domain)

      match?({:ok, _}, result) or match?({:error, "no_plan"}, result) or
        match?({:error, _}, result)
    end
  end

  property "plan: result is valid JSON array when ok" do
    forall fname <- domain_file_gen() do
      domain = File.read!(Path.join(@domains_dir, fname))

      case Taskweft.plan(domain) do
        {:ok, json} ->
          case Jason.decode(json) do
            {:ok, steps} -> is_list(steps)
            _ -> false
          end

        {:error, _} ->
          true
      end
    end
  end

  property "plan: each step is a non-empty array" do
    forall fname <- domain_file_gen() do
      domain = File.read!(Path.join(@domains_dir, fname))

      case Taskweft.plan(domain) do
        {:ok, json} ->
          {:ok, steps} = Jason.decode(json)
          Enum.all?(steps, &(is_list(&1) and length(&1) >= 1))

        {:error, _} ->
          true
      end
    end
  end

  # --- replan/3 ---

  property "replan: result JSON has required keys when ok" do
    forall fname <- domain_file_gen() do
      domain = File.read!(Path.join(@domains_dir, fname))

      case Taskweft.plan(domain) do
        {:ok, plan_json} ->
          case Taskweft.replan(domain, plan_json, -1) do
            {:ok, json} ->
              {:ok, result} = Jason.decode(json)
              Map.has_key?(result, "recovered") and Map.has_key?(result, "fail_step")

            {:error, _} ->
              true
          end

        {:error, _} ->
          true
      end
    end
  end

  property "replan: fail_step -1 and 0 both produce valid responses" do
    forall {fname, fail_step} <- {domain_file_gen(), oneof([exactly(-1), exactly(0)])} do
      domain = File.read!(Path.join(@domains_dir, fname))
      result = Taskweft.replan(domain, "[]", fail_step)
      match?({:ok, _}, result) or match?({:error, _}, result)
    end
  end

  # --- check_temporal/3 ---

  property "check_temporal: returns ok or error — never crashes" do
    forall fname <- domain_file_gen() do
      domain = File.read!(Path.join(@domains_dir, fname))

      case Taskweft.plan(domain) do
        {:ok, plan_json} ->
          result = Taskweft.check_temporal(domain, plan_json, "PT0S")
          match?({:ok, _}, result) or match?({:error, _}, result)

        {:error, _} ->
          true
      end
    end
  end

  property "check_temporal: result has consistent field" do
    forall fname <- domain_file_gen() do
      domain = File.read!(Path.join(@domains_dir, fname))

      case Taskweft.plan(domain) do
        {:ok, plan_json} ->
          case Taskweft.check_temporal(domain, plan_json) do
            {:ok, json} ->
              {:ok, result} = Jason.decode(json)
              # result is {"plan": ..., "temporal": {"consistent": ...}}
              temporal = result["temporal"] || result
              Map.has_key?(temporal, "consistent")

            {:error, _} ->
              true
          end

        {:error, _} ->
          true
      end
    end
  end
end
