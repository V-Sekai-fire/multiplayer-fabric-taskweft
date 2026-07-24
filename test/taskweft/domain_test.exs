# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.DomainTest do
  use ExUnit.Case, async: true

  @domains_path Path.expand("priv/plans/domains", File.cwd!())
  @problems_path Path.expand("priv/plans/problems", File.cwd!())
  @expected_path Path.expand("priv/plans/expected", File.cwd!())

  # Each tuple: {domain_name, problem_name}
  # Golden file: priv/plans/expected/{domain}__{problem}_expected.json
  @pairs [
    {"blocks_world", "blocks_world_1a"},
    {"blocks_world", "blocks_world_1b"},
    {"blocks_world", "blocks_world_2a"},
    {"blocks_world", "blocks_world_2b"},
    {"blocks_world", "blocks_world_3"},
    {"blocks_world", "blocks_world_goal"},
    {"blocks_world", "blocks_world_multigoal"},
    {"entity_capabilities", "entity_caps_amphibious"},
    {"entity_capabilities", "entity_caps_boat"},
    {"entity_capabilities", "entity_caps_drone"},
    {"entity_capabilities", "entity_caps_goal"},
    {"entity_capabilities", "entity_caps_human"},
    {"entity_capabilities", "entity_caps_multi"},
    {"healthcare", "healthcare_one"},
    {"healthcare", "healthcare_shared"},
    {"healthcare", "healthcare_two"},
    {"job_shop_scheduling", "job_shop_both"},
    {"job_shop_scheduling", "job_shop_one"},
    {"rescue", "rescue_move"},
    {"rescue", "rescue_survey"},
    {"robosub", "robosub_full_mission"},
    {"robosub", "robosub_partial"},
    {"simple_travel", "simple_travel_goal"},
    {"simple_travel", "simple_travel_one"},
    {"simple_travel", "simple_travel_two"},
    {"temporal_travel", "temporal_travel_goal"},
    {"temporal_travel", "temporal_travel_one"},
    {"temporal_travel", "temporal_travel_two"},
    {"trust_topology_audit", "trust_topology_audit_curvenet"},
    {"service_bringup", "chi176_local_infra_bringup"}
  ]

  # Standalone domains with no paired problems — tested from domain alone
  @standalone ["meta_loader"]

  for {domain_name, problem_name} <- @pairs do
    golden_path = Path.join(@expected_path, "#{domain_name}__#{problem_name}_expected.json")

    dsl_path = Path.join(@domains_path, "#{domain_name}_dsl.ex")
    jsonld_path = Path.join(@domains_path, "#{domain_name}.jsonld")

    domain_json =
      if File.exists?(dsl_path) do
        case File.read!(dsl_path) |> Taskweft.DSL.compile() do
          {:ok, json} -> json
          {:error, _reason} -> File.read!(jsonld_path)
        end
      else
        File.read!(jsonld_path)
      end

    p = File.read!(Path.join(@problems_path, "#{problem_name}.jsonld"))

    merged =
      Jason.decode!(domain_json) |> Map.merge(Jason.decode!(p)) |> Jason.encode!()

    golden = Jason.decode!(File.read!(golden_path))

    @tag :domain
    test "#{domain_name} + #{problem_name} matches plan and solution tree" do
      {:ok, result_json} = Taskweft.plan_explain(unquote(merged))
      result = Jason.decode!(result_json)

      plan = result["plan"] || []
      explain = result["explain"] || %{}
      tree = get_in(explain, ["solution_tree"]) || []

      expected = unquote(Macro.escape(golden))

      assert length(plan) == expected["steps"],
             "#{unquote(domain_name)}+#{unquote(problem_name)}: expected #{expected["steps"]} steps, got #{length(plan)}"

      assert length(tree) == expected["tree_nodes"],
             "#{unquote(domain_name)}+#{unquote(problem_name)}: expected #{expected["tree_nodes"]} tree nodes, got #{length(tree)}"

      assert plan == expected["plan"],
             "#{unquote(domain_name)}+#{unquote(problem_name)}: plan mismatch"

      assert tree == expected["tree"],
             "#{unquote(domain_name)}+#{unquote(problem_name)}: solution tree mismatch"
    end
  end

  for domain_name <- @standalone do
    golden_path = Path.join(@expected_path, "#{domain_name}_expected.json")
    golden = Jason.decode!(File.read!(golden_path))

    dsl_path = Path.join(@domains_path, "#{domain_name}_dsl.ex")
    jsonld_path = Path.join(@domains_path, "#{domain_name}.jsonld")

    json =
      if File.exists?(dsl_path) do
        case File.read!(dsl_path) |> Taskweft.DSL.compile() do
          {:ok, json} -> json
          {:error, _reason} -> File.read!(jsonld_path)
        end
      else
        File.read!(jsonld_path)
      end

    @tag :domain
    test "#{domain_name} (standalone) matches plan and solution tree" do
      {:ok, result_json} = Taskweft.plan_explain(unquote(json))
      result = Jason.decode!(result_json)

      plan = result["plan"] || []
      explain = result["explain"] || %{}
      tree = get_in(explain, ["solution_tree"]) || []

      expected = unquote(Macro.escape(golden))

      assert length(plan) == expected["steps"],
             "#{unquote(domain_name)}: expected #{expected["steps"]} steps, got #{length(plan)}"

      assert length(tree) == expected["tree_nodes"],
             "#{unquote(domain_name)}: expected #{expected["tree_nodes"]} tree nodes, got #{length(tree)}"

      assert plan == expected["plan"],
             "#{unquote(domain_name)}: plan mismatch"

      assert tree == expected["tree"],
             "#{unquote(domain_name)}: solution tree mismatch"
    end
  end
end
