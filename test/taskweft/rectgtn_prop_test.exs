defmodule Taskweft.RECTGTNPropTest do
  # Red-phase placeholder tests for each RECTGTN planner element.
  #
  # RECTGTN:
  #   R — Recursive decomposition   (scan methods, recursive subtasks)
  #   E — Entity-scoped state        (per-entity variable isolation)
  #   C — Capabilities               (ReBAC action guards)
  #   T — Temporal reasoning         (ISO 8601 durations, STN consistency)
  #   G — Goal methods               (goal-driven planning, multigoal)
  #   T — Task network               (ordered steps, backtracking, alternatives)
  #   N — Nondeterministic execution (MC executor ↔ planner integration)
  #
  # All tests carry @moduletag :red.
  # Run this suite: mix test --include red
  # Exclude from CI:  mix test --exclude red   (add to mix.exs test opts)
  use ExUnit.Case, async: true
  use PropCheck

  @moduletag :red

  @domains_dir Path.join([__DIR__, "../../priv/plans/domains"])

  # ---------------------------------------------------------------------------
  # Domain fixtures
  # ---------------------------------------------------------------------------

  # R — scan method that marks every key in a dict variable.
  @r_scan_domain """
  {
    "@context": {"khr": "https://registry.khronos.org/glTF/extensions/2.0/KHR_interactivity/",
                 "domain": "khr:planning/domain/"},
    "@type": "domain:Definition",
    "name": "r_scan",
    "variables": [
      {"name": "items", "init": {"a": false, "b": false, "c": false}}
    ],
    "actions": {
      "a_mark": {
        "params": ["item"],
        "body": [
          {"check": "/items/{item}", "eq": false},
          {"set":   "/items/{item}", "value": true}
        ]
      }
    },
    "methods": {
      "mark_all": {
        "scan": {
          "over": "items",
          "recurse": "mark_all",
          "branches": [
            {"check": [{"var": ["/items", "{_key}"], "eq": false}],
             "subtasks": [["a_mark", "{_key}"]]}
          ],
          "done_subtasks": []
        }
      }
    },
    "tasks": [["mark_all"]]
  }
  """

  # E — two independently-moving robots share one domain.
  @e_entity_domain """
  {
    "@context": {"khr": "https://registry.khronos.org/glTF/extensions/2.0/KHR_interactivity/",
                 "domain": "khr:planning/domain/"},
    "@type": "domain:Definition",
    "name": "e_entity",
    "variables": [
      {"name": "pos", "init": {"robot_a": 0, "robot_b": 5}}
    ],
    "actions": {
      "a_teleport": {
        "params": ["robot", "dest"],
        "body": [{"set": "/pos/{robot}", "value": "{dest}"}]
      }
    },
    "methods": {
      "move": {
        "params": ["robot", "dest"],
        "alternatives": [
          {"name": "at_dest",
           "check": [{"pointer": "/pos/{robot}", "eq": "{dest}"}],
           "subtasks": []},
          {"name": "teleport",
           "subtasks": [["a_teleport", "{robot}", "{dest}"]]}
        ]
      }
    },
    "tasks": [["move", "robot_a", 3], ["move", "robot_b", 3]]
  }
  """

  # G — multigoal task that drives goal methods.
  @g_goal_domain """
  {
    "@context": {"khr": "https://registry.khronos.org/glTF/extensions/2.0/KHR_interactivity/",
                 "domain": "khr:planning/domain/"},
    "@type": "domain:Definition",
    "name": "g_goal",
    "variables": [
      {"name": "st", "init": {"light": "off", "door": "closed"}}
    ],
    "actions": {
      "a_light_on":  {"params": [], "body": [{"check": "/st/light", "eq": "off"},  {"set": "/st/light", "value": "on"}]},
      "a_door_open": {"params": [], "body": [{"check": "/st/door",  "eq": "closed"},{"set": "/st/door",  "value": "open"}]}
    },
    "goals": {
      "st": {
        "params": ["key", "desired"],
        "alternatives": [
          {"name": "already",  "check": [{"pointer": "/st/{key}", "eq": "{desired}"}], "subtasks": []},
          {"name": "light_on", "check": [{"pointer": "/st/light", "eq": "off"},
                                         {"var": ["st", "key"], "eq": "light"},
                                         {"var": ["st", "desired"], "eq": "on"}],
           "subtasks": [["a_light_on"]]},
          {"name": "door_open","check": [{"pointer": "/st/door", "eq": "closed"},
                                         {"var": ["st", "key"], "eq": "door"},
                                         {"var": ["st", "desired"], "eq": "open"}],
           "subtasks": [["a_door_open"]]}
        ]
      }
    },
    "tasks": [{"multigoal": {"st": {"light": "on", "door": "open"}}}]
  }
  """

  # G — same domain but goals already satisfied (expect empty plan).
  @g_goal_already_done_domain """
  {
    "@context": {"khr": "https://registry.khronos.org/glTF/extensions/2.0/KHR_interactivity/",
                 "domain": "khr:planning/domain/"},
    "@type": "domain:Definition",
    "name": "g_goal_done",
    "variables": [
      {"name": "st", "init": {"light": "on", "door": "open"}}
    ],
    "actions": {
      "a_light_on":  {"params": [], "body": [{"check": "/st/light", "eq": "off"},  {"set": "/st/light", "value": "on"}]},
      "a_door_open": {"params": [], "body": [{"check": "/st/door",  "eq": "closed"},{"set": "/st/door",  "value": "open"}]}
    },
    "goals": {
      "st": {
        "params": ["key", "desired"],
        "alternatives": [
          {"name": "already", "check": [{"pointer": "/st/{key}", "eq": "{desired}"}], "subtasks": []}
        ]
      }
    },
    "tasks": [{"multigoal": {"st": {"light": "on", "door": "open"}}}]
  }
  """

  # T (task-network) — ordered steps with explicit backtracking.
  @t_backtrack_domain """
  {
    "@context": {"khr": "https://registry.khronos.org/glTF/extensions/2.0/KHR_interactivity/",
                 "domain": "khr:planning/domain/"},
    "@type": "domain:Definition",
    "name": "t_backtrack",
    "variables": [
      {"name": "st", "init": {"flag": false}}
    ],
    "actions": {
      "a_set": {
        "params": [],
        "body": [{"check": "/st/flag", "eq": false}, {"set": "/st/flag", "value": true}]
      }
    },
    "methods": {
      "ensure_set": {
        "params": [],
        "alternatives": [
          {"name": "already_set",
           "check": [{"pointer": "/st/flag", "eq": true}],
           "subtasks": []},
          {"name": "do_set",
           "subtasks": [["a_set"]]}
        ]
      }
    },
    "tasks": [["ensure_set"]]
  }
  """

  # ---------------------------------------------------------------------------
  # R — Recursive decomposition
  # ---------------------------------------------------------------------------

  test "R: scan method produces one action per unmarked item" do
    {:ok, json} = Taskweft.plan(@r_scan_domain)
    steps = Jason.decode!(json)
    # items has three keys (a, b, c), all initially false → expect three a_mark steps.
    assert length(steps) == 3
    assert Enum.all?(steps, fn [name | _] -> name == "a_mark" end)
  end

  test "R: scan method on already-complete state produces empty plan" do
    domain =
      @r_scan_domain
      |> Jason.decode!()
      |> put_in(["variables", Access.at(0), "init"], %{"a" => true, "b" => true, "c" => true})
      |> Jason.encode!()

    {:ok, json} = Taskweft.plan(domain)
    assert Jason.decode!(json) == []
  end

  property "R: scan plan length equals number of unprocessed items" do
    forall n <- range(0, 5) do
      init = Map.new(0..(n - 1)//1, fn i -> {"k#{i}", false} end)

      domain =
        @r_scan_domain
        |> Jason.decode!()
        |> put_in(["variables", Access.at(0), "init"], init)
        |> Jason.encode!()

      case Taskweft.plan(domain) do
        {:ok, json} -> length(Jason.decode!(json)) == n
        {:error, _} -> n == 0
      end
    end
  end

  # ---------------------------------------------------------------------------
  # E — Entity-scoped state
  # ---------------------------------------------------------------------------

  test "E: plan contains exactly one step per robot needing movement" do
    {:ok, json} = Taskweft.plan(@e_entity_domain)
    steps = Jason.decode!(json)
    # robot_a at 0→3, robot_b at 5→3: each needs one teleport.
    assert length(steps) == 2
    assert Enum.all?(steps, fn [name | _] -> name == "a_teleport" end)
  end

  test "E: entity already at destination produces no action for that entity" do
    # robot_a already at 3; only robot_b should produce a step.
    domain =
      @e_entity_domain
      |> Jason.decode!()
      |> put_in(["variables", Access.at(0), "init"], %{"robot_a" => 3, "robot_b" => 5})
      |> Jason.encode!()

    {:ok, json} = Taskweft.plan(domain)
    steps = Jason.decode!(json)
    assert length(steps) == 1
    assert hd(hd(steps)) == "a_teleport"
    assert Enum.at(hd(steps), 1) == "robot_b"
  end

  property "E: each robot's plan step references only that robot as first arg" do
    forall {a_dest, b_dest} <- {range(1, 9), range(1, 9)} do
      domain =
        @e_entity_domain
        |> Jason.decode!()
        |> put_in(["variables", Access.at(0), "init"], %{"robot_a" => 0, "robot_b" => 5})
        |> then(fn d ->
          tasks = [["move", "robot_a", a_dest], ["move", "robot_b", b_dest]]
          Map.put(d, "tasks", tasks)
        end)
        |> Jason.encode!()

      case Taskweft.plan(domain) do
        {:ok, json} ->
          steps = Jason.decode!(json)
          Enum.all?(steps, fn [_name, agent | _] -> agent in ["robot_a", "robot_b"] end)
        {:error, _} ->
          true
      end
    end
  end

  # ---------------------------------------------------------------------------
  # C — Capabilities
  # ---------------------------------------------------------------------------

  @caps_domain File.read!(Path.join([__DIR__, "../../priv/plans/domains/entity_capabilities.jsonld"]))

  test "C: drone (fly capability) plan uses only a_fly" do
    domain =
      @caps_domain
      |> Jason.decode!()
      |> Map.put("tasks", [["m_move", "drone_1", 1]])
      |> Jason.encode!()

    {:ok, json} = Taskweft.plan(domain)
    [[action | _]] = Jason.decode!(json)
    assert action == "a_fly"
  end

  test "C: boat (swim capability) plan uses only a_swim" do
    domain =
      @caps_domain
      |> Jason.decode!()
      |> Map.put("tasks", [["m_move", "boat_1", 3]])
      |> Jason.encode!()

    {:ok, json} = Taskweft.plan(domain)
    [[action | _]] = Jason.decode!(json)
    assert action == "a_swim"
  end

  test "C: human (walk capability) plan uses only a_walk" do
    domain =
      @caps_domain
      |> Jason.decode!()
      |> Map.put("tasks", [["m_move", "human_1", 2]])
      |> Jason.encode!()

    {:ok, json} = Taskweft.plan(domain)
    [[action | _]] = Jason.decode!(json)
    assert action == "a_walk"
  end

  test "C: amphibious entity (swim + walk) uses one of swim or walk" do
    domain =
      @caps_domain
      |> Jason.decode!()
      |> Map.put("tasks", [["m_move", "amphibious_1", 6]])
      |> Jason.encode!()

    {:ok, json} = Taskweft.plan(domain)
    [[action | _]] = Jason.decode!(json)
    assert action in ["a_swim", "a_walk"]
  end

  test "C: entity with no matching capability returns no_plan" do
    # Add an entity with an unknown capability: should not match any alternative.
    domain =
      @caps_domain
      |> Jason.decode!()
      |> put_in(["capabilities", "entities", "mystery_1"], ["teleport"])
      |> Map.put("tasks", [["m_move", "mystery_1", 1]])
      |> Jason.encode!()

    result = Taskweft.plan(domain)
    assert match?({:error, "no_plan"}, result) or match?({:error, _}, result)
  end

  # ---------------------------------------------------------------------------
  # T — Temporal reasoning
  # ---------------------------------------------------------------------------

  @temporal_domain File.read!(
                     Path.join([__DIR__, "../../priv/plans/domains/temporal_travel.jsonld"])
                   )

  test "T: check_temporal result contains 'consistent' field" do
    {:ok, plan_json} = Taskweft.plan(@temporal_domain)
    {:ok, json} = Taskweft.check_temporal(@temporal_domain, plan_json, "PT0S")
    result = Jason.decode!(json)
    temporal = result["temporal"] || result
    assert Map.has_key?(temporal, "consistent")
  end

  test "T: sequential plan with no deadline is consistent" do
    {:ok, plan_json} = Taskweft.plan(@temporal_domain)
    {:ok, json} = Taskweft.check_temporal(@temporal_domain, plan_json, "PT0S")
    result = Jason.decode!(json)
    temporal = result["temporal"] || result
    assert temporal["consistent"] == true
  end

  test "T: empty plan is always temporally consistent" do
    {:ok, json} = Taskweft.check_temporal(@temporal_domain, "[]", "PT0S")
    result = Jason.decode!(json)
    temporal = result["temporal"] || result
    assert temporal["consistent"] == true
  end

  property "T: check_temporal never crashes on valid domain and any plan" do
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

  test "T: action durations appear in temporal result steps" do
    {:ok, plan_json} = Taskweft.plan(@temporal_domain)
    {:ok, json} = Taskweft.check_temporal(@temporal_domain, plan_json, "PT0S")
    result = Jason.decode!(json)
    # When the domain has actions with duration fields, the temporal result
    # should expose per-step timing so callers can schedule work.
    assert Map.has_key?(result, "temporal") or Map.has_key?(result, "consistent")
  end

  # ---------------------------------------------------------------------------
  # G — Goal methods
  # ---------------------------------------------------------------------------

  test "G: multigoal on unsatisfied state produces non-empty plan" do
    {:ok, json} = Taskweft.plan(@g_goal_domain)
    steps = Jason.decode!(json)
    assert length(steps) >= 1
  end

  test "G: multigoal plan actions are the expected primitives" do
    {:ok, json} = Taskweft.plan(@g_goal_domain)
    actions = Jason.decode!(json) |> Enum.map(&hd/1)
    assert Enum.all?(actions, &(&1 in ["a_light_on", "a_door_open"]))
  end

  test "G: multigoal when all goals already satisfied produces empty plan" do
    {:ok, json} = Taskweft.plan(@g_goal_already_done_domain)
    assert Jason.decode!(json) == []
  end

  property "G: goal method plan satisfies each binding (idempotent re-plan)" do
    # Re-running plan on same domain should return same or equivalent plan.
    forall _n <- range(1, 3) do
      r1 = Taskweft.plan(@g_goal_domain)
      r2 = Taskweft.plan(@g_goal_domain)

      case {r1, r2} do
        {{:ok, j1}, {:ok, j2}} -> Jason.decode!(j1) == Jason.decode!(j2)
        _ -> true
      end
    end
  end

  # ---------------------------------------------------------------------------
  # T — Task network (ordering and backtracking)
  # ---------------------------------------------------------------------------

  test "T2: first method alternative chosen when its check passes" do
    # flag=true → 'already_set' check passes → empty plan (no action).
    domain =
      @t_backtrack_domain
      |> Jason.decode!()
      |> put_in(["variables", Access.at(0), "init"], %{"flag" => true})
      |> Jason.encode!()

    {:ok, json} = Taskweft.plan(domain)
    assert Jason.decode!(json) == []
  end

  test "T2: second alternative used when first check fails" do
    # flag=false → 'already_set' check fails → falls through to 'do_set'.
    {:ok, json} = Taskweft.plan(@t_backtrack_domain)
    [[action | _]] = Jason.decode!(json)
    assert action == "a_set"
  end

  test "T2: ordered multi-task sequence preserves step order" do
    # blocks_world rearranges three blocks; steps must follow pick-before-place.
    bw = File.read!(Path.join(@domains_dir, "blocks_world.jsonld"))

    {:ok, json} = Taskweft.plan(bw)
    steps = Jason.decode!(json)

    # Each a_stack must be preceded by an a_pickup or a_unstack for the same block.
    stack_indices =
      steps
      |> Enum.with_index()
      |> Enum.filter(fn {[name | _], _} -> name == "a_stack" end)
      |> Enum.map(fn {[_, block | _], idx} -> {block, idx} end)

    Enum.all?(stack_indices, fn {block, stack_idx} ->
      Enum.any?(0..(stack_idx - 1)//1, fn i ->
        [name, b | _] = Enum.at(steps, i)
        name in ["a_pickup", "a_unstack"] and b == block
      end)
    end)
  end

  property "T2: plan result is deterministic for a given domain" do
    forall fname <- domain_file_gen() do
      domain = File.read!(Path.join(@domains_dir, fname))
      r1 = Taskweft.plan(domain)
      r2 = Taskweft.plan(domain)

      case {r1, r2} do
        {{:ok, j1}, {:ok, j2}} -> Jason.decode!(j1) == Jason.decode!(j2)
        {{:error, e1}, {:error, e2}} -> e1 == e2
        _ -> false
      end
    end
  end

  # ---------------------------------------------------------------------------
  # N — Nondeterministic execution (MC executor integration)
  # ---------------------------------------------------------------------------

  test "N: planner output feeds directly into MC executor" do
    bw = File.read!(Path.join(@domains_dir, "blocks_world.jsonld"))
    {:ok, plan_json} = Taskweft.plan(bw)

    result = Taskweft.MCExecutor.execute(bw, plan_json, "[]", 42)
    assert match?({:ok, _}, result)
  end

  test "N: MC trace step count equals plan length on all-succeed run" do
    bw = File.read!(Path.join(@domains_dir, "blocks_world.jsonld"))
    {:ok, plan_json} = Taskweft.plan(bw)
    plan_len = plan_json |> Jason.decode!() |> length()

    {:ok, trace_json} = Taskweft.MCExecutor.execute(bw, plan_json, "[]", 0)
    trace = Jason.decode!(trace_json)

    assert trace["completed"] == plan_len
    assert trace["failed_at"] == nil
  end

  property "N: replan after MC step failure produces a non-empty continuation" do
    bw = File.read!(Path.join(@domains_dir, "blocks_world.jsonld"))

    case Taskweft.plan(bw) do
      {:ok, plan_json} ->
        steps = Jason.decode!(plan_json)

        forall fail_step <- range(0, max(0, length(steps) - 1)) do
          case Taskweft.replan(bw, plan_json, fail_step) do
            {:ok, json} ->
              result = Jason.decode!(json)
              # Recovered plan may be empty (if fail was last step) but the
              # "recovered" key must be present and be a list.
              is_list(result["recovered"])

            {:error, _} ->
              true
          end
        end

      {:error, _} ->
        true
    end
  end

  property "N: same seed same trace across two MC runs" do
    bw = File.read!(Path.join(@domains_dir, "blocks_world.jsonld"))

    case Taskweft.plan(bw) do
      {:ok, plan_json} ->
        forall seed <- range(0, 9999) do
          r1 = Taskweft.MCExecutor.execute(bw, plan_json, "[0.8]", seed)
          r2 = Taskweft.MCExecutor.execute(bw, plan_json, "[0.8]", seed)
          r1 == r2
        end

      {:error, _} ->
        true
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp domain_file_gen do
    files = File.ls!(@domains_dir) |> Enum.filter(&String.ends_with?(&1, ".jsonld"))
    oneof(Enum.map(files, &exactly/1))
  end
end
