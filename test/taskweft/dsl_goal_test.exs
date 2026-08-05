# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.DSLGoalTest do
  use ExUnit.Case, async: true

  alias Taskweft.DSL

  defp goal_of(source) do
    {:ok, json} = DSL.compile(source)
    {:ok, doc} = Jason.decode(json)
    [%{"goal" => [binding]}] = doc["todo_list"]
    binding
  end

  # A goal's `eq` went through to_string/1, thus `true` became the
  # string "true". The state holds a real boolean, "true" never equals
  # it, and the goal silently never matched. The planner then answered
  # no_plan, which names nothing.
  describe "a goal keeps the type of its value" do
    test "a boolean stays a boolean" do
      source = """
      defmodule G do
        use Taskweft.DSL
        @name "g"
        @variables %{have: %{type: :bool, init: %{done: false}}}
        @actions %{a_do: %{params: [], body: [%{pointer_set: "/have/done", value: true}]}}
        @methods %{have: %{params: [:k, :v], alternatives: [%{name: :go, subtasks: [["a_do"]]}]}}
        @todo_list [%{goal: [%{pointer: "/have/done", eq: true}]}]
      end
      """

      assert %{"eq" => true} = goal_of(source)
    end

    test "false is false, and not the string" do
      source = """
      defmodule G do
        use Taskweft.DSL
        @name "g"
        @variables %{have: %{type: :bool, init: %{done: true}}}
        @actions %{a_do: %{params: [], body: [%{pointer_set: "/have/done", value: false}]}}
        @methods %{have: %{params: [:k, :v], alternatives: [%{name: :go, subtasks: [["a_do"]]}]}}
        @todo_list [%{goal: [%{pointer: "/have/done", eq: false}]}]
      end
      """

      assert %{"eq" => false} = goal_of(source)
    end

    test "a number stays a number" do
      source = """
      defmodule G do
        use Taskweft.DSL
        @name "g"
        @variables %{count: %{type: :int, init: %{n: 0}}}
        @actions %{a_do: %{params: [], body: [%{pointer_set: "/count/n", value: 3}]}}
        @methods %{count: %{params: [:k, :v], alternatives: [%{name: :go, subtasks: [["a_do"]]}]}}
        @todo_list [%{goal: [%{pointer: "/count/n", eq: 3}]}]
      end
      """

      assert %{"eq" => 3} = goal_of(source)
    end

    test "a string stays a string" do
      source = """
      defmodule G do
        use Taskweft.DSL
        @name "g"
        @variables %{state: %{type: :ref, init: %{phase: "start"}}}
        @actions %{a_do: %{params: [], body: [%{pointer_set: "/state/phase", value: "end"}]}}
        @methods %{state: %{params: [:k, :v], alternatives: [%{name: :go, subtasks: [["a_do"]]}]}}
        @todo_list [%{goal: [%{pointer: "/state/phase", eq: "end"}]}]
      end
      """

      assert %{"eq" => "end"} = goal_of(source)
    end
  end

  describe "the goal plans" do
    test "a boolean goal reaches its action" do
      source = """
      defmodule G do
        use Taskweft.DSL
        @name "g"
        @variables %{have: %{type: :bool, init: %{done: false}}}
        @actions %{a_do: %{params: [], body: [%{pointer_set: "/have/done", value: true}]}}
        @methods %{have: %{params: [:k, :v], alternatives: [%{name: :go, subtasks: [["a_do"]]}]}}
        @todo_list [%{goal: [%{pointer: "/have/done", eq: true}]}]
      end
      """

      {:ok, json} = DSL.compile(source)
      assert {:ok, out} = Taskweft.plan(json)
      assert %{"plan" => [["a_do"]]} = Jason.decode!(out)
    end
  end
end
