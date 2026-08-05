# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.DSLDiagnosticsTest do
  use ExUnit.Case, async: true

  alias Taskweft.DSL

  describe "syntax errors carry a position" do
    test "names the line, the column, and the token" do
      source = """
      defmodule X do
        use Taskweft.DSL
        @name "x"
        def f, do: [{level, >= 3}]
      end
      """

      assert {:error, message} = DSL.compile(source)

      assert message =~ "line 4"
      assert message =~ "column"
      assert message =~ ">="
    end

    test "shows the offending source line" do
      source = """
      defmodule X do
        use Taskweft.DSL
        @name "x"
        def f, do: [{level, >= 3}]
      end
      """

      assert {:error, message} = DSL.compile(source)
      assert message =~ "def f, do:"
    end

    test "never ends with a dangling colon and no token" do
      source = "defmodule X do\n  @name \"x\"\n  def f, do: [{a, >= 1}]\nend\n"
      assert {:error, message} = DSL.compile(source)
      refute String.ends_with?(String.trim(message), "before:")
    end
  end

  describe "the wrong API is rejected, and not silently accepted" do
    test "use Taskweft.Action names the real attributes" do
      source = """
      defmodule Y do
        use Taskweft.DSL
        @name "y"
        @todo_list [[:receive_package]]

        defmodule ReceivePackage do
          use Taskweft.Action
          def preconditions, do: []
          def effects, do: []
        end
      end
      """

      assert {:error, message} = DSL.compile(source)
      assert message =~ "Taskweft.Action"
      assert message =~ "@actions"
    end

    test "use Taskweft.Constraint is rejected too" do
      source = """
      defmodule Y do
        use Taskweft.DSL
        @name "y"

        defmodule Rule do
          use Taskweft.Constraint
          def check, do: true
        end
      end
      """

      assert {:error, message} = DSL.compile(source)
      assert message =~ "Taskweft.Constraint"
    end
  end

  describe "a todo_list must name something that exists" do
    test "an unknown call is named, and not left to no_plan" do
      source = """
      defmodule Z do
        use Taskweft.DSL
        @name "z"
        @actions %{a_real: %{params: [], body: []}}
        @todo_list [["a_ghost"]]
      end
      """

      assert {:error, message} = DSL.compile(source)
      assert message =~ "a_ghost"
    end

    test "a known call still compiles" do
      source = """
      defmodule Z do
        use Taskweft.DSL
        @name "z"
        @actions %{a_real: %{params: [], body: []}}
        @todo_list [["a_real"]]
      end
      """

      assert {:ok, _json} = DSL.compile(source)
    end

    test "a goal todo_list entry needs no action name" do
      source = """
      defmodule Z do
        use Taskweft.DSL
        @name "z"
        @variables %{have: %{type: :bool, init: %{x: false}}}
        @actions %{a_real: %{params: [], body: []}}
        @todo_list [%{goal: [%{pointer: "/have/x", eq: true}]}]
      end
      """

      assert {:ok, _json} = DSL.compile(source)
    end
  end
end
