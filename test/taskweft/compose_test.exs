# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.ComposeTest do
  use ExUnit.Case, async: true

  alias Taskweft.Compose

  defp domain(name), do: domain(name, %{})

  defp domain(name, extra) do
    Map.merge(
      %{"@type" => "domain:Definition", "name" => name},
      extra
    )
  end

  describe "compose/1" do
    test "one document composes to itself" do
      doc = domain("solo", %{"actions" => %{"a" => %{"params" => []}}})
      assert {:ok, ^doc} = Compose.compose([doc])
    end

    test "an empty list has nothing to compose" do
      assert {:error, _reason} = Compose.compose([])
    end

    test "actions from every document survive" do
      left = domain("left", %{"actions" => %{"a_one" => %{"params" => []}}})
      right = domain("right", %{"actions" => %{"a_two" => %{"params" => []}}})

      assert {:ok, merged} = Compose.compose([left, right])
      assert Map.keys(merged["actions"]) |> Enum.sort() == ["a_one", "a_two"]
    end

    test "the later document wins a name it shares" do
      left = domain("left", %{"actions" => %{"a" => %{"params" => ["old"]}}})
      right = domain("right", %{"actions" => %{"a" => %{"params" => ["new"]}}})

      assert {:ok, merged} = Compose.compose([left, right])
      assert merged["actions"]["a"]["params"] == ["new"]
    end

    test "variables merge by name, and the later one wins" do
      left =
        domain("left", %{
          "variables" => [
            %{"name" => "kept", "type" => "bool", "init" => %{"x" => false}},
            %{"name" => "shared", "type" => "bool", "init" => %{"y" => false}}
          ]
        })

      right =
        domain("right", %{
          "variables" => [%{"name" => "shared", "type" => "bool", "init" => %{"y" => true}}]
        })

      assert {:ok, merged} = Compose.compose([left, right])
      by_name = Map.new(merged["variables"], &{&1["name"], &1})

      assert map_size(by_name) == 2
      assert by_name["shared"]["init"] == %{"y" => true}
      assert by_name["kept"]["init"] == %{"x" => false}
    end

    test "a non-empty todo_list replaces the one before it" do
      left = domain("left", %{"todo_list" => [["a_one"]]})
      right = domain("right", %{"todo_list" => [["a_two"]]})

      assert {:ok, merged} = Compose.compose([left, right])
      assert merged["todo_list"] == [["a_two"]]
    end

    test "an empty todo_list keeps the one before it" do
      left = domain("left", %{"todo_list" => [["a_one"]]})
      right = domain("right", %{"todo_list" => []})

      assert {:ok, merged} = Compose.compose([left, right])
      assert merged["todo_list"] == [["a_one"]]
    end

    test "three documents fold left to right" do
      one = domain("one", %{"actions" => %{"a" => %{"params" => ["first"]}}})
      two = domain("two", %{"actions" => %{"b" => %{"params" => []}}})
      three = domain("three", %{"actions" => %{"a" => %{"params" => ["last"]}}})

      assert {:ok, merged} = Compose.compose([one, two, three])
      assert merged["actions"]["a"]["params"] == ["last"]
      assert Map.has_key?(merged["actions"], "b")
    end

    test "a document that is not a map is rejected" do
      assert {:error, _reason} = Compose.compose([domain("ok"), "not a map"])
    end
  end

  describe "compose_strings/2" do
    test "composes DSL sources" do
      one = """
      defmodule ComposeOne do
        use Taskweft.DSL
        @name "one"
        @actions %{a_one: %{params: [], body: []}}
      end
      """

      two = """
      defmodule ComposeTwo do
        use Taskweft.DSL
        @name "two"
        @actions %{a_two: %{params: [], body: []}}
        @todo_list [["a_one"], ["a_two"]]
      end
      """

      assert {:ok, json} = Compose.compose_strings([one, two], format: "dsl")
      assert {:ok, doc} = Jason.decode(json)
      assert Map.has_key?(doc["actions"], "a_one")
      assert Map.has_key?(doc["actions"], "a_two")
      assert doc["todo_list"] == [["a_one"], ["a_two"]]
    end
  end

  describe "compose_strings/2 with a base_format" do
    test "a JSON base takes DSL overlays" do
      base = ~s({"@type":"domain:Definition","name":"base","actions":{"a_base":{"params":[]}}})

      overlay = """
      defmodule ComposeOverlay do
        use Taskweft.DSL
        @name "overlay"
        @actions %{a_overlay: %{params: [], body: []}}
        @todo_list [["a_base"], ["a_overlay"]]
      end
      """

      assert {:ok, json} =
               Compose.compose_strings([base, overlay], format: "dsl", base_format: "json")

      assert {:ok, doc} = Jason.decode(json)
      assert Map.has_key?(doc["actions"], "a_base")
      assert Map.has_key?(doc["actions"], "a_overlay")
      assert doc["todo_list"] == [["a_base"], ["a_overlay"]]
    end

    test "an unknown format is rejected" do
      assert {:error, reason} = Compose.compose_strings(["{}"], format: "yaml")
      assert reason =~ "unknown format"
    end
  end
end
