defmodule Taskweft.CapabilitiesPropTest do
  use ExUnit.Case, async: true
  use PropCheck

  alias Taskweft.Capabilities
  alias Taskweft.ReBAC

  def name_gen do
    let chars <- non_empty(list(range(?a, ?z))),
        do: List.to_string(chars)
  end

  # --- can/4 ---

  property "can: direct HAS_CAPABILITY edge is authorized" do
    forall {entity, cap} <- {name_gen(), name_gen()} do
      implies entity != cap do
        g = ReBAC.new_graph() |> ReBAC.add_edge(entity, cap, "HAS_CAPABILITY")
        {:ok, result} = Capabilities.can(g, entity, cap)
        result["authorized"] == true and length(result["path"]) >= 2
      end
    end
  end

  property "can: no edge returns unauthorized" do
    forall {entity, cap} <- {name_gen(), name_gen()} do
      implies entity != cap do
        g = ReBAC.new_graph()
        {:ok, result} = Capabilities.can(g, entity, cap)
        result["authorized"] == false and result["path"] == []
      end
    end
  end

  property "can: CONTROLS edge is terminal (authorized)" do
    forall {entity, cap} <- {name_gen(), name_gen()} do
      g = ReBAC.new_graph() |> ReBAC.add_edge(entity, cap, "CONTROLS")
      {:ok, result} = Capabilities.can(g, entity, cap)
      result["authorized"] == true
    end
  end

  property "can: OWNS edge is terminal (authorized)" do
    forall {entity, cap} <- {name_gen(), name_gen()} do
      g = ReBAC.new_graph() |> ReBAC.add_edge(entity, cap, "OWNS")
      {:ok, result} = Capabilities.can(g, entity, cap)
      result["authorized"] == true
    end
  end

  property "can: transitive via IS_MEMBER_OF + HAS_CAPABILITY" do
    forall {entity, group, cap} <- {name_gen(), name_gen(), name_gen()} do
      implies entity != group and entity != cap and group != cap do
        g =
          ReBAC.new_graph()
          |> ReBAC.add_edge(entity, group, "IS_MEMBER_OF")
          |> ReBAC.add_edge(group, cap, "HAS_CAPABILITY")
        {:ok, result} = Capabilities.can(g, entity, cap)
        result["authorized"] == true and length(result["path"]) >= 3
      end
    end
  end

  property "can: fuel=0 always returns unauthorized" do
    forall {entity, cap} <- {name_gen(), name_gen()} do
      g = ReBAC.new_graph() |> ReBAC.add_edge(entity, cap, "HAS_CAPABILITY")
      {:ok, result} = Capabilities.can(g, entity, cap, 0)
      result["authorized"] == false
    end
  end

  # --- get_entity_capabilities/2 ---

  property "get_entity_capabilities: returns assigned capabilities" do
    forall {entity, caps} <- {name_gen(), non_empty(list(name_gen()))} do
      g = Enum.reduce(caps, ReBAC.new_graph(), fn cap, acc ->
        ReBAC.add_edge(acc, entity, cap, "HAS_CAPABILITY")
      end)
      {:ok, result} = Capabilities.get_entity_capabilities(g, entity)
      Enum.all?(caps, &Enum.member?(result, &1))
    end
  end

  property "get_entity_capabilities: empty graph returns empty list" do
    forall entity <- name_gen() do
      {:ok, result} = Capabilities.get_entity_capabilities(ReBAC.new_graph(), entity)
      result == []
    end
  end

  property "get_entity_capabilities: non-HAS_CAPABILITY edges not included" do
    forall {entity, obj} <- {name_gen(), name_gen()} do
      g = ReBAC.new_graph() |> ReBAC.add_edge(entity, obj, "IS_MEMBER_OF")
      {:ok, result} = Capabilities.get_entity_capabilities(g, entity)
      not Enum.member?(result, obj)
    end
  end

  # --- get_entities_with_capability/2 ---

  property "get_entities_with_capability: returns all direct holders" do
    forall {entities, cap} <- {non_empty(list(name_gen())), name_gen()} do
      g = Enum.reduce(entities, ReBAC.new_graph(), fn ent, acc ->
        ReBAC.add_edge(acc, ent, cap, "HAS_CAPABILITY")
      end)
      {:ok, result} = Capabilities.get_entities_with_capability(g, cap)
      Enum.all?(entities, &Enum.member?(result, &1))
    end
  end

  property "get_entities_with_capability: empty when no edges" do
    forall cap <- name_gen() do
      {:ok, result} = Capabilities.get_entities_with_capability(ReBAC.new_graph(), cap)
      result == []
    end
  end

  # --- has_capability?/3 ---

  property "has_capability?: true after adding HAS_CAPABILITY edge" do
    forall {entity, cap} <- {name_gen(), name_gen()} do
      g = ReBAC.new_graph() |> ReBAC.add_edge(entity, cap, "HAS_CAPABILITY")
      Capabilities.has_capability?(g, entity, cap) == true
    end
  end

  property "has_capability?: false on empty graph" do
    forall {entity, cap} <- {name_gen(), name_gen()} do
      implies entity != cap do
        Capabilities.has_capability?(ReBAC.new_graph(), entity, cap) == false
      end
    end
  end

  # --- has_any_capability?/3 ---

  property "has_any_capability?: true if entity has at least one" do
    forall {entity, cap1, cap2} <- {name_gen(), name_gen(), name_gen()} do
      g = ReBAC.new_graph() |> ReBAC.add_edge(entity, cap1, "HAS_CAPABILITY")
      Capabilities.has_any_capability?(g, entity, [cap1, cap2]) == true
    end
  end

  # --- has_all_capabilities?/3 ---

  property "has_all_capabilities?: true only when entity has all" do
    forall {entity, cap1, cap2} <- {name_gen(), name_gen(), name_gen()} do
      g =
        ReBAC.new_graph()
        |> ReBAC.add_edge(entity, cap1, "HAS_CAPABILITY")
        |> ReBAC.add_edge(entity, cap2, "HAS_CAPABILITY")
      Capabilities.has_all_capabilities?(g, entity, [cap1, cap2]) == true
    end
  end

  property "has_all_capabilities?: false when missing one" do
    forall {entity, cap1, cap2} <- {name_gen(), name_gen(), name_gen()} do
      implies cap1 != cap2 and entity != cap2 do
        g = ReBAC.new_graph() |> ReBAC.add_edge(entity, cap1, "HAS_CAPABILITY")
        not Capabilities.has_all_capabilities?(g, entity, [cap1, cap2])
      end
    end
  end
end
