defmodule Taskweft.BridgePropTest do
  use ExUnit.Case, async: true
  use PropCheck

  alias Taskweft.Bridge

  def name_gen do
    let chars <- non_empty(list(range(?a, ?z))),
        do: List.to_string(chars)
  end

  def val_gen do
    let chars <- non_empty(list(range(?a, ?z))),
        do: List.to_string(chars)
  end

  property "binding_content: format is 'var arg val'" do
    forall {var, arg, val} <- {name_gen(), name_gen(), val_gen()} do
      result = Bridge.binding_content(var, arg, val)
      result == "#{var} #{arg} #{val}"
    end
  end

  property "extract_entities: empty state returns empty list" do
    Bridge.extract_entities("{}") == []
  end

  property "extract_entities: private vars are excluded" do
    forall {priv, pub} <- {name_gen(), name_gen()} do
      state_json = ~s({"_#{priv}":{"#{pub}":"x"},"#{pub}":{"#{pub}":"y"}})
      entities = Bridge.extract_entities(state_json)
      Enum.member?(entities, pub) and not Enum.member?(entities, "_#{priv}")
    end
  end

  property "plan_contents: summary fact is first" do
    forall domain <- name_gen() do
      plan_json    = ~s([["move","a","b"],["pick","a"]])
      entities_json = ~s(["alice","bob"])
      result = Bridge.plan_contents(plan_json, domain, entities_json)
      is_binary(result) and result =~ "Plan for #{domain}"
    end
  end

  property "plan_contents: step facts include action names" do
    forall domain <- name_gen() do
      plan_json    = ~s([["move","a","b"]])
      entities_json = ~s([])
      result = Bridge.plan_contents(plan_json, domain, entities_json)
      result =~ "move"
    end
  end

  property "plan_contents: category is planning" do
    forall domain <- name_gen() do
      plan_json    = ~s([["pick","x"]])
      entities_json = ~s([])
      result = Bridge.plan_contents(plan_json, domain, entities_json)
      result =~ "planning"
    end
  end

  property "state_bindings: empty state returns empty array" do
    Bridge.state_bindings("{}", "d", "state") == "[]"
  end

  property "state_bindings: var/arg/val appear in content" do
    forall {var, arg} <- {name_gen(), name_gen()} do
      state_json = ~s({"#{var}":{"#{arg}":"trueval"}})
      result = Bridge.state_bindings(state_json, "dom", "state")
      result =~ var and result =~ arg
    end
  end

  property "state_bindings: rigid vars are excluded" do
    forall {pub, arg} <- {name_gen(), name_gen()} do
      state_json = ~s({"rigid":{"#{arg}":"x"},"#{pub}":{"#{arg}":"y"}})
      result = Bridge.state_bindings(state_json, "dom", "state")
      not (result =~ ~s("rigid")) and result =~ pub
    end
  end
end
