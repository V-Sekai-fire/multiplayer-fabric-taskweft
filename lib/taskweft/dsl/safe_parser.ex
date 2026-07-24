# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.DSL.SafeParser do
  @moduledoc """
  Safe AST parser for Elixir module attributes used in Taskweft DSL.

  Parses module attributes (@name, @variables, @actions, etc.) from an Elixir module
  AST and converts them to RECTGTN JSON-LD format.
  """

  @type parse_result :: {:ok, String.t()} | {:error, String.t()}

  # Parse an Elixir module AST and extract domain attributes into RECTGTN JSON-LD.
  #
  # Returns {:ok, json_string} or {:error, reason} on failure.
  @spec parse(Macro.t()) :: parse_result()
  def parse({:defmodule, _, [_, [do: block]]}) do
    with {:ok, domain_map} <- extract_domain_attributes(block) do
      {:ok, Jason.encode!(finalize(domain_map))}
    end
  end

  defp extract_domain_attributes({:__block__, _, attributes}) do
    domain = %{
      "@context" => %{
        "vsekai" => "https://v-sekai.org/",
        "domain" => "vsekai:planning/domain/"
      },
      "@type" => "domain:Definition",
      "name" => nil,
      "variables" => [],
      "actions" => %{},
      "methods" => %{},
      "todo_list" => []
    }

    domain =
      Enum.reduce(attributes, domain, fn attr, acc ->
        unwrapped = unwrap_at(attr)
        {name, _, args} = unwrapped
        evaluated = Enum.map(args, &ast_to_literal/1)
        handle_attribute({name, [], evaluated}, acc)
      end)

    {:ok, domain}
  end

  # Unwrap the @ operator: @name "foo" becomes {:name, _, ["foo"]}
  defp unwrap_at({:@, _, [{name, _, args}]}), do: {name, [], args}
  defp unwrap_at(other), do: other

  # Convert AST literal node to its runtime value — pure pattern matching, no
  # Code.eval_quoted or function calls.  Handles maps, lists, atoms, strings,
  # numbers, and module aliases (the subset the DSL uses).
  defp ast_to_literal({:%{}, _, pairs}) when is_list(pairs) do
    Map.new(pairs, fn {k, v} -> {ast_to_literal(k), ast_to_literal(v)} end)
  end

  defp ast_to_literal(list) when is_list(list), do: Enum.map(list, &ast_to_literal/1)

  defp ast_to_literal({key, value}) when is_atom(key),
    do: {ast_to_literal(key), ast_to_literal(value)}

  defp ast_to_literal({key, value}), do: {ast_to_literal(key), ast_to_literal(value)}

  defp ast_to_literal(atom) when is_atom(atom), do: atom
  defp ast_to_literal(bin) when is_binary(bin), do: bin
  defp ast_to_literal(num) when is_number(num), do: num
  defp ast_to_literal({:__aliases__, _, [name]}), do: name
  defp ast_to_literal(other), do: other

  defp handle_attribute({:name, _, [value]}, domain) when is_binary(value) do
    Map.put(domain, "name", value)
  end

  defp handle_attribute({:variables, _, [value]}, domain) when is_map(value) do
    vars =
      value
      |> Map.to_list()
      |> Enum.map(fn {name, opts} ->
        %{
          "name" => to_string(name),
          "type" => to_string(opts[:type] || :ref),
          "init" =>
            opts[:init]
            |> Map.new(fn {k, v} -> {to_string(k), to_string(v)} end)
        }
      end)

    Map.put(domain, "variables", vars)
  end

  defp handle_attribute({:actions, _, [value]}, domain) when is_map(value) do
    acts =
      value
      |> Map.to_list()
      |> Enum.map(fn {name, act} ->
        {to_string(name), to_string_act(act)}
      end)
      |> Map.new()

    Map.put(domain, "actions", acts)
  end

  defp handle_attribute({:methods, _, [value]}, domain) when is_map(value) do
    meths =
      value
      |> Map.to_list()
      |> Enum.map(fn {name, meth} ->
        {to_string(name), to_string_method(meth)}
      end)
      |> Map.new()

    Map.put(domain, "methods", meths)
  end

  defp handle_attribute({:todo_list, _, [value]}, domain) when is_list(value) do
    Map.put(domain, "todo_list", to_string_todo_list(value))
  end

  defp handle_attribute(_attr, domain), do: domain

  defp to_string_act(act) when is_map(act) do
    params = (act[:params] || []) |> Enum.map(&to_string/1)
    body = (act[:body] || []) |> Enum.map(&to_string_body/1)

    action_def = %{"params" => params, "body" => body}

    action_def =
      if act[:duration] do
        Map.put(action_def, :duration, to_string(act[:duration]))
      else
        action_def
      end

    action_def
  end

  defp to_string_method(meth) when is_map(meth) do
    params = (meth[:params] || []) |> Enum.map(&to_string/1)
    alternatives = (meth[:alternatives] || []) |> Enum.map(&to_string_alternative/1)

    %{"params" => params, "alternatives" => alternatives}
  end

  defp to_string_alternative(alt) when is_map(alt) do
    %{
      "name" => to_string(alt[:name]),
      "subtasks" => alt[:subtasks] || [],
      "check" => (alt[:check] || []) |> Enum.map(&to_string_guard/1)
    }
  end

  defp to_string_body(%{pointer_set: path, value: value}) do
    %{"pointer/set" => to_string(path), "value" => to_string(value)}
  end

  defp to_string_guard({:condition, _, [type, args]}) do
    cond =
      case args do
        [expr] ->
          %{"type" => to_string(type), "a" => to_string_expr(expr)}

        [expr1, expr2] ->
          %{"type" => to_string(type), "a" => to_string_expr(expr1), "b" => to_string_expr(expr2)}
      end

    cond
  end

  defp to_string_todo_list(list) when is_list(list) do
    list
    |> Enum.map(fn item ->
      case item do
        [first | others] when is_binary(first) ->
          list_string = to_string(first)
          args_string = Enum.map_join(others, ",", &to_string/1)
          "[" <> list_string <> ", " <> args_string <> "]"

        %{goal: goals} when is_list(goals) ->
          %{"goal" => Enum.map(goals, &to_string_goal/1)}

        %{multigoal: mg} when is_map(mg) ->
          %{"multigoal" => mg}

        other ->
          other
      end
    end)
  end

  defp to_string_goal(%{"pointer" => pointer, "eq" => value}) do
    %{"pointer" => to_string(pointer), "eq" => to_string(value)}
  end

  defp to_string_expr(atom) when is_atom(atom), do: to_string(atom)
  defp to_string_expr(str) when is_binary(str), do: str
  defp to_string_expr(num) when is_integer(num), do: to_string(num)
  defp to_string_expr(num) when is_float(num), do: to_string(num)

  defp to_string_expr(%{pointer_get: path}),
    do: %{"type" => "pointer/get", "pointer" => to_string(path)}

  defp to_string_expr(_other), do: {:error, "invalid expression"}

  defp finalize(domain) do
    temp_domain = domain

    temp_domain
    |> Map.delete("capabilities")
    |> then(fn d ->
      if Map.has_key?(temp_domain, "capabilities") do
        Map.put(d, "capabilities", temp_domain["capabilities"])
      else
        d
      end
    end)
  end
end
