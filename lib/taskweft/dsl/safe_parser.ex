# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.DSL.SafeParser do
  @moduledoc """
  Safe AST parser for Elixir module attributes used in Taskweft DSL.

  Operates entirely on Elixir AST forms — never evaluates Code or
  converts AST maps to runtime maps.  Each attribute's value is
  pattern-matched as `{:%{}, _, pairs}` or `[...]` and converted
  directly to RECTGTN JSON-LD strings.
  """

  @type parse_result :: {:ok, String.t()} | {:error, String.t()}

  @spec parse(Macro.t()) :: parse_result()
  def parse({:defmodule, _, [_, [do: block]]}) do
    with {:ok, domain_map} <- extract_domain_attributes(block) do
      {:ok, Jason.encode!(finalize(domain_map))}
    end
  end

  defp extract_domain_attributes({:__block__, _, attrs}) do
    domain = %{
      "@context" => %{
        "khr" => "https://registry.khronos.org/glTF/extensions/2.0/KHR_interactivity/",
        "domain" => "khr:planning/domain/"
      },
      "@type" => "domain:Definition",
      "name" => nil,
      "variables" => [],
      "actions" => %{},
      "methods" => %{},
      "todo_list" => []
    }

    domain =
      Enum.reduce(attrs, domain, fn attr, acc ->
        case attr do
          {:@, _, [{name, _, args}]} -> handle_attribute({name, [], args}, acc)
          _ -> acc
        end
      end)

    {:ok, domain}
  end

  # ── handle_attribute: match each @attribute by name ────────────────
  defp handle_attribute({:name, _, [value]}, domain) when is_binary(value),
    do: Map.put(domain, "name", value)

  defp handle_attribute({:variables, _, [value]}, domain),
    do: Map.put(domain, "variables", vars_to_list(value))

  defp handle_attribute({:actions, _, [value]}, domain),
    do: Map.put(domain, "actions", ast_map_to_json(value, &action_to_json/1))

  defp handle_attribute({:methods, _, [value]}, domain),
    do: Map.put(domain, "methods", ast_map_to_json(value, &method_to_json/1))

  defp handle_attribute({:todo_list, _, [value]}, domain),
    do: Map.put(domain, "todo_list", todo_to_list(value))

  defp handle_attribute(_, domain), do: domain

  # ── AST map helpers ────────────────────────────────────────────────
  # Convert {:%{}, _, pairs} into a JSON map by applying a per-value
  # transform.  Key-pair AST nodes are {key, value} — atoms are fine.
  defp ast_map_to_json({:%{}, _, pairs}, value_fn) when is_list(pairs) do
    Map.new(pairs, fn {k, v} -> {to_string(k), value_fn.(v)} end)
  end

  defp ast_map_to_json(_, _value_fn), do: %{}

  # Extract a key from an AST map pair list by name.
  defp ast_map_get({:%{}, _, pairs}, key) when is_list(pairs) do
    case List.keyfind(pairs, key, 0) do
      {^key, value} -> value
      nil -> nil
    end
  end

  defp ast_map_get(_, _key), do: nil

  # ── Variables ──────────────────────────────────────────────────────
  defp vars_to_list({:%{}, _, pairs}) when is_list(pairs) do
    Enum.map(pairs, fn {name, opts_ast} ->
      %{
        "name" => to_string(name),
        "type" => to_string(ast_map_get(opts_ast, :type) || :ref),
        "init" => ast_map_get(opts_ast, :init) |> init_to_map()
      }
    end)
  end

  defp vars_to_list(_), do: []

  defp init_to_map({:%{}, _, pairs}) when is_list(pairs) do
    Map.new(pairs, fn {k, v} -> {to_string(k), ast_literal(v)} end)
  end

  defp init_to_map(_), do: %{}

  # ── Actions ────────────────────────────────────────────────────────
  defp action_to_json({:%{}, _, pairs}) when is_list(pairs) do
    params = pairs |> List.keyfind(:params, 0) |> elem(1) |> Enum.map(&to_string/1)
    body = pairs |> List.keyfind(:body, 0) |> elem(1) |> body_to_list()

    duration =
      case List.keyfind(pairs, :duration, 0) do
        {_, dur} -> to_string(dur)
        nil -> nil
      end

    bind =
      case List.keyfind(pairs, :bind, 0) do
        {_, b} -> bind_to_list(b)
        nil -> nil
      end

    %{"params" => params, "body" => body}
    |> then(fn m -> if duration, do: Map.put(m, "duration", duration), else: m end)
    |> then(fn m -> if bind != nil, do: Map.put(m, "bind", bind), else: m end)
  end

  defp action_to_json(_), do: %{"params" => [], "body" => []}

  defp bind_to_list(list) when is_list(list) do
    Enum.map(list, fn {:%{}, _, pairs} ->
      name = pairs |> List.keyfind(:name, 0) |> elem(1) |> to_string()
      pointer = pairs |> List.keyfind(:pointer, 0) |> elem(1) |> to_string()
      %{"name" => name, "pointer" => pointer}
    end)
  end

  defp bind_to_list(_), do: []

  # ── Methods ────────────────────────────────────────────────────────
  defp method_to_json({:%{}, _, pairs}) when is_list(pairs) do
    params = pairs |> List.keyfind(:params, 0) |> elem(1) |> Enum.map(&to_string/1)
    alts = pairs |> List.keyfind(:alternatives, 0) |> elem(1) |> alternatives_to_list()
    %{"params" => params, "alternatives" => alts}
  end

  defp method_to_json(_), do: %{"params" => [], "alternatives" => []}

  defp alternatives_to_list(list) when is_list(list) do
    Enum.map(list, fn
      {:%{}, _, pairs} ->
        name = pairs |> List.keyfind(:name, 0) |> elem(1) |> to_string()
        subtasks = pairs |> List.keyfind(:subtasks, 0) |> elem(1) |> subtasks_to_list()

        check =
          case List.keyfind(pairs, :check, 0) do
            {_, checks} -> Enum.map(checks, &check_to_json/1)
            nil -> []
          end

        %{"name" => name, "subtasks" => subtasks}
        |> then(fn m -> if check != [], do: Map.put(m, "check", check), else: m end)
    end)
  end

  defp alternatives_to_list(_), do: []

  # ── Body items (action body) ───────────────────────────────────────
  defp body_to_list(list) when is_list(list) do
    Enum.map(list, fn
      {:%{}, _, pairs} ->
        cond do
          List.keyfind(pairs, :pointer_set, 0) ->
            path = pairs |> List.keyfind(:pointer_set, 0) |> elem(1) |> to_string()
            value = pairs |> List.keyfind(:value, 0) |> elem(1) |> ptr_value_to_json()
            %{"pointer/set" => path, "value" => value}

          List.keyfind(pairs, :eval, 0) ->
            eval_ast = pairs |> List.keyfind(:eval, 0) |> elem(1)
            %{"eval" => eval_to_json(eval_ast)}

          true ->
            %{}
        end
    end)
  end

  defp body_to_list(_), do: []

  defp eval_to_json({:%{}, _, pairs}) when is_list(pairs) do
    type = pairs |> List.keyfind(:type, 0) |> elem(1)
    a = pairs |> List.keyfind(:a, 0) |> elem(1) |> expr_to_json()

    b =
      case List.keyfind(pairs, :b, 0) do
        {_, val} -> expr_to_json(val)
        nil -> nil
      end

    base = %{"type" => type, "a" => a}
    if b != nil, do: Map.put(base, "b", b), else: base
  end

  # ── Check items (method alternative guards) ────────────────────────
  defp check_to_json({:%{}, _, pairs}) when is_list(pairs) do
    eval_ast = pairs |> List.keyfind(:eval, 0) |> elem(1)
    %{"eval" => eval_to_json(eval_ast)}
  end

  defp check_to_json({:condition, _, [type, args]}) do
    # Legacy condition form — kept for compatibility
    a = args |> List.first() |> expr_to_json()
    b = if length(args) > 1, do: args |> Enum.at(1) |> expr_to_json(), else: nil
    base = %{"type" => to_string(type), "a" => a}
    if b != nil, do: Map.put(base, "b", b), else: base
  end

  # ── Expressions (pointer_get, literals) ────────────────────────────
  defp expr_to_json({:%{}, _, pairs}) when is_list(pairs) do
    case List.keyfind(pairs, :pointer_get, 0) do
      {_, path} -> %{"type" => "pointer/get", "pointer" => to_string(path)}
      nil -> %{}
    end
  end

  defp expr_to_json(true), do: true
  defp expr_to_json(false), do: false
  defp expr_to_json(atom) when is_atom(atom), do: to_string(atom)
  defp expr_to_json(bin) when is_binary(bin), do: bin
  defp expr_to_json(num) when is_number(num), do: num

  # pointer_set value — preserve booleans, stringify atoms
  defp ptr_value_to_json(true), do: true
  defp ptr_value_to_json(false), do: false
  defp ptr_value_to_json(val), do: to_string(val)

  # ── Subtasks (todo_list entries) ───────────────────────────────────
  defp subtasks_to_list(list) when is_list(list) do
    Enum.map(list, fn
      [name | args] when is_atom(name) -> [to_string(name) | Enum.map(args, &to_string/1)]
      other -> other
    end)
  end

  defp subtasks_to_list(_), do: []

  defp todo_to_list(list) when is_list(list) do
    Enum.map(list, fn
      [name | args] when is_atom(name) ->
        [to_string(name) | Enum.map(args, &to_string/1)]

      {:%{}, _, pairs} ->
        case List.keyfind(pairs, :goal, 0) do
          {_, goals} ->
            %{"goal" => goal_to_list(goals)}

          nil ->
            case List.keyfind(pairs, :multigoal, 0) do
              {_, mg} -> %{"multigoal" => mg}
              nil -> %{}
            end
        end

      other ->
        other
    end)
  end

  defp todo_to_list(_), do: []

  # ── Goal bindings ──────────────────────────────────────────────────
  defp goal_to_list(list) when is_list(list) do
    Enum.map(list, fn {:%{}, _, pairs} ->
      %{
        "pointer" => pairs |> List.keyfind(:pointer, 0) |> elem(1) |> to_string(),
        "eq" => pairs |> List.keyfind(:eq, 0) |> elem(1) |> to_string()
      }
    end)
  end

  defp goal_to_list(_), do: []

  # ── AST literal (for init values) ──────────────────────────────────
  defp ast_literal(true), do: true
  defp ast_literal(false), do: false
  defp ast_literal(atom) when is_atom(atom), do: to_string(atom)
  defp ast_literal(bin) when is_binary(bin), do: bin
  defp ast_literal(num) when is_number(num), do: num

  defp ast_literal({:%{}, _, pairs}) when is_list(pairs) do
    Map.new(pairs, fn {k, v} -> {to_string(k), ast_literal(v)} end)
  end

  defp ast_literal(other), do: to_string(other)

  # ── Finalize ───────────────────────────────────────────────────────
  defp finalize(domain) do
    # Re-add capabilities if present (handled by the NIF)
    capabilities = Map.get(domain, "capabilities")

    domain
    |> Map.delete("capabilities")
    |> then(fn d ->
      if capabilities, do: Map.put(d, "capabilities", capabilities), else: d
    end)
  end
end
