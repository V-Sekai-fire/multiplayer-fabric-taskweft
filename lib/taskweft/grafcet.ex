# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.Grafcet do
  @moduledoc """
  Compact IEC 60848 GRAFCET (Project-AGRAFE aligned) <-> Taskweft HTN.

  `lower/1` turns a compact GRAFCET JSON-LD map into an HTN domain map in
  the shape the C++ NIF loader consumes (variables / actions / methods /
  tasks, with `pointer/set`, `pointer/get`, `math/eq`). `to_grafcet/1`
  goes the other way, dropping transitively-redundant guards to a
  canonical form. Both are pure functions on parsed maps.

  Round-trip semantics: `to_grafcet(lower(g)) == g` when `g` is canonical;
  `lower(to_grafcet(h))` is idempotent for any well-formed `h`.

  Scope limited to the AND/sequential fragment used by
  `weftspun-build.grafcet.jsonld`: `^`, `Step`, `&>`, `&<`. The `|>`,
  `|<`, `!>`, `%`, `#` markers raise `RuntimeError`.
  """

  @assign_re ~r/^\s*V\.(?<var>\w+)\s*:=\s*(?<val>[^,\s]+)\s*$/
  @atom_re ~r/^X\.(?<step>\w+)$/
  @duration_re ~r/^(?<n>\d+)h$/
  @iso_duration_re ~r/^PT(?<n>\d+)H$/

  ## -- lower: compact GRAFCET -> HTN --------------------------------------

  @spec lower(map()) :: map()
  def lower(%{"S" => steps, "V" => vars} = grafcet) do
    {ordinary, order, or_groups} = walk_steps(steps)

    # or_groups is [{parent_name, [branch_step_names]}, ...]. Each branch
    # step becomes an alternative of a synthesised chooser method rather
    # than a standalone `m_<name>` skip-or-do, and the branch steps are
    # dropped from the top-level buildout sequence (the chooser method
    # replaces them).
    or_branch_to_group =
      for {parent, branches} <- or_groups,
          branch <- branches,
          into: %{},
          do: {branch, parent}

    actions =
      order
      |> Enum.map(fn name ->
        info = Map.fetch!(ordinary, name)
        preds = parse_receptivity(info.when) ++ info.extra_pred

        preds =
          if preds == [] and info.implicit_parent do
            [info.implicit_parent]
          else
            preds
          end

        preds = Enum.uniq(preds)
        {var, val} = parse_action(info.do_)

        body =
          Enum.map(preds, fn p ->
            %{
              "eval" => %{
                "type" => "math/eq",
                "a" => %{"type" => "pointer/get", "pointer" => "/done/#{p}"},
                "b" => true
              }
            }
          end) ++ [%{"pointer/set" => "/done/#{var}", "value" => bool_of(val)}]

        {"a_#{name}",
         %{"params" => [], "duration" => duration_to_iso(info.t), "body" => body}}
      end)
      |> Map.new()

    or_method_names =
      for {parent, _} <- or_groups, into: %{}, do: {parent, "m_choose_after_#{parent}"}

    plain_methods =
      order
      |> Enum.reject(&Map.has_key?(or_branch_to_group, &1))
      |> Enum.map(fn name -> {"m_#{name}", skip_or_do(name)} end)
      |> Map.new()

    chooser_methods =
      for {parent, branches} <- or_groups, into: %{} do
        alts =
          Enum.map(branches, fn b ->
            info = Map.fetch!(ordinary, b)
            preds = parse_receptivity(info.when)
            check =
              Enum.map(preds, fn p ->
                %{
                  "eval" => %{
                    "type" => "math/eq",
                    "a" => %{"type" => "pointer/get", "pointer" => "/done/#{p}"},
                    "b" => true
                  }
                }
              end)

            base = %{"name" => b, "subtasks" => [["a_#{b}"]]}
            if check == [], do: base, else: Map.put(base, "check", check)
          end)

        {or_method_names[parent], %{"params" => [], "alternatives" => alts}}
      end

    methods = Map.merge(plain_methods, chooser_methods)

    buildout_subtasks =
      order
      |> Enum.flat_map(fn n ->
        cond do
          Map.has_key?(or_branch_to_group, n) ->
            parent = or_branch_to_group[n]
            # emit the chooser method exactly once, at the position of the
            # first branch step
            branches = or_groups |> Enum.find(&(elem(&1, 0) == parent)) |> elem(1)
            if hd(branches) == n, do: [[or_method_names[parent]]], else: []

          true ->
            [["m_#{n}"]]
        end
      end)

    methods =
      Map.put(methods, "buildout", %{
        "params" => [],
        "alternatives" => [%{"name" => "do", "subtasks" => buildout_subtasks}]
      })

    %{
      "@context" => %{
        "weftspun" => "https://github.com/weftspun/",
        "domain" => "weftspun:planning/domain/"
      },
      "@type" => "domain:Definition",
      "name" => Map.get(grafcet, "sfc"),
      "description" => Map.get(grafcet, "descr"),
      "variables" => [%{"name" => "done", "init" => vars}],
      "actions" => actions,
      "methods" => methods,
      "tasks" => [["buildout"]]
    }
    |> reject_nil()
  end

  defp bool_of(1), do: true
  defp bool_of(0), do: false
  defp bool_of(other), do: other

  defp skip_or_do(name) do
    %{
      "params" => [],
      "alternatives" => [
        %{
          "name" => "skip",
          "check" => [
            %{
              "eval" => %{
                "type" => "math/eq",
                "a" => %{"type" => "pointer/get", "pointer" => "/done/#{name}"},
                "b" => true
              }
            }
          ],
          "subtasks" => []
        },
        %{"name" => "do", "subtasks" => [["a_#{name}"]]}
      ]
    }
  end

  # Walker state:
  #   {ordinary, order, last, fanout_parent, fanout_pending, pending_conv,
  #    or_parent, or_children, or_groups}
  #
  # or_parent / or_children track an open |> block (mirror of the &> pair).
  # or_groups is a list of {parent_name, [branch_step_names]} once |< closes.
  defp walk_steps(steps) do
    init = {%{}, [], nil, nil, nil, nil, nil, nil, []}

    {ordinary, order, _, _, _, _, _, _, or_groups} =
      Enum.reduce(steps, init, &walk_step/2)

    {ordinary, Enum.reverse(order), or_groups}
  end

  defp walk_step(["^"], acc), do: acc

  defp walk_step(["&>" | children], acc) do
    {ord, order, last, _fp, _pen, pc, op, oc, og} = acc
    {ord, order, last, last, children, pc, op, oc, og}
  end

  defp walk_step(["&<" | preds], acc) do
    {ord, order, last, fp, fpen, _pc, op, oc, og} = acc
    {ord, order, last, fp, fpen, preds, op, oc, og}
  end

  defp walk_step(["|>" | children], acc) do
    {ord, order, last, fp, fpen, pc, _op, _oc, og} = acc
    {ord, order, last, fp, fpen, pc, last, children, og}
  end

  defp walk_step(["|<"], acc) do
    # close the OR block, record its {parent, branches}
    {ord, order, last, fp, fpen, pc, op, _oc, og} = acc
    branches = for n <- Enum.reverse(order), n in Map.keys(ord),
                 Map.get(ord[n], :or_parent) == op, do: n

    {ord, order, last, fp, fpen, pc, nil, nil, [{op, Enum.reverse(branches)} | og]}
  end

  defp walk_step([head | _], _) when head == "!>",
    do: raise("lowering of #{inspect(head)} not in scope (staged; see RFD 2143)")

  defp walk_step([head | _], _)
       when is_binary(head) and (binary_part(head, 0, 1) == "%" or binary_part(head, 0, 1) == "#"),
       do: raise("lowering of #{inspect(head)} not in scope (staged; see RFD 2143)")

  defp walk_step([name, when_, do_, t], acc) do
    {ord, order, last, fp, fpen, pc, op, oc, og} = acc
    extra_pred = pc || []

    {implicit_parent, fp2, fpen2, or_parent, oc2} =
      cond do
        fpen && name in fpen ->
          rest = List.delete(fpen, name)
          if rest == [],
            do: {fp, nil, nil, nil, oc},
            else: {fp, fp, rest, nil, oc}

        oc && name in oc ->
          rest = List.delete(oc, name)
          {op, fp, fpen, op, if(rest == [], do: nil, else: rest)}

        true ->
          {last, fp, fpen, nil, oc}
      end

    info = %{
      when: when_,
      do_: do_,
      t: t,
      extra_pred: extra_pred,
      implicit_parent: implicit_parent,
      or_parent: or_parent
    }

    {Map.put(ord, name, info), [name | order], name, fp2, fpen2, nil, op, oc2, og}
  end

  defp parse_receptivity(""), do: []
  defp parse_receptivity(nil), do: []

  defp parse_receptivity(expr) when is_binary(expr) do
    expr
    |> String.split("&")
    |> Enum.map(&String.trim/1)
    |> Enum.map(fn atom ->
      case Regex.named_captures(@atom_re, atom) do
        %{"step" => s} -> s
        _ -> raise "receptivity atom outside supported fragment: #{inspect(atom)}"
      end
    end)
  end

  defp parse_action(expr) when is_binary(expr) do
    case Regex.named_captures(@assign_re, expr) do
      %{"var" => v, "val" => raw} ->
        val =
          cond do
            String.match?(raw, ~r/^\d+$/) -> String.to_integer(raw)
            raw == "true" -> true
            raw == "false" -> false
            true -> raw
          end

        {v, val}

      _ ->
        raise "action outside supported fragment: #{inspect(expr)}"
    end
  end

  defp duration_to_iso(s) do
    case Regex.named_captures(@duration_re, s || "") do
      %{"n" => n} -> "PT#{n}H"
      _ -> raise "unsupported duration: #{inspect(s)}"
    end
  end

  defp duration_from_iso(s) do
    case Regex.named_captures(@iso_duration_re, s || "") do
      %{"n" => n} -> "#{n}h"
      _ -> raise "unsupported ISO duration: #{inspect(s)}"
    end
  end

  defp reject_nil(map) do
    Map.reject(map, fn {_, v} -> is_nil(v) end)
  end

  ## -- to_grafcet: HTN -> compact GRAFCET ---------------------------------

  @spec to_grafcet(map()) :: map()
  def to_grafcet(%{"actions" => actions, "variables" => [%{"init" => init} | _]} = htn) do
    # Elixir maps at this size are hash-ordered, so we can't read step order
    # from actions' keys. Take it from the buildout method's subtasks list,
    # which is a JSON array and thus order-preserving.
    names =
      htn
      |> get_in(["methods", "buildout", "alternatives"])
      |> List.first()
      |> Map.get("subtasks")
      |> Enum.map(fn [m] -> String.replace_prefix(m, "m_", "") end)

    order = Enum.map(names, &"a_#{&1}")
    parsed = for aname <- order, into: %{}, do: {String.replace_prefix(aname, "a_", ""), parse_action_body(actions[aname])}

    succs = build_succs(names, parsed)
    fanout_members = build_fanout_members(succs, parsed)

    {s_rev, _} =
      names
      |> Enum.with_index()
      |> Enum.reduce({[["^"]], MapSet.new()}, fn {name, i}, {acc, emitted} ->
        info = parsed[name]
        prev = if i > 0, do: Enum.at(names, i - 1)

        {acc, emitted} =
          if Map.has_key?(fanout_members, name) do
            parent = fanout_members[name]

            if MapSet.member?(emitted, parent) do
              {acc, emitted}
            else
              members = for {c, p} <- fanout_members, p == parent, do: c
              members = Enum.filter(names, &(&1 in members))
              {[["&>" | members] | acc], MapSet.put(emitted, parent)}
            end
          else
            {acc, emitted}
          end

        fan_in = length(info.preds) >= 2

        acc = if fan_in, do: [["&<" | info.preds] | acc], else: acc

        when_ =
          cond do
            fan_in -> ""
            Map.has_key?(fanout_members, name) -> ""
            true -> info.preds |> Enum.reject(&(&1 == prev)) |> Enum.map(&"X.#{&1}") |> Enum.join(" & ")
          end

        do_ = "V.#{info.var}:=#{int_of(info.val)}"
        t = duration_from_iso(info.duration)
        {[[name, when_, do_, t] | acc], emitted}
      end)

    %{
      "@context" => %{
        "@version" => 1.1,
        "grafcet" => "https://project-agrafe.github.io/ns/grafcet#",
        "sfc" => %{"@id" => "grafcet:name"},
        "descr" => %{"@id" => "grafcet:description"},
        "V" => %{"@id" => "grafcet:internalVariables", "@container" => "@index"},
        "S" => %{"@id" => "grafcet:steps", "@container" => "@list"},
        "^" => "grafcet:InitialStep",
        "%" => "grafcet:MacroStep",
        "#" => "grafcet:EnclosingStep",
        "&>" => "grafcet:AndDivergence",
        "&<" => "grafcet:AndConvergence",
        "|>" => "grafcet:OrDivergence",
        "|<" => "grafcet:OrConvergence",
        "!>" => "grafcet:ForcingOrder"
      },
      "@type" => "grafcet:SFC",
      "sfc" => Map.get(htn, "name"),
      "descr" => Map.get(htn, "description"),
      "V" => init,
      "S" => Enum.reverse(s_rev)
    }
    |> reject_nil()
  end

  defp parse_action_body(action) do
    {preds, var_val} =
      Enum.reduce(action["body"], {[], nil}, fn
        %{"eval" => eval}, {preds, vv} ->
          "/done/" <> rest = eval["a"]["pointer"]
          {preds ++ [rest], vv}

        %{"pointer/set" => path, "value" => v}, {preds, _} ->
          "/done/" <> rest = path
          {preds, {rest, v}}
      end)

    {var, val} = var_val
    %{preds: preds, var: var, val: val, duration: action["duration"]}
  end

  defp build_succs(names, parsed) do
    base = for n <- names, into: %{}, do: {n, []}

    Enum.reduce(names, base, fn n, acc ->
      Enum.reduce(parsed[n].preds, acc, fn p, a ->
        Map.update(a, p, [n], &(&1 ++ [n]))
      end)
    end)
  end

  defp build_fanout_members(succs, parsed) do
    Enum.reduce(succs, %{}, fn {parent, children}, acc ->
      single = Enum.filter(children, &(parsed[&1].preds == [parent]))

      if length(single) >= 2 do
        Enum.reduce(single, acc, fn c, a -> Map.put(a, c, parent) end)
      else
        acc
      end
    end)
  end

  defp int_of(true), do: 1
  defp int_of(false), do: 0
  defp int_of(n) when is_integer(n), do: n
  defp int_of(other), do: other

  ## -- semantic equality --------------------------------------------------

  @doc "Order-insensitive canonical form for eq check."
  def canon(x) when is_map(x), do: x |> Enum.sort_by(fn {k, _} -> k end) |> Enum.map(fn {k, v} -> {k, canon(v)} end) |> Map.new()
  def canon(x) when is_list(x), do: Enum.map(x, &canon/1)
  def canon(x), do: x
end
