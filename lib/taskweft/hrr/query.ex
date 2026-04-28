defmodule Taskweft.HRR.Query do
  @moduledoc """
  Ecto query evaluator for `Taskweft.HRR.Storage`.

  ## WHERE strategy

  | Predicate       | Method                                                    |
  |-----------------|-----------------------------------------------------------|
  | `== value`      | Exact in-memory equality after `Storage.all/2`            |
  | `!= value`      | Exact inequality after `Storage.all/2`                    |
  | `like pattern`  | `Storage.probe_field/5` (HRR cosine, case-sensitive)      |
  | `ilike pattern` | `Storage.probe_field/5` (HRR cosine, case-insensitive)    |
  | `and` / `or`    | Boolean composition                                       |
  | `not`           | Negation                                                  |
  | anything else   | Pass-through (all rows satisfy the clause)                |

  ## Joins

  Inner joins are supported.  The join condition determines strategy:

  - `on: a.k == b.k`   – exact hash join (group right by key, match left)
  - `on: a.k like b.k` – HRR semantic join: unbind the role from both sides
                          via `Storage.vectors_for_join`, rank by cosine
                          similarity, include right rows above `hrr_threshold`

  Joined rows are `[left_map, right_map, ...]`; the binding index in field
  references selects the correct map.  Unresolvable join sources are silently
  skipped (no rows crossed in).

  ## Aggregates

  | Expression     | Computation                                             |
  |----------------|---------------------------------------------------------|
  | `count(*)`     | Fast path: reads `record_count` from `hrr_bundles`      |
  | `count(field)` | Count non-nil values after filter                       |
  | `sum(field)`   | Sum numeric field values after filter                   |
  | `avg(field)`   | Average numeric field values after filter               |
  | `min(field)`   | Minimum field value after filter                        |
  | `max(field)`   | Maximum field value after filter                        |

  `count(*)` without WHERE reads `hrr_bundles.record_count` — O(1), no scan.

  ORDER BY, LIMIT, and OFFSET are applied after all other processing.
  """

  alias Taskweft.HRR.Storage

  @default_hrr_threshold 0.1

  @doc "Execute a compiled Ecto `:all` query against *store*."
  @spec execute(Storage.store(), :all, term(), list(), keyword()) :: [map()]
  def execute(store, :all, query, params, opts) do
    threshold = Keyword.get(opts, :hrr_threshold, @default_hrr_threshold)
    wheres = Map.get(query, :wheres, [])
    joins = Map.get(query, :joins, [])
    select = Map.get(query, :select)

    case source_name(query) do
      {:error, _} ->
        []

      {:ok, source} ->
        case aggregate_expr(select) do
          {:count_all} when joins == [] and wheres == [] ->
            [[Storage.record_count(store, source)]]

          agg_expr ->
            rows = fetch_rows(store, source, wheres, params, threshold)
            rows = apply_joins(store, rows, joins, params, threshold)

            if agg_expr do
              [[compute_aggregate(rows, agg_expr)]]
            else
              rows
              |> apply_order(Map.get(query, :order_bys, []))
              |> apply_limit_offset(Map.get(query, :limit), Map.get(query, :offset))
            end
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Row fetching
  # ---------------------------------------------------------------------------

  defp fetch_rows(store, source, [], _params, _threshold),
    do: Storage.all(store, source)

  defp fetch_rows(store, source, [%{expr: expr}], params, threshold) do
    case extract_probe(expr, params) do
      {:probe_field, field, text} ->
        Storage.probe_field(store, source, field, text, threshold: threshold)
        |> Enum.map(fn {_sim, map} -> map end)

      :exact ->
        Storage.all(store, source) |> Enum.filter(&eval_expr(&1, expr, params))
    end
  end

  defp fetch_rows(store, source, wheres, params, _threshold) do
    Storage.all(store, source)
    |> Enum.filter(fn row ->
      Enum.all?(wheres, fn %{expr: expr} -> eval_expr(row, expr, params) end)
    end)
  end

  # ---------------------------------------------------------------------------
  # Joins
  # ---------------------------------------------------------------------------

  defp apply_joins(_store, rows, [], _params, _threshold), do: rows

  defp apply_joins(store, left_rows, joins, params, threshold) do
    Enum.reduce(joins, left_rows, fn join_expr, acc ->
      apply_one_join(store, acc, join_expr, params, threshold)
    end)
  end

  defp apply_one_join(store, left_rows, join_expr, params, threshold) do
    case join_source(join_expr) do
      nil ->
        []

      right_source ->
        on_expr = join_on(join_expr)

        case join_strategy(on_expr, params) do
          {:exact, left_field, right_field} ->
            exact_join(store, left_rows, right_source, left_field, right_field)

          {:semantic, left_field, right_field} ->
            semantic_join(store, left_rows, right_source, left_field, right_field, threshold)

          :cross ->
            right_rows = Storage.all(store, right_source)
            for l <- left_rows, r <- right_rows, do: join_row(l, r)
        end
    end
  end

  defp exact_join(store, left_rows, right_source, left_field, right_field) do
    right_by_key =
      Storage.all(store, right_source)
      |> Enum.group_by(&Map.get(&1, to_string(right_field)))

    Enum.flat_map(left_rows, fn left ->
      key = left_field_value(left, left_field)
      Map.get(right_by_key, key, []) |> Enum.map(&join_row(left, &1))
    end)
  end

  defp semantic_join(store, left_rows, right_source, left_field, right_field, threshold) do
    {_pool, dim} = store
    right_probes = Storage.vectors_for_join(store, right_source, to_string(right_field))

    Enum.flat_map(left_rows, fn left ->
      query_text = left_field_value(left, left_field) |> to_string()
      query_phases = Taskweft.NIF.hrr_encode_text(query_text, dim)

      for {probe_bytes, right_map} <- right_probes,
          sim =
            Taskweft.NIF.hrr_similarity(
              Taskweft.NIF.hrr_bytes_to_phases(probe_bytes, 0),
              query_phases
            ),
          sim >= threshold do
        join_row(left, right_map)
      end
    end)
  end

  defp join_row(left, right) when is_list(left), do: left ++ [right]
  defp join_row(left, right), do: [left, right]

  defp left_field_value(row, field) when is_list(row),
    do: Map.get(hd(row), to_string(field))

  defp left_field_value(row, field),
    do: Map.get(row, to_string(field))

  defp join_source(%{source: {name, _}}) when is_binary(name), do: name
  defp join_source(%{source: name}) when is_binary(name), do: name
  defp join_source(_), do: nil

  defp join_on(%{on: %{expr: expr}}), do: expr
  defp join_on(_), do: nil

  defp join_strategy({:==, _, [l, r]}, _params) do
    case {join_field_name(l), join_field_name(r)} do
      {lf, rf} when not is_nil(lf) and not is_nil(rf) -> {:exact, lf, rf}
      _ -> :cross
    end
  end

  defp join_strategy({op, _, [l, r]}, _params) when op in [:like, :ilike] do
    case {join_field_name(l), join_field_name(r)} do
      {lf, rf} when not is_nil(lf) and not is_nil(rf) -> {:semantic, lf, rf}
      _ -> :cross
    end
  end

  defp join_strategy(_, _), do: :cross

  defp join_field_name({{:., _, [{:&, _, [_]}, field]}, _, _}), do: field
  defp join_field_name(_), do: nil

  # ---------------------------------------------------------------------------
  # Aggregates
  # ---------------------------------------------------------------------------

  defp aggregate_expr(nil), do: nil
  defp aggregate_expr(%{expr: {:count, _, []}}), do: {:count_all}
  defp aggregate_expr(%{expr: {:count, _, [field_ref]}}), do: {:count, field_ref}
  defp aggregate_expr(%{expr: {:sum, _, [field_ref]}}), do: {:sum, field_ref}
  defp aggregate_expr(%{expr: {:avg, _, [field_ref]}}), do: {:avg, field_ref}
  defp aggregate_expr(%{expr: {:min, _, [field_ref]}}), do: {:min, field_ref}
  defp aggregate_expr(%{expr: {:max, _, [field_ref]}}), do: {:max, field_ref}
  defp aggregate_expr(_), do: nil

  defp compute_aggregate(rows, {:count_all}), do: length(rows)

  defp compute_aggregate(rows, {:count, field_ref}),
    do: Enum.count(rows, &(resolve(&1, field_ref, []) != nil))

  defp compute_aggregate(rows, {:sum, field_ref}) do
    rows |> Enum.map(&resolve(&1, field_ref, [])) |> Enum.reject(&is_nil/1) |> Enum.sum()
  end

  defp compute_aggregate(rows, {:avg, field_ref}) do
    vals = rows |> Enum.map(&resolve(&1, field_ref, [])) |> Enum.reject(&is_nil/1)
    if vals == [], do: nil, else: Enum.sum(vals) / length(vals)
  end

  defp compute_aggregate(rows, {:min, field_ref}) do
    vals = rows |> Enum.map(&resolve(&1, field_ref, [])) |> Enum.reject(&is_nil/1)
    if vals == [], do: nil, else: Enum.min(vals)
  end

  defp compute_aggregate(rows, {:max, field_ref}) do
    vals = rows |> Enum.map(&resolve(&1, field_ref, [])) |> Enum.reject(&is_nil/1)
    if vals == [], do: nil, else: Enum.max(vals)
  end

  # ---------------------------------------------------------------------------
  # Probe detection
  # ---------------------------------------------------------------------------

  defp extract_probe({op, _, [field_ref, val_ref]}, params) when op in [:like, :ilike] do
    field = field_atom(field_ref)
    text = strip_wildcards(resolve(nil, val_ref, params))

    if field && is_binary(text) && text != "",
      do: {:probe_field, field, text},
      else: :exact
  end

  defp extract_probe(_expr, _params), do: :exact

  # ---------------------------------------------------------------------------
  # In-memory expression evaluator
  # ---------------------------------------------------------------------------

  defp eval_expr(row, {:and, _, [l, r]}, params),
    do: eval_expr(row, l, params) and eval_expr(row, r, params)

  defp eval_expr(row, {:or, _, [l, r]}, params),
    do: eval_expr(row, l, params) or eval_expr(row, r, params)

  defp eval_expr(row, {:not, _, [e]}, params),
    do: not eval_expr(row, e, params)

  defp eval_expr(row, {:==, _, [l, r]}, params),
    do: resolve(row, l, params) == resolve(row, r, params)

  defp eval_expr(row, {:!=, _, [l, r]}, params),
    do: resolve(row, l, params) != resolve(row, r, params)

  defp eval_expr(row, {op, _, [l, r]}, params) when op in [:like, :ilike] do
    val = resolve(row, l, params)
    pattern = resolve(row, r, params)

    is_binary(val) and is_binary(pattern) and
      Regex.match?(like_regex(pattern, op == :ilike), val)
  end

  defp eval_expr(_row, _expr, _params), do: true

  # ---------------------------------------------------------------------------
  # Value resolution
  # ---------------------------------------------------------------------------

  defp resolve(row, {{:., _, [{:&, _, [idx]}, field]}, _, _}, _params),
    do: row_binding(row, idx) |> Map.get(to_string(field))

  defp resolve(_row, {:^, _, [index]}, params),
    do: Enum.at(params, index)

  defp resolve(_row, value, _params)
       when is_atom(value) or is_binary(value) or is_number(value) or is_nil(value),
       do: value

  defp resolve(_row, _expr, _params), do: nil

  defp row_binding(row, 0) when is_map(row), do: row
  defp row_binding(rows, idx) when is_list(rows), do: Enum.at(rows, idx) || %{}
  defp row_binding(_, _), do: %{}

  defp field_atom({{:., _, [{:&, _, [_]}, field]}, _, _}) when is_atom(field), do: field
  defp field_atom(_), do: nil

  # ---------------------------------------------------------------------------
  # ORDER BY
  # ---------------------------------------------------------------------------

  defp apply_order(rows, []), do: rows

  defp apply_order(rows, order_bys) do
    Enum.sort(rows, fn a, b ->
      Enum.reduce_while(order_bys, :eq, fn ob, _ ->
        {dir, idx, field} = order_parts(ob)
        va = row_binding(a, idx) |> Map.get(to_string(field))
        vb = row_binding(b, idx) |> Map.get(to_string(field))

        case cmp_dir(va, vb, dir) do
          :eq -> {:cont, :eq}
          cmp -> {:halt, cmp}
        end
      end) == :lt
    end)
  end

  defp order_parts(%{expr: [{dir, {{:., _, [{:&, _, [idx]}, field]}, _, _}}]}),
    do: {dir, idx, field}

  defp order_parts(_), do: {:asc, 0, nil}

  defp cmp_dir(a, b, :asc), do: cmp(a, b)
  defp cmp_dir(a, b, :desc), do: cmp(b, a)

  defp cmp(a, b) when a < b, do: :lt
  defp cmp(a, b) when a > b, do: :gt
  defp cmp(_, _), do: :eq

  # ---------------------------------------------------------------------------
  # LIMIT / OFFSET
  # ---------------------------------------------------------------------------

  defp apply_limit_offset(rows, nil, nil), do: rows

  defp apply_limit_offset(rows, limit, offset) do
    rows |> maybe_drop(unwrap_int(offset)) |> maybe_take(unwrap_int(limit))
  end

  defp maybe_drop(rows, nil), do: rows
  defp maybe_drop(rows, n), do: Enum.drop(rows, n)

  defp maybe_take(rows, nil), do: rows
  defp maybe_take(rows, n), do: Enum.take(rows, n)

  defp unwrap_int(nil), do: nil
  defp unwrap_int(%{expr: {:^, _, [n]}}), do: n
  defp unwrap_int(%{expr: n}) when is_integer(n), do: n
  defp unwrap_int(n) when is_integer(n), do: n
  defp unwrap_int(_), do: nil

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp source_name(query) do
    case Map.get(query, :from) do
      %{source: {name, _}} when is_binary(name) -> {:ok, name}
      %{source: name} when is_binary(name) -> {:ok, name}
      other -> {:error, {:unknown_source, other}}
    end
  end

  defp strip_wildcards(nil), do: nil
  defp strip_wildcards(s), do: String.replace(s, ~r/[%_]/, " ") |> String.trim()

  defp like_regex(pattern, case_insensitive) do
    flags = if case_insensitive, do: "i", else: ""

    regex_str =
      pattern
      |> Regex.escape()
      |> String.replace("\\%", ".*")
      |> String.replace("\\_", ".")

    Regex.compile!("^#{regex_str}$", flags)
  end
end
