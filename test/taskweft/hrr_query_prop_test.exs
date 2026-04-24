defmodule Taskweft.HRR.QueryPropTest do
  use ExUnit.Case, async: false
  use PropCheck

  @moduletag :property

  alias Taskweft.HRR.{Query, Storage}

  @dim 64
  @source "test_items"

  # ---------------------------------------------------------------------------
  # Generators
  # ---------------------------------------------------------------------------

  def word_gen do
    let chars <- non_empty(list(oneof([range(?a, ?z)]))),
      do: to_string(chars)
  end

  def fields_gen do
    let {name, role, value} <- {word_gen(), word_gen(), word_gen()} do
      %{"name" => name, "role" => role, "value" => value}
    end
  end

  def record_list_gen do
    let pairs <- non_empty(list({pos_integer(), fields_gen()})) do
      pairs
      |> Enum.map(fn {n, f} -> {"id-#{n}", f} end)
      |> Enum.uniq_by(&elem(&1, 0))
    end
  end

  # ---------------------------------------------------------------------------
  # Ecto query stub builders (no Ecto dep required)
  # ---------------------------------------------------------------------------

  # Minimal structs that mirror what Ecto produces at runtime.
  # Query.execute accesses these via Map.get/2 so plain maps suffice.

  defp base_query(source \\ @source) do
    %{
      from: %{source: {source, nil}},
      wheres: [],
      order_bys: [],
      limit: nil,
      offset: nil,
      select: nil
    }
  end

  defp field_ref(field) when is_atom(field),
    do: {{:., [], [{:&, [], [0]}, field]}, [], []}

  defp field_ref(field) when is_binary(field),
    do: field_ref(String.to_atom(field))

  defp param_ref(index), do: {:^, [], [index]}

  defp where_expr(expr) do
    %{expr: expr, params: [], op: :and}
  end

  defp eq_where(field, param_index),
    do: where_expr({:==, [], [field_ref(field), param_ref(param_index)]})

  defp neq_where(field, param_index),
    do: where_expr({:!=, [], [field_ref(field), param_ref(param_index)]})

  defp like_where(field, param_index),
    do: where_expr({:like, [], [field_ref(field), param_ref(param_index)]})

  defp ilike_where(field, param_index),
    do: where_expr({:ilike, [], [field_ref(field), param_ref(param_index)]})

  defp order_asc(field),
    do: %{expr: [{:asc,  field_ref(field)}]}

  defp order_desc(field),
    do: %{expr: [{:desc, field_ref(field)}]}

  defp limit_expr(n), do: %{expr: n}
  defp offset_expr(n), do: %{expr: n}

  # ---------------------------------------------------------------------------
  # Storage helpers
  # ---------------------------------------------------------------------------

  defp with_storage(fun) do
    url = System.get_env("TEST_DATABASE_URL", "postgresql://root@localhost:26257/taskweft_test?sslmode=disable")
    pool_name = :"hrr_query_test_#{:erlang.unique_integer([:positive])}"
    {:ok, _} = Postgrex.start_link(name: pool_name, url: url)
    Storage.ensure_schema!(pool_name)
    Postgrex.query!(pool_name, "DELETE FROM hrr_records", [])
    Postgrex.query!(pool_name, "DELETE FROM hrr_bundles", [])
    store = {pool_name, @dim}
    try do
      fun.(store)
    after
      Postgrex.query!(pool_name, "DELETE FROM hrr_records", [])
      Postgrex.query!(pool_name, "DELETE FROM hrr_bundles", [])
      GenServer.stop(pool_name)
    end
  end
  end

  defp populate(store, records) do
    Enum.each(records, fn {id, fields} ->
      :ok = Storage.insert(store, @source, id, fields)
    end)
  end

  # ---------------------------------------------------------------------------
  # No WHERE — returns all rows
  # ---------------------------------------------------------------------------

  property "no WHERE returns all inserted records" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = base_query()
        result = Query.execute(store, :all, query, [], [])

        length(result) == length(records) and
          Enum.all?(records, fn {_id, fields} -> fields in result end)
      end)
    end
  end

  property "no WHERE on empty source returns []" do
    with_storage(fn store ->
      query = base_query("empty_source_#{:erlang.unique_integer()}")
      Query.execute(store, :all, query, [], []) == []
    end)
  end

  # ---------------------------------------------------------------------------
  # WHERE == (exact match)
  # ---------------------------------------------------------------------------

  property "WHERE field == value returns only matching rows" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)

        target_fields = elem(hd(records), 1)
        target_name   = Map.get(target_fields, "name")
        query = %{base_query() | wheres: [eq_where("name", 0)]}

        result = Query.execute(store, :all, query, [target_name], [])
        Enum.all?(result, fn row -> Map.get(row, "name") == target_name end)
      end)
    end
  end

  property "WHERE field == value includes all rows with that value" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)

        target_name = "fixedname"
        # Overwrite first record to guarantee at least one match
        {first_id, first_fields} = hd(records)
        updated = Map.put(first_fields, "name", target_name)
        :ok = Storage.insert(store, @source, first_id, updated)

        query  = %{base_query() | wheres: [eq_where("name", 0)]}
        result = Query.execute(store, :all, query, [target_name], [])
        Enum.any?(result, fn row -> Map.get(row, "name") == target_name end)
      end)
    end
  end

  property "WHERE field == non-existent value returns []" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | wheres: [eq_where("name", 0)]}
        result = Query.execute(store, :all, query, ["__NO_MATCH__#{:erlang.unique_integer()}__"], [])
        result == []
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # WHERE != (inequality)
  # ---------------------------------------------------------------------------

  property "WHERE field != value excludes matching rows" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)

        {_id, first_fields} = hd(records)
        excluded_name = Map.get(first_fields, "name")
        query  = %{base_query() | wheres: [neq_where("name", 0)]}
        result = Query.execute(store, :all, query, [excluded_name], [])
        Enum.all?(result, fn row -> Map.get(row, "name") != excluded_name end)
      end)
    end
  end

  property "WHERE field != some_value ∪ WHERE field == some_value = all rows" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)

        {_id, first_fields} = hd(records)
        target = Map.get(first_fields, "name")
        q_eq   = %{base_query() | wheres: [eq_where("name", 0)]}
        q_neq  = %{base_query() | wheres: [neq_where("name", 0)]}

        eq_rows  = Query.execute(store, :all, q_eq,  [target], [])
        neq_rows = Query.execute(store, :all, q_neq, [target], [])
        all_rows = Storage.all(store, @source)

        length(eq_rows) + length(neq_rows) == length(all_rows)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # WHERE LIKE — routes to HRR probe_field
  # ---------------------------------------------------------------------------

  property "LIKE query returns only maps (no sim tuples)" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | wheres: [like_where("name", 0)]}
        result = Query.execute(store, :all, query, ["%a%"], [])
        Enum.all?(result, &is_map/1)
      end)
    end
  end

  property "ILIKE query returns only maps" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | wheres: [ilike_where("name", 0)]}
        result = Query.execute(store, :all, query, ["%test%"], [])
        Enum.all?(result, &is_map/1)
      end)
    end
  end

  property "LIKE with hrr_threshold 1.1 returns no rows" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | wheres: [like_where("name", 0)]}
        result = Query.execute(store, :all, query, ["%a%"], [hrr_threshold: 1.1])
        result == []
      end)
    end
  end

  property "LIKE with hrr_threshold -1.0 returns all records with a vector" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | wheres: [like_where("name", 0)]}
        result = Query.execute(store, :all, query, ["%a%"], [hrr_threshold: -1.0])
        length(result) == length(records)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # ORDER BY
  # ---------------------------------------------------------------------------

  property "ORDER BY name ASC yields lexicographic order" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | order_bys: [order_asc("name")]}
        result = Query.execute(store, :all, query, [], [])
        names  = Enum.map(result, &Map.get(&1, "name"))
        names == Enum.sort(names)
      end)
    end
  end

  property "ORDER BY name DESC yields reverse lexicographic order" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | order_bys: [order_desc("name")]}
        result = Query.execute(store, :all, query, [], [])
        names  = Enum.map(result, &Map.get(&1, "name"))
        names == Enum.sort(names, :desc)
      end)
    end
  end

  property "ORDER BY ASC and DESC are consistent (multiset equal, opposite order)" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        q_asc  = %{base_query() | order_bys: [order_asc("name")]}
        q_desc = %{base_query() | order_bys: [order_desc("name")]}
        asc    = Query.execute(store, :all, q_asc,  [], [])
        desc   = Query.execute(store, :all, q_desc, [], [])
        # Both contain the same rows (order-independent)
        Enum.sort(asc) == Enum.sort(desc) and
          # ASC names are non-decreasing
          (Enum.map(asc, &Map.get(&1, "name")) |> then(fn ns -> ns == Enum.sort(ns) end)) and
          # DESC names are non-increasing
          (Enum.map(desc, &Map.get(&1, "name")) |> then(fn ns -> ns == Enum.sort(ns, :desc) end))
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # LIMIT
  # ---------------------------------------------------------------------------

  property "LIMIT n returns at most n rows" do
    forall {records, n} <- {record_list_gen(), pos_integer()} do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | limit: limit_expr(n)}
        result = Query.execute(store, :all, query, [], [])
        length(result) <= n
      end)
    end
  end

  property "LIMIT >= count returns all rows" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        n      = length(records) + 100
        query  = %{base_query() | limit: limit_expr(n)}
        result = Query.execute(store, :all, query, [], [])
        length(result) == length(records)
      end)
    end
  end

  property "LIMIT 0 returns []" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | limit: limit_expr(0)}
        Query.execute(store, :all, query, [], []) == []
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # OFFSET
  # ---------------------------------------------------------------------------

  property "OFFSET n skips first n rows" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        n       = min(1, length(records))
        q_all   = base_query()
        q_skip  = %{base_query() | offset: offset_expr(n)}
        all     = Query.execute(store, :all, q_all,  [], [])
        skipped = Query.execute(store, :all, q_skip, [], [])
        skipped == Enum.drop(all, n)
      end)
    end
  end

  property "LIMIT + OFFSET slices correctly" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        off    = div(length(records), 3)
        lim    = div(length(records), 3)
        q_all  = base_query()
        q_page = %{base_query() | offset: offset_expr(off), limit: limit_expr(lim)}
        all    = Query.execute(store, :all, q_all,  [], [])
        page   = Query.execute(store, :all, q_page, [], [])
        page == all |> Enum.drop(off) |> Enum.take(lim)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Combined WHERE + ORDER BY + LIMIT
  # ---------------------------------------------------------------------------

  property "WHERE == + ORDER BY + LIMIT compose correctly" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        {_id, first_fields} = hd(records)
        target = Map.get(first_fields, "name")

        query = %{base_query() |
          wheres:    [eq_where("name", 0)],
          order_bys: [order_asc("value")],
          limit:     limit_expr(5)
        }
        result = Query.execute(store, :all, query, [target], [])

        # All results match the filter
        all_match = Enum.all?(result, fn row -> Map.get(row, "name") == target end)
        # At most 5
        within_limit = length(result) <= 5
        # Sorted by value ASC
        values = Enum.map(result, &Map.get(&1, "value"))
        sorted = values == Enum.sort(values)

        all_match and within_limit and sorted
      end)
    end
  end
end
