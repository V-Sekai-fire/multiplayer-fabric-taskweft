defmodule Taskweft.HRR.TxnJoinAggPropTest do
  use ExUnit.Case, async: false
  use PropCheck

  @moduletag :property

  alias Taskweft.HRR.{Query, Storage}

  @dim 64
  @source "items"
  @source_b "tags"
  @pool :hrr_txn_prop_pool

  setup_all do
    {:ok, _} = Taskweft.Test.DBHelpers.start_pool(@pool)
    Storage.ensure_schema!(@pool)
    on_exit(fn -> GenServer.stop(@pool) end)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Generators
  # ---------------------------------------------------------------------------

  def word_gen do
    let chars <- non_empty(list(oneof([range(?a, ?z)]))),
      do: to_string(chars)
  end

  def num_gen, do: let(n <- integer(1, 100), do: n * 1.0)

  def fields_gen do
    let {name, cat, score} <- {word_gen(), word_gen(), num_gen()} do
      %{"name" => name, "category" => cat, "score" => score}
    end
  end

  def tag_fields_gen(item_name) do
    let label <- word_gen() do
      %{"item_name" => item_name, "label" => label}
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
  # Setup helpers
  # ---------------------------------------------------------------------------

  defp with_storage(fun) do
    Postgrex.query!(@pool, "DELETE FROM hrr_records", [])
    Postgrex.query!(@pool, "DELETE FROM hrr_bundles", [])
    store = {@pool, @dim}
    try do
      fun.(store)
    after
      Postgrex.query!(@pool, "DELETE FROM hrr_records", [])
      Postgrex.query!(@pool, "DELETE FROM hrr_bundles", [])
    end
  end

  defp populate(store, records, source \\ @source) do
    Enum.each(records, fn {id, fields} ->
      :ok = Storage.insert(store, source, id, fields)
    end)
  end

  defp base_query(source \\ @source) do
    %{from: %{source: {source, nil}}, wheres: [], order_bys: [], joins: [],
      limit: nil, offset: nil, select: nil}
  end

  defp eq_where(field, idx) do
    %{expr: {:==, [], [field_ref(field), param_ref(idx)]}, params: [], op: :and}
  end

  defp field_ref(f) when is_atom(f),
    do: {{:., [], [{:&, [], [0]}, f]}, [], []}
  defp field_ref(f),
    do: field_ref(String.to_atom(f))

  defp param_ref(i), do: {:^, [], [i]}

  defp agg_select(fun, field \\ nil) do
    expr =
      if field,
        do: {fun, [], [field_ref(field)]},
        else: {fun, [], []}
    %{expr: expr}
  end

  # ---------------------------------------------------------------------------
  # Transaction helpers using Postgrex.transaction/3
  # ---------------------------------------------------------------------------

  # Run fun inside a Postgrex transaction, making the connection visible to
  # Storage calls via the process dictionary key {:taskweft_hrr_conn, pool}.
  defp with_txn({pool, _dim} = store, fun) do
    Postgrex.transaction(pool, fn conn ->
      Process.put({:taskweft_hrr_conn, pool}, conn)
      try do
        fun.(store)
      after
        Process.delete({:taskweft_hrr_conn, pool})
      end
    end)
  end

  defp in_transaction?({pool, _dim}) do
    Process.get({:taskweft_hrr_conn, pool}) != nil
  end

  # ---------------------------------------------------------------------------
  # Transaction properties
  # ---------------------------------------------------------------------------

  property "committed insert is visible after transaction" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn store ->
        {:ok, :ok} = with_txn(store, fn store ->
          Storage.insert(store, @source, id, fields)
        end)
        Storage.get(store, @source, id) == fields
      end)
    end
  end

  property "rolled-back insert is not visible" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn {pool, _dim} = store ->
        Postgrex.transaction(pool, fn conn ->
          Process.put({:taskweft_hrr_conn, pool}, conn)
          try do
            Storage.insert(store, @source, id, fields)
            # Postgrex.rollback signals rollback without raising, so
            # Postgrex.transaction returns {:error, :rollback} cleanly.
            Postgrex.rollback(conn, :rollback)
          after
            Process.delete({:taskweft_hrr_conn, pool})
          end
        end)
        Storage.get(store, @source, id) == nil
      end)
    end
  end

  property "rolled-back delete preserves the record" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn {pool, _dim} = store ->
        :ok = Storage.insert(store, @source, id, fields)
        Postgrex.transaction(pool, fn conn ->
          Process.put({:taskweft_hrr_conn, pool}, conn)
          try do
            Storage.delete(store, @source, id)
            Postgrex.rollback(conn, :rollback)
          after
            Process.delete({:taskweft_hrr_conn, pool})
          end
        end)
        Storage.get(store, @source, id) == fields
      end)
    end
  end

  property "committed delete removes the record" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, @source, id, fields)
        {:ok, :ok} = with_txn(store, fn store ->
          Storage.delete(store, @source, id)
        end)
        Storage.get(store, @source, id) == nil
      end)
    end
  end

  property "in_transaction? is false outside transaction" do
    with_storage(fn store ->
      not in_transaction?(store)
    end)
  end

  property "in_transaction? is true inside transaction" do
    with_storage(fn {pool, _dim} = store ->
      result_ref = :atomics.new(1, [])
      Postgrex.transaction(pool, fn conn ->
        Process.put({:taskweft_hrr_conn, pool}, conn)
        try do
          :atomics.put(result_ref, 1, if(in_transaction?(store), do: 1, else: 0))
        after
          Process.delete({:taskweft_hrr_conn, pool})
        end
      end)
      :atomics.get(result_ref, 1) == 1
    end)
  end

  property "bundle reflects post-commit state" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        {:ok, _} = with_txn(store, fn store ->
          populate(store, records)
        end)
        Storage.bundle(store, @source) != nil
      end)
    end
  end

  property "bundle is nil after rollback of all inserts" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn {pool, _dim} = store ->
        Postgrex.transaction(pool, fn conn ->
          Process.put({:taskweft_hrr_conn, pool}, conn)
          try do
            Storage.insert(store, @source, id, fields)
            Postgrex.rollback(conn, :rollback)
          after
            Process.delete({:taskweft_hrr_conn, pool})
          end
        end)
        Storage.bundle(store, @source) == nil
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Join properties (exact)
  # ---------------------------------------------------------------------------

  defp join_query(right_source, left_field, right_field) do
    join_expr = %{
      qual:   :inner,
      source: {right_source, nil},
      on:     %{expr: {:==, [],
                  [{{:., [], [{:&, [], [0]}, String.to_atom(left_field)]}, [], []},
                   {{:., [], [{:&, [], [1]}, String.to_atom(right_field)]}, [], []}]}}
    }
    %{base_query() | joins: [join_expr]}
  end

  property "exact join returns only rows with matching keys" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)

        # Seed right source with matching item_name keys
        {_id, first_fields} = hd(records)
        key = Map.get(first_fields, "name")
        tag_id = "tag-#{:erlang.unique_integer([:positive])}"
        :ok = Storage.insert(store, @source_b, tag_id, %{"item_name" => key, "label" => "x"})

        query  = join_query(@source_b, "name", "item_name")
        result = Query.execute(store, :all, query, [], [])

        Enum.all?(result, fn row ->
          is_list(row) and
          Map.get(Enum.at(row, 0), "name") == Map.get(Enum.at(row, 1), "item_name")
        end)
      end)
    end
  end

  property "exact join with no matching right rows returns []" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        # Right source is empty
        query  = join_query("empty_source_#{:erlang.unique_integer()}", "name", "item_name")
        result = Query.execute(store, :all, query, [], [])
        result == []
      end)
    end
  end

  property "exact join row count = |left| × |matching_right| per left key" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)

        {_id, first_fields} = hd(records)
        key = Map.get(first_fields, "name")

        # Insert two matching right rows
        n1 = "t#{:erlang.unique_integer([:positive])}"
        n2 = "t#{:erlang.unique_integer([:positive])}"
        :ok = Storage.insert(store, @source_b, n1, %{"item_name" => key, "label" => "a"})
        :ok = Storage.insert(store, @source_b, n2, %{"item_name" => key, "label" => "b"})

        # Count left rows with this key
        left_matching =
          Storage.all(store, @source)
          |> Enum.count(fn r -> Map.get(r, "name") == key end)

        query  = join_query(@source_b, "name", "item_name")
        result = Query.execute(store, :all, query, [], [])

        length(result) == left_matching * 2
      end)
    end
  end

  property "join rows are lists with two elements" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, @source, id, fields)
        tag_id = "t#{:erlang.unique_integer([:positive])}"
        key = Map.get(fields, "name")
        :ok = Storage.insert(store, @source_b, tag_id, %{"item_name" => key, "label" => "y"})

        query  = join_query(@source_b, "name", "item_name")
        result = Query.execute(store, :all, query, [], [])
        Enum.all?(result, fn row -> is_list(row) and length(row) == 2 end)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Semantic join properties
  # ---------------------------------------------------------------------------

  defp semantic_join_query(right_source, left_field, right_field) do
    join_expr = %{
      qual:   :inner,
      source: {right_source, nil},
      on:     %{expr: {:like, [],
                  [{{:., [], [{:&, [], [0]}, String.to_atom(left_field)]}, [], []},
                   {{:., [], [{:&, [], [1]}, String.to_atom(right_field)]}, [], []}]}}
    }
    %{base_query() | joins: [join_expr]}
  end

  property "semantic join returns list rows" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, @source, id, fields)
        key = Map.get(fields, "name")
        :ok = Storage.insert(store, @source_b, "t1", %{"item_name" => key, "label" => "z"})

        query  = semantic_join_query(@source_b, "name", "item_name")
        result = Query.execute(store, :all, query, [], [hrr_threshold: -1.0])
        Enum.all?(result, fn row -> is_list(row) and length(row) == 2 end)
      end)
    end
  end

  property "semantic join with threshold 1.1 returns no rows" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, @source, id, fields)
        key = Map.get(fields, "name")
        :ok = Storage.insert(store, @source_b, "t1", %{"item_name" => key, "label" => "z"})

        query  = semantic_join_query(@source_b, "name", "item_name")
        result = Query.execute(store, :all, query, [], [hrr_threshold: 1.1])
        result == []
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Aggregate properties
  # ---------------------------------------------------------------------------

  property "count(*) without WHERE matches record_count in hrr_bundles" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | select: agg_select(:count)}
        [[n]]  = Query.execute(store, :all, query, [], [])
        n == Storage.record_count(store, @source)
      end)
    end
  end

  property "count(*) without WHERE does not scan hrr_records (fast path)" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | select: agg_select(:count)}
        [[n]]  = Query.execute(store, :all, query, [], [])
        n == length(records)
      end)
    end
  end

  property "count(field) with WHERE equals filtered row count" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        {_, first_fields} = hd(records)
        cat = Map.get(first_fields, "category")

        q_count = %{base_query() |
          wheres: [eq_where("category", 0)],
          select: agg_select(:count, "name")}
        q_all = %{base_query() | wheres: [eq_where("category", 0)]}

        [[n]]   = Query.execute(store, :all, q_count, [cat], [])
        all_cat = Query.execute(store, :all, q_all, [cat], [])
        n == length(all_cat)
      end)
    end
  end

  property "sum(score) equals Enum.sum over all rows" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query  = %{base_query() | select: agg_select(:sum, "score")}
        [[s]]  = Query.execute(store, :all, query, [], [])
        all    = Storage.all(store, @source)
        expected = all |> Enum.map(&Map.get(&1, "score")) |> Enum.reject(&is_nil/1) |> Enum.sum()
        abs(s - expected) < 1.0e-9
      end)
    end
  end

  property "avg(score) equals Enum.sum / count over all rows" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query = %{base_query() | select: agg_select(:avg, "score")}
        [[a]] = Query.execute(store, :all, query, [], [])
        all   = Storage.all(store, @source)
        vals  = all |> Enum.map(&Map.get(&1, "score")) |> Enum.reject(&is_nil/1)
        expected = if vals == [], do: nil, else: Enum.sum(vals) / length(vals)
        case {a, expected} do
          {nil, nil} -> true
          {v, e}     -> abs(v - e) < 1.0e-9
        end
      end)
    end
  end

  property "min(score) <= every score in the table" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query = %{base_query() | select: agg_select(:min, "score")}
        [[m]] = Query.execute(store, :all, query, [], [])
        all   = Storage.all(store, @source)
        vals  = all |> Enum.map(&Map.get(&1, "score")) |> Enum.reject(&is_nil/1)
        m != nil and Enum.all?(vals, &(m <= &1))
      end)
    end
  end

  property "max(score) >= every score in the table" do
    forall records <- record_list_gen() do
      with_storage(fn store ->
        populate(store, records)
        query = %{base_query() | select: agg_select(:max, "score")}
        [[m]] = Query.execute(store, :all, query, [], [])
        all   = Storage.all(store, @source)
        vals  = all |> Enum.map(&Map.get(&1, "score")) |> Enum.reject(&is_nil/1)
        m != nil and Enum.all?(vals, &(m >= &1))
      end)
    end
  end

  property "count(*) after rollback equals pre-transaction count" do
    forall {records, id, fields} <- {record_list_gen(), word_gen(), fields_gen()} do
      with_storage(fn {pool, _dim} = store ->
        populate(store, records)
        before_count = Storage.record_count(store, @source)

        Postgrex.transaction(pool, fn conn ->
          Process.put({:taskweft_hrr_conn, pool}, conn)
          try do
            Storage.insert(store, @source, id, fields)
            Postgrex.rollback(conn, :rollback)
          after
            Process.delete({:taskweft_hrr_conn, pool})
          end
        end)

        Storage.record_count(store, @source) == before_count
      end)
    end
  end

  property "aggregate on empty source returns nil for min/max/avg and 0 for count" do
    with_storage(fn store ->
      src = "empty_#{:erlang.unique_integer([:positive])}"
      q_count = %{base_query(src) | select: agg_select(:count)}
      q_min   = %{base_query(src) | select: agg_select(:min, "score")}
      q_max   = %{base_query(src) | select: agg_select(:max, "score")}
      q_avg   = %{base_query(src) | select: agg_select(:avg, "score")}

      [[c]] = Query.execute(store, :all, q_count, [], [])
      [[mn]] = Query.execute(store, :all, q_min, [], [])
      [[mx]] = Query.execute(store, :all, q_max, [], [])
      [[av]] = Query.execute(store, :all, q_avg, [], [])

      c == 0 and mn == nil and mx == nil and av == nil
    end)
  end
end
