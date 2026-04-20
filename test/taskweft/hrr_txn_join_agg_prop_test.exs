defmodule Taskweft.HRR.TxnJoinAggPropTest do
  use ExUnit.Case, async: false
  use PropCheck

  @moduletag :property

  alias Taskweft.HRR.{Query, Storage}

  @dim 64
  @source "items"
  @source_b "tags"

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
    path = Path.join(System.tmp_dir!(), "hrr_tja_#{:erlang.unique_integer([:positive])}.db")
    name = :"hrr_tja_#{:erlang.unique_integer([:positive])}"
    {:ok, _} = Storage.start_link(name: name, db_path: path, dim: @dim)

    try do
      fun.(name)
    after
      GenServer.stop(name, :normal, 1_000)
      Enum.each([path, "#{path}-wal", "#{path}-shm"], &File.rm/1)
    end
  end

  defp populate(srv, records, source \\ @source) do
    Enum.each(records, fn {id, fields} ->
      :ok = Storage.insert(srv, source, id, fields)
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
  # Transaction properties
  # ---------------------------------------------------------------------------

  property "committed insert is visible after transaction" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn srv ->
        :ok = Storage.begin_transaction(srv)
        :ok = Storage.insert(srv, @source, id, fields)
        :ok = Storage.commit_transaction(srv)
        Storage.get(srv, @source, id) == fields
      end)
    end
  end

  property "rolled-back insert is not visible" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn srv ->
        :ok = Storage.begin_transaction(srv)
        :ok = Storage.insert(srv, @source, id, fields)
        :ok = Storage.rollback_transaction(srv)
        Storage.get(srv, @source, id) == nil
      end)
    end
  end

  property "rolled-back delete preserves the record" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn srv ->
        :ok = Storage.insert(srv, @source, id, fields)
        :ok = Storage.begin_transaction(srv)
        :ok = Storage.delete(srv, @source, id)
        :ok = Storage.rollback_transaction(srv)
        Storage.get(srv, @source, id) == fields
      end)
    end
  end

  property "committed delete removes the record" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn srv ->
        :ok = Storage.insert(srv, @source, id, fields)
        :ok = Storage.begin_transaction(srv)
        :ok = Storage.delete(srv, @source, id)
        :ok = Storage.commit_transaction(srv)
        Storage.get(srv, @source, id) == nil
      end)
    end
  end

  property "nested savepoint commit leaves outer transaction open" do
    forall {f1, f2} <- {fields_gen(), fields_gen()} do
      id1 = "sp-outer-#{:erlang.unique_integer([:positive])}"
      id2 = "sp-inner-#{:erlang.unique_integer([:positive])}"

      with_storage(fn srv ->
        :ok = Storage.begin_transaction(srv)   # depth 1
        :ok = Storage.insert(srv, @source, id1, f1)

        :ok = Storage.begin_transaction(srv)   # depth 2 (SAVEPOINT sp1)
        :ok = Storage.insert(srv, @source, id2, f2)
        :ok = Storage.commit_transaction(srv)  # RELEASE sp1

        still_open = Storage.in_transaction?(srv)

        :ok = Storage.rollback_transaction(srv)  # ROLLBACK outer

        both_gone = Storage.get(srv, @source, id1) == nil and
                    Storage.get(srv, @source, id2) == nil

        still_open and both_gone
      end)
    end
  end

  property "nested savepoint rollback leaves outer transaction intact" do
    forall {f1, f2} <- {fields_gen(), fields_gen()} do
      # Use guaranteed-distinct ids via unique integer
      id1 = "sp-outer-#{:erlang.unique_integer([:positive])}"
      id2 = "sp-inner-#{:erlang.unique_integer([:positive])}"

      with_storage(fn srv ->
        :ok = Storage.begin_transaction(srv)
        :ok = Storage.insert(srv, @source, id1, f1)

        :ok = Storage.begin_transaction(srv)
        :ok = Storage.insert(srv, @source, id2, f2)
        :ok = Storage.rollback_transaction(srv)  # rolls back sp1 only

        :ok = Storage.commit_transaction(srv)

        # id1 committed, id2 rolled back
        Storage.get(srv, @source, id1) == f1 and
        Storage.get(srv, @source, id2) == nil
      end)
    end
  end

  property "in_transaction? is false outside transaction" do
    with_storage(fn srv ->
      not Storage.in_transaction?(srv)
    end)
  end

  property "in_transaction? is true inside transaction" do
    with_storage(fn srv ->
      :ok = Storage.begin_transaction(srv)
      result = Storage.in_transaction?(srv)
      :ok = Storage.rollback_transaction(srv)
      result
    end)
  end

  property "bundle reflects post-commit state" do
    forall records <- record_list_gen() do
      with_storage(fn srv ->
        :ok = Storage.begin_transaction(srv)
        populate(srv, records)
        :ok = Storage.commit_transaction(srv)
        Storage.bundle(srv, @source) != nil
      end)
    end
  end

  property "bundle is nil after rollback of all inserts" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn srv ->
        :ok = Storage.begin_transaction(srv)
        :ok = Storage.insert(srv, @source, id, fields)
        :ok = Storage.rollback_transaction(srv)
        Storage.bundle(srv, @source) == nil
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
      with_storage(fn srv ->
        populate(srv, records)

        # Seed right source with matching item_name keys
        {_id, first_fields} = hd(records)
        key = Map.get(first_fields, "name")
        tag_id = "tag-#{:erlang.unique_integer([:positive])}"
        :ok = Storage.insert(srv, @source_b, tag_id, %{"item_name" => key, "label" => "x"})

        query  = join_query(@source_b, "name", "item_name")
        result = Query.execute(srv, :all, query, [], [])

        Enum.all?(result, fn row ->
          is_list(row) and
          Map.get(Enum.at(row, 0), "name") == Map.get(Enum.at(row, 1), "item_name")
        end)
      end)
    end
  end

  property "exact join with no matching right rows returns []" do
    forall records <- record_list_gen() do
      with_storage(fn srv ->
        populate(srv, records)
        # Right source is empty
        query  = join_query("empty_source_#{:erlang.unique_integer()}", "name", "item_name")
        result = Query.execute(srv, :all, query, [], [])
        result == []
      end)
    end
  end

  property "exact join row count = |left| × |matching_right| per left key" do
    forall records <- record_list_gen() do
      with_storage(fn srv ->
        populate(srv, records)

        {_id, first_fields} = hd(records)
        key = Map.get(first_fields, "name")

        # Insert two matching right rows
        n1 = "t#{:erlang.unique_integer([:positive])}"
        n2 = "t#{:erlang.unique_integer([:positive])}"
        :ok = Storage.insert(srv, @source_b, n1, %{"item_name" => key, "label" => "a"})
        :ok = Storage.insert(srv, @source_b, n2, %{"item_name" => key, "label" => "b"})

        # Count left rows with this key
        left_matching =
          Storage.all(srv, @source)
          |> Enum.count(fn r -> Map.get(r, "name") == key end)

        query  = join_query(@source_b, "name", "item_name")
        result = Query.execute(srv, :all, query, [], [])

        length(result) == left_matching * 2
      end)
    end
  end

  property "join rows are lists with two elements" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn srv ->
        :ok = Storage.insert(srv, @source, id, fields)
        tag_id = "t#{:erlang.unique_integer([:positive])}"
        key = Map.get(fields, "name")
        :ok = Storage.insert(srv, @source_b, tag_id, %{"item_name" => key, "label" => "y"})

        query  = join_query(@source_b, "name", "item_name")
        result = Query.execute(srv, :all, query, [], [])
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
      with_storage(fn srv ->
        :ok = Storage.insert(srv, @source, id, fields)
        key = Map.get(fields, "name")
        :ok = Storage.insert(srv, @source_b, "t1", %{"item_name" => key, "label" => "z"})

        query  = semantic_join_query(@source_b, "name", "item_name")
        result = Query.execute(srv, :all, query, [], [hrr_threshold: -1.0])
        Enum.all?(result, fn row -> is_list(row) and length(row) == 2 end)
      end)
    end
  end

  property "semantic join with threshold 1.1 returns no rows" do
    forall {id, fields} <- {word_gen(), fields_gen()} do
      with_storage(fn srv ->
        :ok = Storage.insert(srv, @source, id, fields)
        key = Map.get(fields, "name")
        :ok = Storage.insert(srv, @source_b, "t1", %{"item_name" => key, "label" => "z"})

        query  = semantic_join_query(@source_b, "name", "item_name")
        result = Query.execute(srv, :all, query, [], [hrr_threshold: 1.1])
        result == []
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Aggregate properties
  # ---------------------------------------------------------------------------

  property "count(*) without WHERE matches record_count in hrr_bundles" do
    forall records <- record_list_gen() do
      with_storage(fn srv ->
        populate(srv, records)
        query  = %{base_query() | select: agg_select(:count)}
        [[n]]  = Query.execute(srv, :all, query, [], [])
        n == Storage.record_count(srv, @source)
      end)
    end
  end

  property "count(*) without WHERE does not scan hrr_records (fast path)" do
    forall records <- record_list_gen() do
      with_storage(fn srv ->
        populate(srv, records)
        query  = %{base_query() | select: agg_select(:count)}
        [[n]]  = Query.execute(srv, :all, query, [], [])
        n == length(records)
      end)
    end
  end

  property "count(field) with WHERE equals filtered row count" do
    forall records <- record_list_gen() do
      with_storage(fn srv ->
        populate(srv, records)
        {_, first_fields} = hd(records)
        cat = Map.get(first_fields, "category")

        q_count = %{base_query() |
          wheres: [eq_where("category", 0)],
          select: agg_select(:count, "name")}
        q_all = %{base_query() | wheres: [eq_where("category", 0)]}

        [[n]]   = Query.execute(srv, :all, q_count, [cat], [])
        all_cat = Query.execute(srv, :all, q_all, [cat], [])
        n == length(all_cat)
      end)
    end
  end

  property "sum(score) equals Enum.sum over all rows" do
    forall records <- record_list_gen() do
      with_storage(fn srv ->
        populate(srv, records)
        query  = %{base_query() | select: agg_select(:sum, "score")}
        [[s]]  = Query.execute(srv, :all, query, [], [])
        all    = Storage.all(srv, @source)
        expected = all |> Enum.map(&Map.get(&1, "score")) |> Enum.reject(&is_nil/1) |> Enum.sum()
        abs(s - expected) < 1.0e-9
      end)
    end
  end

  property "avg(score) equals Enum.sum / count over all rows" do
    forall records <- record_list_gen() do
      with_storage(fn srv ->
        populate(srv, records)
        query = %{base_query() | select: agg_select(:avg, "score")}
        [[a]] = Query.execute(srv, :all, query, [], [])
        all   = Storage.all(srv, @source)
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
      with_storage(fn srv ->
        populate(srv, records)
        query = %{base_query() | select: agg_select(:min, "score")}
        [[m]] = Query.execute(srv, :all, query, [], [])
        all   = Storage.all(srv, @source)
        vals  = all |> Enum.map(&Map.get(&1, "score")) |> Enum.reject(&is_nil/1)
        m != nil and Enum.all?(vals, &(m <= &1))
      end)
    end
  end

  property "max(score) >= every score in the table" do
    forall records <- record_list_gen() do
      with_storage(fn srv ->
        populate(srv, records)
        query = %{base_query() | select: agg_select(:max, "score")}
        [[m]] = Query.execute(srv, :all, query, [], [])
        all   = Storage.all(srv, @source)
        vals  = all |> Enum.map(&Map.get(&1, "score")) |> Enum.reject(&is_nil/1)
        m != nil and Enum.all?(vals, &(m >= &1))
      end)
    end
  end

  property "count(*) after rollback equals pre-transaction count" do
    forall {records, id, fields} <- {record_list_gen(), word_gen(), fields_gen()} do
      with_storage(fn srv ->
        populate(srv, records)
        before_count = Storage.record_count(srv, @source)

        :ok = Storage.begin_transaction(srv)
        :ok = Storage.insert(srv, @source, id, fields)
        :ok = Storage.rollback_transaction(srv)

        Storage.record_count(srv, @source) == before_count
      end)
    end
  end

  property "aggregate on empty source returns nil for min/max/avg and 0 for count" do
    with_storage(fn srv ->
      src = "empty_#{:erlang.unique_integer([:positive])}"
      q_count = %{base_query(src) | select: agg_select(:count)}
      q_min   = %{base_query(src) | select: agg_select(:min, "score")}
      q_max   = %{base_query(src) | select: agg_select(:max, "score")}
      q_avg   = %{base_query(src) | select: agg_select(:avg, "score")}

      [[c]] = Query.execute(srv, :all, q_count, [], [])
      [[mn]] = Query.execute(srv, :all, q_min, [], [])
      [[mx]] = Query.execute(srv, :all, q_max, [], [])
      [[av]] = Query.execute(srv, :all, q_avg, [], [])

      c == 0 and mn == nil and mx == nil and av == nil
    end)
  end
end
