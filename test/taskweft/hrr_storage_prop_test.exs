defmodule Taskweft.HRR.StoragePropTest do
  use ExUnit.Case, async: false
  use PropCheck

  @moduletag :property

  alias Taskweft.HRR.Storage

  @dim 64
  @table :hrr_storage_prop_table

  setup_all do
    path =
      Path.join(System.tmp_dir!(), "hrr_prop_test_#{:erlang.unique_integer([:positive])}.dets")

    {:ok, _} = Storage.open_table(@table, path)

    on_exit(fn ->
      Storage.close_table(@table)
      File.rm(path)
    end)

    %{path: path}
  end

  # ---------------------------------------------------------------------------
  # Generators
  # ---------------------------------------------------------------------------

  def source_gen, do: oneof(["users", "tasks", "items", "nodes"])

  def id_gen do
    let(n <- pos_integer(), do: "id-#{n}")
  end

  def field_name_gen do
    oneof(["name", "title", "role", "status", "category"])
  end

  def field_value_gen do
    let(
      chars <- non_empty(list(oneof([range(?a, ?z), range(?A, ?Z)]))),
      do: to_string(chars)
    )
  end

  def fields_gen do
    let pairs <- non_empty(list({field_name_gen(), field_value_gen()})) do
      Map.new(pairs)
    end
  end

  def record_list_gen do
    let pairs <- non_empty(list({id_gen(), fields_gen()})) do
      Enum.uniq_by(pairs, &elem(&1, 0))
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp with_storage(fun) do
    :dets.delete_all_objects(@table)
    store = {@table, @dim}

    try do
      fun.(store)
    after
      :dets.delete_all_objects(@table)
    end
  end

  # ---------------------------------------------------------------------------
  # insert / get roundtrip
  # ---------------------------------------------------------------------------

  property "inserted record can be retrieved by id" do
    forall {source, id, fields} <- {source_gen(), id_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, source, id, fields)
        retrieved = Storage.get(store, source, id)
        retrieved == fields
      end)
    end
  end

  property "get on unknown id returns nil" do
    forall source <- source_gen() do
      with_storage(fn store ->
        Storage.get(store, source, "no-such-id-#{:erlang.unique_integer()}") == nil
      end)
    end
  end

  property "get on wrong source returns nil" do
    forall {id, fields} <- {id_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, "source_a", id, fields)
        Storage.get(store, "source_b", id) == nil
      end)
    end
  end

  property "re-inserting same id replaces the record" do
    forall {source, id, fields1, fields2} <- {source_gen(), id_gen(), fields_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, source, id, fields1)
        :ok = Storage.insert(store, source, id, fields2)
        Storage.get(store, source, id) == fields2
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # all/2
  # ---------------------------------------------------------------------------

  property "all/2 returns every inserted record" do
    forall {source, records} <- {source_gen(), record_list_gen()} do
      with_storage(fn store ->
        Enum.each(records, fn {id, fields} ->
          :ok = Storage.insert(store, source, id, fields)
        end)

        all = Storage.all(store, source)
        Enum.all?(records, fn {_id, fields} -> fields in all end)
      end)
    end
  end

  property "all/2 count equals number of distinct inserted ids" do
    forall {source, records} <- {source_gen(), record_list_gen()} do
      with_storage(fn store ->
        Enum.each(records, fn {id, fields} ->
          :ok = Storage.insert(store, source, id, fields)
        end)

        length(Storage.all(store, source)) == length(records)
      end)
    end
  end

  property "all/2 returns empty list for unknown source" do
    with_storage(fn store ->
      Storage.all(store, "no_such_source_#{:erlang.unique_integer()}") == []
    end)
  end

  property "all/2 does not cross source boundaries" do
    forall {id, fields} <- {id_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, "src_x", id, fields)
        Storage.all(store, "src_y") == []
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # delete/3
  # ---------------------------------------------------------------------------

  property "deleted record is gone from get/3" do
    forall {source, id, fields} <- {source_gen(), id_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, source, id, fields)
        :ok = Storage.delete(store, source, id)
        Storage.get(store, source, id) == nil
      end)
    end
  end

  property "deleted record is gone from all/2" do
    forall {source, records} <- {source_gen(), record_list_gen()} do
      with_storage(fn store ->
        Enum.each(records, fn {id, fields} ->
          :ok = Storage.insert(store, source, id, fields)
        end)

        {del_id, _del_fields} = hd(records)
        :ok = Storage.delete(store, source, del_id)

        all = Storage.all(store, source)

        not Enum.any?(all, fn _r -> Storage.get(store, source, del_id) != nil end) and
          length(all) == length(records) - 1
      end)
    end
  end

  property "deleting a non-existent id returns :ok" do
    forall source <- source_gen() do
      with_storage(fn store ->
        :ok = Storage.delete(store, source, "ghost-#{:erlang.unique_integer()}")
        true
      end)
    end
  end

  property "insert after delete restores the record" do
    forall {source, id, fields1, fields2} <- {source_gen(), id_gen(), fields_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, source, id, fields1)
        :ok = Storage.delete(store, source, id)
        :ok = Storage.insert(store, source, id, fields2)
        Storage.get(store, source, id) == fields2
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # probe_field/5 — HRR similarity ranking
  # ---------------------------------------------------------------------------

  property "probe_field returns [{sim, map}] tuples" do
    forall {source, id, fields} <- {source_gen(), id_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, source, id, fields)
        field = hd(Map.keys(fields))
        query = Map.get(fields, field)
        results = Storage.probe_field(store, source, field, query)

        Enum.all?(results, fn
          {sim, map} -> is_float(sim) and is_map(map)
          _ -> false
        end)
      end)
    end
  end

  property "probe_field similarity scores are in [-1.0, 1.0]" do
    forall {source, id, fields} <- {source_gen(), id_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, source, id, fields)
        field = hd(Map.keys(fields))
        query = Map.get(fields, field)

        Storage.probe_field(store, source, field, query)
        |> Enum.all?(fn {sim, _} -> sim >= -1.0 and sim <= 1.0 end)
      end)
    end
  end

  property "probe_field results are sorted descending by similarity" do
    forall {source, records} <- {source_gen(), record_list_gen()} do
      with_storage(fn store ->
        Enum.each(records, fn {id, fields} ->
          :ok = Storage.insert(store, source, id, fields)
        end)

        results = Storage.probe_field(store, source, "name", "alice", threshold: -1.0)
        sims = Enum.map(results, fn {sim, _} -> sim end)
        sims == Enum.sort(sims, :desc)
      end)
    end
  end

  property "probe_field threshold filters out low-similarity records" do
    forall {source, records} <- {source_gen(), record_list_gen()} do
      with_storage(fn store ->
        Enum.each(records, fn {id, fields} ->
          :ok = Storage.insert(store, source, id, fields)
        end)

        threshold = 0.5
        results = Storage.probe_field(store, source, "name", "test", threshold: threshold)
        Enum.all?(results, fn {sim, _} -> sim >= threshold end)
      end)
    end
  end

  property "probe_field respects limit option" do
    forall {source, records} <- {source_gen(), record_list_gen()} do
      with_storage(fn store ->
        Enum.each(records, fn {id, fields} ->
          :ok = Storage.insert(store, source, id, fields)
        end)

        limit = max(1, div(length(records), 2))
        results = Storage.probe_field(store, source, "name", "x", limit: limit, threshold: -1.0)
        length(results) <= limit
      end)
    end
  end

  property "probe_field on empty source returns []" do
    forall source <- source_gen() do
      with_storage(fn store ->
        Storage.probe_field(store, source, "name", "anything") == []
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # probe_text/4 — full-record similarity
  # ---------------------------------------------------------------------------

  property "probe_text returns [{sim, map}] tuples" do
    forall {source, id, fields} <- {source_gen(), id_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, source, id, fields)
        results = Storage.probe_text(store, source, "test", threshold: -1.0)

        Enum.all?(results, fn
          {sim, map} -> is_float(sim) and is_map(map)
          _ -> false
        end)
      end)
    end
  end

  property "probe_text results sorted descending by similarity" do
    forall {source, records} <- {source_gen(), record_list_gen()} do
      with_storage(fn store ->
        Enum.each(records, fn {id, fields} ->
          :ok = Storage.insert(store, source, id, fields)
        end)

        results = Storage.probe_text(store, source, "alice", threshold: -1.0)
        sims = Enum.map(results, fn {sim, _} -> sim end)
        sims == Enum.sort(sims, :desc)
      end)
    end
  end

  property "probe_text threshold filters out low-similarity records" do
    forall {source, records} <- {source_gen(), record_list_gen()} do
      with_storage(fn store ->
        Enum.each(records, fn {id, fields} ->
          :ok = Storage.insert(store, source, id, fields)
        end)

        threshold = 0.5
        results = Storage.probe_text(store, source, "alpha", threshold: threshold)
        Enum.all?(results, fn {sim, _} -> sim >= threshold end)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # HRR record vector integrity
  # ---------------------------------------------------------------------------

  property "record_vector bytes are non-NaN when non-nil" do
    forall {source, id, fields} <- {source_gen(), id_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, source, id, fields)
        field = hd(Map.keys(fields))
        val = Map.get(fields, field)
        results = Storage.probe_field(store, source, field, val, threshold: -1.0)

        Enum.all?(results, fn {sim, _} ->
          is_float(sim) and not (sim != sim)
        end)
      end)
    end
  end

  property "self-probe of inserted record yields non-negative similarity" do
    forall {source, id, fields} <- {source_gen(), id_gen(), fields_gen()} do
      with_storage(fn store ->
        :ok = Storage.insert(store, source, id, fields)
        field = hd(Map.keys(fields))
        val = Map.get(fields, field)
        results = Storage.probe_field(store, source, field, val, threshold: -1.0)

        case Enum.find(results, fn {_, m} -> m == fields end) do
          {sim, _} -> sim >= 0.0
          nil -> false
        end
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Persistence across table close/reopen
  # ---------------------------------------------------------------------------

  property "records survive table close and reopen" do
    forall {source, id, fields} <- {source_gen(), id_gen(), fields_gen()} do
      tmp = :"hrr_persist_#{:erlang.unique_integer([:positive])}"

      path =
        Path.join(System.tmp_dir!(), "hrr_persist_#{:erlang.unique_integer([:positive])}.dets")

      {:ok, _} = Storage.open_table(tmp, path)
      :ok = Storage.insert({tmp, @dim}, source, id, fields)
      Storage.close_table(tmp)

      {:ok, _} = Storage.open_table(tmp, path)
      result = Storage.get({tmp, @dim}, source, id)
      Storage.close_table(tmp)
      File.rm(path)

      result == fields
    end
  end
end
