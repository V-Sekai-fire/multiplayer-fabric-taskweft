defmodule Taskweft.HRR.Storage do
  @moduledoc """
  DETS-backed storage layer for the HRR adapter.

  ## Store type

      @type store :: {table :: atom(), dim :: pos_integer()}

  `table` is an atom registered with `:dets.open_file/2`, managed by
  `Taskweft.HRR.TableServer`.  Records and bundles share one table with
  prefixed keys:

      {:rec, source, id}  → {fields_map, vec_or_nil}
      {:bnd, source}      → {bundle_vec, record_count}

  ## HRR encoding per INSERT

      role(field)   = hrr_encode_atom("role_<field>", dim)   → phases → bytes
      val(v)        = hrr_encode_text(v, dim)                 → phases → bytes
      field_binding = hrr_bind(role(field), val(v))
      record_vector = hrr_bundle([field_binding, ...])
  """

  @type store :: {atom(), pos_integer()}

  @doc "No-op — DETS tables require no SQL schema."
  @spec ensure_schema!(atom()) :: :ok
  def ensure_schema!(_table), do: :ok

  @doc "Open a DETS table at *path* registered under *name*."
  @spec open_table(atom(), Path.t()) :: {:ok, atom()} | {:error, term()}
  def open_table(name, path) do
    File.mkdir_p!(Path.dirname(path))
    :dets.open_file(name, [{:file, String.to_charlist(path)}, {:type, :set}])
  end

  @doc "Close a DETS table."
  @spec close_table(atom()) :: :ok | {:error, term()}
  def close_table(name), do: :dets.close(name)

  # ---------------------------------------------------------------------------
  # Public API – CRUD
  # ---------------------------------------------------------------------------

  @doc "Insert or replace a record, then rebuild the source bundle."
  @spec insert(store(), String.t(), term(), map()) :: :ok
  def insert({table, dim}, source, id, fields_map) do
    vec = build_record_vector(fields_map, dim)
    :ok = :dets.insert(table, {{:rec, source, to_string(id)}, fields_map, vec})
    rebuild_bundle({table, dim}, source)
    :ok
  end

  @doc "Fetch a single record by source and id, or nil if not found."
  @spec get(store(), String.t(), term()) :: map() | nil
  def get({table, _dim}, source, id) do
    case :dets.lookup(table, {:rec, source, to_string(id)}) do
      [{{:rec, _s, _id}, fields, _vec}] -> fields
      [] -> nil
    end
  end

  @doc "Delete a record by source and id, then rebuild the source bundle."
  @spec delete(store(), String.t(), term()) :: :ok
  def delete({table, _dim} = store, source, id) do
    :dets.delete(table, {:rec, source, to_string(id)})
    rebuild_bundle(store, source)
    :ok
  end

  @doc "Return all records for a source as a list of field maps."
  @spec all(store(), String.t()) :: [map()]
  def all({table, _dim}, source) do
    :dets.match_object(table, {{:rec, source, :_}, :_, :_})
    |> Enum.map(fn {_key, fields, _vec} -> fields end)
  end

  # ---------------------------------------------------------------------------
  # Public API – metadata
  # ---------------------------------------------------------------------------

  @doc "Return the source-level bundle vector (bytes), or nil if the source is empty."
  @spec bundle(store(), String.t()) :: binary() | nil
  def bundle({table, _dim}, source) do
    case :dets.lookup(table, {:bnd, source}) do
      [{{:bnd, _s}, vec, _count}] -> vec
      [] -> nil
    end
  end

  @doc "Return the committed record count for *source*. Returns 0 for unknown sources."
  @spec record_count(store(), String.t()) :: non_neg_integer()
  def record_count({table, _dim}, source) do
    case :dets.lookup(table, {:bnd, source}) do
      [{{:bnd, _s}, _vec, count}] -> count
      [] -> 0
    end
  end

  # ---------------------------------------------------------------------------
  # Public API – probe
  # ---------------------------------------------------------------------------

  @doc """
  Rank all records in *source* by cosine similarity of their record_vector
  to `encode(field, query_text)`.

  Options:
  - `:threshold` – minimum similarity to include (default `0.0`)
  - `:limit`     – max results (default `50`)
  """
  @spec probe_field(store(), String.t(), atom() | String.t(), String.t(), keyword()) ::
          [{float(), map()}]
  def probe_field({table, dim}, source, field, query_text, opts \\ []) do
    threshold = Keyword.get(opts, :threshold, 0.0)
    limit = Keyword.get(opts, :limit, 50)

    rows =
      :dets.match_object(table, {{:rec, source, :_}, :_, :_})
      |> Enum.filter(fn {_key, _fields, vec} -> not is_nil(vec) end)

    role_bytes =
      Taskweft.NIF.hrr_encode_atom("role_#{field}", dim)
      |> Taskweft.NIF.hrr_phases_to_bytes()

    query_phases = Taskweft.NIF.hrr_encode_text(query_text, dim)

    rows
    |> Enum.map(fn {_key, fields, vec} ->
      unbound = Taskweft.NIF.hrr_unbind(vec, role_bytes)

      sim =
        Taskweft.NIF.hrr_similarity(Taskweft.NIF.hrr_bytes_to_phases(unbound, 0), query_phases)

      {sim, fields}
    end)
    |> Enum.filter(fn {sim, _} -> sim >= threshold end)
    |> Enum.sort_by(fn {sim, _} -> -sim end)
    |> Enum.take(limit)
  rescue
    _ -> []
  end

  @doc """
  Rank all records in *source* by cosine similarity of their record_vector
  to `encode_text(query_text)`.
  """
  @spec probe_text(store(), String.t(), String.t(), keyword()) ::
          [{float(), map()}]
  def probe_text({table, dim}, source, query_text, opts \\ []) do
    threshold = Keyword.get(opts, :threshold, 0.0)
    limit = Keyword.get(opts, :limit, 50)

    rows =
      :dets.match_object(table, {{:rec, source, :_}, :_, :_})
      |> Enum.filter(fn {_key, _fields, vec} -> not is_nil(vec) end)

    query_phases = Taskweft.NIF.hrr_encode_text(query_text, dim)

    rows
    |> Enum.map(fn {_key, fields, vec} ->
      sim =
        Taskweft.NIF.hrr_similarity(
          Taskweft.NIF.hrr_bytes_to_phases(vec, 0),
          query_phases
        )

      {sim, fields}
    end)
    |> Enum.filter(fn {sim, _} -> sim >= threshold end)
    |> Enum.sort_by(fn {sim, _} -> -sim end)
    |> Enum.take(limit)
  rescue
    _ -> []
  end

  @doc """
  For each record in *source*, unbind `role(join_field)` from its
  record_vector and return `[{probe_bytes, fields_map}]`.
  """
  @spec vectors_for_join(store(), String.t(), String.t()) ::
          [{binary(), map()}]
  def vectors_for_join({table, dim}, source, join_field) do
    rows =
      :dets.match_object(table, {{:rec, source, :_}, :_, :_})
      |> Enum.filter(fn {_key, _fields, vec} -> not is_nil(vec) end)

    role_bytes =
      Taskweft.NIF.hrr_encode_atom("role_#{join_field}", dim)
      |> Taskweft.NIF.hrr_phases_to_bytes()

    Enum.map(rows, fn {_key, fields, vec} ->
      probe = Taskweft.NIF.hrr_unbind(vec, role_bytes)
      {probe, fields}
    end)
  rescue
    _ -> []
  end

  # ---------------------------------------------------------------------------
  # Private: HRR vector construction
  # ---------------------------------------------------------------------------

  defp build_record_vector(fields_map, dim) do
    bindings =
      Enum.map(fields_map, fn {field, value} ->
        role_bytes =
          Taskweft.NIF.hrr_encode_atom("role_#{field}", dim)
          |> Taskweft.NIF.hrr_phases_to_bytes()

        val_bytes =
          Taskweft.NIF.hrr_encode_text(to_string(value), dim)
          |> Taskweft.NIF.hrr_phases_to_bytes()

        Taskweft.NIF.hrr_bind(role_bytes, val_bytes)
      end)

    case bindings do
      [] -> nil
      [one] -> one
      many -> Taskweft.NIF.hrr_bundle(many)
    end
  rescue
    _ -> nil
  end

  # ---------------------------------------------------------------------------
  # Private: bundle maintenance
  # ---------------------------------------------------------------------------

  defp rebuild_bundle({table, _dim}, source) do
    vecs =
      :dets.match_object(table, {{:rec, source, :_}, :_, :_})
      |> Enum.map(fn {_key, _fields, vec} -> vec end)
      |> Enum.reject(&is_nil/1)

    case vecs do
      [] ->
        :dets.delete(table, {:bnd, source})

      [one] ->
        :dets.insert(table, {{:bnd, source}, one, 1})

      many ->
        :dets.insert(table, {{:bnd, source}, Taskweft.NIF.hrr_bundle(many), length(many)})
    end
  rescue
    _ -> :ok
  end
end
