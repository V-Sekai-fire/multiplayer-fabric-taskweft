defmodule Taskweft.HRR.Storage do
  @moduledoc """
  PostgreSQL-backed storage layer for the HRR Ecto adapter.

  ## Data model

  Two tables hold all data regardless of Ecto schema source:

      hrr_records  – one row per record; fields serialised as JSON;
                     record_vector = bundle of bind(role(field), encode(value))
                     for every field in the row.

      hrr_bundles  – one row per source; vector = superposition of all
                     record_vectors in that source.  Kept up to date on
                     every insert/delete.  Exposed via `bundle/2` and
                     used for fast COUNT(*) via `record_count/2`.

  ## Store type

  Functions take a `{pool, dim}` 2-tuple as their first argument:

      @type store :: {GenServer.server(), pos_integer()}

  The pool is a Postgrex connection pool pid or registered name.  Inside a
  `Postgrex.transaction/3` callback the current connection is stored in the
  process dictionary under `{:taskweft_hrr_conn, pool}` so all Storage calls
  automatically participate in the open transaction.

  ## HRR encoding per INSERT

      role(field)   = hrr_encode_atom("role_<field>", dim)   → bytes
      val(v)        = hrr_phases_to_bytes(hrr_encode_text(v, dim))
      field_binding = hrr_bind(role(field), val(v))
      record_vector = hrr_bundle([field_binding, ...])

  ## Probe operations

  `probe_field(store, source, field, query_text, opts)` – unbinds the role
  from every record_vector and ranks by cosine similarity to encode(query).

  `probe_text(store, source, query_text, opts)` – ranks by cosine similarity
  between encode(query) and each record_vector (full scan).

  Both return `[{similarity_float, fields_map}]` sorted descending.

  ## Transactions

  Transactions are handled at the Adapter level via `Postgrex.transaction/3`.
  Storage functions automatically use the transaction connection when one is
  active (see process-dictionary lookup in `query_all/3`).
  """

  @type store :: {GenServer.server(), pos_integer()}

  @schema [
    """
    CREATE TABLE IF NOT EXISTS hrr_records (
      source        TEXT    NOT NULL,
      id            TEXT    NOT NULL,
      fields_json   TEXT    NOT NULL,
      record_vector BYTEA,
      PRIMARY KEY (source, id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hrr_bundles (
      source        TEXT PRIMARY KEY,
      bundle_vector BYTEA    NOT NULL,
      record_count  INTEGER DEFAULT 0,
      updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
  ]

  # ---------------------------------------------------------------------------
  # Schema management
  # ---------------------------------------------------------------------------

  @doc """
  Create the hrr_records and hrr_bundles tables if they do not exist.
  Accepts either a Postgrex connection pool name/pid or a raw connection.
  """
  @spec ensure_schema!(GenServer.server()) :: :ok
  def ensure_schema!(conn_or_pool) do
    Enum.each(@schema, fn sql ->
      Postgrex.query!(conn_or_pool, String.trim(sql), [])
    end)
    :ok
  end

  @doc """
  Called by Postgrex's `:after_connect` hook so the schema exists as soon as
  the first connection is established.
  """
  @spec ensure_schema_on_connect!(DBConnection.t()) :: :ok
  def ensure_schema_on_connect!(conn), do: ensure_schema!(conn)

  # ---------------------------------------------------------------------------
  # Public API – CRUD
  # ---------------------------------------------------------------------------

  @doc "Insert or replace a record, then rebuild the source bundle."
  @spec insert(store(), String.t(), term(), map()) :: :ok
  def insert({pool, dim} = store, source, id, fields_map) do
    json = Jason.encode!(fields_map)
    vec  = build_record_vector(fields_map, dim)

    query_all(store,
      """
      INSERT INTO hrr_records (source, id, fields_json, record_vector)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (source, id) DO UPDATE SET
        fields_json   = EXCLUDED.fields_json,
        record_vector = EXCLUDED.record_vector
      """,
      [source, to_string(id), json, vec])

    rebuild_bundle(store, source)
    :ok
  end

  @doc "Fetch a single record by source and id, or nil if not found."
  @spec get(store(), String.t(), term()) :: map() | nil
  def get(store, source, id) do
    case query_all(store,
           "SELECT fields_json FROM hrr_records WHERE source = $1 AND id = $2",
           [source, to_string(id)]) do
      [row | _] -> Jason.decode!(row["fields_json"])
      []        -> nil
    end
  end

  @doc "Delete a record by source and id, then rebuild the source bundle."
  @spec delete(store(), String.t(), term()) :: :ok
  def delete(store, source, id) do
    query_all(store,
      "DELETE FROM hrr_records WHERE source = $1 AND id = $2",
      [source, to_string(id)])
    rebuild_bundle(store, source)
    :ok
  end

  @doc "Return all records for a source as a list of field maps."
  @spec all(store(), String.t()) :: [map()]
  def all(store, source) do
    query_all(store,
      "SELECT fields_json FROM hrr_records WHERE source = $1",
      [source])
    |> Enum.map(&Jason.decode!(&1["fields_json"]))
  end

  # ---------------------------------------------------------------------------
  # Public API – metadata
  # ---------------------------------------------------------------------------

  @doc "Return the current source-level bundle vector (bytes), or nil if the source is empty."
  @spec bundle(store(), String.t()) :: binary() | nil
  def bundle(store, source) do
    case query_all(store,
           "SELECT bundle_vector FROM hrr_bundles WHERE source = $1",
           [source]) do
      [row | _] -> row["bundle_vector"]
      []        -> nil
    end
  end

  @doc """
  Return the committed record count for *source* directly from `hrr_bundles`
  without scanning `hrr_records`.  Returns 0 for unknown sources.

  Used by the Query layer for O(1) COUNT(*) without a WHERE clause.
  """
  @spec record_count(store(), String.t()) :: non_neg_integer()
  def record_count(store, source) do
    case query_all(store,
           "SELECT record_count FROM hrr_bundles WHERE source = $1",
           [source]) do
      [row | _] -> row["record_count"] || 0
      []        -> 0
    end
  end

  # ---------------------------------------------------------------------------
  # Public API – probe
  # ---------------------------------------------------------------------------

  @doc """
  Rank all records in *source* by cosine similarity of their `record_vector`
  to `encode(field, query_text)`.

  Options:
  - `:threshold` – minimum similarity to include (default `0.0`)
  - `:limit`     – max results (default `50`)

  Returns `[{similarity, fields_map}]` sorted descending.
  """
  @spec probe_field(store(), String.t(), atom() | String.t(), String.t(), keyword()) ::
          [{float(), map()}]
  def probe_field({_pool, dim} = store, source, field, query_text, opts \\ []) do
    threshold = Keyword.get(opts, :threshold, 0.0)
    limit     = Keyword.get(opts, :limit, 50)

    rows = query_all(store,
      "SELECT fields_json, record_vector FROM hrr_records WHERE source = $1 AND record_vector IS NOT NULL",
      [source])

    role_bytes   = Taskweft.NIF.hrr_encode_atom("role_#{field}", dim)
                   |> Taskweft.NIF.hrr_phases_to_bytes()
    query_phases = Taskweft.NIF.hrr_encode_text(query_text, dim)

    rows
    |> Enum.map(fn row ->
      unbound = Taskweft.NIF.hrr_unbind(row["record_vector"], role_bytes)
      sim     = Taskweft.NIF.hrr_similarity(Taskweft.NIF.hrr_bytes_to_phases(unbound, 0), query_phases)
      {sim, Jason.decode!(row["fields_json"])}
    end)
    |> Enum.filter(fn {sim, _} -> sim >= threshold end)
    |> Enum.sort_by(fn {sim, _} -> -sim end)
    |> Enum.take(limit)
  rescue
    _ -> []
  end

  @doc """
  Rank all records in *source* by cosine similarity of their `record_vector`
  to `encode_text(query_text)`.

  Same options and return type as `probe_field/5`.
  """
  @spec probe_text(store(), String.t(), String.t(), keyword()) ::
          [{float(), map()}]
  def probe_text({_pool, dim} = store, source, query_text, opts \\ []) do
    threshold = Keyword.get(opts, :threshold, 0.0)
    limit     = Keyword.get(opts, :limit, 50)

    rows = query_all(store,
      "SELECT fields_json, record_vector FROM hrr_records WHERE source = $1 AND record_vector IS NOT NULL",
      [source])

    query_phases = Taskweft.NIF.hrr_encode_text(query_text, dim)

    rows
    |> Enum.map(fn row ->
      sim = Taskweft.NIF.hrr_similarity(
        Taskweft.NIF.hrr_bytes_to_phases(row["record_vector"], 0),
        query_phases)
      {sim, Jason.decode!(row["fields_json"])}
    end)
    |> Enum.filter(fn {sim, _} -> sim >= threshold end)
    |> Enum.sort_by(fn {sim, _} -> -sim end)
    |> Enum.take(limit)
  rescue
    _ -> []
  end

  @doc """
  For each record in *source*, unbind `role(join_field)` from its
  `record_vector` and return the resulting probe bytes.  Used by the join
  layer to compute per-record join keys in vector space.

  Returns `[{probe_bytes, fields_map}]` for every record that has a
  non-nil record_vector.
  """
  @spec vectors_for_join(store(), String.t(), String.t()) ::
          [{binary(), map()}]
  def vectors_for_join({_pool, dim} = store, source, join_field) do
    rows = query_all(store,
      "SELECT fields_json, record_vector FROM hrr_records WHERE source = $1 AND record_vector IS NOT NULL",
      [source])

    role_bytes = Taskweft.NIF.hrr_encode_atom("role_#{join_field}", dim)
                 |> Taskweft.NIF.hrr_phases_to_bytes()

    Enum.map(rows, fn row ->
      probe = Taskweft.NIF.hrr_unbind(row["record_vector"], role_bytes)
      {probe, Jason.decode!(row["fields_json"])}
    end)
  rescue
    _ -> []
  end

  # ---------------------------------------------------------------------------
  # Private: query helper
  # ---------------------------------------------------------------------------

  defp query_all({pool, _dim}, sql, params) do
    conn = Process.get({:taskweft_hrr_conn, pool}) || pool
    %Postgrex.Result{columns: cols, rows: rows} = Postgrex.query!(conn, sql, params)
    # rows is nil for DML statements (INSERT/UPDATE/DELETE) in Postgrex.
    Enum.map(rows || [], fn row -> Enum.zip(cols, row) |> Map.new() end)
  end

  # ---------------------------------------------------------------------------
  # Private: HRR vector construction
  # ---------------------------------------------------------------------------

  defp build_record_vector(fields_map, dim) do
    bindings =
      Enum.map(fields_map, fn {field, value} ->
        role_bytes = Taskweft.NIF.hrr_encode_atom("role_#{field}", dim)
                     |> Taskweft.NIF.hrr_phases_to_bytes()
        val_bytes  = Taskweft.NIF.hrr_encode_text(to_string(value), dim)
                     |> Taskweft.NIF.hrr_phases_to_bytes()
        Taskweft.NIF.hrr_bind(role_bytes, val_bytes)
      end)

    case bindings do
      []    -> nil
      [one] -> one
      many  -> Taskweft.NIF.hrr_bundle(many)
    end
  rescue
    _ -> nil
  end

  # ---------------------------------------------------------------------------
  # Private: bundle maintenance
  # ---------------------------------------------------------------------------

  defp rebuild_bundle(store, source) do
    rows = query_all(store,
      "SELECT record_vector FROM hrr_records WHERE source = $1 AND record_vector IS NOT NULL",
      [source])

    vecs = Enum.map(rows, & &1["record_vector"])

    case vecs do
      [] ->
        query_all(store, "DELETE FROM hrr_bundles WHERE source = $1", [source])

      [one] ->
        upsert_bundle(store, source, one, 1)

      many ->
        upsert_bundle(store, source, Taskweft.NIF.hrr_bundle(many), length(many))
    end
  rescue
    _ -> :ok
  end

  defp upsert_bundle(store, source, bundle_bytes, count) do
    query_all(store, """
    INSERT INTO hrr_bundles (source, bundle_vector, record_count, updated_at)
    VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
    ON CONFLICT (source) DO UPDATE SET
      bundle_vector = EXCLUDED.bundle_vector,
      record_count  = EXCLUDED.record_count,
      updated_at    = EXCLUDED.updated_at
    """, [source, bundle_bytes, count])
  end
end
