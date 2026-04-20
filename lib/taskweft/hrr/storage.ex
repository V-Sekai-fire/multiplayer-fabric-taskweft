defmodule Taskweft.HRR.Storage do
  @moduledoc """
  SQLite-backed GenServer for the HRR Ecto adapter.

  ## Data model

  Two tables hold all data regardless of Ecto schema source:

      hrr_records  – one row per record; fields serialised as JSON;
                     record_vector = bundle of bind(role(field), encode(value))
                     for every field in the row.

      hrr_bundles  – one row per source; vector = superposition of all
                     record_vectors in that source.  Kept up to date on
                     every insert/delete (outside transactions) or at commit
                     time (inside transactions).  Exposed via `bundle/2` and
                     used for fast COUNT(*) via `record_count/2`.

  ## HRR encoding per INSERT

      role(field)   = hrr_encode_atom("role_<field>", dim)   → bytes
      val(v)        = hrr_phases_to_bytes(hrr_encode_text(v, dim))
      field_binding = hrr_bind(role(field), val(v))
      record_vector = hrr_bundle([field_binding, ...])

  ## Probe operations

  `probe_field(srv, source, field, query_text, opts)` – unbinds the role
  from every record_vector and ranks by cosine similarity to encode(query).

  `probe_text(srv, source, query_text, opts)` – ranks by cosine similarity
  between encode(query) and each record_vector (full scan).

  Both return `[{similarity_float, fields_map}]` sorted descending.

  ## Transactions

  Wrapping operations in `begin/commit/rollback_transaction` uses SQLite
  BEGIN / COMMIT / ROLLBACK (or SAVEPOINT for nested transactions).  Bundle
  rebuilds are deferred until the outermost commit so probes inside a
  transaction see the pre-transaction bundle state.
  """

  use GenServer

  alias Exqlite.Sqlite3

  @schema [
    """
    CREATE TABLE IF NOT EXISTS hrr_records (
      source        TEXT    NOT NULL,
      id            TEXT    NOT NULL,
      fields_json   TEXT    NOT NULL,
      record_vector BLOB,
      PRIMARY KEY (source, id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hrr_bundles (
      source        TEXT PRIMARY KEY,
      bundle_vector BLOB    NOT NULL,
      record_count  INTEGER DEFAULT 0,
      updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
  ]

  # ---------------------------------------------------------------------------
  # OTP child_spec / start_link
  # ---------------------------------------------------------------------------

  def child_spec(opts) do
    name    = Keyword.fetch!(opts, :name)
    db_path = Keyword.get(opts, :db_path, default_db_path(name))
    dim     = Keyword.get(opts, :dim, 1024)

    %{
      id:    name,
      start: {__MODULE__, :start_link, [[name: name, db_path: db_path, dim: dim]]}
    }
  end

  def start_link(opts) do
    name    = Keyword.fetch!(opts, :name)
    db_path = Keyword.get(opts, :db_path, default_db_path(name))
    dim     = Keyword.get(opts, :dim, 1024)

    GenServer.start_link(__MODULE__, {db_path, dim}, name: name)
  end

  # ---------------------------------------------------------------------------
  # Public API – CRUD
  # ---------------------------------------------------------------------------

  @spec insert(GenServer.server(), String.t(), term(), map()) :: :ok
  def insert(srv, source, id, fields_map),
    do: GenServer.call(srv, {:insert, source, to_string(id), fields_map})

  @spec get(GenServer.server(), String.t(), term()) :: map() | nil
  def get(srv, source, id),
    do: GenServer.call(srv, {:get, source, to_string(id)})

  @spec delete(GenServer.server(), String.t(), term()) :: :ok
  def delete(srv, source, id),
    do: GenServer.call(srv, {:delete, source, to_string(id)})

  @spec all(GenServer.server(), String.t()) :: [map()]
  def all(srv, source),
    do: GenServer.call(srv, {:all, source})

  # ---------------------------------------------------------------------------
  # Public API – metadata
  # ---------------------------------------------------------------------------

  @spec dim(GenServer.server()) :: pos_integer()
  def dim(srv), do: GenServer.call(srv, :dim)

  @doc "Return the current source-level bundle vector (bytes), or nil if the source is empty."
  @spec bundle(GenServer.server(), String.t()) :: binary() | nil
  def bundle(srv, source), do: GenServer.call(srv, {:bundle, source})

  @doc """
  Return the committed record count for *source* directly from `hrr_bundles`
  without scanning `hrr_records`.  Returns 0 for unknown sources.

  Used by the Query layer for O(1) COUNT(*) without a WHERE clause.
  """
  @spec record_count(GenServer.server(), String.t()) :: non_neg_integer()
  def record_count(srv, source), do: GenServer.call(srv, {:record_count, source})

  # ---------------------------------------------------------------------------
  # Public API – transactions
  # ---------------------------------------------------------------------------

  @doc "Begin a SQLite transaction (or SAVEPOINT for nested calls)."
  @spec begin_transaction(GenServer.server()) :: :ok
  def begin_transaction(srv), do: GenServer.call(srv, :begin_transaction)

  @doc "Commit the current transaction and rebuild dirty source bundles."
  @spec commit_transaction(GenServer.server()) :: :ok
  def commit_transaction(srv), do: GenServer.call(srv, :commit_transaction)

  @doc "Roll back the current transaction."
  @spec rollback_transaction(GenServer.server()) :: :ok
  def rollback_transaction(srv), do: GenServer.call(srv, :rollback_transaction)

  @doc "Return true if a transaction is currently open."
  @spec in_transaction?(GenServer.server()) :: boolean()
  def in_transaction?(srv), do: GenServer.call(srv, :in_transaction?)

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
  @spec probe_field(GenServer.server(), String.t(), atom() | String.t(), String.t(), keyword()) ::
          [{float(), map()}]
  def probe_field(srv, source, field, query_text, opts \\ []),
    do: GenServer.call(srv, {:probe_field, source, to_string(field), query_text, opts})

  @doc """
  Rank all records in *source* by cosine similarity of their `record_vector`
  to `encode_text(query_text)`.

  Same options and return type as `probe_field/5`.
  """
  @spec probe_text(GenServer.server(), String.t(), String.t(), keyword()) ::
          [{float(), map()}]
  def probe_text(srv, source, query_text, opts \\ []),
    do: GenServer.call(srv, {:probe_text, source, query_text, opts})

  @doc """
  For each record in *source*, unbind `role(join_field)` from its
  `record_vector` and return the resulting probe bytes.  Used by the join
  layer to compute per-record join keys in vector space.

  Returns `[{probe_bytes, fields_map}]` for every record that has a
  non-nil record_vector.
  """
  @spec vectors_for_join(GenServer.server(), String.t(), String.t()) ::
          [{binary(), map()}]
  def vectors_for_join(srv, source, join_field),
    do: GenServer.call(srv, {:vectors_for_join, source, join_field})

  # ---------------------------------------------------------------------------
  # GenServer init
  # ---------------------------------------------------------------------------

  @impl true
  def init({db_path, dim}) do
    db_path = Path.expand(db_path)
    File.mkdir_p!(Path.dirname(db_path))
    {:ok, conn} = Sqlite3.open(db_path)
    Sqlite3.execute(conn, "PRAGMA journal_mode=WAL")
    Enum.each(@schema, &Sqlite3.execute(conn, String.trim(&1)))
    {:ok, %{conn: conn, dim: dim, txn_depth: 0, dirty: MapSet.new()}}
  end

  # ---------------------------------------------------------------------------
  # GenServer handle_call – CRUD
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({:insert, source, id, fields_map}, _from, %{conn: conn, dim: dim} = state) do
    json = Jason.encode!(fields_map)
    vec  = build_record_vector(fields_map, dim)

    exec(conn,
      "INSERT OR REPLACE INTO hrr_records (source, id, fields_json, record_vector) VALUES (?, ?, ?, ?)",
      [source, id, json, vec])

    state = mark_dirty(state, source)
    state = maybe_rebuild_bundle(conn, source, dim, state)
    {:reply, :ok, state}
  end

  def handle_call({:get, source, id}, _from, %{conn: conn} = state) do
    row = query_one(conn,
      "SELECT fields_json FROM hrr_records WHERE source = ? AND id = ?",
      [source, id])
    {:reply, row && Jason.decode!(row["fields_json"]), state}
  end

  def handle_call({:delete, source, id}, _from, %{conn: conn, dim: dim} = state) do
    exec(conn, "DELETE FROM hrr_records WHERE source = ? AND id = ?", [source, id])
    state = mark_dirty(state, source)
    state = maybe_rebuild_bundle(conn, source, dim, state)
    {:reply, :ok, state}
  end

  def handle_call({:all, source}, _from, %{conn: conn} = state) do
    rows = query_all(conn,
      "SELECT fields_json FROM hrr_records WHERE source = ?",
      [source])
    maps = Enum.map(rows, &Jason.decode!(&1["fields_json"]))
    {:reply, maps, state}
  end

  # ---------------------------------------------------------------------------
  # GenServer handle_call – metadata
  # ---------------------------------------------------------------------------

  def handle_call(:dim, _from, state),
    do: {:reply, state.dim, state}

  def handle_call({:bundle, source}, _from, %{conn: conn} = state) do
    row = query_one(conn, "SELECT bundle_vector FROM hrr_bundles WHERE source = ?", [source])
    {:reply, row && row["bundle_vector"], state}
  end

  def handle_call({:record_count, source}, _from, %{conn: conn} = state) do
    row = query_one(conn, "SELECT record_count FROM hrr_bundles WHERE source = ?", [source])
    {:reply, (row && row["record_count"]) || 0, state}
  end

  # ---------------------------------------------------------------------------
  # GenServer handle_call – transactions
  # ---------------------------------------------------------------------------

  def handle_call(:begin_transaction, _from, %{conn: conn, txn_depth: 0} = state) do
    Sqlite3.execute(conn, "BEGIN")
    {:reply, :ok, %{state | txn_depth: 1, dirty: MapSet.new()}}
  end

  def handle_call(:begin_transaction, _from, %{conn: conn, txn_depth: depth} = state) do
    Sqlite3.execute(conn, "SAVEPOINT sp#{depth}")
    {:reply, :ok, %{state | txn_depth: depth + 1}}
  end

  def handle_call(:commit_transaction, _from, %{txn_depth: 0} = state),
    do: {:reply, {:error, :not_in_transaction}, state}

  def handle_call(:commit_transaction, _from, %{conn: conn, txn_depth: 1, dirty: dirty, dim: dim} = state) do
    Sqlite3.execute(conn, "COMMIT")
    Enum.each(dirty, &rebuild_bundle(conn, &1, dim))
    {:reply, :ok, %{state | txn_depth: 0, dirty: MapSet.new()}}
  end

  def handle_call(:commit_transaction, _from, %{conn: conn, txn_depth: depth} = state) do
    Sqlite3.execute(conn, "RELEASE sp#{depth - 1}")
    {:reply, :ok, %{state | txn_depth: depth - 1}}
  end

  def handle_call(:rollback_transaction, _from, %{txn_depth: 0} = state),
    do: {:reply, {:error, :not_in_transaction}, state}

  def handle_call(:rollback_transaction, _from, %{conn: conn, txn_depth: 1} = state) do
    Sqlite3.execute(conn, "ROLLBACK")
    {:reply, :ok, %{state | txn_depth: 0, dirty: MapSet.new()}}
  end

  def handle_call(:rollback_transaction, _from, %{conn: conn, txn_depth: depth} = state) do
    Sqlite3.execute(conn, "ROLLBACK TO sp#{depth - 1}")
    {:reply, :ok, %{state | txn_depth: depth - 1}}
  end

  def handle_call(:in_transaction?, _from, state),
    do: {:reply, state.txn_depth > 0, state}

  # ---------------------------------------------------------------------------
  # GenServer handle_call – probe
  # ---------------------------------------------------------------------------

  def handle_call({:probe_field, source, field, query_text, opts}, _from, %{conn: conn, dim: dim} = state) do
    threshold = Keyword.get(opts, :threshold, 0.0)
    limit     = Keyword.get(opts, :limit, 50)
    {:reply, do_probe_field(conn, source, field, query_text, dim, threshold, limit), state}
  end

  def handle_call({:probe_text, source, query_text, opts}, _from, %{conn: conn, dim: dim} = state) do
    threshold = Keyword.get(opts, :threshold, 0.0)
    limit     = Keyword.get(opts, :limit, 50)
    {:reply, do_probe_text(conn, source, query_text, dim, threshold, limit), state}
  end

  def handle_call({:vectors_for_join, source, join_field}, _from, %{conn: conn, dim: dim} = state) do
    rows = query_all(conn,
      "SELECT fields_json, record_vector FROM hrr_records WHERE source = ? AND record_vector IS NOT NULL",
      [source])

    role_bytes = Taskweft.NIF.hrr_encode_atom("role_#{join_field}", dim)
                 |> Taskweft.NIF.hrr_phases_to_bytes()

    results = Enum.map(rows, fn row ->
      probe = Taskweft.NIF.hrr_unbind(row["record_vector"], role_bytes)
      {probe, Jason.decode!(row["fields_json"])}
    end)

    {:reply, results, state}
  rescue
    _ -> {:reply, [], state}
  end

  # ---------------------------------------------------------------------------
  # Private: dirty tracking and conditional bundle rebuild
  # ---------------------------------------------------------------------------

  defp mark_dirty(%{txn_depth: 0} = state, _source), do: state
  defp mark_dirty(%{dirty: d} = state, source), do: %{state | dirty: MapSet.put(d, source)}

  # Outside a transaction → rebuild immediately
  defp maybe_rebuild_bundle(conn, source, dim, %{txn_depth: 0} = state) do
    rebuild_bundle(conn, source, dim)
    state
  end

  # Inside a transaction → defer
  defp maybe_rebuild_bundle(_conn, _source, _dim, state), do: state

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
  # Private: probe operations
  # ---------------------------------------------------------------------------

  defp do_probe_field(conn, source, field, query_text, dim, threshold, limit) do
    rows = query_all(conn,
      "SELECT fields_json, record_vector FROM hrr_records WHERE source = ? AND record_vector IS NOT NULL",
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

  defp do_probe_text(conn, source, query_text, dim, threshold, limit) do
    rows = query_all(conn,
      "SELECT fields_json, record_vector FROM hrr_records WHERE source = ? AND record_vector IS NOT NULL",
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

  # ---------------------------------------------------------------------------
  # Private: bundle maintenance
  # ---------------------------------------------------------------------------

  defp rebuild_bundle(conn, source, _dim) do
    rows = query_all(conn,
      "SELECT record_vector FROM hrr_records WHERE source = ? AND record_vector IS NOT NULL",
      [source])

    vecs = Enum.map(rows, & &1["record_vector"])

    case vecs do
      [] ->
        exec(conn, "DELETE FROM hrr_bundles WHERE source = ?", [source])

      [one] ->
        upsert_bundle(conn, source, one, 1)

      many ->
        upsert_bundle(conn, source, Taskweft.NIF.hrr_bundle(many), length(many))
    end
  rescue
    _ -> :ok
  end

  defp upsert_bundle(conn, source, bundle_bytes, count) do
    exec(conn, """
    INSERT INTO hrr_bundles (source, bundle_vector, record_count, updated_at)
    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(source) DO UPDATE SET
      bundle_vector = excluded.bundle_vector,
      record_count  = excluded.record_count,
      updated_at    = excluded.updated_at
    """, [source, bundle_bytes, count])
  end

  # ---------------------------------------------------------------------------
  # Private: SQLite helpers
  # ---------------------------------------------------------------------------

  defp exec(conn, sql, params) do
    {:ok, stmt} = Sqlite3.prepare(conn, sql)
    :ok = Sqlite3.bind(stmt, params)
    Sqlite3.step(conn, stmt)
    Sqlite3.release(conn, stmt)
    :ok
  end

  defp query_one(conn, sql, params) do
    case query_all(conn, sql, params) do
      [row | _] -> row
      []        -> nil
    end
  end

  defp query_all(conn, sql, params) do
    {:ok, stmt} = Sqlite3.prepare(conn, sql)
    :ok = Sqlite3.bind(stmt, params)
    {:ok, cols} = Sqlite3.columns(conn, stmt)
    rows = collect_rows(conn, stmt, [])
    Sqlite3.release(conn, stmt)
    Enum.map(rows, fn row -> cols |> Enum.zip(row) |> Map.new() end)
  end

  defp collect_rows(conn, stmt, acc) do
    case Sqlite3.step(conn, stmt) do
      {:row, row} -> collect_rows(conn, stmt, [row | acc])
      :done       -> Enum.reverse(acc)
      _           -> Enum.reverse(acc)
    end
  end

  defp default_db_path(name) do
    dir = System.get_env("TASKWEFT_DATA_DIR") || Path.join(System.user_home!(), ".taskweft")
    Path.join(dir, "#{name}.db")
  end
end
