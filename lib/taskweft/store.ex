defmodule Taskweft.Store do
  @moduledoc """
  SQLite-backed fact store with entity resolution, trust scoring, and HRR vectors.

  Elixir port of the Python MemoryStore from multiplayer-fabric-taskweft-planner.
  Uses exqlite for SQLite access and the Taskweft NIFs for HRR vector computation.
  """

  alias Exqlite.Sqlite3

  @helpful_delta 0.05
  @unhelpful_delta -0.10

  @schema_stmts [
    """
    CREATE TABLE IF NOT EXISTS facts (
        fact_id         INTEGER PRIMARY KEY AUTOINCREMENT,
        content         TEXT NOT NULL UNIQUE,
        category        TEXT DEFAULT 'general',
        tags            TEXT DEFAULT '',
        trust_score     REAL DEFAULT 0.5,
        retrieval_count INTEGER DEFAULT 0,
        helpful_count   INTEGER DEFAULT 0,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        hrr_vector      BLOB
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS entities (
        entity_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL,
        entity_type TEXT DEFAULT 'unknown',
        aliases     TEXT DEFAULT '',
        created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_entities (
        fact_id    INTEGER REFERENCES facts(fact_id),
        entity_id  INTEGER REFERENCES entities(entity_id),
        hrr_vector BLOB,
        PRIMARY KEY (fact_id, entity_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_facts_trust    ON facts(trust_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_facts_category ON facts(category)",
    "CREATE INDEX IF NOT EXISTS idx_entities_name  ON entities(name)",
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts
        USING fts5(content, tags, content=facts, content_rowid=fact_id)
    """,
    """
    CREATE TRIGGER IF NOT EXISTS facts_ai AFTER INSERT ON facts BEGIN
        INSERT INTO facts_fts(rowid, content, tags)
            VALUES (new.fact_id, new.content, new.tags);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS facts_ad AFTER DELETE ON facts BEGIN
        INSERT INTO facts_fts(facts_fts, rowid, content, tags)
            VALUES ('delete', old.fact_id, old.content, old.tags);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS facts_au AFTER UPDATE ON facts BEGIN
        INSERT INTO facts_fts(facts_fts, rowid, content, tags)
            VALUES ('delete', old.fact_id, old.content, old.tags);
        INSERT INTO facts_fts(rowid, content, tags)
            VALUES (new.fact_id, new.content, new.tags);
    END
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_banks (
        bank_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        bank_name  TEXT NOT NULL UNIQUE,
        vector     BLOB NOT NULL,
        dim        INTEGER NOT NULL,
        fact_count INTEGER DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
  ]

  defstruct [:conn, default_trust: 0.5, hrr_dim: 1024]

  @type t :: %__MODULE__{
          conn: reference(),
          default_trust: float(),
          hrr_dim: pos_integer()
        }

  @doc """
  Open (or create) a memory store at `db_path`.

  Options:
  - `:default_trust` — initial trust score for new facts (default `0.5`)
  - `:hrr_dim` — HRR vector dimension (default `1024`)
  """
  @spec open(Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def open(db_path, opts \\ []) do
    db_path = Path.expand(db_path)
    File.mkdir_p!(Path.dirname(db_path))

    with {:ok, conn} <- Sqlite3.open(db_path) do
      store = %__MODULE__{
        conn: conn,
        default_trust: Keyword.get(opts, :default_trust, 0.5),
        hrr_dim: Keyword.get(opts, :hrr_dim, 1024)
      }

      Sqlite3.execute(conn, "PRAGMA journal_mode=WAL")
      init_schema(store)
      migrate(store)
      {:ok, store}
    end
  end

  @doc "Close the database connection."
  @spec close(t()) :: :ok
  def close(%__MODULE__{conn: conn}), do: Sqlite3.close(conn)

  @doc """
  Insert a fact and return `{:ok, fact_id}`.

  Deduplicates by content. On duplicate returns the existing fact_id.
  Extracts entities, computes HRR vectors, and rebuilds the category bank.
  """
  @spec add_fact(t(), String.t(), String.t(), String.t()) :: {:ok, integer()} | {:error, term()}
  def add_fact(%__MODULE__{} = store, content, category \\ "general", tags \\ "") do
    content = String.trim(content)

    if content == "" do
      {:error, "content must not be empty"}
    else
      case insert_fact(store, content, category, tags) do
        {:ok, fact_id} ->
          link_entities(store, fact_id, content)
          compute_hrr_vector(store, fact_id, content)
          rebuild_bank(store, category)
          {:ok, fact_id}

        {:duplicate, fact_id} ->
          {:ok, fact_id}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Full-text search over facts using FTS5.

  Returns a list of fact maps ordered by FTS5 rank, then trust_score descending.
  Increments `retrieval_count` for matched facts.
  """
  @spec search_facts(t(), String.t(), String.t() | nil, float(), pos_integer()) ::
          list(map())
  def search_facts(%__MODULE__{conn: conn}, query, category \\ nil, min_trust \\ 0.3, limit \\ 10) do
    query = String.trim(query)

    if query == "" do
      []
    else
      {category_clause, category_params} =
        if category, do: {"AND f.category = ?", [category]}, else: {"", []}

      sql = """
      SELECT f.fact_id, f.content, f.category, f.tags,
             f.trust_score, f.retrieval_count, f.helpful_count,
             f.created_at, f.updated_at
      FROM facts f
      JOIN facts_fts fts ON fts.rowid = f.fact_id
      WHERE facts_fts MATCH ?
        AND f.trust_score >= ?
        #{category_clause}
      ORDER BY fts.rank, f.trust_score DESC
      LIMIT ?
      """

      params = [query, min_trust] ++ category_params ++ [limit]
      rows = query_all(conn, sql, params)

      if rows != [] do
        ids = Enum.map(rows, & &1["fact_id"])
        placeholders = Enum.map_join(ids, ",", fn _ -> "?" end)

        exec(conn, "UPDATE facts SET retrieval_count = retrieval_count + 1 WHERE fact_id IN (#{placeholders})", ids)
      end

      rows
    end
  end

  @doc """
  Partially update a fact. Returns `{:ok, true}` if found, `{:ok, false}` otherwise.

  Options: `:content`, `:trust_delta`, `:tags`, `:category`.
  """
  @spec update_fact(t(), integer(), keyword()) :: {:ok, boolean()}
  def update_fact(%__MODULE__{conn: conn} = store, fact_id, opts \\ []) do
    row = query_one(conn, "SELECT fact_id, trust_score, category FROM facts WHERE fact_id = ?", [fact_id])

    if row == nil do
      {:ok, false}
    else
      {assignments, params} = build_update_assignments(row, opts)

      exec(
        conn,
        "UPDATE facts SET #{Enum.join(assignments, ", ")} WHERE fact_id = ?",
        params ++ [fact_id]
      )

      if opts[:content] do
        exec(conn, "DELETE FROM fact_entities WHERE fact_id = ?", [fact_id])
        link_entities(store, fact_id, opts[:content])
        compute_hrr_vector(store, fact_id, opts[:content])
      end

      cat = opts[:category] || row["category"]
      rebuild_bank(store, cat)
      {:ok, true}
    end
  end

  @doc "Delete a fact and its entity links. Returns `{:ok, true}` if found."
  @spec remove_fact(t(), integer()) :: {:ok, boolean()}
  def remove_fact(%__MODULE__{conn: conn} = store, fact_id) do
    row = query_one(conn, "SELECT fact_id, category FROM facts WHERE fact_id = ?", [fact_id])

    if row == nil do
      {:ok, false}
    else
      exec(conn, "DELETE FROM fact_entities WHERE fact_id = ?", [fact_id])
      exec(conn, "DELETE FROM facts WHERE fact_id = ?", [fact_id])
      rebuild_bank(store, row["category"])
      {:ok, true}
    end
  end

  @doc """
  Browse facts ordered by trust_score descending.

  Options: `:category`, `:min_trust` (default `0.0`), `:limit` (default `50`).
  """
  @spec list_facts(t(), keyword()) :: list(map())
  def list_facts(%__MODULE__{conn: conn}, opts \\ []) do
    category = opts[:category]
    min_trust = opts[:min_trust] || 0.0
    limit = opts[:limit] || 50

    {category_clause, extra_params} =
      if category, do: {"AND category = ?", [category]}, else: {"", []}

    sql = """
    SELECT fact_id, content, category, tags, trust_score,
           retrieval_count, helpful_count, created_at, updated_at
    FROM facts
    WHERE trust_score >= ?
      #{category_clause}
    ORDER BY trust_score DESC
    LIMIT ?
    """

    query_all(conn, sql, [min_trust] ++ extra_params ++ [limit])
  end

  @doc """
  Record user feedback and adjust trust asymmetrically.

  `helpful: true` → trust += 0.05, `helpful: false` → trust -= 0.10.
  Returns `{:ok, map}` with old/new trust and helpful_count.
  """
  @spec record_feedback(t(), integer(), boolean()) :: {:ok, map()} | {:error, term()}
  def record_feedback(%__MODULE__{conn: conn}, fact_id, helpful) do
    row = query_one(conn, "SELECT fact_id, trust_score, helpful_count FROM facts WHERE fact_id = ?", [fact_id])

    if row == nil do
      {:error, "fact_id #{fact_id} not found"}
    else
      old_trust = row["trust_score"]
      delta = if helpful, do: @helpful_delta, else: @unhelpful_delta
      new_trust = clamp_trust(old_trust + delta)
      helpful_increment = if helpful, do: 1, else: 0

      exec(conn, """
      UPDATE facts
      SET trust_score   = ?,
          helpful_count = helpful_count + ?,
          updated_at    = CURRENT_TIMESTAMP
      WHERE fact_id = ?
      """, [new_trust, helpful_increment, fact_id])

      {:ok, %{
        "fact_id" => fact_id,
        "old_trust" => old_trust,
        "new_trust" => new_trust,
        "helpful_count" => row["helpful_count"] + helpful_increment
      }}
    end
  end

  @doc """
  Recompute all HRR vectors and category banks from text. For migration/recovery.

  Returns `{:ok, count}` with number of facts processed.
  """
  @spec rebuild_all_vectors(t(), pos_integer() | nil) :: {:ok, integer()}
  def rebuild_all_vectors(%__MODULE__{conn: conn} = store, dim \\ nil) do
    store = if dim, do: %{store | hrr_dim: dim}, else: store
    rows = query_all(conn, "SELECT fact_id, content, category FROM facts", [])

    categories =
      Enum.reduce(rows, MapSet.new(), fn row, acc ->
        compute_hrr_vector(store, row["fact_id"], row["content"])
        MapSet.put(acc, row["category"])
      end)

    Enum.each(categories, &rebuild_bank(store, &1))
    {:ok, length(rows)}
  end

  # ---------------------------------------------------------------------------
  # Private: schema and migration
  # ---------------------------------------------------------------------------

  defp init_schema(%__MODULE__{conn: conn}) do
    Enum.each(@schema_stmts, fn sql ->
      Sqlite3.execute(conn, String.trim(sql))
    end)
  end

  defp migrate(%__MODULE__{conn: conn}) do
    rows = query_all(conn, "PRAGMA table_info(facts)", [])
    cols = Enum.map(rows, & &1["name"])

    unless "hrr_vector" in cols do
      Sqlite3.execute(conn, "ALTER TABLE facts ADD COLUMN hrr_vector BLOB")
    end
  end

  # ---------------------------------------------------------------------------
  # Private: insert
  # ---------------------------------------------------------------------------

  defp insert_fact(%__MODULE__{conn: conn, default_trust: dt}, content, category, tags) do
    {:ok, stmt} = Sqlite3.prepare(conn, "INSERT INTO facts (content, category, tags, trust_score) VALUES (?, ?, ?, ?)")
    :ok = Sqlite3.bind(stmt, [content, category, tags, dt])

    case Sqlite3.step(conn, stmt) do
      :done ->
        {:ok, row_id} = Sqlite3.last_insert_rowid(conn)
        Sqlite3.release(conn, stmt)
        {:ok, row_id}

      {:error, reason} ->
        Sqlite3.release(conn, stmt)
        reason_str = to_string(reason)
        if String.contains?(reason_str, "UNIQUE") do
          row = query_one(conn, "SELECT fact_id FROM facts WHERE content = ?", [content])
          {:duplicate, row["fact_id"]}
        else
          {:error, reason}
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Private: entity helpers
  # ---------------------------------------------------------------------------

  defp link_entities(store, fact_id, content) do
    content
    |> extract_entities()
    |> Enum.each(fn name ->
      entity_id = resolve_entity(store, name)
      link_fact_entity(store, fact_id, entity_id)
    end)
  end

  defp extract_entities(text) do
    cap = Regex.scan(~r/\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b/, text, capture: :all_but_first)
    dquote = Regex.scan(~r/"([^"]+)"/, text, capture: :all_but_first)
    squote = Regex.scan(~r/'([^']+)'/, text, capture: :all_but_first)
    aka = Regex.scan(~r/(\w+(?:\s+\w+)*)\s+(?:aka|also known as)\s+(\w+(?:\s+\w+)*)/i, text, capture: :all_but_first)

    (List.flatten(cap) ++ List.flatten(dquote) ++ List.flatten(squote) ++ List.flatten(aka))
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.reduce({MapSet.new(), []}, fn name, {seen, acc} ->
      key = String.downcase(name)
      if MapSet.member?(seen, key), do: {seen, acc}, else: {MapSet.put(seen, key), acc ++ [name]}
    end)
    |> elem(1)
  end

  defp resolve_entity(%__MODULE__{conn: conn}, name) do
    row = query_one(conn, "SELECT entity_id FROM entities WHERE name LIKE ?", [name])

    if row do
      row["entity_id"]
    else
      alias_row = query_one(conn, "SELECT entity_id FROM entities WHERE ',' || aliases || ',' LIKE '%,' || ? || ',%'", [name])

      if alias_row do
        alias_row["entity_id"]
      else
        {:ok, stmt} = Sqlite3.prepare(conn, "INSERT INTO entities (name) VALUES (?)")
        :ok = Sqlite3.bind(stmt, [name])
        Sqlite3.step(conn, stmt)
        Sqlite3.release(conn, stmt)
        {:ok, id} = Sqlite3.last_insert_rowid(conn)
        id
      end
    end
  end

  defp link_fact_entity(%__MODULE__{conn: conn}, fact_id, entity_id) do
    exec(conn, "INSERT OR IGNORE INTO fact_entities (fact_id, entity_id) VALUES (?, ?)", [fact_id, entity_id])
  end

  # ---------------------------------------------------------------------------
  # Private: HRR helpers
  # ---------------------------------------------------------------------------

  defp compute_hrr_vector(%__MODULE__{conn: conn, hrr_dim: dim}, fact_id, content) do
    entity_rows = query_all(conn, """
    SELECT e.entity_id, e.name FROM entities e
    JOIN fact_entities fe ON fe.entity_id = e.entity_id
    WHERE fe.fact_id = ?
    """, [fact_id])

    entities = Enum.map(entity_rows, & &1["name"])
    vector_bytes = Taskweft.NIF.hrr_encode_fact(content, entities, dim)

    exec(conn, "UPDATE facts SET hrr_vector = ? WHERE fact_id = ?", [vector_bytes, fact_id])

    Enum.each(entity_rows, fn row ->
      binding_bytes = Taskweft.NIF.hrr_encode_binding(content, row["name"], dim)
      exec(conn, "UPDATE fact_entities SET hrr_vector = ? WHERE fact_id = ? AND entity_id = ?",
        [binding_bytes, fact_id, row["entity_id"]])
    end)
  rescue
    _ -> :ok
  end

  defp rebuild_bank(%__MODULE__{conn: conn, hrr_dim: dim}, category) do
    bank_name = "cat:#{category}"
    rows = query_all(conn, "SELECT hrr_vector FROM facts WHERE category = ? AND hrr_vector IS NOT NULL", [category])

    if rows == [] do
      exec(conn, "DELETE FROM memory_banks WHERE bank_name = ?", [bank_name])
    else
      vectors = Enum.map(rows, fn row ->
        Taskweft.NIF.hrr_bytes_to_phases(row["hrr_vector"], 0)
      end)

      bank_vector = bundle_phases(vectors)
      fact_count = length(vectors)
      bank_bytes = Taskweft.NIF.hrr_phases_to_bytes(bank_vector)

      exec(conn, """
      INSERT INTO memory_banks (bank_name, vector, dim, fact_count, updated_at)
      VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(bank_name) DO UPDATE SET
          vector = excluded.vector,
          dim = excluded.dim,
          fact_count = excluded.fact_count,
          updated_at = excluded.updated_at
      """, [bank_name, bank_bytes, dim, fact_count])
    end
  rescue
    _ -> :ok
  end

  defp bundle_phases([single]), do: single
  defp bundle_phases(vectors) do
    n = length(vectors)
    vectors
    |> Enum.zip_reduce([], fn elems, acc -> [elems | acc] end)
    |> Enum.reverse()
    |> Enum.map(fn elems ->
      :math.fmod(Enum.sum(elems) / n, 2 * :math.pi())
    end)
  end

  # ---------------------------------------------------------------------------
  # Private: SQLite query helpers
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
      [] -> nil
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
      :done -> Enum.reverse(acc)
      _ -> Enum.reverse(acc)
    end
  end

  defp build_update_assignments(row, opts) do
    {assignments, params} = {["updated_at = CURRENT_TIMESTAMP"], []}

    {assignments, params} =
      if opts[:content] do
        {assignments ++ ["content = ?"], params ++ [String.trim(opts[:content])]}
      else
        {assignments, params}
      end

    {assignments, params} =
      if opts[:tags] do
        {assignments ++ ["tags = ?"], params ++ [opts[:tags]]}
      else
        {assignments, params}
      end

    {assignments, params} =
      if opts[:category] do
        {assignments ++ ["category = ?"], params ++ [opts[:category]]}
      else
        {assignments, params}
      end

    if opts[:trust_delta] do
      new_trust = clamp_trust(row["trust_score"] + opts[:trust_delta])
      {assignments ++ ["trust_score = ?"], params ++ [new_trust]}
    else
      {assignments, params}
    end
  end

  defp clamp_trust(value), do: max(0.0, min(1.0, value))
end
