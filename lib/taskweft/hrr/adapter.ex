defmodule Taskweft.HRR.Adapter do
  @moduledoc """
  Ecto adapter backed by Holographic Reduced Representations, persisted to PostgreSQL
  (CockroachDB-compatible).

  ## What HRR replaces SQL with

  | SQL concept           | HRR operation                                                     |
  |-----------------------|-------------------------------------------------------------------|
  | Table                 | Per-source bundle (superposition of record vectors in PostgreSQL) |
  | INSERT                | `bundle(table_mem, bind(role(field), encode(value)))`             |
  | DELETE                | Remove record vector, rebuild bundle                              |
  | WHERE field = value   | exact in-memory equality after full scan                          |
  | WHERE LIKE %query%    | `probe_field` → cosine-rank by `encode(stripped_pattern)`         |
  | SELECT *              | Return deserialized JSON rows from PostgreSQL                     |
  | UPDATE                | DELETE + INSERT                                                   |

  ## Repo configuration

      config :my_app, MyApp.Repo,
        adapter: Taskweft.HRR.Adapter,
        hrr_dim: 1024,                    # vector dimension (default 1024)
        name: MyApp.HRRPool,              # Postgrex pool name
        url: "postgresql://...",          # database URL (default DATABASE_URL env var)
        pool_size: 10                     # connection pool size (default 10)

  ## Caveats

  * Records are persisted in PostgreSQL as JSON; vectors are stored as BYTEA columns.
  * `LIKE` / `ILIKE` use HRR cosine similarity (`probe_field`) rather than
    string pattern matching.  Pass `:hrr_threshold` in query opts to tune
    the minimum similarity score (default `0.1`).
  * Queries beyond `==`, `!=`, `like`, and `ilike` fall back to in-memory
    linear scan.
  * Transactions, inner joins (exact and HRR semantic), and common aggregates are supported.
  * LEFT JOIN is silently treated as INNER JOIN.
  """

  # Register behaviours only when Ecto is compiled into the load path.
  # This lets taskweft compile standalone while still satisfying the
  # Ecto.Adapter contract in downstream projects that depend on both.
  if Code.ensure_loaded?(Ecto.Adapter) do
    @behaviour Ecto.Adapter
  end

  if Code.ensure_loaded?(Ecto.Adapter.Schema) do
    @behaviour Ecto.Adapter.Schema
  end

  if Code.ensure_loaded?(Ecto.Adapter.Queryable) do
    @behaviour Ecto.Adapter.Queryable
  end

  if Code.ensure_loaded?(Ecto.Adapter.Transaction) do
    @behaviour Ecto.Adapter.Transaction
  end

  alias Taskweft.HRR.{Query, Storage}

  # ---------------------------------------------------------------------------
  # Ecto.Adapter callbacks
  # ---------------------------------------------------------------------------

  defmacro __before_compile__(_env), do: :ok

  def ensure_all_started(_config, _type), do: {:ok, []}

  def init(config) do
    dim       = Keyword.get(config, :hrr_dim, 1024)
    url       = Keyword.get(config, :url, System.get_env("DATABASE_URL"))
    pool_size = Keyword.get(config, :pool_size, 10)
    pool_name = Keyword.get(config, :name, Taskweft.HRR.Pool)

    pool_opts = [name: pool_name, url: url, pool_size: pool_size,
                 after_connect: {Storage, :ensure_schema_on_connect!, []}]

    meta = %{adapter: __MODULE__, store: {pool_name, dim}}
    {:ok, Postgrex.child_spec(pool_opts), meta}
  end

  def checkout(_meta, _config, fun), do: fun.()

  def checked_out?(_meta), do: false

  def loaders(:binary_id, type), do: [type]
  def loaders(_primitive, type), do: [type]

  def dumpers(:binary_id, type), do: [type]
  def dumpers(_primitive, type), do: [type]

  # ---------------------------------------------------------------------------
  # Ecto.Adapter.Schema callbacks
  # ---------------------------------------------------------------------------

  def autogenerate(:id),        do: nil
  def autogenerate(:embed_id),  do: generate_uuid()
  def autogenerate(:binary_id), do: generate_uuid()

  def insert(meta, schema_meta, fields, _on_conflict, returning, _opts) do
    %{store: store} = meta
    source = schema_meta.source

    id         = resolve_id(fields)
    fields_map = fields_to_string_map(fields)

    :ok = Storage.insert(store, source, id, fields_map)

    returning_vals = Enum.map(returning, fn f -> {f, Keyword.get(fields, f)} end)
    {:ok, returning_vals}
  end

  def insert_all(meta, schema_meta, _header, rows, _on_conflict, _returning, _placeholders, _opts) do
    %{store: {pool, _dim} = store} = meta
    source = schema_meta.source

    result =
      Postgrex.transaction(pool, fn conn ->
        Process.put({:taskweft_hrr_conn, pool}, conn)
        try do
          Enum.reduce(rows, 0, fn fields, acc ->
            id         = resolve_id(fields)
            fields_map = fields_to_string_map(fields)
            :ok = Storage.insert(store, source, id, fields_map)
            acc + 1
          end)
        after
          Process.delete({:taskweft_hrr_conn, pool})
        end
      end)

    case result do
      {:ok, count} -> {count, nil}
      {:error, err} -> raise err
    end
  end

  def update(meta, schema_meta, fields, filters, returning, _opts) do
    %{store: {pool, _dim} = store} = meta
    source = schema_meta.source
    id     = Keyword.get(filters, :id) || Keyword.get(filters, :binary_id)

    case Storage.get(store, source, id) do
      nil ->
        {:error, :stale}

      existing ->
        updates = fields_to_string_map(fields)
        merged  = Map.merge(existing, updates)

        Postgrex.transaction(pool, fn conn ->
          Process.put({:taskweft_hrr_conn, pool}, conn)
          try do
            :ok = Storage.delete(store, source, id)
            :ok = Storage.insert(store, source, id, merged)
          after
            Process.delete({:taskweft_hrr_conn, pool})
          end
        end)

        returning_vals = Enum.map(returning, fn f -> {f, Map.get(merged, to_string(f))} end)
        {:ok, returning_vals}
    end
  end

  def delete(meta, schema_meta, filters, _returning, _opts) do
    %{store: store} = meta
    source = schema_meta.source
    id     = Keyword.get(filters, :id) || Keyword.get(filters, :binary_id)

    case Storage.get(store, source, id) do
      nil    -> {:error, :stale}
      _entry ->
        :ok = Storage.delete(store, source, id)
        {:ok, []}
    end
  end

  # ---------------------------------------------------------------------------
  # Ecto.Adapter.Queryable callbacks
  # ---------------------------------------------------------------------------

  def prepare(operation, query), do: {:nocache, {operation, query}}

  def execute(meta, _query_meta, {:nocache, {:all, query}}, params, opts) do
    %{store: store} = meta
    rows = Query.execute(store, :all, query, params, opts)
    {length(rows), Enum.map(rows, &row_to_list(&1, query))}
  end

  def execute(_meta, _query_meta, {:nocache, {_op, _query}}, _params, _opts) do
    {0, []}
  end

  def stream(meta, _query_meta, {:nocache, {:all, query}}, params, opts) do
    %{store: store} = meta

    Stream.resource(
      fn -> Query.execute(store, :all, query, params, opts) end,
      fn
        []           -> {:halt, []}
        [row | rest] -> {[row_to_list(row, query)], rest}
      end,
      fn _ -> :ok end
    )
  end

  def stream(_meta, _query_meta, {:nocache, {_op, _}}, _params, _opts) do
    Stream.into([], [])
  end

  # ---------------------------------------------------------------------------
  # Ecto.Adapter.Transaction callbacks
  # ---------------------------------------------------------------------------

  def transaction(%{store: {pool, _dim} = store}, _opts, fun) do
    Postgrex.transaction(pool, fn conn ->
      Process.put({:taskweft_hrr_conn, pool}, conn)
      try do
        fun.()
      catch
        :throw, {:ecto_rollback, value} -> throw({:ecto_rollback, value})
      after
        Process.delete({:taskweft_hrr_conn, pool})
      end
    end)
    |> case do
      {:ok, result}  -> {:ok, result}
      {:error, err}  -> {:error, err}
    end
  end

  def in_transaction?(%{store: {pool, _dim}}),
    do: Process.get({:taskweft_hrr_conn, pool}) != nil

  # rollback/2 signals transaction/3 via throw; Postgrex.transaction catches it.
  def rollback(_meta, value), do: throw({:ecto_rollback, value})

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp fields_to_string_map(fields) do
    Map.new(fields, fn {k, v} -> {to_string(k), v} end)
  end

  defp resolve_id(fields) do
    Keyword.get(fields, :id) ||
      Keyword.get(fields, :binary_id) ||
      generate_uuid()
  end

  # Crypto-based UUID v4 that works with or without Ecto loaded.
  defp generate_uuid do
    if Code.ensure_loaded?(Ecto.UUID) do
      apply(Ecto.UUID, :generate, [])
    else
      hex = :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
      <<a::binary-8, b::binary-4, c::binary-4, d::binary-4, e::binary-12>> = hex
      "#{a}-#{b}-#{c}-#{d}-#{e}"
    end
  end

  # Build a flat list of field values in select order.
  # row is either a plain map (no joins) or a list of maps indexed by binding.
  # Falls back to the raw row for unrecognised select expressions.
  defp row_to_list(row, %{select: %{expr: {:&, [], [idx]}}}) do
    binding(row, idx)
  end

  defp row_to_list(row, %{select: %{expr: {:{}, [], refs}}}) do
    Enum.map(refs, fn {:., [], [{:&, [], [idx]}, field]} ->
      Map.get(binding(row, idx), to_string(field))
    end)
  end

  defp row_to_list(row, %{select: %{expr: {:., [], [{:&, [], [idx]}, field]}}}) do
    [Map.get(binding(row, idx), to_string(field))]
  end

  defp row_to_list(row, _query), do: row

  # Resolve a binding index from a row.  A plain map is always binding 0.
  defp binding(row, 0) when is_map(row), do: row
  defp binding(rows, idx) when is_list(rows), do: Enum.at(rows, idx, %{})
  defp binding(_row, _idx), do: %{}
end
