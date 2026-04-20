defmodule Taskweft.HRR.Adapter do
  @moduledoc """
  Ecto adapter backed by Holographic Reduced Representations, persisted to SQLite.

  ## What HRR replaces SQL with

  | SQL concept           | HRR operation                                                  |
  |-----------------------|----------------------------------------------------------------|
  | Table                 | Per-source bundle (superposition of record vectors in SQLite)  |
  | INSERT                | `bundle(table_mem, bind(role(field), encode(value)))`          |
  | DELETE                | Remove record vector, rebuild bundle                           |
  | WHERE field = value   | exact in-memory equality after full scan                       |
  | WHERE LIKE %query%    | `probe_field` → cosine-rank by `encode(stripped_pattern)`      |
  | SELECT *              | Return deserialized JSON rows from SQLite                      |
  | UPDATE                | DELETE + INSERT                                                |

  ## Repo configuration

      config :my_app, MyApp.Repo,
        adapter: Taskweft.HRR.Adapter,
        hrr_dim: 1024,                    # vector dimension (default 1024)
        name: MyApp.HRRStorage,           # GenServer name
        db_path: "/var/data/hrr.db"       # SQLite path (default ~/.taskweft/<name>.db)

  ## Caveats

  * Records are persisted in SQLite as JSON; vectors are stored as BLOB columns.
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
    dim     = Keyword.get(config, :hrr_dim, 1024)
    name    = Keyword.get(config, :name, Storage)
    db_path = Keyword.get(config, :db_path)

    storage_opts =
      [name: name, dim: dim]
      |> then(fn opts -> if db_path, do: Keyword.put(opts, :db_path, db_path), else: opts end)

    child_spec = Storage.child_spec(storage_opts)
    meta       = %{adapter: __MODULE__, storage: name, dim: dim}
    {:ok, child_spec, meta}
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
    %{storage: srv} = meta
    source = schema_meta.source

    id         = resolve_id(fields)
    fields_map = fields_to_string_map(fields)

    :ok = Storage.insert(srv, source, id, fields_map)

    returning_vals = Enum.map(returning, fn f -> {f, Keyword.get(fields, f)} end)
    {:ok, returning_vals}
  end

  def insert_all(meta, schema_meta, _header, rows, _on_conflict, _returning, _placeholders, _opts) do
    %{storage: srv} = meta
    source = schema_meta.source

    :ok = Storage.begin_transaction(srv)
    try do
      count =
        Enum.reduce(rows, 0, fn fields, acc ->
          id         = resolve_id(fields)
          fields_map = fields_to_string_map(fields)
          :ok = Storage.insert(srv, source, id, fields_map)
          acc + 1
        end)
      :ok = Storage.commit_transaction(srv)
      {count, nil}
    rescue
      e ->
        Storage.rollback_transaction(srv)
        reraise e, __STACKTRACE__
    end
  end

  def update(meta, schema_meta, fields, filters, returning, _opts) do
    %{storage: srv} = meta
    source = schema_meta.source
    id     = Keyword.get(filters, :id) || Keyword.get(filters, :binary_id)

    case Storage.get(srv, source, id) do
      nil ->
        {:error, :stale}

      existing ->
        updates = fields_to_string_map(fields)
        merged  = Map.merge(existing, updates)

        :ok = Storage.begin_transaction(srv)
        :ok = Storage.delete(srv, source, id)
        :ok = Storage.insert(srv, source, id, merged)
        :ok = Storage.commit_transaction(srv)

        returning_vals = Enum.map(returning, fn f -> {f, Map.get(merged, to_string(f))} end)
        {:ok, returning_vals}
    end
  end

  def delete(meta, schema_meta, filters, _returning, _opts) do
    %{storage: srv} = meta
    source = schema_meta.source
    id     = Keyword.get(filters, :id) || Keyword.get(filters, :binary_id)

    case Storage.get(srv, source, id) do
      nil    -> {:error, :stale}
      _entry ->
        :ok = Storage.delete(srv, source, id)
        {:ok, []}
    end
  end

  # ---------------------------------------------------------------------------
  # Ecto.Adapter.Queryable callbacks
  # ---------------------------------------------------------------------------

  def prepare(operation, query), do: {:nocache, {operation, query}}

  def execute(meta, _query_meta, {:nocache, {:all, query}}, params, opts) do
    %{storage: srv} = meta
    rows = Query.execute(srv, :all, query, params, opts)
    {length(rows), Enum.map(rows, &row_to_list(&1, query))}
  end

  def execute(_meta, _query_meta, {:nocache, {_op, _query}}, _params, _opts) do
    {0, []}
  end

  def stream(meta, _query_meta, {:nocache, {:all, query}}, params, opts) do
    %{storage: srv} = meta

    Stream.resource(
      fn -> Query.execute(srv, :all, query, params, opts) end,
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

  def transaction(%{storage: srv}, _opts, fun) do
    :ok = Storage.begin_transaction(srv)

    try do
      result = fun.()
      :ok = Storage.commit_transaction(srv)
      {:ok, result}
    rescue
      exception ->
        :ok = Storage.rollback_transaction(srv)
        reraise exception, __STACKTRACE__
    catch
      :throw, {:ecto_rollback, value} ->
        :ok = Storage.rollback_transaction(srv)
        {:error, value}
    end
  end

  def in_transaction?(%{storage: srv}), do: Storage.in_transaction?(srv)

  # rollback/2 signals transaction/3 via throw; transaction/3 calls rollback_transaction.
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
      Ecto.UUID.generate()
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
