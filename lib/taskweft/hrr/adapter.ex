defmodule Taskweft.HRR.Adapter do
  @moduledoc """
  Ecto adapter backed by Holographic Reduced Representations.

  ## What HRR replaces SQL with

  | SQL concept           | HRR operation                                        |
  |-----------------------|------------------------------------------------------|
  | Table                 | Per-source bundle (superposition of record vectors)  |
  | INSERT                | `bundle(table_mem, bind(role(field), encode(value)))` |
  | DELETE                | Remove record vector, rebuild bundle                 |
  | WHERE field = value   | `probe_field` → cosine-rank by `encode(value)`       |
  | WHERE LIKE %query%    | `probe_text`  → cosine-rank by `encode(query)`       |
  | SELECT *              | Return raw catalog entries                           |
  | UPDATE                | DELETE + INSERT                                      |

  ## Repo configuration

      config :my_app, MyApp.Repo,
        adapter: Taskweft.HRR.Adapter,
        hrr_dim: 1024,          # vector dimension (default 1024)
        name: MyApp.HRRStorage  # GenServer name (default module name)

  ## Caveats

  * This adapter is **in-process and non-persistent** — state lives in the
    GenServer's memory.  Pair with `Taskweft.Store` if you need SQLite
    persistence.
  * Queries beyond `==`, `!=`, `like`, and `ilike` fall back to in-memory
    linear scan.
  * No transactions, joins, or aggregates.
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable

  alias Taskweft.HRR.{Query, Storage}

  # ---------------------------------------------------------------------------
  # Ecto.Adapter
  # ---------------------------------------------------------------------------

  @impl Ecto.Adapter
  defmacro __before_compile__(_env), do: :ok

  @impl Ecto.Adapter
  def ensure_all_started(_config, _type), do: {:ok, []}

  @impl Ecto.Adapter
  def init(config) do
    dim  = Keyword.get(config, :hrr_dim, 1024)
    name = Keyword.get(config, :name, Storage)

    child_spec = Storage.child_spec(name: name, dim: dim)
    meta       = %{adapter: __MODULE__, storage: name, dim: dim}
    {:ok, child_spec, meta}
  end

  @impl Ecto.Adapter
  def checkout(_meta, _config, fun), do: fun.()

  @impl Ecto.Adapter
  def checked_out?(_meta), do: false

  @impl Ecto.Adapter
  def loaders(:binary_id, type), do: [Ecto.UUID, type]
  def loaders(_primitive, type), do: [type]

  @impl Ecto.Adapter
  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(_primitive, type), do: [type]

  # ---------------------------------------------------------------------------
  # Ecto.Adapter.Schema
  # ---------------------------------------------------------------------------

  @impl Ecto.Adapter.Schema
  def autogenerate(:id),        do: nil
  def autogenerate(:embed_id),  do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  @impl Ecto.Adapter.Schema
  def insert(meta, schema_meta, fields, _on_conflict, returning, _opts) do
    %{storage: srv} = meta
    source = schema_meta.source

    id         = resolve_id(fields)
    fields_map = fields_to_string_map(fields)

    :ok = Storage.insert(srv, source, id, fields_map)

    returning_vals = Enum.map(returning, fn f -> {f, Keyword.get(fields, f)} end)
    {:ok, returning_vals}
  end

  @impl Ecto.Adapter.Schema
  def insert_all(meta, schema_meta, _header, rows, _on_conflict, _returning, _placeholders, _opts) do
    %{storage: srv} = meta
    source = schema_meta.source

    count =
      Enum.reduce(rows, 0, fn fields, acc ->
        id         = resolve_id(fields)
        fields_map = fields_to_string_map(fields)
        :ok = Storage.insert(srv, source, id, fields_map)
        acc + 1
      end)

    {count, nil}
  end

  @impl Ecto.Adapter.Schema
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

        :ok = Storage.delete(srv, source, id)
        :ok = Storage.insert(srv, source, id, merged)

        returning_vals = Enum.map(returning, fn f -> {f, Map.get(merged, to_string(f))} end)
        {:ok, returning_vals}
    end
  end

  @impl Ecto.Adapter.Schema
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
  # Ecto.Adapter.Queryable
  # ---------------------------------------------------------------------------

  @impl Ecto.Adapter.Queryable
  def prepare(operation, query), do: {:nocache, {operation, query}}

  @impl Ecto.Adapter.Queryable
  def execute(meta, _query_meta, {:nocache, {:all, query}}, params, opts) do
    %{storage: srv, dim: _dim} = meta
    rows = Query.execute(srv, :all, query, params, opts)
    {length(rows), Enum.map(rows, &row_to_list(&1, query))}
  end

  def execute(_meta, _query_meta, {:nocache, {op, _query}}, _params, _opts) do
    raise "#{__MODULE__}: unsupported operation #{inspect(op)}"
  end

  @impl Ecto.Adapter.Queryable
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

  def stream(_meta, _query_meta, {:nocache, {op, _}}, _params, _opts) do
    raise "#{__MODULE__}: unsupported stream operation #{inspect(op)}"
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Convert a keyword-list of Ecto fields to a string-keyed map.
  defp fields_to_string_map(fields) do
    Map.new(fields, fn {k, v} -> {to_string(k), v} end)
  end

  # Extract the record id from a field keyword list.
  defp resolve_id(fields) do
    Keyword.get(fields, :id) ||
      Keyword.get(fields, :binary_id) ||
      Ecto.UUID.generate()
  end

  # Build a flat list of field values in the order the query selects them.
  # Falls back to returning the raw map when the select is "all fields".
  defp row_to_list(fields_map, %{select: %{expr: {:&, [], [0]}}}) do
    # `select all` — Ecto will call loaders on the raw map
    fields_map
  end

  defp row_to_list(fields_map, %{select: %{expr: {:{}, [], refs}}}) do
    Enum.map(refs, fn {:., [], [{:&, [], [0]}, field]} ->
      Map.get(fields_map, to_string(field))
    end)
  end

  defp row_to_list(fields_map, %{select: %{expr: {:., [], [{:&, [], [0]}, field]}}}) do
    [Map.get(fields_map, to_string(field))]
  end

  defp row_to_list(fields_map, _query), do: fields_map
end
