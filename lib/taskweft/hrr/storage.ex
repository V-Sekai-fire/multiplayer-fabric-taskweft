defmodule Taskweft.HRR.Storage do
  @moduledoc """
  GenServer that stores Ecto records as HRR phase-vector superpositions.

  ## Storage model

  Each record is encoded as a **bundle of role-filler bindings**:

      record_vec = bundle([ bind(role("field"), encode(value))
                            for each {field, value} in the record ])

  A whole table's memory is the bundle of all its record vectors:

      table_mem = bundle([record_vec_1, record_vec_2, ...])

  ## Operations

  * **insert** — encode record → add to catalog → rebuild table bundle
  * **delete** — remove from catalog → rebuild table bundle
  * **all**    — return raw catalog entries
  * **probe_field(table, field, value, limit)** — rank catalog entries by
    `similarity(encode(entry[field]), encode(value))`, guided by
    `unbind(table_mem, role(field))` as a confirmatory probe
  * **probe_text(table, query, limit)** — rank entries by
    `similarity(query_vec, entry_vec)` (full-record similarity)
  """

  use GenServer

  alias Taskweft.NIF

  # catalog: %{source => %{id => %{fields: %{String.t => term}, vector: binary}}}
  # bundles: %{source => binary}   (superposition of all record vectors)
  defstruct dim: 1024, catalog: %{}, bundles: %{}

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    dim  = Keyword.get(opts, :dim, 1024)
    GenServer.start_link(__MODULE__, %__MODULE__{dim: dim}, name: name)
  end

  def child_spec(opts) do
    %{
      id:      Keyword.get(opts, :name, __MODULE__),
      start:   {__MODULE__, :start_link, [opts]},
      type:    :worker,
      restart: :permanent
    }
  end

  @doc "Insert a record. Encodes fields as HRR and bundles into table memory."
  def insert(srv, source, id, fields_map),
    do: GenServer.call(srv, {:insert, source, id, fields_map})

  @doc "Fetch a single record's raw field map by id, or nil."
  def get(srv, source, id),
    do: GenServer.call(srv, {:get, source, id})

  @doc "Delete a record and rebuild table memory."
  def delete(srv, source, id),
    do: GenServer.call(srv, {:delete, source, id})

  @doc "Return all records for a source as a list of field maps (id injected)."
  def all(srv, source),
    do: GenServer.call(srv, {:all, source})

  @doc """
  Rank catalog entries for `source` by similarity of `entry[field]` to `value`.

  Uses `unbind(table_mem, role(field))` as an approximate probe, then re-ranks
  by direct per-entry similarity for precision.  Returns up to `limit` entries.
  """
  def probe_field(srv, source, field, value, limit),
    do: GenServer.call(srv, {:probe_field, source, field, value, limit})

  @doc """
  Rank all catalog entries by full-record similarity to `query_text`.

  Encodes `query_text` with `hrr_encode_text`, then ranks entries by
  `similarity(query_vec, entry_vec)`.  Returns up to `limit` entries.
  """
  def probe_text(srv, source, query_text, limit),
    do: GenServer.call(srv, {:probe_text, source, query_text, limit})

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:insert, source, id, fields_map}, _from, %{dim: dim} = state) do
    vector = encode_record(fields_map, dim)

    source_catalog =
      state.catalog
      |> Map.get(source, %{})
      |> Map.put(id, %{fields: fields_map, vector: vector})

    catalog = Map.put(state.catalog, source, source_catalog)
    bundles = rebuild_bundle(catalog, state.bundles, source)
    {:reply, :ok, %{state | catalog: catalog, bundles: bundles}}
  end

  def handle_call({:get, source, id}, _from, state) do
    result =
      state.catalog
      |> Map.get(source, %{})
      |> Map.get(id)

    {:reply, result && result.fields, state}
  end

  def handle_call({:delete, source, id}, _from, state) do
    source_catalog =
      state.catalog
      |> Map.get(source, %{})
      |> Map.delete(id)

    catalog = Map.put(state.catalog, source, source_catalog)
    bundles = rebuild_bundle(catalog, state.bundles, source)
    {:reply, :ok, %{state | catalog: catalog, bundles: bundles}}
  end

  def handle_call({:all, source}, _from, state) do
    entries =
      state.catalog
      |> Map.get(source, %{})
      |> Enum.map(fn {id, entry} -> Map.put(entry.fields, "__id__", id) end)

    {:reply, entries, state}
  end

  def handle_call({:probe_field, source, field, value, limit}, _from, %{dim: dim} = state) do
    entries  = Map.get(state.catalog, source, %{})
    _bundle  = Map.get(state.bundles, source)   # kept for future probe-guided pre-filter

    results =
      if map_size(entries) == 0 do
        []
      else
        value_phases = NIF.hrr_encode_text(to_string(value), dim)
        field_s      = to_string(field)

        entries
        |> Enum.map(fn {id, entry} ->
          raw     = Map.get(entry.fields, field_s, "")
          phases  = NIF.hrr_encode_text(to_string(raw), dim)
          sim     = NIF.hrr_similarity(value_phases, phases)
          {sim, Map.put(entry.fields, "__id__", id)}
        end)
        |> top_n(limit)
      end

    {:reply, results, state}
  end

  def handle_call({:probe_text, source, query_text, limit}, _from, %{dim: dim} = state) do
    entries = Map.get(state.catalog, source, %{})

    results =
      if map_size(entries) == 0 do
        []
      else
        query_phases = NIF.hrr_encode_text(query_text, dim)

        entries
        |> Enum.map(fn {id, entry} ->
          entry_phases = NIF.hrr_bytes_to_phases(entry.vector, 0)
          sim          = NIF.hrr_similarity(query_phases, entry_phases)
          {sim, Map.put(entry.fields, "__id__", id)}
        end)
        |> top_n(limit)
      end

    {:reply, results, state}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Encode a record as a bundle of role-filler bindings:
  #   bundle([ bind(role(field_name), encode_text(value)) for each field ])
  defp encode_record(fields_map, dim) do
    bindings =
      Enum.map(fields_map, fn {field, value} ->
        role_phases  = NIF.hrr_encode_atom(to_string(field), dim)
        role_bytes   = NIF.hrr_phases_to_bytes(role_phases)
        filler_phases = NIF.hrr_encode_text(to_string(value), dim)
        filler_bytes  = NIF.hrr_phases_to_bytes(filler_phases)
        NIF.hrr_bind(role_bytes, filler_bytes)
      end)

    case bindings do
      []       -> NIF.hrr_phases_to_bytes(List.duplicate(0.0, dim))
      [single] -> single
      many     -> NIF.hrr_bundle(many)
    end
  end

  # Rebuild the table bundle from its catalog entries.
  defp rebuild_bundle(catalog, bundles, source) do
    entries = Map.get(catalog, source, %{})

    if map_size(entries) == 0 do
      Map.delete(bundles, source)
    else
      vectors = Enum.map(entries, fn {_id, e} -> e.vector end)
      bundle  = NIF.hrr_bundle(vectors)
      Map.put(bundles, source, bundle)
    end
  end

  # Sort by descending similarity and take top n, stripping the score.
  defp top_n(scored, n) do
    scored
    |> Enum.sort_by(fn {sim, _} -> -sim end)
    |> Enum.take(n)
    |> Enum.map(fn {_sim, fields} -> fields end)
  end
end
