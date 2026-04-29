defmodule Taskweft.HRR.TableServer do
  @moduledoc """
  In-memory HRR store with JSON-LD file persistence.

  State shape:
    %{records: %{{source, id} => {fields_map, vec_or_nil}},
      bundles: %{source => {bundle_vec, count}},
      dim: pos_integer(),
      dir: Path.t()}

  One `.jsonld` file per source is written to *dir* on every mutation.
  Files are loaded at startup if *dir* already contains them.
  """

  use GenServer

  # ---------------------------------------------------------------------------
  # Client API called by Storage
  # ---------------------------------------------------------------------------

  def get(srv, source, id),       do: GenServer.call(srv, {:get, source, id})
  def all(srv, source),           do: GenServer.call(srv, {:all, source})
  def bundle(srv, source),        do: GenServer.call(srv, {:bundle, source})
  def record_count(srv, source),  do: GenServer.call(srv, {:record_count, source})
  def dim(srv),                   do: GenServer.call(srv, :dim)

  def put(srv, source, id, fields, vec),
    do: GenServer.call(srv, {:put, source, id, fields, vec})

  def remove(srv, source, id),
    do: GenServer.call(srv, {:remove, source, id})

  def set_bundle(srv, source, vec, count),
    do: GenServer.call(srv, {:set_bundle, source, vec, count})

  def delete_bundle(srv, source),
    do: GenServer.call(srv, {:delete_bundle, source})

  def all_vecs(srv, source),
    do: GenServer.call(srv, {:all_vecs, source})

  # ---------------------------------------------------------------------------
  # GenServer lifecycle
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    {name, rest} = Keyword.pop!(opts, :name)
    GenServer.start_link(__MODULE__, {name, rest}, name: name)
  end

  @impl GenServer
  def init({_name, opts}) do
    dim = Keyword.get(opts, :hrr_dim, 1024)
    dir = Keyword.get(opts, :dets_path, Path.join(System.tmp_dir!(), "hrr_store"))
    File.mkdir_p!(dir)
    state = %{records: %{}, bundles: %{}, dim: dim, dir: dir}
    {:ok, load_all(state)}
  end

  # ---------------------------------------------------------------------------
  # Handlers
  # ---------------------------------------------------------------------------

  @impl GenServer
  def handle_call({:get, source, id}, _from, state) do
    result = case Map.get(state.records, {source, id}) do
      {fields, _vec} -> fields
      nil -> nil
    end
    {:reply, result, state}
  end

  def handle_call({:all, source}, _from, state) do
    rows = for {{s, _id}, {fields, _vec}} <- state.records, s == source, do: fields
    {:reply, rows, state}
  end

  def handle_call({:all_vecs, source}, _from, state) do
    rows = for {{s, _id}, {fields, vec}} <- state.records, s == source, not is_nil(vec),
              do: {fields, vec}
    {:reply, rows, state}
  end

  def handle_call({:bundle, source}, _from, state) do
    result = case Map.get(state.bundles, source) do
      {vec, _count} -> vec
      nil -> nil
    end
    {:reply, result, state}
  end

  def handle_call({:record_count, source}, _from, state) do
    count = case Map.get(state.bundles, source) do
      {_vec, count} -> count
      nil -> 0
    end
    {:reply, count, state}
  end

  def handle_call(:dim, _from, state), do: {:reply, state.dim, state}

  def handle_call({:put, source, id, fields, vec}, _from, state) do
    new_records = Map.put(state.records, {source, id}, {fields, vec})
    new_state   = %{state | records: new_records}
    persist(new_state, source)
    {:reply, :ok, new_state}
  end

  def handle_call({:remove, source, id}, _from, state) do
    new_records = Map.delete(state.records, {source, id})
    new_state   = %{state | records: new_records}
    persist(new_state, source)
    {:reply, :ok, new_state}
  end

  def handle_call({:set_bundle, source, vec, count}, _from, state) do
    new_bundles = Map.put(state.bundles, source, {vec, count})
    new_state   = %{state | bundles: new_bundles}
    persist(new_state, source)
    {:reply, :ok, new_state}
  end

  def handle_call({:delete_bundle, source}, _from, state) do
    new_bundles = Map.delete(state.bundles, source)
    new_state   = %{state | bundles: new_bundles}
    persist(new_state, source)
    {:reply, :ok, new_state}
  end

  # ---------------------------------------------------------------------------
  # JSON-LD persistence
  # ---------------------------------------------------------------------------

  defp persist(state, source) do
    records = for {{s, id}, {fields, vec}} <- state.records, s == source do
      %{"@id" => id, "fields" => stringify_keys(fields),
        "vector" => encode_vec(vec)}
    end

    {bvec, bcount} = Map.get(state.bundles, source, {nil, 0})

    doc = %{
      "@context" => %{
        "khr"    => "https://registry.khronos.org/glTF/extensions/2.0/KHR_interactivity/",
        "domain" => "khr:planning/domain/",
        "hrr"    => "khr:planning/hrr/"
      },
      "@type"        => "hrr:Store",
      "hrr:source"   => source,
      "hrr:records"  => records,
      "hrr:bundle"   => encode_vec(bvec),
      "hrr:count"    => bcount
    }

    path = jsonld_path(state.dir, source)
    File.write!(path, Jason.encode!(doc, pretty: true))
  end

  defp load_all(state) do
    state.dir
    |> File.ls!()
    |> Enum.filter(&String.ends_with?(&1, ".jsonld"))
    |> Enum.reduce(state, fn file, acc ->
      path = Path.join(acc.dir, file)
      load_file(acc, path)
    end)
  rescue
    _ -> state
  end

  defp load_file(state, path) do
    doc     = path |> File.read!() |> Jason.decode!()
    source  = doc["hrr:source"] || doc["source"]
    records = doc["hrr:records"] || doc["records"] || []
    bvec    = decode_vec(doc["hrr:bundle"] || doc["bundle"])
    bcount  = doc["hrr:count"] || doc["record_count"] || 0

    new_records =
      Enum.reduce(records, state.records, fn r, acc ->
        id  = r["@id"]
        vec = decode_vec(r["vector"])
        Map.put(acc, {source, id}, {atomize_keys(r["fields"] || %{}), vec})
      end)

    new_bundles =
      if bvec, do: Map.put(state.bundles, source, {bvec, bcount}),
               else: state.bundles

    %{state | records: new_records, bundles: new_bundles}
  rescue
    _ -> state
  end

  defp jsonld_path(dir, source),
    do: Path.join(dir, "#{source}.jsonld")

  defp encode_vec(nil),   do: nil
  defp encode_vec(bytes), do: Base.encode64(bytes)

  defp decode_vec(nil),    do: nil
  defp decode_vec(""),     do: nil
  defp decode_vec(b64),    do: Base.decode64!(b64)

  defp stringify_keys(map),
    do: Map.new(map, fn {k, v} -> {to_string(k), v} end)

  defp atomize_keys(map),
    do: Map.new(map, fn {k, v} -> {String.to_atom(k), v} end)
end
