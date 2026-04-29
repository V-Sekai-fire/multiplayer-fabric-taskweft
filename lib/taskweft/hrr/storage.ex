defmodule Taskweft.HRR.Storage do
  @moduledoc """
  HRR storage backed by in-memory Map with JSON-LD file persistence.

  Delegates all state to `Taskweft.HRR.TableServer`. Records and bundles
  are persisted as `<source>.jsonld` files using the KHR interactivity
  context (`khr:planning/hrr/`).

  ## Store type

      @type store :: {server :: atom(), dim :: pos_integer()}
  """

  alias Taskweft.HRR.TableServer

  @type store :: {atom(), pos_integer()}

  @doc false
  def ensure_schema!(_), do: :ok

  # ---------------------------------------------------------------------------
  # CRUD
  # ---------------------------------------------------------------------------

  @spec insert(store(), String.t(), term(), map()) :: :ok
  def insert({srv, dim}, source, id, fields_map) do
    vec = build_record_vector(fields_map, dim)
    :ok = TableServer.put(srv, source, to_string(id), fields_map, vec)
    rebuild_bundle({srv, dim}, source)
  end

  @spec get(store(), String.t(), term()) :: map() | nil
  def get({srv, _dim}, source, id),
    do: TableServer.get(srv, source, to_string(id))

  @spec delete(store(), String.t(), term()) :: :ok
  def delete({srv, dim} = store, source, id) do
    :ok = TableServer.remove(srv, source, to_string(id))
    rebuild_bundle(store, source)
  end

  @spec all(store(), String.t()) :: [map()]
  def all({srv, _dim}, source),
    do: TableServer.all(srv, source)

  # ---------------------------------------------------------------------------
  # Metadata
  # ---------------------------------------------------------------------------

  @spec bundle(store(), String.t()) :: binary() | nil
  def bundle({srv, _dim}, source),
    do: TableServer.bundle(srv, source)

  @spec record_count(store(), String.t()) :: non_neg_integer()
  def record_count({srv, _dim}, source),
    do: TableServer.record_count(srv, source)

  # ---------------------------------------------------------------------------
  # Probe
  # ---------------------------------------------------------------------------

  @spec probe_field(store(), String.t(), atom() | String.t(), String.t(), keyword()) ::
          [{float(), map()}]
  def probe_field({srv, dim}, source, field, query_text, opts \\ []) do
    threshold = Keyword.get(opts, :threshold, 0.0)
    limit     = Keyword.get(opts, :limit, 50)

    role_bytes  = Taskweft.NIF.hrr_encode_atom("role_#{field}", dim) |> Taskweft.NIF.hrr_phases_to_bytes()
    query_phases = Taskweft.NIF.hrr_encode_text(query_text, dim)

    TableServer.all_vecs(srv, source)
    |> Enum.map(fn {fields, vec} ->
      unbound = Taskweft.NIF.hrr_unbind(vec, role_bytes)
      sim     = Taskweft.NIF.hrr_similarity(Taskweft.NIF.hrr_bytes_to_phases(unbound, 0), query_phases)
      {sim, fields}
    end)
    |> Enum.filter(fn {sim, _} -> sim >= threshold end)
    |> Enum.sort_by(fn {sim, _} -> -sim end)
    |> Enum.take(limit)
  rescue
    _ -> []
  end

  @spec probe_text(store(), String.t(), String.t(), keyword()) :: [{float(), map()}]
  def probe_text({srv, dim}, source, query_text, opts \\ []) do
    threshold    = Keyword.get(opts, :threshold, 0.0)
    limit        = Keyword.get(opts, :limit, 50)
    query_phases = Taskweft.NIF.hrr_encode_text(query_text, dim)

    TableServer.all_vecs(srv, source)
    |> Enum.map(fn {fields, vec} ->
      sim = Taskweft.NIF.hrr_similarity(Taskweft.NIF.hrr_bytes_to_phases(vec, 0), query_phases)
      {sim, fields}
    end)
    |> Enum.filter(fn {sim, _} -> sim >= threshold end)
    |> Enum.sort_by(fn {sim, _} -> -sim end)
    |> Enum.take(limit)
  rescue
    _ -> []
  end

  @spec vectors_for_join(store(), String.t(), String.t()) :: [{binary(), map()}]
  def vectors_for_join({srv, dim}, source, join_field) do
    role_bytes = Taskweft.NIF.hrr_encode_atom("role_#{join_field}", dim) |> Taskweft.NIF.hrr_phases_to_bytes()

    TableServer.all_vecs(srv, source)
    |> Enum.map(fn {fields, vec} ->
      {Taskweft.NIF.hrr_unbind(vec, role_bytes), fields}
    end)
  rescue
    _ -> []
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp build_record_vector(fields_map, dim) do
    bindings = Enum.map(fields_map, fn {field, value} ->
      role = Taskweft.NIF.hrr_encode_atom("role_#{field}", dim) |> Taskweft.NIF.hrr_phases_to_bytes()
      val  = Taskweft.NIF.hrr_encode_text(to_string(value), dim) |> Taskweft.NIF.hrr_phases_to_bytes()
      Taskweft.NIF.hrr_bind(role, val)
    end)

    case bindings do
      []     -> nil
      [one]  -> one
      many   -> Taskweft.NIF.hrr_bundle(many)
    end
  rescue
    _ -> nil
  end

  defp rebuild_bundle({srv, _dim}, source) do
    vecs = TableServer.all_vecs(srv, source) |> Enum.map(fn {_fields, vec} -> vec end)

    case vecs do
      [] ->
        TableServer.delete_bundle(srv, source)

      [one] ->
        TableServer.set_bundle(srv, source, one, 1)

      many ->
        TableServer.set_bundle(srv, source, Taskweft.NIF.hrr_bundle(many), length(many))
    end
  rescue
    _ -> :ok
  end
end
