# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.Blackboard do
  @moduledoc """
  Stateful Blackboard — HRR-backed KV store for within-episode task state.

  Each entry is stored as an HRR binding (value ⊗ key) in an ETS table.
  Exact lookup uses the key directly; the HRR bytes enable future semantic
  similarity search via `Taskweft.hrr_similarity/2`.

  HRR is the authoritative blackboard — this module is the sole write path.
  """

  @table __MODULE__

  defp ensure_table do
    case :ets.whereis(@table) do
      :undefined -> :ets.new(@table, [:named_table, :public, :set])
      _ -> @table
    end
  end

  @spec put(term(), term()) :: :ok
  def put(key, value) do
    ensure_table()
    key_str = to_string(key)
    val_str = if is_binary(value), do: value, else: Jason.encode!(value)
    hrr_bytes = Taskweft.hrr_encode_binding(val_str, key_str)
    :ets.insert(@table, {key, hrr_bytes, value})
    :ok
  end

  @spec get(term()) :: term()
  def get(key) do
    ensure_table()

    case :ets.lookup(@table, key) do
      [{^key, _hrr_bytes, value}] -> value
      [] -> nil
    end
  end

  @spec search(String.t()) :: [{term(), float()}]
  def search(query_text) do
    ensure_table()
    query_phases = Taskweft.hrr_encode_text(query_text)

    :ets.tab2list(@table)
    |> Enum.map(fn {key, hrr_bytes, _value} ->
      sim = Taskweft.hrr_similarity(query_phases, Taskweft.hrr_bytes_to_phases(hrr_bytes))
      {key, sim}
    end)
    |> Enum.sort_by(fn {_, sim} -> -sim end)
  end
end
