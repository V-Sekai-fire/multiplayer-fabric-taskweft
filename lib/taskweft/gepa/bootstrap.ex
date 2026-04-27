# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.Bootstrap do
  @moduledoc """
  BootstrapFewShot — HRR-indexed experience buffer of successful episode traces.

  Each trace is encoded as an HRR text vector and stored in ETS. Retrieval
  returns all traces; `similar/1` ranks them by HRR cosine similarity to a
  query string so prompts can inject the most relevant few-shot examples.
  """

  @table __MODULE__

  defp ensure_table do
    case :ets.whereis(@table) do
      :undefined -> :ets.new(@table, [:named_table, :public, :duplicate_bag])
      _ -> @table
    end
  end

  @spec store(map()) :: :ok
  def store(trace) when is_map(trace) do
    ensure_table()
    content = Jason.encode!(trace)
    hrr_bytes = Taskweft.hrr_phases_to_bytes(Taskweft.hrr_encode_text(content))
    :ets.insert(@table, {hrr_bytes, trace})
    :ok
  end

  @spec get() :: [map()]
  def get do
    ensure_table()
    :ets.tab2list(@table) |> Enum.map(fn {_hrr, trace} -> trace end)
  end

  @spec similar(String.t(), non_neg_integer()) :: [map()]
  def similar(query_text, top_n \\ 3) do
    ensure_table()
    query_phases = Taskweft.hrr_encode_text(query_text)

    :ets.tab2list(@table)
    |> Enum.map(fn {hrr_bytes, trace} ->
      sim = Taskweft.hrr_similarity(query_phases, Taskweft.hrr_bytes_to_phases(hrr_bytes))
      {sim, trace}
    end)
    |> Enum.sort_by(fn {sim, _} -> -sim end)
    |> Enum.take(top_n)
    |> Enum.map(fn {_, trace} -> trace end)
  end
end
