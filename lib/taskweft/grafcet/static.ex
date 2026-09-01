# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.Grafcet.Static do
  @moduledoc """
  Static analyser for compact GRAFCET documents. Wraps
  `libgrafcet_static` from `3-interactor/taskweft-grafcet-static` via
  the NIF at `priv/grafcet_static_nif.so`.

  Two structural analyses per RFD 2144:

    * `reachable` — step ids reachable from the initial marking
    * `concurrent_pairs` — ordered `[a, b]` pairs that co-occur in
      some reachable marking

  Abstract interpretation is staged (see RFD 2144's staging table).

  ## Example

      iex> {:ok, json} = Taskweft.Grafcet.Static.analyse(File.read!("chart.grafcet.jsonld"))
      iex> Jason.decode!(json)
      %{"reachable" => ["init", "find", ...], "concurrent_pairs" => [["a", "b"]]}
  """

  alias Taskweft.Grafcet.Static.Nif

  @doc "Analyse a compact GRAFCET JSON document. Returns `{:ok, json_reply}`."
  @spec analyse(iodata()) :: {:ok, binary()}
  def analyse(sfc_json), do: Nif.analyse(sfc_json)
end

defmodule Taskweft.Grafcet.Static.Nif do
  @moduledoc false
  @on_load :load

  def load do
    path = :filename.join(:code.priv_dir(:taskweft), ~c"grafcet_static_nif")

    case :erlang.load_nif(path, 0) do
      :ok ->
        :ok

      {:error, reason} ->
        IO.warn("failed to load grafcet_static_nif: #{inspect(reason)}")
        :ok
    end
  end

  def analyse(_sfc_json), do: :erlang.nif_error(:not_loaded)
end
