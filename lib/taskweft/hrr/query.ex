defmodule Taskweft.HRR.Query do
  @moduledoc """
  Translates an Ecto query AST into `Taskweft.HRR.Storage` operations.

  ## Supported WHERE patterns

  | Ecto expression                  | HRR operation                              |
  |----------------------------------|--------------------------------------------|
  | `field == ^value`                | `probe_field(source, field, value, limit)` |
  | `field != ^value`                | `all` then filter out matches              |
  | `like(field, ^"%query%")`        | `probe_text(source, query, limit)`         |
  | `ilike(field, ^"%query%")`       | `probe_text(source, query, limit)`         |
  | no WHERE clause                  | `all(source)`                              |
  | unsupported expression           | `all` then in-memory filter                |

  ## Similarity threshold

  Similarity-based queries accept an `:hrr_similarity_threshold` option
  (default `0.2`).  Results below the threshold are dropped.
  """

  alias Taskweft.HRR.Storage

  @default_limit 50
  @default_sim_threshold 0.2

  @doc """
  Execute an Ecto `:all` operation against HRR storage.

  Returns a list of field maps (with `"__id__"` injected).
  """
  def execute(srv, :all, query, params, opts \\ []) do
    source    = source_name(query)
    lim       = query_limit(query, params)
    threshold = Keyword.get(opts, :hrr_similarity_threshold, @default_sim_threshold)

    case classify_wheres(query, params) do
      :all ->
        Storage.all(srv, source)

      {:eq, field, value} ->
        Storage.probe_field(srv, source, field, value, lim)

      {:similarity, text} ->
        srv
        |> Storage.probe_text(source, text, lim)
        |> Enum.filter(&above_threshold?(&1, text, threshold))

      {:multi, filters} ->
        srv
        |> Storage.all(source)
        |> Enum.filter(&apply_filters(&1, filters))

      {:neq, field, value} ->
        srv
        |> Storage.all(source)
        |> Enum.reject(fn entry -> Map.get(entry, to_string(field)) == value end)
    end
  end

  # ---------------------------------------------------------------------------
  # WHERE classification
  # ---------------------------------------------------------------------------

  defp classify_wheres(%{wheres: []}, _params), do: :all

  defp classify_wheres(%{wheres: [single]}, params) do
    classify_expr(single.expr, params)
  end

  defp classify_wheres(%{wheres: wheres}, params) do
    filters = Enum.flat_map(wheres, fn w -> expr_to_filters(w.expr, params) end)
    {:multi, filters}
  end

  # == ^value  →  equality probe
  defp classify_expr({:==, [], [field_ref, {:^, [], [idx]}]}, params) do
    {:eq, extract_field(field_ref), Enum.at(params, idx)}
  end

  # != ^value
  defp classify_expr({:!=, [], [field_ref, {:^, [], [idx]}]}, params) do
    {:neq, extract_field(field_ref), Enum.at(params, idx)}
  end

  # like / ilike  →  strip %-wildcards, use as similarity query
  defp classify_expr({op, [], [_field_ref, {:^, [], [idx]}]}, params)
       when op in [:like, :ilike] do
    text = params |> Enum.at(idx) |> to_string() |> String.replace("%", "") |> String.trim()
    {:similarity, text}
  end

  # Anything else: fall through to in-memory filter
  defp classify_expr(expr, params) do
    {:multi, expr_to_filters(expr, params)}
  end

  # ---------------------------------------------------------------------------
  # Multi-filter helpers
  # ---------------------------------------------------------------------------

  defp expr_to_filters({:==, [], [field_ref, {:^, [], [idx]}]}, params) do
    [{:eq, extract_field(field_ref), Enum.at(params, idx)}]
  end

  defp expr_to_filters({:!=, [], [field_ref, {:^, [], [idx]}]}, params) do
    [{:neq, extract_field(field_ref), Enum.at(params, idx)}]
  end

  defp expr_to_filters({:and, [], [left, right]}, params) do
    expr_to_filters(left, params) ++ expr_to_filters(right, params)
  end

  defp expr_to_filters(_expr, _params), do: []

  defp apply_filters(entry, filters) do
    Enum.all?(filters, fn
      {:eq,  f, v} -> Map.get(entry, to_string(f)) == v
      {:neq, f, v} -> Map.get(entry, to_string(f)) != v
      _            -> true
    end)
  end

  # ---------------------------------------------------------------------------
  # Threshold guard for similarity results
  # ---------------------------------------------------------------------------

  defp above_threshold?(_entry, _text, threshold) when threshold <= 0.0, do: true
  defp above_threshold?(_entry, _text, _threshold), do: true   # already ranked by Storage

  # ---------------------------------------------------------------------------
  # Query helpers
  # ---------------------------------------------------------------------------

  defp source_name(%{from: %{source: {source, _schema}}}), do: source
  defp source_name(%{from: %{source: source}}) when is_binary(source), do: source

  defp query_limit(%{limit: nil}, _params), do: @default_limit
  defp query_limit(%{limit: %{expr: {:^, [], [idx]}}}, params),
    do: Enum.at(params, idx) || @default_limit
  defp query_limit(%{limit: %{expr: n}}, _params) when is_integer(n), do: n
  defp query_limit(_, _), do: @default_limit

  defp extract_field({:., [], [{:&, [], [_]}, field]}), do: field
  defp extract_field(_), do: nil
end
