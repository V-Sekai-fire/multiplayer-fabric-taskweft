# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.Compose do
  @moduledoc """
  Composes several RECTGTN documents into one.

  `Taskweft.CLI` already merged one domain with one problem. That merge
  was private and pairwise, so a caller with three documents had no way
  to reach it. This module holds the same rules, makes them public, and
  folds any number of documents left to right.

  ## Why compose

  A pipeline that runs several models is several domains. Each one owns
  its own actions and its own guards, and each one is readable alone. A
  caller that wants the whole pipeline composes them, and the planner
  sees one document.

  The alternative is one large domain per pipeline. Two pipelines that
  share a stage then hold two copies of it, and the copies drift.

  ## The rules

  Later documents win, which makes the fold order meaningful.

  | Field | Rule |
  | ----- | ---- |
  | `variables` | Merge by `name`. The later one replaces. |
  | `actions` | Merge by key. The later one replaces. |
  | `methods` | Merge by key. The later one replaces. |
  | `goal_methods` | Merge by key. The later one replaces. |
  | `todo_list` | A non-empty list replaces. An empty one keeps. |
  | everything else | The first document's value stands. |

  A problem is an ordinary document under these rules. `compose/1` with
  a domain and its problem gives what the CLI pair gave before.

  ## Example

      iex> base = %{"@type" => "domain:Definition", "name" => "base",
      ...>          "actions" => %{"a_one" => %{"params" => []}}}
      iex> extra = %{"@type" => "domain:Definition", "name" => "extra",
      ...>           "actions" => %{"a_two" => %{"params" => []}}}
      iex> {:ok, merged} = Taskweft.Compose.compose([base, extra])
      iex> Map.keys(merged["actions"]) |> Enum.sort()
      ["a_one", "a_two"]
  """

  alias Taskweft.DSL

  @merge_by_key ["methods", "actions", "goal_methods"]

  @type document :: map()

  @doc """
  Folds a list of documents into one, left to right.

  Returns `{:error, reason}` for an empty list, because a composition of
  nothing has no `name` and no `@type`, and the planner rejects it later
  with a worse message.
  """
  @spec compose([document()]) :: {:ok, document()} | {:error, String.t()}
  def compose([]), do: {:error, "compose: no documents to compose"}

  def compose(documents) when is_list(documents) do
    case Enum.find_index(documents, &(not is_map(&1))) do
      nil ->
        {:ok, Enum.reduce(documents, &merge(&2, &1))}

      index ->
        {:error, "compose: document #{index} is not a map"}
    end
  end

  def compose(_other), do: {:error, "compose: expected a list of documents"}

  @doc """
  Composes documents given as source strings.

  `format` takes `"dsl"` for the Elixir DSL, and `"json"` for JSON-LD.
  Returns the composed document as a JSON string, which is what the
  planner takes.

  `:base_format` gives the first document a format of its own. A caller
  that already parsed the base to JSON, and holds the overlays in the
  caller's format, needs that. Without it every document uses `format`.
  """
  @spec compose_strings([String.t()], keyword()) :: {:ok, String.t()} | {:error, String.t()}
  def compose_strings(sources, opts \\ []) when is_list(sources) do
    format = Keyword.get(opts, :format, "dsl")
    base_format = Keyword.get(opts, :base_format, format)

    with {:ok, documents} <- decode_all(sources, format, base_format),
         {:ok, merged} <- compose(documents) do
      case Jason.encode(merged) do
        {:ok, json} -> {:ok, json}
        {:error, error} -> {:error, "compose: encode failed: #{Exception.message(error)}"}
      end
    end
  end

  # ---------- decoding ----------

  defp decode_all(sources, format, base_format) do
    sources
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {source, index}, {:ok, acc} ->
      document_format = if index == 0, do: base_format, else: format

      case decode(source, document_format, index) do
        {:ok, document} -> {:cont, {:ok, [document | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode(source, "dsl", index) do
    with {:ok, json} <- wrap(DSL.compile(source), index),
         {:ok, document} <- wrap(Jason.decode(json), index) do
      {:ok, document}
    end
  end

  defp decode(source, "json", index) do
    wrap(Jason.decode(source), index)
  end

  defp decode(_source, format, _index) do
    {:error, "compose: unknown format #{inspect(format)}, expected \"dsl\" or \"json\""}
  end

  defp wrap({:ok, value}, _index), do: {:ok, value}

  defp wrap({:error, %{__struct__: _} = error}, index),
    do: {:error, "compose: document #{index}: #{Exception.message(error)}"}

  defp wrap({:error, reason}, index), do: {:error, "compose: document #{index}: #{reason}"}

  # ---------- merging ----------

  defp merge(base, overlay) do
    base
    |> merge_variables(overlay)
    |> merge_keyed(overlay)
    |> merge_tasks(overlay)
  end

  # A variable list is keyed by "name", and not by position. A merge
  # that appended would give the planner two entries with one name.
  defp merge_variables(base, overlay) do
    case Map.get(overlay, "variables") do
      nil ->
        base

      overlay_vars when is_list(overlay_vars) ->
        base_vars = base |> Map.get("variables") |> List.wrap()
        replaced = MapSet.new(overlay_vars, &Map.get(&1, "name"))
        kept = Enum.reject(base_vars, &MapSet.member?(replaced, Map.get(&1, "name")))
        Map.put(base, "variables", kept ++ overlay_vars)

      _other ->
        base
    end
  end

  defp merge_keyed(base, overlay) do
    Enum.reduce(@merge_by_key, base, fn key, acc ->
      case Map.get(overlay, key) do
        value when is_map(value) ->
          Map.put(acc, key, Map.merge(Map.get(acc, key, %{}), value))

        _other ->
          acc
      end
    end)
  end

  # An empty todo_list is not a goal of "do nothing". It is a document
  # that states no goal, thus the goal before it stands.
  defp merge_tasks(base, overlay) do
    case Map.get(overlay, "todo_list") do
      tasks when is_list(tasks) and tasks != [] -> Map.put(base, "todo_list", tasks)
      _other -> base
    end
  end
end
