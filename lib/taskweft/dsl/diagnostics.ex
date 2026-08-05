# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.DSL.Diagnostics do
  @moduledoc """
  Turns a DSL fault into a message a writer can act on.

  Two faults reached callers with almost no information.

  A syntax error reported `"DSL syntax error: syntax error before: "`.
  `Code.string_to_quoted/1` gives the line, the column, and the token,
  and all three were dropped. The trailing colon promised a token that
  never arrived.

  A domain written against an API that does not exist compiled without
  complaint. `use Taskweft.Action` with `def preconditions` looks like a
  planner DSL, and it parses as ordinary Elixir, thus the parser saw a
  module with no `@actions` and returned an empty domain. The planner
  then answered `no_plan`, which names nothing.

  This module makes both say what is wrong and what to write instead.
  """

  # Names that read like a planner API, and are not one. Each entry
  # gives the attribute that does the job.
  @wrong_api %{
    "Taskweft.Action" => "@actions %{name: %{params: [], body: [...]}}",
    "Taskweft.Constraint" => "a guard step in an action body: %{eval: %{...}}",
    "Taskweft.Method" => "@methods %{name: %{params: [], alternatives: [...]}}",
    "Taskweft.Task" => "@todo_list [[\"action_name\"]]",
    "Taskweft.Goal" => "@todo_list [%{goal: [%{pointer: \"/var/key\", eq: true}]}]"
  }

  @doc """
  Formats a `Code.string_to_quoted/1` error with its position and an
  excerpt of the source.
  """
  @spec syntax_error(term(), String.t()) :: String.t()
  def syntax_error({meta, error, token}, source) do
    line = position(meta, :line)
    column = position(meta, :column)

    [
      "DSL syntax error",
      location(line, column),
      ": ",
      describe(error, token),
      excerpt(source, line, column)
    ]
    |> IO.iodata_to_binary()
  end

  def syntax_error(other, _source), do: "DSL syntax error: #{inspect(other)}"

  @doc """
  Returns `{:error, message}` when the source uses an API the DSL does
  not have, and `:ok` when it does not.

  The check is textual on purpose. The wrong API parses as valid
  Elixir, thus no parse result can reveal it.
  """
  @spec check_api(String.t()) :: :ok | {:error, String.t()}
  def check_api(source) when is_binary(source) do
    case Enum.find(@wrong_api, fn {name, _fix} -> source =~ "use #{name}" end) do
      nil ->
        :ok

      {name, fix} ->
        {:error,
         """
         DSL error: `use #{name}` does not exist.

         A RECTGTN domain is one module with module attributes. It has no \
         nested modules, no `use` beyond `Taskweft.DSL`, and no function \
         definitions. `def preconditions`, `def effects`, and `def check` \
         are read by nothing.

         Write this instead:

             #{fix}

         A guard goes in the body as `%{eval: %{type: "math/eq", a: \
         %{pointer_get: "/var/key"}, b: value}}`, and an effect goes in \
         the body as `%{pointer_set: "/var/key", value: value}`.

         See docs/rectgtn.md.\
         """}
    end
  end

  @doc """
  Returns `{:error, message}` when a `todo_list` call, or a method
  subtask, names no action and no method.

  Without this the document compiles, and the planner answers
  `no_plan`. That answer names nothing, and the writer has no way to
  find the typo.
  """
  @spec check_calls(map()) :: :ok | {:error, String.t()}
  def check_calls(%{} = document) do
    defined =
      MapSet.union(
        document |> Map.get("actions", %{}) |> map_keys(),
        document |> Map.get("methods", %{}) |> map_keys()
      )

    unknown =
      document
      |> called_names()
      |> Enum.reject(&MapSet.member?(defined, &1))
      |> Enum.uniq()

    case unknown do
      [] ->
        :ok

      names ->
        {:error,
         "DSL error: #{plural(names)} #{list(names)}, and no action or " <>
           "method has that name. Defined: #{list(Enum.sort(MapSet.to_list(defined)))}."}
    end
  end

  def check_calls(_other), do: :ok

  # ---------- syntax formatting ----------

  defp position(meta, key) when is_list(meta), do: Keyword.get(meta, key)
  defp position(line, :line) when is_integer(line), do: line
  defp position(_meta, _key), do: nil

  defp location(nil, _column), do: ""
  defp location(line, nil), do: " on line #{line}"
  defp location(line, column), do: " on line #{line}, column #{column}"

  # The parser splits its message into a prefix and the token. Joining
  # them is the whole point: "syntax error before: " alone says nothing.
  defp describe(error, token) when is_binary(error) and is_binary(token) do
    String.trim_trailing(error) <> " " <> token
  end

  defp describe({prefix, suffix}, token) when is_binary(prefix) and is_binary(suffix) do
    String.trim_trailing(prefix <> suffix) <> " " <> to_string(token)
  end

  defp describe(error, token), do: "#{inspect(error)} #{inspect(token)}"

  defp excerpt(_source, nil, _column), do: ""

  defp excerpt(source, line, column) do
    lines = String.split(source, "\n")

    case Enum.at(lines, line - 1) do
      nil ->
        ""

      text ->
        caret =
          if is_integer(column) and column > 0, do: String.duplicate(" ", column - 1), else: ""

        "\n\n    " <> text <> "\n    " <> caret <> "^"
    end
  end

  # ---------- call checking ----------

  defp map_keys(%{} = map), do: map |> Map.keys() |> MapSet.new()
  defp map_keys(_other), do: MapSet.new()

  # Every place a name may be called: the todo_list, and each subtask
  # of each alternative of each method.
  defp called_names(document) do
    from_todo = document |> Map.get("todo_list", []) |> List.wrap() |> Enum.flat_map(&call_name/1)

    from_methods =
      document
      |> Map.get("methods", %{})
      |> values()
      |> Enum.flat_map(fn method ->
        method
        |> get("alternatives", [])
        |> List.wrap()
        |> Enum.flat_map(fn alternative ->
          alternative
          |> get("subtasks", [])
          |> List.wrap()
          |> Enum.flat_map(&call_name/1)
        end)
      end)

    from_todo ++ from_methods
  end

  # A call is an array whose head is the name. A goal and a multigoal
  # are objects, and they name no action.
  defp call_name([name | _args]) when is_binary(name), do: [name]
  defp call_name(_other), do: []

  defp values(%{} = map), do: Map.values(map)
  defp values(_other), do: []

  defp get(%{} = map, key, default), do: Map.get(map, key, default)
  defp get(_other, _key, default), do: default

  defp plural([_one]), do: "a task calls"
  defp plural(_many), do: "tasks call"

  defp list(names), do: Enum.map_join(names, ", ", &"`#{&1}`")
end
