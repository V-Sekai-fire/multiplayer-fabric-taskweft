# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.DSL do
  @moduledoc """
  Elixir DSL for building RECTGTN HTN domains using real Elixir code.

  ## Usage

      defmodule BlocksWorld do
        use Taskweft.DSL

        @name "blocks_world"

        @variables %{
          pos: %{type: :ref, init: %{a: "table", b: "hand"}},
          clear: %{type: :bool, init: %{a: true, b: false}}
        }

        @actions %{
          pickup: %{
            params: [:block],
            body: [%{pointer_set: "/pos/{block}", value: "hand"}]
          }
        }

        @todo_list [
          [:move, :a, :table],
          [:move, :b, :table]
        ]
      end

  ## I/O

  - **Input**: Real Elixir module code with module attributes
  - **Output**: RECTGTN JSON-LD string

  ## Example

      iex> domain = \"\"\n      ...>       defmodule MyDomain do\n      ...>         use Taskweft.DSL\n      ...>         @name \"my_domain\"\n      ...>         @todo_list [[:test, :a]]\n      ...>       end\n      ...> \"\"\n      iex> Taskweft.DSL.compile(domain)\n      {:ok, \"{\\\"@context\\\":...}\"
  """

  @type compile_result :: {:ok, String.t()} | {:error, String.t()}

  @doc """
  Compile an Elixir module that uses `use Taskweft.DSL` and return a RECTGTN domain JSON-LD string.

  Returns {:ok, json_string} or {:error, reason} on failure.
  """
  @spec compile(String.t()) :: compile_result()
  def compile(dsl_source) when is_binary(dsl_source), do: compile(dsl_source, [])

  @doc """
  Compile with options.

  `:check_calls` decides whether a task calling an unknown name is an
  error. It defaults to true. `Taskweft.Compose` passes false, because
  a document meant for composition may call an action a sibling
  defines. That check then runs once on the composed document, where
  every name is present.
  """
  @spec compile(String.t(), keyword()) :: compile_result()
  def compile(dsl_source, opts) when is_binary(dsl_source) and is_list(opts) do
    alias Taskweft.DSL.Diagnostics

    check_calls? = Keyword.get(opts, :check_calls, true)

    with :ok <- Diagnostics.check_api(dsl_source),
         {:ok, ast} <- parse(dsl_source),
         {:ok, json} <- Taskweft.DSL.SafeParser.parse(ast),
         :ok <- maybe_check_calls(json, check_calls?) do
      {:ok, json}
    else
      {:error, %Jason.DecodeError{} = error} ->
        {:error, "DSL error: parser produced invalid JSON: #{Exception.message(error)}"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp maybe_check_calls(_json, false), do: :ok

  defp maybe_check_calls(json, true) do
    case Jason.decode(json) do
      {:ok, document} -> Taskweft.DSL.Diagnostics.check_calls(document)
      {:error, error} -> {:error, error}
    end
  end

  defp parse(dsl_source) do
    case Code.string_to_quoted(dsl_source) do
      {:ok, ast} ->
        {:ok, ast}

      {:error, detail} ->
        {:error, Taskweft.DSL.Diagnostics.syntax_error(detail, dsl_source)}
    end
  end
end
