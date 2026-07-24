# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.MCP.Server do
  @moduledoc """
  MCP server for Taskweft.

  RECTGTN HTN planner exposed as MCP tools (`plan`, `validate`).

  ## Two call paths

  - **Runtime**: `mix taskweft.mcp` (or `taskweft mcp`) starts the HTTP server.
  - **Training time**: Python optimization loops call these tools via MCP.
  """

  use ExMCP.Server.Handler
  use ExMCP.Server.DSL, name: "taskweft"

  # ---------- TOOLS ----------

  tool "plan",
       "Run the IPyHOP-style HTN planner over an Elixir DSL or JSON-LD domain. Returns the plan as JSON." do
    param(:domain_dsl, :string,
      required: true,
      description: """
      A RECTGTN HTN domain in Elixir DSL or JSON-LD format.

      **Elixir DSL** (preferred - valid Elixir code with module attributes):

          defmodule MyDomain do
            use Taskweft.DSL
            @name "my_domain"
            @variables %{pos: %{type: :ref, init: %{a: "table"}}}
            @todo_list [[:pickup, :a]]
          end

      Note: Elixir requires a space after a keyword colon before an
      opening bracket — `todo: [type: list]`, not `todo:[type: list]`.

      **JSON-LD** (full Linked Data):

          {"@context": {...}, "@type": "domain:Definition", "name": "...", ...}

      Pass "format" => "dsl" for DSL input, "json" for JSON-LD. Defaults to "dsl".
      """
    )

    param(:format, :string,
      default: "dsl",
      description: "Parse format - 'dsl' for Elixir DSL or 'json' for JSON-LD"
    )

    param(:explain, :boolean,
      default: false,
      description: "If true, include explain tree in the plan response"
    )

    param(:plan_json, :string,
      description:
        "JSON string containing the original plan steps array.  When set, replans from a failure step instead of doing a fresh plan."
    )

    param(:fail_step, :integer,
      description: "Step index to replan from (0-based, requires plan_json; ignored otherwise)."
    )

    handle(fn args, _state ->
      with {:ok, raw} <- fetch_param(args, :domain_dsl),
           {:ok, domain_dsl} <-
             parse_domain_input(raw, Map.get(args, :format, "dsl")),
           {:ok, validated} <- validate_domain(domain_dsl),
           explain = Map.get(args, :explain, false) do
        case Map.get(args, :plan_json) do
          nil ->
            {:ok, result} = do_plan(validated, explain)
            {:ok, tool_text(result)}

          plan_str ->
            fail_step = Map.get(args, :fail_step, -1)
            {:ok, result} = Taskweft.replan(validated, plan_str, fail_step)
            {:ok, tool_text(result)}
        end
      else
        {:error, reason} -> {:ok, tool_error(reason)}
      end
    end)
  end

  tool "validate",
       "Validate a RECTGTN HTN domain without generating a plan." do
    param(:domain_dsl, :string,
      required: true,
      description: "A RECTGTN HTN domain in Elixir DSL or JSON-LD format."
    )

    param(:format, :string,
      default: "dsl",
      description: "Parse format - 'dsl' for Elixir DSL or 'json' for JSON-LD"
    )

    handle(fn args, _state ->
      with {:ok, raw} <- fetch_param(args, :domain_dsl),
           {:ok, domain_dsl} <-
             parse_domain_input(raw, Map.get(args, :format, "dsl")),
           {:ok, validated} <- validate_domain(domain_dsl) do
        {:ok, tool_text(validated)}
      else
        {:error, reason} -> {:ok, tool_error(reason)}
      end
    end)
  end

  # ---------- HELPERS ----------

  # Parse raw input string into a domain JSON string.
  # Accepts 'dsl' or 'json'.
  defp parse_domain_input(raw, "json") when is_binary(raw) do
    # Validate JSON but pass through the JSON string
    case Jason.decode(raw) do
      {:ok, _map} -> {:ok, raw}
      {:error, reason} -> {:error, "invalid JSON-LD: #{reason}"}
    end
  end

  defp parse_domain_input(raw, "dsl") when is_binary(raw) do
    Taskweft.DSL.compile(raw)
  end

  defp parse_domain_input(_raw, format) do
    {:error, ~s(unknown format: #{inspect(format)}. Use "dsl" or "json".)}
  end

  # Validate a domain JSON string using the JSON-LD loader.
  # Returns {:ok, normalized_json_string} or {:error, reason}.
  defp validate_domain(domain_json) do
    case Taskweft.JSONLD.Loader.load_string(domain_json) do
      {:ok, normalized} -> {:ok, normalized}
      {:error, reason} -> {:error, "Validation error: #{reason}"}
    end
  end

  # Plan a domain. If explain is true, include the explain tree.
  defp do_plan(domain_json, true), do: Taskweft.plan_explain(domain_json)
  defp do_plan(domain_json, false), do: Taskweft.plan(domain_json)

  # Safe param fetch — returns {:error, ...} instead of raising KeyError.
  defp fetch_param(args, key) do
    case Map.fetch(args, key) do
      {:ok, value} when not is_nil(value) -> {:ok, value}
      {:ok, nil} -> {:error, "missing required parameter: #{key}"}
      :error -> {:error, "missing required parameter: #{key}"}
    end
  end

  # MCP content item wrapping — protocol requires content to be an array
  # of objects with type/text fields, not a bare string.
  defp tool_text(text), do: %{content: [%{type: "text", text: text}]}

  defp tool_error(reason) do
    tool_text(encode_error(reason))
  end

  # Encode an error reason as safe JSON — never raise, never crash.
  # Non-encodable values (tuples, pids, refs) become inspect strings.
  defp encode_error(reason) do
    case Jason.encode(%{error: to_string(reason)}) do
      {:ok, json} -> json
      {:error, _} -> ~s({"error":"internal error"})
    end
  end
end
