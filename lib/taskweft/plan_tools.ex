defmodule Taskweft.PlanTools do
  @moduledoc """
  High-level JSON-LD plan/simulate/replan entry point.

  Port of `plan/tools/plan_jsonld.py` `handle_taskweft` and
  `plan/tools/_common.py` state serialization helpers.
  """

  @valid_modes ~w(plan simulate replan)

  @doc """
  Dispatch a plan/simulate/replan request.

  `params` is a map with string keys:
  - `"mode"` → `"plan" | "simulate" | "replan"` (required)
  - `"domain"` → JSON-LD domain string (required)
  - `"plan"` → plan JSON string (required for simulate/replan)
  - `"origin_iso"` → ISO 8601 duration (simulate, optional, default `"PT0S"`)
  - `"fail_step"` → integer (replan, optional, default `-1`)
  """
  def handle(%{"mode" => mode}) when mode not in @valid_modes do
    {:error, "mode must be one of: #{Enum.join(@valid_modes, ", ")}"}
  end

  def handle(%{"mode" => _mode} = params) when not is_map_key(params, "domain") do
    {:error, "missing required key: domain"}
  end

  def handle(%{"mode" => "plan", "domain" => domain}) do
    Taskweft.plan(domain)
  end

  def handle(%{"mode" => "simulate", "domain" => domain} = params) do
    plan_json  = Map.get(params, "plan", "[]")
    origin_iso = Map.get(params, "origin_iso", "PT0S")
    Taskweft.check_temporal(domain, plan_json, origin_iso)
  end

  def handle(%{"mode" => "replan", "domain" => domain} = params) do
    plan_json = Map.get(params, "plan", "[]")
    fail_step = Map.get(params, "fail_step", -1)
    Taskweft.replan(domain, plan_json, fail_step)
  end

  def handle(_params) do
    {:error, "missing required key: mode"}
  end

  @doc """
  Serialize a flat state map (string keys → any) to a JSON string.

  Skips private (`_`-prefixed) keys. Mirrors `_common.py` `_serialize_state`.
  """
  def serialize_state(state_map) when is_map(state_map) do
    filtered =
      state_map
      |> Enum.reject(fn {k, _} -> String.starts_with?(to_string(k), "_") end)
      |> Map.new()
    {:ok, Jason.encode!(filtered)}
  end

  @doc """
  Pass through a nested map — Elixir has no `ast.literal_eval`, so tuple-key
  strings remain as strings. Mirrors `_common.py` `_coerce_keys`.
  """
  def coerce_keys(map) when is_map(map) do
    {:ok, Map.new(map, fn {k, v} ->
      coerced_v =
        case v do
          m when is_map(m) ->
            {:ok, inner} = coerce_keys(m)
            inner
          other ->
            other
        end
      {k, coerced_v}
    end)}
  end
end
