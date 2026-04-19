defmodule Taskweft.MCExecutor do
  @moduledoc """
  Monte Carlo plan executor.

  Port of `plan/ipyhop/mc_executor.py` `MonteCarloExecutor`.
  Simulates stochastic plan execution: each step succeeds with probability
  `p` or fails, halting execution at that step.
  """

  alias Taskweft.NIF

  @doc """
  Execute `plan_json` against `domain_json` stochastically.

  - `probs_json` — JSON array of per-step success probabilities, e.g. `[0.9,0.8]`.
    Missing entries default to `1.0` (always succeed).
  - `seed` — integer random seed for reproducibility (default 10, matching Python).

  Returns `{:ok, trace_json}` or `{:error, reason}`.
  """
  def execute(domain_json, plan_json, probs_json \\ "[]", seed \\ 10) do
    result = NIF.mc_execute(domain_json, plan_json, probs_json, seed)
    case Jason.decode(result) do
      {:ok, %{"error" => reason}} -> {:error, reason}
      {:ok, trace} -> {:ok, Jason.encode!(trace)}
      {:error, _} -> {:error, "invalid_json"}
    end
  end
end
