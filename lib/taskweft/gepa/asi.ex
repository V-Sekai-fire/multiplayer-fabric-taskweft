# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.ASI do
  @moduledoc """
  ASI Serialization — extracts Actionable Side Information from a failed episode.

  Converts a replan result map into a structured ASI map that the Reflective
  Cycle and GEPA optimizer can act on.
  """

  @spec serialize(map()) :: {:ok, map()} | {:error, term()}
  def serialize(replan_result) when is_map(replan_result) do
    fail_step = Map.get(replan_result, "fail_step", -1)
    recovered = Map.get(replan_result, "recovered", false)

    asi = %{
      "failed_action" => Map.get(replan_result, "failed_action", "unknown"),
      "reason" =>
        if(recovered, do: "recovered_at_step_#{fail_step}", else: "failed_at_step_#{fail_step}"),
      "fail_step" => fail_step,
      "recovered" => recovered
    }

    {:ok, asi}
  end
end
