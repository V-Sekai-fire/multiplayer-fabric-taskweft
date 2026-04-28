# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.Reflect do
  @moduledoc """
  Reflective Cycle — self-critique step of the GEPA training loop.

  Takes a numeric episode score and an ASI map and returns a language critique
  string.  Uses Instructor with the configured adapter for structured output.
  """

  @spec reflect(float(), map()) :: {:ok, String.t()} | {:error, term()}
  def reflect(score, asi) when is_number(score) and is_map(asi) do
    tools = Taskweft.GEPA.ToolContext.build()

    prompt = """
    Episode score: #{score} (range -1.0 worst to +1.0 best)
    Failure context: #{Jason.encode!(asi)}
    Available tools:
    #{tools}

    In one sentence, critique what went wrong and suggest one concrete improvement.
    """

    case Instructor.chat_completion(
           response_model: Taskweft.GEPA.Critique,
           messages: [%{role: "user", content: prompt}]
         ) do
      {:ok, %{critique: critique}} -> {:ok, critique}
      {:error, reason} -> {:error, reason}
    end
  end
end
