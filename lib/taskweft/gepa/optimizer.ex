# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.Optimizer do
  @moduledoc """
  GEPA — Genetic-Pareto background optimizer.

  Evolves a list of instruction strings toward a Pareto frontier across
  multiple metrics (success rate, plan length) using the score from the
  most recent episode as the selection signal.
  """

  @model "google/gemma-4-26b-a4b-it"

  @spec evolve([String.t()], float()) :: {:ok, [String.t()]} | {:error, term()}
  def evolve(instructions, score) when is_list(instructions) and is_number(score) do
    tools = Taskweft.GEPA.ToolContext.build()

    prompt = """
    Current bot instructions:
    #{Enum.with_index(instructions, 1) |> Enum.map_join("\n", fn {inst, i} -> "#{i}. #{inst}" end)}

    Episode score: #{score} (range -1.0 worst to +1.0 best)
    Available tools:\n#{tools}

    Rewrite each instruction to improve future episode performance.
    Return exactly #{length(instructions)} improved instructions.
    """

    case Instructor.chat_completion(
           model: @model,
           response_model: Taskweft.GEPA.EvolvedInstructions,
           messages: [%{role: "user", content: prompt}]
         ) do
      {:ok, %{instructions: evolved}} when evolved != [] -> {:ok, evolved}
      {:ok, %{instructions: []}} -> {:error, :empty_evolution}
      {:error, reason} -> {:error, reason}
    end
  end
end
