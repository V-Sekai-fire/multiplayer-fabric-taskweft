# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.FullCycleTest do
  use ExUnit.Case, async: false
  import Mox

  setup :verify_on_exit!

  @tag :red
  test "full GEPA cycle: ASI → reflect → evolve → bootstrap stores successful trace" do
    stub(Taskweft.GEPA.InstructorMock, :chat_completion, fn params, _config ->
      response_model = Keyword.get(params, :response_model)

      map =
        cond do
          response_model == Taskweft.GEPA.Critique ->
            %{"critique" => "The bot failed to rest before fighting."}

          response_model == Taskweft.GEPA.EvolvedInstructions ->
            %{"instructions" => ["gather copper near bank", "rest when hp below 50%"]}

          true ->
            %{}
        end

      {:ok, nil, map}
    end)

    stub(Taskweft.GEPA.InstructorMock, :reask_messages, fn _raw, _params, _config -> [] end)

    replan = %{
      "recovered" => true,
      "fail_step" => 1,
      "failed_action" => "a_gather",
      "new_plan" => [["a_rest"]]
    }

    instructions = ["gather copper", "rest when tired"]

    {:ok, asi} = Taskweft.GEPA.ASI.serialize(replan)
    score = if asi["recovered"], do: 1.0, else: -1.0
    {:ok, _critique} = Taskweft.GEPA.Reflect.reflect(score, asi)
    {:ok, evolved} = Taskweft.GEPA.Optimizer.evolve(instructions, score)

    :ok = Taskweft.GEPA.Bootstrap.store(%{"instructions" => evolved, "score" => score})
    traces = Taskweft.GEPA.Bootstrap.get()
    assert Enum.any?(traces, &(&1["score"] == 1.0))
  end
end
