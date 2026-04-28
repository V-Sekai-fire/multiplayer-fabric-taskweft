# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.ReflectTest do
  use ExUnit.Case, async: false
  import Mox

  setup :verify_on_exit!

  @tag :red
  test "reflect/2 returns a non-empty string critique for a failed episode" do
    stub(Taskweft.GEPA.InstructorMock, :chat_completion, fn _params, _config ->
      {:ok, nil, %{"critique" => "The bot ran out of HP by fighting without resting first."}}
    end)

    stub(Taskweft.GEPA.InstructorMock, :reask_messages, fn _raw, _params, _config -> [] end)

    asi = %{"failed_action" => "a_fight", "reason" => "hp_too_low"}
    assert {:ok, critique} = Taskweft.GEPA.Reflect.reflect(-1.0, asi)
    assert is_binary(critique) and byte_size(critique) > 0
  end
end
