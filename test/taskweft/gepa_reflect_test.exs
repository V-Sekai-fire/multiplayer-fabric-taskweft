# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.ReflectTest do
  use ExUnit.Case, async: true

  @tag :red
  test "reflect/2 returns a non-empty string critique for a failed episode" do
    asi = %{"failed_action" => "a_fight", "reason" => "hp_too_low"}
    assert {:ok, critique} = Taskweft.GEPA.Reflect.reflect(-1.0, asi)
    assert is_binary(critique) and byte_size(critique) > 0
  end
end
