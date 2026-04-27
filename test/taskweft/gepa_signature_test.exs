# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.SignatureTest do
  use ExUnit.Case, async: true

  @tag :red
  test "validate/2 returns :ok when all required keys are present" do
    schema = [:score, :failed_action]
    input = %{score: -1.0, failed_action: "a_fight"}
    assert :ok = Taskweft.GEPA.Signature.validate(schema, input)
  end

  @tag :red
  test "validate/2 returns {:error, missing} when keys are absent" do
    schema = [:score, :failed_action]
    input = %{score: -1.0}
    assert {:error, [:failed_action]} = Taskweft.GEPA.Signature.validate(schema, input)
  end
end
