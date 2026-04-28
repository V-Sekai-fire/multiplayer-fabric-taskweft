# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.Signature do
  @moduledoc """
  Signatures — data contract validation for GEPA task inputs and outputs.
  """

  @spec validate([atom()], map()) :: :ok | {:error, [atom()]}
  def validate(schema, input) when is_list(schema) and is_map(input) do
    missing = Enum.reject(schema, &Map.has_key?(input, &1))
    if missing == [], do: :ok, else: {:error, missing}
  end
end
