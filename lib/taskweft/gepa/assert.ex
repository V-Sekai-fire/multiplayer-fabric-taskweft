# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.Assert do
  @moduledoc """
  Runtime Guardrails — declarative constraints that trigger backtracking.
  """

  @spec assert_(term(), (-> boolean())) :: :ok | {:backtrack, term()}
  def assert_(label, predicate) when is_function(predicate, 0) do
    if predicate.(), do: :ok, else: {:backtrack, label}
  end
end
