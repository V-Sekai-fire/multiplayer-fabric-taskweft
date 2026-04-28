# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.LLM do
  @moduledoc """
  Configurable LLM adapter for GEPA.

  Implement this behaviour and set `:gepa_llm_impl` to use any backend:

      # config/runtime.exs (production — TurboquantLlm wrapper)
      config :taskweft, gepa_llm_impl: MyApp.TurboquantGEPAAdapter

      # config/test.exs
      config :taskweft, gepa_llm_impl: Taskweft.GEPA.LLMMock

  The adapter receives a messages list and must return the LLM reply as a
  raw string (JSON expected by callers).
  """

  @callback chat([map()]) :: {:ok, String.t()} | {:error, term()}

  def chat(messages) do
    impl().chat(messages)
  end

  def impl do
    Application.get_env(:taskweft, :gepa_llm_impl, __MODULE__.Stub)
  end
end

defmodule Taskweft.GEPA.LLM.Stub do
  @behaviour Taskweft.GEPA.LLM
  def chat(_messages), do: {:error, :gepa_llm_not_configured}
end
