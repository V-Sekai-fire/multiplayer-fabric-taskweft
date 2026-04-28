# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.GEPA.Critique do
  use Ecto.Schema
  use Instructor

  @llm_doc "A one-sentence critique of what went wrong in an episode and one concrete improvement."
  @primary_key false
  embedded_schema do
    field :critique, :string
  end
end

defmodule Taskweft.GEPA.EvolvedInstructions do
  use Ecto.Schema
  use Instructor

  @llm_doc "A revised list of bot instructions improved based on episode feedback."
  @primary_key false
  embedded_schema do
    field :instructions, {:array, :string}
  end
end
