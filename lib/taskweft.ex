defmodule Taskweft do
  @moduledoc """
  Elixir bindings for the Taskweft HTN planner (C++20, Fine NIF).

  All JSON strings use the taskweft JSON-LD domain format.
  See the standalone/ C++ headers for the schema reference.
  """

  alias Taskweft.NIF

  @doc """
  Run the HTN planner on a self-contained JSON-LD domain document.

  Returns `{:ok, plan_json}` where `plan_json` is a JSON array of
  `[action_name, arg1, arg2, ...]` arrays, or `{:error, reason}`.
  """
  def plan(domain_json) do
    {:ok, NIF.plan(domain_json)}
  rescue
    e -> {:error, Exception.message(e)}
  end

  @doc """
  Replan after an action failure.

  `fail_step` is the 0-based index of the failed action in `plan_json`,
  or `-1` to auto-detect the first failing step by simulation.

  Returns `{:ok, replan_result_json}` or `{:error, reason}`.
  The result JSON includes `original_plan`, `fail_step`, `completed_steps`,
  `recovered`, and `new_plan` fields.
  """
  def replan(domain_json, plan_json, fail_step \\ -1) do
    {:ok, NIF.replan(domain_json, plan_json, fail_step)}
  rescue
    e -> {:error, Exception.message(e)}
  end

  @doc """
  Temporal analysis of a plan using ISO 8601 duration metadata.

  `origin_iso` is the plan start offset as an ISO 8601 duration (default `"PT0S"`).

  Returns `{:ok, temporal_json}` with `consistent`, `total`, `origin`, and
  per-step `start`/`end`/`duration` fields, or `{:error, reason}`.
  """
  def check_temporal(domain_json, plan_json, origin_iso \\ "PT0S") do
    {:ok, NIF.check_temporal(domain_json, plan_json, origin_iso)}
  rescue
    e -> {:error, Exception.message(e)}
  end

  @doc """
  Encode a word as a Holographic Reduced Representation phase vector.

  Returns a list of `dim` floats (phase angles in radians, 0..2π).
  The encoding is deterministic and identical to the Python implementation.
  Default `dim` is `4096`; use the same value for all vectors in a session.
  """
  def hrr_encode_atom(word, dim \\ 4096) do
    NIF.hrr_encode_atom(word, dim)
  end

  @doc """
  Cosine similarity between two HRR phase vectors.

  Both must have the same dimension. Returns a float in `[-1.0, 1.0]`.
  """
  def hrr_similarity(a, b) do
    NIF.hrr_similarity(a, b)
  end

  @doc """
  Encode text as a bag-of-words HRR phase vector (bundle of token atoms).

  Returns a list of `dim` floats (phase angles in radians).
  """
  def hrr_encode_text(text, dim \\ 4096) do
    NIF.hrr_encode_text(text, dim)
  end

  @doc """
  Encode a content-entity binding as raw bytes (little-endian float64).

  Computes `encode_text(content) ⊗ encode_atom(entity.lower())`.
  Returns a binary suitable for storage in SQLite.
  """
  def hrr_encode_binding(content, entity, dim \\ 4096) do
    NIF.hrr_encode_binding(content, entity, dim)
  end

  @doc """
  Encode a fact with role-vector bundling as raw bytes (little-endian float64).

  Bundles `encode_text(content) ⊗ role_content` with per-entity
  `encode_atom(entity) ⊗ role_entity` components.
  Returns a binary suitable for storage in SQLite.
  """
  def hrr_encode_fact(content, entities, dim \\ 4096) do
    NIF.hrr_encode_fact(content, entities, dim)
  end

  @doc """
  Serialize a phase vector (list of floats) to raw bytes (little-endian float64).
  """
  def hrr_phases_to_bytes(phases) do
    NIF.hrr_phases_to_bytes(phases)
  end

  @doc """
  Deserialize raw bytes back to a phase vector (list of floats).

  `len` is the number of phases (pass `0` to infer from binary size).
  """
  def hrr_bytes_to_phases(data, len \\ 0) do
    NIF.hrr_bytes_to_phases(data, len)
  end
end
