defmodule Taskweft.Retriever do
  @moduledoc """
  Hybrid keyword/HRR retrieval scoring.

  Wraps the C++ `tw_retriever.hpp` via Fine NIF. Three scoring modes:

  - `score/7` — weighted blend of FTS rank, Jaccard, and HRR cosine similarity.
  - `probe/3` — algebraic unbind-based probe (exact HRR retrieval).
  - `reason/3` — multi-entity AND-semantics reasoning (min-sim across entities).

  All return a decoded list of scored fact maps sorted descending by score.
  The `hrr_vector` / `binding_vector` fields are stripped from output.
  """

  alias Taskweft.NIF

  @default_fts_w 0.3
  @default_jaccard_w 0.3
  @default_hrr_w 0.4
  @default_half_life 30.0
  @default_dim 4096

  @doc """
  Score a candidate list with a hybrid FTS + Jaccard + HRR blend.

  ## Parameters

  - `candidates_json` — JSON array of fact objects with fields:
    `fact_id`, `content`, `trust_score`, `tags`, `fts_rank`,
    `hrr_vector` (binary bytes), `age_days` (optional).
  - `query_text` — raw query string for Jaccard tokenisation.
  - `query_hrr_bytes` — phase vector for the query as a binary.
  - `opts` — keyword list of overrides for weights/half_life/dim.

  Returns a JSON string (sorted desc) with a `score` field added per fact.
  """
  def score(candidates_json, query_text, query_hrr_bytes, opts \\ []) do
    fts_w      = Keyword.get(opts, :fts_w,       @default_fts_w)
    jaccard_w  = Keyword.get(opts, :jaccard_w,   @default_jaccard_w)
    hrr_w      = Keyword.get(opts, :hrr_w,       @default_hrr_w)
    half_life  = Keyword.get(opts, :half_life,   @default_half_life)
    dim        = Keyword.get(opts, :dim,         @default_dim)
    NIF.retriever_score(candidates_json, query_text, query_hrr_bytes,
      fts_w, jaccard_w, hrr_w, half_life, dim)
  end

  @doc """
  Algebraic probe: score facts by unbind similarity to an entity vector.

  Candidates must have a `binding_vector` field (binary bytes).
  Returns a JSON string sorted descending by score.
  """
  def probe(candidates_json, entity_hrr_bytes, dim \\ @default_dim) do
    NIF.retriever_probe(candidates_json, entity_hrr_bytes, dim)
  end

  @doc """
  Multi-entity reasoning: score by min-sim across all given entity vectors.

  `entity_hrr_bytes_list` is a list of binary phase vectors.
  Returns a JSON string sorted descending by score (AND semantics).
  """
  def reason(candidates_json, entity_hrr_bytes_list, dim \\ @default_dim) do
    NIF.retriever_reason(candidates_json, entity_hrr_bytes_list, dim)
  end
end
