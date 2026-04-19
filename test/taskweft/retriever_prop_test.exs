defmodule Taskweft.RetrieverPropTest do
  use ExUnit.Case, async: true
  use PropCheck

  alias Taskweft.NIF
  alias Taskweft.Retriever

  @dim 64

  def content_gen do
    let words <- non_empty(list(oneof([
          exactly("alice"), exactly("bob"), exactly("task"),
          exactly("resource"), exactly("plan"), exactly("domain")
        ]))),
        do: Enum.join(words, " ")
  end

  def trust_gen, do: let(n <- range(0, 100), do: n / 100.0)

  def fact_json(content, trust) do
    ~s({"fact_id":1,"content":"#{content}","trust_score":#{trust},"tags":"test","fts_rank":0.5})
  end

  def candidates_json(content, trust) do
    "[#{fact_json(content, trust)}]"
  end

  property "score: returns valid JSON array" do
    forall {content, trust} <- {content_gen(), trust_gen()} do
      query_vec = NIF.hrr_encode_text("alice task", @dim)
      query_bytes = NIF.hrr_phases_to_bytes(query_vec)
      cands = candidates_json(content, trust)
      result = Retriever.score(cands, "alice task", query_bytes, dim: @dim)
      is_binary(result) and String.starts_with?(result, "[")
    end
  end

  property "score: result contains a score field" do
    forall content <- content_gen() do
      query_vec = NIF.hrr_encode_text(content, @dim)
      query_bytes = NIF.hrr_phases_to_bytes(query_vec)
      cands = candidates_json(content, 0.9)
      result = Retriever.score(cands, content, query_bytes, dim: @dim)
      result =~ "\"score\""
    end
  end

  property "score: zero trust yields zero score" do
    forall content <- content_gen() do
      query_vec   = NIF.hrr_encode_text(content, @dim)
      query_bytes = NIF.hrr_phases_to_bytes(query_vec)
      cands  = candidates_json(content, 0.0)
      result = Retriever.score(cands, content, query_bytes, dim: @dim)
      # score = relevance * trust * decay, trust=0 → score=0
      [scored | _] = Jason.decode!(result)
      scored["score"] == 0.0
    end
  end

  property "score: hrr_vector field is stripped from output" do
    forall content <- content_gen() do
      query_vec = NIF.hrr_encode_text(content, @dim)
      query_bytes = NIF.hrr_phases_to_bytes(query_vec)
      hrr_bytes = NIF.hrr_phases_to_bytes(query_vec)
      fact = ~s({"fact_id":1,"content":"#{content}","trust_score":0.8,"tags":"t","fts_rank":0.5,"hrr_vector":"#{Base.encode64(hrr_bytes)}"})
      cands = "[#{fact}]"
      result = Retriever.score(cands, content, query_bytes, dim: @dim)
      not (result =~ "hrr_vector")
    end
  end

  property "probe: returns valid JSON array" do
    forall content <- content_gen() do
      entity_vec   = NIF.hrr_encode_atom("alice", @dim)
      entity_bytes = NIF.hrr_phases_to_bytes(entity_vec)
      fact = ~s({"fact_id":1,"content":"#{content}","trust_score":0.8,"binding_vector":"x"})
      cands = "[#{fact}]"
      result = Retriever.probe(cands, entity_bytes, @dim)
      is_binary(result) and String.starts_with?(result, "[")
    end
  end

  property "probe: binding_vector field is stripped from output" do
    forall content <- content_gen() do
      entity_vec   = NIF.hrr_encode_atom("alice", @dim)
      entity_bytes = NIF.hrr_phases_to_bytes(entity_vec)
      fact = ~s({"fact_id":1,"content":"#{content}","trust_score":0.8,"binding_vector":"x"})
      cands = "[#{fact}]"
      result = Retriever.probe(cands, entity_bytes, @dim)
      not (result =~ "binding_vector")
    end
  end

  property "reason: returns valid JSON for single entity" do
    forall content <- content_gen() do
      entity_vec   = NIF.hrr_encode_atom("alice", @dim)
      entity_bytes = NIF.hrr_phases_to_bytes(entity_vec)
      fact = ~s({"fact_id":1,"content":"#{content}","trust_score":0.8})
      cands = "[#{fact}]"
      result = Retriever.reason(cands, [entity_bytes], @dim)
      is_binary(result) and String.starts_with?(result, "[")
    end
  end

  property "reason: empty entity list returns empty array" do
    forall content <- content_gen() do
      fact  = ~s({"fact_id":1,"content":"#{content}","trust_score":0.8})
      cands = "[#{fact}]"
      Retriever.reason(cands, [], @dim) == "[]"
    end
  end
end
