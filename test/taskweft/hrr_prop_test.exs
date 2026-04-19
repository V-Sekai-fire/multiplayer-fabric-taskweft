defmodule Taskweft.HRRPropTest do
  use ExUnit.Case, async: false
  use PropCheck

  @moduletag :property

  # Dimensions kept small so tests run fast but are statistically meaningful.
  def dim_gen, do: oneof([64, 128, 256, 512])

  def word_gen do
    such_that w <- let(chars <- non_empty(list(oneof([range(?a, ?z), range(?A, ?Z), range(?0, ?9)]))), do: to_string(chars)),
      when: String.length(w) >= 1
  end

  def entities_gen do
    let words <- list(word_gen()) do
      Enum.take(words, 3)
    end
  end

  # ---------------------------------------------------------------------------
  # encode_atom
  # ---------------------------------------------------------------------------

  property "encode_atom is deterministic" do
    forall {word, dim} <- {word_gen(), dim_gen()} do
      v1 = Taskweft.hrr_encode_atom(word, dim)
      v2 = Taskweft.hrr_encode_atom(word, dim)
      v1 == v2
    end
  end

  property "encode_atom output length matches dim" do
    forall {word, dim} <- {word_gen(), dim_gen()} do
      v = Taskweft.hrr_encode_atom(word, dim)
      length(v) == dim
    end
  end

  property "encode_atom phases are in [0, 2*pi)" do
    forall {word, dim} <- {word_gen(), dim_gen()} do
      v = Taskweft.hrr_encode_atom(word, dim)
      two_pi = 2.0 * :math.pi()
      Enum.all?(v, fn p -> p >= 0.0 and p < two_pi end)
    end
  end

  # ---------------------------------------------------------------------------
  # hrr_similarity
  # ---------------------------------------------------------------------------

  property "similarity is in [-1, 1]" do
    forall {w1, w2, dim} <- {word_gen(), word_gen(), dim_gen()} do
      a = Taskweft.hrr_encode_atom(w1, dim)
      b = Taskweft.hrr_encode_atom(w2, dim)
      sim = Taskweft.hrr_similarity(a, b)
      sim >= -1.0 and sim <= 1.0
    end
  end

  property "similarity is symmetric" do
    forall {w1, w2, dim} <- {word_gen(), word_gen(), dim_gen()} do
      a = Taskweft.hrr_encode_atom(w1, dim)
      b = Taskweft.hrr_encode_atom(w2, dim)
      abs(Taskweft.hrr_similarity(a, b) - Taskweft.hrr_similarity(b, a)) < 1.0e-12
    end
  end

  property "self-similarity is approximately 1.0" do
    forall {word, dim} <- {word_gen(), dim_gen()} do
      a = Taskweft.hrr_encode_atom(word, dim)
      sim = Taskweft.hrr_similarity(a, a)
      abs(sim - 1.0) < 1.0e-9
    end
  end

  # ---------------------------------------------------------------------------
  # phases_to_bytes / bytes_to_phases roundtrip
  # ---------------------------------------------------------------------------

  property "phases_to_bytes -> bytes_to_phases is a perfect roundtrip" do
    forall {word, dim} <- {word_gen(), dim_gen()} do
      original = Taskweft.hrr_encode_atom(word, dim)
      bytes = Taskweft.hrr_phases_to_bytes(original)
      restored = Taskweft.hrr_bytes_to_phases(bytes, 0)
      original == restored
    end
  end

  property "serialized byte length is dim * 8 (float64)" do
    forall {word, dim} <- {word_gen(), dim_gen()} do
      v = Taskweft.hrr_encode_atom(word, dim)
      bytes = Taskweft.hrr_phases_to_bytes(v)
      byte_size(bytes) == dim * 8
    end
  end

  # ---------------------------------------------------------------------------
  # encode_text
  # ---------------------------------------------------------------------------

  property "encode_text is deterministic" do
    forall {word, dim} <- {word_gen(), dim_gen()} do
      v1 = Taskweft.hrr_encode_text(word, dim)
      v2 = Taskweft.hrr_encode_text(word, dim)
      v1 == v2
    end
  end

  property "encode_text output length matches dim" do
    forall {word, dim} <- {word_gen(), dim_gen()} do
      length(Taskweft.hrr_encode_text(word, dim)) == dim
    end
  end

  # ---------------------------------------------------------------------------
  # encode_binding
  # ---------------------------------------------------------------------------

  property "encode_binding is deterministic" do
    forall {content, entity, dim} <- {word_gen(), word_gen(), dim_gen()} do
      b1 = Taskweft.hrr_encode_binding(content, entity, dim)
      b2 = Taskweft.hrr_encode_binding(content, entity, dim)
      b1 == b2
    end
  end

  property "encode_binding byte length is dim * 8" do
    forall {content, entity, dim} <- {word_gen(), word_gen(), dim_gen()} do
      bytes = Taskweft.hrr_encode_binding(content, entity, dim)
      byte_size(bytes) == dim * 8
    end
  end

  # ---------------------------------------------------------------------------
  # encode_fact
  # ---------------------------------------------------------------------------

  property "encode_fact is deterministic" do
    forall {content, dim} <- {word_gen(), dim_gen()} do
      entities = ["Alice", "Bob"]
      b1 = Taskweft.hrr_encode_fact(content, entities, dim)
      b2 = Taskweft.hrr_encode_fact(content, entities, dim)
      b1 == b2
    end
  end

  property "encode_fact byte length is dim * 8" do
    forall {content, dim} <- {word_gen(), dim_gen()} do
      bytes = Taskweft.hrr_encode_fact(content, ["Alice"], dim)
      byte_size(bytes) == dim * 8
    end
  end

  property "encode_fact with no entities byte length is dim * 8" do
    forall {content, dim} <- {word_gen(), dim_gen()} do
      bytes = Taskweft.hrr_encode_fact(content, [], dim)
      byte_size(bytes) == dim * 8
    end
  end
end
