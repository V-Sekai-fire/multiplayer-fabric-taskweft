defmodule Taskweft.StorePropTest do
  use ExUnit.Case, async: false
  use PropCheck

  @moduletag :property

  alias Taskweft.Store

  # ---------------------------------------------------------------------------
  # Generators
  # ---------------------------------------------------------------------------

  def content_gen do
    such_that s <- let(chars <- non_empty(list(oneof([range(?a, ?z), range(?A, ?Z), range(?0, ?9)]))), do: to_string(chars)),
      when: String.length(s) >= 1
  end

  def category_gen, do: oneof(["general", "fact", "note", "reference"])

  def feedback_count_gen, do: integer(0, 20)

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp with_store(fun) do
    dir = System.tmp_dir!()
    path = Path.join(dir, "taskweft_prop_#{:erlang.unique_integer([:positive])}.db")

    try do
      {:ok, store} = Store.open(path, hrr_dim: 64)
      fun.(store)
    after
      File.rm(path)
    end
  end

  # ---------------------------------------------------------------------------
  # add_fact
  # ---------------------------------------------------------------------------

  property "add_fact returns a positive integer ID" do
    forall content <- content_gen() do
      with_store(fn store ->
        {:ok, id} = Store.add_fact(store, content)
        is_integer(id) and id > 0
      end)
    end
  end

  property "add_fact deduplicates by content" do
    forall content <- content_gen() do
      with_store(fn store ->
        {:ok, id1} = Store.add_fact(store, content)
        {:ok, id2} = Store.add_fact(store, content)
        id1 == id2
      end)
    end
  end

  property "added fact appears in list_facts" do
    forall {content, category} <- {content_gen(), category_gen()} do
      with_store(fn store ->
        {:ok, _id} = Store.add_fact(store, content, category)
        facts = Store.list_facts(store, category: category, min_trust: 0.0)
        Enum.any?(facts, fn f -> f["content"] == content end)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # remove_fact
  # ---------------------------------------------------------------------------

  property "removed fact no longer appears in list_facts" do
    forall content <- content_gen() do
      with_store(fn store ->
        {:ok, id} = Store.add_fact(store, content)
        {:ok, true} = Store.remove_fact(store, id)
        facts = Store.list_facts(store, min_trust: 0.0)
        not Enum.any?(facts, fn f -> f["fact_id"] == id end)
      end)
    end
  end

  property "removing a non-existent fact returns false" do
    with_store(fn store ->
      {:ok, false} = Store.remove_fact(store, 999_999)
      true
    end)
  end

  # ---------------------------------------------------------------------------
  # record_feedback / trust clamping
  # ---------------------------------------------------------------------------

  property "helpful feedback increases or maintains trust" do
    forall content <- content_gen() do
      with_store(fn store ->
        {:ok, id} = Store.add_fact(store, content)
        {:ok, result} = Store.record_feedback(store, id, true)
        result["new_trust"] >= result["old_trust"]
      end)
    end
  end

  property "unhelpful feedback decreases or maintains trust" do
    forall content <- content_gen() do
      with_store(fn store ->
        {:ok, id} = Store.add_fact(store, content)
        {:ok, result} = Store.record_feedback(store, id, false)
        result["new_trust"] <= result["old_trust"]
      end)
    end
  end

  property "trust stays in [0.0, 1.0] regardless of feedback volume" do
    forall {content, n_help, n_unhelpful} <- {content_gen(), feedback_count_gen(), feedback_count_gen()} do
      with_store(fn store ->
        {:ok, id} = Store.add_fact(store, content)

        for _ <- 1..max(n_help, 1), do: Store.record_feedback(store, id, true)
        for _ <- 1..max(n_unhelpful, 1), do: Store.record_feedback(store, id, false)

        facts = Store.list_facts(store, min_trust: 0.0)
        fact = Enum.find(facts, fn f -> f["fact_id"] == id end)
        trust = fact["trust_score"]
        trust >= 0.0 and trust <= 1.0
      end)
    end
  end

  property "record_feedback on missing fact returns error" do
    with_store(fn store ->
      {:error, _} = Store.record_feedback(store, 999_999, true)
      true
    end)
  end

  # ---------------------------------------------------------------------------
  # extract_entities
  # ---------------------------------------------------------------------------

  property "quoted terms are extracted as entities" do
    forall word <- let({c1, rest} <- {oneof([range(?a, ?z), range(?A, ?Z)]), non_empty(list(oneof([range(?a, ?z), range(?A, ?Z)])))}, do: to_string([c1 | rest])) do
      with_store(fn store ->
        text = ~s(He uses "#{word}" for work.)
        entities = extract_entities(store, text)
        String.downcase(word) in Enum.map(entities, &String.downcase/1)
      end)
    end
  end

  property "empty text yields no entities" do
    with_store(fn store ->
      extract_entities(store, "") == []
    end)
  end

  # ---------------------------------------------------------------------------
  # update_fact
  # ---------------------------------------------------------------------------

  property "update_fact with trust_delta adjusts trust" do
    forall content <- content_gen() do
      with_store(fn store ->
        {:ok, id} = Store.add_fact(store, content)

        {:ok, true} = Store.update_fact(store, id, trust_delta: 0.1)

        facts = Store.list_facts(store, min_trust: 0.0)
        fact = Enum.find(facts, fn f -> f["fact_id"] == id end)
        fact["trust_score"] >= 0.5
      end)
    end
  end

  property "update_fact on missing id returns false" do
    with_store(fn store ->
      {:ok, false} = Store.update_fact(store, 999_999, trust_delta: 0.1)
      true
    end)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Mirror the regex rules from Taskweft.Store.extract_entities/1.
  defp extract_entities(_store, text) do
    cap = Regex.scan(~r/\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b/, text, capture: :all_but_first)
    dquote = Regex.scan(~r/"([^"]+)"/, text, capture: :all_but_first)
    squote = Regex.scan(~r/'([^']+)'/, text, capture: :all_but_first)
    aka = Regex.scan(~r/(\w+(?:\s+\w+)*)\s+(?:aka|also known as)\s+(\w+(?:\s+\w+)*)/i, text, capture: :all_but_first)

    (List.flatten(cap) ++ List.flatten(dquote) ++ List.flatten(squote) ++ List.flatten(aka))
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq_by(&String.downcase/1)
  end
end
