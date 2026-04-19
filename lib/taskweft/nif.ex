defmodule Taskweft.NIF do
  @on_load :__on_load__

  def __on_load__ do
    path = :code.priv_dir(:taskweft) |> to_string() |> Kernel.<>("/libtaskweft_nif")
    :erlang.load_nif(path, 0)
  end

  def plan(_domain_json), do: :erlang.nif_error(:not_loaded)
  def replan(_domain_json, _plan_json, _fail_step), do: :erlang.nif_error(:not_loaded)
  def check_temporal(_domain_json, _plan_json, _origin_iso), do: :erlang.nif_error(:not_loaded)
  def hrr_encode_atom(_word, _dim), do: :erlang.nif_error(:not_loaded)
  def hrr_similarity(_a, _b), do: :erlang.nif_error(:not_loaded)
  def hrr_encode_text(_text, _dim), do: :erlang.nif_error(:not_loaded)
  def hrr_encode_binding(_content, _entity, _dim), do: :erlang.nif_error(:not_loaded)
  def hrr_encode_fact(_content, _entities, _dim), do: :erlang.nif_error(:not_loaded)
  def hrr_phases_to_bytes(_phases), do: :erlang.nif_error(:not_loaded)
  def hrr_bytes_to_phases(_data, _len), do: :erlang.nif_error(:not_loaded)

  def hrr_bind(_a_bytes, _b_bytes), do: :erlang.nif_error(:not_loaded)
  def hrr_unbind(_bound_bytes, _key_bytes), do: :erlang.nif_error(:not_loaded)
  def hrr_bundle(_vecs), do: :erlang.nif_error(:not_loaded)

  def rebac_add_edge(_graph_json, _subj, _obj, _rel), do: :erlang.nif_error(:not_loaded)
  def rebac_check(_graph_json, _subj, _expr_json, _obj, _fuel), do: :erlang.nif_error(:not_loaded)
  def rebac_expand(_graph_json, _rel, _obj, _fuel), do: :erlang.nif_error(:not_loaded)
  def rebac_parse_relation_edges(_facts_json, _trust_threshold), do: :erlang.nif_error(:not_loaded)

  def retriever_score(_candidates_json, _query_text, _query_hrr_bytes,
        _fts_w, _jaccard_w, _hrr_w, _half_life_days, _dim),
      do: :erlang.nif_error(:not_loaded)
  def retriever_probe(_candidates_json, _entity_hrr_bytes, _dim), do: :erlang.nif_error(:not_loaded)
  def retriever_reason(_candidates_json, _entity_hrr_bytes_list, _dim), do: :erlang.nif_error(:not_loaded)

  def bridge_binding_content(_var, _arg, _val), do: :erlang.nif_error(:not_loaded)
  def bridge_extract_entities(_state_json), do: :erlang.nif_error(:not_loaded)
  def bridge_plan_contents(_plan_json, _domain, _entities_json), do: :erlang.nif_error(:not_loaded)
  def bridge_state_bindings(_state_json, _domain, _category), do: :erlang.nif_error(:not_loaded)
end
