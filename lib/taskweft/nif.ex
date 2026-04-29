defmodule Taskweft.NIF do
  @on_load :__on_load__

  def __on_load__ do
    path = :code.priv_dir(:taskweft) |> to_string() |> Kernel.<>("/libtaskweft_nif")
    :erlang.load_nif(path, 0)
  end

  def plan(_domain_json), do: :erlang.nif_error(:not_loaded)
  def replan(_domain_json, _plan_json, _fail_step), do: :erlang.nif_error(:not_loaded)
  def check_temporal(_domain_json, _plan_json, _origin_iso), do: :erlang.nif_error(:not_loaded)

  def rebac_add_edge(_graph_json, _subj, _obj, _rel), do: :erlang.nif_error(:not_loaded)
  def rebac_check(_graph_json, _subj, _expr_json, _obj, _fuel), do: :erlang.nif_error(:not_loaded)
  def rebac_expand(_graph_json, _rel, _obj, _fuel), do: :erlang.nif_error(:not_loaded)

  def rebac_parse_relation_edges(_facts_json, _trust_threshold),
    do: :erlang.nif_error(:not_loaded)

  def rebac_can(_graph_json, _subj, _capability, _max_depth), do: :erlang.nif_error(:not_loaded)

  def rebac_get_entity_capabilities(_graph_json, _entity), do: :erlang.nif_error(:not_loaded)

  def rebac_get_entities_with_capability(_graph_json, _capability),
    do: :erlang.nif_error(:not_loaded)

  def bridge_binding_content(_var, _arg, _val), do: :erlang.nif_error(:not_loaded)
  def bridge_extract_entities(_state_json), do: :erlang.nif_error(:not_loaded)

  def bridge_plan_contents(_plan_json, _domain, _entities_json),
    do: :erlang.nif_error(:not_loaded)

  def bridge_state_bindings(_state_json, _domain, _category), do: :erlang.nif_error(:not_loaded)

  def mc_execute(_domain_json, _plan_json, _probs_json, _seed), do: :erlang.nif_error(:not_loaded)
end
