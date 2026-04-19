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
end
