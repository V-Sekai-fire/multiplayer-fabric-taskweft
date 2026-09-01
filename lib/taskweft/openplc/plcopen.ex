# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.OpenPLC.PLCopen do
  @moduledoc """
  Emit PLCopen TC6 XML for the **FBD** POU of a compact GRAFCET
  document (RFD 2143 profile). SFC, ST, and LD are all blocklisted
  as RECTGTN targets per RFD 2145; the state machine is encoded as
  an FBD network of `SR_L` flip-flops (one per step), `AND` gates
  (one per transition), and `MOVE` blocks (one per action).

  Stage-1 coverage:

    * Each step X in the compact GRAFCET's `S` array becomes one
      `SR_L` block named `step_X`. `step_X.Q` is "step X active".
    * Each transition from X to Y guarded by receptivity G becomes
      `AND(step_X.Q, G) -> step_Y.S`, and `step_Y.Q -> step_X.R`.
    * Each step's action (`V.foo:=1`) becomes a `MOVE` block enabled
      by `step_X.Q`, moving TRUE into `done_foo`.
    * AND-divergence (`&>`): one AND-gated transition drives every
      sibling step's S input in parallel.
    * AND-convergence (`&<`): the merged step's S input is gated by
      an `AND` over each predecessor's Q.
    * Time-delayed transitions (`t/X_i/1h`) use a `TON` block with
      `IN=step_X.Q` and `PT=T#1h`, feeding the transition AND gate.
    * The `^` initial step has its own SR_L pre-set on power-up via
      an initialised `first_scan_done : BOOL := FALSE` that fires
      once and sets the initial step's S input on scan 1.

  `|>`, `|<`, `!>`, `%`, `#` raise (staged in RFD 2143 / RFD 2147).
  """

  # -- public --------------------------------------------------------------

  @doc "Emit PLCopen XML for one compact GRAFCET map. Returns a string."
  def emit(%{"S" => steps, "V" => vars} = grafcet) do
    pou_name = Map.get(grafcet, "sfc", "rectgtn_plan")
    {names, transitions} = walk(steps)
    initial = "__init"

    local_vars = var_decls(vars, names)
    net = fbd_body(initial, names, transitions)

    ~s"""
    <pou name="#{escape(pou_name)}" pouType="program">
      <interface>
        <localVars>
    #{local_vars}
        </localVars>
      </interface>
      <body>
        <FBD>
    #{net}
        </FBD>
      </body>
    </pou>
    """
  end

  # -- variables -----------------------------------------------------------
  # Two families:
  #   done_<v>    : one BOOL per V entry (persistent "task complete" flag)
  #   step_<n>_Q  : one BOOL per step, mirroring the SR flip-flop's Q
  # Plus the first_scan latch that gates the initial step's S input on scan 1.
  defp var_decls(vars, names) do
    dones =
      vars
      |> Map.keys()
      |> Enum.map_join("\n", fn v ->
        ~s(          <variable name="done_#{escape(v)}"><type><BOOL/></type><initialValue><simpleValue value="FALSE"/></initialValue></variable>)
      end)

    steps =
      Enum.map_join(names, "\n", fn n ->
        ~s(          <variable name="step_#{escape(n)}_Q"><type><BOOL/></type></variable>)
      end)

    first_scan =
      ~s(          <variable name="first_scan_done"><type><BOOL/></type><initialValue><simpleValue value="FALSE"/></initialValue></variable>)

    dones <> "\n" <> steps <> "\n" <> first_scan
  end

  # -- FBD body assembly ---------------------------------------------------
  # Numbering:
  #   1..N              : SR_L blocks, one per step, in `names` order
  #   next block for each transition (AND + TON if delay) + a MOVE for the
  #   step's action, all in a single deterministic sequence.
  defp fbd_body(initial, names, transitions) do
    n_steps = length(names)
    step_id = fn name -> Enum.find_index(names, &(&1 == name)) + 1 end

    sr_blocks =
      names
      |> Enum.with_index(1)
      |> Enum.map_join("\n", fn {name, id} -> sr_block(id, name) end)

    action_blocks =
      names
      |> Enum.reject(&(&1 == initial))
      |> Enum.with_index(n_steps + 1)
      |> Enum.map_join("\n", fn {name, id} ->
        move_action_block(id, name, step_id.(name))
      end)

    tr_blocks =
      transitions
      |> Enum.with_index(n_steps + n_steps + 1)
      |> Enum.map_join("\n", fn {t, base_id} ->
        transition_blocks(base_id, t, step_id)
      end)

    # scan-1 fire for the initial step: first_scan_done is FALSE at boot,
    # feeds an EDGE-like set into the initial SR's S once, then latches
    # itself TRUE so the next scans do not re-trigger.
    first_scan_ff = first_scan_gate(step_id.(initial), n_steps * 4 + 100)

    sr_blocks <> "\n" <> action_blocks <> "\n" <> tr_blocks <> "\n" <> first_scan_ff
  end

  defp sr_block(id, name) do
    ~s"""
          <block localId="#{id}" typeName="SR_L" instanceName="step_#{escape(name)}">
            <inputVariables>
              <variable formalParameter="S"><connectionPointIn/></variable>
              <variable formalParameter="R"><connectionPointIn/></variable>
            </inputVariables>
            <outputVariables>
              <variable formalParameter="Q">
                <connectionPointOut/>
                <expression>step_#{escape(name)}_Q</expression>
              </variable>
            </outputVariables>
          </block>\
    """
  end

  # MOVE block: on step_<name>_Q, write TRUE into done_<name>. In real IEC
  # 61131-3 FBD the enable is `EN`; a MOVE with EN wired to a step Q is the
  # standard "action while step active" pattern.
  defp move_action_block(id, name, step_local_id) do
    ~s"""
          <block localId="#{id}" typeName="MOVE" instanceName="do_#{escape(name)}">
            <inputVariables>
              <variable formalParameter="EN"><connectionPointIn><connection refLocalId="#{step_local_id}" formalParameter="Q"/></connectionPointIn></variable>
              <variable formalParameter="IN"><expression>TRUE</expression></variable>
            </inputVariables>
            <outputVariables>
              <variable formalParameter="OUT">
                <connectionPointOut/>
                <expression>done_#{escape(name)}</expression>
              </variable>
            </outputVariables>
          </block>\
    """
  end

  # A transition from src(s) with guard atoms → dst. Emitted as:
  #   AND(src.Q..., guard atoms..., TON.Q if delayed) → dst.SR.S
  #   dst.Q → src.SR.R (each src)
  defp transition_blocks(base_id, %{sources: srcs, target: dst, atoms: atoms, delay: delay}, step_id) do
    and_id = base_id
    ton_id = if delay, do: base_id + 1, else: nil

    src_inputs =
      srcs
      |> Enum.with_index(1)
      |> Enum.map_join("\n", fn {s, i} ->
        ~s(                  <variable formalParameter="IN#{i}"><connectionPointIn><connection refLocalId="#{step_id.(s)}" formalParameter="Q"/></connectionPointIn></variable>)
      end)

    atom_inputs =
      atoms
      |> Enum.with_index(length(srcs) + 1)
      |> Enum.map_join("\n", fn {a, i} ->
        ~s(                  <variable formalParameter="IN#{i}"><expression>#{escape(a)}</expression></variable>)
      end)

    ton_input =
      if ton_id do
        idx = length(srcs) + length(atoms) + 1
        ~s(                  <variable formalParameter="IN#{idx}"><connectionPointIn><connection refLocalId="#{ton_id}" formalParameter="Q"/></connectionPointIn></variable>)
      else
        ""
      end

    and_block = ~s"""
          <block localId="#{and_id}" typeName="AND">
            <inputVariables>
    #{src_inputs}
    #{atom_inputs}
    #{ton_input}
            </inputVariables>
            <outputVariables>
              <variable formalParameter="OUT">
                <connectionPointOut/>
                <expression>__tr_#{and_id}</expression>
              </variable>
            </outputVariables>
          </block>\
    """

    ton_block =
      if ton_id do
        ~s"""
              <block localId="#{ton_id}" typeName="TON" instanceName="tr_#{and_id}_ton">
                <inputVariables>
                  <variable formalParameter="IN"><connectionPointIn><connection refLocalId="#{step_id.(hd(srcs))}" formalParameter="Q"/></connectionPointIn></variable>
                  <variable formalParameter="PT"><expression>T##{escape(delay)}</expression></variable>
                </inputVariables>
                <outputVariables>
                  <variable formalParameter="Q"><connectionPointOut/></variable>
                </outputVariables>
              </block>\
        """
      else
        ""
      end

    # dst.SR.S <- and_id.OUT
    set_wire =
      ~s"""
            <connection sourceLocalId="#{and_id}" sourceFormalParameter="OUT" targetLocalId="#{step_id.(dst)}" targetFormalParameter="S"/>\
      """

    # reset each source when dst.Q rises
    reset_wires =
      srcs
      |> Enum.map_join("\n", fn s ->
        ~s(          <connection sourceLocalId="#{step_id.(dst)}" sourceFormalParameter="Q" targetLocalId="#{step_id.(s)}" targetFormalParameter="R"/>)
      end)

    [ton_block, and_block, set_wire, reset_wires]
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("\n")
  end

  # Set the initial step's S input once, at scan 1. `first_scan_done` starts
  # FALSE; the AND(NOT(first_scan_done)) fires exactly once, then a MOVE
  # latches first_scan_done to TRUE so subsequent scans do not re-fire.
  defp first_scan_gate(initial_step_id, base_id) do
    not_id = base_id
    latch_id = base_id + 1

    ~s"""
          <block localId="#{not_id}" typeName="NOT">
            <inputVariables>
              <variable formalParameter="IN"><expression>first_scan_done</expression></variable>
            </inputVariables>
            <outputVariables>
              <variable formalParameter="OUT">
                <connectionPointOut/>
                <expression>__first_scan</expression>
              </variable>
            </outputVariables>
          </block>
          <block localId="#{latch_id}" typeName="MOVE" instanceName="latch_first_scan">
            <inputVariables>
              <variable formalParameter="EN"><connectionPointIn><connection refLocalId="#{not_id}" formalParameter="OUT"/></connectionPointIn></variable>
              <variable formalParameter="IN"><expression>TRUE</expression></variable>
            </inputVariables>
            <outputVariables>
              <variable formalParameter="OUT">
                <connectionPointOut/>
                <expression>first_scan_done</expression>
              </variable>
            </outputVariables>
          </block>
          <connection sourceLocalId="#{not_id}" sourceFormalParameter="OUT" targetLocalId="#{initial_step_id}" targetFormalParameter="S"/>\
    """
  end

  # -- walker: compact GRAFCET S array -> {names, transitions} -----------

  defp walk(steps) do
    {names, _} =
      Enum.reduce(steps, {[], nil}, fn row, {acc, _last} ->
        case row do
          ["^"] -> {acc ++ ["__init"], "__init"}
          [head | _] when head in ["&>", "&<"] -> {acc, nil}
          [head | _] when head in ["|>", "|<", "!>"] ->
            raise "PLCopen emit for #{inspect(head)} is staged; see RFD 2147"

          [head | _] when is_binary(head) and byte_size(head) > 0 and
                          (binary_part(head, 0, 1) == "%" or
                           binary_part(head, 0, 1) == "#") ->
            raise "PLCopen emit for #{inspect(head)} is staged; see RFD 2143"

          [name, _when, _do, _t] -> {acc ++ [name], name}
          _ -> {acc, nil}
        end
      end)

    {_, transitions, _, _} =
      Enum.reduce(steps, {nil, [], nil, nil}, fn row, {last, tacc, fp, fpen} ->
        case row do
          ["^"] -> {"__init", tacc, fp, fpen}

          ["&>" | children] -> {last, tacc, last, children}

          ["&<" | preds] -> {last, tacc, fp, {:conv, preds}}

          [name, when_expr, _do, t_iso] ->
            {srcs, fp2, fpen2} =
              case {fpen, fp} do
                {{:conv, preds}, _} -> {preds, fp, nil}

                {fanout_children, parent} when is_list(fanout_children) ->
                  if name in fanout_children do
                    rest = List.delete(fanout_children, name)
                    {[parent], parent, if(rest == [], do: nil, else: rest)}
                  else
                    {[last], fp, fpen}
                  end

                _ -> {[last], fp, fpen}
              end

            atoms = parse_atoms(when_expr)
            delay = normalise_delay(t_iso)
            tr = %{sources: srcs, target: name, atoms: atoms, delay: delay}
            {name, tacc ++ [tr], fp2, fpen2}

          _ -> {last, tacc, fp, fpen}
        end
      end)

    {names, transitions}
  end

  defp parse_atoms(""), do: []
  defp parse_atoms(nil), do: []
  defp parse_atoms(expr) when is_binary(expr) do
    expr
    |> String.split("&")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.map(fn "X." <> step -> "step_" <> step <> "_Q"; a -> a end)
  end

  defp normalise_delay(nil), do: nil
  defp normalise_delay(""), do: nil
  defp normalise_delay("0h"), do: nil
  defp normalise_delay(d), do: d

  defp escape(s) do
    s
    |> to_string()
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
  end
end
