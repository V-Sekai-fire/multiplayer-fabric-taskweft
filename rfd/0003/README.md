---
authors: K. S. Ernest (iFire) Lee <fire@users.noreply.github.com>
state: committed
discussion: https://github.com/taskweft/taskweft/pull/83
labels: planner, khr-interactivity
---

# RFD 0003: KHR_interactivity Tier 1 node catalog — decompose-first and b-selector convention

## Context and Problem Statement

`taskweft_nif` implements 68 of ~130 KHR_interactivity node types as `eval`-node
value computations. The remaining ~26 Tier 1 nodes add pure value computation
without changing the execution model. Two design questions recur:

1. Should every node get a bespoke C++ implementation, or should compound nodes
   reuse existing simpler primitives?
2. `kNodeTypes()`'s calling convention passes one `TwValue` in and one out.
   Several nodes need more than 4 named inputs, a configuration field, or
   multiple outputs.

## Decision Outcome

**1. Decompose before implementing a new primitive.** If a node is defined by
the spec in terms of already-implemented operations, inline that formula rather
than introducing new algebra. Before writing C++ for numerically subtle nodes
(`smoothStep`, `rotate2D`), write a Lean reference model and witness-certify
invariants, then implement and cross-check.

**2. Structural nodes for >4 inputs or config fields.** Nodes needing more than
the `(a,b,c,d)` table slots become `if` blocks in `eval_node()` with direct
dict access — the same treatment `select`/`switch`/`clamp`/`mix` already get.

**3. `b`-selector for multi-output nodes.** A node with N logical outputs takes
an extra `b` index argument selecting which output to return, matching how
`extract2/3/4` already use `b` as an array index.

## Consequences

Good: most Tier 1 nodes are pure drop-in table entries. The decompose-first +
Lean-witness pattern applies proportional rigor.
Neutral: the `b`-selector convention costs one extra evaluation per output
(matches existing `extract*` precedent).
