---
authors: K. S. Ernest (iFire) Lee <fire@users.noreply.github.com>
state: committed
discussion: https://github.com/taskweft/taskweft/pull/96
labels: planner, rebac, capabilities
---

# RFD 0005: Unify domain capabilities with the ReBAC relation-expression engine

## Context and Problem Statement

RECTGTN's `C` (Capability) and `R` (Relationship) are documented as one
coherent concept, but the implementation was two disconnected mechanisms:

- Domain `capabilities` was flattened at load time to boolean state variables
  (`_cap_<capability>[entity] = true`) — no relationship graph, no transitive
  membership, no expression composition.
- `Taskweft.ReBAC` is a real relationship graph engine with transitive
  `IS_MEMBER_OF` expansion and composable relation expressions. It was
  invoked only directly — domains could not reference it at all.

A domain author wanting "agent qualifies if it holds capability X, OR is a
member of a team that holds X" could not express that.

## Decision Outcome

Unify by making the flat shape valid sugar for the simple case while routing
all guard evaluation through the same ReBAC relation-expression engine.

- `capabilities` gains an optional `"graph"` key (edges + definitions).
- `"actions"` entries accept either a bare capability name (sugar) or a full
  `{"rel": <expression>, "object": <string>}` requirement.
- `tw_loader.hpp` stopped pre-flattening; capabilities now compile to
  `HAS_CAPABILITY` edges on a `TwReBACGraph`.
- `tw_planner.hpp` needed zero changes — guard evaluation changed entirely
  inside the `TwActionFn`-wrapping closure.

## Consequences

Good: one authorization model. Existing domains keep working unchanged (flat
shape is sugar, not removed). Transitive team-membership cases now plan
correctly.
