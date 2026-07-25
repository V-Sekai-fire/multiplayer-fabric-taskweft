---
authors: K. S. Ernest (iFire) Lee <fire@users.noreply.github.com>
state: committed
discussion: https://github.com/taskweft/taskweft/pull/68
labels: planner, khr-interactivity
---

# RFD 0002: Align the planner stack on the glTF Interactivity node shape

## Context and Problem Statement

Taskweft domains describe action bodies as a list of steps. Two shapes existed:

- **Legacy shorthand** — a state read is `{"check": "/ptr", "eq": v}` and a write
  is `{"set": "/ptr", "value": v}`.
- **glTF Interactivity node shape** — a read is an `{"eval": {...}}` step whose
  node is a `math/<op>` comparison over a `pointer/get`, and a write is
  `{"pointer/set": "/ptr", "value": v}`.

The pieces of the stack migrated to the node shape at different times leaving
two live contradictions: the pinned NIF could not execute domains the
validator accepted, and the app could not consume post-migration fixtures.

## Decision Outcome

Chosen: bump the whole stack to the node shape.

- `taskweft_nif` drops `check`/`set` in favor of `eval` + `pointer/set`.
- All in-repo test domains migrated off the removed shorthand.
- No `lib/` change — `Loader.validate` was already consistent with the target.

## Consequences

Good: NIF, bundled plans, validator, and tests now agree on one shape.
Bad: the node shape is more verbose than the shorthand. CI can no longer cache
`_build` (NIF must compile fresh each run to match the locked sha).

## More Information

Comparison ops accepted by the NIF: `math/eq`, `math/neq`, `math/lt`,
`math/le`, `math/gt`, `math/ge`. Canonical forms in `taskweft_plans` domains
and `tw_loader.hpp`.
