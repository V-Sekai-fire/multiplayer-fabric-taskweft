# AGENTS.md — multiplayer-fabric-taskweft

Guidance for AI coding agents working in this submodule.

## What this is

Elixir library wrapping a C++20 HTN planner NIF. Provides planning, replanning,
temporal consistency checking, HRR (Holographic Reduced Representation) encoding
for semantic memory, and ReBAC (relationship-based access control) checks.
Used by `multiplayer-fabric-artifacts-mmog` and `multiplayer-fabric-zone-console`.

A breaking API change here requires coordinated updates in both consumers
before either is released.

## Build and test

```sh
mix compile           # compiles C++ NIF via elixir_make
mix test --include property   # ExUnit + PropCheck property tests
```

## Key files

| Path | Purpose |
|------|---------|
| `mix.exs` | Deps: elixir_make, exqlite, jason, propcheck |
| `lib/taskweft.ex` | Public API: `plan/1`, `replan/3`, `check_temporal/3`, HRR ops, ReBAC |
| `c_src/taskweft_nif.cpp` | NIF entry points |
| `c_src/tw_planner.hpp` | C++20 HTN planner core |
| `c_src/tw_hrr.hpp` | HRR phase-vector encoding/binding |
| `c_src/tw_rebac.hpp` | ReBAC graph traversal |
| `c_src/tw_temporal.hpp` | ISO 8601 temporal constraint checking |
| `standalone/` | Header-only C++ standalone (used by multiplayer-fabric-sandbox) |

## Domain JSON format

Domains are JSON-LD documents. See `standalone/tw_domain.hpp` and
`priv/plans/` in `multiplayer-fabric-artifacts-mmog` for examples.
The `"op"` field syntax (`"add"`, `"get"`) must match what `domain.ex`
emits — do not revert to the old `"type": "math/add"` form.

## Conventions

- All Elixir public functions return `{:ok, value}` or `{:error, reason}`.
- Property tests live alongside unit tests; run both with `--include property`.
- Every new `.ex` / `.exs` file needs SPDX headers:
  ```elixir
  # SPDX-License-Identifier: MIT
  # Copyright (c) 2026 K. S. Ernest (iFire) Lee
  ```
- Commit message style: sentence case, no `type(scope):` prefix.
  Example: `Add hrr_bundle NIF binding for phase-vector aggregation`
