<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 K. S. Ernest (iFire) Lee -->

# multiplayer-fabric-taskweft

HTN planner server exposing `plan` and `validate` tools over MCP.

The planner model is **RECTGTN** (Relationship-Enabled Capability-Temporal
Goal-Task-Network). See [docs/rectgtn.md](docs/rectgtn.md).

Download the binary from the
[latest release](https://github.com/taskweft/taskweft/releases).

```sh
taskweft plan <domain.jsonld>        # plan from a file, --problem <d> <p>, or stdin
taskweft plan <domain_dsl.ex>        # plan from an Elixir DSL file
taskweft mcp [--port N] [--host H]   # run the MCP server over HTTP
taskweft version                     # print version
```

## MCP client

Point your MCP config at the binary:

```json
{ "mcpServers": { "taskweft": { "url": "https://taskweft-mcp.fly.dev/mcp" } } }
```

Input via Elixir DSL (preferred) or JSON-LD. See `priv/plans/domains/blocks_world_dsl.ex`
for a working example.

## Project status

This planner is being replaced by the s7-Lisp-in-libriscv stack from
[weft-warp-loop](https://github.com/weftspun/weft-warp-loop). Bug fixes and
small improvements still accepted; new features should target the replacement.
