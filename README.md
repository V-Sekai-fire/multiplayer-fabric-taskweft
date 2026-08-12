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

Maintained, and the planner to build against.

This section used to send new work to the s7-Lisp-in-libriscv stack in
[weft-warp-loop](https://github.com/weftspun/weft-warp-loop). That stack is
abandoned, so the replacement it pointed at is not coming and the advice had
outlived the plan: a reader following it would have started on something with
no maintainer instead of on this.
