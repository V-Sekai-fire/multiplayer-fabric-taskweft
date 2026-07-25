---
authors: K. S. Ernest (iFire) Lee <fire@users.noreply.github.com>
state: committed
discussion:
labels: mcp, api
---

# RFD 0008: MCP as the public API

## Context and Problem Statement

The taskweft planner has two access paths: a standalone CLI binary and a
hosted MCP server. Earlier versions also exposed a C++ CLI (`cli/main.cpp`)
and a library API through `Taskweft.plan/1`. Each path needed separate
maintenance and had different authentication/versioning patterns.

## Decision Outcome

MCP tools are the public API surface. The `plan` and `validate` tools define
the contract. The CLI binary (`taskweft` via Burrito) delegates to the same
MCP server implementation internally. The hosted endpoint at
`taskweft-mcp.fly.dev` exposes the same tools with OAuth gate.

The legacy C++ CLI is removed. The Burrito binary uses HTTP-only transport
(no stdio MCP). All external consumers point at `taskweft://` resources or
call the `plan`/`validate` tools.

## Consequences

Good: one API surface to document, version, and secure. The same tools work
locally (`mix taskweft.mcp`) and remotely.
