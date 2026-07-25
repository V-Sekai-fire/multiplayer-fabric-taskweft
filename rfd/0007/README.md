---
authors: K. S. Ernest (iFire) Lee <fire@users.noreply.github.com>
state: committed
discussion:
labels: dsl, domain-format
---

# RFD 0007: Elixir DSL as primary I/O format

## Context and Problem Statement

Taskweft domains can be expressed in two formats: JSON-LD (the native NIF wire
format) and Elixir DSL (real Elixir modules using `use Taskweft.DSL`). Earlier
versions used JSON-LD as the primary authoring format, with DSL as a secondary
option. This led to two parallel sets of domain files being maintained, and
the JSON-LD variants drifted from the canonical DSL versions.

## Decision Outcome

The Elixir DSL is the primary I/O format. All new domains are authored as
`.ex` files. JSON-LD is accepted only as a legacy input format for backward
compatibility — the `plan` tool's `format` parameter defaults to `"dsl"`.

The DSL uses real Elixir code with module attributes, not a custom parser
dialect. This gives free IDE autocomplete, type checking, and format-on-save
for domain files.

JSON-LD files with a working DSL equivalent are removed. The only exception is
`meta_loader.jsonld`, a meta-test domain that exercises the JSON-LD loader
itself and cannot be expressed in DSL form.

## Consequences

Good: one canonical format to maintain. DSL files are ~60% fewer tokens than
equivalent JSON-LD. IDE tooling works on domain files.
