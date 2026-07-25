---
authors: K. S. Ernest (iFire) Lee <fire@users.noreply.github.com>
state: published
discussion:
labels: planner, khr-interactivity, libriscv
---

# RFD 0004: KHR_interactivity Tier 2 — embed libriscv, compile behavior graphs to riscv64

## Context and Problem Statement

Tier 1 covers pure value-computation nodes. The remaining ~22 KHR_interactivity
nodes — flow/*, event/*, animation/*, pointer/interpolate — need a flow-graph
execution model with asynchronous event triggers and time-based suspend/resume.
No such model exists in `taskweft_nif`.

## Considered Options

1. **Hand-roll a from-scratch flow-graph interpreter** — full control, but
   re-derives sandboxing from nothing.
2. **Reuse `godot-sandbox`** — rejected: bound to a live Godot process.
3. **Embed `libriscv`** — standalone C++ RISC-V emulator, no engine dependency.
4. **Emit C and shell out to an external riscv64 compiler** — rejected: adds
   runtime toolchain dependency on every deployment target.
5. **Dedicated RECTGTN→riscv64 compiler** — lowers behavior graphs directly
   to target-ISA machine code, no external toolchain.

## Decision Outcome

Chosen: **option 3 (embed `libriscv`) + option 5 (dedicated RECTGTN→riscv64
compiler)**.

- Vendor `libriscv` into `taskweft_nif` matching existing vendoring pattern.
- New `tw_graph_compile.hpp` lowers flow/event graphs to riscv64 machine code.
- Values cross the ABI as JSON, reusing existing `TwValue` (de)serialization.
- Suspend/resume via `libriscv`'s native machine snapshot.
- New entry point `tw_execute_graph` with new NIF functions; `tw_planner.hpp`
  needs zero changes.

## Consequences

Good: uses a real, vetted sandboxed VM. No runtime cross-compiler dependency.
`tw_planner.hpp`/`tw_domain.hpp` unchanged.
Bad: a genuinely new, sizeable subsystem — sequenced after Tier 1 completes.

Not yet implemented as of this RFD. Planned milestone order: (1) vendor
`libriscv`, prove trivial guest runs and serializes; (2) compiler for
non-state subset (`flow/sequence`/`branch`); (3) event model + suspend/resume;
(4) remaining flow nodes, animation, interpolation; (5) sync-vs-async
integration with `tw_planner.hpp`.
