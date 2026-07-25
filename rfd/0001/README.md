---
authors: K. S. Ernest (iFire) Lee <fire@users.noreply.github.com>
state: published
discussion: https://github.com/taskweft/taskweft/pull/1
labels: process
---

# RFD 0001: Requests for Discussion

## Problem Statement

Design decisions in the taskweft project are made across PRs, issues, chat
messages, and direct commits. No single record captures what was decided, why,
and whether the decision is still current. New contributors and future maintainers
can't reconstruct the rationale behind the current state.

## Proposal

Adopt the Oxide Computer Company RFD (Request for Discussion) process as a
lightweight decision-capture mechanism.

Each RFD is a Markdown document living under `rfd/XXXX/README.md` with a YAML
front-matter metadata block:

```yaml
---
authors: Name <email>
state: prediscussion | ideation | discussion | published | committed | abandoned
discussion: https://github.com/taskweft/taskweft/pull/<num>
labels: comma, separated, labels
---
```

**States:**

| State | Meaning |
|-------|---------|
| `prediscussion` | Placeholder, being drafted |
| `ideation` | Topic identified, not yet drafted |
| `discussion` | Active PR open for review |
| `published` | Merged, represents current consensus |
| `committed` | Fully implemented |
| `abandoned` | Deliberately not pursued |

**Lifecycle:**

1. Reserve the next number by checking `rfd/*/` directories
2. Create `rfd/XXXX/README.md` from the prototype at `rfd/prototypes/prototype.md`
3. Draft in a branch named `rfd-XXXX`
4. Open a PR, set state to `discussion`
5. Merge and set state to `published`
6. When implementation is complete, set state to `committed`

RFDs are mutable — update them as understanding evolves (keep the `committed`
state current, don't leave stale rationale sitting in published RFDs).

## Alternatives considered

- **No formal process** — the status quo; we've already lost rationale
- **ADRs** — used previously (see `docs/adr/`), but lacked state tracking and
  per-RFD branches for iterative discussion
- **GitHub Issues** — no document permanence or reviewable diff

## Implementation status

- [x] Process defined (this RFD)
- [x] Existing ADRs migrated to RFDs 0002-0006
