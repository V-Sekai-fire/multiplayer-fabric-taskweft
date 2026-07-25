---
authors: K. S. Ernest (iFire) Lee <fire@users.noreply.github.com>
state: committed
discussion: https://github.com/taskweft/taskweft/pull/105
labels: domain-design, heuristics, bitter-lesson
---

# RFD 0006: Favor generic compute-scalable search over hand-authored domain heuristics

## Context and Problem Statement

RECTGTN domains can be authored two ways: many hand-crafted methods encoding
human-decided strategies per case, or a small number of generic methods that
recurse over data and leave choice among alternatives to the planner's search.

The first style caps solution quality at author cleverness. It does not
improve as the domain grows or as more compute is thrown at planning. Every
new item or edge case needs its own method. This does not scale.

## Decision Drivers

Sutton's Bitter Lesson: general methods that leverage computation (search)
consistently beat methods that encode human domain knowledge as problem scale
grows. The planner's own fast-path linear-advancement loop, witness-oracle
pruning cache, and depth budget all make backtracking search cheaper — capacity
that sits unused if domains route around search with hand-authored strategy.

## Decision Outcome

Domain actions still encode real game facts (preconditions, effects,
durations) — those are ground truth, not heuristics. But method-level
strategy ("gather materials first" vs. "already have them") is expressed as
generic alternative sets over data, not as bespoke per-case methods with
pre-decided branch order.

When search over generic structure proves too slow, the fix is to spend more
search compute — not to add hand-authored heuristics.

## Consequences

Good: new content extends data, not method count. Aligns domain-design effort
with search performance investment.
Bad: generic methods are harder to reason about per-case; debugging bad plans
may require inspecting search behavior rather than reading one method.
