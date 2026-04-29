import Planner.Types
import Planner.Capabilities
import Planner.ReBACCorrectness

/-!
# ReBAC-backed Unigoal / Multigoal

Replaces the unigoal `(var, arg, val)` equality check with a ReBAC
capability check, enabling type inheritance via IS_MEMBER_OF chains.

## Mapping

  unigoal `('loc', 'c1', 'loc2')` → `hasCapability graph "c1" LOC "loc2" fuel`

A block `c1 IS_MEMBER_OF SomeGroup` where `SomeGroup --[LOC]--> loc2` satisfies
the goal without a direct edge — this is the inheritance benefit over plain
state-variable equality.

## Relation to TwGoalBinding (C++)

  TwGoalBinding { var = "LOC", key = "c1", desired = "loc2" }
  → uniSatisfied graph fuel { subj := "c1", rel := LOC, obj := "loc2" }

Goals and multigoals continue to work: a multigoal is a conjunction and
the planner iterates unsatisfied bindings with backtracking as before.
-/

namespace ReBACGoal

open RelationType ReBACCorrectness

-- ── Unigoal as a ReBAC triple ────────────────────────────────────────────────

/-- A unigoal: (subject, relation, object).
    Replaces the IPyHOP `(var_name, arg, desired_val)` tuple. -/
structure UniGoal where
  subj : Entity
  rel  : RelationType
  obj  : Entity
  deriving DecidableEq, Repr

/-- A multigoal is a conjunction of unigoals. -/
abbrev MultiGoal := List UniGoal

-- ── Satisfaction ─────────────────────────────────────────────────────────────

def uniSatisfied (graph : List Relationship) (fuel : Nat) (g : UniGoal) : Bool :=
  hasCapability graph g.subj g.rel g.obj fuel

def multiSatisfied (graph : List Relationship) (fuel : Nat) (gs : MultiGoal) : Bool :=
  gs.all (uniSatisfied graph fuel)

-- ── 1. Direct-edge soundness ─────────────────────────────────────────────────

/-- A direct edge `⟨g.subj, g.rel, g.obj⟩` satisfies the unigoal at fuel ≥ 1. -/
theorem uniSatisfied_direct (graph : List Relationship) (g : UniGoal) (n : Nat)
    (hmem : ⟨g.subj, g.rel, g.obj⟩ ∈ graph) :
    uniSatisfied graph (n + 1) g = true :=
  hasCapability_direct graph g.subj g.rel g.obj n hmem

-- ── 2. IS_MEMBER_OF inheritance ──────────────────────────────────────────────

/-- If `s IS_MEMBER_OF grp` and `grp` satisfies the goal, so does `s`.
    This is the key inheritance property: a block inherits the location
    (or any other relation) of any group it belongs to. -/
theorem uniSatisfied_inherited (graph : List Relationship)
    (s grp : Entity) (g : UniGoal) (n : Nat)
    (hmem  : ⟨s, IS_MEMBER_OF, grp⟩ ∈ graph)
    (hgrp  : uniSatisfied graph n { g with subj := grp } = true) :
    uniSatisfied graph (n + 1) { g with subj := s } = true :=
  hasCapability_member_trans graph s grp g.rel g.obj n hmem hgrp

-- ── 3. Multigoal conjunction ─────────────────────────────────────────────────

@[simp]
theorem multiSatisfied_nil (graph : List Relationship) (fuel : Nat) :
    multiSatisfied graph fuel [] = true := by simp [multiSatisfied]

theorem multiSatisfied_cons (graph : List Relationship) (fuel : Nat)
    (g : UniGoal) (gs : MultiGoal)
    (hg  : uniSatisfied graph fuel g = true)
    (hgs : multiSatisfied graph fuel gs = true) :
    multiSatisfied graph fuel (g :: gs) = true := by
  simp only [multiSatisfied, List.all_cons, Bool.and_eq_true]
  exact ⟨hg, hgs⟩

theorem multiSatisfied_iff (graph : List Relationship) (fuel : Nat) (gs : MultiGoal) :
    multiSatisfied graph fuel gs = true ↔
    ∀ g ∈ gs, uniSatisfied graph fuel g = true := by
  simp [multiSatisfied, List.all_eq_true]

/-- If one goal in a multigoal is unsatisfied, the whole multigoal is unsatisfied. -/
theorem multiSatisfied_false_of_member (graph : List Relationship) (fuel : Nat)
    (gs : MultiGoal) (g : UniGoal)
    (hmem : g ∈ gs) (hfalse : uniSatisfied graph fuel g = false) :
    multiSatisfied graph fuel gs = false := by
  have hne : ¬multiSatisfied graph fuel gs = true := by
    rw [multiSatisfied_iff]
    intro hall
    exact absurd (hall g hmem) (by simp [hfalse])
  cases hb : multiSatisfied graph fuel gs with
  | false => rfl
  | true  => exact absurd hb hne

-- ── 4. Fuel monotonicity ─────────────────────────────────────────────────────

/-- A satisfied unigoal remains satisfied with more fuel. -/
theorem uniSatisfied_fuel_mono (graph : List Relationship) (g : UniGoal)
    (n : Nat) (h : uniSatisfied graph n g = true) (k : Nat) :
    uniSatisfied graph (n + k) g = true :=
  hasCapability_fuel_mono graph g.subj g.rel g.obj n h k

/-- A satisfied multigoal remains satisfied with more fuel. -/
theorem multiSatisfied_fuel_mono (graph : List Relationship) (gs : MultiGoal)
    (n : Nat) (h : multiSatisfied graph n gs = true) (k : Nat) :
    multiSatisfied graph (n + k) gs = true := by
  rw [multiSatisfied_iff] at h ⊢
  exact fun g hg => uniSatisfied_fuel_mono graph g n (h g hg) k

-- ── 5. Subset monotonicity ───────────────────────────────────────────────────

/-- Removing goals from a satisfied multigoal leaves it satisfied. -/
theorem multiSatisfied_of_subset (graph : List Relationship) (fuel : Nat)
    (gs gs' : MultiGoal)
    (hsub : ∀ g ∈ gs', g ∈ gs)
    (h : multiSatisfied graph fuel gs = true) :
    multiSatisfied graph fuel gs' = true := by
  rw [multiSatisfied_iff] at h ⊢
  exact fun g hg => h g (hsub g hg)

end ReBACGoal
