import ZoneProtocol

/-!
# Zone Observer Model

Formalises `headless_log_observer.gd`:

- Connects to the zone server on `ZONE_SERVER_PORT_LOCAL`
- Collects entities from CH_INTEREST datagrams each frame
- Exits 0 (pass) as soon as any entity is received
- Exits 1 (timeout) if `maxFrame` frames elapse with no entities
-/

set_option autoImplicit false

-- ---------------------------------------------------------------------------
-- State and outcome
-- ---------------------------------------------------------------------------

structure ObserverState where
  frame    : Nat   -- frames elapsed since start
  maxFrame : Nat   -- timeout threshold (default 600)
  entities : Nat   -- total entities received so far
  deriving Repr

inductive ObserverResult | pass | timeout
  deriving DecidableEq, Repr

/-- Advance one frame, adding any newly received entities. -/
def observerStep (s : ObserverState) (newEntities : Nat) : ObserverState :=
  { s with frame := s.frame + 1, entities := s.entities + newEntities }

/-- Emit pass as soon as any entity arrives; timeout at maxFrame. -/
def observerOutcome (s : ObserverState) : Option ObserverResult :=
  if s.entities > 0 then some .pass
  else if s.frame ≥ s.maxFrame then some .timeout
  else none

-- ---------------------------------------------------------------------------
-- Theorems
-- ---------------------------------------------------------------------------

/-- Entities received → observer exits 0 (pass). -/
theorem observer_pass_on_entities (s : ObserverState) (h : s.entities > 0) :
    observerOutcome s = some .pass := by
  simp [observerOutcome, h]

/-- Timeout → no entities were ever received. -/
theorem observer_timeout_means_no_entities (s : ObserverState)
    (h : observerOutcome s = some .timeout) : s.entities = 0 := by
  unfold observerOutcome at h
  split at h <;> simp_all <;> omega

/-- Pass and timeout are mutually exclusive. -/
theorem observer_pass_not_timeout (s : ObserverState) :
    observerOutcome s ≠ some .pass ∨ observerOutcome s ≠ some .timeout := by
  cases hv : observerOutcome s with
  | none   => left; simp
  | some r => cases r with
    | pass    => right; simp
    | timeout => left;  simp

/-- Each step strictly advances the frame counter. -/
theorem observerStep_advances_frame (s : ObserverState) (n : Nat) :
    (observerStep s n).frame = s.frame + 1 := by
  simp [observerStep]

/-- Steps never decrease the entity count. -/
theorem observerStep_entities_nondecreasing (s : ObserverState) (n : Nat) :
    s.entities ≤ (observerStep s n).entities := by
  simp [observerStep, Nat.le_add_right]
