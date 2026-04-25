import ZoneProtocol

/-!
# Zone Player Model

Formalises two aspects of the Godot native PCVR player:

## CH_PLAYER write path (`send_player_input`)

Datagrams use the same 100-byte layout as CH_INTEREST.
The low byte of payload[0] (offset 44) carries the command code.
Three commands are distinguished: heartbeat (0), jellyfish spawn (1),
stroke knot (3). They must be distinct so the zone server can demultiplex.

## Operator camera swing invariant

`operator_camera.gd` decomposes camera orientation into twist [0,1] and
swing (fixed at SWING_ELEVATION = 0.153 turns = 153/1000).  No operator
input — rotate, zoom, follow, exit-follow — may change the swing.
-/

set_option autoImplicit false

-- ---------------------------------------------------------------------------
-- CH_PLAYER command codes
-- ---------------------------------------------------------------------------

inductive PlayerCmd
  | heartbeat   -- cmd=0: XR head-pose position update (_send_xr_heartbeat)
  | spawnJelly  -- cmd=1: spawn jellyfish entity
  | strokeKnot  -- cmd=3: pen stroke knot (spawn_stroke_knot)
  deriving DecidableEq, Repr

/-- The byte written to offset 44 (low byte of payload[0]). -/
def cmdByte : PlayerCmd → Nat
  | .heartbeat  => 0
  | .spawnJelly => 1
  | .strokeKnot => 3

theorem cmd_heartbeat_ne_spawnJelly  : cmdByte .heartbeat  ≠ cmdByte .spawnJelly  := by decide
theorem cmd_heartbeat_ne_strokeKnot  : cmdByte .heartbeat  ≠ cmdByte .strokeKnot  := by decide
theorem cmd_spawnJelly_ne_strokeKnot : cmdByte .spawnJelly ≠ cmdByte .strokeKnot  := by decide

/-- All cmd bytes fit in one byte. -/
theorem cmdByte_fits (c : PlayerCmd) : cmdByte c < 256 := by cases c <;> decide

/-- A player packet records its size and the cmd byte at its fixed offset. -/
structure PlayerPacket where
  size      : Nat
  cmdOffset : Nat
  cmd       : Nat

def mkPlayerPacket (c : PlayerCmd) : PlayerPacket :=
  { size := PACKET_SIZE, cmdOffset := PAYLOAD_OFFSET, cmd := cmdByte c }

theorem playerPacket_size_correct (c : PlayerCmd) :
    (mkPlayerPacket c).size = PACKET_SIZE := by simp [mkPlayerPacket]

theorem playerPacket_cmd_at_payload_offset (c : PlayerCmd) :
    (mkPlayerPacket c).cmdOffset = PAYLOAD_OFFSET := by simp [mkPlayerPacket]

-- ---------------------------------------------------------------------------
-- Operator camera swing invariant
-- ---------------------------------------------------------------------------

/-- SWING_ELEVATION numerator; denominator is 1000.
    153/1000 ≈ 0.153 turns ≈ 55° pitch. -/
def SWING_NUM : Nat := 153
def SWING_DEN : Nat := 1000

structure CameraState where
  twist : Nat   -- [0,3] quarters of one full turn
  zoom  : Nat   -- spring arm length (arbitrary units)
  swing : Nat   -- always SWING_NUM; denominator implicit SWING_DEN
  deriving Repr

def initialCamera : CameraState := { twist := 0, zoom := 400, swing := SWING_NUM }

def rotateLeft  (s : CameraState) : CameraState := { s with twist := (s.twist + 3) % 4 }
def rotateRight (s : CameraState) : CameraState := { s with twist := (s.twist + 1) % 4 }
def zoomIn      (s : CameraState) : CameraState := { s with zoom  := max s.zoom 50 - 50 }
def zoomOut     (s : CameraState) : CameraState := { s with zoom  := min s.zoom 600 + 50 }
def enterFollow (s : CameraState) : CameraState := s
def exitFollow  (s : CameraState) : CameraState := s

-- `cases s` exposes the three Nat fields so `rfl` reduces the struct update.
theorem rotateLeft_preserves_swing  (s : CameraState) : (rotateLeft  s).swing = s.swing := by cases s; rfl
theorem rotateRight_preserves_swing (s : CameraState) : (rotateRight s).swing = s.swing := by cases s; rfl
theorem zoomIn_preserves_swing      (s : CameraState) : (zoomIn      s).swing = s.swing := by cases s; rfl
theorem zoomOut_preserves_swing     (s : CameraState) : (zoomOut     s).swing = s.swing := by cases s; rfl
theorem enterFollow_preserves_swing (s : CameraState) : (enterFollow s).swing = s.swing := rfl
theorem exitFollow_preserves_swing  (s : CameraState) : (exitFollow  s).swing = s.swing := rfl

inductive Op | RotL | RotR | ZoomI | ZoomO | Follow | Unfollow

def applyOp (s : CameraState) : Op → CameraState
  | .RotL     => rotateLeft  s
  | .RotR     => rotateRight s
  | .ZoomI    => zoomIn      s
  | .ZoomO    => zoomOut     s
  | .Follow   => enterFollow s
  | .Unfollow => exitFollow  s

theorem applyOp_preserves_swing (s : CameraState) (op : Op) :
    (applyOp s op).swing = s.swing := by cases s; cases op <;> rfl

def applyOps (s : CameraState) : List Op → CameraState
  | []         => s
  | op :: rest => applyOps (applyOp s op) rest

theorem applyOps_preserves_swing (s : CameraState) (ops : List Op) :
    (applyOps s ops).swing = s.swing := by
  induction ops generalizing s with
  | nil        => rfl
  | cons op rest ih =>
    simp only [applyOps]
    rw [ih (applyOp s op), applyOp_preserves_swing]

/-- No operator input sequence can change the camera pitch. -/
theorem swing_always_swing_elevation (ops : List Op) :
    (applyOps initialCamera ops).swing = SWING_NUM := by
  rw [applyOps_preserves_swing]; rfl
