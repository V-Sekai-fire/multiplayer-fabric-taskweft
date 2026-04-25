import Lake
open Lake DSL

package «taskweft» where
  leanOptions := #[
    ⟨`autoImplicit, false⟩
  ]

-- Holographic Reduced Representations: algebra and formal properties
@[default_target]
lean_lib «HRR» where
  srcDir := "."

-- HTN planner: types, capabilities/ReBAC, blocks world, temporal, unified GTN
@[default_target]
lean_lib «Planner» where
  srcDir := "."

-- WASM equivalence: libriscv + taskweft purity across execution environments
@[default_target]
lean_lib «WasmEquiv» where
  srcDir := "."

-- Zone protocol: port assignment and 100-byte packet layout
@[default_target]
lean_lib «ZoneProtocol» where
  srcDir := "."

-- Zone observer: headless_log_observer.gd exit semantics
@[default_target]
lean_lib «ZoneObserver» where
  srcDir := "."

-- Zone player: CH_PLAYER cmd codes + operator camera swing invariant
@[default_target]
lean_lib «ZonePlayer» where
  srcDir := "."
