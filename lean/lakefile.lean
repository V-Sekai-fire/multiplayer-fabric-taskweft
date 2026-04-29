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

-- Zone protocol: port assignment and 100-byte packet layout
@[default_target]
lean_lib «ZoneProtocol» where
  srcDir := "."
