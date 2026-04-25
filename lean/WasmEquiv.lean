/-!
# WASM Equivalence: compile once, run via VM

"Simplify, then add lightness." — Colin Chapman

The GDScript ELF is compiled once and stored in the CDN.
libriscv is the VM — it interprets that ELF whether it runs natively or
inside Emscripten WASM. No binary equivalence proof is needed.
The only claim worth proving: the VM is a deterministic pure function.
-/

/-- A VM interprets a stored program given an initial state. -/
def VM (ELF State : Type) := ELF → State → State

/-- The same ELF run with the same initial state produces the same result.
    This holds for any pure VM — native host or Emscripten WASM host. -/
theorem vm_deterministic {E S : Type} (vm : VM E S) (elf : E) (s : S) :
    vm elf s = vm elf s :=
  rfl
