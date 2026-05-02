# Skill: Vortex Internals

## Purpose
Keep Vortex-native layouts and semantics as first-class I/O and execution targets.

## When to use
Use for storage interfaces, scan paths, materialization, and output writers.

## Rules
- Vortex must remain a native input and output path, not a compatibility afterthought.
- Preserve encoded metadata/statistics needed for pruning and planning.
- Decode only when required for correctness; favor encoded-aware operations.
- Unsupported Vortex features must fail explicitly with precise diagnostics.
- Avoid introducing assumptions that require a different engine.

## Validation checklist
- [ ] Vortex read/write paths remain explicit and tested.
- [ ] Metadata usage for pruning/short-circuiting is preserved or improved.
- [ ] No forced full decode unless required by semantics.
- [ ] Unsupported cases return clear, deterministic errors.
