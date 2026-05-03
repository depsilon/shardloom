# Spill reservation lifecycle integration checklist

This document records the current planning-phase integration points for spill reservation lifecycle behavior.

## Integrated surfaces

- Spill reservation types are modeled through `MemoryReservation`, `MemoryReservationStatus`, and `MemoryPoolPlan` in `shardloom-exec`.
- Spill lifecycle integration is modeled through `SpillLifecycleRequest` and `plan_spill_lifecycle`.
- Memory integration is modeled through `MemoryBudget`, `OomSafetyPlan`, and memory pressure decisions.
- Vortex memory bridge integration is modeled through `plan_vortex_memory_safety` and `VortexMemoryBridgeReport`.
- Bounded execution integration is modeled through bounded Vortex local execution planning surfaces.
- CLI integration is exposed via `spill-lifecycle` and `vortex-memory-plan` with explicit machine-readable integration fields.

## Feature behavior

- Planning remains side-effect-free for memory/spill planning surfaces.
- Unsupported behavior returns explicit deterministic diagnostics.
- Fallback execution remains disabled by policy.

## Validation

Run:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`
