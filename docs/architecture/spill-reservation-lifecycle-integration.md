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

## Spill payload roundtrip CLI

- `spill-payload-roundtrip` exposes the synthetic local payload roundtrip API.
- Default build is feature-disabled/report-only when `spill-payload-fs` is not enabled.
- Feature build under `spill-payload-fs` writes and reads only synthetic payloads.
- Optional cleanup removes only the exact payload file created for the request.
- No `Vortex`/query data spill is performed.
- No object-store IO is performed.
- No output dataset writes are performed.
- No fallback execution is allowed.

## All-phase epic retro

- Synthetic spill payload support is now plan/write/read/roundtrip/CLI complete.
- It is still synthetic only.
- It is not permission to spill Vortex/query data.
- Phase 11A.3b must connect bounded execution to spill payload support through explicit reservation and feature gates.
- Required next machine-readable fields:
  - `reservation_required`
  - `reservation_status`
  - `payload_write_allowed`
  - `payload_written`
  - `payload_read`
  - `cleanup_performed`
  - `spill_data_is_synthetic`
  - `fallback_execution_allowed=false`

## Validation

Run:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`


## Bounded execution spill payload integration

- Bounded execution can now plan synthetic spill payload integration.
- Synthetic payload support is not query/`Vortex` data spill.
- Machine-readable fields include `reservation_required`, `reservation_status`, `payload_write_allowed`, `payload_written`, `payload_read`, `cleanup_performed`, `spill_data_is_synthetic`, `query_spill_data_written=false`, and `fallback_execution_allowed=false`.
- Phase 11A.3b.1 fixes status propagation: nested `SpillPayloadRoundTripReport` states (feature-disabled, blocked, unsupported, or verification/error states) cannot be advertised as `PayloadRoundTripAvailable`.
- Existing blockers from reservation planning must not be downgraded to `PayloadPlanReady`.
- Synthetic spill support remains distinct from query/`Vortex` data spill.


## Recovery context for bounded spill integration

- Bounded spill integration can now produce recovery planning context for task attempts and synthetic artifacts.
- Synthetic payload cleanup is planned only; cleanup is not executed in this phase.
- Unknown artifacts block cleanup/retry planning until they are explicitly classified.
- No object-store IO, output dataset write, or fallback execution behavior is introduced.
