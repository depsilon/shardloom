# Spill Reservation Lifecycle Integration

## Purpose

This document records memory/spill/recovery integration points that support future bounded
execution. Active phase status lives in `docs/architecture/phased-execution-plan.md`; this document
is a supporting checklist and completed ledger.

It does not authorize broad query-data spill, object-store spill, output dataset writes, retry
execution, cancellation execution, or fallback execution.

## Integrated Surface Map

- Memory reservation model
  - `MemoryReservation`
  - `MemoryReservationStatus`
  - `MemoryPoolPlan`
- Spill lifecycle planning
  - `SpillLifecycleRequest`
  - `plan_spill_lifecycle`
- Memory and OOM planning
  - `MemoryBudget`
  - `OomSafetyPlan`
  - memory pressure decisions
  - `PreOomMemoryGuardFixtureReport`
- Vortex memory bridge planning
  - `plan_vortex_memory_safety`
  - `VortexMemoryBridgeReport`
- Bounded local execution planning surfaces.
- CLI integration through `spill-lifecycle`, `vortex-memory-plan`, and
  `pre-oom-memory-guard-smoke`.

## Behavior Map

- Planning remains side-effect-free for memory/spill planning surfaces.
- Unsupported behavior returns explicit deterministic diagnostics.
- Fallback execution remains disabled by policy.
- Synthetic spill payload roundtrip exists behind feature gating.
- Exact synthetic payload cleanup exists behind feature gating.
- Retry and cancellation gates exist as planning/report surfaces.
- `pre-oom-memory-guard-smoke` exercises a bounded local reservation denial before process OOM and
  releases the granted reservation as cleanup evidence.
- Real query-data spill remains deferred until authorized in `phased-execution-plan.md`.
- Object-store spill remains deferred until authorized in `phased-execution-plan.md`.
- Retry execution remains deferred until authorized in `phased-execution-plan.md`.
- Cancellation execution remains deferred until authorized in `phased-execution-plan.md`.
- Output commit cleanup remains deferred until authorized in `phased-execution-plan.md`.

## Required Report Fields

Future spill and bounded-execution reports should preserve:

- `reservation_required`
- `reservation_status`
- `payload_write_allowed`
- `payload_written`
- `payload_read`
- `cleanup_performed`
- `spill_data_is_synthetic`
- `query_spill_data_written`
- `fallback_execution_allowed=false`
- `fail_before_oom`
- `cleanup_completed`
- `real_query_spill_admitted=false`
- `distributed_execution_admitted=false`

## Completed Ledger

- [x] Synthetic spill payload planning/write/read/roundtrip/CLI.
  - Synthetic payload support is not query or `Vortex` data spill.
  - Default build remains feature-disabled/report-only when `spill-payload-fs` is not enabled.
  - Feature build writes and reads only synthetic payloads.
- [x] Bounded execution spill payload integration.
  - `VortexBoundedSpillIntegrationReport` can model synthetic payload availability.
  - Nested blocked/unsupported states cannot be advertised as available.
  - Existing reservation blockers must not be downgraded.
- [x] Recovery context for bounded spill integration.
  - Task attempt and synthetic artifact cleanup context can be planned.
  - Unknown artifacts block cleanup/retry planning.
- [x] Exact synthetic payload cleanup execution.
  - Cleanup is limited to one exact known synthetic payload file.
  - Directories/workspaces are not deleted.
  - Unknown artifacts block deterministically.
- [x] Retry gate report and CLI integration.
  - `retry-gate-plan` is planning/report-only.
  - Cleanup completion derives only from actual cleanup execution state.
- [x] Cancellation gate report and CLI integration.
  - `cancellation-gate-plan` is planning/report-only.
  - Cleanup completion derives only from actual cleanup execution state.
- [x] Bounded pre-OOM memory guard fixture.
  - `pre-oom-memory-guard-smoke` emits `shardloom.pre_oom_memory_guard_fixture.v1`.
  - The fixture denies a bounded reservation request before process OOM and releases the granted
    reservation.
  - The fixture does not authorize real query-data spill, native spill IO, object-store IO,
    distributed runtime, or fallback execution.
- [x] Phase 11 recovery closeout.
  - Synthetic spill path is complete through CLI and cleanup.
  - Bounded spill integration exists.
  - Retry/cancellation gates exist.

## Validation

For implementation changes touching this area, run:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`

## Guardrails

- Synthetic spill support is not query-data spill.
- Do not perform object-store IO from these contracts.
- Do not write output datasets from spill cleanup/recovery contracts.
- Do not execute retry or cancellation until an explicit phase authorizes it.
- Promote future implementation work into `phased-execution-plan.md` before changing behavior.
