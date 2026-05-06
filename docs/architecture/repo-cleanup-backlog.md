# Repo Cleanup Backlog

## Purpose

This document collects cleanup, refactor, and audit items needed before or alongside future Competitive Engine (CG) implementation. It is a prioritization and sequencing artifact only; it is **not** runtime authorization for execution behavior, IO behavior, or fallback behavior.

## Current cleanup priority

- **P0 — Documentation and traceability correctness**
- **P1 — CLI usage/name consistency**
- **P2 — Diagnostics normalization**
- **P3 — Terminology consolidation**
- **P4 — Feature-footprint/doctor centralization**
- **P5 — Cross-crate invariant tests**
- **P6 — Future refactor candidates**

## P0 — Documentation and traceability correctness

Backlog items:

- Ensure `phased-execution-plan.md` current status always matches the last merged CG/doc PR.
- Ensure `rfc-phase-traceability.md` has valid Markdown table structure for all matrix rows.
- Keep CG-1 through CG-18 visible in phase proposals.
- Ensure Foundry remains under CG-18 optional deployment/comparison.
- Ensure `systems-learning-map.md` remains conceptual only.
- Keep hidden/bidi Unicode scan in docs PRs.

## P1 — CLI usage/name consistency

Audit scope: `shardloom-cli/src/main.rs`.

Backlog items:

- Usage/help banner should consistently say `shardloom`, not `shardloom-cli`, unless intentionally naming the crate.
- Command names should distinguish plan/report/probe/write/execute.
- A future command registry may centralize command names, usage text, and JSON mode fields.
- All commands should eventually support stable `--format json`.
- No command should imply execution unless it performs execution.

Notes from this audit:

- Current top-level usage banner already uses `shardloom` and is aligned with this requirement.
- Command family size has grown enough that centralized registry generation is now a high-value cleanup candidate, but should land as a targeted follow-up PR rather than in this inventory PR.

### R3.2 audit result

- Usage/help banner status: user-facing usage remains `shardloom`; no `shardloom-cli` binary-facing drift identified.
- Small text fixes made: none required for the top-level usage string in this pass.
- Future registry cleanup candidates: centralizing command names, usage/help text, and JSON mode field ownership in a command registry/generated-help follow-up.
- Command families that should remain behavior-distinguished: `*-plan`, `*-probe`, `*-write`, and explicitly-scoped `*-execute` commands.
- This PR did not implement a command registry.

Do not implement command registry in this PR.

## P2 — Diagnostics normalization

Backlog items:

- Remaining user-visible parse/argument failures should route through stable diagnostic codes where feasible.
- Avoid vague string-only errors in CLI outputs.
- Preserve `fallback_execution_allowed=false`.
- Diagnostics should distinguish invalid input, unsupported feature, configuration, planning, execution, object-store, materialization, and no-fallback policy.
- Future report schemas from RFC 0012 should be implemented incrementally.

Do not refactor diagnostics in this PR.

### R3.3 audit result

- diagnostics normalization backlog document added
- broad migration deferred
- next recommended diagnostics PRs:
  - R3.3a CLI missing/unknown argument diagnostic helpers
  - R3.3b unknown signal diagnostic normalization
  - R3.3c output envelope command-status derivation audit

## P3 — Terminology consolidation

Audit of overlapping concepts and planned posture:

- `MaterializationPolicy` — **keep**.
  - Reason: planner/user intent at encoded execution layer is distinct from sink/runtime requirements.
- `MaterializationRequirement` — **keep**.
  - Reason: sink/output contract requirement, not equivalent to planner intent.
- `MaterializationBoundary` — **keep**.
  - Reason: runtime/streaming boundary point; separate concern from intent and requirement.
- `ExecutionState` — **keep**.
  - Reason: canonical execution status vocabulary for reports/diagnostics.
- `DataWorkLevel` — **needs mapping helper**.
  - Reason: work-ranking concept should map consistently into `ExecutionState` without collapsing layers.
- `DatasetFormat` — **keep**.
  - Reason: input/reference identity differs from output targeting.
- `OutputTargetKind` — **keep**.
  - Reason: output contract target should remain separate from input format classification.
- `VortexOutputFidelity` — **needs mapping helper**.
  - Reason: adapter-local fidelity should map predictably into canonical fidelity.
- `FidelityLevel` — **keep**.
  - Reason: cross-layer canonical fidelity vocabulary.
- `ResourceBudget` — **keep**.
  - Reason: task/runtime-level budget concept distinct from memory-pool policy.
- `MemoryBudget` — **keep**.
  - Reason: memory/spill/OOM budgeting layer.
- `BoundedMemoryPolicy` — **keep**.
  - Reason: streaming/runtime boundedness policy should remain explicit.
- `RuntimePlanSkeleton` — **keep**.
  - Reason: runtime planning contract root for execution-facing surfaces.
- `StreamingPlanSkeleton` — **consolidate later**.
  - Reason: keep separate now, but review for field harmonization with runtime/scan plan skeletons.
- `ScanPlanSkeleton` — **consolidate later**.
  - Reason: keep separate now, but review for shared plan/report field helpers.

Do not rename public types in this PR.

## P4 — Feature-footprint/doctor centralization

Backlog items:

- Current feature posture is scattered across capability docs, Vortex feature gates, adapter readiness, output fields, and doctor/capabilities.
- A future `FeatureFootprintReport` should centralize:
  - compiled features
  - enabled Vortex gates
  - upstream dependency status
  - object-store/write/spill gates
  - fallback-engine absence
  - external baseline availability
- Doctor/capabilities should eventually expose the same normalized fields.

Do not implement `FeatureFootprintReport` in this PR.

## P5 — Cross-crate invariant tests

High-priority future invariant tests:

- No fallback engines in dependency graph.
- Translation fidelity ↔ Vortex fidelity mapping consistency.
- CLI command list/help parity.
- JSON field stability for encoded-read boundary/fixture/metadata probe.
- Plan-only/no-side-effect invariants across scan/runtime/Vortex commands.
- Placeholder payload artifacts never satisfy real payload write gate.
- Systems-learning references do not become dependencies.

Do not add broad new tests in this PR; keep this as backlog inventory.

## P6 — Future refactor candidates

Candidates (deferred until targeted follow-up PRs):

- Command registry / generated help.
- Diagnostic code constants for all CLI errors.
- Centralized report field helpers.
- Canonical feature footprint report.
- Traceability matrix validator.
- RFC acceptance checker.
- Systems-learning contract implementation tracker.
