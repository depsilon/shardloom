# Terminology Consolidation Backlog

## Purpose

This document inventories terminology families that are intentionally distinct today but need stable
mappings before deeper CG implementation. Active queue placement lives in
`docs/architecture/phased-execution-plan.md`; canonical definitions live in
`docs/architecture/canonical-terminology.md`.

It does not authorize runtime behavior, rename public types, or collapse layer boundaries by itself.

## Consolidation Principles

- Preserve public names until a compatibility plan exists.
- Prefer mapping helpers before broad type consolidation.
- Keep translation distinct from fallback execution.
- Keep Arrow boundaries distinct from default execution.
- Keep Vortex-native output distinct from compatibility output.

## Backlog Checklist

- [ ] P0 - Materialization terminology family
  - Keep `MaterializationPolicy`.
  - Keep `MaterializationRequirement`.
  - Keep `MaterializationBoundary`.
  - Add mapping helpers only when cross-layer rendering requires them.
  - Candidate helpers: `materialization_policy_to_requirement`,
    `materialization_requirement_to_boundary`, `materialization_boundary_to_execution_state`.
- [ ] P1 - Execution/data-work terminology family
  - Keep `ExecutionState`.
  - Keep `DataWorkLevel`.
  - Do not collapse execution state and work-rank abstraction.
  - Candidate helper: `data_work_level_to_execution_state`.
- [ ] P2 - Input/output/fidelity terminology family
  - Keep `DatasetFormat` and `OutputTargetKind` separate.
  - Keep `FidelityLevel` canonical.
  - Treat `VortexOutputFidelity` as adapter-local and map it to `FidelityLevel`.
  - Candidate helpers: `dataset_format_to_default_output_target_kind`,
    `vortex_output_fidelity_to_fidelity_level`, `output_target_kind_to_default_fidelity_level`.
- [ ] P3 - Resource/memory terminology family
  - Keep `ResourceBudget`.
  - Keep `MemoryBudget`.
  - Keep `BoundedMemoryPolicy`.
  - Candidate helpers: `resource_budget_to_memory_budget_summary`,
    `bounded_memory_policy_to_spill_requirement`.
- [ ] P4 - Plan skeleton terminology family
  - Keep `RuntimePlanSkeleton`.
  - Keep `StreamingPlanSkeleton`.
  - Keep `ScanPlanSkeleton`.
  - Add shared field/report helpers later if duplication becomes material.
  - Candidate helpers: `plan_skeleton_common_fields`, `plan_skeleton_diagnostics_summary`,
    `plan_skeleton_side_effect_summary`.
- [ ] P5 - Status/report naming conventions
  - `Request`: caller-provided intent/input.
  - `Plan`: planned behavior, usually no side effects.
  - `Report`: evaluated result/status/diagnostics.
  - `Status`: top-level state.
  - `Mode`: behavior class.
  - `Signal`: input evidence/gate indicator.
  - `Effect`: action that actually happened.
  - `Ref`: external or filesystem/object reference, may not imply IO.
  - `Id`: stable logical identity.
- [ ] P6 - Future tests and helpers
  - Mapping helpers are stable.
  - Translation fidelity maps consistently to Vortex fidelity.
  - `DataWorkLevel` maps consistently to `ExecutionState`.
  - Materialization mapping is stable.
  - Plan skeleton common fields are consistent.
  - Compatibility output is never treated as fallback.

## Completed Ledger

- [x] R3.4 terminology audit.
  - Added this backlog.
  - Deferred public type renames.
  - Confirmed mapping-helper-first posture.

## Guardrails

- Do not rename public types from this document alone.
- Do not collapse separate planning/execution/runtime boundaries for cosmetic consistency.
- Promote any implementation cleanup into `phased-execution-plan.md` before changing code.
