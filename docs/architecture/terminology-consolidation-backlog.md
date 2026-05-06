# Terminology Consolidation Backlog

## Purpose

- This document inventories terminology families that are intentionally distinct today but need stable mappings before deeper CG implementation.
- It is not runtime authorization.
- It does not rename public types.
- It favors mapping helpers and documentation before type consolidation.

## Current posture

- `docs/architecture/canonical-terminology.md` is the source of canonical vocabulary.
- Public types must not be renamed without compatibility plan.
- Translation is not fallback.
- Arrow boundaries are not default execution.
- Vortex-native output remains distinct from compatibility output.
- Mapping helpers are preferred before consolidation.

## P0 — Materialization terminology family

Audit:

- `MaterializationPolicy`
- `MaterializationRequirement`
- `MaterializationBoundary`

Required decision:

- Keep all three for now.
- Add mapping helpers later.

Explanation:

- `MaterializationPolicy`: planner/user-intent or encoded execution preference.
- `MaterializationRequirement`: sink/output compatibility requirement.
- `MaterializationBoundary`: runtime/streaming execution boundary.

Future helper candidates:

- `materialization_policy_to_requirement`
- `materialization_requirement_to_boundary`
- `materialization_boundary_to_execution_state`

## P1 — Execution/data-work terminology family

Audit:

- `ExecutionState`
- `DataWorkLevel`
- encoded/readiness/probe execution labels if present

Required decision:

- Keep separate.
- Add mapping helper from `DataWorkLevel` to `ExecutionState`.
- Do not collapse execution state and work-rank abstraction.

Explanation:

- `ExecutionState`: command/report execution state.
- `DataWorkLevel`: work-level/rank indicating metadata-only, zero-decode, encoded-evaluation, partial/full materialization.

Future helper:

- `data_work_level_to_execution_state`

## P2 — Input/output format terminology family

Audit:

- `DatasetFormat`
- `OutputTargetKind`
- `VortexOutputFidelity`
- `FidelityLevel`

Required decision:

- Keep `DatasetFormat` and `OutputTargetKind` separate.
- Keep `FidelityLevel` canonical.
- Treat `VortexOutputFidelity` as Vortex-local and map to `FidelityLevel`.

Future helper candidates:

- `dataset_format_to_default_output_target_kind`
- `vortex_output_fidelity_to_fidelity_level`
- `output_target_kind_to_default_fidelity_level`

Explanation:

- Input/reference format is not output target.
- Compatibility output is not fallback.
- Native Vortex output is highest fidelity.

## P3 — Resource/memory terminology family

Audit:

- `ResourceBudget`
- `MemoryBudget`
- `BoundedMemoryPolicy`
- spill reservation policy terms if present

Required decision:

- Keep separate for now.
- Add mapping/reporting helper later.

Explanation:

- `ResourceBudget`: task/runtime-wide budget concept.
- `MemoryBudget`: memory/spill/OOM budget.
- `BoundedMemoryPolicy`: streaming/runtime boundedness policy.

Future helper candidates:

- `resource_budget_to_memory_budget_summary`
- `bounded_memory_policy_to_spill_requirement`

## P4 — Plan skeleton terminology family

Audit:

- `RuntimePlanSkeleton`
- `StreamingPlanSkeleton`
- `ScanPlanSkeleton`
- explain/estimate plan nodes if relevant

Required decision:

- Keep separate.
- Add shared field/report helper later.
- Do not collapse scan/streaming/runtime layers.

Explanation:

- scan plan = input/read intent
- streaming plan = materialization/backpressure boundary
- runtime plan = task/execution skeleton

Future helper candidates:

- `plan_skeleton_common_fields`
- `plan_skeleton_diagnostics_summary`
- `plan_skeleton_side_effect_summary`

## P5 — Status/report naming conventions

Audit common suffixes:

- `Plan`
- `Request`
- `Report`
- `Status`
- `Mode`
- `Signal`
- `Effect`
- `Ref`
- `Id`

Required decision:

- Keep current suffixes.
- Document conventions.

Conventions:

- `Request`: caller-provided intent/input.
- `Plan`: planned behavior, usually no side effects.
- `Report`: evaluated result/status/diagnostics.
- `Status`: top-level state.
- `Mode`: behavior class.
- `Signal`: input evidence/gate indicator.
- `Effect`: action that actually happened.
- `Ref`: external or filesystem/object reference, may not imply IO.
- `Id`: stable logical identity.

## P6 — Future tests and helpers

Future tests:

- terminology mapping helpers are stable
- translation fidelity ↔ Vortex fidelity mapping consistency
- DataWorkLevel ↔ ExecutionState mapping stability
- materialization mapping stability
- plan skeleton common field consistency
- compatibility output never treated as fallback

Do not add broad tests now.
