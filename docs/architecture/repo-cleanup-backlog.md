# Repo Cleanup Backlog

## Purpose

This document inventories cleanup, refactor, and audit work that supports future Competitive Engine
(CG) implementation. It is a supporting backlog only. Active status, active queue, and CG closeout
decisions live in `docs/architecture/phased-execution-plan.md`.

This document does not authorize runtime behavior, IO behavior, dependency additions, or fallback
execution.

## How To Use

- Promote actionable cleanup into `phased-execution-plan.md` before implementation.
- Keep completed cleanup here as a historical ledger, not a second active queue.
- Keep cleanup PRs narrow, reviewable, and no-fallback.
- Preserve public commands and report schemas unless an explicit phase item says otherwise.

## Backlog Checklist

- [x] P0 - Documentation and traceability correctness
  - Ensure `phased-execution-plan.md` matches merged CG/doc work.
  - Keep `rfc-phase-traceability.md` Markdown tables valid.
  - Keep CG-1 through CG-20 visible in planning artifacts.
  - Keep Foundry under CG-18 optional deployment/comparison.
  - Keep systems-learning references conceptual only.
  - Keep hidden/bidi Unicode scans in docs PRs.
- [x] P1 - CLI usage/name consistency
  - User-facing usage should say `shardloom`, not `shardloom-cli`, unless naming the crate.
  - Command names should distinguish plan/report/probe/write/execute.
  - Future command registry can centralize names, usage text, and JSON mode fields.
  - Commands should not imply execution unless they perform execution.
- [x] P2 - Diagnostics normalization
  - Route missing/unknown CLI arguments through stable invalid-input diagnostics where feasible.
  - Preserve `fallback_execution_allowed=false`.
  - Distinguish invalid input, unsupported feature, configuration, planning, execution,
    object-store, materialization, and no-fallback categories.
- [x] P3 - Terminology consolidation
  - Keep layer-specific public types stable.
  - Prefer mapping helpers before type consolidation.
  - Keep translation, compatibility output, and fallback execution distinct.
- [x] P4 - Feature-footprint/doctor centralization
  - Centralize feature/dependency/capability posture in `FeatureFootprintReport`.
  - Keep doctor/capabilities alignment report-only and no-probe by default.
  - Keep external baselines separate from runtime fallback availability.
- [x] P5 - Cross-crate invariant tests
  - Verify forbidden fallback engines are absent from manifests and lockfile.
  - Keep docs/conceptual references out of dependency scans.
- [x] P6 - Planned refactor candidates promoted to `GAR-0039-B` and `GAR-0043-A`
  - Command registry / generated help.
  - Diagnostic code constants for all CLI errors.
  - Centralized report field helpers.
  - Traceability matrix validator.
  - RFC acceptance checker.
  - Systems-learning contract implementation tracker.
  - Capability certification implementation tracker for CG-20.

## Completed Ledger

- [x] R3.1 cleanup backlog inventory.
- [x] R3.2 CLI usage/name consistency audit.
- [x] R3.3 diagnostics normalization backlog.
- [x] R3.3a CLI missing/unknown argument diagnostic helpers.
- [x] R3.3b unknown signal diagnostic normalization.
- [x] R3.3c output envelope command-status derivation audit.
- [x] R3.4 terminology consolidation backlog.
- [x] R3.5 feature-footprint/doctor centralization plan.
- [x] R3.5a feature-footprint report core contract.
- [x] R3.5d no-fallback dependency invariant tests.

## Guardrails

- Do not rename public commands as part of cleanup unless the phase plan explicitly calls for it.
- Do not add runtime behavior from this backlog alone.
- Do not add dependencies from this backlog alone.
- Do not infer CG completion from cleanup-only work.
- Do not introduce fallback execution.
