# Feature Footprint and Doctor Centralization Plan

## Purpose

This document inventories feature, dependency, capability, and doctor status surfaces that should converge on `FeatureFootprintReport`. Active queue and completion state live in `docs/architecture/phased-execution-plan.md`.

It does not implement runtime behavior, authorize fallback execution, or add dependencies.

## Design Posture

- Default `shardloom-vortex` builds should remain lightweight.
- Upstream `Vortex` remains feature-gated.
- Feature-gated Vortex IO paths must stay explicit and narrow.
- Fallback engines remain absent and disallowed.
- `doctor`, `capabilities`, and output envelopes should expose consistent feature/fallback state through stable report fields.

## Surface Inventory

- `shardloom-core::CapabilityReport` / `EngineCapabilities`.
- `doctor` CLI command surface.
- `capabilities` CLI command surface.
- `OutputEnvelope` fallback object.
- `ShardLoomError`, `Diagnostic`, and `FallbackStatus`.
- `VortexAdapterCapabilityReport`.
- Vortex feature helpers.
- Staged output/write feature gates.
- Spill payload feature gates.
- Cleanup/retry/cancellation gate surfaces.
- Release/dependency/license review surfaces.
- External baseline documentation surfaces.

## FeatureFootprintReport Checklist

- [x] Core no-probe report contract
  - `schema_version`
  - `engine_version`
  - `crate_versions`
  - `compiled_features`
  - `enabled_features`
  - `disabled_features`
  - `fallback_engines_absent`
  - `fallback_execution_allowed=false`
  - `diagnostics`
- [x] Vortex gate fields
  - `upstream_vortex`
  - `vortex_file_io`
  - `vortex_metadata_executor`
  - `vortex_encoded_read_executor`
  - `vortex_staged_output_fs`
  - `vortex_write`
  - `vortex_object_store`
  - `vortex_output_payload`
  - `vortex_commit_execution`
- [ ] Doctor/capabilities alignment
  - `doctor` eventually reports environment/readiness through `FeatureFootprintReport`.
  - `capabilities` eventually reports supported/planned/disabled features using normalized names.
  - CLI JSON fields use consistent keys.
  - Output envelope fallback state matches feature-footprint fallback fields.
- [x] No-fallback dependency checks
  - No direct or transitive Spark runtime dependency.
  - No direct or transitive DataFusion runtime dependency.
  - No direct or transitive `vortex-datafusion` runtime dependency.
  - No direct or transitive DuckDB, Polars, or Velox runtime dependency.
  - External engines may appear only in external baseline scopes and must not be runtime dependencies.
- [ ] Future tests
  - Default `shardloom-vortex` feature graph remains lightweight.
  - `FeatureFootprintReport` fallback allowed false.
  - Doctor/capabilities share normalized feature keys.
  - Feature-gated Vortex status is stable.
  - Toolchain mismatch is reported, not ignored.
  - External baseline availability is separate from runtime fallback.

## Completed Ledger

- [x] R3.5a
  - Implemented `FeatureFootprintReport` core contract.
  - Kept it no-probe.
  - Did not change doctor/capabilities behavior.
  - Did not add CLI exposure or dependency scanning in that pass.
- [x] R3.5d
  - Added no-fallback dependency invariant tests.
  - Tests inspect manifests and `Cargo.lock`, not docs.
  - External systems remain conceptual references or external baselines only.
  - Arrow transitive packages are not treated as fallback engines.
- [x] CG-1.2d note
  - CG-1.2d uses feature-specific validation before local metadata/footer IO.
  - `FeatureFootprintReport` behavior remains unchanged by that phase.

## Guardrails

- Do not probe filesystem, network, catalogs, or adapters by default.
- Do not add generated timestamps until deterministic timestamp policy exists.
- Do not treat external baseline availability as fallback availability.
- Promote future implementation work into `phased-execution-plan.md` before editing behavior.
