# Feature Footprint and Doctor Centralization Plan

## Purpose

- This document inventories scattered feature/dependency/capability/doctor status surfaces.
- It defines a future `FeatureFootprintReport` contract.
- It does not implement runtime behavior.
- It does not authorize fallback execution.
- It does not add dependencies.

## Current posture

- Default `shardloom-vortex` build should remain lightweight.
- Upstream `Vortex` is feature-gated.
- `vortex-encoded-read-executor` is feature-gated.
- `vortex-file-io` may be locally blocked by rustc/upstream dependency mismatch and should not be required unless toolchain validates.
- Fallback engines remain absent and disallowed.
- `doctor`/`capabilities`/output envelope already expose some feature/fallback state, but not through one centralized report contract.

## Current scattered surfaces

Current feature/dependency/doctor/capability posture appears across these surfaces:

- `shardloom-core::CapabilityReport` / core capabilities model (`EngineCapabilities`).
- `doctor` CLI command surface.
- `capabilities` CLI command surface.
- `OutputEnvelope` fallback object (`fallback`).
- `ShardLoomError` / `Diagnostic` fallback status (`FallbackStatus`).
- `VortexAdapterCapabilityReport`.
- Vortex feature helpers:
  - `vortex_encoded_read_executor_feature_enabled`
  - `vortex_file_io_feature_enabled`
  - `vortex_metadata_executor_feature_enabled`
  - `vortex_encoded_read_spike_feature_enabled`
  - spill feature helper parity where relevant (for cross-crate gate posture)
- Staged output/write feature fields and staged artifact gate reports.
- Spill payload feature fields (`spill_payload_fs_feature_enabled`) and related report gates.
- Cleanup/retry/cancellation gate surfaces.
- Release/dependency/license review surfaces.
- External baseline availability documentation surfaces (comparison-only, non-runtime).

## Future FeatureFootprintReport contract

Future contract fields:

- `schema_version`
- `engine_version`
- `crate_versions`
- `compiled_features`
- `enabled_features`
- `disabled_features`
- `upstream_vortex_dependency_status`
- `upstream_vortex_version`
- `vortex_gates`
- `encoded_read_gates`
- `metadata_io_gates`
- `write_gates`
- `spill_gates`
- `cleanup_gates`
- `object_store_gates`
- `distributed_execution_gates`
- `external_baseline_availability`
- `fallback_engines_absent`
- `fallback_execution_allowed=false`
- `diagnostics`
- `generated_at` (optional/deferred)

Contract guardrails:

- `generated_at` should be omitted or deterministic until timestamp policy is accepted.
- No filesystem/object-store probing by default.
- No network calls.
- No external engine execution.

## Vortex gate fields

Normalized gate names:

- `upstream_vortex`
- `vortex_file_io`
- `vortex_metadata_executor`
- `vortex_encoded_read_executor`
- `vortex_staged_output_fs`
- `vortex_write`
- `vortex_object_store`
- `vortex_output_payload`
- `vortex_commit_execution`

Per-gate field shape:

- `compiled`
- `enabled`
- `default_enabled`
- `requires_toolchain`
- `allows_io`
- `allows_scan`
- `allows_write`
- `allows_object_store`
- `diagnostics`

## Doctor/capabilities alignment

Future alignment direction:

- `doctor` should eventually report environment/readiness using `FeatureFootprintReport`.
- `capabilities` should eventually report supported/planned/disabled features using the same normalized feature names.
- CLI JSON fields should use consistent keys.
- Output envelope fallback status must remain consistent with feature-footprint fallback fields.

## No-fallback dependency checks

Future checks should assert:

- no direct or transitive Spark
- no direct or transitive DataFusion
- no direct or transitive vortex-datafusion
- no direct or transitive DuckDB
- no direct or transitive Polars
- no direct or transitive Velox
- external engines may appear only in external baseline harness scopes and must not be runtime dependencies

Scope note:

- This PR does not implement dependency scanning.
- Future tests can use `cargo tree` or manifest inspection.

## Future tests

- default `shardloom-vortex` feature graph remains lightweight
- fallback engines absent from dependency graph
- `FeatureFootprintReport` fallback allowed false
- doctor/capabilities share normalized feature keys
- `vortex-encoded-read-executor` feature status stable
- `vortex-file-io` toolchain mismatch is reported, not silently ignored
- external baseline availability is separate from runtime fallback

## Follow-up implementation sequence

- R3.5a feature-footprint report core contract, no probing
- R3.5b doctor/capabilities docs alignment
- R3.5c optional CLI feature-footprint report, no filesystem/network
- R3.5d dependency invariant tests for no fallback engines
- R4 resume CG implementation, unless user keeps cleanup queue active


## R3.5a implementation status

- `FeatureFootprintReport` core contract implemented.
- No probing.
- No `doctor`/`capabilities` behavior change.
- No CLI exposure yet.
- No dependency scanning yet.
- Next possible follow-up:
  - R3.5d no-fallback dependency invariant tests, or
  - R4 resume CG implementation if user chooses.

## R3.5d invariant status

- no-fallback dependency invariant tests added.
- tests inspect manifests and Cargo.lock, not docs.
- external systems remain conceptual references or external baselines only.
- Arrow transitive packages are not treated as fallback engines.
- no runtime behavior changed.


## CG-1.2d note

- CG-1.2d uses feature-specific validation before enabling local metadata/footer IO.
- `FeatureFootprintReport` behavior remains unchanged in this phase.
