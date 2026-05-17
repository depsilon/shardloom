# Workspace Feature Build Matrix

## Purpose

This document records the Priority 3.5 workspace feature/build validation matrix. It is a
release-readiness gate only. It does not publish packages, expand dependencies, promote runtime
behavior, invoke external engines, or permit fallback execution.

The corresponding code surface is:

```text
WorkspaceFeatureBuildMatrixReport
WorkspaceFeatureBuildMatrixRow
WorkspaceFeatureBuildMatrixFeatureSet
WorkspaceFeatureBuildMatrixRowStatus
plan_workspace_feature_build_matrix()
```

## Required Rows

| Feature set | Command | Purpose |
| --- | --- | --- |
| `default_features` | `cargo check --workspace` | Ensure default workspace compile remains healthy. |
| `all_features` | `cargo check --workspace --all-features` | Ensure optional feature surfaces compile together. |
| `no_default_features` | `cargo check --workspace --no-default-features` | Ensure default-free report surfaces still compile. |
| `upstream_vortex` | `cargo check -p shardloom-vortex --features upstream-vortex` | Validate upstream Vortex dependency linkage posture. |
| `vortex_file_io` | `cargo check -p shardloom-vortex --features vortex-file-io` | Validate Vortex file-I/O feature-gated surfaces compile. |
| `vortex_local_primitives` | `cargo check -p shardloom-vortex --features vortex-local-primitives` | Validate local primitive feature-gated surfaces compile. |
| `vortex_encoded_read_spike` | `cargo check -p shardloom-vortex --features vortex-encoded-read-spike` | Validate encoded-read spike feature-gated surfaces compile. |
| `packaging_deployment` | `cargo test -p shardloom-contract-tests --test conda_packaging_recipes` | Validate current packaging/deployment recipe contracts. |
| `benchmark_extras` | `cargo check -p shardloom-vortex --features vortex-traditional-analytics-benchmark` | Validate optional benchmark-extra feature surfaces compile. |
| `future_foundry_optional` | future optional package; no crate exists yet | Keep the future Foundry package lane explicit without inventing a crate. |

## Release Gate

Public release/package claims stay blocked until this matrix is run in the release environment and
the required rows pass. The current report surface keeps:

```text
public_release_claim_blocked_until_matrix_passes=true
public_package_claim_blocked_until_matrix_passes=true
package_publication_performed=false
dependency_expansion_performed=false
runtime_expansion_performed=false
external_engine_invoked=false
fallback_attempted=false
```

Feature-disabled execution commands must continue returning deterministic unsupported diagnostics
instead of reaching for a runtime fallback.

## Optimized Build Profile Follow-Up

`GAR-PERF-2H` is the planned optimized build-profile and PGO benchmark lane. It is separate from the
current feature/build matrix. Future release-readiness evidence should distinguish portable release
profiles from benchmark-only profiles such as `release-native-benchmark`, record LTO/PGO/native
status when benchmark rows use optimized binaries, and keep `target-cpu=native` out of portable
release artifacts.
