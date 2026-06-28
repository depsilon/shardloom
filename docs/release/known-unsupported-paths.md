<!-- SPDX-License-Identifier: Apache-2.0 -->

# Known Unsupported Paths

Status: release-gate input. This document scopes public claims and does not authorize new runtime
behavior.

ShardLoom currently exposes a certified local Vortex analytics slice and evidence-first planning
surfaces. These unsupported surfaces are explicit boundaries, not accidental blockers to the
finished-product v1 support rows. A surface is outside the current finished-product v1 support boundary
until a matching phase-plan item closes with implementation, correctness, benchmark, security,
documentation, and no-fallback evidence.

The v1 inclusion queue is tracked in
[`docs/release/v1-inclusion-scope-matrix.md`](v1-inclusion-scope-matrix.md). Broad platform
families that are marked as v1 candidates pending feasibility are not outside v1 by default;
deferred rows require deterministic unsupported diagnostics, `fallback_attempted=false`, and
`external_engine_invoked=false`.

ShardLoom must not be described as a broad replacement for every SQL, DataFrame, streaming,
object-store, platform, or Foundry workload until those exact paths have claim-grade evidence.

## Unsupported Or Future Before Public Claims

- broad SQL/DataFrame execution
  - `capabilities sql` and `capabilities dataframe` expose the GAR-0001A-A planner-readiness matrix
    as report-only/unsupported evidence, not SQL parser/planner or DataFrame runtime support
- broad live/hybrid production behavior
- object-store runtime
- distributed scheduler/runtime
- lakehouse/table runtime claims beyond report-only catalog/table compatibility planning
- real Foundry proof-of-use beyond the local Foundry-style smoke; Foundry remains a future
  validation target only, with no Palantir endorsement or Foundry-certified claim
- Foundry dataset source/sink execution
- external platform execution through Spark, Snowflake, Databricks, BigQuery, Trino, Ray, Dask, or
  Velox
- internal local-source smoke compatibility execution as a Vortex-native claim
- native Vortex operator coverage beyond the documented supported scenarios
- layout/write, device/GPU, object-store I/O, and managed-platform comparison claims unless
  `vortex_layout_device_managed_boundary_ref` has claim-grade execution, Native I/O, benchmark, and
  no-fallback evidence for the exact workload
- distributed, object-store, and lakehouse runtime claims unless
  `global_architecture_runtime_claim_gate` has workload-scoped execution, Native I/O, credential,
  benchmark, policy/no-fallback, and release-readiness evidence for the exact claim
- production security posture beyond the current release-gate evidence

## Production-Family Diagnostic Catalog

Schema marker: `shardloom.production_unsupported_diagnostics.v1`.

This catalog gives users and agents a stable unsupported diagnostic boundary for production-family
entrypoints that exist as stubs, preview routes, fixture smokes, report-only commands, release
gates, or public-claim gates. These rows are not deferrals by default; they are the deterministic
firewall until the matching phase-plan item closes with runtime, safety, release, and claim
evidence.

Catalog invariant:

```text
fallback_attempted=false
external_engine_invoked=false
side_effects_performed=false
claim_gate_status=not_claim_grade
```

| Diagnostic row | Production family | User-facing entrypoints | Diagnostic code | Blocker | Boundary |
| --- | --- | --- | --- | --- | --- |
| `diagnostic_row_id=broad_sql_dataframe_runtime` | SQL/DataFrame | `sql`, `LazyFrame.collect`, `workflow-unsupported-plan`, `capabilities sql`, `capabilities dataframe` | `SL_UNSUPPORTED_PRODUCTION_SQL_DATAFRAME` | `cg21.workflow.sql.frontend_unsupported` | Broad production SQL/DataFrame execution is not admitted; scoped local-source/source-free smokes remain fixture evidence only. |
| `diagnostic_row_id=object_store_runtime` | Object store | `object_store_read`, `object_store_write`, cloud URI inputs, object-store fixture smokes | `SL_UNSUPPORTED_PRODUCTION_OBJECT_STORE` | `review-p0-3.object_store_runtime_and_path_safety_required`; `object_store_local_emulator_runtime_v1_candidate` | Scoped local-emulator object-store fixture evidence is admitted as non-claim-grade; production cloud reads/writes, credentials, retries, bounded streaming, range-read scale, table commits, and real-backend proof remain blocked outside that fixture scope. |
| `diagnostic_row_id=lakehouse_table_runtime` | Lakehouse/table | `table_commit`, `catalog_integration`, local table commit/recovery smokes | `SL_UNSUPPORTED_PRODUCTION_TABLE_RUNTIME` | `platform.table_catalog_runtime_evidence_required` | Lakehouse catalog transactions and production table commit/recovery are blocked. |
| `diagnostic_row_id=foundry_integration_pack` | Foundry | Foundry generated-output reports, dataset source/sink references, adapter capability reports | `SL_UNSUPPORTED_PRODUCTION_FOUNDRY` | `platform.foundry_integration_evidence_required` | Foundry proof-of-use, dataset source/sink execution, and platform-certified behavior are not in the local v1 scope. |
| `diagnostic_row_id=live_hybrid_remote_distributed_runtime` | Execution fabric | `live`, `hybrid`, `remote`, `distributed`, event-stream fixtures | `SL_UNSUPPORTED_PRODUCTION_EXECUTION_FABRIC` | `cg22.cg23.object_store_runtime_evidence_required` | Production live/hybrid/remote/distributed execution is blocked outside scoped fixtures and planning reports. |
| `diagnostic_row_id=rest_event_remote_api_runtime` | REST/event API | REST contract plans, plan previews, remote result delivery, event streams | `SL_UNSUPPORTED_PRODUCTION_REMOTE_API` | `cg23.remote_api.lifecycle.uncertified_blocked` | REST/event APIs are discovery/control-plane plans only; remote execution and data-plane delivery are blocked. |
| `diagnostic_row_id=arbitrary_extension_effect_runtime` | Extensions/effects | Extension registry/inspection, UDF runtime plans, API/model calls, embedding generation, SQLite effects | `SL_UNSUPPORTED_PRODUCTION_EXTENSION_EFFECT` | `gar-0032-d.effectful_runtime_blocked` | Arbitrary extensions, plugins, Python callables, network APIs, model calls, and external effects remain blocked unless a typed effect policy admits a specific fixture. |
| `diagnostic_row_id=public_package_publication` | Future package channels | Scoop, winget, conda-forge, GHCR, crates.io | `SL_UNSUPPORTED_PUBLIC_PACKAGE_PUBLICATION` | `release.package_publication_gate_required` | Selected v0.2.1 GitHub/TestPyPI/PyPI/Homebrew channels are published and proof-backed; future package channels, signing expansion, containers, feedstocks, and public Rust crates require separate approval and channel proof. |
| `diagnostic_row_id=performance_superiority_replacement_claim` | Public claims | Performance superiority, Spark displacement, engine replacement | `SL_UNSUPPORTED_PERFORMANCE_SUPERIORITY_CLAIM` | `cg5.cg6.claim_grade_correctness_and_benchmark_evidence_required` | Performance superiority and replacement claims require workload-scoped correctness and benchmark evidence with timing surface and evidence tier stated. |
| `diagnostic_row_id=production_readiness_claim` | Production readiness | `production_ready`, finished-product, public-release-ready claims | `SL_UNSUPPORTED_PRODUCTION_READINESS_CLAIM` | `release.production_readiness_gate_required` | Finished-product and production-readiness claims require runtime scope, package channels, security, docs, benchmark, schema, and approval gates. |

## Required Reporting Rule

Unsupported paths must emit deterministic unsupported or blocked diagnostics. They must preserve:

```text
fallback_attempted=false
external_engine_invoked=false
```

External engines may appear only as local benchmark baselines, optional migration references, or
oracles in explicitly labeled tests. They must not execute unsupported ShardLoom work as fallback.
