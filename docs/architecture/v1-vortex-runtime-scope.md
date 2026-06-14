<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Vortex Runtime Scope

Status: canonical v1 Vortex runtime scope.

Schema marker: `shardloom.v1_vortex_runtime_scope.v1`.

This document defines the exact Vortex runtime surface admitted for ShardLoom v1. It is narrower
than broad Vortex support. A route is in v1 only when it starts from one of the supported starting
states, lowers through a ShardLoom-native provider or wrapper boundary, emits execution and Native
I/O evidence, and preserves:

```text
fallback_attempted=false
external_engine_invoked=false
```

## Source Of Truth

The machine-readable sources for this scope are:

- `ShardLoomContext.local_vortex_primitive_route_report()`
- `ShardLoomContext.user_route_capability_report()`
- `ShardLoomContext.local_file_benchmark_route_report()`
- `scripts/check_v1_vortex_runtime_scope.py`
- `scripts/check_user_route_capability_report.py`

Public docs, release gates, and benchmark website views may summarize this file, but they must not
turn it into a broad Vortex runtime, object-store, table/catalog, or performance claim.

## Supported Starting States

| Starting state | Supported v1 meaning | Runtime boundary |
| --- | --- | --- |
| `native_local_vortex_file` | A local `.vortex` file enters at `native_vortex_boundary` and uses the scoped local primitive runtime family. | `vortex-run`, `vortex-count-where`, `vortex-filter`, `vortex-project`, and `vortex-filter-project` with execution certificate, Native I/O certificate, and no-fallback evidence. |
| `prepared_local_vortex_state` | A previously created `VortexPreparedState` is reused for a warm prepared query. | `prepared_vortex` starts after preparation and reports preparation exclusion, prepared-state reuse, execution evidence, and result boundary evidence. |
| `prepared_compatibility_artifact` | A local compatibility source is normalized through `SourceState -> vortex_ingest -> VortexPreparedState` before query execution. | Prepared benchmark-family rows include preparation/reuse evidence, source and split refs, route timing fields, execution certificate, Native I/O certificate, and no-fallback evidence. |
| `generated_local_vortex_artifact` | Source-free/generated rows create a local Vortex-preparable artifact with explicit local output evidence. | Generated-source certificates and local output evidence prove the artifact boundary; this is not object-store or table/catalog support. |

## Supported V1 Local Vortex Primitive Operations

The scoped local primitive report admits these route ids:

| Route id | Operation | Source-order limit |
| --- | --- | --- |
| `vortex_count_all` | Count all rows from one local `.vortex` source. | No |
| `vortex_count_where` | Count rows matching a tiny supported predicate. | No |
| `vortex_filter_collect` | Filter one local `.vortex` source and return a bounded report/collect boundary. | No |
| `vortex_filter_limit_collect` | Filter with source-order limit. | Yes |
| `vortex_project_collect` | Project supported columns. | No |
| `vortex_project_limit_collect` | Project supported columns with source-order limit. | Yes |
| `vortex_select_star_limit_collect` | Select all columns with source-order limit. | Yes |
| `vortex_filter_project_collect` | Filter and project supported columns. | No |
| `vortex_filter_project_limit_collect` | Filter and project supported columns with source-order limit. | Yes |

Each route must expose SQL, Python, DataFrame-style, context, session, and CLI surfaces. Each route
must name output route, evidence route, materialization/decode boundary, required evidence,
`claim_gate_status=not_claim_grade`, and the scoped claim boundary.

## Supported Prepared Vortex Benchmark Families

The v1 Vortex scope includes the current local benchmark-family prepared/native rows, not as a
performance superiority claim but as route-support evidence:

```text
selective_filter
filter_projection_limit
group_by_aggregation
multi_key_group_by
join_aggregate
sort_top_k
row_number_window
top_n_per_group
clean_cast_filter_write
partition_pruning
many_small_files_scan
null_heavy_aggregate
high_cardinality_string_group_distinct
nested_json_field_scan
small_change_over_large_base
```

Prepared rows must route through `vortex_ingest` or an existing `VortexPreparedState` before
`prepared_vortex` execution. Native local `.vortex` rows start at the Vortex boundary. Result sink
and publication-proof work must remain separated from hot runtime timing by `timing_surface` and
evidence-tier fields.

## Feature Profile Decision

The v1 package/build decision is:

```text
feature_gated_local_vortex_runtime
```

Upstream Vortex stays outside the default lightweight build. v1 admits feature-gated local primitive
routes, prepared-state routes, compatibility-import Vortex artifact creation, and generated local
Vortex artifacts only when CI feature checks and route evidence prove the boundary. Package and
install docs must explain that broad Vortex functionality is not enabled or claimed by default.

## Vortex-First Provider Check

Vortex-first provider check:

- Subject area: v1 local native/prepared Vortex runtime scope.
- Upstream Vortex concept checked: Vortex file, scan/read APIs, arrays, layouts, statistics,
  Arrow interop for compatibility import, and sink/output concepts.
- Decision:
  - `use_vortex_native_provider` for approved feature-gated local `.vortex` primitive routes and
    prepared/native benchmark rows that already carry execution and Native I/O evidence.
  - `wrap_vortex_concept` for report-only scope, capability, provider, and route normalization
    surfaces.
  - `blocked_until_vortex_or_shardloom_evidence` for object-store Vortex I/O, table/catalog Vortex
    I/O, generalized Source/Sink runtime, broad Vortex SQL/DataFrame parity,
    nested/complex dtype general Vortex behavior, vector/device/GPU Vortex runtime, and other
    unproved shapes.
- Vortex API/provider surface: upstream Vortex `0.75` behind `shardloom-vortex` feature gates such
  as `vortex-local-primitives`, `vortex-file-io`, and
  `vortex-traditional-analytics-benchmark`.
- ShardLoom provider/report/certificate surface: route capability reports, local Vortex primitive
  report, prepared benchmark route rows, execution certificates, Native I/O certificates, and
  materialization/decode boundary fields.
- Residual handling: supported residuals are ShardLoom-native or not required; unsupported
  residuals are blocked with deterministic diagnostics.
- Materialization/decode boundary: primitive report, bounded preview/collect, explicit local sink,
  or publication-proof evidence boundary only.
- Evidence added: `scripts/check_v1_vortex_runtime_scope.py` validates route ids, starting states,
  feature profile, unsupported boundaries, no-fallback fields, and docs linkage.
- Gates still blocked: broad Vortex runtime support remains unclaimed until each unsupported
  boundary closes with correctness, execution, Native I/O, decode/materialization, and release
  evidence.
- `fallback_attempted=false`: required for every admitted row.
- `external_engine_invoked=false`: required for every admitted row.

## Unsupported V1 Vortex Boundaries

These boundary ids remain outside v1 support unless a later phase-plan item closes them with real
runtime evidence and deterministic blockers:

| Boundary id | Current v1 posture |
| --- | --- |
| `object_store_vortex_io` | Unsupported for v1 Vortex runtime. Local object-store fixtures do not authorize object-store Vortex scan/write support. |
| `table_catalog_vortex_io` | Unsupported for v1 Vortex runtime. Table/catalog metadata rows do not authorize table execution. |
| `generalized_source_sink_api` | Unsupported outside the admitted local primitive/prepared/generated artifact routes. |
| `broad_vortex_sql_dataframe_parity` | Unsupported outside the scoped SQL/Python/DataFrame shapes listed by route reports. |
| `nested_complex_dtype_general_vortex` | Unsupported as a broad Vortex claim. Individual benchmark rows may cover scoped nested/dirty data workflows only when they emit route evidence. |
| `vector_device_gpu_vortex_runtime` | Unsupported; extension dtype discovery or device awareness is not vector search, GPU execution, or device-resident output support. |

Unsupported shapes must fail before hidden data reads, row materialization, writes, or external
execution. They must preserve deterministic diagnostics and:

```text
runtime_execution=false
data_read=false
write_io=false
fallback_attempted=false
external_engine_invoked=false
```

## Claim Boundary

After this scope is closed, ShardLoom may claim scoped v1 local/prepared Vortex route support for
the starting states and operations above. It still may not claim:

- universal Vortex input/output support;
- object-store or table/catalog Vortex runtime;
- broad Vortex SQL/DataFrame parity;
- broad nested/complex dtype Vortex execution;
- vector, device, or GPU Vortex runtime;
- package publication or production readiness; or
- performance superiority, Spark displacement, or external engine replacement.
