<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Source Prepared-State Scope

Status: canonical v1 SourceState and VortexPreparedState reuse scope.

Schema marker: `shardloom.v1_source_prepared_state_scope.v1`.

This document defines the local compatibility-source normalization and prepared-state reuse surface
admitted for ShardLoom v1. It is a scoped local route contract, not a broad input-adapter,
object-store, table/catalog, persistent-cache, production-readiness, or performance-superiority
claim.

Every admitted row in this scope must preserve:

```text
claim_gate_status=not_claim_grade
fallback_attempted=false
external_engine_invoked=false
```

## Source Of Truth

The machine-readable sources for this scope are:

- `ShardLoomContext.source_prepared_state_scope_report()`
- `ShardLoomContext.user_route_capability_report()`
- `ShardLoomContext.local_file_benchmark_route_report()`
- `scripts/check_v1_source_prepared_state_scope.py`
- `docs/architecture/fixtures/v1-source-prepared-state/source-state-golden.json`
- `docs/architecture/fixtures/v1-source-prepared-state/vortex-prepared-state-golden.json`
- `docs/architecture/fixtures/v1-source-prepared-state/reuse-invalidation-matrix.json`

Public docs, benchmark pages, and release summaries may point here, but they must not translate
this local v1 scope into a broad production adapter or cache claim.

## Canonical Routes

The canonical non-Vortex local compatibility route is:

```text
UniversalIngress -> SourceState -> vortex_ingest -> VortexPreparedState -> prepared_vortex
```

The internal local-source smoke compatibility route boundary is internal smoke-only:

```text
UniversalIngress -> SourceState -> internal_local_source_smoke
```

The internal local-source smoke path is not an admitted public workflow runtime route. Public local-file
`auto` workflows must prepare into Vortex or run from native Vortex input; explicit `direct` public
workflow requests fail closed. Internal local-source smoke rows remain only as lower-level smoke safeguards and
must report:

```text
prepared_state_reuse_scope=not_applicable_no_prepared_state
route_runtime_status=internal_smoke_only
```

## Supported Local Compatibility Formats

The v1 local compatibility formats in this scope are:

```text
csv
jsonl
parquet
arrow-ipc
avro
orc
```

Structured formats remain feature-gated where the current build requires it. The scope admits
format normalization into SourceState/prepared-state evidence for the existing local benchmark and
user route families only.

## Prepared Route Families

The v1 route ids that require or consume `VortexPreparedState` are:

| Route id | Route meaning | Required reuse scope |
| --- | --- | --- |
| `local_file_cold_certified_route` | Cold certified local file route, including preparation and first execution evidence. | `workspace_manifest_local_vortex_artifacts` |
| `local_file_prepare_once_first_query` | Prepare local compatibility input once, then run the first query. | `workspace_manifest_local_vortex_artifacts` |
| `local_file_prepare_once_batch` | Prepare local compatibility input once, then reuse it across a batch. | `workspace_manifest_local_vortex_artifacts` |
| `prepared_vortex_warm_query` | Start from an explicit prepared local Vortex state. | `explicit_prepared_state_input` |

Prepared local Vortex artifacts may contain ShardLoom-internal derived columns such as compact
`UInt32` UTF-8 byte length for high-value URL/search/title text fields and dictionary-encoded
URL/Referer/URI domain values. These are part of the single `.vortex` artifact and are not adjacent
manifests, query-answer caches, or public output columns. Native planning may consume them for
admitted string predicates and aggregate expressions when present. Product columnar source adapters
must not synthesize those physical columns through a slower per-row preprocessing pass; they should
report the derived-column posture in source evidence and only embed them when a source-native or
dictionary-aware generator is admitted.

Large local compatibility-source preparation remains a single-artifact route. Source evidence must
record the active capillary stream policy, including adaptive large-source batch sizing such as
`product_columnar_stream_batch_size_262144_rows`, source-unit hints such as Parquet row-group count,
and the writer/layout profile. Current local 100M UAT retains source-text dictionary-Zstd writer
coverage for the official ClickBench text-shaped columns and leaves writer profile timing as a
tuning item; it is not a performance-superiority or load-speed claim.

The source-free generated route id in this scope is:

| Route id | Route meaning | Required reuse scope |
| --- | --- | --- |
| `generated_rows_local_output` | Generate local rows and write a local Vortex-preparable artifact. | `single_vortex_artifact_no_sidecar` |

The internal local-source smoke route id in this scope is:

| Route id | Route meaning | Required reuse scope |
| --- | --- | --- |
| `local_file_internal_source_smoke_route` | Run a scoped local compatibility route without persistent `VortexPreparedState`. | `not_applicable_no_prepared_state` |

## Required Runtime Evidence Fields

Prepared benchmark rows must expose all of these fields:

```text
source_state_id
source_state_digest
source_state_fingerprint
source_schema_fingerprint
source_parse_plan_id
source_split_manifest_id
prepared_state_id
prepared_state_digest
prepared_state_reuse_hit
prepared_state_reuse_reason
prepared_state_reuse_manifest_digest
prepared_state_invalidation_reason
fallback_attempted
external_engine_invoked
```

The stage and timing fields may differ by route lane and timing surface. The required fields above
are identity, reuse, invalidation, and no-fallback evidence, not a performance claim.

## Reuse And Invalidation Matrix

The v1 prepared-state reuse contract must cover these cases:

| Case id | Expected posture |
| --- | --- |
| `cold_prepare_no_manifest` | Misses reuse and prepares because no workspace manifest exists. |
| `warm_reuse_manifest_match` | Reuses when source, schema, policy, manifest, and artifacts match. |
| `source_changed` | Invalidates when a source fingerprint changes. |
| `artifact_changed` | Invalidates when a prepared artifact fingerprint changes. |
| `schema_changed` | Invalidates when the source-admission packet or schema evidence changes. |
| `policy_changed` | Invalidates when prepare policy changes. |
| `version_changed` | Invalidates when reuse manifest schema version changes. |
| `missing_artifact` | Invalidates when a required prepared artifact manifest/path is missing. |
| `corrupted_manifest` | Invalidates when manifest JSON cannot be parsed. |

The machine-readable matrix lives at
`docs/architecture/fixtures/v1-source-prepared-state/reuse-invalidation-matrix.json`.

## Vortex-First Provider Check

Vortex-first provider check:

- Subject area: v1 local compatibility SourceState and VortexPreparedState reuse scope.
- Upstream Vortex concept checked: Vortex file, arrays, local writer/reopen surfaces, Arrow
  RecordBatch interop for admitted structured sources, source/split concepts, and sink/output
  concepts.
- Decision:
  - `use_vortex_native_provider` for the existing feature-gated local `vortex_ingest` preparation
    path and admitted Vortex array/write/reopen provider surfaces.
  - `wrap_vortex_concept` for SourceState, VortexPreparedState, reuse manifest, invalidation, and
    route-scope evidence reports.
  - `blocked_until_vortex_or_shardloom_evidence` for global hidden cache, external cache service,
    object-store prepared-state reuse, table/catalog prepared-state reuse, and broad non-local
    preparation.
- Vortex API/provider surface: upstream Vortex provider version derived from root `Cargo.toml`
  `[workspace.dependencies].vortex` behind `shardloom-vortex` feature gates such as
  `vortex-write`, `vortex-file-io`, `vortex-traditional-analytics-benchmark`, and
  `universal-format-io` where relevant.
- ShardLoom provider/report/certificate surface: route capability reports, local-file benchmark
  route rows, SourceState id/digest fields, VortexPreparedState id/digest fields, workspace reuse
  evidence where still applicable, single-artifact Vortex output evidence, execution certificates,
  Native I/O certificates, and materialization/decode boundary fields.
- Residual handling: supported residuals are ShardLoom-native or not required; unsupported
  residuals are blocked with deterministic diagnostics.
- Materialization/decode boundary: scoped local preparation, internal local-source smoke scalar runtime, or
  bounded result/publication evidence boundary only.
- Evidence added: `scripts/check_v1_source_prepared_state_scope.py` validates route ids, fixture
  refs, invalidation cases, benchmark artifact required fields, docs linkage, and no-fallback
  fields.
- Gates still blocked: global hidden cache, external cache service, object-store prepared-state
  reuse, table/catalog prepared-state reuse, broad non-local preparation, production adapter
  certification, and performance claims.
- `fallback_attempted=false`: required for every admitted row.
- `external_engine_invoked=false`: required for every admitted row.

## Unsupported V1 Boundaries

These boundary ids remain outside v1 support unless a later phase-plan item closes them with real
runtime evidence, deterministic diagnostics, and no-fallback proof:

| Boundary id | Current v1 posture |
| --- | --- |
| `global_hidden_cache` | Unsupported. Prepared-state reuse must be explicit, scoped, and evidence-backed. |
| `external_cache_service` | Unsupported. No Redis, database, service, or remote cache participates in v1 reuse. |
| `object_store_prepared_state_reuse` | Unsupported. Local object-store fixtures do not authorize object-store prepared-state reuse. |
| `table_catalog_prepared_state_reuse` | Unsupported. Table/catalog metadata rows do not authorize table execution or table-prepared reuse. |
| `broad_non_local_preparation` | Unsupported. v1 admits scoped local routes only. |

Unsupported shapes must fail before hidden reads, writes, cache probes, or external execution. They
must report deterministic diagnostics and preserve:

```text
runtime_execution=false
data_read=false
write_io=false
fallback_attempted=false
external_engine_invoked=false
```

## Claim Boundary

After this scope is closed, ShardLoom may claim scoped local SourceState normalization and
prepared-state reuse/invalidation behavior for the route families listed above. It still may not
claim:

- broad compatibility input support;
- object-store or table/catalog prepared-state reuse;
- a global cache or external cache service;
- broad non-local preparation;
- production adapter certification;
- package publication or production readiness; or
- performance superiority, Spark displacement, or external engine replacement.
