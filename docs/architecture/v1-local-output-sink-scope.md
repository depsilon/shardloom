<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Local Output And Sink Scope

Status: canonical v1 local output and sink scope.

Schema marker: `shardloom.v1_local_output_sink_scope.v1`.

This document defines the local output and sink surface admitted for ShardLoom v1. It is a scoped
local artifact contract, not broad object-store output, table/catalog writes, lakehouse
transactions, production-readiness, or performance-superiority evidence.

Every admitted row in this scope must preserve:

```text
claim_gate_status=not_claim_grade
fallback_attempted=false
external_engine_invoked=false
```

## Source Of Truth

The machine-readable sources for this scope are:

- `ShardLoomContext.local_output_sink_scope_report()`
- `ShardLoomContext.user_route_capability_report()`
- `scripts/check_v1_local_output_sink_scope.py`
- `docs/architecture/fixtures/v1-local-output-sink/output-scope-golden.json`
- `docs/architecture/fixtures/v1-local-output-sink/output-policy-matrix.json`
- `docs/architecture/fixtures/v1-local-output-sink/output-replay-manifest-golden.json`

Public docs, benchmark pages, and release summaries may point here, but they must not translate
this local v1 scope into broad output compatibility, object-store/table support, package release,
or production claims.

## Supported Local Output Formats

The v1 local output formats in scope are:

```text
jsonl
csv
parquet
arrow-ipc
avro
orc
vortex
```

`vortex` is the admitted native local sink behind `vortex-write` and remains the
highest-fidelity ShardLoom persistence target. Generated-source and internal smoke routes can
exercise `jsonl`, `csv`, and feature-gated flat scalar compatibility exports for evidence, but
public local-source runtime routes must not execute direct decoded compatibility sinks as their
runtime middle. Exact provider-backed Vortex result summaries may now export bounded `result_json`
to workspace-safe `jsonl` or `csv` sinks after native Vortex execution. Scoped primitive
filter/project/filter-project row streams may export workspace-safe `jsonl` or `csv`, including
JSONL+CSV fanout, through `native_vortex_primitive_row_export` after native/prepared Vortex input
with explicit selected-column decode/materialization evidence. Broad local-source row streams,
unsupported formats, unsafe/duplicate fanout targets, and arbitrary local-source workflows remain
blocked when the workflow does not have a Vortex-derived typed export contract.

## User-Facing Write Methods

The v1 local output write helpers in scope are:

```text
write
write_jsonl
write_csv
write_parquet
write_arrow_ipc
write_avro
write_orc
write_vortex
fanout
```

These helpers write only through admitted local routes. They do not authorize arbitrary SQL,
broad DataFrame execution, object-store paths, table/catalog writes, remote result delivery, or
fallback execution. For public local-source workflows in the current v1 surface, native
`write_vortex` is the highest-fidelity sink where the upstream operator route or scoped structured
projection route is admitted. Exact provider-backed result summaries also admit `write_jsonl` and
`write_csv` as bounded result exports with explicit decode/materialization evidence. Scoped
primitive row streams admit `write_jsonl`, `write_csv`, and JSONL+CSV `fanout` through
`native_vortex_primitive_row_export`. Scoped structured expression-project row streams additionally
admit Vortex, Parquet, Arrow IPC, and Avro output through the same native Vortex-derived route.
Compatibility-output helpers remain deterministic blockers for arbitrary local-source workflows,
unsupported formats, ORC nested output, and non-admitted fanout targets until those paths have their
own Vortex-derived typed export contracts.

## Output Routes

The route ids covered by this scope are:

| Route id | Output posture |
| --- | --- |
| `local_file_internal_source_smoke_route` | Internal smoke-only local JSONL/CSV and feature-gated structured/Vortex sink safeguard; not a public runtime middle. |
| `local_file_cold_certified_route` | Local result sink and evidence output for cold certified local-file route rows. |
| `local_file_prepare_once_first_query` | Prepared query result, bounded report, or local result sink. |
| `local_file_prepare_once_batch` | Batch prepared query result, bounded report, or local result sink. |
| `prepared_vortex_warm_query` | Prepared Vortex query result, bounded report, or local result sink. |
| `native_vortex_query` | Native local Vortex result/report route with scoped result sink evidence. |
| `native_vortex_primitive_row_export` | Native/prepared Vortex primitive filter/project/filter-project row stream to JSONL/CSV, including JSONL+CSV fanout, plus scoped structured expression-project export to Vortex/Parquet/Arrow IPC/Avro with explicit decode/materialization boundary. |
| `generated_rows_local_output` | Local JSONL/CSV, feature-gated structured/Vortex output, artifact-adjacent prepared-state reuse manifest, and fanout. |
| `quarantine_output_route` | Local quarantine sink for admitted schema/data-quality rows. |

## Write Policies

The v1 write-policy vocabulary is:

| Policy id | Meaning |
| --- | --- |
| `error_if_exists_by_default` | The default user-facing policy rejects existing local output paths. |
| `explicit_allow_overwrite` | Callers must pass `allow_overwrite=True` or `--allow-overwrite` to replace local output paths. |
| `append_mode_unsupported` | Append is not part of v1 local output support and must fail deterministically. |
| `atomic_rename_same_directory` | Admitted local sinks use same-directory atomic commit posture where the runtime writer supports it. |
| `partial_write_cleanup_reported` | Partial-write cleanup or non-required cleanup posture is reported through output fields instead of hidden. |

Unsupported write modes must fail before hidden reads, writes, external engine execution, or
best-effort append emulation.

## Required Runtime Evidence Fields

Benchmark and runtime rows that include a result sink must expose these fields:

```text
output_route
output_native_io_certificate_status
computed_result_sink_native_io_certificate_status
computed_result_sink_replay_verified
output_materialization_required
output_plan_digest
result_sink_write_millis
sink_timing_included_in_route_total
timing_surface
fallback_attempted
external_engine_invoked
```

Rows without a requested sink may report not-applicable values, but they must not substitute for
sink-support evidence.

## Vortex-First Provider Check

Vortex-first provider check:

- Subject area: v1 local output and sink runtime scope.
- Upstream Vortex concept checked: Vortex sink/output concepts, Vortex local writer/reopen surfaces,
  Vortex Arrow conversion, Vortex file layout, array DType preservation, result-sink replay
  evidence, Parquet-to-Vortex conversion posture, and feature-gated compatibility writer boundaries.
- Decision:
  - `use_vortex_native_provider` for admitted feature-gated local Vortex sinks that call upstream
    Vortex writer/reopen provider surfaces and report no-fallback evidence.
  - `wrap_vortex_concept` for OutputPlan, SinkArtifact, output Native I/O certificate,
    metadata-preservation/loss report, digest, replay, fanout, and local write-policy evidence.
  - `blocked_until_vortex_or_shardloom_evidence` for append mode, object-store output paths,
    table/catalog writes, Iceberg/Delta transactions, remote URI sinks, and broad nested/complex
    sink shapes.
- Vortex API/provider surface: upstream Vortex provider version derived from root `Cargo.toml`
  `[workspace.dependencies].vortex` behind `shardloom-vortex` feature gates such as
  `vortex-write`, `vortex-file-io`, and `vortex-traditional-analytics-benchmark`.
- ShardLoom provider/report/certificate surface: Python write helpers, public workflow route/run
  facade, local generated-source output smokes, SQL local-source sinks, prepared/native Vortex
  result sinks, output Native I/O certificates, output fidelity reports, result-sink replay fields,
  and benchmark output-plan fields.
- Residual handling: supported residuals are ShardLoom-native or not required; unsupported residuals
  are blocked with deterministic diagnostics.
- Materialization/decode boundary: explicit local sink boundary, bounded report/collect boundary, or
  publication-proof result-sink replay boundary only.
- Evidence added: `scripts/check_v1_local_output_sink_scope.py` validates method rows, route rows,
  fixture refs, benchmark result-sink fields, docs linkage, and no-fallback fields.
- Gates still blocked: append mode, object-store output paths, table/catalog writes, Iceberg/Delta
  transactions, remote URI sinks, broad nested/complex sink shapes, production readiness, and
  performance claims.
- `fallback_attempted=false`: required for every admitted row.
- `external_engine_invoked=false`: required for every admitted row.

## Unsupported V1 Boundaries

These boundary ids remain outside v1 support unless a later phase-plan item closes them with real
runtime evidence, deterministic diagnostics, and no-fallback proof:

| Boundary id | Current v1 posture |
| --- | --- |
| `append_mode` | Unsupported. v1 writes are create/error-if-exists or explicit overwrite only. |
| `object_store_output_paths` | Unsupported for this scope. Local-emulator proofs do not authorize object-store user sinks. |
| `table_catalog_writes` | Unsupported. Table/catalog metadata rows do not authorize table writes. |
| `iceberg_delta_transactions` | Unsupported. Compatibility files are not table transactions. |
| `remote_uri_sinks` | Unsupported. v1 local output scope does not write remote URIs. |
| `broad_nested_complex_sink_shapes` | Unsupported unless a narrower format-specific route and test proves the shape. |

Unsupported shapes must fail before hidden data reads, writes, cache probes, or external execution.
They must report deterministic diagnostics and preserve:

```text
runtime_execution=false
data_read=false
write_io=false
fallback_attempted=false
external_engine_invoked=false
```

## Claim Boundary

After this scope is closed, ShardLoom may claim scoped local output/write helper support for the
formats, methods, route ids, and policies above. It still may not claim:

- object-store output or remote URI sink support;
- table/catalog writes or lakehouse transactions;
- append mode;
- broad nested/complex sink support;
- production adapter certification;
- package publication or production readiness; or
- performance superiority, Spark displacement, or external engine replacement.
