<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Local Output And Sink Scope

Status: canonical declarative v1 local output and sink scope.

Schema marker: `shardloom.v1_local_output_sink_scope.v1`.

This document records the output contract declared by `ShardLoomContext.local_output_sink_scope_report()`.
The scope checker verifies declarations, fixture agreement, public-document links, and no-fallback
policy. Its report has `evidence_class=declarative_contract` and
`runtime_evidence_verified=false`. It does not prove that a writer ran, that output values were
read back correctly, or that package, performance, or production gates passed. Actual complete-value
readback belongs to live tests or a harness; the golden field fixture is only a declaration.

Every admitted declaration preserves:

```text
claim_gate_status=not_claim_grade
fallback_attempted=false
external_engine_invoked=false
```

## Source Of Truth

The machine-readable sources are:

- `ShardLoomContext.local_output_sink_scope_report()`
- `ShardLoomContext.user_route_capability_report()`
- `scripts/check_v1_local_output_sink_scope.py`
- `docs/architecture/fixtures/v1-local-output-sink/output-scope-golden.json`
- `docs/architecture/fixtures/v1-local-output-sink/output-policy-matrix.json`
- `docs/architecture/fixtures/v1-local-output-sink/output-evidence-fields-golden.json`

The checker contains no benchmark artifact reader or runtime replay result parser. A passed
`declarative_contract_ready` result means the declaration, fixtures, and checked documentation
agree; it is not runtime evidence.

## Shared Route And Output Formats

The sole output route id is `native_vortex_query`. Its owner is `shared_native_workflow` and its
execution mode is `native_vortex`. SQL, Python, DataFrame, context, session, and CLI are front doors
to this shared engine; input adapters normalize data into the Vortex-native middle, and output
adapters translate a requested result at the sink boundary. They do not select separate execution
engines. The public route contract is the shared Vortex-native engine, with no external-engine
fallback.

The declared local formats are `json`, `jsonl`, `csv`, `parquet`, `arrow-ipc`, `avro`, `orc`, and
`vortex`. JSON, JSONL, and CSV need no extra format adapter beyond the enabled native engine; this
does not make them part of the lean default execution build. Parquet, Arrow IPC, Avro, ORC, and Vortex
remain feature-gated. Vortex is the highest-fidelity persistence target. Compatibility formats are
translation outputs and must report relevant metadata loss; they are not execution fallbacks.

The ten declared write methods are `write`, `write_json`, `write_jsonl`, `write_csv`,
`write_parquet`, `write_arrow_ipc`, `write_avro`, `write_orc`, `write_vortex`, and `fanout`.
Registration in this scope is not evidence that every type, shape, or workflow is supported by every
format. Unsupported shapes must fail explicitly before writing.

## Write And Fanout Contract

The policy ids are:

| Policy id | Declared behavior |
| --- | --- |
| `error_if_exists_by_default` | Existing local targets are rejected by default. |
| `explicit_allow_overwrite` | An overwrite request is an explicit policy input; shared native computed writes still deny an existing target, even when `allow_overwrite` is requested. |
| `append_mode_unsupported` | Append is outside this v1 scope and fails deterministically. |
| `atomic_rename_same_directory` | The policy label records same-directory commit behavior where the admitted writer supports it. |
| `partial_write_cleanup_reported` | Cleanup status is exposed instead of hidden. |

Fanout replays the shared query once per requested adapter; there is no answer cache. It
stages all output files before publication, then publishes targets sequentially with create-if-absent
semantics. Local paths cannot be committed as one atomic group: if a later publication fails,
already-published complete targets remain, and the failure reports which targets were published.
Every existing target is denied, including when overwrite was requested. The writer cleans up
unpublished staged files according to its reported cleanup contract.

These declarations do not prove atomic durability, group transactions, or replay success. Runtime
certificates and complete-value readback are checked by the live tests or harness, not inferred from
the scope report.

## Declared Runtime Evidence Fields

Successful native writes declare the following fields for runtime reports:

```text
native_vortex_result_export_format
native_vortex_result_export_path
native_vortex_result_export_rows_written
native_vortex_result_export_projected_columns
native_vortex_result_export_target_count
native_vortex_result_export_all_targets_committed
native_vortex_result_export_fanout_atomicity_contract
native_vortex_result_export_partial_write_cleanup_status
native_vortex_result_export_target_fidelity_statuses
typed_sink_contract
decode_materialization_boundary
local_primitive_native_io_certificate_emitted
public_workflow_fallback_attempted
public_workflow_external_engine_invoked
```

The [evidence-fields fixture](fixtures/v1-local-output-sink/output-evidence-fields-golden.json)
lists these names only. It records no replay, output digest, runtime certificate, or successful
readback result.

## Vortex-first provider check

- Subject: local output and sink boundary.
- Vortex concepts checked: native sinks and local writer/reopen APIs, Arrow conversion, file layout,
  DType preservation, and compatibility-writer boundaries.
- Decision: `use_vortex_native_provider` for admitted feature-gated Vortex sinks; `wrap_vortex_concept`
  for ShardLoom's output plan, sink contract, Native I/O certificate, fidelity/loss report, and local
  write policy.
- Boundaries: `blocked_until_vortex_or_shardloom_evidence` for append, object-store paths,
  table/catalog writes, Iceberg/Delta transactions, remote URI sinks, and broad nested/complex sink
  shapes.
- Materialization is explicit at the requested bounded result or sink boundary.
- The scope checker validates declarations and fixtures only; live tests or the harness provide
  runtime value and writer evidence.

## Unsupported V1 Boundaries

| Boundary id | Current posture |
| --- | --- |
| `append_mode` | Unsupported; fail with a deterministic diagnostic. |
| `object_store_output_paths` | Unsupported for user sinks in this scope. |
| `table_catalog_writes` | Unsupported; local files do not authorize table writes. |
| `iceberg_delta_transactions` | Unsupported; compatibility files are not table transactions. |
| `remote_uri_sinks` | Unsupported; local output does not write remote URIs. |
| `broad_nested_complex_sink_shapes` | Unsupported unless a narrower typed route and live test establish it. |

Unsupported behavior must fail explicitly and preserve `fallback_attempted=false` and
`external_engine_invoked=false`.

## Claim Boundary

A passing scope report establishes only consistency of the declared formats, methods, route,
policies, required runtime field names, fixtures, documentation links, and no-fallback posture. It
does not establish runtime output correctness, complete-value readback, package release readiness,
production certification, performance superiority, Spark displacement, or external-engine
replacement. Object-store output, table/catalog writes, lakehouse transactions, append, and broad
nested/complex sink support remain outside this scope.
