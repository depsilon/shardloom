# Scale Readiness Contract

Status: implemented report-only contracts for `GAR-SCALE-1A` and `GAR-SCALE-1B`.

ShardLoom must not claim literal "any volume" support. Scale readiness is a declared resource and
evidence contract, not a slogan. A row can become scale-grade only when it proves the appropriate
scale class with real workload bytes, correctness evidence, resource envelope evidence,
no-fallback evidence, and the relevant runtime-specific certificates.

## Scale Classes

The scale claim gate uses these classes:

| Scale class | Current status | Claim meaning |
| --- | --- | --- |
| `local_smoke` | current default | Local fixture or smoke evidence only. |
| `local_claim_grade` | local-only future/current gate | Local workload claim evidence only, not scale-grade. |
| `larger_than_memory_local` | blocked | Requires bounded-memory and spill/backpressure proof. |
| `split_parallel_local` | blocked | Requires split manifest and local split execution proof. |
| `object_store_read_report_only` | report-only | Object-store read posture only; no runtime read claim. |
| `object_store_runtime` | blocked | Requires credential/network/range/streaming read proof. |
| `table_metadata_report_only` | report-only | Table metadata posture only; no table runtime claim. |
| `table_runtime` | blocked | Requires table scan/runtime evidence. |
| `distributed_report_only` | report-only | Protocol vocabulary only; no worker runtime. |
| `distributed_runtime` | blocked | Requires coordinator/worker/retry/merge proof. |
| `foundry_dev_stack_proof` | report-only | Local/dev-stack proof boundary only. |
| `managed_platform_proof` | blocked | Requires real managed-platform evidence and claim approval. |

Current benchmark rows are restricted to `local_smoke` or `local_claim_grade` for ShardLoom, and
external baselines remain comparison context only.

## Benchmark Row Fields

`benchmarks/traditional_analytics/run.py` now emits
`scale_contract_schema_version=shardloom.traditional_analytics.scale_claim_gate.v1` and the
following fields on ShardLoom benchmark rows:

```text
scale_supported_classes
scale_profile
scale_claim_status
scale_claim_reason
data_volume_bytes
row_count_estimate
file_count
partition_count
split_count
memory_budget_bytes
peak_memory_bytes
spill_allowed
spill_bytes_written
spill_bytes_read
shuffle_required
shuffle_strategy
shuffle_bytes_written
shuffle_bytes_read
skew_detected
retry_count
idempotency_key
output_commit_status
object_store_involved
table_format_involved
remote_workers_involved
foundry_runtime_invoked
foundry_spark_invoked
scale_fallback_attempted
scale_external_engine_invoked
scale_claim_gate_status
scale_claim_boundary
```

For `GAR-SCALE-1A`, current ShardLoom rows must keep:

```text
scale_profile=local_smoke|local_claim_grade
scale_claim_gate_status=not_scale_grade
memory_budget_bytes=null
spill_allowed=false
object_store_involved=false
table_format_involved=false
remote_workers_involved=false
foundry_runtime_invoked=false
foundry_spark_invoked=false
scale_fallback_attempted=false
scale_external_engine_invoked=false
```

## SplitManifest Contract

`GAR-SCALE-1B` adds
`split_manifest_contract_schema_version=shardloom.traditional_analytics.split_manifest.v1` and a
report-only SplitManifest/per-split evidence contract to benchmark rows. The contract sits between
SourceState and execution:

```text
SourceState -> SplitManifest -> VortexPreparedState -> ExecutionPlan
```

Current local rows may expose a single-local-source split summary, but this is split planning
evidence only. It is not split-parallel runtime, distributed execution, larger-than-memory runtime,
object-store execution, table execution, or performance evidence.

ShardLoom SplitManifest rows carry:

```text
split_manifest_contract_schema_version
split_manifest_status
split_manifest_id
split_manifest_digest
split_manifest_source_state_id
split_manifest_split_count
split_id
source_state_id
byte_range
row_range
estimated_rows
estimated_bytes
projection_mask
filter_pushdown_status
split_retry_count
split_runtime_millis
split_rows_scanned
split_rows_output
split_spill_bytes
split_output_ref
split_claim_status
split_fallback_attempted=false
split_external_engine_invoked=false
split_claim_gate_status=not_split_scale_grade
split_claim_boundary
```

For `GAR-SCALE-1B`, runtime split execution remains blocked. Rows must keep:

```text
split_claim_gate_status=not_split_scale_grade
split_retry_count=0
split_runtime_millis=null
split_spill_bytes=0
split_fallback_attempted=false
split_external_engine_invoked=false
```

Unsupported or unsplittable sources must emit deterministic `unsupported` or `blocked` posture
instead of delegating to Spark, DataFusion, DuckDB, Polars, Dask, Ray, object stores, table engines,
or managed platforms.

## Claim Gate

A row is not scale-grade when any required proof is missing:

- no declared memory budget,
- no larger-than-memory input proof,
- no split manifest proof,
- no spill/backpressure proof,
- no shuffle/repartition proof where required,
- no object-store/table runtime proof,
- no distributed worker proof,
- no Foundry or managed-platform runtime proof,
- no correctness digest over the claimed scale workload.

Synthetic metadata-only evidence can explain a plan or blocker, but it cannot become runtime scale
evidence.

## Non-Goals

This slice does not add:

- larger-than-memory runtime,
- split-parallel runtime,
- spill runtime,
- object-store or table runtime,
- distributed runtime,
- Foundry production support,
- benchmark volume changes,
- performance or superiority claims,
- Spark-replacement claims,
- package publication.

## No-Fallback Boundary

External engines may be baselines or correctness oracles only. They cannot satisfy ShardLoom scale
evidence and cannot execute unsupported ShardLoom work as fallback.

Every ShardLoom scale row must preserve:

```text
scale_fallback_attempted=false
scale_external_engine_invoked=false
fallback_attempted=false
external_engine_invoked=false
```

## Verification

Required checks:

```powershell
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python scripts/check_website_readiness.py
git diff --check
```
