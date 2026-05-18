# Scale Readiness Contract

Status: implemented report-only contracts for `GAR-SCALE-1A` through `GAR-SCALE-1D`.

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

## Memory, Spill, And Backpressure Contract

`GAR-SCALE-1C` adds
`memory_spill_contract_schema_version=shardloom.traditional_analytics.memory_spill_backpressure.v1`
and a fail-closed memory, spill, and backpressure evidence contract to benchmark rows.

Current local rows expose the vocabulary and deterministic blockers required for future
larger-than-memory execution, but they do not declare a scale memory budget, admit runtime spill,
prove backpressure, or permit hidden full materialization.

ShardLoom memory/spill rows carry:

```text
memory_spill_contract_schema_version
memory_spill_status_vocabulary
memory_spill_claim_status_vocabulary
memory_spill_status
memory_spill_id
memory_spill_digest
memory_budget_bytes
operator_memory_budget_bytes
peak_memory_bytes
memory_budget_exceeded
spill_allowed
spill_location
spill_bytes_written
spill_bytes_read
spill_file_count
spill_cleanup_status
backpressure_status
oom_prevention_status
memory_spill_claim_status
memory_spill_fallback_attempted=false
memory_spill_external_engine_invoked=false
memory_spill_claim_gate_status
memory_spill_claim_boundary
```

For `GAR-SCALE-1C`, current rows must keep:

```text
memory_budget_bytes=null
operator_memory_budget_bytes=null
memory_budget_exceeded=false
spill_allowed=false
spill_location=not_admitted
spill_bytes_written=0
spill_bytes_read=0
spill_file_count=0
spill_cleanup_status=not_needed_no_spill_runtime
backpressure_status=not_admitted_report_only
oom_prevention_status=not_larger_than_memory_proof
memory_spill_claim_gate_status=not_larger_than_memory_grade
memory_spill_fallback_attempted=false
memory_spill_external_engine_invoked=false
```

Any future larger-than-memory claim requires a declared resource envelope, operator memory budgets,
deterministic block-or-spill admission, spill read/write/cleanup evidence when spill is allowed,
backpressure evidence when work is throttled or chunked, and correctness evidence over the claimed
workload bytes.

## Shuffle, Repartition, And Join Scale Contract

`GAR-SCALE-1D` adds
`shuffle_contract_schema_version=shardloom.traditional_analytics.shuffle_repartition.v1` and a
report-only `ShufflePlan`/`ShuffleEvidence` contract to benchmark rows.

The contract covers scale-sensitive families such as group-by, join, window, top-N per group,
repartition write, and CDC overlay. Current rows classify local operator posture and deterministic
blockers only. They do not prove distributed shuffle, Spark-scale joins, skew handling, retryable
shuffle, partitioned writes, or performance.

ShardLoom shuffle rows carry:

```text
shuffle_contract_schema_version
shuffle_evidence_status_vocabulary
shuffle_claim_status_vocabulary
shuffle_evidence_status
shuffle_plan_id
shuffle_plan_digest
shuffle_required
shuffle_strategy
partitioning_strategy
shuffle_partition_count
target_shuffle_partition_bytes
local_combine_used
global_merge_used
broadcast_candidate
broadcast_admitted
skew_detected
skew_strategy
shuffle_spill_bytes
shuffle_retry_count
shuffle_correctness_digest
shuffle_claim_status
shuffle_fallback_attempted=false
shuffle_external_engine_invoked=false
shuffle_claim_gate_status
shuffle_claim_boundary
```

For `GAR-SCALE-1D`, current rows must keep:

```text
shuffle_claim_gate_status=not_shuffle_scale_grade
shuffle_partition_count=0
target_shuffle_partition_bytes=null
local_combine_used=false
global_merge_used=false
broadcast_admitted=false
skew_detected=false
skew_strategy=not_evaluated_report_only
shuffle_spill_bytes=0
shuffle_retry_count=0
shuffle_correctness_digest=not_emitted_no_scale_shuffle
shuffle_fallback_attempted=false
shuffle_external_engine_invoked=false
```

Any future shuffle/repartition claim requires partitioning strategy evidence, target partition
bytes, local-combine/global-merge evidence when used, broadcast admission proof, skew strategy
evidence, spill/retry evidence, correctness digests over the claimed workload, and remote-worker
evidence before any distributed shuffle claim.

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
