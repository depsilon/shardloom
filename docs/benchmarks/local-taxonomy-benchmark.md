<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local Taxonomy Benchmark

The local taxonomy benchmark harness lives at
`benchmarks/traditional_analytics/run.py`. It separates timing rows from
coverage rows and keeps external engines labeled as baselines, not fallback
execution.

## ShardLoom Smoke

```powershell
python examples\local-vortex-benchmark\run.py --repo-root .
```

Equivalent direct command:

```powershell
python benchmarks\traditional_analytics\run.py `
  --engines shardloom `
  --formats csv,parquet `
  --scenario "selective filter" `
  --dataset-profile tiny_smoke `
  --rows 256 `
  --iterations 3 `
  --shardloom-build-profile debug `
  --shardloom-result-sink `
  --skip-shardloom-native `
  --no-markdown `
  --data-dir target\shardloom-local-taxonomy-smoke-data `
  --output target\shardloom-local-taxonomy-smoke.json `
  --regenerate
```

When `--regenerate` is used without an explicit `--data-dir`, the harness now
derives an isolated generated-data directory from the output artifact path and
holds a regeneration lock. Supplying `--data-dir` in documented commands keeps
the artifact/data relationship obvious.

## Comparative Local Baselines

Optional local baselines may be added for comparison:

```powershell
python benchmarks\traditional_analytics\run.py `
  --engines shardloom,pandas,polars,duckdb,datafusion `
  --formats csv,parquet `
  --include-taxonomy-extra `
  --dataset-profile narrow_fact_dim `
  --rows 100000 `
  --iterations 3 `
  --shardloom-result-sink `
  --data-dir target\shardloom-local-taxonomy-comparative-data `
  --output target\shardloom-local-taxonomy-comparative.json `
  --regenerate
```

Only install baseline engines in benchmark environments. They are not ShardLoom
runtime dependencies and must not execute unsupported ShardLoom work as
fallback.

See `docs/benchmarks/baseline-comparison-boundary.md` for the release boundary
between runtime packages and optional benchmark comparison tooling.

## Claim-Readiness Rerun

The selected P7.4.4 closeout rerun is:

```powershell
python benchmarks\traditional_analytics\run.py `
  --claim-readiness-rerun `
  --dataset-profile narrow_fact_dim `
  --rows 100000 `
  --iterations 3 `
  --data-dir target\shardloom-claim-readiness-rerun-data `
  --output target\shardloom-claim-readiness-rerun.json `
  --regenerate
```

The preset uses ShardLoom, the ShardLoom Vortex fixture lane, and selected
local optional baselines. It keeps managed platforms out, enables ShardLoom
result-sink proof, includes taxonomy extras when no explicit scenario is
provided, and rejects fewer than three iterations.

## Prepared/Native Evidence Snapshot

`GAR-PERF-1A` refreshed the committed website benchmark snapshot after the
prepared/native batch runner and source-state reuse work. The snapshot keeps
`compatibility_import_certified`, `prepared_vortex`, `native_vortex`, and
batch-runner rows separated. `compatibility_import_certified` rows are the certified cold route and
include UniversalIngress/source-adapter, `vortex_ingest`, write/reopen, scan, sink, and evidence
costs; they must not be presented as pure query speed. `prepared_vortex` rows are the prepared warm
route and start from `VortexPreparedState`; they must not be read as direct CSV/Parquet/JSONL input
timing.

The current website artifact preserves `source_metadata_snapshot_*`,
`source_state_*`, `session_*`, `execution_mode`, `claim_gate_status`,
materialization/decode, Native I/O, `fallback_attempted=false`, and
`external_engine_invoked=false` evidence. The expanded direct batch smoke rows
also expose multi-family source-state reuse, including selective-filter reuse
and the date/null metric reuse signal for partition-pruning plus null-heavy
aggregate scenarios. The benchmark remains local pre-release evidence, not a
leaderboard, performance claim, superiority claim, production claim, or Spark
replacement claim.

## Route-First Presentation Contract

Benchmark pages and release packets should lead with route-level rows, then show stage
attribution. Public lane names are:

| Lane | Starts from | Includes preparation? | Use |
| --- | --- | --- | --- |
| ShardLoom Cold Certified Route | Raw compatibility source | Yes, plus certified evidence | Cold ingest/stage/query/output proof. |
| ShardLoom Prepare-Once First Query | Raw compatibility source | Yes, once | Primary raw-source-to-prepared-Vortex user route. |
| ShardLoom Prepare-Once Batch | Raw compatibility source | Once per batch | Reuse/amortization evidence for multiple prepared queries. |
| ShardLoom Warm Prepared Query | `VortexPreparedState` | No | Warm query/runtime evidence after preparation exists. |
| ShardLoom Native Vortex Query | Existing Vortex artifact | No | Native input and operator maturity evidence. |
| ShardLoom Direct Transient Route | Raw compatibility source | No persistent Vortex preparation | Scoped one-shot local compatibility work. |
| External Baseline End-to-End | External engine raw-source path | Not applicable | Baseline context only, never fallback evidence. |

Stage pieces such as source admission, source read, parse/decode, Vortex array build, Vortex write,
reopen/verify, scan, operator compute, sink write, and evidence render explain route timing. They
must not be rendered as competing products or as end-to-end rows unless the route timing ledger says
they are included in `total_route_ms` / `total_runtime_millis`.

HOTPATH-1 adds `route_shape_stratification_*` fields to every benchmark row so route lane, route
family, start/end state, row-count class, source-file shape, timing-total field, and diagnostic
stage-attribution scope are explicit. HOTPATH-5 adds `source_to_vortex_array_guard_*` fields so the
Vortex array-build substage is guarded separately from the inclusive compatibility-import bundle.
These fields improve interpretation only; they do not refresh route timings or authorize a new
performance claim.

The public workflow route facade, exposed as `shardloom route <sql|python|dataframe|cli> --format
json`, `shardloom run <sql|python|dataframe|cli> --format json`, and Python `.route()` / `.run()`
helpers, mirrors route-admission metadata before or during scoped execution. The `route` envelope is
side-effect-free and is not a benchmark timing row. It must report `runtime_execution=false`,
`fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status=route_inspection_only` unless a later phase intentionally changes the contract
with new evidence. The `run` / `prepare` facades may attach the same metadata to admitted runtime
envelopes, but those helper envelopes still do not refresh benchmark timing values or performance
interpretation.

Readiness fields also stay separate:

```text
route_runtime_status=scoped_runtime_supported|smoke_supported|feature_gated|blocked|unsupported|external_baseline_only
claim_gate_status=claim_grade|not_claim_grade|fixture_smoke_only|external_baseline_only
performance_claim_allowed=false
production_claim_allowed=false
spark_replacement_claim_allowed=false
```

An unsupported external baseline row is an external-engine limitation. It is not a ShardLoom
runtime gap unless the corresponding ShardLoom route row reports its own `route_runtime_status` as
blocked or unsupported.

`GAR-SCALE-1A` adds a fail-closed scale claim-gate contract to the benchmark artifact. Rows now
include `scale_contract_schema_version=shardloom.traditional_analytics.scale_claim_gate.v1`,
`scale_profile`, `scale_claim_status`, `data_volume_bytes`, `row_count_estimate`, `file_count`,
`partition_count`, `split_count`, `memory_budget_bytes`, `peak_memory_bytes`, `spill_allowed`,
`shuffle_required`, `retry_count`, `output_commit_status`, `object_store_involved`,
`table_format_involved`, `remote_workers_involved`, `foundry_runtime_invoked`,
`foundry_spark_invoked`, `scale_fallback_attempted=false`,
`scale_external_engine_invoked=false`, `scale_claim_gate_status=not_scale_grade`, and
`scale_claim_boundary`. Current ShardLoom rows remain `local_smoke` or `local_claim_grade` only;
prepared/native Vortex rows may additionally certify in-route local split-operator replay for
admitted stateless and stateful/shuffle families through `prepared_vortex_scale_split_operator_*`
fields. They still do not prove larger-than-memory, object-store, table, distributed, Foundry,
managed-platform, any-volume, Spark-replacement, or performance claims.

`GAR-SCALE-1B` adds a report-only SplitManifest/per-split evidence contract to benchmark rows.
Rows now include
`split_manifest_contract_schema_version=shardloom.traditional_analytics.split_manifest.v1`,
`split_manifest_status`, `split_manifest_id`, `split_manifest_digest`,
`split_manifest_source_state_id`, `split_manifest_split_count`, `split_id`, `source_state_id`,
`byte_range`, `row_range`, `estimated_rows`, `estimated_bytes`, `projection_mask`,
`filter_pushdown_status`, `split_retry_count`, `split_runtime_millis`, `split_rows_scanned`,
`split_rows_output`, `split_spill_bytes`, `split_output_ref`, `split_claim_status`,
`split_fallback_attempted=false`, `split_external_engine_invoked=false`,
`split_claim_gate_status=not_split_scale_grade`, and `split_claim_boundary`. Current rows expose
split planning posture only; they do not prove split-parallel runtime, larger-than-memory support,
distributed execution, object-store/table runtime, Spark-replacement, or performance claims.

`GAR-SCALE-1C` adds a fail-closed memory, spill, and backpressure contract to benchmark rows. Rows
now include
`memory_spill_contract_schema_version=shardloom.traditional_analytics.memory_spill_backpressure.v1`,
`memory_spill_status`, `memory_spill_id`, `memory_spill_digest`, `memory_budget_bytes`,
`operator_memory_budget_bytes`, `peak_memory_bytes`, `memory_budget_exceeded=false`,
`spill_allowed=false`, `spill_location=not_admitted`, `spill_bytes_written=0`,
`spill_bytes_read=0`, `spill_file_count=0`,
`spill_cleanup_status=not_needed_no_spill_runtime`,
`backpressure_status=not_admitted_report_only`,
`oom_prevention_status=not_larger_than_memory_proof`,
`memory_spill_fallback_attempted=false`, `memory_spill_external_engine_invoked=false`,
`memory_spill_claim_gate_status=not_larger_than_memory_grade`, and
`memory_spill_claim_boundary`. Current rows expose memory/spill vocabulary and deterministic
fail-closed posture only; they do not prove larger-than-memory execution, runtime spill,
backpressure, hidden materialization safety, Spark-replacement, or performance claims.

`GAR-SCALE-1D` adds a report-only shuffle, repartition, and join scale contract to benchmark rows.
Rows now include
`shuffle_contract_schema_version=shardloom.traditional_analytics.shuffle_repartition.v1`,
`shuffle_evidence_status`, `shuffle_plan_id`, `shuffle_plan_digest`, `shuffle_required`,
`shuffle_strategy`, `partitioning_strategy`, `shuffle_partition_count`,
`target_shuffle_partition_bytes`, `local_combine_used`, `global_merge_used`,
`broadcast_candidate`, `broadcast_admitted`, `skew_detected`, `skew_strategy`,
`shuffle_spill_bytes`, `shuffle_retry_count`, `shuffle_correctness_digest`,
`shuffle_fallback_attempted=false`, `shuffle_external_engine_invoked=false`,
`shuffle_claim_gate_status=not_shuffle_scale_grade`, and `shuffle_claim_boundary`. Current rows
classify local join, group-by, window, top-N, repartition, and CDC posture. Prepared/native Vortex
rows can certify local stateful/shuffle split-operator replay with family, local-combine,
global-merge, retry replay, output commit proof, and execution-certificate fields; they still do
not prove distributed shuffle, Spark-scale joins, larger-than-memory spill, skew handling,
partitioned writes, Spark-replacement, or performance claims.

`GAR-SCALE-1E` adds a report-only object-store/table-scale ladder to benchmark rows. Rows now
include
`object_table_ladder_schema_version=shardloom.traditional_analytics.object_table_scale_ladder.v1`,
object-store URI/listing/split-planning/read/write/commit status fields, table metadata, snapshot,
append, merge/update/delete, commit, and rollback status fields, `credential_policy_status`,
`network_effect_status`, `listing_strategy`, `object_version_or_etag`, `split_manifest_id`,
`commit_protocol`, `idempotency_key`, `rollback_status`, `table_snapshot_id`,
`table_manifest_count`, `table_data_file_count`, `object_store_involved=false`,
`table_format_involved=false`, separate object-store read/write and table runtime/commit claim
gates, `object_table_ladder_fallback_attempted=false`,
`object_table_ladder_external_engine_invoked=false`, and
`object_table_ladder_claim_gate_status=not_object_table_scale_grade`. Current rows expose staged
readiness only; they do not prove object-store runtime, table runtime, table commit, credential
resolution, network effects, lakehouse production support, Spark-replacement, or performance claims.

`GAR-SCALE-1F` adds a report-only distributed protocol contract to benchmark rows. Rows now include
`distributed_protocol_schema_version=shardloom.traditional_analytics.distributed_protocol.v1`,
`coordinator_invoked=false`, `worker_count=0`, `remote_worker_invoked=false`,
`task_lease_id=none`, `task_attempt_id=none`, `worker_input_ref=none`,
`worker_output_ref=none`, `worker_retry_count=0`, `worker_failure_class=none`,
`result_fragment_digest=not_emitted_report_only`, `merge_digest=not_emitted_report_only`,
`distributed_claim_status=report_only`, `distributed_fallback_attempted=false`,
`distributed_external_engine_invoked=false`, and
`distributed_claim_gate_status=not_distributed_runtime_grade`. Current rows expose protocol
vocabulary only; they do not prove coordinator, worker, task lease, remote split execution, retry,
fragment merge, network API, distributed runtime, Spark-replacement, or performance claims.

`GAR-SCALE-1G` adds a scale benchmark profile contract to benchmark rows and artifacts. Rows now
include
`scale_benchmark_profile_schema_version=shardloom.traditional_analytics.scale_benchmark_profile.v1`,
profile vocabulary for `local_stress`, `larger_than_memory_local`, `many_small_files`,
`partitioned_table_metadata`, `object_store_report_only`, `table_metadata_report_only`,
`foundry_dev_stack_scale_proof`, and `distributed_report_only`, plus row metrics for rows, input
bytes, file count, split count, peak memory, spill bytes, shuffle bytes, retry count, correctness
digest, synthetic-evidence status, public-leaderboard separation, no-fallback evidence, and
`scale_benchmark_claim_gate_status=not_scale_benchmark_grade`. Scale benchmark profiles are
publishing/evidence posture only. Synthetic metadata-only rows cannot become runtime scale proof,
and actual large-volume evidence requires real input bytes, correctness proof, a declared resource
envelope, no-fallback evidence, and the relevant runtime gates.

`GAR-PERF-1B` adds the source-state coverage matrix at
`docs/architecture/source-state-reuse-coverage-matrix.md` and propagates
`source_state_coverage_*` fields into prepared/native batch evidence. Those
fields classify requested scenario rows as `source-state-reused`,
`source-state-not-needed`, `blocked-with-reason`, or
`unsupported-with-reason`. The rows also make the current digest boundary
explicit with `source_state_digest_status=emitted_scoped_in_memory_source_state_digest`,
`source_state_digest_algorithm=fnv1a64`, `source_state_digest_scope`, and
`source_state_family_digests`; benchmark rows expose the direct scoped batch digest as
`batch_source_state_digest` to avoid colliding with the universal SourceState row digest. Those
fields remain the scoped in-process batch source-state coverage matrix.

`GAR-IOREUSE-1A` adds a separate universal SourceState row contract to the benchmark artifact. Rows
now carry `source_state_contract_schema_version=shardloom.traditional_analytics.source_state.v1`,
`source_state_status`, `source_state_id`, `source_state_digest`, `source_format`,
`source_location`, `source_fingerprint_kind`, `schema_digest`, `row_count_known`, `file_count`,
`byte_size`, `partition_columns`, `compression`, `source_state_reuse_allowed`,
`source_discovery_millis`, `schema_inference_millis`, `source_parse_millis`,
`parse_decode_plan_digest`, `source_state_reuse_hit`, `source_state_reuse_reason`,
`source_state_materialization_decode_boundary_ref`, `source_state_fallback_attempted=false`,
`source_state_external_engine_invoked=false`, `source_state_claim_gate_status=not_claim_grade`, and
`source_state_claim_boundary`. This SourceState contract covers local source discovery/schema
identity/fingerprint/parse-plan posture and scoped reuse visibility; it is not output support,
Vortex-native execution, object-store/lakehouse support, SQL/DataFrame runtime, or performance
evidence.

`GAR-IOREUSE-1B` adds a companion VortexPreparedState row contract to the benchmark artifact. Rows
now carry
`prepared_state_contract_schema_version=shardloom.traditional_analytics.vortex_prepared_state.v1`,
`prepared_state_status`, `prepared_state_id`, `prepared_state_digest`,
`prepared_state_source_state_id`, `vortex_artifact_ref`, `vortex_artifact_digest`,
`layout_summary`, `encoding_summary`, `statistics_summary`, `prepared_state_reuse_allowed`,
`prepared_state_reuse_hit`, `prepared_state_reuse_reason`, `preparation_included_in_timing`,
`vortex_prepare_millis`, `prepared_state_materialization_decode_boundary_ref`,
`prepared_state_native_io_certificate_status`, `prepared_state_fallback_attempted=false`,
`prepared_state_external_engine_invoked=false`, `prepared_state_claim_gate_status=not_claim_grade`,
and `prepared_state_claim_boundary`. This prepared-state contract covers scoped local prepared
artifact identity, digest, preparation timing separation, source-state linkage, and reuse posture;
it is not output support, encoded-native operator coverage, object-store/lakehouse support,
SQL/DataFrame runtime, or performance evidence.
For `shardloom-prepare-batch`, HOTPATH-9 adds
`prepare_batch_prepared_state_dependency_*` and
`prepare_batch_prepared_state_partial_repair_*` fields. These report which workspace-manifest
dependencies were checked, which role changed on a miss, and whether partial repair/regeneration was
performed. Current prepared-batch partial repair remains blocked with
`prepare_batch_prepared_state_partial_repair_regeneration_performed=false` and
`prepare_batch_prepared_state_partial_repair_stale_segment_reuse_allowed=false`; mismatched source,
policy, packet, manifest, or artifact dependencies force full reprepare rather than silent stale
segment reuse.

The source-to-array guard keeps `vortex_array_build_*` and
`exclusive_source_to_vortex_array_*` as the exclusive Vortex array-build evidence while preserving
`compatibility_to_vortex_import_*` and `inclusive_compatibility_to_vortex_import_*` as inclusive
source-read/parse/array-build/write bundle evidence. The benchmark validator fails ShardLoom
compatibility-import rows if this distinction disappears, if route-shape lane metadata mismatches
the selected execution mode, or if either guard reports fallback/external-engine execution.

`GAR-IOREUSE-1I` extends the harness schema with
`vortex_preparation_spine_schema_version=shardloom.traditional_analytics.vortex_preparation_spine.v1`.
The next full data refresh will carry Vortex-first provider decisions, source split refs, byte/row
range refs, Vortex sink refs, prepared-artifact segment evidence, Native I/O posture,
no-standalone-lane status, and no-fallback fields. The refresh is intentionally tabled until the
directly related cold-lane evidence bundle is merged so published data does not churn between
related items.

`GAR-IOREUSE-1J` extends the harness schema with
`vortex_differential_preparation_schema_version=shardloom.traditional_analytics.vortex_differential_preparation.v1`.
The next full data refresh will distinguish full cold prepare rows from scoped append-only delta
overlay rows using base/delta SourceState and VortexPreparedState IDs, delta manifest digest,
changed byte/row/segment refs, schema compatibility, update-mode policy, overlay/correctness
digests, Native I/O posture, no-standalone-lane status, and no-fallback fields. This is local
append-only differential-preparation evidence only; it does not claim broad CDC/table transaction
support.

`GAR-IOREUSE-1K` extends the harness schema with
`vortex_capillary_preparation_schema_version=shardloom.vortex_capillary_preparation.v1`.
The next full data refresh will expose the `dynamic_size_complexity_gate.v1` activation policy plus
the source split discovery, read chunk, columnarize/encode, Vortex segment write, reopen
verification, and sink evidence capillary tasks through the existing `vortex_ingest` route. Small
local rows can explicitly carry `not_requested_below_threshold`, `activation_result=skipped`,
`task_count=0`, and `pulseweave_status=not_requested`; larger/complex rows carry activated task
manifest IDs/digests, byte/row range refs, Vortex segment refs, writer sink refs,
memory/sink pressure posture, Native I/O and execution certificates, prefixed PulseWeave evidence,
no-standalone-lane status, and no-fallback fields. Benchmark measurement refresh remains tabled
until the directly related cold-lane bundle is merged.

`GAR-IOREUSE-1L` extends the harness schema with
`vortex_scout_ingress_schema_version=shardloom.traditional_analytics.vortex_scout_ingress.v1`.
The next full data refresh will carry source scope, metadata/sample ranges, schema digest before/
after, anomaly families, malformed row refs where safe, quarantine planning, redaction status,
unsupported diagnostics, correctness policy, no-standalone-lane status, and no-fallback fields.

`GAR-PERF-2J` extends the harness schema with
`vortex_layout_write_advisor_schema_version=shardloom.traditional_analytics.vortex_layout_write_advisor.v1`.
The next full data refresh will carry workload constitution, source statistics, pushdown/sink
requirements, strategy/provider posture, write/reopen verification depth, correctness refs,
benchmark refs, no-standalone-lane status, and no-fallback fields.

`GAR-PERF-2K` extends the harness schema with
`vortex_copy_budget_schema_version=shardloom.traditional_analytics.vortex_copy_budget.v1`. The next
full data refresh will carry allocation/copy scope, measured or explicit `not_measured` copy
segments, ownership policy, buffer reuse blockers, unsafe-lifetime posture, correctness parity
refs, no-standalone-lane status, and no-fallback fields. These are schema and evidence-surface
updates only until the public benchmark measurement refresh runs.

`GAR-IOREUSE-1C` adds a companion OutputPlan row contract to the benchmark artifact. Rows now carry
`output_plan_contract_schema_version=shardloom.traditional_analytics.output_plan.v1`,
`output_plan_status`, `output_plan_id`, `output_plan_digest`, `output_format`, `output_location`,
`output_schema_digest`, `output_partitioning`, `output_compression`, `output_encoding`,
`output_write_mode`, `output_plan_reuse_allowed`, `output_metadata_preservation_status`,
`output_materialization_required`, `output_plan_reuse_hit`, `output_plan_reuse_reason`,
`output_plan_millis`, `output_write_millis`, `result_replay_verified`,
`output_native_io_certificate_status`, `sink_artifact_ref`, `sink_artifact_digest`,
`output_plan_fallback_attempted=false`, `output_plan_external_engine_invoked=false`,
`output_plan_claim_gate_status=not_claim_grade`, and `output_plan_claim_boundary`. This
output-plan contract covers scoped local Vortex result-sink planning, write/replay refs, metadata
preservation posture, and sink artifact identity; it is not cross-format fanout, object-store/
lakehouse support, table commit support, production sink support, or performance evidence.

`GAR-IOREUSE-1D` adds a first-class `io_reuse_and_fanout` benchmark matrix to the artifact. The
matrix lists required fanout cases and their current deterministic blockers:

```text
CSV input -> Parquet + JSONL + Vortex outputs
Parquet input -> CSV + Vortex outputs
JSONL input -> Parquet + Vortex outputs
generated source -> CSV + Parquet + Vortex outputs
prepared Vortex -> multiple output formats
```

Current fanout rows are `fanout_status=report_only`, `fanout_output_count=0`,
`fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status=not_claim_grade`. They expose the required timing/reuse columns as explicit
not-measured values until a future runtime slice writes and replays multiple local outputs.

`GAR-IOREUSE-1E` adds a first-class cache invalidation/fingerprint matrix to the artifact. Current
rows expose `source_fingerprint_kind`, `source_content_digest`, `source_mtime`, `source_size`,
`object_etag`, `manifest_version`, `schema_digest`, `plan_digest`, `output_plan_digest`,
`cache_valid`, `invalidation_reason`, `cache_invalidation_fallback_attempted=false`,
`cache_invalidation_external_engine_invoked=false`,
`cache_invalidation_claim_gate_status=not_claim_grade`, and
`cache_invalidation_redaction_status=no_credentials_or_tokens_in_fingerprint_fields`.
`cache_valid=true` means current-row local fingerprints are internally consistent; it is not a
persistent cache hit, hidden fast mode, object-store cache, or performance claim.

`GAR-IOREUSE-1F` adds evidence-safe reuse levels through
`reuse_level_contract_schema_version=shardloom.traditional_analytics.evidence_safe_reuse_levels.v1`
and `reuse_level_matrix`. Matrix rows classify `discovery_reuse`, `schema_reuse`,
`parse_plan_reuse`, `prepared_vortex_reuse`, `operator_source_state_reuse`, `output_plan_reuse`,
and `result_replay_reuse` independently from execution mode, evidence level, output format, and
claim gate. Reuse hits or misses remain `not_claim_grade` visibility evidence only.

`GAR-PERF-1C` adds scoped fused-pipeline evidence for the current prepared/native
filter/projection/limit row and selective-filter selection-vector metric aggregation row. The
benchmark harness now carries `fused_pipeline_*` fields, including `fused_pipeline_used`,
`fused_operator_family`, `intermediate_materialization_avoided`,
`fused_pipeline_rows_selected`, `fused_pipeline_rows_output`,
`fused_pipeline_correctness_digest_status`,
`fused_pipeline_unfused_correctness_digest`, `fused_pipeline_fused_correctness_digest`,
`fused_pipeline_correctness_digest_match`, `fused_pipeline_data_decoded`,
`fused_pipeline_data_materialized`, `fused_pipeline_blocker_id`,
`fused_pipeline_blocker_reason`, `fused_pipeline_claim_gate_status`,
`fused_pipeline_fallback_attempted=false`, and
`fused_pipeline_external_engine_invoked=false`. These rows are scoped residual-native runtime
evidence only; `fused_pipeline_encoded_native_claim_allowed=false` remains required until later
end-to-end encoded-native certificates exist.

## Evidence-Level Runtime Tiering Queue

`GAR-PERF-2A` adds first-class benchmark evidence levels to the scoped prepared/native batch
runner:

```text
minimal_runtime
certified
full_replay
```

The goal is to let benchmark readers see proof overhead without creating a hidden fast mode. Batch
rows now report `runtime_evidence_level_schema_version`, `requested_evidence_level`,
`selected_evidence_level`, `evidence_level`, `evidence_level_claim_gate_status`,
`evidence_level_result_sink_replay_required`, `evidence_level_result_sink_replay_verified`,
`evidence_level_certificate_refs`, `evidence_level_source_state_digest`,
`evidence_level_output_digest`, `evidence_level_fallback_attempted=false`, and
`evidence_level_external_engine_invoked=false`.

`minimal_runtime` omits result-sink replay and remains `claim_gate_status=not_claim_grade`.
`certified` emits normal certificate evidence without replay by default. `full_replay` requires
result-sink replay proof. None of these levels create a performance, superiority, production,
SQL/DataFrame, object-store/lakehouse, Foundry, package, or Spark-displacement claim.

## Vortex Scan Pushdown Completion Queue

`GAR-PERF-2C` adds an explicit completion contract for prepared/native Vortex Scan API pushdown.
Benchmark rows now expose scoped `source_backed_scan_*` evidence plus `scan_pushdown_*` fields so
every prepared/native scenario family has a filter/projection/limit status or deterministic
blocker.

Benchmark rows expose:

```text
scan_pushdown_status
scan_filter_pushed_down
scan_projection_pushed_down
scan_limit_pushed_down
scan_limit_requested_rows
scan_limit_request_scope
scan_residual_limit_required
scan_residual_limit_applied
scan_residual_limit_status
scan_residual_limit_executor
scan_residual_limit_input_rows
scan_residual_limit_rows_output
scan_residual_limit_reason
scan_filter_columns_read
scan_output_columns_read
scan_filter_only_columns_read
data_materialized
data_decoded
scan_pushdown_blocker_id
scan_pushdown_blocker_reason
scan_pushdown_fallback_attempted=false
scan_pushdown_external_engine_invoked=false
scan_pushdown_claim_gate_status=not_claim_grade
```

The row contract must distinguish filter-only columns from output columns. Limit-like operators that
cannot be admitted into Vortex Scan must report the ShardLoom-native residual executor, requested
row scope, input rows, and output rows instead of hiding the work behind a blocker. Pushdown evidence
is source/provider-boundary evidence only; it is not an encoded-native operator claim, generalized
Source/Split runtime claim, object-store/lakehouse claim, SQL/DataFrame claim, or performance claim.
Current limit/slice pushdown fields remain explicit blockers for order-sensitive or grouped
residual limit-like scenarios, with residual executor evidence when ShardLoom performs the work.

## Evidence-Aware Logical Optimizer Queue

`GAR-PERF-2B` adds the report-only optimizer rule registry and optimizer trace. ShardLoom benchmark
rows now link timing/resource rows to optimizer traces, but those traces must not be read as lazy
optimizer parity or performance proof.

Benchmark rows and optimizer trace artifacts expose:

```text
optimizer_trace_id
optimizer_registry_version
optimizer_phase
optimizer_rule_id
optimizer_rule_family
optimizer_rule_status
optimizer_rule_admitted
optimizer_rule_applied
optimizer_rule_blocked_reason
before_plan_digest
after_plan_digest
rewrite_safety_status
evidence_preserved=true
no_fallback_preserved=true
claim_boundary_preserved=true
materialization_boundary_preserved
source_state_reuse_admitted
estimated_input_cardinality
estimated_output_cardinality
cardinality_estimation_status
correctness_smoke_ref
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

Initial rule families are predicate pushdown, projection pushdown, slice/limit pushdown, common
subplan/source-state reuse, expression simplification, constant folding, type coercion, join
ordering, and cardinality estimation. Rules should be visible as admitted, applied, blocked,
unsupported, not applicable, or report-only. Any applied runtime rewrite needs before/after digest
evidence and correctness smoke before it can be treated as supported.

## I/O Reuse And Cross-Format Fanout Queue

`GAR-IOREUSE-1` tracks the planned reusable I/O state, Vortex preparation, output planning, and
cross-format fanout benchmark bundle. The goal is to avoid repeated source discovery, schema/dtype
inference, parsing, Vortex preparation, operator source-state construction, and output write
planning across multi-step workflows while keeping input and output formats decoupled.

The stable path is:

```text
InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan -> SinkArtifact
```

Planned benchmark families:

```text
io_reuse_and_fanout
source_state_reuse
prepared_state_reuse
output_plan_reuse
cross_format_output
generated_source_output
```

Planned fanout cases:

```text
CSV input -> Parquet + JSONL + Vortex outputs
Parquet input -> CSV + Vortex outputs
JSONL input -> Parquet + Vortex outputs
generated source -> CSV + Parquet + Vortex outputs
prepared Vortex -> multiple output formats
```

The current SourceState, VortexPreparedState, OutputPlan, fanout matrix, and cache invalidation
slices emit the source discovery/schema/parse, prepared artifact, local output-plan, report-only
fanout, and fingerprint/invalidation subsets above. Required future timing fields for runtime
fanout rows:

```text
operator_compute_millis
output_plan_millis
output_write_millis
output_replay_millis
total_runtime_millis
```

Required future reuse fields:

```text
source_state_reuse_hit
output_plan_reuse_hit
fanout_output_count
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

The benchmark must separate one-shot timing from reuse/fanout timing. It must not mark an output
sink as supported without replay/evidence proof, and it must not present reuse or fanout rows as
public performance, superiority, production, object-store/lakehouse, Foundry, SQL/DataFrame, or
Spark-replacement claims.

## Compressed/Encoded Kernel Registry Evidence

`GAR-PERF-2D` adds scoped compressed/encoded kernel registry evidence to prepared/native rows. The
registry makes encoding/operator support visible without turning the row into an encoded-native
operator claim. Current fixtures execute the observed `flag:fastlanes.bitpacked`,
`value:vortex.sequence`, and `flag:vortex.constant` reader-generated filter inputs when Vortex emits
those encodings, and execute dictionary group-by evidence from a real prepared Vortex `vortex.dict`
reader chunk. Sorted/min-max, FSST/string, sparse, TurboQuant/vector, and broader operator-family
rows remain deterministic blockers or future candidates.

Benchmark rows expose:

```text
compressed_kernel_registry_schema_version
compressed_kernel_registry_pair_ids
compressed_kernel_registry_pair_statuses
compressed_kernel_registry_encoding_ids
compressed_kernel_registry_logical_dtypes
compressed_kernel_registry_physical_encodings
compressed_kernel_registry_operator_families
compressed_kernel_registry_kernel_admitted
compressed_kernel_registry_kernel_executed
compressed_kernel_registry_canonicalization_required
compressed_kernel_registry_decoded
compressed_kernel_registry_materialized
compressed_kernel_registry_selection_vector_emitted
compressed_kernel_registry_input_rows
compressed_kernel_registry_selected_rows
compressed_kernel_registry_operator_kernel_micros
compressed_kernel_registry_decoded_reference_micros
compressed_kernel_registry_input_shape_classes
compressed_kernel_registry_kernel_specialization_profiles
compressed_kernel_registry_focused_microbenchmark_refs
compressed_kernel_registry_focused_microbenchmark_statuses
compressed_kernel_registry_promotion_statuses
compressed_kernel_registry_decoded_reference_compared
compressed_kernel_registry_correctness_digest_status
compressed_kernel_registry_correctness_digests
compressed_kernel_registry_validity_semantics
compressed_kernel_registry_unsupported_kernel_reasons
compressed_kernel_registry_encoded_native_claim_allowed
compressed_kernel_registry_claim_gate_status
compressed_kernel_registry_fallback_attempted=false
compressed_kernel_registry_external_engine_invoked=false
```

Unsupported encodings remain deterministic blockers, and
`compressed_kernel_registry_encoded_native_claim_allowed=false` remains required until a future
end-to-end encoded-native certificate exists.

## Fused Operator Pipeline Evidence

`GAR-PERF-2E` adds the scoped fused local prepared/native pipeline evidence layer. The benchmark
now classifies the planned family set and reports executed or blocked posture without creating an
encoded-native or performance claim.

Benchmark rows expose:

```text
fused_pipeline_schema_version
fused_pipeline_family_statuses
fused_pipeline_used
fused_operator_family
intermediate_materialization_avoided
fused_pipeline_rows_scanned
fused_pipeline_rows_selected
fused_pipeline_rows_output
fused_pipeline_correctness_digest_status
fused_pipeline_unfused_correctness_digest
fused_pipeline_fused_correctness_digest
fused_pipeline_correctness_digest_match
fused_pipeline_data_materialized
fused_pipeline_data_decoded
fused_pipeline_blocker_id
fused_pipeline_blocker_reason
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

Current executed families are filter + projection + limit, filter + aggregate through the
selection-vector metric path, and top-k with projection. Filter + group-by is explicitly blocked as
`gar-perf-2e.filter_group_by_filter_absent` until a scoped filtered grouped scenario exists.
Unsupported fusion paths are deterministic blockers, not fallback execution. Fusion rows are local
pre-release evidence and not a performance ranking or broad SQL/DataFrame claim.

## In-Process Session Runtime Queue

`GAR-PERF-2F` now has a scoped in-process session-backed prepared/native batch lane. The existing
`traditional-analytics-vortex-batch-run` command opens a caller-owned local session over supplied
Vortex artifacts, executes the requested scenarios without respawning the CLI, emits
session/cache/lifecycle evidence, and closes the session before returning the typed envelope. A
general reusable public local session API is still not exposed.

Current session row fields include:

```text
session_schema_version
session_id
session_runtime_status
session_state_scope
session_open_status
session_close_status
session_prepared_artifact_cache_hit_count
session_prepared_artifact_cache_miss_count
session_prepared_artifact_reuse_count
session_source_metadata_cache_hit_count
session_source_metadata_cache_miss_count
session_source_state_cache_hit_count
session_source_state_cache_miss_count
session_source_state_reuse_count
session_hidden_global_cache=false
session_daemon_or_service=false
session_fallback_attempted=false
session_external_engine_invoked=false
session_claim_gate_status=fixture_smoke_only
```

The session queue exists to reduce redundant local setup and make reuse visible. It is not a daemon,
remote server, hidden fast mode, SQL/DataFrame runtime, object-store/lakehouse runtime, production
claim, or performance claim.

## Allocation And Buffer-Pool Optimization Queue

`GAR-PERF-2G` now emits a scoped allocation/resource-profile evidence slice on the
session-backed prepared/native batch lane. Current benchmark rows should not imply that a global
allocation or buffer-pool optimization pass exists.

Batch rows and future memory/resource reports should expose:

```text
allocation_profile_status
allocation_profile_scope
allocation_count
allocation_count_status
allocation_bytes
allocation_bytes_status
buffer_pool_enabled
buffer_pool_scope
buffer_reuse_count
buffer_reuse_family
buffer_reuse_blocker
peak_rss_delta
peak_rss_delta_status
source_state_digest
output_digest
correctness_digest
evidence_regression_status
unsafe_lifetime_shortcut_used=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

The first planned families are result buffers, temporary vectors, hash tables, dictionary/string
state, and source-state arrays. Buffer reuse must be opt-in or scoped to an explicit run/session and
must preserve correctness and evidence parity with the no-reuse path. The current slice reports
allocation counts, allocation bytes, and peak RSS as `not_available`; that means unknown/not
measured, not zero. It also reports `buffer_pool_enabled=false`, `buffer_reuse_count=0`, and a
deterministic buffer-reuse blocker until safe reuse exists.

These rows are resource-profile evidence only. They are not speed, memory-efficiency, production,
SQL/DataFrame, object-store/lakehouse, Foundry, or Spark-replacement claims.

## Optimized Build Profiles And PGO Lane

`GAR-PERF-2H` adds explicit optimized build-profile evidence for benchmark rows. The harness accepts
`debug`, `release`, `release-lto`, `release-pgo`, and `release-native-benchmark` through
`--shardloom-build-profile`, records compiler/toolchain posture in JSON/Markdown artifacts, and
keeps build time excluded from per-scenario timing.

Benchmark rows expose:

```text
build_profile
build_profile_kind
rustc_version
cargo_version
target_triple
target_cpu_policy
target_cpu_native_enabled
lto_enabled
lto_mode
codegen_units
pgo_status
pgo_profile_generate_status
pgo_profile_use_status
pgo_profile_artifact_ref
pgo_training_workload_ref
pgo_training_workload_digest
build_reproducibility_status
portable_release_artifact
benchmark_only_build
build_profile_correctness_digest
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

`release-lto` is the portable ThinLTO lane. `release-pgo` is a benchmark-only PGO lane and remains
report-only unless a merged profile is supplied through `SHARDLOOM_PGO_PROFILE`.
`release-native-benchmark` is host-native and benchmark-only; `target-cpu=native` is never portable
release/package evidence. These rows are build/config evidence only and are not public performance
rankings.

## Native Microbenchmark Suite

Native microbenchmark rows are separate from traditional end-to-end rows,
compatibility-import rows, prepared/native batch rows, and external baseline
rows. Older artifacts may show native microbenchmarks as skipped; current
artifacts emit first-class subsystem rows or deterministic blockers for every
required microbenchmark family.

The suite covers or explicitly blocks:

- Vortex scan only: deterministic blocker until an isolated scan-only primitive exists.
- filter predicate only: smoke-supported through current `vortex-run count-where` rows.
- projection only: smoke-supported through current `vortex-run project` rows.
- group-by kernel: smoke-supported through `operator-microkernel-benchmark`, which reports
  dictionary group-by timing plus bitpacked, sequence, and constant predicate-pair timing fields.
- hash join kernel: deterministic blocker until an isolated native kernel primitive exists.
- top-k: deterministic blocker until an isolated native top-k primitive exists.
- result-sink write: deterministic blocker until an isolated result-sink write primitive exists.
- evidence render: smoke-supported through benchmark harness JSON/Markdown rendering rows.

Rows are labeled as `benchmark_category=native_microbenchmark` and expose
primitive family, subsystem, optimization question, support status, rows
scanned/selected/materialized where available, decoded/materialized status,
timing scope, `fallback_attempted=false`, `external_engine_invoked=false`,
`claim_gate_status`, and a deterministic unsupported reason when unavailable.
The operator microkernel row also exposes `operator_microkernel_*` pair IDs, shape classes,
operator-kernel timings, decoded-reference timings, correctness digests, focused benchmark refs, and
promotion statuses. These are subsystem evidence only, not route totals.

These rows identify which subsystem needs optimization. They are not end-to-end performance claims,
not public rankings, not Spark-replacement evidence, and not production SQL/DataFrame,
object-store/lakehouse, or Foundry claims.

## Claim Scope

Coverage rows are separate from timing rows. Each row carries a
`row_classification`/`status` and a `support_status` so support evidence,
claim evidence, fixture-smoke rows, unsupported rows, blocked rows, and
external baselines are not conflated.

ShardLoom rows are claim-grade only when the artifact includes stable
correctness digests across at least three iterations, benchmark and coverage
refs, execution certificate evidence, source Native I/O certificate evidence,
result Native I/O certificate evidence when result-sink proof is enabled,
materialization/decode boundary evidence, `fallback_attempted=false`, and
`external_engine_invoked=false`. Unsupported or incompatible scenario/profile
pairs should emit deterministic coverage rows rather than crash or delegate to
an external engine.
