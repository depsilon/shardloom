# Traditional Analytics Benchmark Harness

This harness runs conventional dataframe/SQL workloads against ShardLoom plus
external comparison engines:

- ShardLoom
- ShardLoom native Vortex
- ShardLoom prepared Vortex
- ShardLoom direct transient CSV smoke
- pandas
- Polars
- DuckDB
- Spark/PySpark default local profile
- Spark/PySpark tuned local profile
- DataFusion Python
- Dask

The external engines are benchmark tooling only. They are never ShardLoom
runtime dependencies and never execute unsupported ShardLoom plans as fallback engines.

## Taxonomy-Driven Local Suite

The harness runs the executable `local_analytics` suite. Its machine-readable scenario catalog
lives at `benchmarks/common/scenario_catalog.json`; broader benchmark architecture and historical
status live in `docs/architecture/benchmark-suite-catalog.md` and the phased execution ledger.

Timing tables and support/coverage tables are separate. Managed platforms such as Photon, Fabric,
Snowflake, BigQuery, Redshift, and Databricks managed services are design references only, not
default benchmark lanes or fallback engines.

Every result row now records:

- `benchmark_suite`
- `scenario_id`
- `scenario_category`
- `dataset_profile`
- `engine_role`
- `benchmark_constitution`

The JSON artifact also includes a `coverage_table` so functional support/coverage remains visible
even when timing rows fail, are unsupported, or belong to external baselines. Coverage rows carry a
separate `claim_gate_status` so `claim_grade`, `not_claim_grade`, `fixture_smoke_only`, and
`external_baseline_only` rows are not confused with raw timing rows. Unsupported rows remain
`support_status=unsupported`, carry `native_unsupported_coverage_ref`, and name a deterministic
`unsupported_diagnostic_code` plus future evidence where the harness can derive it. Rows also carry
`requested_execution_mode`, `selected_execution_mode`, `mode_selection_reason`,
`execution_mode_family`, `native_io_source_sink_coverage_ref`, and stage-attribution fields so
compatibility-import-certified timing is not confused with prepared/native Vortex timing. The
Native I/O source/sink ref points to the RFC 0031 matrix in `native-io-envelope-plan`; it is
coverage evidence, not a timing or production-readiness claim.

## Workloads

The deterministic generator creates a fact table and a dimension table as CSV,
then writes requested compatibility-format copies. The default run covers CSV
and Parquet; `--formats` can also include JSONL/NDJSON, Arrow IPC, Avro, and
ORC. Each engine runs only the formats it declares support for, and unsupported
rows are captured without aborting the report. The `shardloom` lane imports
each selected compatibility format into local Vortex files before running the
temporary benchmark operator. The `shardloom-vortex` and `shardloom-prepared-vortex`
lanes prepare native Vortex artifacts once for each requested source format and then
report the native/prepared scenario result under that source-format row, such as CSV
or Parquet. They do not add standalone `.vortex` report rows.
The optional `shardloom-direct-transient` lane runs only the scoped local CSV
`selective filter` smoke path without persistent Vortex write/reopen. It exists to
prove direct transient admission and evidence shape; it is not a Vortex-native,
SQL/DataFrame, or performance-claim lane.
Native Vortex rows start from prepared/existing Vortex artifacts, but they may still use temporary
ShardLoom operator paths unless the row's evidence proves encoded/native execution. External-engine
rows are baseline comparisons only and never execute unsupported ShardLoom work as fallback.
The scenarios are:

- `csv/file ingest`
- `selective filter`
- `group by aggregation`
- `sort and top-k`
- `hash join`
- `wide projection`
- `distinct count`

The opt-in taxonomy-expanded local analytics scenarios are available with
`--include-taxonomy-extra` or repeated `--scenario` flags:

- `filter + projection + limit`
- `multi-key group by`
- `join + aggregate`
- `row number window`
- `partition pruning`
- `many-small-files scan`
- `null-heavy aggregate`
- `high-cardinality string group/distinct`
- `top-N per group`
- `clean/cast/filter/write`
- `malformed timestamp / dirty CSV`
- `small change over large base`
- `nested JSON field scan`

The `shardloom` lane executes the base-schema taxonomy extras
`filter + projection + limit`, `multi-key group by`, `join + aggregate`, `row number window`,
`partition pruning`, `many-small-files scan`, `null-heavy aggregate`,
`high-cardinality string group/distinct`, and `top-N per group`, plus dirty-CSV
`clean/cast/filter/write`, dirty-CSV `malformed timestamp / dirty CSV`, and `nested JSON field
scan`, plus CDC-overlay `small change over large base`, through the same local Vortex
import/replay/result-sink path as the default scenarios. The many-file row uses generated split
CSV/JSONL fact parts, the null-heavy row uses generated `nullable_metric_00` fixture coverage, the
nested row uses JSONL or Arrow-family fixture inputs, and the CDC row uses an explicit generated
delta sidecar. Unsupported scenario/profile/engine pairs are reported in the coverage table without
invoking a fallback engine.

An opt-in stress lane is available with `--include-stress`:

- `scale stress skewed join aggregation`
- `scale stress multi-stage etl`

The stress lane is for volume/complexity bring-up. It combines larger CSV
inputs with shuffle-heavy joins, skewed grouped aggregation, derived metrics,
sort/top-N, and multi-stage ETL work that is usually where Spark-style engines
are most relevant. On a small local smoke dataset these rows are not expected to
prove Spark-only behavior; use larger-than-memory data, repeated runs, and the
same hardware/cache settings before drawing conclusions.

Each run writes a machine-readable JSON artifact and a human-readable Markdown
report. The report begins with fairness parameters, then includes an engine
overview with startup/warmup timing, scenario timing matrix, resource metrics,
ShardLoom runtime-effect evidence, fastest-row table, ASCII timing bars,
ShardLoom native microbenchmarks, ShardLoom DecisionTrace/WhyReport evidence,
ShardLoom work-avoidance evidence, ShardLoom write/commit evidence,
ShardLoom result-sink write timing, universal-I/O evidence lanes, correctness summary, and separate
failure/unsupported rows.

Each result artifact records engine versions, Python/runtime details, dataset
shape, selected source file sizes, wall/query time, sampled peak RSS when
`psutil` is available, rows scanned, rows materialized, bytes read, object-store
request count, and a correctness digest. ShardLoom rows also retain the emitted
native I/O evidence fields for per-path certificate id/status, source
capability, pushdown, sink requirement, adapter fidelity, materialization
boundary, decode, materialization, row reads, Arrow conversion, writes, spill,
auto-derived resource sizing, NativeIoCertificate status, local runtime task
graph/scheduler refs, bounded queue/backpressure status, memory reservation
request/grant/release counts, retry/cancellation gate status, operator spill
blockers, runtime execution certificate status, and report-only Vortex layout/write advisor fields
for chunking, encoding, statistics, dictionary, clustering, flush, compaction, read/write tradeoff,
evidence-source status, no-claim status, no-write status, and no-fallback status.

ShardLoom's reported benchmark version appends `-dirty` when the workspace has
uncommitted tracked changes, so local bring-up reports do not look like clean
revision evidence by accident.

ShardLoom native microbenchmark rows are separated from the traditional
compatibility-file engine rows. They include the approved local encoded
`CountAll` path plus local
`vortex-run` primitive evidence for count, projection, validity-filter
counting, and scan-pushdown comparison predicates. The report includes each
row's timing scope plus filter/projection pushdown fields so in-command
repeated timings are not mixed up with CLI-process wall-time smoke measurements.
The first-class native microbenchmark contract labels every row with
`benchmark_category=native_microbenchmark`,
`native_microbenchmark_primitive_family`,
`native_microbenchmark_support_status`, subsystem, optimization question,
rows scanned/selected/materialized, no-fallback/no-external-engine fields, and
a native microbenchmark claim boundary. Implemented smoke rows currently cover
encoded count, Vortex count/projection/filter-style primitives, local commit
manifest evidence, and benchmark evidence-rendering cost. Scan-only, group-by
kernel, hash-join kernel, top-k, and result-sink write rows remain visible as
deterministic blocked rows until isolated primitives exist. These rows are
subsystem evidence only, not end-to-end speed claims or public rankings.

The ShardLoom work-avoidance table is based on final row evidence, not only
plan analysis. The JSON artifact includes `work_avoidance_evidence_schema`
with the status vocabulary `measured`, `not_available`, `unsupported`, and
`not_applicable`. Each ShardLoom scenario row and native microbenchmark row
reports status/value/reason triples for rows avoided, segments pruned, bytes
avoided, encoded-vector reuse, and pushdown proof. Missing rows skipped,
segment-prune, bytes-not-read, encoded-vector reuse, or pushdown values are
not interpreted as zero, and no optimization, Spark-displacement, superiority,
or best-default claim is allowed from `not_available` evidence.

The ShardLoom DecisionTrace/WhyReport evidence table explains why each native
runtime row is or is not claim-grade. It records decision-trace counts, the
primary reason, summarized claim blockers, and the next evidence needed before
runtime measurements can become correctness/benchmark/certification claims.

The ShardLoom write/commit evidence table runs the local committed-manifest step against a
synthetic staged workspace and records commit
execution, manifest-commit status, bytes written, and average commit latency.
It is a local smoke benchmark only.

Numeric benchmark outputs are rounded to four decimal places before correctness
hashing. This keeps result comparison stable across engines with different
floating-point aggregation orders while preserving the two-decimal source metric
precision used by the deterministic dataset.

The generated dataset profile defaults to `narrow_fact_dim`. Supported profiles are
`tiny_smoke`, `narrow_fact_dim`, `skewed_keys`, `high_cardinality_strings`, `wide_table`,
`very_wide_table`, `null_heavy`, `many_small_files`, `few_large_files`, `partitioned_by_date`,
`poorly_clustered`, `well_clustered`, `schema_drift`, `dirty_csv`, `nested_json`, and
`cdc_delta_overlay`. Advanced profiles emit local fixture sidecars where needed: split CSV/JSONL
fact parts, malformed timestamp/numeric columns, nested JSON payloads, and deterministic CDC
overlay rows. Engines that do not support a selected scenario record an unsupported coverage row
instead of aborting the run.

ShardLoom traditional analytics rows call the workspace-local native Rust
command `shardloom traditional-analytics-run`. Build time is excluded from
per-scenario timing and the selected `--shardloom-build-profile` is recorded in
the fairness parameters. The harness builds ShardLoom with the
`vortex-traditional-analytics-benchmark` feature and times compatibility source
adapter/import, local Vortex file write, Vortex file reopen, Vortex scan, and
temporary benchmark operators over Vortex-derived arrays. These rows cover the feature-gated
universal-I/O path for CSV, JSONL/NDJSON, Parquet, Arrow IPC, Avro, and ORC.
When `--compatibility-output-format` is used by the CLI or fixture tests, the
same feature-gated path writes local compatibility outputs from Vortex-derived
tables for CSV, JSONL, Parquet, Arrow IPC, Avro, and ORC. That is fixture-smoke
translation evidence only: Vortex remains the highest-fidelity native output,
and the benchmark does not claim production sink APIs, object-store output, or
Iceberg/Delta table commit support.

For workflow replay evidence, the same command accepts `--verify-native-replay`.
That mode re-opens the emitted Vortex source artifacts, compares native replay
output against the compatibility-file execution result, and emits
`local_vortex_analytics_v1` evidence fields: artifact digests, schema summary,
benchmark and coverage row refs, replay Native I/O certificate status,
commit/cleanup status, and no-fallback policy fields. Add
`--write-result-vortex` to write the computed result envelope as `result.vortex`,
re-open it, compare the stored result JSON and materialized-row count, and emit
result-sink digest/schema/replay/certificate fields plus
`scenario_compute_micros` and `computed_result_sink_write_micros`. The harness promotes those into
top-level metrics as `scenario_compute_millis`, `computed_result_sink_write_millis`, and
`computed_result_sink_bytes` so write-path cost is visible separately from scenario runtime. The same
workflow emits local scheduler/runtime evidence: deterministic task graph
refs, scheduled/completed task counts, bounded queue/backpressure fields,
retry/cancellation gate status, memory reservation release counts,
fail-before-OOM status, operator spill blockers, and a runtime execution
certificate. The harness option `--shardloom-result-sink` enables replay,
result-sink evidence, and runtime evidence for ShardLoom rows; default
benchmark timings stay focused on the normal harness path unless the caller opts
into certification evidence.

ShardLoom prepared/native Vortex rows call `shardloom traditional-analytics-vortex-run`
against `.vortex` files produced before scenario timing. This separates preparation
timing from scenario timing while keeping the row attached to the requested source
format. The row records `preparation_millis`, `preparation_included_in_timing`,
`prepared_artifact_ref`, `prepared_artifact_digest`, `execution_mode=prepared_vortex`,
and the current materialization/scan evidence. The scoped `selective filter`, `wide projection`,
`filter + projection + limit`, `group by aggregation`, `multi-key group by`, `hash join`, and
`join + aggregate`, `sort and top-k`, `top-N per group`, `row number window`,
`high-cardinality string group/distinct`, `distinct count`, `null-heavy aggregate`,
`clean/cast/filter/write`, `malformed timestamp / dirty CSV`, `nested JSON field scan`, and
CDC-overlay `small change over large base`, plus scoped local `partition pruning`, prepared/native
paths use Vortex scan filter/projection pushdown where applicable and avoid full fact-table
materialization; other benchmark operators still materialize Vortex-derived arrays after scan.
These rows are not broad SQL/DataFrame performance claims.

Prepared artifacts now also carry lifecycle evidence: fact/dim artifact refs, fact/dim digests
when generated by ShardLoom, optional CDC delta artifact refs/digests for the scoped overlay row,
source Native I/O certificate status, reuse eligibility, workspace, lifecycle status, and
caller-owned cleanup policy. The benchmark may reuse those artifacts for prepared/native rows, but
cleanup remains explicit and caller-owned rather than hidden in the timed query path.
`GAR-IOREUSE-1B` adds a separate VortexPreparedState contract to the benchmark JSON/Markdown
artifact with `prepared_state_contract_schema_version=shardloom.traditional_analytics.vortex_prepared_state.v1`,
`prepared_state_status_vocabulary`, `prepared_state_status`, `prepared_state_id`,
`prepared_state_digest`, `prepared_state_source_state_id`, `vortex_artifact_ref`,
`vortex_artifact_digest`, `layout_summary`, `encoding_summary`, `statistics_summary`,
`prepared_state_reuse_allowed`, `prepared_state_reuse_hit`, `prepared_state_reuse_reason`,
`preparation_included_in_timing`, `vortex_prepare_millis`,
`prepared_state_materialization_decode_boundary_ref`,
`prepared_state_native_io_certificate_status`, `prepared_state_fallback_attempted=false`,
`prepared_state_external_engine_invoked=false`, `prepared_state_claim_gate_status=not_claim_grade`,
and `prepared_state_claim_boundary`. This prepared-state contract records scoped local prepared
artifact identity, digest, source-state linkage, preparation timing separation, and reuse posture
only; it is not output support, encoded-native coverage, object-store/lakehouse support, or a
performance claim.
`GAR-IOREUSE-1C` adds a separate OutputPlan contract to the benchmark JSON/Markdown artifact with
`output_plan_contract_schema_version=shardloom.traditional_analytics.output_plan.v1`,
`output_plan_status_vocabulary`, `output_plan_status`, `output_plan_id`, `output_plan_digest`,
`output_format`, `output_location`, `output_schema_digest`, `output_partitioning`,
`output_compression`, `output_encoding`, `output_write_mode`, `output_plan_reuse_allowed`,
`output_metadata_preservation_status`, `output_materialization_required`,
`output_plan_reuse_hit`, `output_plan_reuse_reason`, `output_plan_millis`, `output_write_millis`,
`result_replay_verified`, `output_native_io_certificate_status`, `sink_artifact_ref`,
`sink_artifact_digest`, `output_plan_fallback_attempted=false`,
`output_plan_external_engine_invoked=false`, `output_plan_claim_gate_status=not_claim_grade`, and
`output_plan_claim_boundary`. Local Vortex result-sink rows can report
`output_plan_status=output_plan_supported` only when write/replay evidence is present; rows without
an output request remain explicit `not_needed` posture.
`GAR-IOREUSE-1D` adds the first `io_reuse_and_fanout` benchmark matrix to the JSON/Markdown
artifact. It lists the required fanout cases for CSV -> Parquet/JSONL/Vortex, Parquet -> CSV/
Vortex, JSONL -> Parquet/Vortex, generated source -> CSV/Parquet/Vortex, and prepared Vortex ->
multiple output formats. Current rows are deterministic `fanout_status=report_only` blockers with
`fanout_output_count=0`, `fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status=not_claim_grade`; local Vortex result-sink proof remains visible as
`currently_proven_output_formats=vortex_result_sink_when_requested` when the artifact was generated
with result-sink proof.
`GAR-IOREUSE-1E` adds the first cache invalidation/fingerprint contract to the JSON/Markdown
artifact with `cache_invalidation_contract_schema_version`, `cache_invalidation_status`,
`cache_invalidation_layer_scope`, `source_content_digest`, `source_mtime`, `source_size`,
`object_etag`, `manifest_version`, `plan_digest`, `cache_valid`, `invalidation_reason`,
`cache_invalidation_fallback_attempted=false`,
`cache_invalidation_external_engine_invoked=false`,
`cache_invalidation_claim_gate_status=not_claim_grade`, and
`cache_invalidation_secret_redaction_status`. `cache_valid=true` means current-row local
fingerprints are internally consistent; it is not a persistent cache hit, hidden fast mode, or
performance evidence.

The scoped direct-transient lane can be run explicitly:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines shardloom-direct-transient,pandas --formats csv --scenario "selective filter" --rows 10000 --iterations 1
```

That lane reports `execution_mode=direct_compatibility_transient`,
`direct_transient_execution=true`, `vortex_file_written=false`, `vortex_file_read=false`,
`upstream_vortex_scan_called=false`, `runtime_execution_certificate_status=certified`, and
`vortex_native_claim_allowed=false`. Adjacent formats, operators, result sinks, replay paths, and
SQL/DataFrame access remain unsupported unless later phase-plan slices add evidence.

Prepared/native rows also expose provider-admission evidence. The current local Vortex file
scan/source boundary is admitted as the Vortex-native provider surface; residual scenario operators
remain explicitly ShardLoom-native temporary/materialized where that is still the implementation.
They also emit `source_backed_scan_*` evidence fields for the scoped local prepared/native scan:
schema version, report ID, provider kind/surface/scope, source roles, source refs/digests,
projected columns, pushdown flags, Native I/O certificate status, materialization boundary rows,
residual executor, claim gate, and `fallback_attempted=false` /
`external_engine_invoked=false`. This makes benchmark rows easier to interpret without relabeling
residual-native work as encoded-native or claim-grade performance evidence.
`GAR-PERF-2C` adds the uniform `scan_pushdown_*` contract across prepared/native scenario families.
Rows now separately report filter, projection, and limit/slice pushdown, distinguish filter-only
columns from output columns, and emit deterministic blockers when expressions cannot be lowered
safely into the Vortex Scan/source-backed boundary. Current limit/slice pushdown remains blocked for
order-sensitive or grouped residual limit-like scenarios.
Those fields remain source/provider evidence only; they do not create an encoded-native operator,
SQL/DataFrame, object-store/lakehouse, production, or performance claim.
`GAR-PERF-2B` tracks the evidence-aware logical optimizer follow-up. Future benchmark rows may link
to optimizer trace IDs and rule statuses for predicate/projection/slice pushdown, common
subplan/source-state reuse, expression simplification, constant folding, type coercion, join
ordering, and cardinality estimation. Optimizer traces should report before/after plan digests,
rewrite safety, `evidence_preserved=true`, no-fallback fields, correctness smoke refs for applied
rewrites, and claim gates. They are explainability evidence only, not lazy optimizer parity,
SQL/DataFrame runtime, or performance proof.
`GAR-IOREUSE-1` tracks the I/O reuse and cross-format fanout benchmark follow-up. Rows model the
workflow as `InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan ->
SinkArtifact`, not as matching input/output formats. `GAR-IOREUSE-1A` adds the first SourceState
contract to the benchmark JSON/Markdown artifact with
`source_state_contract_schema_version=shardloom.traditional_analytics.source_state.v1`,
`source_state_status_vocabulary`, `source_state_status`, `source_state_id`,
`source_state_digest`, `source_format`, `source_location`, `source_fingerprint_kind`,
`schema_digest`, `row_count_known`, `file_count`, `byte_size`, `partition_columns`,
`compression`, `source_state_reuse_allowed`, `source_discovery_millis`,
`schema_inference_millis`, `source_parse_millis`, `parse_decode_plan_digest`,
`source_state_reuse_hit`, `source_state_reuse_reason`,
`source_state_materialization_decode_boundary_ref`, `source_state_fallback_attempted=false`,
`source_state_external_engine_invoked=false`, `source_state_claim_gate_status=not_claim_grade`, and
`source_state_claim_boundary`. CSV, JSONL, Parquet, Arrow IPC, Avro, and ORC rows can all report a
SourceState posture. `GAR-IOREUSE-1B` adds the companion VortexPreparedState contract for local
prepared artifact refs/digests, preparation timing separation, source-state linkage, and scoped
reuse posture. `GAR-IOREUSE-1C` adds the companion OutputPlan contract for local Vortex result-sink
planning, metadata preservation posture, write/replay refs, and sink artifact identity. Planned
fanout families remain `io_reuse_and_fanout`, `source_state_reuse`,
`prepared_state_reuse`, `output_plan_reuse`, `cross_format_output`, and
`generated_source_output`. `GAR-IOREUSE-1D` adds report-only fanout rows for CSV input -> Parquet +
JSONL + Vortex outputs, Parquet input -> CSV + Vortex outputs, JSONL input -> Parquet + Vortex
outputs, generated source -> CSV + Parquet + Vortex outputs, and prepared Vortex -> multiple output
formats. Future rows must replace those blockers with runtime cross-format fanout, output-plan
reuse hits, fanout output count, no-fallback/no-external-engine fields, and claim gate status.
`GAR-IOREUSE-1E` adds cache invalidation/fingerprint rows for current local source/prepared/plan/
output posture. These rows are local workflow/evidence rows, not a performance leaderboard,
persistent cache, or cache-hit claim.
`GAR-PERF-2D` adds scoped compressed/encoded kernel registry evidence for selective-filter
prepared/native rows. Current rows classify bitpacked filter, sequence predicate, dictionary
equality/group-by, constant count/filter, sorted min/max pruning, and FSST/dictionary string
equality where available. Non-empty selective-filter rows admit the observed
`flag:fastlanes.bitpacked` and `value:vortex.sequence` reader-generated filter inputs; the other
initial rows remain deterministic blockers or not available. Rows report
`compressed_kernel_registry_pair_ids`, `compressed_kernel_registry_pair_statuses`,
`compressed_kernel_registry_kernel_admitted`, `compressed_kernel_registry_kernel_executed`,
`compressed_kernel_registry_canonicalization_required`, `compressed_kernel_registry_decoded`,
`compressed_kernel_registry_materialized`, and
`compressed_kernel_registry_encoded_native_claim_allowed=false`.
`GAR-PERF-2E` adds the scoped fused operator pipeline evidence contract. Prepared/native rows now
report `fused_pipeline_used`, `fused_operator_family`, `fused_pipeline_family_statuses`,
`intermediate_materialization_avoided`, row counts, fused/unfused correctness digests,
`fused_pipeline_correctness_digest_match`, materialization/decode status, deterministic blocker
fields, and no-fallback fields. Current executed families are filter/projection/limit,
filter/aggregate through the selective-filter selection-vector metric path, and top-k/projection.
Filter/group-by remains explicitly blocked until a scoped filtered grouped scenario exists. Fusion
remains residual-native unless later representation evidence proves encoded-native execution;
`fused_pipeline_encoded_native_claim_allowed=false` remains required.
For `selective filter`, prepared/native rows also emit `encoded_predicate_provider_*` fields. When
the scoped filter-column probe observes the admitted local encodings, those fields now report
`encoded_predicate_provider_status=reader_generated_filter_column_batches_and_selected_metric_aggregation_admitted`,
`encoded_predicate_provider_filter_only_columns=flag,value`,
`encoded_predicate_provider_projected_output_columns=metric`, and
`encoded_predicate_provider_encoded_native_claim_allowed=false`. The GAR-0026-U/GAR-0026-V provider
fields separate the admitted filter-column probe from the selected metric aggregation. The scoped
filter-column probe reports
`encoded_predicate_provider_filter_column_probe_requested=true`,
`encoded_predicate_provider_filter_column_probe_reader_chunk_columns_observed=flag,value`,
`encoded_predicate_provider_filter_column_probe_reader_chunk_encoding_summary=flag:fastlanes.bitpacked,value:vortex.sequence`,
and `encoded_predicate_provider_filter_column_probe_data_decoded=false`.
The exported reader-generated conjunctive bridge now consumes those admitted kernel inputs with
`encoded_predicate_provider_conjunctive_bridge_status=intersected_selection_vectors`,
`encoded_predicate_provider_reader_backed_bridge_status=bridge_consumed_reader_generated_filter_column_kernel_inputs_and_metric_selection_vector`,
`encoded_predicate_provider_kernel_input_lowering_status=reader_generated_encoded_kernel_inputs_admitted`,
and `encoded_predicate_provider_kernel_input_count=2`. The selected metric aggregation then reports
`encoded_predicate_provider_selected_metric_aggregation_status=selection_vector_consumed`,
`encoded_predicate_provider_selected_metric_selection_vector_consumed=true`, selected row count,
selected metric sum, scan split count, and decode/materialization boundary fields. Admitted empty
selections report a consumed selection vector with selected row count `0`. Blocked encodings still
use deterministic residual/no-fallback paths. This records real filter-column selection-vector and
scoped selected-metric evidence, but it still does not permit an encoded-native or performance claim
because the metric aggregation remains residual-native and not a generalized encoded aggregation
kernel.

ShardLoom also exposes the scoped `traditional-analytics-vortex-batch-run` command. It runs a
comma-separated list of prepared/native scenarios against the same prepared `.vortex` artifacts in
one ShardLoom process and emits `shardloom.traditional_analytics.vortex_batch.v1` fields, including
`runner_kind=single_process_prepared_native_batch`,
`typed_envelope_preserved=true`, `process_startup_amortization_supported=true`,
`source_metadata_snapshot_status=per_batch_source_metadata_reused`,
`evidence_level=minimal_runtime|certified|full_replay`, per-scenario operator/source/Native I/O
fields, and `fallback_attempted=false` / `external_engine_invoked=false`. The per-batch source
metadata snapshot records fact/dimension/CDC
Vortex artifact sizes and digests once per command invocation and reuses that evidence for child
scenario reports instead of recomputing it per scenario. Hash-join and join-aggregate child
scenarios also share one per-batch dimension-label lookup state when both are present. That emits
`source_state_reuse_status=per_batch_dimension_label_state_reused`,
`source_state_reuse_consumer_count`, `source_state_recompute_avoided_count`, and
`source_state_prepare_micros` with
`source_state_prepare_timing_scope=batch_shared_pre_scenario`. Distinct-count and
high-cardinality string-group/distinct child scenarios share one per-batch category/metric grouped
state when both are present and emit
`source_state_reuse_status=per_batch_category_metric_state_reused` plus family-specific
`source_state_category_metric_*` fields such as
`source_state_category_metric_reuse_status`. The shared setup cost is explicit batch evidence and is
not hidden in per-scenario timings. Group-by aggregation and multi-key group-by child scenarios
share one per-batch `group_key,category,metric` grouped state when both are present and emit
`source_state_reuse_status=per_batch_group_category_metric_state_reused` plus family-specific
`source_state_group_category_metric_*` fields such as
`source_state_group_category_metric_reuse_status`. The Python comparative harness uses this command
for eligible prepared/native Vortex scenario groups, one batch process per format/iteration, and
reports `persistent_runner_status=single_process_batch_runner_supported` on those rows. That status
means the CLI process wall time is shared across the grouped rows; per-scenario
`scenario_compute_micros`, `vortex_scan_micros`, and optional
`computed_result_sink_write_micros` remain row-level evidence fields. This is a runtime support
slice for scoped local prepared/native process, source-metadata, dimension-label source-state,
category/metric source-state reuse, group/category/metric source-state reuse, ranked-metric
source-state reuse, selective-filter source-state reuse, dirty-input source-state reuse, and
date/null metric source-state reuse.
Evidence-level fields separate proof depth from execution mode: `minimal_runtime` omits
result-sink replay and stays `not_claim_grade`, `certified` emits normal certificates without replay
by default, and `full_replay` requires result-sink replay proof through `--write-result-vortex`.
Selective-filter plus filter/projection/limit child scenarios share one per-batch filtered
`id,value,metric` state when both are present and emit
`source_state_reuse_status=per_batch_selective_filter_state_reused` plus family-specific
`source_state_selective_filter_*` fields such as
`source_state_selective_filter_reuse_status`. Sort/top-k, top-N per group, and
row-number/window child scenarios share one
per-batch `group_key,id,metric` ranked state when multiple ranked consumers are present and emit
`source_state_reuse_status=per_batch_ranked_metric_state_reused` plus family-specific
`source_state_ranked_metric_*` fields such as `source_state_ranked_metric_reuse_status`.
Clean/cast/filter/write and malformed timestamp / dirty CSV child scenarios share one per-batch
`raw_event_time,dirty_numeric,dirty_flag` dirty-input cleanup state when both are present and emit
`source_state_reuse_status=per_batch_dirty_input_state_reused` plus family-specific
`source_state_dirty_input_*` fields such as `source_state_dirty_input_reuse_status`. It is not a
persistent daemon, hidden fast mode, performance claim, encoded-native claim, SQL/DataFrame claim,
object-store claim, or Spark-displacement claim. Partition-pruning plus null-heavy aggregate child
scenarios share one per-batch `event_date,metric,nullable_metric_00` date/null metric state when
both are present and emit
`source_state_reuse_status=per_batch_date_null_metric_state_reused` plus family-specific
`source_state_date_null_metric_*` fields such as
`source_state_date_null_metric_reuse_status`; this is scoped residual-native reuse evidence only.
GAR-PERF-1B adds the complete source-state coverage matrix at
`docs/architecture/source-state-reuse-coverage-matrix.md`. Batch evidence now also emits
`source_state_coverage_schema_version`,
`source_state_coverage_matrix_ref`, `source_state_coverage_status_vocabulary`,
`source_state_coverage_all_requested_scenarios_classified`, `source_state_coverage_matrix`, and
per-child `scenario_<slug>_source_state_coverage_*` fields. Coverage statuses are
`source-state-reused`, `source-state-not-needed`, `blocked-with-reason`, and
`unsupported-with-reason`. Rows also report
`source_state_digest_status=not_emitted_scoped_in_memory_source_state` because the current batch
source states are scoped in-process derived state. GAR-IOREUSE-1A adds a separate universal
SourceState identity/digest contract at the benchmark row level; it does not replace the
family-specific batch source-state fields or turn scoped reuse into output support, Vortex-native
execution, or a performance claim.
GAR-IOREUSE-1B adds a separate VortexPreparedState identity/digest/reuse-posture contract at the
benchmark row level. It does not replace prepared-artifact lifecycle fields and does not turn
prepared artifact reuse into output support, encoded-native execution, object-store/lakehouse
runtime, or a performance claim.
GAR-IOREUSE-1C adds a separate OutputPlan identity/digest/status contract at the benchmark row
level. It does not replace result-sink replay fields and does not turn local Vortex result-sink
proof into cross-format fanout, object-store/lakehouse runtime, table commit, production sink
support, or a performance claim.
GAR-IOREUSE-1D adds a separate fanout benchmark matrix. It does not execute multi-output fanout and
does not turn required fanout cases into supported outputs until future rows attach per-output
write/replay/correctness evidence.
GAR-IOREUSE-1E adds a separate cache invalidation/fingerprint matrix. It does not add a persistent
disk cache, daemon/service cache, distributed cache, object-store cache, hidden fast mode, or cache
performance claim.

`GAR-PERF-2F` adds a scoped in-process session-backed prepared/native batch lane for local artifacts.
Batch rows now expose `session_id`, explicit open/close/drop status, prepared-artifact
registry/cache counts, source-metadata cache counts, source-state cache/reuse counts,
`session_hidden_global_cache=false`, `session_daemon_or_service=false`,
`session_fallback_attempted=false`, `session_external_engine_invoked=false`, and
`session_claim_gate_status=fixture_smoke_only`. The session remains caller-owned and local; it is
not a public Python session API, daemon, remote server, hidden global cache, SQL/DataFrame runtime,
object-store/lakehouse runtime, production claim, or performance claim.

`GAR-PERF-2G` adds a scoped allocation/resource-profile evidence slice to the session-backed
prepared/native batch rows. The harness now propagates allocation profile status/scope, family
classification, allocation count/byte status, buffer-pool status/scope, buffer-reuse count/family
and blocker, peak RSS status, correctness/evidence-regression posture,
`unsafe_lifetime_shortcut_used=false`, `allocation_fallback_attempted=false`, and
`allocation_external_engine_invoked=false`. The current row values intentionally report
`allocation_count=not_available`, `allocation_bytes=not_available`, `peak_rss_delta=not_available`,
`buffer_pool_enabled=false`, and `buffer_reuse_count=0` until safe measurement and reuse exist.
This is resource-profile visibility only, not a speed or memory-efficiency claim.

`GAR-PERF-2H` tracks optimized build profiles and the PGO benchmark lane. The harness already records
`shardloom_build_profile`; future rows should also record build-profile kind, rustc/cargo versions,
target triple, target CPU policy, `target_cpu_native_enabled`, LTO status/mode, codegen units, PGO
status, PGO artifact/training workload refs, build reproducibility status, portable release artifact
status, benchmark-only build status, correctness digest, and claim gate. Planned lanes are
`release-lto`, `release-pgo`, and `release-native-benchmark`. `target-cpu=native` is benchmark-only,
not a portable release setting, and optimized build rows are not public performance claims.

### Website Evidence Snapshot

The static website benchmark page is generated from local smoke artifacts under
`target/shardloom-benchmark-evidence/`:

```powershell
python website\build_static_pages.py --benchmark-dir target\shardloom-benchmark-evidence
```

The generated `website/benchmarks.html` and
`website/assets/data/benchmark-evidence.json` preserve execution-mode separation for
`compatibility_import_certified`, `prepared_vortex`, and `native_vortex`; stage timing fields such
as source read, compatibility parse/import, Vortex write/reopen/scan, operator compute, result sink,
evidence render, and total runtime; `encoded_predicate_provider_*` rows where applicable;
`source_backed_scan_*` rows; materialization/decode evidence; and no-fallback fields. The page is a
claim-safe evidence surface only. It must not be read as performance proof, Spark replacement proof,
or production SQL/DataFrame/object-store/lakehouse/Foundry support.

The website also commits a benchmark publishing manifest under
`website/assets/benchmarks/latest/manifest.json` plus the corresponding
`website/assets/benchmarks/latest/benchmark-results.json`. The manifest records the selected
profile, expected lanes, available lanes, missing lanes, lane versions/reasons, environment
fingerprint, artifact paths, and `performance_claim_allowed=false`. Website rendering consumes this
committed artifact; it must not rediscover competitor availability from the environment that builds
or deploys the static site. Use `scripts/check_benchmark_environment.py` before producing a full
artifact and `scripts/check_benchmark_artifact_completeness.py` before publishing it. The runbook is
`docs/benchmarks/static-benchmark-publishing-runbook.md`.

`filter + projection + limit` now reports a scoped residual-native fused scan path for prepared/native
rows when filter/projection pushdown runs without full-table materialization. `group by aggregation`
now reports a scoped residual-native grouped scan path when projection pushdown over
`group_key`/`metric` feeds ShardLoom-native grouped residual state without full-table
materialization. `multi-key group by` now reports the same residual-native boundary for composite
`group_key`/`category` state after projection pushdown over `group_key`/`category`/`metric`.
`hash join` now scans projected dimension and fact columns into bounded ShardLoom-native dimension
state plus residual grouped join output. `join + aggregate` adds fact-side value filter pushdown and
residual grouped `(dim_label, category)` aggregation over projected fact/dimension scans.
`sort and top-k` scans projected `id`/`metric` columns into bounded ShardLoom-native global top-k
state.
`top-N per group` now scans projected `group_key`/`id`/`metric` columns into bounded
ShardLoom-native per-group ranking state. `row number window` uses the same projected scan boundary
with bounded rank-1 per-group state. `high-cardinality string group/distinct` scans projected
`category`/`metric` columns into ShardLoom-native string grouping state. `distinct count` scans only
the projected `category` column into ShardLoom-native distinct state. `null-heavy aggregate` scans
only projected `nullable_metric_00` values into ShardLoom-native null-skipping aggregate state.
`clean/cast/filter/write` scans only projected `raw_event_time`, `dirty_numeric`, and `dirty_flag`
values into ShardLoom-native cleanup/filter/aggregate state. `malformed timestamp / dirty CSV`
scans only projected `raw_event_time` and `dirty_numeric` values into ShardLoom-native
validation/parse/aggregate state. `nested JSON field scan` scans only projected `nested_payload`
values into ShardLoom-native generated-field extraction state. `small change over large base` scans
projected base `id`/`metric` values plus CDC delta `id`/`op`/`value`/`metric`/`effective_ts` values
into ShardLoom-native overlay state. `partition pruning` scans projected `event_date`/`metric`
columns with a Vortex date-range filter before ShardLoom-native residual scalar aggregation; it is
not an object-store partition-pruning, layout-pruning, or statistics-pruning claim. None of these
paths are encoded-native operator claims or broad CDC/table-transaction claims.
Prepared/native rows also emit an operator blocker matrix:
`operator_execution_class`, `operator_admission_status`, `operator_blocker_id`,
`operator_blocker_reason`, and `operator_encoded_native_claim_allowed`. Current
residual-native and materialized-temporary classes are never counted as
encoded-native operator execution.

The native `vortex-count-benchmark` microbenchmark also exposes
`native_vortex_admission_*` fields. The only admitted lane today is
`local_vortex_count_scalar`: local Vortex file scan, `CountAll`, and a typed scalar result. Its
claim boundary is fixture-smoke-only, so it can support the scoped local native Vortex lane report
but not universal native Vortex, SQL/DataFrame, object-store, sink/write, or performance claims.

When `--shardloom-result-sink` is enabled, prepared/native rows now pass
`--workspace <caller-owned-dir> --write-result-vortex` to
`traditional-analytics-vortex-run`. The row writes `result.vortex`, replays it,
emits result Native I/O certificate fields, and reports
`computed_result_sink_write_micros`/`result_sink_write_micros` separately from
`scenario_compute_micros`. If result-sink evidence is missing, prepared/native
claim promotion remains blocked through `result_sink_claim_gate_status`; the
operator timing is not relabeled as a broader claim-grade benchmark.

Process attribution is explicit. ShardLoom rows report `cli_process_wall_millis`
when the Python harness invokes the CLI. Per-scenario CLI rows derive
`python_harness_overhead_millis` from the outer harness timing. Eligible
prepared/native batch rows instead report `batch_cli_process_wall_millis`,
`batch_process_wall_shared=true`, and
`process_startup_attribution=single_process_batch_cli_wall_shared_across_scenarios`
so the shared process wall is not mistaken for per-scenario operator time. All rows report
`build_time_millis` separately and keep `build_time_excluded=true`. Prepared artifact setup is
reported as `preparation_millis` and `preparation_cli_process_wall_millis`; it
is not folded into startup/warmup or pure operator timing. The current decision
record is `docs/architecture/benchmark-persistent-runner-decision.md`.

The JSON artifact and Markdown report also include
`persistent_runner_admission_gate`. This is a report-only gate, not a runtime
feature flag. It requires any broader persistent runner or harness migration to
preserve per-run `shardloom.output.v2` typed envelopes, execution-mode
selection, Native I/O and operator-blocker evidence, materialization/decode
boundaries, result-sink replay evidence when enabled, deterministic unsupported
diagnostics, and row-level `fallback_attempted=false` /
`external_engine_invoked=false`. The scoped batch command satisfies that
evidence boundary for local prepared/native batch process reuse only. No hidden benchmark fast mode,
process-overhead claim, or performance claim is allowed
until claim-grade reruns pass.

The report also emits `format_preparation_matrix`. That matrix compares
ShardLoom compatibility preparation costs by source format: source read,
compatibility parse, compatibility-to-Vortex import, Vortex write/reopen/scan,
operator compute, optional result sink, and total runtime. It keeps
prepared/native Vortex query timing separate from compatibility preparation and
states `native_execution_format=vortex` so CSV, JSONL, Parquet, Arrow IPC, Avro,
and ORC are not mistaken for native execution formats.

Execution modes are explicit:

- `compatibility_import_certified`: compatibility source adapter -> Vortex import -> write/reopen -> compute -> optional result sink/evidence.
- `prepared_vortex`: one-time compatibility import per dataset/profile/format, then scenario timing from prepared Vortex artifacts.
- `native_vortex`: existing `.vortex` input -> Vortex-native scan/operator path.
- `direct_compatibility_transient`: scoped local CSV one-shot compatibility compute where evidence exists, otherwise deterministic unsupported; not a Vortex-native claim.
- `auto`: transparent selection only; the selected mode and reason must be reported.

The JSON artifact and Markdown report include
`execution_mode_attribution_contract`, which locks the interpretation surface for
each row. Every row must carry `requested_execution_mode`,
`selected_execution_mode`, `mode_selection_reason`, `execution_mode_family`,
`vortex_native_claim_allowed`, `compatibility_import_included`,
`vortex_prepare_included`, `vortex_write_reopen_included`,
`direct_transient_execution`, and `claim_gate_status`.

Every row also carries the stage timing fields
`source_read_millis`, `compatibility_parse_millis`,
`compatibility_to_vortex_import_millis`, `vortex_write_millis`,
`vortex_reopen_millis`, `vortex_scan_millis`,
`operator_compute_millis`, `result_sink_write_millis`,
`evidence_render_millis`, and `total_runtime_millis`. Unknown or
not-yet-isolated values stay present as `null`, `n/a`, or
`not_measured` rather than being omitted. In particular,
`compatibility_import_certified` rows time the ingest/stage/certification
workflow; do not read those rows as pure ShardLoom query-speed rows.

The CLI `runtime-report --format json` exposes the same field order as a
GAR-0018-A report-only introspection schema. It is useful for tooling that wants
to verify benchmark row shape before interpreting micro-metrics, but it does not
collect live profiles, emit traces, write profile artifacts, enable distributed
introspection, invoke external engines, or create performance claims.

Every row also carries the persistent-runner admission fields
`persistent_runner_status`, `process_startup_attribution`,
`python_harness_overhead_status`, `cli_process_wall_millis`,
`python_harness_overhead_millis`, `startup_warmup_millis`,
`build_time_millis`, `build_time_excluded`, `preparation_millis`,
`preparation_cli_process_wall_millis`, and
`preparation_included_in_timing`. These fields keep process lifecycle,
preparation, typed-envelope rendering/parsing, and scenario compute visible.

Every ShardLoom row carries work-avoidance evidence fields with the
`work_avoidance_` prefix. The status/value/reason triples make unknown work
avoidance explicit instead of silently treating unknown rows, segments, or
bytes avoided as zero.

Every ShardLoom coverage row also carries `vortex_source_split_admission_ref`,
pointing to the GAR-0042A source/split admission proof in
`vortex-api-inventory`. That reference classifies the scoped local fixture path
and keeps generalized Source/Split runtime, field-mask proof, predicate-ordering
proof, object-store, table/catalog, and write paths blocked until evidence
exists.

Coverage rows also carry `vortex_segment_extraction_admission_ref`, pointing to
the GAR-0003-A sparse segment extraction admission report in
`vortex-api-inventory`. Sparse patch/fill extraction is explicitly blocked
until correctness, execution-certificate, Native I/O, materialization/decode,
and no-fallback evidence exists.

Coverage rows also carry `materialization_policy_ref`, pointing to the
GAR-0003-B shared materialization/decode policy in `compute-capability-matrix`.
That policy distinguishes encoded-native, residual-native, materialized
temporary, and unsupported paths; materialized temporary rows cannot satisfy
encoded-native claims.

Coverage rows also carry `vortex_layout_device_managed_boundary_ref`, pointing
to the GAR-0042B boundary matrix. Layout/write, device/GPU, object-store, and
managed-platform comparison rows stay `not_claim_grade`; managed platforms are
comparison-only and cannot satisfy ShardLoom-native claims.

The canonical flow reference for these modes is
`docs/architecture/compute-engine-flow-reference.md`. The companion timing-attribution reference is
`docs/architecture/performance-attribution-and-execution-structure.md`.

Useful focused prepared/native checks:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines shardloom-vortex,pandas --formats csv,parquet --scenario "selective filter" --rows 10000 --iterations 3
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines shardloom-prepared-vortex,pandas --formats csv,jsonl,parquet,arrow-ipc,avro,orc --scenario "filter + projection + limit" --rows 10000 --iterations 1
```

ShardLoom's compatibility-format rows report `row_read=true` and
`data_materialized=true` because the benchmark source adapters parse or convert
local compatibility files before Vortex import. That is intentionally
conservative: native Vortex microbenchmark rows remain separate and expose the
available zero-decode/no-row-read primitive evidence.

ShardLoom resource sizing is automatic by default. The CLI derives applied
parallelism from local CPU availability and derives batch/partition sizing from
the source footprint plus the resource budget. `--memory-gb` and
`--max-parallelism` are optional caps for reproducible troubleshooting, not
required user tuning knobs.

Dask is sensitive to partitioning, scheduler choice, file count, and dataset
size. The harness records `--dask-blocksize` and `--dask-scheduler`; small
single-file CSV runs can make scheduler overhead dominate.

This benchmark is intentionally explicit about fairness parameters. Before
interpreting results, check row count, storage format, cache mode, timing scope,
Dask partitioning, Spark Java status, Spark default/tuned-local profile split,
ShardLoom build profile/feature gate, whether CSV/Parquet/JSONL/Arrow IPC/Avro/ORC/native
Vortex rows were included, the applied ShardLoom resource policy, and whether
object-store lanes were included.

## Setup

Use an isolated virtual environment. Do not add these packages to the Rust
workspace.

```powershell
python -m venv benchmarks\traditional_analytics\.venv
benchmarks\traditional_analytics\.venv\Scripts\python -m pip install -r benchmarks\traditional_analytics\requirements.txt
```

Avro fixture generation uses `fastavro` from the benchmark virtual environment
only. Rust runtime Avro coverage is feature-gated in `shardloom-vortex` through
Apache Arrow's `arrow-avro` crate.

Spark/PySpark also requires a local JDK. Install JDK 17 or newer, set
`JAVA_HOME`, and ensure `java` is on `PATH` before expecting Spark rows to run.
Without Java, the harness records Spark profiles as missing dependencies while
still running the other engines.

Spark rows are split into `spark-default` and `spark-local-tuned`. The default
profile uses `local[*]` plus Spark defaults, while the tuned profile caps
shuffle/default parallelism to the local CPU count and enables AQE. The `spark`
engine alias expands to both profiles. Each Spark profile starts and warms its
own Spark session immediately before its scenario rows, and the harness records
that startup/warmup time separately from per-scenario timings.

On Windows the harness also checks common Temurin/Eclipse Adoptium install
paths and will set `JAVA_HOME` for the benchmark process when it finds a local
JDK there.

## Run

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --rows 100000 --iterations 3 --formats csv,parquet --require-all-engines
```

`--require-all-engines` is strict for automation: it still writes JSON and
Markdown artifacts, but exits nonzero if an engine dependency is missing. Use the
default mode while bringing up local dependencies so partial results remain easy
to inspect.

For a fast smoke run:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --rows 10000 --iterations 1
```

Run the taxonomy-expanded local analytics suite:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --include-taxonomy-extra --rows 10000 --iterations 1
```

Run the selected local rerun preset across ShardLoom and local baselines:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --claim-readiness-rerun --dataset-profile narrow_fact_dim --rows 100000 --iterations 3
```

Run focused profile checks for supported ShardLoom taxonomy extras:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines shardloom,pandas,polars,duckdb --formats csv --scenario "top-N per group" --dataset-profile narrow_fact_dim --rows 100000 --iterations 3
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines shardloom,pandas,polars,duckdb --formats csv --scenario "high-cardinality string group/distinct" --dataset-profile high_cardinality_strings --rows 100000 --iterations 3
```

The preset keeps managed platforms out, enables ShardLoom result-sink evidence, includes
taxonomy-extra scenarios when no explicit scenario list is provided, and requires at least three
iterations. Scenario catalog `dataset_profiles` are enforced before engine execution, so an
incompatible scenario/profile pair records a coverage row instead of an engine-specific error or
accidental success.

P7.4.4 claim-readiness coverage is separated from timing. Each coverage row carries
`row_classification`, `support_status`, `claim_gate_status`,
`claim_grade_requirements_met`, `claim_grade_missing_evidence`,
`reproducible_benchmark_row`, and `timing_row_claim_grade`. ShardLoom rows can promote to
`claim_grade` only when at least three iterations produce a stable correctness digest and the row
contains benchmark/coverage refs, runtime execution certificate evidence, source Native I/O
certificate evidence, result Native I/O certificate evidence when result-sink proof is enabled,
materialization/decode boundary evidence, `fallback_attempted=false`, and
`external_engine_invoked=false`. External engines remain `external_baseline_only`, fixture lanes
remain `fixture_smoke_only`, and incompatible scenario/profile combinations remain deterministic
`blocked` or `unsupported` coverage rows.
ShardLoom rows additionally include `native_io_source_sink_coverage_ref` so source/sink support can
be interpreted against the explicit local, compatibility, object-store, catalog, streaming, and
external-adapter matrix instead of inferred from benchmark timing.

Run one engine or one scenario while troubleshooting:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines polars --scenario "group by aggregation" --rows 10000 --iterations 1
```

Run only ShardLoom's universal-I/O smoke row while troubleshooting its local
Vortex artifacts:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines shardloom --scenario "group by aggregation" --rows 10000 --iterations 1
```

Run the direct CLI workflow replay evidence when you want the per-command evidence
fields rather than comparative harness output:

```powershell
cargo run -p shardloom-cli --features vortex-traditional-analytics-benchmark -- traditional-analytics-run "selective filter" benchmarks\traditional_analytics\data\fact.csv benchmarks\traditional_analytics\data\dim.csv --workspace target\shardloom-traditional-replay --input-format csv --verify-native-replay --write-result-vortex --format json
```

Add result-sink replay evidence to the comparative ShardLoom row:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines shardloom --scenario "selective filter" --rows 10000 --iterations 1 --shardloom-result-sink
```

Run ShardLoom across all supported local compatibility formats:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines shardloom --scenario "csv/file ingest" --formats csv,jsonl,parquet,arrow-ipc,avro,orc --rows 10000 --iterations 1
```

Run the optional stress lane:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --include-stress --rows 1000000 --iterations 3
```

Run with a skewed local dataset profile:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --dataset-profile skewed_keys --include-taxonomy-extra --rows 100000 --iterations 3
```

Run a specific advanced fixture profile:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines pandas --formats csv --scenario "many-small-files scan" --dataset-profile many_small_files --rows 100000 --iterations 1
```

Artifacts are written to `benchmarks/traditional_analytics/results/` by default.
Generated data and result artifacts are intentionally ignored by git unless a
specific report is promoted into `docs/benchmarks/`.
