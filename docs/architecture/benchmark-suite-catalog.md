# Benchmark Suite Catalog And Source-Backed Matrix

## Purpose

This document records the CG-6.25 benchmark-suite architecture and the Priority 2.7 source-backed
correctness/benchmark matrix.

The benchmark direction is local-first and platform-neutral:

```text
more workload shapes
more dataset profiles
explicit fairness metadata
support/coverage separate from timing
ShardLoom evidence fields preserved
external engines comparison-only
fallback_attempted=false
```

## Executable Local Analytics Runner Status

The runnable local analytics harness now consumes the machine-readable scenario catalog at:

```text
benchmarks/common/scenario_catalog.json
```

The harness remains local-first and platform-neutral. It now records taxonomy and constitution
metadata on every result row:

```text
benchmark_suite
scenario_id
scenario_category
dataset_profile
engine_role
benchmark_constitution
```

The JSON artifact also emits a `coverage_table` separate from timing rows. Coverage rows classify
ShardLoom rows as `claim_grade`, `not_claim_grade`, `fixture_smoke_only`, `unsupported`, or
`blocked` based on visible evidence, classify supported ShardLoom rows with `support_status`, and
classify external local engines as `external_baseline_only`. The rows also expose
`row_classification`, `claim_gate_status`, `claim_grade_requirements_met`,
`claim_grade_missing_evidence`, `benchmark_row_ref`, `coverage_row_ref`,
`execution_certificate_status`, source/result Native I/O certificate status,
`materialization_decode_evidence_present`, and `timing_row_claim_grade` so raw timings cannot be
mistaken for promoted benchmark claims. When ShardLoom result-sink proof is enabled, rows also
expose `scenario_compute_millis`, `computed_result_sink_write_millis`,
`computed_result_sink_bytes`, and coverage-table `write_timing_present` so local write-path cost is
visible separately from scenario compute timing. The harness also exposes a
`--claim-readiness-rerun` preset for the selected P7.4.4 local comparative rerun: ShardLoom,
ShardLoom Vortex fixture smoke, pandas, Polars, DuckDB, and DataFusion; CSV and Parquet; taxonomy
extras; ShardLoom result-sink proof; no managed platforms; and at least three iterations. ShardLoom
timing rows cannot promote to claim-grade unless `reproducible_benchmark_row=true`, which requires
stable correctness digests and the configured reproducibility iteration floor. The harness also
uses each catalog scenario's `dataset_profiles` list to block incompatible scenario/profile pairs
before engine execution, preserving coverage rows without letting profile mismatch look like a
runtime failure or support claim.

The default local run remains conservative. `--include-taxonomy-extra` adds executable local
taxonomy scenarios for:

```text
filter + projection + limit
multi-key group by
join + aggregate
row number window
partition pruning
many-small-files scan
null-heavy aggregate
high-cardinality string group/distinct
top-N per group
clean/cast/filter/write
malformed timestamp / dirty CSV
small change over large base
nested JSON field scan
```

The runnable generator currently supports these dataset profiles:

```text
tiny_smoke
narrow_fact_dim
skewed_keys
high_cardinality_strings
wide_table
very_wide_table
null_heavy
many_small_files
few_large_files
partitioned_by_date
poorly_clustered
well_clustered
schema_drift
dirty_csv
nested_json
cdc_delta_overlay
```

Advanced profiles now emit local fixture sidecars where needed: split CSV/JSONL fact parts for
many-small/few-large file-shape coverage, malformed timestamp/numeric columns for dirty CSV
coverage, nested JSON payloads, and a deterministic CDC delta overlay. The dirty CSV
`clean/cast/filter/write` scenario now executes through pandas and ShardLoom's local Vortex
import/replay/result-sink path for ETL/write-path benchmark coverage. The local `partition pruning`
scenario also executes through ShardLoom's local Vortex path for generated `event_date` fixture
coverage. The local `many-small-files scan`, `null-heavy aggregate`,
`malformed timestamp / dirty CSV`, `nested JSON field scan`, and `small change over large base`
scenarios execute through that same path for generated split-file, nullable-metric, dirty-column,
nested-payload, and explicit CDC-overlay fixture coverage. Remaining advanced rows are local fixture
or deterministic unsupported/blocked coverage only. They do not promote general incremental-state,
general JSON execution, object-store multi-file, object-store/table partition pruning, or
performance claims. P7.4.4 is closed for the local taxonomy/claim-readiness scope once the selected
`--claim-readiness-rerun` artifact produces separate coverage/timing rows with claim-grade versus
not-claim-grade classification; broader table/catalog/object-store/runtime claims remain blocked
outside this benchmark closeout.

## P7.4.8/P7.4.9 Execution-Mode Attribution Follow-Up

The next benchmark correction is structural rather than a new architecture category. The harness
must make execution mode and timing scope explicit so readers do not compare a certified
compatibility ingest/stage workflow against direct CSV/Parquet local baselines as if both were pure
query compute.

Use `docs/architecture/compute-engine-flow-reference.md` as the canonical flow reference for
execution-mode semantics. Use
`docs/architecture/performance-attribution-and-execution-structure.md` for the companion timing
vocabulary and stage-attribution fields. Use
`docs/architecture/compute-engine-flow-overhaul-review.md` for the repo-alignment gaps and P7.5
overhaul sequence.

Required execution modes:

```text
compatibility_import_certified
prepared_vortex
direct_compatibility_transient
native_vortex
auto
```

Required benchmark distinction:

```text
compatibility_import_certified rows include source parse/import, Vortex preparation, Vortex
write/reopen/scan, optional result-sink proof, evidence rendering, and process overhead.

prepared_vortex rows record preparation once per dataset/profile/format and start scenario timing
from prepared Vortex artifacts.

Prepared Vortex artifacts are now represented as lifecycle evidence, not just benchmark temp files:
rows should expose artifact refs, digests when generated, source Native I/O certificate status,
reuse eligibility, lifecycle status, workspace or owner, and cleanup policy separately from query
timing.

Prepared/native Vortex result sinks are opt-in and caller-owned. When enabled, rows must expose
`computed_result_sink_requested`, `computed_result_sink_written`,
`computed_result_sink_replay_verified`, `computed_result_sink_write_micros`,
`computed_result_sink_native_io_certificate_status`, `result_sink_claim_gate_status`,
`commit_state`, and `rollback_cleanup_status`. Missing result-sink replay or certificate evidence
must block claim-grade promotion rather than silently omitting the sink proof.

shardloom-vortex, shardloom-prepared-vortex, and shardloom-prepare-batch rows are reported under
requested source-format rows such as CSV, JSONL, Parquet, Arrow IPC, Avro, or ORC. They do not add a
synthetic standalone `.vortex` report format row; preparation metadata records the Vortex artifact
boundary. The shardloom-prepare-batch lane is the scoped single-process
`compatibility_import_certified -> prepared_vortex batch` route and emits `prepare_batch_*` adapter
timing/evidence fields without treating preparation as child query timing.
HOTPATH-9 adds prepare/batch workspace-manifest dependency and partial-repair guard evidence through
`prepare_batch_prepared_state_dependency_*` and
`prepare_batch_prepared_state_partial_repair_*` fields. A manifest hit requires source, policy,
source-admission packet, prepared-artifact, manifest-digest, and no-fallback dependencies to match.
Any miss reports the changed dependency role and forces full reprepare; partial repair/regeneration
and stale segment reuse remain false until separately admitted with replay/certificate proof.

native_vortex rows start from existing `.vortex` input and are the cleanest future ShardLoom
performance lane once operator coverage matures. Current native rows can still use temporary
ShardLoom operator paths unless their representation-transition, materialization/decode,
provider-admission, and certificate evidence prove encoded/native execution.

direct_compatibility_transient rows are small one-shot local adapter compatibility rows where
explicit evidence exists and are not Vortex-native claims. Current comparative harness coverage is
limited to scoped CSV/JSONL and feature-gated Parquet/Arrow IPC/Avro/ORC selective-filter and
filter/projection/limit smoke rows.

External-engine rows remain benchmark baseline comparisons only. They cannot satisfy ShardLoom
execution certificates, Native I/O certificates, Vortex-native claim gates, or no-fallback proof,
and they must never run unsupported ShardLoom work as fallback.

`shardloom-prepare-batch` is now a required ShardLoom lane for `full_local`,
`full_local_plus_spark`, and `extended_local` published profiles. The current publication target is
`full_local`, which does not require Spark/PySpark. The historical/explicit
`full_local_plus_spark` profile still requires the baseline PySpark module lane `pyspark` plus the
split Spark-profile baselines `spark-default` and `spark-local-tuned`; missing PySpark/JDK setup
fails that explicit profile instead of being treated as an optional lane. The completeness validator checks that the promoted
artifact covers every profile-required format, scenario, and baseline lane and that the
single-process prepare/batch route appears as row evidence instead of being omitted from the public
benchmark bundle.
```

Public benchmark presentation must add a route-runtime layer on top of those internal modes. Every
published row should expose:

```text
route_runtime_status=scoped_runtime_supported|feature_gated|fixture_smoke_only|unsupported|external_baseline_only
route_lane_id
route_display_name
start_state
end_state
includes_preparation
includes_query
includes_output
includes_evidence
route_comparable_to_external_end_to_end
performance_claim_allowed=false
production_claim_allowed=false
spark_replacement_claim_allowed=false
```

Use route lanes for end-to-end comparison and stage attribution for explanation. Public route labels
are:

```text
ShardLoom Cold Certified Route
ShardLoom Prepare-Once First Query
ShardLoom Prepare-Once Batch
ShardLoom Warm Prepared Query
ShardLoom Native Vortex Query
ShardLoom Direct Transient Route
External Baseline End-to-End
```

The internal `shardloom` lane is specifically `ShardLoom Cold Certified Route`; it must not be
rendered as the generic ShardLoom runtime. `ShardLoom Warm Prepared Query` and
`ShardLoom Native Vortex Query` are valid runtime evidence, but their start states are
`VortexPreparedState` and `Vortex`, so they are not raw-source end-to-end comparisons unless that
boundary is shown. `claim_gate_status=claim_grade` remains an evidence-quality signal only. Runtime
support, evidence quality, performance claims, production claims, and Spark-replacement claims must
be separate fields and separate page cues. Unsupported counts must distinguish ShardLoom route gaps
from external baseline limitations, for example `ShardLoom unsupported rows: 0` and
`External baseline unsupported rows: 6`.

Required row fields include requested/selected execution mode, mode-selection reason, preparation
timing, prepared artifact refs/digests, source read/parse/import timing, Vortex write/reopen/scan
timing, operator compute timing, result-sink write timing, evidence-rendering timing,
route-shape stratification fields, source-to-Vortex-array guard fields, representation-transition
summary, encoded-native status, fusion status, Scan API status, persistent-runner/process-overhead
status, no-fallback status, and external-engine invocation status. Unknown or not-yet-isolated
fields must be represented explicitly instead of silently omitted.

HOTPATH-1/HOTPATH-5 adds `route_shape_stratification_*` and
`source_to_vortex_array_guard_*` as required artifact fields. Route-shape metadata exists to
interpret route totals. Source-to-array guard metadata keeps `vortex_array_build_*` and
`exclusive_source_to_vortex_array_*` as exclusive substage evidence while preserving
`compatibility_to_vortex_import_*` as the inclusive import bundle.

The traditional analytics JSON artifact and Markdown report now include
`execution_mode_attribution_contract`. That contract lists the canonical mode
vocabulary, the required execution-mode fields, and the required stage timing
fields beside the measurements. The harness validates the contract before
writing a report, so downstream readers can rely on field presence rather than
inferring timing scope from prose. If `requested_execution_mode=auto`, the row
must still preserve the selected mode and reason.

`GAR-PERF-2A` adds the scoped prepared/native batch evidence-level row contract. Batch rows expose
`evidence_level=minimal_runtime|certified|full_replay` beside `execution_mode` so readers can
separate proof overhead from runtime path. `minimal_runtime` preserves `fallback_attempted=false`,
`external_engine_invoked=false`, and `claim_gate_status=not_claim_grade`; `certified` emits normal
certificates without replay by default; `full_replay` requires result-sink replay proof. Future
Python, API, and broader benchmark rows should reuse the same contract only where evidence exists.

`GAR-PERF-2B` adds the report-only evidence-aware logical optimizer contract. ShardLoom benchmark
rows now reference optimizer traces with optimizer trace ID, registry version, rule statuses,
report-only before/after plan-digest placeholders, rewrite safety, evidence preservation,
materialization boundary preservation, cardinality estimation status, correctness smoke refs,
no-fallback fields, and claim gate. Optimizer trace refs are explainability evidence only unless a
future applied rewrite has correctness smoke and evidence-preserving before/after digests.

`GAR-PERF-2C` adds the Vortex Scan API pushdown completion contract. Prepared/native rows classify
filter, projection, and limit/slice pushdown separately, name filter-only columns and output
columns, preserve materialization/decode fields, and emit deterministic blockers for unsupported
dimensions. Current limit/slice pushdown remains blocked for order-sensitive or grouped residual
limit-like scenarios. This is scan/source-boundary evidence only; it must not be rendered as
encoded-native operator execution or public performance proof.

`GAR-PERF-2D` adds the scoped compressed/encoded kernel registry contract. Prepared/native
benchmark rows now expose aggregate `compressed_kernel_registry_*` fields for
encoding IDs, logical dtypes, physical encodings, operator families, admitted/executed flags,
canonicalization, decode, materialization, selection-vector behavior, input rows,
selected rows, operator-kernel timing, decoded-reference timing, shape classes, specialization
profiles, focused microbenchmark refs, promotion statuses, decoded-reference correctness digests,
validity semantics, unsupported reasons, encoded-native claim status, no-fallback status, and a
claim gate. Unsupported encoding/operator pairs are visible deterministic blockers or not-available
rows.

`GAR-PERF-2E` adds the scoped fused operator pipeline contract. Prepared/native rows now expose
`fused_pipeline_used`, `fused_operator_family`, `fused_pipeline_family_statuses`,
`intermediate_materialization_avoided`, `rows_scanned`, `rows_selected`, `rows_output`,
filter/projection columns, selection-vector posture, fused and unfused correctness digest fields,
materialization/decode fields, no-fallback status, deterministic blocker fields, and a claim gate.
Executed scoped families are filter/projection/limit, filter/aggregate through selective-filter
selection vectors, and top-k/projection. Filter/group-by remains blocked with
`gar-perf-2e.filter_group_by_filter_absent` until a filtered grouped scenario exists. Fusion rows are
local residual-native runtime evidence only unless later end-to-end representation-state
certificates prove encoded-native execution.

`GAR-PERF-2F` adds the scoped in-process session-backed prepared/native batch lane. The
`traditional-analytics-vortex-batch-run` rows now expose `session_id`, explicit open/close/drop
status, prepared-artifact registry/cache counts, source-metadata cache counts, source-state reuse
counts, `session_hidden_global_cache=false`, `session_daemon_or_service=false`,
`session_fallback_attempted=false`, `session_external_engine_invoked=false`, and
`session_claim_gate_status=fixture_smoke_only`. This is local-artifact-only session evidence. It
does not authorize a public Python session API, daemon, remote server, hidden fast mode, SQL/DataFrame
runtime, object-store/lakehouse runtime, production claim, or performance claim.

`GAR-PERF-2G` adds the scoped allocation/resource-profile evidence contract for the prepared/native
batch lane. Current rows expose allocation profile status/scope, family classification, allocation
count/byte status, buffer-pool enabled/scope, buffer-reuse count/family and blocker, peak RSS
status, correctness/evidence-regression posture, `unsafe_lifetime_shortcut_used=false`,
`allocation_fallback_attempted=false`, `allocation_external_engine_invoked=false`, and
`allocation_claim_gate_status`. The current slice reports allocation counts, allocation bytes, and
peak RSS as `not_available`, keeps `buffer_pool_enabled=false`, and keeps reuse count at `0` until
safe measurement and reuse are implemented. Buffer evidence must not be rendered as speed or
memory-efficiency proof.

`GAR-RUNTIME-IMPL-5F` closes the prepared/native Vortex lifecycle backstop inside the existing
prepared batch path. Per-scenario rows now expose `prepared_native_vortex_lifecycle_*` fields for
artifact refs/digests, scan/pushdown status, materialization/decode status, optional result-sink
replay, Native I/O certificate status, `prepared_native_vortex_lifecycle_no_standalone_lane=true`,
`prepared_native_vortex_lifecycle_fallback_attempted=false`, and
`prepared_native_vortex_lifecycle_external_engine_invoked=false`. The combined prepare/batch route
also exposes `prepare_batch_lifecycle_*` rollups for preparation, write/reopen, scan, pushdown,
materialization/decode, output, and claim boundary. These fields prove lifecycle visibility for the
local Vortex fixture bytes in the normal Vortex route; they do not create object-store/table,
distributed, SQL/DataFrame, production, performance, or encoded-native claims.

`GAR-PERF-2H` adds the optimized build-profile and PGO benchmark lane. Benchmark rows expand the
existing `shardloom_build_profile` fairness field with `build_profile_kind`, rustc/cargo version,
target triple, target CPU policy, `target_cpu_native_enabled`, LTO status/mode, codegen units, PGO
status, profile-generate/profile-use status, PGO artifact/training refs, build reproducibility
status, portable-release-artifact status, benchmark-only-build status, build-profile correctness
digest, no-fallback fields, and claim gate. `release-lto` is portable ThinLTO,
`release-pgo` is report-only unless `SHARDLOOM_PGO_PROFILE` points to a merged profile, and
`release-native-benchmark` is host-native benchmark-only. `target-cpu=native` is not portable
release evidence, and optimized profile rows must not be rendered as performance claims.

`GAR-PERF-2I` adds first-class native microbenchmark suite rows. Native microbenchmark rows stay
separate from traditional compatibility-file rows, prepared/native end-to-end rows, and external
baseline rows. The row families are Vortex scan-only, filter predicate-only, projection-only,
group-by kernel, hash-join kernel, top-k, result-sink write, and evidence-render. Implemented
families emit smoke-supported subsystem evidence; HOTPATH-11 adds an
`operator-microkernel-benchmark` row for dictionary group-by plus bitpacked, sequence, and constant
predicate-pair timings. Missing isolated primitives emit deterministic blocked rows rather than
disappearing from the report. Each row exposes
`benchmark_category=native_microbenchmark`, primitive family, subsystem, optimization question,
support status, row counts, decode/materialization status, `fallback_attempted=false`,
`external_engine_invoked=false`, `claim_gate_status`, and an unsupported reason where applicable.

`GAR-IOREUSE-1` adds the I/O reuse and cross-format fanout benchmark bundle. Current rows already
emit SourceState, VortexPreparedState, OutputPlan, report-only fanout benchmark, cache
invalidation/fingerprint, and evidence-safe reuse-level contracts for the stable path
`InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan ->
SinkArtifact`; future runtime fanout rows must continue using that path and must not couple input
and output formats. Benchmark families are `io_reuse_and_fanout`, `source_state_reuse`,
`prepared_state_reuse`, `output_plan_reuse`, `cross_format_output`, and
`generated_source_output`. Current report-only fanout cases include CSV input -> Parquet + JSONL +
Vortex outputs, Parquet input -> CSV + Vortex outputs, JSONL input -> Parquet + Vortex outputs,
generated source -> CSV + Parquet + Vortex outputs, and prepared Vortex -> multiple output formats.

Required future fanout metrics are `source_discovery_millis`, `schema_inference_millis`,
`source_parse_millis`, `vortex_prepare_millis`, `operator_compute_millis`, `output_plan_millis`,
`output_write_millis`, `output_replay_millis`, `total_runtime_millis`,
`source_state_reuse_hit`, `prepared_state_reuse_hit`, `output_plan_reuse_hit`,
`fanout_output_count`, `fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status`. SourceState, VortexPreparedState, OutputPlan, and fanout matrix rows already
expose their scoped subsets; future runtime fanout rows must separate one-shot speed from
reuse/fanout timing and cannot mark any sink supported without output replay/evidence proof.

Current cache invalidation rows expose `source_fingerprint_kind`, `source_content_digest`,
`source_mtime`, `source_size`, `object_etag`, `manifest_version`, `schema_digest`, `plan_digest`,
`output_plan_digest`, `cache_valid`, `invalidation_reason`, no-fallback/no-external-engine fields,
credential-redaction status, and `claim_gate_status=not_claim_grade`. These rows are local fingerprint
posture only; they do not add a persistent cache, hidden fast mode, object-store cache, or cache
performance claim.

Current evidence-safe reuse-level rows expose the
`shardloom.traditional_analytics.evidence_safe_reuse_levels.v1` contract and classify
`discovery_reuse`, `schema_reuse`, `parse_plan_reuse`, `prepared_vortex_reuse`,
`operator_source_state_reuse`, `output_plan_reuse`, and `result_replay_reuse` independently from
execution mode, evidence level, output format, and claim gate. Reuse hits or misses remain
`not_claim_grade` visibility evidence only.

The cold ingestion/preparation research carry-forward adds schema-backed benchmark families for
`cold_lane_attribution`, `vortex_native_preparation`, `differential_preparation`, `capillary_io`,
`ingress_triage`, `cold_layout_advisor`, and `cold_copy_budget`. These families must preserve the
existing execution-mode timing split and remain `not_claim_grade` until they attach correctness,
Native I/O, benchmark, materialization/decode, no-fallback, and claim-gate evidence. They are
tracked by `GAR-IOREUSE-1H` through `GAR-IOREUSE-1L` plus adjacent `GAR-PERF-2J` and
`GAR-PERF-2K` and funnel through `vortex_ingest`; they do not authorize hidden warm-cache
interpretation, persistent cache claims, object-store/table runtime, or performance claims.

`runtime-report --format json` now mirrors this timing vocabulary as the
GAR-0018-A report-only runtime-introspection schema. That command is an
interpretation aid for local benchmark rows only: it reports the stage-timing
field order and deterministic blockers for live profiling, distributed runtime
introspection, profiler backends, trace backends, metrics exporters, profile
artifacts, and debug bundles. It does not collect profiles, emit traces, invoke
external engines, or upgrade benchmark rows into performance claims.

Prepared/native rows also carry the operator blocker matrix:
`operator_execution_class`, `operator_admission_status`, `operator_blocker_id`,
`operator_blocker_reason`, and `operator_encoded_native_claim_allowed`. Valid
classes are `encoded_native`, `residual_native`, `materialized_temporary`, and
`unsupported`; only `encoded_native` can support an encoded-native operator
claim.
The `selective filter` prepared/native row also carries
`encoded_predicate_provider_*` fields. The current status is
`blocked_until_reader_generated_kernel_input_certificate` for CSV/JSON prepared
rows whose reopened Vortex reader exposes primitive `flag,value` chunks. Scoped
encoded-kernel and selection-vector-backed aggregation proofs remain fixture
evidence only until the reader-generated chunks have admitted kernel-input
certificates. When that certificate exists, a scoped local scan may project real
`flag,value` reader chunks without decode/materialization, lower admitted
encodings into ShardLoom-owned encoded kernel inputs, intersect their selection
vectors, and consume the admitted selection vector for scoped
`row_count` and `metric_sum` evidence. GAR-0026-S adds the bridge contract,
GAR-0026-T adds filter-column probe evidence, GAR-0026-U adds the scoped
encoding-specific kernel-input lowering, and GAR-0026-V adds selected metric
selection-vector consumption. The row still sets
`encoded_predicate_provider_operator_execution_class=residual_native` and
`encoded_predicate_provider_encoded_native_claim_allowed=false` because this is
scoped residual-native metric aggregation, not a generalized encoded aggregation
kernel. Unsupported or changed encodings must remain deterministic no-fallback
diagnostics, not hidden decode or external-engine execution.
The current scoped `filter + projection + limit` prepared/native row is a
residual-native fused scan path: Vortex scan filter/projection pushdown and
bounded top-N state avoid full fact-table materialization, but the row still
sets `operator_encoded_native_claim_allowed=false`. The scoped `group by
aggregation` prepared/native row now also uses Vortex scan projection pushdown
over `group_key`/`metric` and ShardLoom-native grouped residual state without
full fact-table materialization; it is likewise not an encoded-native operator
claim. The scoped `multi-key group by` prepared/native row extends that
residual-native pattern to composite `group_key`/`category` state after
projection pushdown over `group_key`/`category`/`metric`. The scoped `hash join`
prepared/native row scans projected dimension and fact columns into bounded
ShardLoom-native dimension state plus residual grouped join output without
full fact-table materialization. The scoped `join + aggregate` prepared/native
row adds fact-side value filter pushdown and residual grouped `(dim_label,
category)` aggregation over projected fact/dimension scans without full
fact-table materialization. The scoped `top-N per group` prepared/native row
scans projected `group_key`/`id`/`metric` columns into bounded ShardLoom-native
per-group ranking state without full fact-table materialization. The scoped
`sort and top-k` prepared/native row scans projected `id`/`metric` columns into
bounded ShardLoom-native global top-k state without full fact-table
materialization. The scoped `row number window` prepared/native row uses the
same projected scan boundary with bounded rank-1 per-group state. The scoped
`high-cardinality string group/distinct` prepared/native row scans projected
`category`/`metric` columns into ShardLoom-native string grouping state without
full fact-table materialization. The scoped `partition pruning`
prepared/native row scans projected `event_date`/`metric` columns with a Vortex
date-range filter and residual scalar aggregation; it is local date-range scan
evidence, not an object-store partition-pruning, layout-pruning, or
statistics-pruning claim.
`compute-capability-matrix` exposes the same class vocabulary and per-row
operator class/blocker fields so CLI capability discovery matches benchmark
evidence.
`cpu-specialization-plan` now contributes host CPU feature labels and a
filter/encoded vector-kernel admission status to the same evidence posture.
The admission is diagnostic-only until correctness and benchmark evidence are
attached; it is not a SIMD-dispatch or performance-claim benchmark row.

GAR-FLOW-2G keeps process attribution explicit while letting eligible prepared/native Vortex rows
use the scoped batch command. ShardLoom rows must report `cli_process_wall_millis`,
`build_time_millis`, `build_time_excluded`, `preparation_cli_process_wall_millis`, and
`persistent_runner_status` where feasible. Per-scenario CLI rows report derived
`python_harness_overhead_millis`; batch rows report `batch_cli_process_wall_millis`,
`batch_process_wall_shared=true`, and `batch_process_startup_attribution` instead of allocating
shared CLI wall time to a single scenario. Build time, prepared-artifact setup, and batch process
wall time are not pure query/operator timing.

GAR-FLOW-2C adds a report-only `persistent_runner_admission_gate` to the JSON artifact and
Markdown report. GAR-FLOW-2F adds `traditional-analytics-vortex-batch-run`, and GAR-FLOW-2G wires
the Python harness to consume it for eligible prepared/native groups. Those rows may emit
`persistent_runner_status=single_process_batch_runner_supported` for scoped single-process
prepared/native batch runs only; other ShardLoom rows keep
`persistent_runner_status=process_per_scenario_attributed_not_reduced`. Any broader persistent
runner must preserve
per-run typed envelopes, execution-mode fields, Native I/O refs, operator blocker fields,
materialization/decode boundaries, result-sink replay evidence, deterministic unsupported
diagnostics, and `fallback_attempted=false` / `external_engine_invoked=false`. No hidden runner,
daemon, service, or process-overhead claim is admitted from the benchmark artifact alone.

GAR-FLOW-2H adds per-batch source metadata reuse inside
`traditional-analytics-vortex-batch-run`. The command computes one fact/dimension/CDC Vortex source
metadata snapshot per invocation, reuses artifact size/digest evidence across child scenarios, and
emits `source_metadata_snapshot_*` fields. This is scoped runtime plumbing for prepared/native
batch evidence, not a public performance, encoded-native, object-store, SQL/DataFrame, production,
or Spark-displacement claim.

GAR-FLOW-2I adds one scoped prepared/native source-state reuse path inside
`traditional-analytics-vortex-batch-run`. Hash-join and join-aggregate child scenarios share one
per-batch dimension-label lookup state when both are present. The batch envelope emits
`source_state_reuse_status=per_batch_dimension_label_state_reused`,
`source_state_reuse_consumer_count`, `source_state_recompute_avoided_count`,
`source_state_prepare_micros`, and
`source_state_prepare_timing_scope=batch_shared_session_open_only_deferred_family_build_reported_separately`.
The shared session-open setup timing is reported explicitly, with deferred family-build timing
reported separately; it is not a hidden fast mode or performance claim. Encoded-native operators,
SQL/DataFrame, object-store/lakehouse, production, and Spark-displacement claims remain blocked.

GAR-FLOW-2J adds a second scoped prepared/native source-state reuse path inside
`traditional-analytics-vortex-batch-run`. Distinct-count and high-cardinality
string-group/distinct child scenarios share one per-batch `category,metric` grouped state when both
are present. The batch envelope can emit
`source_state_reuse_status=per_batch_category_metric_state_reused`,
`source_state_category_metric_reuse_status`,
`source_state_category_metric_reuse_consumer_count`, and
`source_state_category_metric_recompute_avoided_count`. This is runtime-plumbing evidence for
local prepared/native batch rows only; it is not a performance, encoded-native, SQL/DataFrame,
object-store/lakehouse, production, or Spark-displacement claim.

GAR-FLOW-2K adds a third scoped prepared/native source-state reuse path inside
`traditional-analytics-vortex-batch-run`. Sort/top-k, top-N per group, and row-number/window child
scenarios share one per-batch `group_key,id,metric` ranked state when multiple ranked consumers are
present. The batch envelope can emit
`source_state_reuse_status=per_batch_ranked_metric_state_reused`,
`source_state_ranked_metric_reuse_status`, `source_state_ranked_metric_reuse_consumer_count`, and
`source_state_ranked_metric_recompute_avoided_count`. This is residual-native runtime-plumbing
evidence for local prepared/native batch rows only; it is not a distributed sort, encoded-native,
performance, SQL/DataFrame, object-store/lakehouse, production, or Spark-displacement claim.

GAR-FLOW-2L adds a fourth scoped prepared/native source-state reuse path inside
`traditional-analytics-vortex-batch-run`. Group-by aggregation and multi-key group-by child
scenarios share one per-batch `group_key,category,metric` grouped state when both are present. The
batch envelope can emit
`source_state_reuse_status=per_batch_group_category_metric_state_reused`,
`source_state_group_category_metric_reuse_status`,
`source_state_group_category_metric_reuse_consumer_count`, and
`source_state_group_category_metric_recompute_avoided_count`. This is residual-native
runtime-plumbing evidence for local prepared/native batch rows only; it is not encoded-native,
performance, SQL/DataFrame, object-store/lakehouse, production, or Spark-displacement claim.

GAR-FLOW-2M adds a fifth scoped prepared/native source-state reuse path inside
`traditional-analytics-vortex-batch-run`. Clean/cast/filter/write and malformed timestamp / dirty
CSV child scenarios share one per-batch `raw_event_time,dirty_numeric,dirty_flag` dirty-input
cleanup state when both are present. The batch envelope can emit
`source_state_reuse_status=per_batch_dirty_input_state_reused`,
`source_state_dirty_input_reuse_status`, `source_state_dirty_input_reuse_consumer_count`, and
`source_state_dirty_input_recompute_avoided_count`. This is residual-native runtime-plumbing
evidence for local prepared/native batch rows only; it is not encoded-native, performance,
SQL/DataFrame, object-store/lakehouse, production, or Spark-displacement claim.

GAR-FLOW-2N adds a sixth scoped prepared/native source-state reuse path inside
`traditional-analytics-vortex-batch-run`. Selective-filter and filter/projection/limit child
scenarios share one per-batch filtered `id,value,metric` state when both are present. The batch
envelope can emit `source_state_reuse_status=per_batch_selective_filter_state_reused`,
`source_state_selective_filter_reuse_status`,
`source_state_selective_filter_reuse_consumer_count`, and
`source_state_selective_filter_recompute_avoided_count`. Selective-filter rows retain scoped
`encoded_predicate_provider_*` evidence, but the shared-state aggregate is reported as
`batch_source_state_metric_aggregation_used` rather than encoded-native execution. This is
residual-native runtime-plumbing evidence for local prepared/native batch rows only; it is not an
encoded-native, performance, SQL/DataFrame, object-store/lakehouse, production, or
Spark-displacement claim.

GAR-PERF-1A refreshed the local prepared/native benchmark artifact after the source-state reuse
work. GAR-PERF-1B closes the source-state classification gap with
`docs/architecture/source-state-reuse-coverage-matrix.md`. Batch rows now emit
`source_state_coverage_schema_version`,
`source_state_coverage_matrix_ref`, `source_state_coverage_status_vocabulary`,
`source_state_coverage_all_requested_scenarios_classified`, `source_state_coverage_matrix`, and
per-child `scenario_<slug>_source_state_coverage_*` fields. The status vocabulary is
`source-state-reused`, `source-state-not-needed`, `blocked-with-reason`, and
`unsupported-with-reason`. The matrix also records
`source_state_digest_status=emitted_scoped_in_memory_source_state_digest`,
`source_state_digest_algorithm=fnv1a64`, `source_state_digest_scope`, and
`source_state_family_digests` for the scoped in-process derived source states; benchmark rows carry
the direct batch digest as `batch_source_state_digest` so the universal SourceState digest remains
unambiguous. GAR-IOREUSE-1A now adds separate universal local
SourceState IDs and digests to the benchmark row contract; those fields are SourceState posture
evidence and do not replace the family-specific batch coverage matrix. GAR-IOREUSE-1B adds
separate VortexPreparedState IDs, digests, artifact refs/digests, preparation timing separation,
source-state linkage, and reuse posture to the benchmark row contract; those fields do not imply
output support, encoded-native coverage, object-store/lakehouse runtime, or performance claims.
GAR-IOREUSE-1C adds separate OutputPlan IDs, digests, target format/schema/location posture,
metadata preservation, local Vortex write/replay refs, sink artifact refs/digests, and no-fallback
fields to the benchmark row contract; those fields do not imply cross-format fanout,
object-store/lakehouse runtime, table commit support, production sink support, or performance
claims.
GAR-IOREUSE-1D adds the report-only fanout benchmark matrix with required fanout case IDs, requested
outputs, current blocker IDs/reasons, timing/reuse columns, `fanout_output_count=0`,
`fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status=not_claim_grade`.
GAR-IOREUSE-1E adds the cache invalidation/fingerprint matrix with current local source/prepared/
plan/output fingerprint posture, source mtime/size fields, plan/output-plan digests, cache-valid
status, invalidation reason, object-store ETag posture, credential-redaction status, no-fallback fields,
and `claim_gate_status=not_claim_grade`.
GAR-IOREUSE-1F adds the evidence-safe reuse-level matrix with seven normalized reuse layers,
per-level status, hit/allowed/digest/blocker fields, invalidation reason linkage, evidence-level
visibility, `claim_grade_requirements_met=false`, no-fallback fields, and
`claim_gate_status=not_claim_grade`.
GAR-PERF-1D adds the report-only Bayesian performance/layout advisor contract. Benchmark rows now
carry advisory-only confidence, uncertainty, input evidence refs, and future execution-mode/reuse/
sizing/layout decision-surface fields. The advisor never applies runtime decisions, never upgrades
claim gates, and cannot make public performance claims. Remaining GAR-PERF-1 follow-up work is
fresh evidence generation and any later fitted Bayesian model or claim blocker that is backed by a
separate evidence-bearing slice. Benchmark outputs must remain local pre-release evidence, not
leaderboards or public performance claims. Compatibility-import rows continue to include ingest/
stage/certification work and must not be presented as pure query speed.

GAR-FLOW-2D adds `work_avoidance_evidence_schema` to the JSON artifact and Markdown report. The
schema uses only `measured`, `not_available`, `unsupported`, and `not_applicable` as status values.
Every ShardLoom benchmark row reports status/value/reason triples for rows avoided, segments
pruned, bytes avoided, encoded-vector reuse, and pushdown proof. Unknown work-avoidance metrics
remain `not_available` with a reason rather than being converted to zero; those rows cannot support
performance, superiority, Spark-displacement, production, or best-default claims.

GAR-0031A adds `native_io_source_sink_coverage_ref` to ShardLoom coverage rows. The ref points to
the `native-io-envelope-plan` source/sink coverage matrix, which distinguishes fixture-certified
local lanes from report-only or unsupported object-store, table/catalog, streaming, compatibility
export, unstructured/media, and external-adapter paths. This is coverage attribution only; it does
not upgrade benchmark rows to source/sink, object-store, or production-runtime claims.

GAR-0042A adds `vortex_source_split_admission_ref` to ShardLoom coverage rows. The ref points to the
`vortex-api-inventory` source/split admission proof for the scoped local Vortex scan fixture path.
It records provider/version/API-surface, Source/Split refs, field-mask and predicate-ordering
blockers, certificate refs, Native I/O refs, and no-fallback policy. This is admission attribution
only; generalized Source/Split runtime, object-store/table/catalog scans, writes, and performance
claims remain blocked without evidence.

GAR-0003-A adds `vortex_segment_extraction_admission_ref` to ShardLoom coverage rows. The ref points
to the `vortex-api-inventory` sparse segment extraction admission report for
`sparse_patch_fill`. This is explicit unsupported attribution only; sparse extraction, broad layout
coverage, production segment extraction, and performance claims remain blocked until correctness,
execution-certificate, Native I/O, materialization/decode, and no-fallback evidence exists.

GAR-0003-B adds `materialization_policy_ref` to ShardLoom coverage rows. The ref points to the
shared `compute-capability-matrix` materialization/decode policy for `encoded_native`,
`residual_native`, `materialized_temporary`, and `unsupported` operator paths. This keeps
materialized temporary execution visibly separate from encoded-native evidence and blocks
encoded-native claims unless the row stayed encoded with the required certificates.

GAR-0042B adds `vortex_layout_device_managed_boundary_ref` to ShardLoom coverage rows and
benchmark claim-gate metadata. The ref points to the runtime-utilization boundary matrix for
layout/write, device execution, object-store I/O, and managed-platform comparison rows. All rows are
`not_claim_grade`; managed platforms are comparison-only; and device/object-store lanes cannot
satisfy native claims without execution certificates, Native I/O certificates, and workload-scoped
metrics.

P7.5.9 adds `format_preparation_matrix` to the JSON/Markdown report. The matrix is limited to
ShardLoom rows and separates source read, compatibility parse, compatibility-to-Vortex import,
Vortex write/reopen/scan, operator compute, result sink, evidence rendering, and total runtime by
source format. It records `native_execution_format=vortex` for every row and treats CSV, JSONL,
Parquet, Arrow IPC, Avro, and ORC as compatibility preparation inputs, not native execution
formats.

Capability discovery is mode-aware: `compute-capability-matrix` rows now distinguish
`compatibility_import_certified`, `prepared_vortex`, `native_vortex`,
`direct_compatibility_transient`, and `auto`. Direct transient remains non-Vortex-native; only the
scoped local adapter smoke paths are executable, currently covering CSV, JSONL/NDJSON, and
feature-gated Parquet/Arrow IPC/Avro/ORC traditional analytics rows plus the broader
`local-source-runtime` local adapter universe.

## Code Surfaces

ShardLoom core owns the suite-level benchmark catalog:

```text
BenchmarkSuiteCatalogReport
BenchmarkSuiteKind
BenchmarkScenarioCategory
BenchmarkSuiteDatasetProfileKind
BenchmarkEnginePluginContract
BenchmarkCoverageTableRow
BenchmarkConstitution
BenchmarkConstitutionRequirementReport
BenchmarkResultSchemaV2Report
```

The Vortex crate owns the source-backed matrix:

```text
SourceBackedBenchmarkMatrixReport
SourceBackedBenchmarkMatrixRow
SourceBackedBenchmarkMeasuredRow
SourceBackedBenchmarkLane
SourceBackedBenchmarkOperation
SourceBackedBenchmarkRowStatus
measure_source_backed_benchmark_matrix_smoke
```

## Source-Backed Matrix Rows

The matrix names executable-evidence lanes:

```text
prepared-batch-only encoded filter/projection/filter-project
source-bound encoded filter/projection/filter-project
reader-backed constant filter/projection/filter-project
reader-backed dictionary filter/projection/filter-project
reader-backed run-end filter/projection/filter-project
```

It also names deterministic blocked lanes:

```text
sparse or nullable dictionary/RLE paths
device-buffer paths
nested parent/child paths
extension DType paths
```

Executable rows require:

```text
source URI
split refs
provider kind
provider API surface
Vortex version
row counts
selected/projected row counts
representation transitions
residual executor
execution certificate ref
Native I/O certificate ref
correctness fixture/ref-output ref
benchmark row ref
Rust performance profile
no-fallback evidence
```

The report-only `current()` / `plan_source_backed_benchmark_matrix()` path remains non-executing
and claim-blocking. P7.4.4 also adds `measure_source_backed_benchmark_matrix_smoke()`, which
executes deterministic in-memory Vortex-encoded fixture rows for the eligible matrix lanes and
records:

```text
benchmark_row_ref
elapsed_nanos
row_count
selected_or_projected_count
provider kind/API/version
source refs
split refs
representation transitions
residual_executor=none
execution certificate refs
Native I/O certificate refs and path refs
correctness refs
benchmark constitution
reproducibility ref
external_engine_invoked=false
fallback_attempted=false
performance_claim_allowed=false
```

The measured smoke rows are fixture evidence only. They populate the source-backed benchmark matrix
rows for local reproducibility and coverage accounting, but they still do not permit source-backed
performance or production claims.

Blocked rows require:

```text
deterministic blocker
unsupported_blocked residual executor
external_engine_invoked=false
fallback_attempted=false
```

## Benchmark Suite Catalog

Suite families:

```text
common
local_analytics
native_vortex
etl_workflows
source_backed_encoded
layout_and_pruning
incremental_state
stress
```

Scenario categories:

```text
scan_and_pruning
projection_and_layout
aggregation
joins
sort_and_window
etl_write
messy_lakehouse_data
incremental_state
operational_cache_concurrency
```

Dataset profiles:

```text
tiny_smoke
narrow_fact_dim
wide_table
very_wide_table
high_cardinality_strings
null_heavy
skewed_keys
many_small_files
few_large_files
partitioned_by_date
poorly_clustered
well_clustered
schema_drift
dirty_csv
nested_json
cdc_delta_overlay
```

Every benchmark row must attach constitution metadata before claims:

```text
scenario_id
scenario_category
dataset_profile
engine_role
input_format
table_format
storage_mode
native_vortex_or_compatibility_import
startup_included
conversion_included
staging_included
result_delivery_included
write_included
cache_mode
iterations
warmup_policy
correctness_oracle
materialization_policy
materialization_policy_ref
resource_policy
claim_level
```

`shardloom.benchmark_constitution_validation.v1` is now the executable validator surface for that
requirement. `shardloom benchmark-constitution [foundation|traditional-analytics] --format json`
projects the current plan/manifest/comparison posture, and
`python scripts/check_benchmark_constitution.py` rejects any claim-bearing artifact row missing
result identity, source admission, preparation route, execution route, output route, correctness
proof, hardware/build metadata, cold/warm cache state, stage timings, cost/unit fields where
available, no-fallback proof, or external-baseline boundary fields. The validator is evidence-only:
it does not run benchmarks, invoke external engines, or upgrade `performance_claim_allowed`.

## Baseline Policy

Local baseline lanes are comparison contracts, not runtime dependencies. The `full_local_plus_spark`
profile requires the local CPU baselines, the baseline PySpark module lane, and the two Spark
profile lanes; extended/GPU lanes remain explicit opt-ins:

```text
pandas
Polars eager
Polars lazy
DuckDB
DataFusion
Dask
pyspark
spark-default
spark-local-tuned
pyarrow dataset / Acero, clickhouse-local, Daft, Ray Data, Ibis adapters, and cuDF GPU as explicit
extended/GPU optional lanes
Vortex + DataFusion integration
Vortex + DuckDB integration
```

Managed platforms remain design references only:

```text
Photon
Microsoft Fabric
Snowflake
BigQuery
Redshift
Databricks managed services
```

They are not benchmark dependencies, not default benchmark lanes, and never fallback engines.

## Claim Status

The suite catalog, executable local analytics taxonomy, and source-backed matrix are populated. The
default source-backed matrix path remains report-only. The explicit smoke measurement path populates
fixture benchmark rows for eligible prepared/source-bound/reader-backed encoded rows. The
ShardLoom traditional analytics lane executes the base-schema expanded taxonomy scenarios
`filter + projection + limit`, `multi-key group by`, `join + aggregate`, `row number window`,
`partition pruning`, `many-small-files scan`, `null-heavy aggregate`,
`high-cardinality string group/distinct`, and `top-N per group`, plus dirty-CSV
`clean/cast/filter/write`, dirty-CSV `malformed timestamp / dirty CSV`, and
`nested JSON field scan`, plus CDC-overlay `small change over large base`, through the local Vortex
import/replay/result-sink evidence path.
Result-sink ShardLoom rows also surface report-only
Vortex layout/write advisor fields derived from workload, benchmark, runtime, and Native I/O
evidence. It does not execute comparative benchmarks, apply layout rewrites, or publish performance
claims.

The current benchmark suite profile for publication is `full_local`: ShardLoom
cold/import, ShardLoom native Vortex, ShardLoom prepared Vortex, `shardloom-prepare-batch`, pandas,
Polars eager/lazy, DuckDB, DataFusion, and Dask are required lanes for CSV, JSONL, Parquet,
Arrow IPC, Avro, and ORC local taxonomy scenarios. Spark/PySpark lanes are explicit-only
historical baselines, not required current publication evidence. The suite catalog
also includes a first-class `runtime_control` suite and `pulseweave_runtime_control` scenario
category so PulseWeave evidence is not hidden inside an optional lane. Promoted website artifacts
must include row evidence for every required baseline lane before they can remain marked complete;
rows still remain local pre-release benchmark evidence, not performance, superiority,
Spark-displacement, or production claims.

The `compute-capability-matrix` now carries a GAR-0006-A predicate/DType coverage table for
predicate, DType, null-semantics, nested-shape, and statistics families. Those rows are benchmark
interpretation aids, not new runtime paths: each family records support status, required statistics,
fixture/evidence gaps, unsupported diagnostic codes where applicable, `fallback_attempted=false`,
`external_engine_invoked=false`, and a claim boundary so local benchmark coverage is not mistaken
for broad predicate, DType, null, nested, or production metadata-only support.

The next benchmark closeout step is claim-grade and broad-format local benchmark readiness, not
release work. That closeout should keep managed platforms out, preserve coverage rows separately
from timing rows, extend current profile evidence across JSONL/Arrow IPC/Avro/ORC where required,
and promote rows only when the artifact carries workload-scoped correctness, benchmark,
execution-certificate, Native I/O, materialization/decode, and no-fallback evidence. Rows without
that evidence must remain `fixture_smoke_only`, `not_claim_grade`, `unsupported`, `blocked`, or
`external_baseline_only` as appropriate.

Suggested first full local preflight:

```powershell
python scripts\check_benchmark_environment.py --profile full_local
python benchmarks\traditional_analytics\run.py --claim-readiness-rerun --engines shardloom,shardloom-vortex,shardloom-prepared-vortex,shardloom-prepare-batch,pandas,polars-eager,polars-lazy,duckdb,datafusion,dask --formats csv,jsonl,parquet,arrow-ipc,avro,orc --dataset-profile tiny_smoke --rows 100000 --iterations 3 --require-all-engines
```

Then run profile-specific checks:

```powershell
python benchmarks\traditional_analytics\run.py --engines shardloom,pandas,polars-eager,polars-lazy,duckdb --formats csv --scenario "top-N per group" --dataset-profile narrow_fact_dim --rows 100000 --iterations 3
python benchmarks\traditional_analytics\run.py --engines shardloom,pandas,polars-eager,polars-lazy,duckdb --formats csv --scenario "high-cardinality string group/distinct" --dataset-profile high_cardinality_strings --rows 100000 --iterations 3
```

For scenarios ShardLoom does not support yet, expected evidence is an unsupported or blocked row,
a coverage row, `fallback_attempted=false`, and `external_engine_invoked=false`.

`plan_source_backed_benchmark_matrix()` keeps:

```text
measured_benchmark_rows_present=false
source_backed_claim_closeout_allowed=false
benchmark_execution_performed=false
external_engine_invoked=false
fallback_attempted=false
```

`measure_source_backed_benchmark_matrix_smoke()` records:

```text
measured_benchmark_rows_present=true
benchmark_execution_performed=true
measured_row_count=15
source_backed_claim_closeout_allowed=false
external_engine_invoked=false
fallback_attempted=false
```
