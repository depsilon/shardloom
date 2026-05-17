# Performance Attribution And Execution Structure

Status: implemented baseline for P7.4.8/P7.4.9 with P7.5 overhaul follow-ups.

## Purpose

ShardLoom's current comparative benchmark artifacts include useful certified workflow evidence, but
some rows measure more than query compute. This document defines the timing vocabulary and execution
mode boundaries that benchmark reports, CLI envelopes, Python clients, and future REST surfaces must
preserve before any public performance interpretation.

The canonical top-level flow reference is
`docs/architecture/compute-engine-flow-reference.md`. This document is the more detailed
performance-attribution companion for that flow: it explains which costs belong to each execution
mode and which fields must keep those costs visible.

The repo-alignment review and next overhaul steps are tracked in
`docs/architecture/compute-engine-flow-overhaul-review.md`.

The immediate correction is structural: compatibility-import-certified rows must not be read as pure
operator compute. They include compatibility source parsing, compatibility-to-Vortex import, Vortex
file write/reopen/scan, temporary materialization, optional result-sink replay, evidence rendering,
and CLI/process overhead. Those costs are valid for an ingest/stage workflow, but they are not the
same as prepared/native Vortex query timing.

## Vortex-First Provider Check

- Subject area: benchmark timing structure, Vortex prepared/native execution, Scan API/source-backed
  timing, and encoded/native operator evidence.
- Upstream Vortex concept checked: arrays and deferred/compressed representations; Scan API Source,
  Sink, Split, filter/projection/limit pushdown; execution fusion/deferred execution; I/O
  coalescing, prefetch, concurrency, and memory backpressure.
- Decision: `wrap_vortex_concept` for report-only stage attribution and execution-mode vocabulary;
  `use_vortex_native_provider` only for prepared/native paths with Vortex provider evidence;
  `blocked_until_vortex_or_shardloom_evidence` for unsupported fused, encoded-native, or Scan API
  paths.
- Vortex API/provider surface: local Vortex files and current feature-gated benchmark provider
  paths; future Scan API Source/Sink/Split and pushdown surfaces when they can be admitted with
  evidence.
- ShardLoom provider/report/certificate surface: benchmark rows, typed command/result envelopes,
  Native I/O certificates, execution certificates, materialization/decode boundaries, prepared
  artifact refs/digests, and deterministic unsupported diagnostics.
- Residual handling: residuals must be executed by ShardLoom-native code or blocked; they must not
  be delegated to DataFusion, DuckDB, Spark, Polars, Dask, or another external engine as fallback.
- Materialization/decode boundary: every row must record whether native/compressed, canonical, or
  materialized representations were used and whether decode/materialization was required.
- Evidence added: planned P7.4.8 stage timing fields, planned P7.4.9 prepared/native benchmark
  lanes, and the GAR-0003-B shared materialization/decode policy ref that distinguishes
  encoded-native, residual-native, materialized-temporary, and unsupported operator paths.
- Gates still blocked: broad SQL/DataFrame maturity, broad performance superiority, object-store
  runtime, table/catalog runtime, and production claims.
- `fallback_attempted=false`: required for every ShardLoom execution-mode row.

## Structural Paths

### One-Shot Compatibility Query

Shape:

```text
CSV/Parquet/etc -> direct transient ShardLoom-native compute -> optional result
```

This path is for small local jobs and developer quick checks. It does not persist a Vortex artifact
and does not carry a Vortex-native claim. If exposed before implementation, it must be deterministic
report-only or unsupported.

Required facts:

```text
selected_execution_mode=direct_compatibility_transient
vortex_native_claim_allowed=false
direct_transient_execution=true
compatibility_import_included=false
vortex_write_reopen_included=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_vortex_native
```

### Ingest/Stage Workflow

Shape:

```text
CSV/Parquet/etc -> compatibility adapter -> Vortex import -> certify -> write/reopen -> compute
```

This is the current certified compatibility-import workflow shape. It is useful because it proves
source compatibility, Native I/O certificate evidence, artifact digests, Vortex staging, replay, and
no-fallback behavior. It is not the default lane for pure query-speed comparison.

Required facts:

```text
selected_execution_mode=compatibility_import_certified
execution_mode_family=compatibility_import
compatibility_import_included=true
vortex_prepare_included=true
vortex_write_reopen_included=true
result_sink_included=<true when result-sink proof is requested>
fallback_attempted=false
external_engine_invoked=false
```

### Prepared Vortex Query

Shape:

```text
CSV/Parquet/etc -> one-time Vortex preparation -> many scenario runs from prepared .vortex artifacts
```

This is the primary comparative benchmark lane while ShardLoom matures native Vortex operators. The
preparation step is measured and recorded, but per-scenario timing starts after prepared artifact
creation unless a caller explicitly asks to include preparation.

In the comparative harness, prepared/native rows stay attached to the requested source-format rows
such as CSV, JSONL, Parquet, Arrow IPC, Avro, or ORC. The report should not add a standalone
`.vortex` storage-format row just to show native timing; prepared artifact refs and digests record
the Vortex boundary.

Required facts:

```text
selected_execution_mode=prepared_vortex
execution_mode_family=native_vortex
preparation_millis=<measured separately>
preparation_included_in_timing=false
prepared_artifact_ref=<fact/dim refs>
prepared_artifact_digest=<digest refs>
compatibility_import_included=false for scenario timing
fallback_attempted=false
external_engine_invoked=false
```

### Native Vortex Query

Shape:

```text
existing .vortex input -> Vortex-native scan/operator path -> result/evidence
```

This is the cleanest ShardLoom performance lane once operator coverage matures. Rows in this lane
must record provider/API surface, split/pushdown evidence where available, representation
transitions, and whether compute happened on compressed/native arrays, canonical arrays, or
materialized arrays.

Required facts:

```text
selected_execution_mode=native_vortex
execution_mode_family=native_vortex
compatibility_import_included=false
vortex_prepare_included=false
direct_transient_execution=false
fallback_attempted=false
external_engine_invoked=false
```

## Execution Modes

The stable mode names are:

```text
auto
compatibility_import_certified
prepared_vortex
direct_compatibility_transient
native_vortex
```

`auto` is transparent selection only. It must always report the selected mode and reason, and it
must never silently invoke an external fallback engine.

Every relevant surface should carry:

```text
requested_execution_mode
selected_execution_mode
mode_selection_reason
execution_mode_family
vortex_native_claim_allowed
compatibility_import_included
vortex_prepare_included
vortex_write_reopen_included
direct_transient_execution
fallback_attempted
external_engine_invoked
claim_gate_status
```

## Evidence Levels

`GAR-PERF-2A` adds planned evidence-level tiering beside the execution-mode fields:

```text
minimal_runtime
certified
full_replay
```

Evidence level describes proof depth, not a different execution semantics. `minimal_runtime` may
omit heavy result-sink replay unless requested, but it must still preserve `execution_mode`,
`evidence_level`, `fallback_attempted=false`, `external_engine_invoked=false`, available
source/output digests, and `claim_gate_status=not_claim_grade` unless a future scoped gate approves
otherwise. `certified` carries normal certificate evidence. `full_replay` carries result-sink replay
proof in addition to certificate evidence.

Do not use evidence levels as a hidden fast-mode toggle, fallback policy, public speed ranking, or
performance/superiority claim.

## Vortex Scan Pushdown Completion

`GAR-PERF-2C` adds planned Vortex Scan API pushdown completion for prepared/native rows. Existing
source-backed scan evidence proves scoped local provider boundaries, but every scenario family needs
an explicit classification for:

```text
scan_filter_pushed_down
scan_projection_pushed_down
scan_limit_pushed_down
filter_columns_read
output_columns_read
data_materialized
data_decoded
unsupported_pushdown_reason
```

Filter-only columns may be read to evaluate predicates, but they must not appear in output streams
unless requested. Projection pushdown must prove the output read set. Limit/slice pushdown must be
tracked separately from ordered top-N or window semantics. Unsupported expressions are deterministic
blockers or ShardLoom-native residual work, never external-engine fallback.

This layer explains scan/source-boundary work avoidance. It does not authorize encoded-native
operator claims, generalized Vortex Source/Split runtime claims, production SQL/DataFrame claims,
object-store/lakehouse claims, or public performance claims.

## Evidence-Aware Logical Optimizer

`GAR-PERF-2B` adds the planned optimizer rule registry and optimizer trace layer. Optimizer trace
evidence belongs beside timing and work-avoidance fields so a row can explain which rewrites were
admitted, applied, blocked, unsupported, not applicable, or report-only.

Initial rule families:

```text
predicate pushdown
projection pushdown
slice/limit pushdown
common subplan/source-state reuse
expression simplification
constant folding
type coercion
join ordering
cardinality estimation
```

Trace rows should expose:

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
estimated_input_cardinality
estimated_output_cardinality
cardinality_estimation_status
correctness_smoke_ref
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

An applied rewrite must have before/after plan digests, semantic safety, evidence preservation, and
correctness smoke before it can be used as runtime support evidence. Optimizer traces are not Polars
or DataFusion parity, SQL/DataFrame runtime, object-store/lakehouse support, production readiness,
or performance proof.

## I/O Reuse And Cross-Format Fanout

`GAR-IOREUSE-1` adds the planned timing and evidence split for reusable I/O state and flexible
output fanout. The runtime/benchmark path is:

```text
InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan -> SinkArtifact
```

This path is deliberately decoupled from matching input and output formats. A CSV input can plan a
Parquet, JSONL, and Vortex fanout; a Parquet input can plan CSV and Vortex outputs; a JSONL input
can plan Parquet and Vortex outputs; a generated source can plan CSV, Parquet, and Vortex outputs;
and a prepared Vortex artifact can fan out to multiple local output formats when each output has
evidence.

Future rows should separate:

```text
source_discovery_millis
schema_inference_millis
source_parse_millis
vortex_prepare_millis
operator_compute_millis
output_plan_millis
output_write_millis
output_replay_millis
total_runtime_millis
```

and expose:

```text
source_state_reuse_hit
prepared_state_reuse_hit
output_plan_reuse_hit
fanout_output_count
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

Reuse/fanout timing is workflow coverage and attribution evidence. It does not authorize
performance, superiority, production, object-store/lakehouse, Foundry, SQL/DataFrame,
Spark-replacement, package, or release claims.

## Compressed Encoded Kernel Registry

`GAR-PERF-2D` adds planned compressed/encoded kernel registry rows for:

```text
bitpacked boolean/integer filter
sequence equality/range predicate
dictionary equality/group-by
constant array count/filter
sorted min/max range pruning
FSST/dictionary string equality if available
```

Each row should expose:

```text
encoding_id
logical_dtype
physical_encoding
operator_family
kernel_admitted
kernel_executed
canonicalization_required
decoded
materialized
selection_vector_emitted
validity_semantics
encoded_native_claim_allowed
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

Kernel admission and execution must stay separate from encoded-native claim permission. Unsupported
encodings should produce deterministic blockers. `encoded_native_claim_allowed=false` remains the
default until the end-to-end path proves correctness, representation state, materialization/decode
boundaries, certificates, and no-fallback evidence.

## Fused Operator Pipelines

`GAR-PERF-2E` adds planned fused local prepared/native pipelines for:

```text
filter + projection + limit
filter + aggregate
filter + group-by
top-k with projection
```

Fused pipeline rows should report:

```text
fused_pipeline_used
fused_operator_family
intermediate_materialization_avoided
rows_scanned
rows_selected
rows_output
unfused_correctness_digest
fused_correctness_digest
correctness_digest_match
data_materialized
data_decoded
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

Fusion is valid only when the fused path avoids intermediate full-table materialization and produces
the same correctness digest as the unfused ShardLoom-native path. Unsupported fusion remains an
explicit blocker. These rows are residual-native pipeline evidence unless later certificates prove
encoded-native representation state end to end.

Do not use fused pipeline rows as broad SQL/DataFrame support, encoded-native operator coverage,
object-store/lakehouse runtime evidence, production readiness, or performance/superiority claims.

## In-Process Session Runtime

`GAR-PERF-2F` adds the scoped in-process session-backed prepared/native batch lane. The session layer
makes prepared/native reuse explicit across multiple local scenario executions without turning
process reuse into a hidden fast mode.

Scoped session state:

```text
prepared_artifact_registry
source_metadata_cache
source_state_cache
schema_cache
dictionary_cache
buffer_pool status
kernel_registry reference
evidence_recorder
```

Every session-backed row should expose `session_id`, cache hit/miss fields, source-state reuse
count, prepared-artifact reuse count, close/drop status, `session_hidden_global_cache=false`,
`session_daemon_or_service=false`, `session_fallback_attempted=false`,
`session_external_engine_invoked=false`, and `session_claim_gate_status`. Session state must be
scoped, caller-owned, and explicitly closed.

The session layer is not a daemon, service runtime, remote API, hidden global cache, or performance
claim. It must preserve typed envelopes, execution-mode fields, evidence-level fields, Native I/O
refs, materialization/decode boundaries, result-sink evidence when requested, and deterministic
unsupported diagnostics.

## Allocation And Buffer-Pool Optimization

`GAR-PERF-2G` adds a scoped allocation/resource-profile evidence layer to the prepared/native batch
lane. It makes resource posture visible without turning reuse into a hidden fast mode or public
performance claim.

Planned allocation families:

```text
result buffers
temporary vectors
hash tables
dictionary/string state
source-state arrays
```

Rows or memory/resource reports should expose:

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
allocation_fallback_attempted=false
allocation_external_engine_invoked=false
allocation_claim_gate_status
```

Buffer reuse is admissible only when it is opt-in or scoped to an explicit run/session, has a clear
owner/lifecycle, preserves correctness and evidence parity with the no-reuse path, and avoids unsafe
lifetime shortcuts. Allocation counts, allocation bytes, and peak RSS may be `not_available` until
measurement is stable; that status means unknown/not measured, not zero.

The current implementation slice reports `buffer_pool_enabled=false`, `buffer_reuse_count=0`, and a
deterministic buffer-reuse blocker. It does not measure allocation counts/bytes or peak RSS yet.

Allocation and buffer-pool rows are resource-profile evidence. They do not authorize performance,
memory-efficiency, Spark-displacement, production, SQL/DataFrame, object-store/lakehouse, Foundry,
or package claims.

## Optimized Build Profiles And PGO

`GAR-PERF-2H` adds the planned optimized build-profile and PGO benchmark lane. This layer makes
compiler/build configuration part of the evidence model so timing rows can be interpreted without
guessing which binary produced them.

Planned lanes:

```text
release-lto
release-pgo
release-native-benchmark
```

Rows should report:

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
correctness_digest
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

The default release build remains the portable release baseline. `target-cpu=native` is allowed only
inside explicitly labeled host-local benchmark rows. PGO rows need a reproducible
instrumented-build, training-run, `llvm-profdata` merge, and profile-use rebuild sequence, plus
training workload refs.

Build-profile evidence is not performance proof. It cannot create performance, superiority,
Spark-displacement, production, package/public-release, SQL/DataFrame, object-store/lakehouse, or
Foundry claims.

## Native Microbenchmark Rows

`GAR-PERF-2I` adds planned native microbenchmark suite expansion. These rows measure one primitive or
subsystem boundary at a time and must stay separate from compatibility-import, prepared/native
end-to-end, and external baseline rows.

Planned primitives:

```text
Vortex scan only
filter predicate only
projection only
group-by kernel
hash join kernel
top-k
result-sink write
evidence render
```

Each row should expose the benchmark category, primitive, rows, decode/materialization status,
fallback status, external-engine status, and claim gate. Missing primitives should produce
deterministic skipped or unsupported rows so optimization gaps remain visible.

Native microbenchmark rows answer "which subsystem needs optimization?" They do not answer "is
ShardLoom faster end to end?" and they do not authorize performance, superiority, Spark-displacement,
SQL/DataFrame, object-store/lakehouse, Foundry, or production claims.

## Stage Timing Fields

Benchmark JSON and Markdown should preserve these fields where available:

```text
total_runtime_millis
scenario_compute_millis
operator_compute_millis
computed_result_sink_write_millis
result_sink_write_millis
startup_warmup_millis
preparation_millis
preparation_included_in_timing
prepared_artifact_ref
prepared_artifact_digest
source_read_millis
compatibility_parse_millis
compatibility_to_vortex_import_millis
vortex_write_millis
vortex_reopen_millis
vortex_scan_millis
evidence_render_millis
build_time_excluded
compatibility_to_vortex_included
vortex_reopen_scan_included
result_sink_included
representation_transition_summary
encoded_native_execution_status
fusion_status
scan_api_status
persistent_runner_status
```

Unknown or not-yet-isolated fields should be explicit `null`, `not_measured`, or
`included_in_total_runtime` values rather than silently omitted.

## Benchmark Artifact Contract

The traditional analytics harness now emits
`execution_mode_attribution_contract` in JSON and Markdown reports. That contract
is intentionally redundant with the fields above: it makes the attribution rules
machine-readable next to the measurements so consumers do not have to infer them
from prose.

Every benchmark row must carry these execution-mode fields:

```text
requested_execution_mode
selected_execution_mode
mode_selection_reason
execution_mode_family
vortex_native_claim_allowed
compatibility_import_included
vortex_prepare_included
vortex_write_reopen_included
direct_transient_execution
claim_gate_status
```

Every benchmark row must carry these stage timing fields:

```text
source_read_millis
compatibility_parse_millis
compatibility_to_vortex_import_millis
vortex_write_millis
vortex_reopen_millis
vortex_scan_millis
operator_compute_millis
result_sink_write_millis
evidence_render_millis
total_runtime_millis
```

The harness validates those fields before writing the artifact. External
baseline rows use `selected_execution_mode=external_baseline_only`; ShardLoom
rows use one of the canonical ShardLoom modes. If
`requested_execution_mode=auto`, the row must also preserve the selected mode and
the selection reason.

Prepared/native rows must also preserve the operator blocker matrix:

```text
operator_execution_class
operator_admission_status
operator_blocker_id
operator_blocker_reason
operator_encoded_native_claim_allowed
```

The valid execution classes are `encoded_native`, `residual_native`,
`materialized_temporary`, and `unsupported`. Current residual-native and
materialized-temporary rows may be useful smoke evidence, but they must not be
counted as encoded-native operator execution.

Every benchmark row must also carry the persistent-runner admission fields:

```text
persistent_runner_status
process_startup_attribution
python_harness_overhead_status
cli_process_wall_millis
python_harness_overhead_millis
startup_warmup_millis
build_time_millis
build_time_excluded
preparation_millis
preparation_cli_process_wall_millis
preparation_included_in_timing
```

The companion `persistent_runner_admission_gate` is report-only. Default
comparative rows must keep
`persistent_runner_status=process_per_scenario_attributed_not_reduced`. The
explicit `traditional-analytics-vortex-batch-run` command may emit
`persistent_runner_status=single_process_batch_runner_supported` for scoped
single-process prepared/native batch runs only. No hidden persistent runner,
daemon, service, or fast mode is admitted until typed envelopes, mode-selection
evidence, Native I/O refs, operator blocker fields, materialization/decode
boundaries, result-sink replay evidence, deterministic unsupported diagnostics,
and no-fallback fields are preserved per run.

Every ShardLoom benchmark row must carry work-avoidance status/value/reason
triples for:

```text
rows_avoided
segments_pruned
bytes_avoided
encoded_vector_reuse
pushdown_proof
```

The status vocabulary is:

```text
measured
not_available
unsupported
not_applicable
```

`not_available` is distinct from zero. Missing skipped-row, pruned-segment,
avoided-byte, encoded-vector reuse, or pushdown values cannot be used as
performance, superiority, Spark-displacement, production, or best-default
evidence.

`compatibility_import_certified` rows are valid ingest/stage/certification
evidence, but they are not pure query-speed evidence. Public performance,
superiority, Spark-displacement, best-default, production, or replacement claims
remain blocked unless workload-scoped claim-grade evidence is attached.

## Current Interpretation

Current ShardLoom compatibility rows answer:

```text
How expensive is the certified local compatibility -> Vortex ingest/stage workflow plus current
temporary benchmark operator and evidence path?
```

They do not answer:

```text
How fast is pure ShardLoom operator compute over already-prepared Vortex data?
```

Prepared/native Vortex rows should answer the second question, with preparation timing and
artifact evidence recorded separately.

## Vortex Alignment Notes

The Vortex Scan API documentation describes Source, Sink, and Split concepts plus filter,
projection, and limit pushdown, but notes that the API is still under active development. ShardLoom
should align source-backed evidence with those concepts while emitting blockers when an upstream or
local path is not ready.

The Vortex I/O documentation describes positional reads, read coalescing, prefetching, backend
concurrency, segment caching, and memory backpressure. ShardLoom should treat those as Native I/O
evidence dimensions rather than hiding them in opaque benchmark time.

References:

- <https://docs.vortex.dev/concepts/arrays>
- <https://docs.vortex.dev/concepts/scanning>
- <https://docs.vortex.dev/developer-guide/internals/execution>
- <https://docs.vortex.dev/developer-guide/internals/io>
