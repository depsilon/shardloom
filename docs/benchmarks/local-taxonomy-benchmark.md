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
  --output target\shardloom-local-taxonomy-smoke.json `
  --regenerate
```

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
  --output target\shardloom-claim-readiness-rerun.json `
  --regenerate
```

The preset uses ShardLoom, the ShardLoom Vortex fixture lane, and selected
local optional baselines. It keeps managed platforms out, enables ShardLoom
result-sink proof, includes taxonomy extras when no explicit scenario is
provided, and rejects fewer than three iterations.

## Prepared/Native Refresh Queue

`GAR-PERF-1A` tracks the next prepared/native benchmark refresh after the latest
batch runner and source-state reuse work. That refresh must keep
`compatibility_import_certified`, `prepared_vortex`, `native_vortex`, and
batch-runner rows separated. Compatibility-import rows include import, write,
reopen, scan, and evidence costs; they must not be presented as pure query
speed.

Fresh artifacts should preserve `source_metadata_snapshot_*`, `source_state_*`,
`execution_mode`, `claim_gate_status`, materialization/decode, Native I/O,
`fallback_attempted=false`, and `external_engine_invoked=false` evidence. The
benchmark remains local pre-release evidence, not a leaderboard, performance
claim, superiority claim, production claim, or Spark replacement claim.

## Evidence-Level Runtime Tiering Queue

`GAR-PERF-2A` tracks first-class benchmark evidence levels:

```text
minimal_runtime
certified
full_replay
```

The goal is to let benchmark readers see proof overhead without creating a hidden fast mode.
`minimal_runtime` may omit heavy result-sink replay unless requested, but it must still report
`execution_mode`, `evidence_level`, `fallback_attempted=false`, `external_engine_invoked=false`,
`claim_gate_status`, and available source/output digests. `certified` emits normal certificate
evidence. `full_replay` emits result-sink replay proof.

Until implementation lands, benchmark rows should not imply an evidence-light runtime tier exists.
After it lands, `evidence_level=minimal_runtime` remains `not_claim_grade` unless a later
workload-scoped gate explicitly approves otherwise.

## Vortex Scan Pushdown Completion Queue

`GAR-PERF-2C` tracks the planned completion pass for prepared/native Vortex Scan API pushdown. The
current benchmark rows expose scoped `source_backed_scan_*` evidence, but every prepared/native
scenario family still needs an explicit filter/projection/limit pushdown status or deterministic
blocker.

Future benchmark rows should expose:

```text
scan_filter_pushed_down
scan_projection_pushed_down
scan_limit_pushed_down
filter_columns_read
output_columns_read
data_materialized
data_decoded
unsupported_pushdown_reason
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

The row contract must distinguish filter-only columns from output columns. Pushdown evidence is
source/provider-boundary evidence only; it is not an encoded-native operator claim, generalized
Source/Split runtime claim, object-store/lakehouse claim, SQL/DataFrame claim, or performance claim.

## Evidence-Aware Logical Optimizer Queue

`GAR-PERF-2B` tracks the planned optimizer rule registry and report-only optimizer trace. Future
benchmark rows may link timing/resource rows to optimizer traces, but those traces must not be read
as lazy optimizer parity or performance proof.

Future benchmark rows or optimizer trace artifacts should expose:

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

Required future timing fields:

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

Required future reuse fields:

```text
source_state_reuse_hit
prepared_state_reuse_hit
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

## Compressed/Encoded Kernel Registry Queue

`GAR-PERF-2D` tracks the planned compressed/encoded kernel registry. The benchmark currently has
scoped selective-filter encoded-predicate evidence, but encoded-native operator coverage is not
broad and encoding/operator support is not yet a stable matrix.

Future benchmark rows should expose:

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

Initial registry rows should cover bitpacked boolean/integer filter, sequence equality/range
predicate, dictionary equality/group-by, constant array count/filter, sorted min/max range pruning,
and FSST/dictionary string equality where available. Unsupported encodings should be deterministic
blockers, and `encoded_native_claim_allowed=false` remains the default until end-to-end evidence
passes.

## Fused Operator Pipeline Queue

`GAR-PERF-2E` tracks the planned fused local prepared/native pipeline layer. The benchmark currently
has scoped residual-native paths and narrow fusion vocabulary, but it does not yet have a stable
cross-family fused-pipeline evidence contract.

Future benchmark rows should expose:

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

The first planned families are filter + projection + limit, filter + aggregate, filter + group-by,
and top-k with projection. A fused row should have an identical correctness digest to the unfused
ShardLoom-native path. Unsupported fusion paths should be deterministic blockers, not fallback
execution. Fusion rows are local pre-release evidence and not a performance ranking or broad
SQL/DataFrame claim.

## In-Process Session Runtime Queue

`GAR-PERF-2F` tracks the planned in-process `ShardLoomSession` runtime. The existing scoped
prepared/native batch runner proves one-process scenario execution and selected source-state reuse,
but a general reusable local session is not yet exposed.

The planned session row fields include:

```text
session_id
cache_hit/miss
source_state_reuse_count
prepared_artifact_reuse_count
session_close_status
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

The session queue exists to reduce redundant local setup and make reuse visible. It is not a daemon,
remote server, hidden fast mode, or benchmark claim.

## Allocation And Buffer-Pool Optimization Queue

`GAR-PERF-2G` tracks the planned allocation profiling and scoped buffer-reuse layer. Current
benchmark rows should not imply that a global allocation or buffer-pool optimization pass exists.

Future benchmark rows or memory/resource reports should expose:

```text
allocation_profile_status
allocation_profile_scope
allocation_count
allocation_bytes
buffer_pool_enabled
buffer_pool_scope
buffer_reuse_count
buffer_reuse_family
peak_rss_delta
correctness_digest
evidence_regression_status
unsafe_lifetime_shortcut_used=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

The first planned families are result buffers, temporary vectors, hash tables, dictionary/string
state, and source-state arrays. Buffer reuse must be opt-in or scoped to an explicit run/session and
must preserve correctness and evidence parity with the no-reuse path. `not_available` for
allocation counts or peak RSS means unknown/not measured, not zero.

These rows are resource-profile evidence only. They are not speed, memory-efficiency, production,
SQL/DataFrame, object-store/lakehouse, Foundry, or Spark-replacement claims.

## Optimized Build Profiles And PGO Queue

`GAR-PERF-2H` tracks the planned optimized build-profile and PGO benchmark lane. The harness already
records `shardloom_build_profile`, but future artifacts need a fuller build-profile contract before
optimized binaries can be interpreted.

Future benchmark rows should expose:

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

Planned build lanes are `release-lto`, `release-pgo`, and `release-native-benchmark`.
`target-cpu=native` belongs only to the benchmark-only native lane, not portable release artifacts.
PGO rows must record the training workload and profile artifact refs. These rows are build/config
evidence only and are not public performance rankings.

## Native Microbenchmark Suite Queue

`GAR-PERF-2I` tracks the next native microbenchmark expansion. Older artifacts may show native
microbenchmark rows as skipped, and current coverage does not fully isolate every kernel family
needed for optimization planning.

The planned suite should add implemented or deterministic skipped/unsupported rows for:

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

Rows must be labeled as `benchmark_category=native_microbenchmark` and should expose primitive,
rows, decoded/materialized status, timing scope, `fallback_attempted=false`,
`external_engine_invoked=false`, and `claim_gate_status`.

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
