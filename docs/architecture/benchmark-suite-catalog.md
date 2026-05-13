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
resource_policy
claim_level
```

## Baseline Policy

Local optional baselines are plugin contracts, not runtime dependencies:

```text
pandas
Polars
DuckDB
DataFusion
Dask
local Spark default
local Spark tuned
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

The next benchmark closeout step is P7.4.4 claim-grade local benchmark readiness, not release work.
That closeout should run selected local comparative reruns, keep managed platforms out, preserve
coverage rows separately from timing rows, and promote rows only when the artifact carries
workload-scoped correctness, benchmark, execution-certificate, Native I/O, materialization/decode,
and no-fallback evidence. Rows without that evidence must remain `fixture_smoke_only`,
`not_claim_grade`, `unsupported`, `blocked`, or `external_baseline_only` as appropriate.

Suggested first local smoke:

```powershell
python benchmarks\traditional_analytics\run.py --engines shardloom,shardloom-vortex,pandas,polars,duckdb,datafusion --formats csv,parquet --include-taxonomy-extra --dataset-profile narrow_fact_dim --rows 100000 --iterations 3
```

Then run profile-specific checks:

```powershell
python benchmarks\traditional_analytics\run.py --engines shardloom,pandas,polars,duckdb --formats csv --scenario "top-N per group" --dataset-profile narrow_fact_dim --rows 100000 --iterations 3
python benchmarks\traditional_analytics\run.py --engines shardloom,pandas,polars,duckdb --formats csv --scenario "high-cardinality string group/distinct" --dataset-profile high_cardinality_strings --rows 100000 --iterations 3
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
