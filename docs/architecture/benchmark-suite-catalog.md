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
SourceBackedBenchmarkLane
SourceBackedBenchmarkOperation
SourceBackedBenchmarkRowStatus
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

This pass populates the matrix and suite catalog. It does not execute the comparative benchmarks and
does not publish performance claims.

`SourceBackedBenchmarkMatrixReport` keeps:

```text
measured_benchmark_rows_present=false
source_backed_claim_closeout_allowed=false
benchmark_execution_performed=false
external_engine_invoked=false
fallback_attempted=false
```
