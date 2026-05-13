# RFC 0040: Benchmark Suite and Platform-Learning Hardening

## Purpose

Improve ShardLoom's optional benchmark architecture and capability model using public design lessons
from systems such as Photon, Microsoft Fabric, and Snowflake without adding managed-platform
benchmark lanes or platform-specific runtime dependencies.

The benchmark direction should shift from "add more engines" to:

```text
add more workload shapes
add more data shapes
record fairness metadata
show support/coverage separately from timing
preserve ShardLoom evidence and no-fallback proof
```

## Status

Accepted as benchmark and capability hardening guidance.

This RFC does not authorize Databricks Photon, Fabric, Snowflake, BigQuery, Redshift, or other
managed-platform benchmark lanes. It does not add managed-platform dependencies, credentials,
external API clients, benchmark execution, performance claims, or fallback execution.

## Benchmark Suite Architecture

Move from one flat traditional analytics script toward suite-level organization:

```text
benchmarks/common/
benchmarks/local_analytics/
benchmarks/native_vortex/
benchmarks/etl_workflows/
benchmarks/source_backed_encoded/
benchmarks/layout_and_pruning/
benchmarks/incremental_state/
benchmarks/stress/
```

The current traditional analytics harness remains a local optional baseline suite and can migrate
into `local_analytics`.

## Local Baseline Policy

Core optional baselines should remain local and comparison-only:

```text
ShardLoom
ShardLoom native Vortex
pandas
Polars
DuckDB
DataFusion
local Spark default
local Spark tuned
optional local ClickHouse
optional Vortex+DataFusion integration
optional Vortex+DuckDB integration
```

Vortex integrations and external engines are benchmark/oracle/reference rows only:

```text
external_integration_baseline_only
not_shardloom_execution
not_fallback
```

Dask, Trino, and other distributed SQL or task engines are design references for workload shape,
fairness metadata, and optional externally managed comparison environments only. This RFC does not
authorize ShardLoom to add them as benchmark dependencies, dev-cluster requirements, runtime
providers, or fallback execution paths.

## Scenario Taxonomy

Benchmark coverage should span:

```text
scan and pruning
projection and layout
aggregation
joins
sort and window
ETL/write
messy lakehouse data
incremental/state
operational/cache/concurrency
```

Dataset profiles should include:

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

## Benchmark Constitution

Every benchmark row must reference a `BenchmarkConstitution` describing:

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

## Coverage Table

Benchmark reports must separate timing from support coverage:

```text
supported
certified
planned
unsupported
blocked
external_baseline_only
```

ShardLoom rows must preserve:

```text
execution_provider_kind
provider_api_surface
residual_executor
representation_transition_order
materialization/decode evidence
execution certificate status
Native I/O certificate status
fallback_attempted=false
external_engine_invoked=false
```

## Platform-Inspired Neutral Capabilities

Use public system lessons as design references only.

Photon-inspired neutral concepts:

```text
NativeExecutionCoverageReport
BatchShapeReport
VectorizedKernelEligibilityReport
UnsupportedOperatorCoverageReport
WritePathOptimizationReport
```

Fabric-inspired neutral concepts:

```text
VortexLayoutAdvisorReport
AccessTemperatureReport
VirtualDataRef
QueryInsightsStore
LiveIngestionEvidence
```

Snowflake-inspired neutral concepts:

```text
DeclaredMaterializationPlan
IncrementalOperatorEligibilityMatrix
SelectiveAccessPathReport
OutlierWorkloadDetectionReport
StreamingIngestionChannelContract
OpenCatalogCompatibilityReport
```

These are ShardLoom-native capability and evidence concepts, not managed-platform clones.

## Language And Runtime Posture

ShardLoom's engine remains Rust-first. Python remains the primary wrapper and data workflow
language. SQL is a frontend DSL lowered into ShardLoom plans. TypeScript, JVM, Go, .NET, R, and
similar languages are client/wrapper surfaces. WASM/WIT is the preferred future sandbox direction
for safe UDF/plugin extensibility. CUDA is a future device-kernel lane only when device-residency
evidence exists. C++ is not a core-engine requirement.

## Acceptance

```text
Benchmarks remain local/platform-neutral by default.
Managed platforms remain design references, not benchmark dependencies.
External/local baseline engines remain comparison-only and never fallback.
Scenario and dataset coverage expands beyond clean synthetic analytics.
Benchmark reports include both timings and support/coverage evidence.
ShardLoom evidence fields remain visible in every ShardLoom row.
No performance/superiority claim is allowed without measured rows, correctness evidence,
benchmark constitutions, reproducibility metadata, and no-fallback evidence.
```
