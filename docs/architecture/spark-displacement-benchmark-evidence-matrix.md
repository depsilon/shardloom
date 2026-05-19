# Spark-Displacement Benchmark Evidence Matrix

Status: `report_only`

GAR slice: `GAR-0009-A`

Schema marker:
`spark_displacement_matrix_schema_version=shardloom.spark_displacement_benchmark_evidence_matrix.v1`

## Purpose

This matrix classifies the benchmark evidence required before ShardLoom can make any
Spark-displacement, performance, superiority, or replacement claim. It ties workload families,
ShardLoom execution lanes, external baseline/oracle lanes, correctness refs, timing refs,
environment refs, execution-mode refs, and no-fallback policy refs into one release-visible surface.

The matrix does not run benchmarks and does not authorize a public performance claim.

## Release And Benchmark Fields

`benchmark-claim-evidence-plan` emits these report-only fields:

- `spark_displacement_matrix_claim_gate_status=not_claim_grade`
- `spark_displacement_matrix_all_rows_not_claim_grade=true`
- `spark_displacement_matrix_all_external_lanes_baseline_only=true`
- `spark_displacement_matrix_performance_claim_allowed=false`
- `spark_displacement_matrix_superiority_claim_allowed=false`
- `spark_displacement_matrix_spark_displacement_claim_allowed=false`
- `spark_displacement_matrix_benchmark_rerun_performed=false`
- `spark_displacement_matrix_fallback_attempted=false`
- `spark_displacement_matrix_external_engine_invoked=false`

## Matrix Rows

| Row | ShardLoom lane | Baseline/oracle lanes | Current claim status |
| --- | --- | --- | --- |
| `compatibility_import_certified_lane` | `compatibility_import_certified` | pandas, Polars, DuckDB, Spark, DataFusion, Dask | `not_claim_grade` |
| `prepared_native_runtime_lane` | `prepared_vortex`, `native_vortex` | pandas, Polars, DuckDB, Spark, DataFusion, Dask | `not_claim_grade` |
| `messy_data_etl_lane` | `compatibility_import_certified`, `prepared_vortex` | pandas, Polars, DuckDB, Spark, DataFusion, Dask | `not_claim_grade` |
| `scale_and_table_boundary_lane` | `report_only` | Spark and managed-platform rows as baselines/oracles only | `not_claim_grade` |
| `public_claim_attachment_lane` | release claim gate | external rows remain baseline-only | `not_claim_grade` |

## Missing Evidence

Every row remains `not_claim_grade` because at least one of these evidence classes is missing:

- claim-grade rerun with full local plus Spark profile.
- approved external comparison rows with versions and environment fingerprint.
- reproducible benchmark manifest.
- workload-scoped correctness refs and execution certificates.
- Native I/O and materialization/decode certificates.
- source-state, prepared-state, and output replay evidence where relevant.
- scale, object-store, table/lakehouse, and managed-platform proof for those claim families.
- `GAR-0041-A` per-claim evidence attachment and release approval.

## Boundaries

- No public performance claim.
- No superiority claim.
- No Spark-displacement claim.
- External engines are baseline/oracle context only.
- No external engine fallback.
- `fallback_attempted=false`.
- `external_engine_invoked=false`.

Compatibility-import rows carry ingest, staging, certification, Vortex preparation, scan, optional
sink proof, and evidence costs. They must not be read as pure query speed rows. Prepared/native
Vortex rows are the runtime-development direction, but they still require workload-scoped
correctness, benchmark, certificate, Native I/O, materialization/decode, release, and no-fallback
evidence before any public displacement or performance claim.
