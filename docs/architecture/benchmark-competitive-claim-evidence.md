# Benchmark Competitive Claim Evidence

## Purpose

`BenchmarkClaimEvidenceReport` is the CG-6 aggregate surface for deciding
whether ShardLoom has enough benchmark evidence to publish performance,
superiority, cost, replacement, or best-default-engine claims.

It does not run benchmarks. It combines the current benchmark plan, run
manifest requirements, comparison report requirements, reproducibility
requirements, and no-fallback policy into one stable report that humans and
agents can inspect before accepting any claim.

## Command

```text
shardloom benchmark-claim-evidence-plan [foundation|traditional-analytics]
shardloom benchmark-constitution [foundation|traditional-analytics]
```

The command is report-only. It performs no query execution, no benchmark
execution, no external engine invocation, no data reads, no object-store I/O, no
writes, and no fallback execution.

## Evidence Surfaces

The report tracks these surfaces in deterministic order:

- `benchmark_plan`
- `required_metrics`
- `correctness_evidence`
- `benchmark_result_rows`
- `external_comparison_results`
- `comparison_report`
- `reproducibility_manifest`
- `no_fallback_policy`
- `claim_publication_gate`

The current foundation and traditional-analytics reports intentionally remain
`needs_evidence` because they have planned scenarios and required metrics, but
do not yet have complete correctness evidence, measured benchmark result rows,
external comparison rows, or reproducible run metadata.

`benchmark-constitution` adds the fail-closed row validator for this evidence. It requires
benchmark result identity, source admission, preparation route, execution route, output route,
correctness proof, hardware/build metadata, cold/warm cache state, stage timings, cost/unit fields
where available, no-fallback proof, and external-baseline boundary markers before any row can be
treated as claim-grade.

## Claim Rules

`performance_claim_allowed` can only become true when the claim gate has:

- correctness evidence
- benchmark result evidence
- required metric coverage
- comparison report evidence
- reproducibility evidence
- no-fallback evidence

`superiority_claim_allowed` and `best_default_claim_allowed` remain false in
this aggregate until broader CG-20 capability certification and benchmark
evidence are attached. This prevents the benchmark plan itself from becoming a
marketing claim.

## Baseline Policy

Spark, DataFusion, DuckDB, Polars, pandas, Dask, Vortex integration, and other
systems may appear as benchmark baselines only. They are comparison rows, not
execution dependencies and not fallback engines.

If any future report marks a baseline as fallback-capable or records fallback
attempts, the aggregate status must become `unsafe_fallback_policy`.

## GAR-0009-A Spark-Displacement Matrix

`benchmark-claim-evidence-plan` now also emits the report-only
`spark_displacement_matrix_schema_version=shardloom.spark_displacement_benchmark_evidence_matrix.v1`
surface. The matrix keeps compatibility-import, prepared/native, messy-data ETL, scale/table
boundary, and public-claim attachment rows visible with `claim_gate_status=not_claim_grade`.

The matrix is an evidence checklist, not a benchmark result:

- `spark_displacement_matrix_performance_claim_allowed=false`
- `spark_displacement_matrix_superiority_claim_allowed=false`
- `spark_displacement_matrix_spark_displacement_claim_allowed=false`
- `spark_displacement_matrix_all_external_lanes_baseline_only=true`
- `spark_displacement_matrix_fallback_attempted=false`
- `spark_displacement_matrix_external_engine_invoked=false`

## GAR-0040-A Comparative Rerun And Managed-Platform Gate

`benchmark-claim-evidence-plan` and `release-plan` now also emit
`comparative_rerun_managed_platform_gate_schema_version=shardloom.comparative_rerun_managed_platform_gate.v1`.
This gate separates fresh local reruns from external baseline/oracle rows and from optional
managed-platform comparison lanes.

The gate is fail-closed:

- `comparative_rerun_managed_platform_gate_claim_gate_status=not_claim_grade`
- `comparative_rerun_managed_platform_gate_local_comparative_rerun_performed=false`
- `comparative_rerun_managed_platform_gate_external_baselines_comparison_only=true`
- `comparative_rerun_managed_platform_gate_managed_platform_lanes_comparison_only=true`
- `comparative_rerun_managed_platform_gate_managed_platform_credentials_required=true`
- `comparative_rerun_managed_platform_gate_managed_platform_credentials_resolved=false`
- `comparative_rerun_managed_platform_gate_managed_platform_dependencies_added=false`
- `comparative_rerun_managed_platform_gate_managed_platform_execution_performed=false`
- `comparative_rerun_managed_platform_gate_performance_claim_allowed=false`
- `comparative_rerun_managed_platform_gate_fallback_attempted=false`
- `comparative_rerun_managed_platform_gate_external_engine_invoked=false`

Managed platforms remain design references and optional comparison targets only. They require
explicit credential policy evidence before any future run and cannot become ShardLoom execution or
fallback evidence.

## Acceptance Boundary

This surface closes only the benchmark-claim aggregate inventory. It does not
close CG-6, publish benchmark numbers, implement external benchmark runners,
certify traditional analytics coverage, or prove ShardLoom is the best default
engine.

Next implementation work should attach real measured result rows,
reproducibility metadata, correctness proof linkage, and external baseline rows
from approved benchmark harnesses.
