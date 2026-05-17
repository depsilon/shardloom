# Fused Operator Pipeline

## Purpose

This document is the architecture reference for the scoped `GAR-PERF-2E` fused local
prepared/native operator pipeline evidence. It defines how benchmark scenario families report
runtime work avoidance, correctness-digest parity, and deterministic blockers without claiming a
broad optimizer or encoded-native execution engine.

The target is practical runtime work avoidance, not a broad SQL/DataFrame runtime, not an
encoded-native operator claim, and not a public performance claim.

## Current State

Prepared/native rows increasingly use residual-native ShardLoom operator paths over projected local
Vortex scans. `GAR-PERF-2E` extends the existing `fused_pipeline_*` block so rows now report family
coverage, deterministic blockers, correctness digest parity fields, materialization/decode status,
claim gate, and no-fallback fields while keeping
`fused_pipeline_encoded_native_claim_allowed=false`.

Current scoped executed families:

```text
filter + projection + limit -> fused_operator_family=filter_projection_limit
filter + aggregate -> fused_operator_family=filter_aggregate
top-k with projection -> fused_operator_family=top_k_projection
```

Current deterministic blocker:

```text
filter + group-by -> gar-perf-2e.filter_group_by_filter_absent
```

The filter/group-by blocker is intentional: current grouped rows have projection pushdown and
residual grouping, but no scoped grouped scenario with an admitted filter predicate.

## Pipeline Families

`GAR-PERF-2E` implements or deterministically blocks fused local prepared/native pipelines for:

```text
filter + projection + limit
filter + aggregate
filter + group-by
top-k with projection
```

Each pipeline should consume prepared/native local artifacts through admitted source-backed scan
boundaries, preserve no-fallback evidence, and avoid full intermediate materialization when fusion
is admitted.

## Required Evidence Contract

Every fused or blocked candidate row exposes:

```text
fused_pipeline_schema_version
fused_pipeline_report_id
fused_pipeline_scope
fused_pipeline_planned_family_count
fused_pipeline_family_statuses
fused_pipeline_used
fused_operator_family
intermediate_materialization_avoided
fused_pipeline_rows_scanned
fused_pipeline_rows_selected
fused_pipeline_rows_output
fused_pipeline_filter_columns
fused_pipeline_projection_columns
fused_pipeline_selection_vector_consumed
fused_pipeline_selection_vector_status
fused_pipeline_correctness_digest_status
fused_pipeline_unfused_correctness_digest
fused_pipeline_fused_correctness_digest
fused_pipeline_correctness_digest_match
fused_pipeline_unfused_reference_status
fused_pipeline_data_materialized
fused_pipeline_data_decoded
fused_pipeline_operator_execution_class
fused_pipeline_blocker_id
fused_pipeline_blocker_reason
fused_pipeline_claim_boundary
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

If fusion is unavailable, rows should emit a deterministic blocker such as:

```text
unsupported_fusion_reason
fused_pipeline_used=false
intermediate_materialization_avoided=false
claim_gate_status=not_claim_grade
```

## Correctness Gate

Fusion must be semantics-preserving before any runtime row is promoted. Current rows emit matching
canonical result digests for the fused result and its unfused-reference slot with
`fused_pipeline_unfused_reference_status=canonical_result_digest_reference_only`; focused tests also
compare prepared/native result JSON against the compatibility/materialized reference for the same
scenario fixtures. A future claim-grade promotion would need a separate independent unfused runtime
re-execution or stronger certificate.

The fused path may not rely on Spark, DataFusion, DuckDB, Polars, Velox, pandas, or another engine
for execution or residual evaluation. External engines may remain comparison baselines or test
oracles only where existing benchmark policy allows them.

## Benchmark Interpretation

Fused rows should be rendered as local pre-release runtime evidence. They may show timing
attribution and work-avoidance signals, but they must not be interpreted as:

- public speed rankings.
- superiority claims.
- Spark replacement claims.
- broad prepared/native coverage.
- encoded-native operator claims.
- SQL/DataFrame production support.
- object-store/lakehouse support.

## Non-Goals

- No broad SQL/DataFrame runtime.
- No external engine fallback.
- No object-store/lakehouse runtime.
- No generated source runtime.
- No production claim.
- No performance/superiority claim.
- No encoded-native operator claim unless later end-to-end representation evidence proves it.

## Acceptance

- Each planned pipeline family is implemented with evidence or emits a deterministic blocker.
- Fused rows avoid intermediate full-table materialization when fusion applies.
- Fused and unfused ShardLoom-native paths produce identical correctness digests.
- Benchmark rows expose fused pipeline fields, row counts, materialization/decode status, and
  claim-gate status.
- Unsupported or unsafe fusion paths are blocked without fallback.

## Verification Plan

Current verification includes:

```text
cargo test -p shardloom-vortex selective_filter_lowers_observed_bitpacked_and_sequence_filter_columns --features vortex-traditional-analytics-benchmark
cargo test -p shardloom-vortex enabled_filter_projection_limit_uses_prepared_native_vortex_scan --features vortex-traditional-analytics-benchmark
cargo test -p shardloom-vortex enabled_sort_top_k_uses_prepared_native_vortex_scan --features vortex-traditional-analytics-benchmark
cargo test -p shardloom-vortex enabled_top_n_per_group_uses_prepared_native_vortex_scan --features vortex-traditional-analytics-benchmark
cargo test -p shardloom-vortex enabled_group_by_aggregation_uses_prepared_native_vortex_scan --features vortex-traditional-analytics-benchmark
traditional benchmark row contract tests
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python -m compileall -q benchmarks/traditional_analytics
python scripts/check_website_readiness.py
git diff --check
```

Planning-only updates should run release-readiness metadata and website readiness checks.
