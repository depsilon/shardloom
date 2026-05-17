# Fused Operator Pipeline

## Purpose

This document is the report-only architecture reference for `GAR-PERF-2E`. It defines the planned
fused local prepared/native operator pipeline layer for benchmark scenario families that can avoid
intermediate full-table materialization.

The target is practical runtime work avoidance, not a broad SQL/DataFrame runtime, not an
encoded-native operator claim, and not a public performance claim.

## Current State

Prepared/native rows increasingly use residual-native ShardLoom operator paths over projected local
Vortex scans. `GAR-PERF-1C` adds a scoped `fused_pipeline_*` evidence block for the current
filter/projection/limit row and the selective-filter selection-vector metric aggregation row. Those
rows report `fused_pipeline_used`, `fused_operator_family`,
`intermediate_materialization_avoided`, row counts, materialization/decode status, claim gate, and
no-fallback fields while keeping `fused_pipeline_encoded_native_claim_allowed=false`.

The current state is not a broad fused pipeline contract. Fusion is not uniform across scenario
families, correctness comparison to unfused paths is not a general gate, and the broader
`GAR-PERF-2E` family still owns filter/group-by, filter/aggregate, top-k/projection, and uniform
correctness-digest parity.

## Planned Pipeline Families

`GAR-PERF-2E` should implement or deterministically block fused local prepared/native pipelines for:

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

Every fused or blocked candidate row should expose:

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

If fusion is unavailable, rows should emit a deterministic blocker such as:

```text
unsupported_fusion_reason
fused_pipeline_used=false
intermediate_materialization_avoided=false
claim_gate_status=not_claim_grade
```

## Correctness Gate

Fusion must be semantics-preserving before any runtime row is promoted. The implementation gate
should compare fused output against an unfused ShardLoom-native path for the same prepared/native
artifact, scenario, projection, predicate, grouping, limit, and ordering semantics.

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

Future implementation should include:

```text
differential correctness tests for fused versus unfused paths
benchmark smoke before and after fusion
traditional benchmark row contract tests
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python -m compileall -q benchmarks/traditional_analytics
python scripts/check_website_readiness.py
git diff --check
```

Planning-only updates should run release-readiness metadata and website readiness checks.
