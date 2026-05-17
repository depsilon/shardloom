# Vortex Scan Pushdown Completion

## Purpose

This document is the report-only architecture reference for `GAR-PERF-2C`. It defines the planned
completion pass for prepared/native Vortex Scan API pushdown evidence across traditional analytics
scenario families.

The goal is not to claim encoded-native operator execution. The goal is to ensure every
prepared/native scenario either maps eligible filter, projection, and limit requirements into the
Vortex scan/source boundary or emits a deterministic unsupported/blocker row that explains why it
could not.

## Current State

Prepared/native rows already expose scoped `source_backed_scan_*` evidence for local Vortex source
roles, projected columns, provider scope, Native I/O status, materialization boundaries, residual
executor, no-fallback status, and claim gate. Several scenario families already avoid full
fact-table materialization through projected scans and ShardLoom-native residual state.

Pushdown is still not complete as a uniform contract across every scenario family. Some rows expose
filter/projection pushdown evidence; others need explicit field coverage or blockers for filter,
projection, and limit/slice behavior.

## Required Pushdown Contract

Each prepared/native scenario family should report:

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

Existing `source_backed_scan_*` fields may remain the outer evidence envelope, but pushdown-specific
fields should be stable enough for benchmark rows, compute-flow documentation, and capability
matrix/status views.

## Capability Matrix Projection

Capability rows should classify each prepared/native scenario family as one of:

```text
scan_pushdown_supported
scan_pushdown_partially_supported
scan_pushdown_not_needed
scan_pushdown_blocked
scan_pushdown_unsupported
```

The row must name the supported or blocked dimensions separately for filter, projection, and
limit/slice. A projection-only success does not imply filter pushdown. A filter pushdown success does
not imply output projection correctness unless `output_columns_read` is also explicit. A limit/slice
success does not imply ordered top-N support.

Capability rows may reference Vortex Scan API evidence as a source/provider boundary, but they must
not report encoded-native operator execution unless a later certificate proves the operator stayed
encoded end to end.

## Scenario Family Rules

- Filter/project/limit scenarios should avoid reading unused output columns.
- Filter-only columns may be read for predicate evaluation, but they must not appear in the output
  stream unless requested.
- Projection-only scenarios should report the projected output column set and avoid unused columns.
- Limit/slice pushdown should be reported when admitted and blocked with a reason when unsupported.
- Unsupported predicates, expressions, nested-field paths, casts, joins, windows, or aggregates must
  emit deterministic blockers rather than fall back to another engine.

## Vortex-First Provider Check

- Subject area: Vortex Scan API pushdown completion for prepared/native benchmark rows.
- Upstream concept checked: Scan requests, Source/Split, projection pushdown, filter pushdown,
  limit/slice pushdown, field masks for filter-only versus output columns, and residual handling.
- Decision: wrap/admit Vortex scan concepts behind ShardLoom policy and evidence where available;
  emit deterministic blockers where a scenario cannot be lowered safely.
- ShardLoom evidence surface: `source_backed_scan_*`, `scan_*_pushed_down`, filter/output column
  lists, materialization/decode fields, claim gate, no-fallback status, and capability matrix rows.
- Residual handling: residual expressions remain ShardLoom-native or blocked. They must not be
  delegated to DataFusion, DuckDB, Spark, Polars, Velox, or another external engine.

## Non-Goals

- No encoded-native operator claim.
- No broad SQL/DataFrame runtime claim.
- No object-store/lakehouse runtime.
- No production performance or superiority claim.
- No fallback engine.
- No dependency expansion.

## Claim Boundary

Completing this lane may allow ShardLoom to say that scoped local prepared/native rows expose
filter/projection/limit pushdown or deterministic blockers at the Vortex Scan/source boundary. It
does not allow claims about encoded-native operators, generalized Source/Split runtime,
object-store/table scans, SQL/DataFrame execution, production readiness, or public performance.

## Acceptance

- Every prepared/native scenario family reports pushdown status or a deterministic blocker.
- Filter/project/limit scenarios avoid reading unused columns where evidence supports it.
- Filter-only columns are distinguished from output columns.
- Unsupported expressions are blocked, not executed through fallback.
- Capability views and benchmark rows can explain pushdown posture without reading prose.

## Verification Plan

Future implementation should include:

```text
selective filter smoke
filter+projection+limit smoke
source-backed scan tests
benchmark row contract tests
python scripts/check_website_readiness.py
git diff --check
```

Planning-only updates should run release-readiness metadata and website readiness checks.
