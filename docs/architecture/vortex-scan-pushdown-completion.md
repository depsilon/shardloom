# Vortex Scan Pushdown Completion

## Purpose

This document is the architecture reference for completed `GAR-PERF-2C` /
`GAR-RUNTIME-IMPL-4I` local prepared/native Vortex Scan API pushdown evidence across traditional
analytics scenario families.

The goal is not to claim encoded-native operator execution. The goal is to ensure every
prepared/native scenario either maps eligible filter, projection, and limit requirements into the
Vortex scan/source boundary or emits a deterministic unsupported/blocker row that explains why it
could not.

## Completed State

Prepared/native rows expose scoped `source_backed_scan_*` evidence for local Vortex source roles,
projected columns, provider scope, Native I/O status, materialization boundaries, residual executor,
no-fallback status, and claim gate.

`GAR-PERF-2C` / `GAR-RUNTIME-IMPL-4I` completes the stable `scan_pushdown_*` contract for the
current local prepared/native runtime. Each row reports filter, projection, and limit/slice
dimensions independently, distinguishes filter-only columns from scan output columns, and emits
deterministic blockers when a dimension is not admitted. Limit/slice pushdown remains blocked in the
current local prepared/native runtime because the covered limit-like scenarios require source-order,
ordered-top-k, grouped top-N, or window residual semantics. Those paths execute as explicit
ShardLoom-native residuals or remain blocked; they are not reported as Vortex Scan limit pushdown.

## Required Pushdown Contract

Each prepared/native scenario family reports:

```text
scan_pushdown_schema_version
scan_pushdown_report_id
scan_pushdown_status
scan_filter_required
scan_projection_required
scan_limit_required
scan_filter_pushed_down
scan_projection_pushed_down
scan_limit_pushed_down
scan_limit_requested_rows
scan_limit_request_scope
scan_filter_pushdown_status
scan_projection_pushdown_status
scan_limit_pushdown_status
scan_residual_limit_required
scan_residual_limit_applied
scan_residual_limit_status
scan_residual_limit_executor
scan_residual_limit_input_rows
scan_residual_limit_rows_output
scan_residual_limit_reason
scan_filter_columns_read
scan_output_columns_read
scan_filter_only_columns_read
scan_data_materialized
scan_data_decoded
scan_pushdown_blocker_id
scan_pushdown_blocker_reason
scan_pushdown_claim_gate_status
scan_pushdown_claim_boundary
scan_pushdown_fallback_attempted
scan_pushdown_external_engine_invoked
```

Existing `source_backed_scan_*` fields remain the outer evidence envelope. The benchmark harness
also emits a `scan_pushdown_matrix` artifact/report section so users and agents can inspect the
prepared/native pushdown posture without reading prose.

## Capability Matrix Projection

`compute-capability-matrix` projects the runtime contract as
`shardloom.prepared_vortex.scan_pushdown_matrix.v1`. Capability rows classify each current
prepared/native scenario family as one of:

```text
scan_pushdown_supported
scan_pushdown_partially_supported
scan_pushdown_not_needed
scan_pushdown_blocked
scan_pushdown_unsupported
```

The row names supported or blocked dimensions separately for filter, projection, and limit/slice. A
projection-only success does not imply filter pushdown. A filter pushdown success does not imply
output projection correctness unless `output_columns_read` is also explicit. A limit/slice success
does not imply ordered top-N support.

Capability rows reference Vortex Scan API evidence as a source/provider boundary, but they must not
report encoded-native operator execution unless a later certificate proves the operator stayed
encoded end to end.

The current matrix records:

- `scan_pushdown_supported`: CSV ingest metric scan, selective filter, group-by aggregate,
  multi-key group-by, hash join, join aggregate, wide projection, distinct count,
  high-cardinality string group/distinct, small-change CDC overlay, nested JSON field scan, and
  null-heavy aggregate rows.
- `scan_pushdown_partially_supported`: filter/projection/limit, sort/top-k, top-N per group,
  row-number/window, partition-pruning, clean/cast/filter/write, and malformed timestamp dirty CSV.
- `scan_pushdown_unsupported`: many-small-files and scale-stress rows without an admitted local
  Vortex Scan pushdown path.

## Scenario Family Rules

- Filter/project/limit scenarios avoid reading unused output columns.
- Filter-only columns may be read for predicate evaluation, but they must not appear in the output
  stream unless requested.
- Projection-only scenarios report the projected output column set and avoid unused columns.
- Limit/slice pushdown is blocked for the current local prepared/native runtime and must carry
  `scan_limit_pushed_down=false`, `scan_limit_pushdown_status=blocked_no_scan_limit_admission`, and
  a ShardLoom-native residual executor when the scenario executes.
- Unsupported predicates, expressions, nested-field paths, casts, joins, windows, or aggregates
  must emit deterministic blockers rather than fall back to another engine.

## Vortex-First Provider Check

- Subject area: Vortex Scan API pushdown completion for prepared/native benchmark rows.
- Upstream concept checked: Scan requests, Source/Split, projection pushdown, filter pushdown,
  limit/slice pushdown, field masks for filter-only versus output columns, and residual handling.
- Decision: wrap/admit Vortex scan concepts behind ShardLoom policy and evidence where available;
  emit deterministic blockers where a scenario cannot be lowered safely.
- ShardLoom evidence surface: `source_backed_scan_*`, `scan_*_pushed_down`, required-dimension
  flags, residual-limit rows, filter/output column lists, materialization/decode fields, claim
  gate, no-fallback status, benchmark `scan_pushdown_matrix`, compute capability rows, and Python
  typed client rows.
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

Completing this lane allows ShardLoom to say that scoped local prepared/native rows expose
filter/projection/limit pushdown or deterministic blockers at the Vortex Scan/source boundary. It
does not allow claims about encoded-native operators, generalized Source/Split runtime,
object-store/table scans, SQL/DataFrame execution, production readiness, or public performance.

## Acceptance

- Every prepared/native scenario family reports pushdown status or a deterministic blocker.
- Filter/project/limit scenarios avoid reading unused columns where evidence supports it.
- Filter-only columns are distinguished from output columns.
- Limit-like scenarios report ShardLoom-native residual executor and row-count evidence instead of
  claiming Vortex Scan limit pushdown.
- Unsupported expressions are blocked, not executed through fallback.
- Capability views and benchmark rows can explain pushdown posture without reading prose.

## Verification Plan

Implementation verification includes:

```text
selective filter smoke
filter+projection+limit smoke
source-backed scan tests
benchmark row contract tests
python scripts/check_website_readiness.py
git diff --check
```
