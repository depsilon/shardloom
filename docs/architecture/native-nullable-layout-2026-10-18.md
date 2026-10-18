# Native nullable structural-layout correctness

Status: implementation and local acceptance complete under PERF-07, with PERF-06
private native-run readback verification. Hosted checks and merge remain pending.
This is a scoped correctness unit; the whole PERF owners and CG-1 through CG-23
remain open. See the [acceptance report](../benchmarks/native-nullable-layout-2026-10-18.md).

## Decision and contract

The October 18 maintainer request resumes the remaining queue after a finite
review of dropped optimizations. The first concrete source/type defect is the
[nullable-root finding](native-file-metadata-buffers-2026-10-16.md#provider-test-integration-and-separate-nullable-root-finding).
The previous byte-buffer intake deliberately left this defect separate. A
default structural-layout scan must return the same logical DType and values as
the written native array, including a null record whose stored child values are
non-null. A null record is distinct from a valid record containing null fields.

Whole-root and `select` projections must preserve parent validity and the
original child DTypes. Field access must apply parent validity before subsequent
operators, including `is_null`, comparisons and expressions producing a new
record. Literal expressions must retain their declared semantics even on null
input rows. Filtering must evaluate the same expression semantics as projection;
child-only statistics must not discard rows whose parent validity changes a
predicate's answer. Row ranges and incoming selections apply identically to
fields and validity.

The expert comparator is a native columnar scan implementation that preserves
typed nested validity through pruning, partitioned expression evaluation and
persisted readback, without eagerly loading unreferenced columns.

## Vortex-first provider check

- Subject: nullable `StructLayout` reads in the pinned Vortex 0.85.0 provider.
- Decision: `use_vortex_native_provider`, correcting the vendored reader inside
  the existing `shardloom-vortex` native boundary.
- Existing surfaces: `StructLayout`, `BoundExpression`, native `Mask`/`Pack`
  expressions, `partition_bound`, `BoundPartitionedExprEval`, and the layout's
  existing boolean validity child. No query-engine integration is involved.
- Approach: represent parent validity as a private partition dependency and
  apply it when reconstructing the original root or accessing a field. Evaluate
  surrounding expressions only after that mask. Keep private partition names
  disjoint from user field names. Preserve the non-nullable reader path.
  Whole-record null tests read only the validity child; literals read no columns.
- Pruning: retain incoming candidate rows whenever a nullable partition cannot
  safely be reduced using child statistics. Correctness precedes pruning gains.
- Admission and evidence: existing feature gates, memory ownership, explicit
  output/spill policy, native I/O and execution certificates remain in force.
  Provider tests alone do not certify a ShardLoom spill workflow.
- Representation: use Vortex arrays and native expressions. Do not introduce a
  decoded Arrow execution substrate, an alternate file format or eager whole-file
  materialization. Output encoding identity is not a promise of every expression.
- Residual execution remains Vortex-native or explicitly unsupported;
  `fallback_attempted=false` and `external_engine_invoked=false` remain required.

## Alternatives and risks

Changing only the final root DType would leave null-sensitive filters incorrect.
Masking every result after expression evaluation also changes literal and
`is_null` semantics. Reconstructing every input column before evaluating any
expression would discard field pruning. The selected design uses the existing
native partitioner and adds only a private validity dependency.

Nullable scans may need to read a validity partition that the faulty filter path
previously omitted. This is required work, not a speed claim. Preserve typed
empty results, nullable all-valid arrays, nested masks and hidden child values.
Keep the on-disk format and public schemas unchanged. Inspect retention and
cleanup when results pass through ShardLoom's owned arrays and query-run store.

## Acceptance checklist

- [x] Capture the existing whole-root failure before the provider change.
- [x] Prove whole-root, field, reordered/empty selection, new-record and literal
  projections against direct native expression evaluation.
- [x] Prove null-sensitive filters, conservative pruning, row ranges and incoming
  selections, including hidden non-null child values under a null parent.
- [x] Cover empty, all-valid, all-null and mixed validity, nested records and an
  encoded child; verify unreferenced fields are not read.
- [x] Prove default native-file write/read and ShardLoom owned-result readback.
- [x] Prove nullable-root query-run round trip, source/type identity, retention
  and owned cleanup; retain existing cancellation, corruption and quota checks.
- [x] Wire provider regressions into hosted CI; run focused provider checks,
  required workspace format/lint/tests and relevant native/public acceptance.
- [x] Review the patch adversarially and preserve exact validation receipts.
- [ ] Complete authorized PR, hosted checks and merge, then record accepted
  scope and advance the remaining queue without closing broader PERF/CG gates.
