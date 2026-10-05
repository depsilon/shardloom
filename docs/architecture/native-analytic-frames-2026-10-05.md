<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native analytic frames

Status: complete frozen local acceptance and independent packet inspection;
hosted review/checks remain pending. This is the analytic-frame
continuation already required by the universal workflow queue. The preceding
consolidated engine has completed its independent local acceptance.

## Scope and ownership

Extend the existing relational window node under PERF-02/03/07/10/11/12 and
CG-3/5/19/20/21. SQL, Python, DataFrame and CLI declarations continue through the
same Vortex-normalized engine. This is one operator-family implementation, not
another execution mode or a benchmark-specific provider.

The existing window binder and kernel already own native input retention,
partition/order key identity, shared sorting, peer detection, cancellation,
result ordinals and delivery in input order. Extend those owners with framed
COUNT, COUNT DISTINCT, SUM, AVG, MIN, MAX, FIRST_VALUE, LAST_VALUE and NTH_VALUE.
Computed arguments use the existing native expression projection before the
window. Preserve the eight existing ranking/navigation functions and their
result types. A frame does not change ranking or LAG/LEAD navigation.

## Semantic contract

Admit ROWS, GROUPS and RANGE frames with explicit bounds, including empty frames,
and CURRENT ROW/GROUP/TIES/NO OTHERS exclusions. ROWS offsets count ordered rows;
GROUPS offsets count ordering peers. RANGE current-row bounds include peers.
Bounded RANGE requires one order key and a compatible nonnegative numeric or
fixed-duration offset. Bind types, offsets, bound ordering and function arguments
before reading rows, including for empty input. Named windows, variable offsets,
calendar-month intervals and IGNORE NULLS require separate declared semantics.

An omitted frame is RANGE UNBOUNDED PRECEDING through CURRENT ROW. Without
ordering all rows in a partition are peers. Retain original row ordinals as the
stable tie breaker for positional operations and retain input-order output.
FIRST/LAST/NTH respect NULL values; an empty frame or out-of-range NTH yields a
typed NULL. COUNT(*) counts frame rows, COUNT(argument) ignores parent NULLs, and
COUNT DISTINCT shares the existing exact native key domain. Empty SUM/AVG/MIN/MAX
yield typed NULL. Existing explicit null-order admission still applies.

These frame and NULL rules are grounded in the PostgreSQL 18
[value-expression reference](https://www.postgresql.org/docs/18/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS)
and [window-function reference](https://www.postgresql.org/docs/18/functions-window.html),
consulted October 5, 2026. They are semantic references only; no PostgreSQL code
or runtime is used.

Numeric RANGE offsets follow the bound order domain: integer offsets for integer
keys, F64 arithmetic for floating keys, and exact scaled decimal comparison for
decimal keys. Fixed durations address Date32 and TimestampMicros. F64 boundaries
round as ordinary F64 addition/subtraction; an overflowed boundary can compare
beyond all finite keys without becoming an admitted nonfinite observation.
This follows the [RANGE ordering support contract](https://www.postgresql.org/docs/18/btree.html)
and the [floating boundary semantics](https://doxygen.postgresql.org/float_8c.html)
inspected October 5, 2026. No source implementation was copied.

## Reuse and provider decision

| Existing owner | Extension |
| --- | --- |
| SQL scalar parser and relational projection lowerer | Parse frame declarations and preproject computed arguments through the existing expression IR. |
| `local_primitive_relational_window_bind` | Bind result types and frames; intern common partition/order groups before measure keys. |
| `native_relational_window` | Reuse ordering and peers; advance frame ranges and store results at original ordinals. |
| `native_relational_batch` and exact keys | Expose bounded retained key access; gather selected payloads without rebuilding decoded row trees. |
| Native aggregate type rules and decimal totals | Share numeric admission, decimal result types, checked wide totals and exact finalization. |
| Native capacity, execution context and result writers | Credit every state/output owner, check cancellation, deliver bounded batches and preserve atomic publication. |

Vortex-first decision: `implement_shardloom_kernel` for frame policy and moving
state, using the pinned Vortex 0.85.0 native array, validity, take and output
providers inside `shardloom-vortex`. The pinned `vortex-array` aggregate registry
has array/grouped aggregate functions, but no relational partition/frame
scheduler. Its SUM/MEAN policies also differ from the exact decimal contract
recorded in the preceding typed-reduction implementation. Neither a query-engine
integration nor Arrow execution is admitted.

This extends the existing ShardLoom window provider; it does not introduce a
second sort, key, scan, resource grant or writer. Ordering keys may require native
canonicalization and retained window state. Report those boundaries honestly;
native execution does not itself establish zero decode or streaming state.

## Moving state and arithmetic

Frame endpoints advance over each sorted partition. Represent exclusions as at
most three disjoint intervals and advance each interval's state independently.
Do not rescan a complete frame for every output row. Share peer boundaries and
sorting across expressions with the same partition/order specification.

Retain native row ordinals for extrema and value selection. Moving extrema use
reserved candidate ordinals and existing exact comparisons; selected values are
gathered only when emitted. DISTINCT uses shared native identity with reserved
occurrence counts. Decimal SUM/AVG reuse `native_decimal_reduce::Total` and its
fixed precision/scale and exact-average policy.

New floating framed totals need reversible updates. Plain floating subtraction
can lose a small remaining value after a large value leaves a frame. A fixed
integer accumulator over IEEE-754 coefficients can retain exact add/remove
state without a new dependency: 34 64-bit limbs cover every finite F64 value
times a UInt64 observation count in units of the smallest subnormal. Final SUM
or AVG rounds once to nearest, ties to even; only an unrepresentable final
result fails. AVG divides the wide total before rounding, so an oversized SUM
need not reject a representable mean. Reject nonfinite observations. Preserve
the existing primitive-to-F64 argument conversion policy. Existing grouped and
rolling floating calculation policies do not change in this unit.

The fixed accumulator is a new strategy required by reversible frame updates;
the existing ordered floating aggregate and rolling accumulators do not meet
that arithmetic contract. Prove it independently with exact rational fixtures,
subnormals, halfway cases, cancellation, removal and representable means before
admitting it. This design is not a performance result.

## Resources and failure

Reserve frame indexes, candidate queues, distinct counts, fixed totals and
full-length result owners before allocation. Preserve replacement peaks and
credits until the last output owner is released. Every substantial loop checks
cancellation. Binding, arithmetic, denied growth and sink errors must release
state and leave no published partial output.

This unit retains the existing window input/state memory boundary. Bounded
output batches do not imply bounded operator state or window spill. General
window state spill remains in the resource continuation and must fail explicitly
when the current grant is insufficient. Reservations are not an RSS limit.

## Acceptance

- [x] Implement frame binding and shared execution, including computed arguments,
  peers, exclusions, empty frames and all admitted logical value types.
- [x] Prove moving arithmetic and selected ownership with independent expected
  results, dictionary/chunk boundaries, input reordering and repeated execution.
- [x] Prove cancellation, constrained grants, empty-plan rejection and writer
  rollback through shared execution and resource reports.
- [x] Freeze complete public SQL/DataFrame/CLI workflows, all representable sinks,
  input-format coverage, required source checks and Full43 regression evidence.
- [ ] Complete hosted review and checks and update the canonical phase ledger.

Frozen source `22f1e6ba` passes 22,658 public checks and 15,349,350 complete row
comparisons, including 2,213 frame checks and 1,067,280 independently specified
rows. The separate direct matrix passes 202 checks; all 129 Full43 executions,
22 source gates, 145 admitted-semantics stages and nine golden stages pass.
The [acceptance report](../benchmarks/native-analytic-frames-full43-2026-10-05.md)
links the immutable packet and successful independent streaming inspection.
The earlier focused checks and interrupted attempts remain recorded separately.

Scalar-value subqueries, nested pivot extensions, adapters and general state
spill retain their existing owners. Website and hardware work follow core
completion; the revised release train follows complete core UAT. CG-1 through
CG-23 remain visible and retain their independent evidence requirements. Real
Vortex payload proof remains distinct from placeholder artifacts. No fallback,
external effects, release publication or competitive claim is introduced here.
