<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native engine optimization review

Status: source review, October 23, 2026. Reviewed revision: `013bc6d2`, whose
runtime matches accepted commit `7c65f339473e1f937d08da4b33fb538c00c171f7`.

The strongest next work unit is purpose-specific column preparation. Several
native operators use a hash-key owner simply to read or compare values. That
owner eagerly hashes every string or binary value in its domain. Aggregation
also prepares the same source column separately for multiple measures. Removing
unneeded preparation has a narrower correctness boundary than changing ordering,
scheduling or physical layout.

This review follows execution from prepared expressions into aggregation,
ordering, joins, scans and sinks. Priorities below rank observable unnecessary
work and implementation confidence, **not measured attainable speedup**. The
recent shared-provider admission work already makes existing specialized
providers available to equivalent public plans; the remaining findings concern
the operators themselves and compositions outside those admitted shapes.

## Priorities and affected work

| Order | Candidate | Work visible in current source | Primary workload needed to evaluate it |
| --- | --- | --- | --- |
| 1 | Separate value access from hash preparation | String/binary hash computation and an eight-byte hash slot per domain entry even for value-only consumers | Native expressions, string extrema and composed ordering over plain and dictionary arrays |
| 2 | Reuse preparation within aggregate batches | Repeated column-owner construction, distinct-key preparation and per-row scalar count updates | Multiple measures over one column; aggregate after a join, expression or memory source |
| 3 | Resolve sort row locations once per comparison | Four segment searches for each non-null key comparison | Composed multikey sorting across many input batches |
| 4 | Carry finite output demand into sorting | Full output gathering before a following limit discards most rows | Composed sort with limit and offset, including wide payloads |
| 5 | Keep predicate results in masks | Recompile a bounded recipe per batch, expand truth words into values, then inspect each value again in the filter | Admitted pure compound I64 predicates on composed inputs |
| 6 | Retain only existence state for eligible semi/anti joins | All right rows and duplicate index links retained although probing asks only whether a match exists | Duplicate-heavy right inputs with wide payloads and no residual join condition |

## 1 Separate value access from hash preparation

[`keys`](../../shardloom-vortex/src/local_primitives/native_relational_expression.rs#L471)
constructs a `KeyColumn` for scalar arguments, casts, comparisons and aggregate
values. [`VariableColumn::new`](../../shardloom-vortex/src/local_primitives/native_relational_keys.rs#L558)
reserves `8 * domain.len()` bytes, prepares the Vortex value domain and dictionary
codes, validates UTF-8 where applicable, and computes one hash for every valid
domain value. For dictionary arrays this is domain-sized, not necessarily
row-sized. [`hash_into`](../../shardloom-vortex/src/local_primitives/native_relational_keys.rs#L293)
consumes those hashes; raw value access and comparison do not.

Consequently binary byte length, text functions, comparisons and string extrema
can pay for hashes they never consume. Composed sorting additionally calls
[`batch.hash`](../../shardloom-vortex/src/local_primitives/native_relational_sort.rs#L77)
for every input row and discards the result. That call also validates values, so
deleting it without replacing the validation is incorrect.

Make preparation purpose-specific within the existing native owner: validated
value access, comparison and hash-key use should request only their necessary
state. Use existing Vortex arrays and dictionary domains. The
[primitive sort column](../../shardloom-vortex/src/local_primitives/native_sort_block.rs)
already demonstrates a comparison owner without a string-hash buffer. This is
not a request for another array representation or a cross-query cache.

Preserve full-domain UTF-8 validation where it is currently observable, dictionary
code checks, null behavior, cancellation and credited ownership. Text validation
can still require walking bytes; avoiding hashing does not imply avoiding every
byte read. Hash consumers must retain their reservation and collision-safe full
equality checks. Existing
[dictionary/hash-reservation tests](../../shardloom-vortex/src/local_primitives/native_relational_keys.rs#L858)
provide a starting point: add value-only controls that do not reserve hash slots,
while genuine hash consumers still reject insufficient grants. Exercise invalid
unreferenced dictionary values as well as invalid referenced codes.

## 2 Reuse preparation within aggregate batches

[`consume_inner`](../../shardloom-vortex/src/local_primitives/native_relational_aggregate.rs#L227)
allocates a zero-filled group-ordinal vector for ungrouped input, then walks the
measures. Each measure resolves its field and prepares a new `KeyColumn` before
updating rows. `SUM(x)` and `AVG(x)` therefore prepare `x` independently. This
establishes repeated preparation, not necessarily repeated decoding: an already
canonical array may not need another physical decode.

[`accumulate_distinct`](../../shardloom-vortex/src/local_primitives/native_relational_aggregate.rs#L497)
uses an already-prepared owner for null checks, constructs group/value input,
then passes it to [`RowSet`](../../shardloom-vortex/src/local_primitives/native_relational_set.rs#L103),
which prepares keys again. The unique rows retained by the set need their own
durable owners; distinguish that ownership transfer from redundant preparation
of the incoming batch. Separate distinct measures currently own separate sets.

For ungrouped `COUNT(*)`, the
[ordinary update](../../shardloom-vortex/src/local_primitives/native_relational_aggregate.rs#L569)
increments a checked counter once per row. A checked batch-length addition can
avoid that loop and its zero-ordinal vector when no other measure needs them.
The source still has to be drained and validated. Root-file count already has a
metadata provider; this opportunity concerns compositions that cannot use it.

Bind the consumers of a source column together, prepare compatible views once
per batch, and release them at their last consumer. Do not retain every column
for the whole batch by default: sharing can increase peak live memory if owners
that were previously sequential overlap. Preserve measure/error order, checked
integer overflow, floating accumulation order, nullable and decimal results,
dictionary identity and independent distinct semantics. Identical distinct
measures may share state only after their equivalence is established.

The primitive aggregate family already has
[bound numeric updates](../../shardloom-vortex/src/local_primitives/bound_numeric_updates.rs)
and dictionary-aware strategies. Reuse an admitted strategy where its semantics
match; there is no basis for a claim that ShardLoom lacks typed/vectorized kernels.
Acceptance needs preparation counters, peak credited memory and complete results
for mixed repeated/unique measures, empty batches, nulls, overflow, cancellation
and late source errors.

## 3 Resolve sort row locations once per comparison

The [composed comparator](../../shardloom-vortex/src/local_primitives/native_relational_sort.rs#L102)
calls `Table::key_is_null` for both rows, then `Table::compare_key` for a non-null
pair. Each null check resolves the global row ordinal, and comparison resolves
both rows again. [`Table::locate`](../../shardloom-vortex/src/local_primitives/native_relational_batch.rs#L228)
uses a partition-point search over retained segments. At the source level this
requests four segment searches per evaluated non-null key, repeated for subsequent
equal keys; counters or generated-code inspection must establish how much of this
work remains after compilation.

Resolve the pair of segment/local-row coordinates once per comparator call and
reuse them across its keys. That avoids introducing a second full-row index.
Keep fallible comparison, null placement, cancellation and the global input
ordinal tie-break. This change is independent of top-K and can be evaluated
without changing which rows are sorted or gathered. Tests must span many uneven
and empty input batches, equal keys, mixed null placement, nested/decimal keys
and rejected values; a single-batch benchmark would conceal most of the target.

## 4 Carry finite output demand into sorting

[`Sort::Spec`](../../shardloom-vortex/src/local_primitives/native_relational_sort.rs#L23)
has no finite output demand. Its finish path allocates and sorts every ordinal,
then gathers every output column for every sorted row. A subsequent
[`Limit`](../../shardloom-vortex/src/local_primitive_relational_transform.rs#L73)
discards batches after its requested range while continuing to drain the child.
The binder's
[`cap_rolling_prefix`](../../shardloom-vortex/src/local_primitive_relational_transform_bind.rs#L139)
does not carry this demand through a sort.

The first candidate is explicit demand at the sort boundary and gathering only
the required prefix or range. It still retains all input and sorts all ordinals.
Bounded candidate retention is a separate algorithmic step; the primitive top-K
path cannot simply be substituted for every nested, decimal or tie-policy case.

There is a substantial correctness condition beyond draining the source:
[`native_payload::copy_rows`](../../shardloom-vortex/src/local_primitives/native_payload.rs#L356)
validates selected payload values under `ValidateValues`. Current full gathering
can expose invalid **non-key** values in rows that the outer limit will discard.
Streaming input detachment and ordinary retained file input also have different
validation boundaries. An optimized gather must preserve the existing validation
obligation, including its required diagnostic behavior, without relying only on
sort-key validation. This makes the change less isolated than it first appears.

Acceptance requires late invalid keys and non-key payloads outside the requested
range, nested values, stable ties, offset overflow, zero limits, denied grants,
cancellation and explicit spill behavior. Do not change the existing distinction
between a zero limit on ordinary input and a finite streaming source that still
owes validation. Preserve required source consumption before attributing a gain
to reduced gathering.

## 5 Keep predicate results in masks

[`Expression::evaluate`](../../shardloom-vortex/src/local_primitives/native_relational_expression.rs#L81)
attempts bounded predicate-recipe compilation on every batch. The admitted
[predicate block](../../shardloom-vortex/src/local_primitives/native_relational_predicate_block.rs#L53)
computes true/false bit words, then expands them row by row into a nullable Boolean
array. The [filter consumer](../../shardloom-vortex/src/local_primitive_relational_transform.rs#L47)
prepares that array as keys and visits every row again to construct its selection.

Bind the immutable program once for the supported pure I64/Boolean family and
let filter evaluation consume a native mask. Vortex already provides
[`null_as_false().execute`](../../vendor/vortex-array/src/mask.rs#L45), which folds
Boolean validity into a mask, and its mask type can represent the predicate
result. First check whether this existing provider removes the conversion work
before adding a specialized direct producer.

Projected Boolean expressions still need three-valued results. Preserve lazy
`CASE`, `COALESCE` and `NULLIF`, selected-row evaluation, errors and resource
accounting; this finite pure program does not authorize general eager expression
fusion. Test nullable truth tables and late errors, repeated batches, all/none
selection and lazy branches with invalid unselected values. The previously
rejected contiguous-range selection mechanism is not part of this proposal.

## 6 Retain only existence state for eligible semi and anti joins

[`Join::build`](../../shardloom-vortex/src/local_primitives/native_relational_join.rs#L63)
retains the right `Table` and inserts every non-null key row into `RowIndex`.
The [index](../../shardloom-vortex/src/local_primitives/native_relational_index.rs#L69)
keeps duplicate row links. With no residual condition, left semi/anti
[probing](../../shardloom-vortex/src/local_primitives/native_relational_join.rs#L110)
uses only `entry.is_some()` and emits left rows; it never traverses those duplicate
links or gathers the right payload.

Admit an existence-only build strategy for keyed left semi/anti joins without a
residual predicate. Retain one complete key per distinct matching value and no
unused right payload after the required validation. Existing `RowSet` demonstrates
unique-key ownership, but its null-equality policy must not be copied into join
semantics. Hashes remain candidate lookups, never proof of equality.

Keep the general strategy for residual predicates and other join kinds. The
explicit spill path needs its own admission and equivalence checks. Validate
null keys, dictionary domains, forced collisions, duplicate-heavy inputs, empty
sides, late failures and tight memory grants. Existing
[join tests](../../shardloom-vortex/src/local_primitives/native_relational_join_tests.rs)
and [index collision tests](../../shardloom-vortex/src/local_primitives/native_relational_index.rs#L194)
are useful controls. Peak retained state is the first measure here; runtime
improvement depends on the input's duplicates and payload width.

## What the current measurements establish

The accepted [five-surface evidence](../benchmarks/native-shared-surfaces-2026-10-23.md)
contains one complete Full43 execution per surface with complete result and schema
checks. CLI SQL totals **52.413672785 engine seconds**. Its twelve slowest queries
sum to **29.757477413 engine seconds**, about **56.77%** of that total:

| Query | Engine seconds | Query | Engine seconds |
| --- | ---: | --- | ---: |
| Q19 | 4.722239957 | Q28 | 2.237020584 |
| Q23 | 3.293213833 | Q33 | 1.895134709 |
| Q29 | 3.118414707 | Q10 | 1.813909501 |
| Q35 | 2.682682500 | Q14 | 1.661836125 |
| Q34 | 2.568378916 | Q16 | 1.641411124 |
| Q17 | 2.526202500 | Q15 | 1.597032957 |

Q23 reports 7,128 selected rows across 1,313 read arrays, making preparation work
worth examining. Q34 and Q35 each report 9,038,838,007 UTF-8 bytes hashed and zero
native dictionary chunks in their worker counters. Those are genuine
group-key consumers: their necessary hashing is **not** evidence of the unused
generic hashes described in finding 1. Neither counters nor overlapping internal
phase clocks establish an achievable engine-time reduction.

The shared provider can execute a relationally expressed root. In particular,
Q30/Q36 having a `native_vortex_relational_collect` entry route does not establish that they
exercise the generic composed sort/aggregate implementation. Provider evidence,
not entry-route labels, must determine coverage. Current Full43 is a regression
control but does not measure the six candidates comprehensively; joins and
multi-operator compositions need targeted workloads.

The typed-expression reports omit `aggregate_update_strategy` for all 43 queries;
DataFrame reports contain it for 23. This limits cross-surface strategy comparison,
even though complete results and provider evidence pass. Missing fields do not
establish missing execution or provider divergence. Align these existing evidence
fields when instrumenting the chosen work, without turning this review into a
new reporting framework.

## Areas that need a narrower investigation

- **Column liveness below the frontend.** SQL already has
  [backward pruning](../../shardloom-cli/src/sql_native_relational_pruning.rs).
  Direct plans bind explicit scan projections. A shared dependency pass may
  avoid wider reads, but must preserve validation of explicit expressions and
  every ordering, window and set dependency. Measure actual projected/read
  columns before calling this an execution benefit.
- **Composed scan scheduling.** The
  [scan runner](../../shardloom-vortex/src/local_primitive_relational_scan.rs)
  uses bounded ordered ranges with concurrency one. That is a concrete scheduling
  choice, not proof of an I/O bottleneck. Establish memory ownership, cancellation,
  source generation and downstream backpressure before testing another schedule.
- **Correlated subqueries.** The
  [subquery runner](../../shardloom-vortex/src/local_primitives/native_relational_subquery.rs)
  executes a fresh inner instance for each selected outer parameter; uncorrelated
  instances already have reuse. Decorrelation or parameter reuse requires an
  explicit contract for determinism, errors, null semantics, source changes and
  bounded memory. It is a larger algorithmic work unit than the first candidates.
- **Primitive hot-path costs.** Q19/Q23/Q29 and the high-volume string grouping
  families warrant targeted investigation if Full43 improvement is the objective.
  Existing typed updates, dictionary strategies and late measures are already
  active. Profile the selected strategy before replacing it or changing input
  layout; the present measurements do not identify a general missing kernel.
- **Result ownership and sinks.** The
  [sink batch collector](../../shardloom-vortex/src/local_primitive_native_sink_batches.rs)
  already passes through a single array and uses explicit native ownership when
  coalescing multiple arrays. This review does not establish an unnecessary copy
  there. Preserve that boundary instead of broadly labeling materialization waste.

## First implementation and acceptance boundary

Start with a cohesive value-preparation change: purpose-specific owners, callers
that require values versus hashes, and bounded reuse for repeated aggregate
consumers. Keep the sort locator, finite-demand, mask and existence-join changes
separate until that contract is established. A file split alone is not an
optimization, and changing every operator at once would obscure both correctness
and cost attribution.

Before implementing, fix the workload shapes, encodings, input identity, resource
grants and material-benefit criterion. Use complete-result comparisons plus
malformed-input and resource-denial controls. Record actual preparation counts,
hash slots/bytes and peak live reservations; reductions in those quantities are
mechanistic evidence, while an engine-time comparison establishes performance.
Preserve floating order, complete required validation, cancellation, source
lifetimes and native Vortex output. Keep all provider use inside the existing
versioned, admitted, certificate-backed boundary, with no external-engine fallback.

Run workspace/native correctness gates after a cohesive implementation and one
isolated standard Full43 as its regression check. Targeted comparative runs need
a stated decision purpose and must exercise the changed path. No new engine run,
runtime change or speedup claim is part of this source review.

The existing evidence packet passed read-only replay against the current runtime
source: 215 final query results, 43 matching cross-surface schemas, 215 small-fixture
comparisons and 370 accepted historical query results. The report's source links
and line anchors were checked. Workspace builds and execution tests were not
repeated for this documentation-only change; the linked acceptance packet retains
their results for the unchanged runtime.
