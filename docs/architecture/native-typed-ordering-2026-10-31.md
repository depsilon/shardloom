# Typed numeric comparisons in validated native ordering

Status: **dropped** after both complete cohorts; accepted runtime sources restored.
The 347-query total increases from 11.979219492 to 12.086481459 engine seconds;
the supplemental 48 total falls from 0.920503372 to 0.808468919. Both original
gates were required, so the accepted baseline is unchanged. See the
[complete experiment](../benchmarks/native-typed-ordering-2026-10-31.md).

The original prospective contract below was declared before runtime edits or candidate
query execution. This is the allocation-free comparison experiment from the
[shared-owner queue](native-shared-owner-optimizations-2026-10-30.md).
The accepted ordered-input checkpoint records 11.979219492 engine seconds for
the unchanged 347-query cohort and 0.920503372 engine seconds for the separate
48-workflow cohort. Those existing observations remain the baselines; do not
rerun them merely to start this candidate. Hosted integration of #1632 is a
separate prerequisite before declaring its final closeout.

## Hypothesis and scope

The generic numeric ordering path repeatedly constructs `AggregateDistinctValue`
and normalized `Cell` values after the ordering owner has already completely
validated its immutable input. The bounded Top-K owner already compares borrowed
typed primitive slices directly. Reuse its exact typed comparison semantics for
full sorting, window ordering, as-of build ordering and ordered collections.

Start with an allocation-free helper over the existing prepared numeric owners.
It may borrow typed views for the current comparison, but does not retain new
per-batch views or per-row locations. It removes repeated scalar wrapping and
value normalization/validation from an already validated matching domain.
Physical-type dispatch, validity checks and existing ordinal lookup remain;
do not claim those costs disappear. Keep the heap and ordered-input scan,
comparison sequence, NULL/direction handling and source-ordinal tie breakers.

General expression, hash/equality, nested-key, peer-boundary, as-of probe and
ordinary unvalidated key comparisons retain their current path. Grouped selectors
use pure group/source ordinals and are unchanged regression controls. Existing
Top-K behavior, eligibility, caps and resource accounting stay unchanged while
sharing the pure typed comparison implementation where practical.

Full `Sort::ordered_rows` also creates initial spill runs. Cover this reuse with
pressure correctness and ownership tests. The changing-block spill merge and
as-of search comparators are a separate boundary: `Cursor::load` prepares a
`Batch`, but this does not establish the same explicit complete-value validation
proof as the resident ordering owners. Preserve that generic path. This candidate
does not claim fewer spill generations, faster spill merging or faster probes.

## Why no retained typed-view table

A full-sort view table could retain O(batches × keys) borrowed metadata instead
of expanding the Top-K K-location tuple to N. Collections, however, emit result
chunks after sorting each group. Holding charged view metadata across all chunks
would overlap existing gather/compaction allocations; recreating it per group
would repeat preparation. A free-memory fraction does not prove the prior
output still fits. Eagerly mutating group ordering or changing output traversal
would also change a different contract. The allocation-free comparison unit
avoids those additional lifetime and memory behaviors. Any later retained-view
candidate needs its own measured hypothesis and resource proof.

## Provider check

Decision: `implement_shardloom_kernel`, reusing the existing ShardLoom comparison
over pinned Vortex 0.90.0 primitive arrays and validity masks. The checked Vortex
primitive comparison provider produces elementwise Boolean arrays from paired
operands; its floating comparison distinguishes signed zero and bitwise equality.
Its `IsSorted` and sorted-search providers do not replace ShardLoom's normalized
floating keys, per-key direction/NULL placement, cross-batch lexicographic order,
stable source ties or resource/cancellation contracts.

The helper uses existing prepared owners and adds no decode, canonicalization,
Arrow execution, source I/O, column copy, persistent cache or dependency. Preserve
the existing Vortex-native I/O and execution certificates, including
`fallback_attempted=false` and `external_engine_invoked=false`.

## Admission and failure contract

Admit only when the owning operation has validated all compared values in its
original complete domain/order and established matching flat domains across
retained batches. Reuse its existing metadata proof. Numeric operands must
have the same primitive type and float policy. Signed/unsigned integer widths
and f32/f64 are eligible; F16, mixed types, differing policy and all nonnumeric
keys keep the original comparator. NULL bits and bounds remain authoritative.

Borrow primitive slices directly from the existing owners. Compare integers
at their original widths, preserving exact large-u64 behavior. Float32 widens
to f64 exactly as before; f32/f64 keys apply the existing `float_key_bits`
normalization before total comparison. Preserve signed-zero equivalence, extended
NaN identity, strict nonfinite refusal, explicit NULL policy, mixed directions
and stable ties. No arithmetic reassociation is introduced.

Unproven admission must not alter comparison/error sequence or error text.
Keep complete input validation, including invalid later values and discarded
payload checks. Keep original ordinal reservations, payload gathering, memory
ownership and final refunds. No auxiliary allocation, capacity growth or retained
result state is added. Cancellation polling stays in the existing ordering loop.

## Proof before timing

Use independently constructed expected orders plus the unchanged generic path
for all integer widths/endpoints, f32/f64 policies, NULLs, ties, multikey and
mixed directions, empty/singleton inputs and changed batch boundaries. Cover
primitive, constant, dictionary and chunked owners without claiming new encoded
execution. Include nullable parent validity, invalid late keys, changed domains,
explicit NULL-policy failures, memory refusal, cancellation and final refunds.

Demonstrate activation independently for full sort, windows, as-of build and
ordered collections. Count typed comparisons and ordinary numeric scalar
conversions in debug tests; prove the admitted comparator avoids the latter.
Assert that ordinal lookups and comparison counts/trace remain equivalent where
their existing test controls expose them. Declines, selectors, Top-K caps and
nonordering users remain regression controls. Diagnostic counters must be absent
from optimized shipping artifacts; inspect the relevant optimized symbols.

Run focused native tests, required default/native workspace tests, Clippy and
formatting, source Python checks, fresh complete public-interface controls and
installed-wheel acceptance. Reuse the previously frozen complete references
through SQL/DataFrame/typed/CLI routes and Vortex/Parquet/Arrow files. Exercise
full-result retention beyond a collect limit through the established export
boundary. Pressure controls must cover initial run creation, stable merge
results, cancellation/failure cleanup and no additional charged allocations.

## Prospective disposition

After correctness, resource and package acceptance, execute the unchanged 347
queries once with the candidate and compare their complete engine total against
11.979219492. Separately execute the same frozen 48 workflows once with the
candidate and compare their complete total against 0.920503372. Preserve all
declarations, inputs, references, grants, parallelism, limits, engine clock and
source identities. Both complete totals must be lower; either unchanged or
higher total drops the candidate. No selected case or family rescues a loss.
Retain all regressions, failed attempts and single-observation limits.

Timed passes are isolated: stop unrelated task-owned work and keep agents,
builds, packaging and profiling idle. Keep standard supervision, storage ceilings,
deadlines, lightweight concurrency guards, complete-result checks and cleanup.
Full hashing and recursive inventories occur outside query execution. Do not
manipulate caches, pace queries or add repeats. Diagnostic work is separate.
Portable complete evidence and source/build binding precede temporary retirement.

Full43 stays held until item 18. This unit remains inside RFC 0044/item 20 and
does not complete the larger shared-owner queue, the under-ten objective,
CG-1 through CG-23 or package publication.
