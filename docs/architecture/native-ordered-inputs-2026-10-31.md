# Preserve already ordered validated native inputs

Status: prospective item-20 experiment, declared before implementation or new
query execution. The accepted runtime is the compact grouped-state checkpoint,
merged in #1630 as `3b4c7d92`, with **11.997152207 engine seconds** for the
unchanged 347-query cohort. The rejected mask and adjacent-read experiments
remain dropped. This is the order-aware sorting candidate from the
[shared-owner queue](native-shared-owner-optimizations-2026-10-30.md).

## Current operation and evidence boundary

`native_relational_order::sort` always constructs and drains a heap. Its callers
include full relational sorting, analytic window ordering, as-of build ordering,
ordered collection aggregates and grouped selectors. These owners already keep
charged source ordinals and prepared native keys; stable comparators finish with
the original ordinal. Top-K selection has a separate bounded heap and remains
unchanged in this experiment.

An exact walk of the saved 347 declarations finds 256 sort nodes: 187 over
aggregate results, 59 over projections, four each over DISTINCT and set results,
and two over filters. All 19 join nodes are ordinary equality-key joins without
residual conditions; no window node is present. Saved execution reports show no
requested or actual spill. These facts establish coverage limits, not ordered
input or a number of avoidable comparisons. Existing ordering retention counters
do not measure monotonicity. Do not infer activation or savings from a family
name, an output's sorted order, or summed peak-retention fields.

## Vortex-first provider check

Decision: `implement_shardloom_kernel`, extending the existing native ordinal
sorter rather than creating another array or ordering representation. Pinned
Vortex 0.90.0 supplies prepared arrays, validity and encoded value execution.
Its `IsSorted` aggregate and exact sortedness statistics describe ascending
array order. The checked provider in
`vendor/vortex-array/src/aggregate_fn/fns/is_sorted/mod.rs` does not implement
ShardLoom's cross-batch, multiple-key comparator with per-key direction, explicit
NULL placement, immutable float policy and source-ordinal ties. It also does not
replace the existing complete-value validation contract.

The proposed check uses already prepared native key owners and the exact
existing comparator. It adds no canonicalization, Arrow execution, source read,
decode, materialization, persistent statistic or answer cache. Existing Vortex
input/output and provider certificates remain authoritative. No dependency,
external engine or query-engine integration is introduced;
`fallback_attempted=false` and `external_engine_invoked=false` remain required.

## Admission and algorithm

Preserve the current fallible heap sorter and its comparison sequence for
unproven domains. Add a separate entry point that may check adjacent ordinals
only when the caller establishes both conditions:

- Every compared value has already been validated in the owner's original
  complete input domain and order, including NULL-policy and nonfinite checks.
- Relevant prepared keys have matching, supported flat comparison domains
  across every retained batch. Check numeric width and float policy, variable
  binary/text kind, decimal metadata and temporal metadata explicitly. Nested,
  missing, incompatible or otherwise unproven key domains decline the check.

The domain proof reads metadata only, adds no allocation, and checks
cancellation while traversing batches and keys. It is not a replacement for
value validation. Grouped-selector ordinals compare only known group indexes
and source indexes, so their existing comparator is independently infallible.

For admitted input, inspect adjacent entries using the existing comparator and
return unchanged if every pair is nondecreasing. A nonempty ordered input of N
rows needs exactly N-1 comparisons and no swaps. Stop at the first inversion
and invoke the unchanged heap algorithm on the untouched ordinals. Check
cancellation before, during and after the scan, with at most the existing
1,024-comparison polling interval. Empty and singleton inputs retain final
cancellation checks.

Apply the same mechanism to full sort, window ordering, as-of build ordering,
ordered collection aggregates and grouped selectors. For collections, establish
the immutable key-domain proof once per retained collection state, not once per
group. Do not change the Top-K heap, payload gathering, validation domains,
reserved capacities, shared allocation ownership or final refunds. Reverse-run
reversal, run merging, typed comparator reuse and spill merge geometry remain
separate hypotheses.

## Correctness and mechanism acceptance

Keep exact output values, metadata, NULL order, mixed directions, stable ties,
float identity and original error precedence. Exercise already ordered, random,
reversed, one-inversion and short/long-run inputs, including ties and changed
batch boundaries. Compare complete results with independent references and the
ordinary native sorter where appropriate. Show that declined admission retains
the original comparison/error trace. Invalid later keys, malformed discarded
payloads and unsupported cross-batch types must still fail at their original
boundaries. Include cancellation during both paths, zero/one-row inputs, memory
denial, retained results after source/session release and complete credit refunds.

Native tests must prove ordered-check activation and removed heap comparisons,
as well as ordinary-heap use after inversion and admission decline. Count
comparisons without production instrumentation. Reuse the existing debug-test
observation boundary for owner activation; shipping builds contain no candidate
diagnostic timers or counters. Cover each of the five callers, not merely the
common helper. Inspect optimized code for the intended early-return boundary.

Run focused native checks followed by required default/native workspace tests,
Clippy and formatting, source Python checks, fresh public-interface acceptance
and installed-wheel tests. Fresh complete-result controls span all five public
interfaces and Vortex, Parquet and Arrow files, with sorted and shuffled inputs,
NULLs, ties, renaming/projection compositions and multiple producer batches.
Use independent expectations frozen before native execution. Large result
retention is a correctness/resource control, not a throughput claim.

## Prospective complete-workflow decision

The ordinary 347 suite is unchanged. After all correctness/resource/package
gates, run exactly one isolated execution per case with the established runner,
same original inputs, references, grant, parallelism and engine clock. Lower
complete engine time passes its benefit gate; unchanged or higher drops the
candidate under the standing maintainer rule. Preserve all case/family regressions
and original observations. Do not retain a losing candidate from selected cases.

Broader routes additionally require a separately frozen 48-workflow cohort:
12 templates at 0, 33, 4,097 and 65,537 rows. Templates are four full-sort shapes
(ascending, shuffled, reversed, and nullable mixed direction), ordered/shuffled
partitioned windows, ordered/shuffled as-of builds, ordered/shuffled collection
aggregation, and ordered/shuffled grouped selectors. Define exact schemas,
fresh deterministic seeds/layouts, typed/SQL declarations, complete independent
references and input identities before either runtime executes the cohort.
Use one fresh native session per complete operation and the engine-owned clock.
Both versions consume and validate complete results. Keep result serialization
and independent checking outside the engine clock.

Execute one isolated pass with the accepted frozen runtime and one with the
candidate, with no routine repeats. The supplemental complete engine total must
also be lower; report its outcome separately from the 347 observation. Neither
cohort may rescue a loss in the other. Preserve every attempt, failure and
regression. Diagnostic work must run separately from ordinary observations.

Keep current storage ceilings, deadlines, lightweight concurrency guards and
process cleanup. Finish other task-owned jobs and keep agents, builds, packaging
and profiling idle during each timed pass. Full hashing and recursive inventories
belong before/after execution. Do not manipulate caches, add pacing or run a
competitor. Retain portable complete evidence before retiring temporary payloads.
Full43 stays held until item 18; CG-1 through CG-23 and publication remain separate
gates. The under-ten-second objective is not an acceptance claim.

## Supplemental fixture declaration

Before either runtime executes, the independent generator in
[`native_ordered_input_cases.py`](../../python/tests/native_ordered_input_cases.py)
defines all 48 cases in size-major order. Each input has nonnullable `id: u64`,
`grp: i16` and UTF-8 `payload`, plus nullable `ord: i64`, `minor: i32`,
`value: i64` and UTF-8 `text`. Seven groups, repeated ordering keys, NULL keys,
NULL values, empty strings, Unicode and integers above 2^53 are explicit.
Physical source position resolves ties; logical `id` is deliberately not a
tie-breaking query key.

The four full sorts project/rename `ord` to `ordered_key`; the mixed variant
orders that key ascending with NULLs last and `minor` descending with NULLs
first. Partitioned windows compute `ROW_NUMBER` and preserve source order.
As-of operations preserve a separate reverse-logical-order probe input and
choose the last right-source occurrence at the greatest eligible key.
Collections produce complete ordered nullable integer arrays and joined text.
Selectors combine FIRST, LAST, ARG_MIN and ARG_MAX with aggregate-local order.
Ordered and shuffled variants retain the same logical population. Reversed
sort input reverses ties as well as keys, making stability observable.

Inputs are fresh Parquet files with Zstd compression, dictionaries and 2,048-row
row groups, reopened through PyArrow for complete schema/value comparison.
Python standard-library operations generate every complete reference before
native execution; PyArrow performs file I/O only. Exact SQL, references, inputs,
driver and frozen Python harness identities are retained in the supplemental
protocol. Each operation runs through native Python SQL with a fresh session;
the result is exported only after its plan/session are released. The existing
standard supervisor and lightweight guard provide deadlines, storage limits,
process cleanup and observations. The unchanged 347 runner is separate.

The accepted binary's existing build/source manifest remains authoritative.
The new fixture and observer are explicitly outside that earlier build and
receive their own immutable identities. Pure reference controls check complete
populations, NULL and stable-tie expectations, and compare the as-of bisect
reference against an exhaustive independent selection on small inputs. No
native query or runtime change is needed to prepare these controls.

## Implementation clarification before candidate execution

Collection completion receives one output chunk of groups at a time. Review of
the first implementation found that placing its metadata proof at that boundary
could repeatedly scan every retained batch. The corrected implementation proves
all collection states before the aggregate's output loop and passes one stack
boolean to each chunk. If any state's domain is unproven, all collection measures
conservatively use the original sorter. No cache, retained state or allocation is
added. Window groups similarly share their retained table's metadata proof.

Regression checks cover multiple collection states and window order groups,
one-row input batches and output batches of one, seven and 33 rows. They assert
complete values, fixed metadata-check counts and full memory-credit release.
The first full native test pass remains recorded, and checks run again for the
correction. Validation domains, query declarations, reference values and both
prospective complete-cohort benefit gates are unchanged; no candidate benchmark
has executed before this correction.

## Recorded complete-cohort outcome

The candidate is retained under both original gates. The unchanged 347-query
cohort passes all 12,565 result rows at **11.979219492 engine seconds**, against
11.997152207. The separate 48-workflow cohort passes all 557,420 rows at
**0.920503372 engine seconds**, against 1.307041040. Each candidate cohort ran
once in isolation. The [complete report](../benchmarks/native-ordered-inputs-2026-10-31.md)
preserves slower cases/families, original failures, corrected test fixtures and
the single-observation limits. Portable replay confirms both outcomes without
new queries. Hosted integration and closeout remain pending; Full43 remains held
until item 18. The prospective contract and original frozen snapshots remain
unchanged as historical evidence.
