<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native optimization propagation

Status: the bounded dictionary/COUNT/matcher propagation unit is verified for
integration under the maintainer's useful-behavior retention instruction;
broader work remains active under item 20/RFC 0044 and the existing
[coverage priority](native-optimization-coverage-2026-11-13.md). The November 15
maintainer review requests improving propagation while retaining the current
work. Shared implementation and positive activation counts do not establish
that every eligible occurrence reaches an optimized owner.

## Current disposition and next unit

The combined observation at `3cec5a410` completes all 11,579 fresh requests and
540 prepared operations per runtime with matching complete results, schemas,
order and no-fallback/resource evidence. Its original gate remains **not
admitted**: 60 required selected-dictionary witnesses fail, and engine totals
increase from 894.235685490 to 894.646518681 seconds for fresh requests and from
0.516182452 to 0.521095814 seconds for prepared operations. These are one
observation per request, not per-change causal evidence. The population contains
5,309 exact original-full10k requests and 6,270 mechanism cases; it is not the
full10k with only additions. No 1,500-to-900-second speedup is claimed.

The maintainer separately permits retaining useful behavior and directs a
focused selection fix, a cohesive verified PR, then continued development. The
original protocol, gate failures and observations remain unchanged. Future
investigations follow the [affected-scope policy](../skills/benchmarking.md#investigation-scope).
All 120 required grouped-COUNT projection witnesses pass. The dictionary misses
are work/scale, dictionary/sparse/all-null, UNION ALL then noncontiguous selection,
checksum/group-summary compositions across all five public surfaces.

The focused correction at `75596d0ee` now passes all 2,190 fresh requests and
540 prepared operations, all 120 required dictionary-retention and 80 kernel-
reduction obligations, and all 60 previously missing witnesses. Its frozen
performance decision is **drop**: fresh engine time increases from 99.601052609
to 100.161598045 seconds while prepared time decreases from 0.521095814 to
0.513496733 seconds. Complete source/package checks and relocated portable replay
pass. The [report and separate integration record](../benchmarks/native-optimization-propagation-2026-11-15.md)
retain useful behavior within the maintainer's instruction without rewriting
either failed performance decision. UNION ALL and scale all-null increases
remain explicit. The subsequent
[resident join attribution/copy unit](native-resident-join-attribution-2026-11-16.md)
passes local and portable acceptance: 54 diagnostic requests, three debug
gather observations and 403 complete production requests per runtime. The
complete engine total decreases 60.778284678 → 60.597904154 seconds, with all
186 individual regressions retained. This small, roughly 0.3% result satisfies
its frozen rule without establishing material improvement or repeatability;
hosted integration remains pending.

The maintainer separately requests one Polars pass on the exact 11,579 saved
fresh requests. The [November 16 observation](../benchmarks/polars-propagation-once-2026-11-16.md)
executes each once: 11,489 pass and 90 retain a constant-projection adapter
cardinality error. No query is retried and no new native pass is added. The
passing subset supplies a labelled collect-time observation; the failed full
population has no accepted competitive total. Preserve physical-format and
clock differences, OS peak-memory exceptions, and the original native gate
decision. This does not make full11k a default per-change suite.

The November 16 maintainer direction now requires credible **material
potential before starting new optimization work**. Screen join input and
intermediate volume, repeated scans/builds, expensive sorting and spill using
actual cost and removable-work evidence. Do not add sliver-potential tuning
merely because it might lower a total. Useful existing work may remain and a
smaller-than-expected positive result does not by itself force a drop. Preserve
all original gates; no fixed material percentage is inferred. This priority
precedes the previously next spatial work without deleting that required
feature roadmap.

Saved-packet inspection verifies all 23,158 fresh records without executing
queries. Candidate activation includes join membership in 2,668 requests,
dictionary text in 1,020, COUNT partials in 559, existing-order owners in 6,157,
and representation retention in 3,205. These are observed request counts, not
eligible-opportunity denominators. Join and multiway families contain
500.383376588 seconds of complete-query engine time, about 55.93% of the candidate
total. The selected-dictionary misses contain 10.408416377 seconds, about 1.16%.
After closing this bounded propagation gap, attribute resident join input, keys,
indexing, payload and downstream consumption before selecting the next change.
Existing counters do not divide the family total among those phases.

The selected-dictionary regression reproduces correct results with 204 row text
kernel calls and no dictionary calls. A noncontiguous selection wraps the
compact dictionary; preparation sees the inner row count as the value domain.
The local fix preserves checked native views for the already copied small domain
and composes plain host codes only at demanded UTF-8 values-only preparation.
Complete inner-code and outer-index bounds remain required. An immutable UTF-8
validation receipt prevents newly observing unproved discarded values. Hash
preparation, deferred/unknown shapes and the original native error paths remain
unchanged. The prepared column retains opaque source ownership; final expression
outputs retain independent receiving credits. Focused work/lifetime controls,
3,344 default and 7,288 native tests, provider checks, 455 installed-wheel tests
and the complete targeted result/work replay pass. This does not establish a
complete-cohort performance gain.

The pinned Vortex `Dict::take` supplies the correct composition semantics, but
its fixed-width provider uses the infallible buffer allocator. A pressure
regression caught that allocation outside the fallible host budget. The fix
therefore reuses ShardLoom's existing typed native result builder for composed
codes and Vortex's dictionary representation, preserving width and nullability.
It adds no dependency, unsafe code or query-engine fallback.

## Intake and sequence

| Reviewed opportunity | Disposition and evidence boundary |
| --- | --- |
| Dictionary text above set alignment and selection | Accepted first. `cast_batch` uses `take_column`, whose canonicalization and output copy remove dictionaries before later text expressions. Preserve the selected dictionary in independently owned output through that shared boundary. |
| Grouped COUNT under pure rename/reorder projections | Accepted follow-on in the existing binder. Trace sort keys through proven column-only projections without relocating evaluation or weakening the complete group-order requirement. |
| Resident versus spill work elimination | Accepted owner audit. Ordered spill has separate representations; resident membership, duplicate ranges and COUNT partials cannot simply be attached to it. Each change needs its own semantic/resource and complete-request evidence. |
| Source demand reaching physical readers | Merge into the current source-demand audit. Distinguish logical columns, completed requested reads and decode work. Preserve value/error obligations. |
| Dictionary, source-fact and FSST scale evidence | Accepted evidence extension with verified physical encodings, changing dictionaries, many chunks, high cardinality and memory-pressure controls. Row count alone is not activation evidence. |
| Prior declined candidates | Preserve their dispositions and evidence. This work does not reinstall per-measure metadata, constant-key grouping, Bloom admission, text signatures or unconditional survivor batching. |

The first implementation changes the existing shared gather/alignment owner,
not public-interface dispatch. Downstream expression, filter, group, distinct
and output owners keep exact value equality; code numbers from different
dictionaries never establish equality. Spill strategy changes remain a separate
verification boundary. Item 19, item 20, RFC 0044 and CG-1 through CG-23 remain open.

## Dictionary selection contract

An encoded-columnar maintainer must be able to trace every selected row to its
original code and value, and every surviving buffer to its receiving budget.
Admit actual UTF8 dictionaries, original-width integer codes, native U64 row
indices, identity/nullable-widening types and a value domain smaller than the
selected row population. Empty, non-dictionary, unsupported representation,
nullability-narrowing and non-beneficial domains keep the existing native copy.
An optional bookkeeping reservation may decline before execution; invalid indices,
codes, UTF8, allocation failures and cancellation must propagate as errors.

Check the complete selection bounds before selected-value access. Copy each
referenced value once into a compact receiving domain, preserve selected row
order/duplicates/nulls, and remap the original-width codes to that domain.
Do not retain unselected source text or source code buffers. Do not validate
unreferenced text earlier than the existing selected-value boundary. Copied
metadata, codes, values and validity retain their credits through clones,
slices and source/session destruction. Failure and cancellation release all
temporary reservations. The existing ordinary path remains the correctness
oracle for admitted selections.

Record actual representation-retention admission/decline at the existing Native
I/O decision owner, including input/output row counts and bounded memory decline.
Existing dictionary-expression counters separately establish that the following
consumer receives a dictionary and transforms referenced values. Neither record
is a whole-engine opportunity denominator or a zero-decode/RSS claim.

## Grouped COUNT projection contract

Trace each bound Sort key backward through column-only Project nodes and
column-only compact Pipeline stages. Match output names to their bound source
column expressions at every stage; names alone are insufficient. Admit the
existing partial owner only when a traced key is the sole admitted group key.
The ordinary binder has already validated the whole projection, payload schema
and order key. Leave every operator in place. Computed expressions anywhere in
a projection, filters, ranges, observations and other stages remain barriers.
COUNT-only ordering, nullable arguments, multiple measures and grouping sets
retain their prior admission decisions. Explicit spill keeps its ordered owner.

The regression matrix checks rename/reorder/duplicate projections in tree and
compact forms, ascending/descending order, secondary group keys, count-only ties,
computed/filtered/ranged controls, complete producer consumption, late failures,
computed failures and spill. Existing COUNT work certificates distinguish
considered, admitted, unique-declined and capacity-declined batches, input rows
and reduced groups. They do not explain every binder refusal or establish a
global opportunity denominator. Earlier cardinality admission remains a separate
cost investigation; this change does not infer the cause of old timing changes.

## Audited remaining ownership boundaries

| Owner | Verified connection and work evidence | Remaining opportunity |
| --- | --- | --- |
| Ordered aggregate | `local_primitive_prepared_relational.rs` selects the spill branch before resident `consume_reordered_count`; `native_relational_aggregate_spill.rs` builds ordered records and uses the existing ordered finalizer. Reports input, distinct and quantile records. | Reduce records before ordered aggregation where its own semantics and reservations permit. Resident COUNT state is not automatically available. |
| Ordered join | The spill branch precedes resident join construction. `native_relational_join_spill.rs` probes `lookup::Search` over ordered right records; candidate rows, probes, match records and lookup blocks are reported. | Impossible-probe and duplicate-range work elimination appropriate to the ordered representation. Resident `RowIndex` membership and skew counters do not describe this route. |
| Spill Sort | `local_primitive_relational_transform.rs::run_sort` selects `Ordering` before resident ranked/prefix paths. Run creation remains threshold-driven inside that owner. | Bound final construction while retaining complete key/payload validation, bounded buffers, merge ordering and cleanup. A configured spill policy does not imply a file was written. |
| Physical source demand | `ExecutedVortexBatchInput` reports projected/available/physical-leaf columns, row groups, decoded rows, requested data bytes and requests; ranked reads separate rank and payload work. | Demonstrate actual requested-read/decode changes for each source layout and composition. These totals are source-operation observations, not per-operator attribution or unique device bytes. |
| Dictionary gathering | The new path admits an actual dictionary and preserves a detached compact domain; chunked multi-domain sources still reach the existing native copy when not directly admitted. | Inspect join, ordering, retained intermediates and spill/reload individually. Do not infer dictionary preservation from the common helper name. |

Existing batch aggregate/join pressure, fault and recovery suites compare
resident and explicit-spill values and ownership. The new projected COUNT spill
regression confirms that broadening the ordering proof does not bypass that
boundary. None of these existing checks measures a new spill performance gain.

## Scale and coverage evidence

The explicit dictionary fixture adds a `scale` mode: 1,048,577 nonempty rows in
256 Flat chunks, with a different value-to-code mapping across successive chunks.
Dictionary, sparse unused-domain, all-null, unique and flat profiles preserve
their logical definitions; the empty control stays empty. Per-chunk original
codes remain U16. The existing physical-encoding inventory must inspect every
stored array tree before measurement. Logical references remain independent
Python populations. Large cases use bounded aggregate outputs; no output-limit
or memory-limit increase is authorized by the larger source.

The original frozen combined population contains 5,309 original declarations,
1,380 dictionary cases, 3,510 text-signature controls, 285 versioned COUNT cases
and 1,095 new propagation cases (525 dictionary and 570 COUNT projections).
It includes the complete earlier 2,543-request COUNT cohort after deduplication:
1,964 grouped originals, 134 scalar COUNT originals, 160 neighboring controls
and 285 versioned compositions. Its prospective protocol explicitly binds the
matcher and propagation changes together; no independent matcher speedup is
established. Selection used declarations, not timing values. The focused follow-on
reuses the proven 2,190-request/540-operation baseline subset and executes only
the new candidate. Neither population is a default future investigation suite.

## Vortex-first provider check

- Subject: native dictionary selection, set field alignment and output ownership.
- Checked pinned Vortex 0.93.0 `DictArray`, `Dict::take`, dictionary casts,
  `PrimitiveArray`, `VarBinViewArray` and native host allocator, plus ShardLoom's
  numeric gather, selected variable-value accessor and credited output builders.
- Decision: `implement_shardloom_kernel` for independent compact receiving
  ownership; reuse the existing Vortex arrays and ShardLoom builders. Upstream
  dictionary take retains the entire source value domain, which does not satisfy
  this existing detached-output contract.
- Residual work uses the existing explicit native copy. No query-engine helper,
  dependency, unsafe code, Arrow execution or external fallback is added.
- Values/codes may be decoded by their native providers. Preserve a dictionary
  between consumers; do not call this zero decode or zero copy.

## Verification and decision

First prove value/schema/error equivalence and the new composed activation with
Unicode collisions, different per-chunk dictionaries, duplicate and null indices,
null codes/values, unused values, casts, empty/high-cardinality controls and
owner release after failure/cancellation. Exercise set, selection and downstream
group/distinct/filter/full-output consumers through the shared engine. Add large
fixtures only with explicit encoding readback and bounded output/storage.

Freeze declaration-based affected populations and controls, exact current and
candidate sources/runtimes, independent results, resource settings and measured
work before performance execution. Preserve one execution per declared request,
existing isolation, deadlines and storage guards, and all observations. Judge
the complete declared engine total; do not infer benefit from kernel counts or
sum unlike historical cohorts. No extra full10k, Full43, warmup or competitor
pass follows from this item. Default/native workspace, feature and installed
proof precede acceptance. The original matcher/propagation combined result and
the focused correction retain their separate failed performance decisions; the
later useful-behavior integration judgment is recorded independently.

ShardLoom technique review: retain current PulseWeave/capillary batch geometry;
reduce representation expansion inside existing work units. Shared memory
admission bounds optional mapping, metadata-first selection can decline without
allocation, and evidence keeps engine time separate from preparation and replay.

## Latest complete observation

The November 15 single latest-state run at `be9fc2c2f` passes all 10,000 original
queries at 1,563.106319749 engine seconds, versus the saved 1,560.142540948 at
`9f4580c72`. Same-ID saved-result reconciliation identifies 21 corrected file-Sort
cases rising from 0.191429042 to 63.657835499 seconds, and the other 9,979 falling
from 1,559.951111906 to 1,499.448484250 seconds. All 98 prior excessive-column
Sort cases now record narrower source columns; their engine sum falls from
52.121035209 to 12.703020922 seconds. These are two single observations, not
repeatability or per-change causal proof. The old Sort shortcut omitted mandatory
payload validation; preserving that correction is required.

Offline verification of the same 10,000 saved raw packets confirms native
pipeline execution in every request. Join membership activates in 3,154 requests
and records 437,893,233 avoided index lookups; dictionary text activates in 130
and records 38,601,782 retained dictionary rows across expression invocations;
grouped COUNT partials activate in 91 and consume 22,452,743 input rows. Exact
source finite-float, ALP, cyclic, hot-duplicate and matcher-reuse counters are
present but zero for this corpus. Dictionary-text counters exist in only 401
packets; absent counters stay unavailable. These counts prove executed work,
not semantically eligible opportunities, unique rows or causal timing savings.
