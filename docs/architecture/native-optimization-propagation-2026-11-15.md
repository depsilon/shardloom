<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native optimization propagation

Status: active implementation under item 20/RFC 0044 and the existing
[coverage priority](native-optimization-coverage-2026-11-13.md). The November 15
maintainer review requests improving propagation while retaining the current
work. Shared implementation and positive activation counts do not establish
that every eligible occurrence reaches an optimized owner.

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

The affected population includes all 4,150 existing text/UTF8 identity requests
and the complete 2,543-request COUNT cohort, deduplicated by declaration identity,
plus the 1,380 existing dictionary cases and new explicit propagation/scale
controls. The COUNT cohort is not 2,543 grouped originals: it contains 1,964
grouped originals, 134 scalar COUNT originals, 160 neighboring controls and 285
versioned composition cases. Selection uses declarations, not timing values.
Freeze the final identity-complete population and exact runtime boundaries before
any new scored execution. Preserve the existing matcher candidate unchanged;
its separate acceptance remains open unless explicitly bound into a combined
prospective protocol.

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
proof precede acceptance. The current matcher experiment's acceptance remains
separate unless a prospective combined contract explicitly binds both changes.

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
