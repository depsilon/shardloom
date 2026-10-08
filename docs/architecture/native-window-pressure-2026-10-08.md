# Native analytic windows under an explicit shared spill policy

Status: accepted implementation design under PERF-02/03/06/10/12 and CG-20/21,
following [join acceptance](native-join-pressure-2026-10-08.md). Runtime work and
complete acceptance remain open in the [phase plan](phased-execution-plan.md).
The expert comparator is an external-memory analytic operator preserving exact
frame semantics, observation order and complete output under one query grant.
The resident strategy remains the default.

The intended cohesive unit covers the existing ranking/navigation functions and
framed COUNT, COUNT DISTINCT, SUM, AVG, MIN, MAX, FIRST, LAST and NTH over admitted
ROWS/GROUPS/RANGE frames and exclusions. It also connects one finite single-use
batch source to that same native window strategy. It does not add new SQL frame
semantics, a repeated-source spool, compatibility streaming sinks or execution
resume. The [six-area/eight-investigation scope](native-local-completion-scope-2026-10-07.md)
remains open.

## Reuse and retained owners

The existing Window retains a Table, one sorted ordinal vector per group,
per-partition peers, full-length Values for every function, exact DISTINCT
membership/counts and potentially partition-sized extrema candidate queues.
Spilling only its input would not close its pressure contract.

Use the existing native Ordering, StoredOrder, QueryRunStore/Reader, private
record builders, exact key comparison, frame endpoint/exclusion policy, exact
floating/decimal totals and native result writers. Keep the current resident
strategy as the default. An explicit spill policy selects the bounded strategy
before execution; no catch-and-retry strategy switch is allowed. All owners
share the query grant and disk quota. Pinned Vortex arrays, builders, take and
bounded file scans remain the providers; relational frame orchestration remains
inside shardloom-vortex. There is no external-engine execution or new dependency.

## Vortex-first provider decision

Classification: `implement_shardloom_kernel` for analytic frame orchestration,
using pinned Vortex 0.85.0 arrays, DTypes, validity, native take/builders, Flat
native file writes and bounded row-range scans. The checked existing provider
boundaries are `native_relational_spill`, `native_relational_stored_order`,
`local_primitive_query_run_store`, `native_payload` and `result_batch`.
They supply representation, generation-validated I/O and credited construction;
they do not supply ShardLoom's frame/exclusion, error-order and quota contract.

Reuse these providers and the existing relational binder/kernel semantics inside
`shardloom-vortex`, behind `vortex-local-primitives` and `vortex-write` for
spill. Add no dependency, unsafe code, Arrow execution or query-engine integration.
Execution and Native I/O evidence must retain `fallback_attempted=false` and
`external_engine_invoked=false`. Native temporary files are query-local state,
not durable answers, a result cache or an execution-resume checkpoint.

## Native input, grouping and output

Compact input into private native records with a checked original ordinal and
typed payload. Validate every existing ordering-key domain during input in the
same order as the resident builder. Release a batch producer's private owner
before requesting its next batch. Complete and validate the producer before
evaluating window functions, preserving the current source-error boundary.

For each existing partition/order group, validate its explicit null-order
requirement before any of its function evaluation. Build stable native ordered
records containing original ordinals and the bound native ordering/measure
keys. Private field names are separate from the user payload namespace. Do not
charge an internal ordinal against the public output-field limit. Keep source
payload in the shared original-order store. Partition keys,
order keys, directions, NULL placement and stable original-ordinal ties retain
the existing comparator semantics.

Visit partitions in the same order as the resident implementation. Find each
partition's complete extent without retaining all its rows. Store peer starts
and the partition-end sentinel as native ordinal records. Use bounded key/block
access over the completed held file; retain generation validation on cache hits,
before/after reads and before completion.

Preserve group -> partition -> ranking/navigation pass -> each framed function
over that whole partition. Globally evaluating one function across every
partition or interleaving all framed functions by output row changes error order.
Factor shared ranking and frame policy away from full Values storage; resident
vectors and bounded native result records should consume the same values.

Each partition may retain a bounded ranking-result store and a store for each
framed result until its group record is assembled. Selected-value functions can
store nullable original source ordinals, delaying payload gathering. Retire these
temporary stores after assembling the partition. Order each completed group's
results by original ordinal. Finally zip complete group stores with the source
store, gather selected columns, and emit bounded credited native output in input
order. Check exact row cardinality, ordinal correspondence and completion for
every store; this is a private result assembly, not a separate relational engine.

## Frame arithmetic and positional selection

Adapt existing row/peer access to either resident ordinal mappings or a bounded
view over a contiguous sorted native range. Reuse Cursor, exclusions represented
as three monotonic intervals, fixed COUNT and exact reversible SUM/AVG totals,
and FIRST/LAST/NTH selection. Do not introduce prefix subtraction for ordinary
floating totals or change exact decimal finalization. Keep the current nullable
selected-source behavior and root-validity masking.

## Exact DISTINCT without resident membership

For an input position p and one monotonic frame interval s(i)..e(i), the output
positions containing p form [first i with e(i)>p, first i with s(i)>p). Both
endpoints can be found with forward cursors over stored frame bounds. For each
non-NULL value, emit at most three nonempty output-position intervals, one for
each exclusion interval. Sort by exact value and interval start through the
native Ordering; merge overlapping or touching intervals for the same value.
Emit +1 and -1 boundary events for each union interval, sort by output ordinal,
and scan exact counts. Adjacent value comparison decides identity; a hash alone
cannot establish equality. Process departures and arrivals with checked counts
and verify the final event balance. No per-output full-frame rescan, mutable
disk hash table or full-partition membership array is required.

## Extrema without an unbounded deque

Build native binary range summaries that retain a selected source position,
choosing the first ordered position for equal extrema. Summaries use the existing
exact native comparator and ignore parent NULLs. Each level combines adjacent
entries from the preceding level. A range query combines its binary interval
decomposition; at most three frame ranges feed the final selection. Small upper
levels may stay resident under the same Ordering threshold; lower levels use
native runs. Account for every level descriptor, held reader/footer and cached
block. A bounded small block cache may be needed to avoid alternating endpoint
reads; cache capacity must be explicit and credited, not a new memory allowance.

Before accepting this strategy, preserve the existing observation boundary:
values never included in any frame must not acquire new validation errors merely
because a summary was built. The inverse-interval coverage gives a way to mask
unobserved leaves. Prove this with nonfinite/invalid unobserved values as well as
ordinary nullable and nested values. Error order across functions and partitions
must remain explicit. This is a design risk to resolve in implementation.

## Early mathematical screen

The separate deterministic Python screen compares interval inversion and binary
range selection with complete brute-force set/extremum results. It passes
56,448 cases and 2,116,800 result values over individual partitions of 0–64 rows,
peer shapes, finite integer ordering offsets, all four exclusions, NULL/constant/
unique/repeated observations and empty frames. It uses in-memory Python lists.
It is evidence for the interval mathematics only, not native type, memory, disk,
failure, platform or performance acceptance. The screen does not itself test
multiple partitions in one execution; its inputs model individual partitions.

## Resource, recovery and acceptance obligations

Charge record/schema descriptors, frame-bound records, peer indexes, function
result stores, summary levels, lookup blocks, selected-value compaction, output
and finalization overlap before allocation. Footers and large individual values
can still exhaust the declared grant. The flush threshold is not an RSS cap.
Every loop and read remains cancellable. A single shared native store owns quota,
generation checks and cleanup; success follows complete input, function/output
completion and cleanup. Recovery means owned cleanup and restart, not resume.

Acceptance needs every existing function/frame/exclusion/type/null/order/error
contract against independent complete outputs, varied input/block sizes,
several partition/order groups, retained outputs and all representable writers.
Prove real constrained spill versus constrained resident denial and ample
resident controls for wide input, many peers/results, DISTINCT and extrema.
Exercise cancellation while constructing/reading summaries and interval events,
grant/quota denial, corruption/replacement, source change, sink/consumer failure,
protected destinations and dead-owner cleanup/restart. Connect one-shot input
only after its full pre-demand admission and owner-release contract passes.
Finish with required source checks, frozen public/direct/adapter regressions,
Full43, independent evidence inspection, source review and hosted integration.

## Implementation boundaries and alternatives

Share frame logic through a private positional key-access contract. The resident
adapter maps positions through its existing ordinal slice; the stored adapter
uses bounded held blocks of the native order. Frame positions carry partition
length and current peer edges; arbitrary GROUPS boundaries come from either the
resident peer vector or the completed native peer index. This abstracts access
to the same Vortex keys, not a new data format or query execution engine.

Separate completed order lifetime from the temporary sorting specification:
completed native readers depend on the shared query store, not on a discarded
sort descriptor. Every held lookup block and key owner remains charged. Compare
nested keys through native KeyColumn/Batch comparison with both blocks held;
never substitute scalar conversion or hash equality. Drop replaced blocks before
allocating replacements and validate the held generation even on cache hits.

Preserve the observation boundary before building DISTINCT/extrema summaries:
walk newly included observations in the existing frame/interval order, validate
only admitted values, then build interval unions or range summaries from that
coverage. COUNT merely checks nullness. Positional selection may return a NULL
value. Unobserved nonfinite values do not become new errors. Add direct error-order
and parent-null tests; the current tests cover wholly excluded invalid measures
but do not establish every multiple-function or multiple-partition error case.

The rejected alternatives are retaining the full partition behind a disk-backed
input, rescanning each complete frame for every output, maintaining a new mutable
disk hash table, and computing ordinary floating results by prefix subtraction.
They respectively leave the original pressure gap, impose avoidable quadratic
work, duplicate storage machinery, or change accepted arithmetic. Binary extrema
summaries and sorted DISTINCT events add I/O; measure complete workloads and keep
the resident default. The mathematical screen alone makes no speed claim.

Footers, lookup work and small retained stores scale with admitted functions and
summary levels. No global two-reader cap exists in the shared store. A grant may
deny those overlapping owners even if each payload block is bounded. Tests and
reports must give actual observed peaks and explicit limits rather than infer a
total-process memory guarantee from the flush threshold.
