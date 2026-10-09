# Native sparse-pivot pressure

Status: complete local engine acceptance, independent packet inspection and
support alignment; hosted integration pending under `NATIVE-PIVOT-PRESSURE`, PERF-02/03/06/10/12
and CG-20/21. The [phase plan](phased-execution-plan.md) owns its checklist.
This is the next coherent resource family after window integration in
[PR #1533](https://github.com/depsilon/shardloom/pull/1533), merged at `a88ec5c9`.
It implements the maintainer's [remaining-scope contract](native-local-completion-scope-2026-10-07.md)
under RFC 0044; it does not close that whole contract or introduce a release.
The [acceptance report](../benchmarks/native-pivot-pressure-2026-10-08.md) binds
runtime `5665eee5`, exact pressure controls, complete public results and retained
failure history. The separately authorized `RELEASE-050` train follows current
PR/documentation closeout and full fresh UAT before any version bump.

## Decision and scope

Extend the existing sparse pivot and pivot-table state through the shared native
query-run store when the caller supplies the existing relational spill policy.
Keep the resident strategy as the default. Preserve current aggregates, exact
typed/nested values, domain naming, fill, margins, source-order limits and error
observation. Query state, lookups, output and native file metadata share one
grant; all temporary files share one quota and cleanup owner.

Admit file-backed and explicit resident-memory sources through the existing
public relational SQL/Python/DataFrame/CLI route, including composed input and
incremental result delivery, complete collection and existing representable
writers. The direct prepared unary API has no spill policy today; it remains
resident and is a correctness reference, not an implicitly expanded API.

One-shot dynamic batch input stays rejected before producer consumption. Dynamic
preparation currently retains an arbitrary lowering closure that can execute
input while discovering a schema. Removing its batch-source rejection would not
establish complete-plan admission before demand. The current transport already
sends schema with the first result batch; the missing contract is safe admission.
That extension belongs to the broader streaming unit alongside remaining input
and sink ownership work. Keep the existing 128-field/domain-name, collection and
transport bounds. No new aggregate, nested arithmetic, wider margin rule, repeated
source spool, execution resume, process-RSS bound or speed claim is introduced.

## Reuse and Vortex-first provider decision

Classification: `implement_shardloom_kernel` for sparse latest-state resolution
and pivot orchestration. Pinned Vortex 0.85.0 supplies typed arrays, DTypes,
validity, native builders/take, Flat file writing and bounded row-range reads.
Those providers do not supply ShardLoom's online pivot/error semantics, keyed
replacement order, memory policy or publication contract.

| Existing owner | Required use |
| --- | --- |
| `local_primitive_unary_pivot`, `PivotRowExportState`, pivot values/cells | Share domain naming, exact first/first-unique, primitive/decimal/nested transitions, fill/coercion and margin finalization. |
| `native_relational_dynamic_bind` and `CompletedPivot` | Consume the preceding relation once; retain its discovered schema and a single-consumption result owner. |
| `native_relational_spill::State` and `QueryRunStore` | Reuse one quota, exclusive files, schema/length/checksum validation, held source generations, cancellation and exact owned cleanup. |
| Existing native ordering and stored lookup | Reuse credited block movement, exact comparisons, stable adjacent lineage and bounded positional access; ordinary ordering does not itself coalesce latest cell states. |
| `native_decimal_reduce::Total` | Preserve the complete exact wide total and count, with final precision/divisibility checked at the existing boundary. |
| `CompletedRows`, `result_batch`, native payload and writers | Keep bounded typed output, shared allocation owners, consumer cancellation and complete publication. |

Keep code within `shardloom-vortex`, behind the existing native feature gates and
`vortex-write` for spill. Add no dependency, unsafe code, Arrow execution or
Vortex query-engine integration. Certificates retain `fallback_attempted=false`
and `external_engine_invoked=false`. Temporary native state is not a durable
answer, result cache or checkpoint that can resume an interrupted execution.

## Preserve online updates

Process input in its current order. Reserve key/state growth before allocation.
Perform existing domain-width/name admission before numeric or first-unique cell
checks. Retain the first index representative before domain naming and cell
update. Domain collision suffixes follow first encounter; output indices and
domains follow their existing canonical serialized-key order.

First and first-unique retain the complete first value, including NULL. Exact
native comparison decides repeated equality; hashes alone cannot do so. COUNT
does not inspect the value column. Primitive SUM/MEAN retain sequential floating
updates and nonfinite-prefix denial. MIN/MAX retain their existing value checks
and selected extrema even if an unused internal sum becomes nonfinite. Decimal
state stores the exact wide total/count. Nested extrema skip NULL parents and
retain the selected complete value. Factor these transitions so resident and
stored state do not grow independent semantic implementations.

Use a bounded credited sparse buffer plus immutable sorted native runs. Resolve
the latest complete state before updating a previously flushed key. Write a full
replacement state, not a partial total to reassociate later. Keep index-marker
records separate from cell records in one exact ordered keyspace: markers own
the first index representative once, so no unbounded resident index map or
duplicated per-cell representative is required. The bounded domain map remains
resident and credited.

Each retained run has charged key bounds and a checked held reader. Its index
markers also have exact charged bounds: later cell updates may extend a run's
range without adding index markers. Preserve those bounds through replacement
merges so an impossible marker lookup does not displace a useful cached block.
Search the buffer and runs from newest to oldest, skipping only ranges excluded
by exact bounds. Cache a fixed small number of native blocks under the same grant and
validate the held generation even on cache hits. Do not reopen and rehash a whole
run for every cell lookup. Before whole-run binary search, narrow to a cached
block when its exact first/last keys enclose the target. This avoids evicting a
useful block with search ancestors on every adjacent cell, without another key
directory or additional payload slots. A failed search may retain one credited
half-open gap per immutable run, from the missing target to its next stored key.
Generation validation still precedes a gap hit, and each new merged run discards
its predecessors' gaps. The two payload slots use recent access for replacement.
Keep reader/footer ownership explicit as the run count grows; metadata exhaustion
remains a deterministic denial.

## Native state and merge lifecycle

Flush sorted unique buffer records at the configured threshold, preserving
headroom for old/new payload, writer, footer and merge overlap. The threshold is
not a second grant. Serialize integer counts, raw floating bits, optional extrema,
exact decimal wide state and selected native values without introducing public
validation of unobserved internal fields. NULL first values remain present cells,
distinct from absent cells. Internal names do not consume public field capacity.

Merge adjacent chronological runs, keeping the newer complete record for equal
keys. Never merge floating partial totals. Preserve exact key order and unique
cardinality. `QueryRunStore::write_arrays` requires the output row count and block
geometry in advance: use a bounded deduplicating count pass and then a bounded
write pass over held, generation-checked inputs. Keep input files and metadata
charged until the completed output is validated; only then retire exact owned
inputs. Bound levels, descriptors, simultaneous readers and cache slots. Clear
obsolete cached blocks before retiring their source. All loops check cancellation.

At completion, coalesce to a unique sorted state run. Retain its owned descriptor,
schema/domain metadata and scalar cardinalities in the completed pivot. Do not
let a reader borrowing relational spill state escape into the dynamic bound node.
Reopen through the same execution state during completion and output. All readers
must drop before shared cleanup, and a completed pivot remains consumable once.
An empty pivot needs no temporary payload run.

The shared native push-writer boundary completes its accepted prefix before a
fallible return. Pinned Vortex 0.85 queues one input array and cancels, without
joining, its spawned layout task if the blocking writer is dropped or its sink
fails. A healthy writer is finished into the owned unpublished file on producer
failure. After a real sink failure, a narrow output adapter retains the first
I/O error and discards pending native output; each push checks that error before
the caller requests more input, closes the input and joins the native writer.
No failed summary reaches publication, and actual partial bytes remain charged
by the existing quota owner. The same boundary serves temporary runs and final
native staging files. This wraps the existing Vortex writer and sequential Flat
strategy; it does not change the shared session, executor or encoding providers.

## Completion and bounded output

Observe complete input before selecting the output index prefix. Apply the
current source-order limit, including its reserved margin row, and retain the
complete pre-limit count. Every observed domain still participates in schema
discovery, including a domain whose first cell is NULL.

Compute column margins domain by domain over selected indices, then the grand
margin in index/domain order. Preserve error precedence and floating accumulation
order; a single interleaved pass is not equivalent. Use bounded scans/lookups,
not a dense resident matrix. Row-margin evaluation and final scalar/decimal
coercion stay at output access, where they occur today.

Enumerate bounded chunks of index positions and gather selected values late.
Reuse the existing result batch sizes, variable-byte admission and column build
order. Scalar getters can be called more than once during size estimation and
construction. Stored lookup must support that safely without retaining an entire
pivot or returning a view after its credits expire. Nested builders receive
compact native owners. Do not eagerly finalize later cells before a consumer
callback that currently precedes their evaluation. Output/source owners, lookup
blocks and finalization overlap remain credited together.

## Acceptance and failure evidence

The comparator is the existing native pivot semantics with independent complete
expected values. Required cases include empty/all-null inputs, every current
aggregate, nullable and nested roles, exact decimal/temporal/binary data, first
NULLs, repeated equal/conflicting values after eviction, naming collisions, fill,
dropna, margins, limits and error order. Vary physical batches, flush thresholds,
multiple merge levels and key/update distributions. Check signed zero, decimal
intermediate cancellation and final inexactness, and values hidden by validity.

Prove full constrained spill against both constrained resident denial and ample
resident controls. Exercise composition before and after the pivot, retained
results, complete collection, incremental results and all representable writers
with exact reopened types/values. Keep native low-grant controls distinct from
the public API's minimum memory grant. Record actual runs, merges, lookups, peak
credits/disk and complete output; counters alone do not establish a speedup.

Fault tests cover grant/quota exhaustion, failed run writes, corruption,
truncation/replacement including cache hits and both merge passes, cancellation,
late source errors, consumer/writer failure and protected destinations. Verify
baseline credits return after every failure, no successful prefix is reported,
and exact owned runs are cleaned. A dead-owner test must reject live/unknown
ownership, then demonstrate safe cleanup and a fresh complete restart. It must
not claim to resume state.

Complete focused semantic/resource tests before the required formatter, linter,
workspace/native/feature checks and frozen public/direct/adapter/Full43 regression.
Freeze executable, sources and independent expectations; inspect the portable
evidence, review the source, align affected support surfaces and finish hosted
checks before ledger closeout. Broader allocation coverage, streaming/adapters,
platform/release acceptance and all eight conditional investigations stay open.

The frozen local acceptance now passes all 15 source gates, 32,497 public checks
covering 18,595,284 complete row comparisons, 202 direct cases, 442 streaming
checks, 48 batch and 19 format checks, 145 admitted-semantic stages, nine golden
stages and all 129 Full43 comparisons. The added explicit-spill cohort contributes
3,626 checks and 1,713,893 rows, including 133 complete incremental transactions
covering 197,148 rows. Existing window cases and their independent expectations
are retained. Independent readback reopens 64,741 public envelopes and complete
values; a separate stream inspector verifies the portable packet and its
190-case positive/negative contract.

One file-backed pressure fixture supplies 24,580 rows with 6,145 distinct
768-byte keys and repeated floating updates. Its 19,342,360-byte input exceeds
the 16-MiB grant. Two explicit-spill executions each check every output value
and canonical key order, peak at 15,657,857 tracked bytes, and remove their owned
runs. The resident strategy denies under the same grant; a 512-MiB control
completes. Each spill execution records 775 runs, 387 merges, 37,896 lookup
blocks and a 37,324,372-byte peak disk footprint. These are finite correctness
and resource controls, not a process-RSS ceiling or performance comparison.

All ten public families completed successfully. Their initial aggregate summary
rejected two identical shared oracle entries; an isolated, tested combiner repair
reconciles the immutable family results after rechecking every source generation
and hash. The original failure and all native receipts remain preserved. The
final verifier also retains its initial summary-reader, environment and empty
batch-schema assertion failures. Empty complete results carry one acknowledged
zero-row batch with the exact typed schema. No native result, expected value or
engine source changed for these readback corrections.

## Alternatives and risks

Sorting all input before reducing cells changes online errors, source demand and
floating prefixes. A dense output matrix or fully resident index directory does
not solve sparse state pressure. In-place mutation adds a separate storage and
crash-consistency contract. External-engine spill is prohibited. The selected
immutable replacement design reuses existing native ownership and cleanup, but
random point lookups and repeated compaction may be expensive. Measure complete
workflow cost and preserve ordinary resident controls; do not infer a speedup.

Run footers, very wide individual values, simultaneous downstream state or output
can still exceed a finite grant. Deny explicitly where the bounded transition
cannot fit. Cost-aware merging, learned indexes and physical-cache admission
remain separate measured investigations, not prerequisites smuggled into this
correctness/resource unit.
