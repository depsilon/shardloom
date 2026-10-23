<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native compute structure and optimization review

Status: source review and cleanup, October 23, 2026. Performance opportunities
below are hypotheses, not measured improvements. The broader required and
conditional queue remains paused under the [phase plan](phased-execution-plan.md).

The strongest structural opportunity is to make existing optimizations available
through the same prepared operator contracts. ShardLoom already has specialized
encoded aggregation and bounded ordering, but composed plans do not always use
those algorithms. Downstream row demand, prepared expressions and required-column
information also stop at different boundaries. Moving those decisions into shared
operators can avoid work across public surfaces without adding another execution
engine or expanding the supported SQL/type surface.

Review baseline: `7f460d0c9d71ea079397e92469a6b7dc222bcacf`. Its native source
matches the accepted restored runtime. The
[audit](../benchmarks/evidence/native-compute-review-2026-10-23/review.json)
records source identities, exact anchors and the checked Full43 route inventory.
The latest accepted standard Full43 is **48.479745297 engine seconds** for one
execution of all 43 queries, with all 381 result rows validated. It contains
35 aggregate primitives, four sort primitives, two filtered counts, one filtered
projection and one count-all route. It contains no composed relational route.
Full43 is therefore a useful regression check for all proposals below, but it is
not sufficient to measure the first three opportunities.

## Priorities

| Priority | Shared component | Avoidable work or structural gap | Evidence needed before retaining a change |
| --- | --- | --- | --- |
| 1 | Composed ordering and finite output demand | Full sorting and output gathering precede a small following limit | Composed sort/limit comparison with exact stable output, credited state and complete source validation |
| 2 | Aggregate admission and batch consumers | Primitive and composed aggregation select different update machinery | Equivalent simple and composed workloads, with the same types, nulls, ordering and memory grants |
| 3 | Prepared expressions and typed batch views | Repeated recipe compilation and repeated preparation of the same batch columns | Repeated-expression and repeated-measure probes, including lazy error semantics |
| 4 | Prepared-plan normalization | Required-column pruning is partly owned by the SQL frontend | Equivalent public plan shapes and measured reductions in actual projected/read columns |
| 5 | Ingest layout and work admission | Worker grants affect source batch size and persisted physical grain | A separately admitted layout comparison; existing region/topology prerequisites remain blocked |

Start with finite output demand. Its unnecessary output gathering is directly
visible in the source and has a small semantic boundary. Aggregate reuse has
broader potential, including the existing Full43 families, but requires more
careful separation of admission, state and finalization. File splitting alone
does not establish a runtime benefit.

## Finite demand should reach ordering

The relational binder computes `offset + count`, but
[`cap_rolling_prefix`](../../shardloom-vortex/src/local_primitive_relational_transform_bind.rs)
only passes a prefix through projections and limits to supported rolling work.
It deliberately stops at sorting. The
[`Limit` runner](../../shardloom-vortex/src/local_primitive_relational_transform.rs)
then discards batches after its requested rows have arrived while continuing to
drain the child. That drain preserves late validation failures.

For a positive, small limit above a sort, the
[composed sort](../../shardloom-vortex/src/local_primitives/native_relational_sort.rs)
currently retains all input, allocates all ordinals, sorts them all and gathers
every output batch before the limit can discard it. Its specification carries no
finite output demand. The primitive ordering path already has bounded candidate
selection and [typed sort blocks](../../shardloom-vortex/src/local_primitives/native_sort_block.rs).

First make the demand explicit at the prepared sort boundary and gather only
the required prefix. Then evaluate bounded candidate retention using the same
admitted comparator and ownership rules. Merely limiting gathered output still
retains the full input and sorts all ordinals; report that distinction. Do not
transplant a primitive comparator into broader decimal or nested semantics.

Acceptance must cover nonzero offsets, zero/large limits, complete multicolumn
keys, null placement, ties resolved by composed-input ordinal, late invalid values,
cancellation, allocation denial and the existing explicit spill path. Finite
output demand does not authorize abandoning a source that still owes validation.
No region-local candidates or global merge are proposed here; the blocked
`PERF-10.ordered_candidates` experiment remains separate.

## Aggregate strategies should be reusable consumers

The [public dispatcher](../../shardloom-cli/src/public_workflow_route.rs)
selects both primitive aggregate execution and prepared relational execution.
Both are native ShardLoom paths. Inside a composed plan,
[`NodeKind::Aggregate`](../../shardloom-vortex/src/local_primitive_prepared_relational.rs)
uses the generic relational accumulator or its explicitly enabled spill family.
It does not select the primitive `GroupedAggregateStates` strategy family.

The [generic accumulator](../../shardloom-vortex/src/local_primitives/native_relational_aggregate.rs)
prepares input values separately for each measure and performs typed cell updates.
The [primitive implementation](../../shardloom-vortex/src/local_primitives.rs)
contains direct numeric updates, dictionary-code group state, bounded top-K
finalization and other specialized consumers. Those optimizations are real;
the opportunity is to expose reusable admitted consumers to compatible composed
inputs, rather than duplicating them or claiming vectorization is absent.

Extract one coherent aggregate family with an immutable admission decision,
owned state and a batch-consumption interface. Reuse it where semantics match.
Keep wider unsupported combinations on their existing explicit native path.
Preserve integer overflow, floating evaluation order, decimal behavior, nulls,
distinct semantics, dictionary identity and the original memory grants. A generic
accumulator is an essential correctness path, not evidence of unused code.

For current Full43 prioritization, the twelve slowest queries account for
27.240830835 engine seconds, or about 56.2% of the total. Q23 reports substantial
first-pass accessor work; Q34 and Q35 each report 9,038,838,007 UTF-8 bytes hashed
and no native dictionary chunks on that path. These observations justify
investigating shared typed/dictionary preparation and reuse. They do not prove
that preparation can be removed, or that the reported work dominates every
worker. Parallel phase, lock and merge clocks overlap and must not be added to
or subtracted from engine elapsed time as a predicted gain.

## Prepare expressions once and share batch views

[`Expression::evaluate`](../../shardloom-vortex/src/local_primitives/native_relational_expression.rs)
attempts to compile the finite predicate-block recipe each time it evaluates a
batch. Binary expressions recursively produce intermediate arrays; projection
evaluates each output expression separately. Generic aggregation also prepares
the same input column again for separate measures such as `SUM(x)` and `AVG(x)`.

The first candidate is to bind an immutable supported predicate program once,
then reuse typed column views within one batch. Sharing repeated pure
subexpressions across outputs is a further step, requiring an explicit dependency
and lifetime contract. Preserve lazy `CASE`/`COALESCE`/`NULLIF` behavior, error
ordering, nulls, floating semantics and any effectful boundary. Avoid cross-query
state or a new general-purpose fusion registry in this polish pass.

The [existing predicate block](../../shardloom-vortex/src/local_primitives/native_relational_predicate_block.rs)
already supports a finite I64/boolean program and shares identical nodes within
that recipe. Vortex execution-context construction already captures a shared
[kernel snapshot](../../vendor/vortex-array/src/executor.rs); this review does
not claim that each context deeply clones a registry. Previous physical-array
and codec-context cache rejections remain binding.

## Normalize below the frontend

The SQL lowerer already has
[backward required-column pruning](../../shardloom-cli/src/sql_native_relational_pruning.rs).
The [direct relational binder](../../shardloom-vortex/src/local_primitive_relational_bind.rs)
binds an explicit scan projection and binds child nodes before their parent's
dependencies are established. Identity projection normalization exists, but it
does not provide a common backward dependency pass for every direct plan.

A schema-aware prepared normalization pass could make equivalent admitted plans
receive the same projection and operator strategy regardless of frontend. This
is primarily an ownership and consistency improvement until measured read-work
evidence shows a benefit. It must validate explicitly written expressions even
when their outputs are unused, retain full ordering/window/set dependencies and
preserve diagnostic behavior. Python paths that already lower through SQL can
already benefit from SQL pruning; do not describe all Python plans as unpruned.

## Keep physical layout and scheduling decisions explicit

The [Parquet ingest implementation](../../shardloom-vortex/src/universal_format_io.rs)
derives stream batches and row-group tasks from admitted memory and parallelism.
The [PGO closeout](../benchmarks/native-pgo-2026-10-23.md) reproduced different
batching under historical and current grants: 3,120 versus 423 source batches.
That is a concrete coupling between work admission and persisted physical grain.
It is not a matched layout performance comparison.

The [composed scan](../../shardloom-vortex/src/local_primitive_relational_scan.rs)
also uses ordered, bounded ranges independently of downstream batch sizing.
These choices protect ownership and finite memory. Review their contracts before
changing sizes or increasing concurrency. Rejected coalescing and scalar-region
mechanisms do not become valid again because the source contains a small range.
Joint preparation and topology changes remain behind their existing prerequisites.

## Organize around ownership rather than file size

`local_primitives.rs` contains about 70,000 lines, including more than 21,000
lines in its inline test section. It is not 70,000 lines of hot execution code.
The more useful concern is that admission choices, many optional state fields,
runtime updates, finalization and evidence rendering live together. That makes
strategy reuse and invariant review harder.

Prefer typed admitted strategies with explicit state and finalization contracts,
extracted one coherent family at a time. Keep reservation and release behavior,
cancellation, evidence fields and native Vortex ownership at those boundaries.
Use Vortex's existing arrays, selections, compute and scan providers before
inventing replacement representations. New public facades or parallel operator
stacks would deepen the structural problem.

The review did not prove any production code removable. Test-only experiments,
feature-gated providers and report-only capability modules are not unused merely
because the default runtime does not call them. The rejected contiguous-range
selection experiment is especially relevant: apparent allocation avoidance alone
did not survive its measured projection controls. The accepted compact-index
representation remains distinct from that rejected mechanism.

## Cleanup and verification

The [cleanup receipt](../benchmarks/evidence/native-compute-review-2026-10-23/cleanup.json)
records exact retired generated paths, their identities, closed-handle checks,
retained results and regeneration bases: **28,047,382,259 logical bytes** across
159,698 regular files and 60 symlinks in 174 exact roots. This is retired logical
size, not a measurement of physical free-space change. Completed compiler/package payloads and
historical validation data are retired. The alternate PGO ingest is also retired
after its experiment closes and this review declines to admit a layout trial.
Current source input, accepted Vortex input and tested runtime remain protected.
Unknown-provenance artifacts remain untouched. This is a bounded cleanup batch,
not a claim that every local historical file is disposable. No production engine
source changes or new Full43 run are part of this review.

Before implementing a selected opportunity, freeze its exact workload, physical
input, resource grant, material-benefit threshold and regression limits. Use
independent complete results and negative controls for the relevant semantic and
lifecycle boundaries. Add a composed workload when Full43 does not activate the
changed path. After a cohesive implementation, run the required workspace/native
checks and one isolated standard Full43; extra comparative runs require a stated
decision reason. Preserve every observation, including unsuccessful candidates.

The reviewed source establishes opportunities to avoid work, not their attainable
speedup. No new runtime provider, external-engine fallback, optional breadth,
package release or competitive-gate completion follows from this review.
