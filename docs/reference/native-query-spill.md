# Native query spill

The local numeric sort operator can use explicitly admitted temporary Vortex runs.
This is separate from synthetic spill tests and from durable ingestion, which still
publishes one native artifact. This path requires a Unix build with
`vortex-local-primitives,vortex-write` (included in `release-user-surfaces`).

For an existing native file with a nonnullable Int64 `priority` column:

```sh
mkdir -p /tmp/shardloom-query-work
shardloom run dataframe \
  --input shipments.vortex --input-format vortex \
  --vortex-primitive sort_rows --vortex-source-order-limit 7 \
  --vortex-sort-rows '{"order_by":[{"column":"priority","descending":true}],"offset":123456,"spill":{"workspace":"/tmp/shardloom-query-work","memory_bytes":4194304,"quota_bytes":33554432}}' \
  --request collect --bounded true --memory-gb 1 --max-parallelism 2 --format json
```

The same explicit sort payload can accompany an equivalent `run sql` request.
The Python client's `public_workflow_run` accepts it through `vortex_sort_rows`.
`route` inspection does not create or probe the workspace. Execution requires an
existing real directory, at least 1 MiB of operator memory within the query's
memory request, and at least 32 KiB of disk quota for ownership metadata.

The admitted shape is one local file, one nonnullable Int64 or UInt64 sort key,
no predicate, an explicit bounded output count, and first/last tie ordering.
For this specialized row-reference strategy, string/float/nullable keys, multiple
keys, tie expansion and partitioned inputs fail explicitly. Admitted completed
results can stream through all eight local writers under the
[native result contract](resident-native-results.md). Supplying this spill policy
does not enable other engine families or external execution.

The operator retains bounded key/row-ordinal candidates, writes sorted native runs,
and performs balanced merges before materializing the selected final rows. Each
native Flat leaf finishes before the next is admitted. At most nine run files are
open concurrently, including a merge output. Byte quota includes simultaneously
live inputs, merge output and ownership metadata; it must cover that overlap.
Returned evidence records runs, merge passes, peak reserved bytes, peak disk bytes
and successful owned cleanup. Values are exact, including unsigned keys above
the signed integer range and signed integer extremes.
Public query and writer envelopes expose this existing report through
`local_primitive_native_sort_spill_*` fields, including `runs_written`,
`runs_validated`, `merge_passes`, `peak_reserved_bytes`, `peak_disk_bytes` and
`owned_cleanup_completed`. Ordinary SQL output preserves the specialized sort
provider and its resource contract.

Run leaves contain 256 to 1,024 rows, chosen within the existing merge reservation.
At 4 MiB the operator uses 1,024-row leaves and up to eight input runs; at 1 MiB
it uses 256-row leaves and two input runs. Each run reader builds and polls only
one exact native row-range task per refill, so machine-core prefetch does not
multiply live payload blocks. The merge reservation includes a 64 KiB initial
footer-read allowance per reader, 16 KiB fixed state, and a conservative 1 KiB per
block row for overlapping row queues, conversion and native writer arrays.
The separate metadata charge remains 1 KiB per leaf plus 4 KiB per run, including
simultaneously live input and output runs. The 1 MiB minimum does not guarantee
that every larger input's run metadata fits; such failures remain explicit and
clean owned files.

Reservations cover owned sort candidates, bounded merge/conversion batches, run
metadata and checksum scratch. Source scanning, provider allocations outside those
owners, and final payload materialization remain separate scopes. The spill
counter is not an RSS limit or proof of a global query memory bound.

Normal completion, errors and cooperative cancellation remove only owned files.
Rust callers can clone `VortexSortSpillPolicy` and call `cancel()`; the operation
checks that token during native work. A process crash or forced termination can
leave an owned run directory. With the original workspace policy, call
`cleanup_abandoned(directory)` on that specific abandoned directory. Recovery
checks recorded file identities and refuses unknown files, symlinks or replaced
ownership metadata. It does not search or delete arbitrary workspace contents.
A crash between creating a run and recording its identity can leave an unknown
file; recovery refuses that directory for inspection. Cancellation is cooperative
at operator checkpoints, so blocking provider I/O must return before cleanup can
finish. Do not recover a directory belonging to an active query.

Correctness coverage lives in the native `local_primitives::sort_spill::tests`
suite and the public `public_numeric_sort_spill` workflow test. These verify
complete 131,072-row public-query results at 4 MiB with offset 123,456, large
integer keys, ties, quota failure, cancellation, corrupt runs,
interrupted cleanup and preservation of unknown files. This scoped operator does
not close the whole PERF-06 shared-spill packet or establish a throughput claim.

## Composed relational ordering

Composed SQL and DataFrame ordering can spill full native rows, including multiple
keys, nullable keys with explicit null order, finite floats, booleans, exact signed
and unsigned integers, UTF8, binary, exact Decimal128, Date32 and timezone-free
microsecond timestamps. The [typed key contract](../architecture/native-typed-keys-2026-10-03.md)
requires matching decimal precision/scale and preserves distinct temporal types.
Its [acceptance report](../benchmarks/native-typed-keys-full43-2026-10-03.md)
includes complete typed run/merge and writer readback checks.
It shares native key comparison, stable ordering,
run storage, memory ownership and sinks with the existing engine. Adjacent two-run
merges preserve input order on ties. All ordering stages in one query share the
same disk quota and resident grant; no nested stage creates another execution budget.

Public `collect`, `run`, `route` and local writers carry `memory_gb`,
`max_parallelism` and optional `spill`. The CLI spelling is:

```sh
--spill '{"workspace":"/tmp/shardloom-query-work","quota_bytes":67108864,"buffer_bytes":2097152}'
```

The directory must already exist for execution. Inspection is inert. Unknown
fields, repeated flags, and simultaneous common and embedded spill declarations
are rejected. The selected specialized numeric sort or aggregate provider retains
its existing contract: the common `buffer_bytes` maps to that provider's
`memory_bytes`, including the aggregate's 2 MiB minimum. For composed relational
ordering, general aggregation, joins, analytic windows and sparse pivots it is a retained-state flush threshold,
at least 1 MiB and no larger
than the query grant. Run metadata, readers, native keys and output allocations
also consume the same grant; a threshold is not a second budget or a guarantee
that every physical source layout fits.

Complete output can exceed collection limits through all eight local writers.
Success requires validated native runs, original source generations and owned
cleanup before output publication. Evidence records actual run writes/merges,
simultaneous readers across nested stages, disk and reservation high-water, and
completed cleanup. Upstream scratch and decoded bytes remain unmeasured; this
does not establish zero-decode execution or a total RSS ceiling.

`VortexRelationalSpillPolicy::cleanup_abandoned` recovers one explicitly selected
owned directory through the same namespace/identity rules. It refuses active,
unknown, replaced or symlinked entries. This permits cleanup and restart, not
resuming an interrupted operator. Set state retains a separate pressure contract.
Relational fanout remains separate work. See the
[ordering contract and acceptance](../architecture/native-relational-resources-2026-10-02.md).

## General aggregation and completion-aware input

Current source builds use the same explicit spill policy for general relational
GROUP BY and COUNT DISTINCT, alongside COUNT, SUM, AVG/MEAN and MIN/MAX.
Native ordering and runs bound group and distinct membership state, preserving
first-seen group order and the existing typed, null and floating reduction
semantics. All stages share the query grant and disk quota. Without an explicit
policy, resident aggregation retains its deterministic memory denial.

One finite `streaming=True` batch source can compose filters, projections, sorting,
aggregation, joins, analytic windows and limits alongside ordinary file/resident sources. Limits
and offsets apply globally and drain all input,
including zero limits. A late producer or value error prevents completion and
file publication. Incremental results, bounded collection and one new native
Vortex destination are admitted; compatibility streaming writes, repeated
sources and other stateful families require their separate contracts.

```python
import tempfile
import shardloom as sl

def aggregate_input():
    yield [{"team": "red", "amount": 3}, {"team": "blue", "amount": 1}]
    yield [{"team": "red", "amount": 4}]

workflow = (
    sl.from_batches(
        aggregate_input, schema={"team": "utf8", "amount": "int64"},
        streaming=True,
    )
    .group_by("team").agg(total="sum(amount)")
    .sort("total", descending=True).limit(1)
)
with tempfile.TemporaryDirectory(prefix="shardloom-query-") as workspace:
    with workflow.iter_batches(
        memory_gb=1, max_parallelism=1,
        spill={"workspace": workspace, "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20},
    ) as batches:
        for batch in batches:
            print(batch.result_rows)
        assert batches.report is not None
```

The complete result is `[{"team": "red", "total": 7}]`. This small example selects
the native spill strategy but need not create a disk run. The
[aggregate design](../architecture/native-aggregate-pressure-2026-10-07.md) and
[acceptance report](../benchmarks/native-stateful-aggregation-ordering-2026-10-08.md)
record actual constrained spill, complete output and failure/cleanup proof.
File-backed aggregate workflows also retain all eight admitted writers under
their individual dtype contracts. This does not extend the streaming-input
writer contract or establish a whole-process memory bound.

## Joins and one-shot input

Current source builds admit the same explicit spill policy for Inner, Left,
Right, Full, Semi, Anti and Cross joins. The default resident strategy still
requires its build state to fit the query grant. An explicit spill policy selects
native ordered build records, bounded exact candidate lookup and, for Right/Full
joins, spilled match tracking and restoration of unmatched rows to their original
order. Hash collisions still require exact key equality. Nullable, typed and
static nested keys retain their existing semantics, including null nonmatches.
ON conditions keep the existing candidate order and whole-batch error behavior.

```python
import tempfile
import shardloom as sl

def incoming_orders():
    yield [{"customer_id": 1, "amount": 3}, {"customer_id": 2, "amount": 5}]
    yield [{"customer_id": 1, "amount": 4}]

ctx = sl.ShardLoomContext()
customers = ctx.from_rows(
    [{"customer_id": 1, "customer": "Ada"}],
    schema={"customer_id": "int64", "customer": "utf8"},
)
orders = ctx.from_batches(
    incoming_orders, schema={"customer_id": "int64", "amount": "int64"},
    streaming=True,
)
workflow = orders.join(customers, on="customer_id", how="left").select(
    "f.amount AS amount", "d.customer AS customer",
)
with tempfile.TemporaryDirectory(prefix="shardloom-query-") as workspace:
    with workflow.iter_batches(
        memory_gb=1, max_parallelism=1,
        spill={"workspace": workspace, "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20},
    ) as batches:
        for batch in batches:
            print(batch.result_rows)
        assert batches.report is not None
```

The complete result has `(amount, customer)` values `(3, "Ada")`, `(5, None)`
and `(4, "Ada")`, in that order. This small example need not write a disk run.
One finite batch source can occur on either join side and compose with nested
joins, aggregation, ordering and draining limits. Its URI must occur exactly
once; multiple batch producers and repeated use of a batch source are rejected
before consumption. Ordinary file/resident sources may occur more than once.
Batch input types, row/frame/count limits and destination admission stay as
defined in the [batch contract](../architecture/native-bounded-adapters-2026-10-06.md).

Build records, lookup blocks, candidate copies, ON work, match/restoration state,
readers and delivered arrays share one query grant. All temporary runs share one
disk quota. A hot key or Cross join can still require quadratic work; spill does
not promise faster execution. A single large value, schema/footer, exhausted
grant or disk quota can still produce a deterministic denial. Public reports
expose `relational_ordered_join_*` counts for stages, build/probe/candidate rows,
match records and lookup blocks, alongside the shared
`relational_spill_*` disk/reader/cleanup counters. Streaming reports separately
count detached join build rows and batches in `native_input_join_build_*` fields.
Those detachment counts cover each build boundary, including ordinary sources
in a streamed plan; they are not counts of distinct input rows or copied bytes.

File/resident joins retain the eight scalar writers and the seven admitted
typed/nested writers under each format's representability contract. Streamed
input permits incremental results, bounded collection or one new native Vortex
file. Final success requires complete input and source validation, sink completion
and owned cleanup. Errors, cancellation, replaced/corrupt runs or failed cleanup
cannot publish a successful result. Dead-owner recovery permits safe cleanup and
restart, not resuming an interrupted join. See the
[join contract](../architecture/native-join-pressure-2026-10-08.md) and
[complete local acceptance](../benchmarks/native-join-pressure-2026-10-08.md)
for the exact implementation and evidence. Hosted integration completed in
[PR #1532](https://github.com/depsilon/shardloom/pull/1532), including all 39
checks and actual preview/production verification.

## Analytic windows and finite streamed input

Current source builds admit the same explicit spill policy for the existing
ranking, navigation and distribution functions, framed COUNT, COUNT DISTINCT,
SUM, AVG, MIN/MAX and FIRST_VALUE/LAST_VALUE/NTH_VALUE. Admitted ROWS, GROUPS,
RANGE, exclusion, type, null and ordering semantics remain those of the
[analytic-frame contract](../architecture/native-analytic-frames-2026-10-05.md).
Resident execution stays the default. An explicit policy selects native stores
for input, peers, frame bounds and results, with bounded exact DISTINCT and
extrema state. All stages share the query grant and disk quota.

One finite single-use batch source can compose these windows with admitted
filters, projections, joins, aggregation, ordering and draining limits. Windows
complete and validate their input before evaluating results. Delivery preserves
the input row order; window ORDER BY defines analytic positions, not final output
sorting. Use a separate sort when the delivered order should change.

```python
import tempfile
import shardloom as sl

def incoming_measurements():
    yield [{"sequence": 1, "team": "red", "amount": 3},
           {"sequence": 2, "team": "blue", "amount": 1}]
    yield [{"sequence": 3, "team": "red", "amount": 4},
           {"sequence": 4, "team": "red", "amount": 3}]

workflow = sl.from_batches(
    incoming_measurements,
    schema={"sequence": "int64", "team": "utf8", "amount": "int64"},
    streaming=True,
).window(
    "SUM(amount) OVER (PARTITION BY team ORDER BY sequence "
    "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS previous_two_total",
    "COUNT(DISTINCT amount) OVER (PARTITION BY team ORDER BY sequence "
    "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS distinct_amounts",
).select("sequence", "team", "previous_two_total", "distinct_amounts")

with tempfile.TemporaryDirectory(prefix="shardloom-query-") as workspace:
    with workflow.iter_batches(
        memory_gb=1, max_parallelism=1,
        spill={"workspace": workspace, "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20},
    ) as batches:
        for batch in batches:
            print(batch.result_rows)
        assert batches.report is not None
```

For sequences 1, 2, 3 and 4, `(previous_two_total, distinct_amounts)` is
`(3.0, 1)`, `(1.0, 1)`, `(7.0, 2)` and `(7.0, 2)`. Partition state spans input
batches. This small example selects the bounded strategy but need not write a
disk run. The [window acceptance](../benchmarks/native-window-pressure-2026-10-08.md)
separately proves actual file-backed and streamed spill at 16 MiB, complete
values, resident denial, ample controls and owned cleanup/restart.

File/resident windows retain all eight scalar writers and the seven admitted
typed/nested writers under their format contracts. Streamed input permits
incremental results, bounded collection or one new Vortex file. Shared
`relational_spill_*` fields report actual run, reader, quota and cleanup work;
`relational_ordered_window_*` reports stages, input rows, groups, partitions,
peer/frame records, distinct intervals/events, extrema summaries and lookup
blocks. `native_input_window_*_detached` counts input ownership released at each
window boundary, including ordinary sources inside a streamed plan.

Source validation, complete function/result evaluation, sink completion and
owned cleanup precede success and publication. Corrupt/replaced runs, exhausted
grants/quotas, cancellation and source/consumer failures remain errors. Recovery
cleans a named abandoned directory and permits a fresh restart; it does not
resume execution. Large individual values or overlapping metadata/readers may
still exceed the grant. This window contract does not bound total RSS, admit set
spill, repeat a one-shot source, widen input types or enable compatibility
streaming destinations. See the [window design](../architecture/native-window-pressure-2026-10-08.md)
for the precise ownership and observation-order contract.

## Sparse pivot state

Current source builds implement explicit native spill for relational `pivot` and
`pivot_table` over file and resident-memory sources. Complete local acceptance
passes in the [pivot acceptance report](../benchmarks/native-pivot-pressure-2026-10-08.md);
hosted acceptance remains pending. The default remains resident. Supply the existing `spill`
argument to collection, incremental results or an admitted writer to select the
stored strategy. The direct prepared unary API remains resident, and dynamic
one-shot batch input rejects before producer consumption.

For an existing Vortex file with `entity`, `category` and numeric `amount` columns:

```python
import tempfile
import shardloom as sl

workflow = sl.context().read("sales.vortex").pivot_table(
    index="entity", columns="category", values="amount", aggfunc="sum",
)
with tempfile.TemporaryDirectory(prefix="shardloom-query-") as workspace:
    with workflow.iter_batches(
        memory_gb=1, max_parallelism=1,
        spill={"workspace": workspace, "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20},
    ) as batches:
        for batch in batches:
            print(batch.result_rows)
        assert batches.report is not None
```

Sparse updates preserve source order, first representatives and NULLs, sequential
floating accumulation, exact decimal state, domain naming and existing margins.
Native sorted runs retain complete replacement cells. Exact key bounds, a fixed
two-block cache and at most one missing-key range per run bound lookup retention;
all payload and metadata reservations share the query grant. Merging preserves
chronological replacement order rather than reassociating floating partial sums.
Output gathers bounded native batches without retaining a dense pivot matrix.

The strategy keeps the current 128-field, aggregate, type, fill and margin rules.
All eight scalar writers remain subject to representability; typed/nested output
keeps its existing ORC restrictions and CSV JSON-text translation. Small collection
retains its independent row/byte limits. Large individual values or overlapping
source, writer and run metadata can still cause deterministic memory denial.

The shared `relational_spill_*` fields report runs, merges, quota and cleanup.
`relational_spilled_pivot_*` records stages, input rows, index rows, domains, cells,
lookup block loads and reader opens. Early incremental batches remain provisional
until the final report; source validation, complete evaluation and owned cleanup
precede success and file publication. Failed work preserves existing destinations.
Recovery removes only a verified abandoned directory and permits a new execution;
it does not resume an interrupted pivot. This contract does not bound process RSS,
expand batch-input admission or establish a speedup. See the
[pivot design](../architecture/native-pivot-pressure-2026-10-08.md).
