# Native query sort spill

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
ordering it is a retained-input flush threshold, at least 1 MiB and no larger
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
unknown, replaced or symlinked entries. Aggregate, join, set and window state do
not gain spill support from ordering permission. Relational fanout remains
separate work. See the [contract and acceptance](../architecture/native-relational-resources-2026-10-02.md).
