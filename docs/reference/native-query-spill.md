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
String/float/nullable keys, multiple keys, tie expansion, partitioned inputs and
spill-backed exports fail explicitly. Supplying a spill policy does not enable
other engine families or external execution.

The operator retains bounded key/row-ordinal candidates, writes sorted native runs,
and performs balanced merges before materializing the selected final rows. Each
native Flat leaf finishes before the next is admitted. At most nine run files are
open concurrently, including a merge output. Byte quota includes simultaneously
live inputs, merge output and ownership metadata; it must cover that overlap.
Returned evidence records runs, merge passes, peak reserved bytes, peak disk bytes
and successful owned cleanup. Values are exact, including unsigned keys above
the signed integer range and signed integer extremes.

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
