# Column-addressable durable writer experiment

This isolated PERF-08/PERF-09 experiment changes the logical footer hierarchy,
not the selected codec policy. Production dispatch remains unchanged until a
paired complete-lifecycle measurement supports retention.

Pinned Vortex 0.85 separates `LayoutRef` trees from `SegmentSink` writes. The
existing bounded ingest wrapper awaits one input batch's complete native child
subtree using a local EOF before advancing the source. Its root is Chunked over
batch Structs. The candidate retains that exact physical execution and instead
returns a Struct whose fields are Chunked over the existing corresponding column
subtrees. Native dictionaries, zones, encoded arrays, statistics, segment IDs and
payload buffers are reused unchanged. The wrapper performs no array execution,
decode, compression, or serialization. Existing per-batch child work is unchanged.

Admission requires a stable native nonnullable Struct root with scalar columns.
Each completed child must itself be an exact Struct with the same dtype and row
count, and every field child must match the admitted field dtype and batch rows.
Nullable roots, nested input fields, lazy/non-Struct batches and incompatible
provider output fail explicitly. Empty batches create no data subtree; an empty
stream retains its exact scalar schema with empty column Chunked layouts. No
whole-column min/max or file statistic is invented by transposition.

Configuration bounds actual columns, nonempty row groups and rows per input
group. Defaults admit at most 256 columns, 65,536 actual groups and 1,048,576 rows
per group. Source batch/row estimates are not hard admission bounds. Native input
buffer ownership and child-writer scratch retain their existing contracts.
Reference vectors grow under a shared reservation before allocation, including
old/new capacity during growth. Returned root/column children retain that credit
through native footer serialization; cloned children share their original vector.
Cancellation drops the active child future and all retained partial layouts;
already-issued physical writes remain subject to the enclosing owned sink's
abort/cleanup contract. The wrapper never publishes a partial footer.

The eventual candidate-only switchpoint is
`LocalVortexWriteContext::stream_write_options_for_decision`: replace the existing
`BoundedIngestLayout` wrapper with this alternative around the *same*
`strategy_for_decision` result and the same shared pool. Do not stack both wrappers
or change compression, row batching, source validation, sink publication, or
checksumming at the same time. Existing per-batch column work is bounded by the
admitted schema; the wrapper adds no per-column tasks or global stream fanout.

Acceptance pairs both wrappers against identical encoded batches, proves every
physical segment specification and payload byte is unchanged, then reopens the
real footer and checks complete scalar values, schema and selective column/range
reads. A separate pair leaves the native default file-statistics policy enabled
and checks reopened global min/max/null counts against literal expected values,
then compares every stored statistic and its precision between writers.
Empty/null cases, distinct dictionary domains, incompatible child output,
reference denial, cancellation and credit lifetime after strategy drop are
required. Later measurements must include open/bind/tree work, actual completed
reads, complete queries, write/sync/hash/reopen time, artifact size and all source
identities. This design alone establishes no performance improvement or phase
closure.
