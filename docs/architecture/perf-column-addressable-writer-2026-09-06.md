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

The private candidate switchpoint is
`LocalVortexWriteContext::stream_write_options_for_decision`, implemented by
`vortex_ingest_column_layout::stream_options`. Its
`DEFAULT_STREAM_FOOTER_LAYOUT` remains `RetainedRows`. A separately frozen
benchmark candidate may change that one private constant to `ColumnAddressable`;
there is no new public option or environment-dependent dispatch. Tests explicitly
select each variant. CPU lane changes can therefore be measured and frozen as a
separate control before changing the footer layout.

Selection requires the existing shared native ingest pool and statically admitted
scalar/nonnullable Struct schema. Missing shared memory, nested/extension fields,
nullable roots or excess columns retain the current writer before consuming any
batch; the private evidence explains that choice. This selection does not expand
the retained writer's schema support: pinned default file statistics do not admit
nullable top-level structs. The executable retained-route test uses a nonnullable
nested root with nullable scalar children, while candidate admission tests reject
nullable roots before writing. An admitted candidate replaces
the `BoundedIngestLayout` wrapper around the *same* `strategy_for_decision` result
and the same shared pool. It never stacks both wrappers. Malformed physical input
or incompatible child output after candidate selection fails explicitly; it does
not restart or switch strategies mid-write. Compression, row batching, source
validation, sink publication and checksumming remain unchanged. Existing
per-batch column work is bounded by the admitted schema; the wrapper adds no
per-column tasks or global stream fanout.

Successful candidate writes append their actual nonempty group/child invocation/
transposed-reference counts and peak owned reference bytes to the existing
`writer_layout_strategy_applied` field. These bytes cover footer vectors and
associated metadata, not arbitrary provider scratch or process RSS. The default
writer's evidence remains unchanged. Coalescing remains within each source batch;
footer transposition does not claim larger physical segments or cross-batch codec
coalescing.

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

The promotion packet adds pairs using the real `strategy_for_decision` default,
fast-load numeric, balanced and selected source-text compositions, beyond the
original Flat-child tests. Each variant constructs fresh native arrays with
renamed columns, exact identifiers above 2^60, nullable narrow integers, nullable
UTF8 and changed dictionary code domains across three 1,033-row batches. It
compares all physical segment specifications and payload bytes, every preserved
scalar subtree including codec/zoned metadata, complete independently calculated
values and native default file statistics. Real files are synced, fully read for
SHA-256 and reopened. The small fixture forces these existing compositions; it
does not establish ordinary large-source advisor admission or performance.
Fixture input construction and the independent reader use their existing test
allocation scope; this pair does not claim they are covered by the candidate's
layout-reference lease.

The new seam and composition pairs pass the root-owned native Cargo gate.
Retention still requires matched write/sync/full-hash/reopen
and full query measurements with identical source identities, codec policy,
row-group geometry and CPU allocation. The private selector is not a decision to
promote the alternative writer.
