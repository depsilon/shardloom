# Addressable Immutable Memory Generations

This continuation implements the memory-generation portion of RFC 0044 under
PERF-07/PERF-11. The expert comparator is a native columnar source whose projected
reads reach only the addressed segments and whose slices cannot outlive their
allocation credits. Direct tiny operations and the borrowed-copy intake remain
available; choosing file generation is an explicit, once-per-generation action.

## Native provider contract

Pinned Vortex 0.85 supplies `StructLayout`, `ChunkedLayout`, `FlatLayoutStrategy`,
`Footer`, `VortexFile`, `SegmentSource`, native row-range scans, and immutable
`Buffer`/`ByteBuffer` owners. Wrap these providers rather than inventing a file
format or a prepared descriptor. A nonnullable root Struct has one Chunked child
per column; each child contains independently serialized Flat row-group leaves.
The configured row-group size and segment-count limit are explicit. Construction
processes one leaf at a time; upstream ChunkedLayoutStrategy's unbounded task
fanout is not used. Empty input retains its exact schema and needs no data leaf.

Each real SegmentId maps to one column and row interval. Per-segment requests and
returned byte counters prove addressability through the ordinary native scanner.
These count memory-provider requests, not filesystem or device reads. Root dtype,
column names, nullability, row order and values remain exact. Native serialized
array statistics are preserved as provided by slicing/serialization, including
exact long text bounds without the Flat writer's optional truncation. Slices
inherit only subset-valid upstream statistics; whole-column min/max are not
claimed exact for a smaller interval. This does
not invent file statistics, zoned pruning, or fresh statistics for absent fields.
Construction serializes the footer once to validate its explicit size bound,
reports those calls/bytes, and discards that temporary footer. The publication
path writes the same admitted segment bytes and offsets, then serializes the
footer again. Queries do neither. Publication retains independent checksum/readback, native reopen,
exclusive staging and the existing final file/directory identity checks.

## Owned intake and resource lifetime

An additional owned-column API consumes caller vectors into native buffers using
safe Vortex Buffer ownership and Bytes::from_owner. Full vector capacity is
reserved before transfer into the source; preexisting caller allocation is not
represented as an engine allocation. The credit is attached to the actual buffer
owner, so slices and returned arrays retain the complete backing capacity.
Only private constructors can create admitted owned columns. Foreign arbitrary
ArrayRefs or sliced buffers whose backing capacity cannot be established are not
accepted as proof of ownership. Columns from different shared budgets are rejected.

Admitted shapes match existing flat intake: Int64, finite Float64, Boolean and
UTF8, with explicit validity. Numeric/payload/offset vectors can transfer without
a payload copy; Boolean/validity packing is separately allocated and owned.
Names, primitive metadata, provider scratch and allocations bypassing the native
allocator remain outside buffer-credit claims. Existing borrowed columns continue
to copy into the session allocator. Neither path retains a borrowed caller pointer.

Segment bytes and alignment padding are checked before allocation; segment count,
layout-reference capacity and metadata reservation are bounded before visibility.
The layout children retain the reference credit through footer serialization and
reader lifetime; serializer/provider metadata allocations remain outside the
native buffer allocator claim. Failed or cancelled construction publishes no
generation. Cancellation is checked between synchronous leaves and before
visibility; an active leaf finishes before observing it. Dropping the last result
releases its associated native buffers. No background encode,
mutable generation, answer cache, external engine or durable side effect is added
to a tiny query. `fallback_attempted=false` remains required.

## Acceptance

Tests must exercise multiple columns and row groups, selective projection and
native row ranges with actual SegmentSource requests, empty and nullable values,
Unicode, sliced owned input, cross-budget rejection, failed/cancelled construction,
and result lifetime after dropping source/session handles. Durable publication
must reopen to independent exact values with matching schema, reuse the existing
segment bytes, and preserve foreign destinations on failure. Root owns serial
Cargo validation and any benchmark; no latency improvement is claimed by this
design. Broader streaming input, mutable generations and automatic layout choice
remain separate obligations.

The separate `memory_generation_addressable` example takes `ROWS ITERATIONS
ROW_GROUP_ROWS` (for example, `16384 100 2048`) and writes no files. It compares
borrowed-copy and owned-vector intake, then direct prefix, generation prefix and
interior row-range operations. It preserves raw samples and warmups, verifies
every full-generation scalar against independently generated Rust values, and
checks zero owned bytes after all results/sources drop. Native-array return times
exclude separate scalar verification and drop; they are not complete scalar-query
times. Caller preparation, validation-session allocations and provider metadata
remain outside the measured session's buffer-credit scope. Existing publication
tests/example supply the separate durable readback proof.
