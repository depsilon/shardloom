# ShardLoom Arrow IPC provider controls

This is the Apache-2.0 `arrow-ipc` 59.3.1 registry provider with local decoder
controls. Original source identities and preserved license/notice files remain
recorded in `upstream-provenance.json`. The optional Zstandard dependency stays
pinned to `=0.14.3`; `publish = false` prevents publishing this local provider.
`bytes`, already in the runtime graph and formerly a provider test dependency,
is a normal dependency for safe buffer ownership. No package version changes.

`RecordBatchDecoder::with_decoder_memory` admits independently retained
decompressed buffers and required alignment copies. Already aligned input stays
shared. Credits survive output clones and slices. Controlled Zstandard requires
the caller's admitted codec workspace; it cannot silently use an unowned context.
Controlled LZ4 validates complete encoded framing, reserves the pinned decoder's
declared block/history capacities, and reads into a bounded destination. False
output lengths, partial frames and trailing input return errors. Legacy LZ4 may
end at a complete block boundary, matching the pinned codec's admitted behavior.

`read_schema_with_memory` validates logical schema shape and admits Arrow field,
type, collection, name and metadata capacity before conversion. `OwnedSchema`
retains that capacity. Callers retaining detached schema/field references must
also retain its explicit metadata owner. Controlled schema nesting is bounded at
128 fields before recursive construction. Capacity describes named structures,
not allocator bookkeeping or process RSS.

Message/block extents, field-node lengths, projected buffer exhaustion, variadic
counts, struct validity and union offsets are checked before slicing or scalar
buffer construction. Validation remains enabled by default; this patch adds no
unsafe blocks or external execution provider.

`read_owned_record_batch` admits represented array structures and construction
scratch before decoding. `OwnedRecordBatch` keeps an explicit metadata owner,
including bufferless arrays. Preflight borrows schema/message nodes and existing
dictionaries without allocating an inspection tree. Capacity includes nested
construction overlap, buffer control records and logical-null validation masks;
checked lengths reject fixed-list multiplication overflow and inconsistent
struct child lengths before upstream construction.

`OwnedFileDecoder` retains the admitted schema, projection and dictionary maps.
Dictionary replacement commits only after successful decode; deltas admit
concatenation output and scratch, including repeated dense-union/list children
and dictionary merge workspace. New payloads retain independent credits, and
shared view buffers retain their original owners without generation-credit
chains. Returned batches retain schema/array owners after decoder teardown.
The message APIs accept separately owned metadata and exact bodies, validating
versions and lengths; the block APIs use the same decoder.

The native demanded reader now admits physical footer, schema, block indexes
and message bytes through the shared pool and uses these owned APIs. Its
Zstandard workspace uses the existing native allocator. Owned batches survive
alignment, queues and native conversion; ordinary Arrow export remains a
separate metadata-lifetime boundary. The old row-based IPC allowance is removed.
An IPC physical record batch remains indivisible; prefix slicing reports the
full decoded row count and records no adaptive decoder completion. Broader
engine/interface acceptance remains separate.

The root lock owns shipped dependencies. The separate reviewed
`.github/provider-locks/arrow-ipc.lock` fixes standalone tests at Arrow 59.3.1.
The `formats` provider CI group runs all library tests with `lz4,zstd` and strict
library/test Clippy; all fixtures are generated locally. At this scoped revision,
154 library tests and strict library/test Clippy pass with both codecs; the
default build passes 145 tests, without ignored tests. Seven native IPC controls
cover complete values/owners, codecs, dictionary generations, pressure,
cancellation, changed sources, framing, prefixes and small/irreducible physical
units. The broader selected source/ingestion suite passes all 287 tests, and
native Vortex/CLI all-target strict lint passes. Complete engine acceptance
remains separate.
