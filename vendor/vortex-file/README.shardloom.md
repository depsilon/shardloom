# ShardLoom file metadata ownership patch

This directory contains the reviewed Apache-2.0 `vortex-file` 0.90.0 registry
source. `upstream-provenance.json` records the authoritative archive, all
original member hashes, license and dirty upstream revision. The workspace
lockfile owns production dependency resolution; the archive's crate-local
lockfile is recorded but omitted.

The changes in `src/footer/deserializer.rs`, `src/open.rs` and
`src/segments/source.rs` use the existing
session `HostAllocator` for footer growth, copied DType/layout FlatBuffers,
cached and in-memory segment realignment, and requested user-metadata payloads. Buffer owners
retain their allocator reservations through shared clones and lazy metadata
views. Already aligned slices remain shared. Default opens skip optional
metadata cache entries, and in-memory opens do not build a discarded tail cache.
Allocation errors propagate
without substituting unbounded storage. The original infallible `prefix_data`
operation remains a documented raw compatibility surface; file opens call the
new fallible `try_prefix_data` operation.

The refresh also makes the V2 segment-location table and plan cache in
`src/file.rs` lazy. Ordinary cached V1 opens and scans do not allocate that
unused copy of the admitted segment table. Clones share initialization; source
replacement resets source-dependent plans while retaining already initialized
immutable locations. Tests cover complete V1/V2 results, cold clones, metadata
locations and replacement sources. This preserves the upstream V2 API without
admitting it as a ShardLoom execution provider or claiming resource coverage for
its explicit-use allocations.

The October 24 refresh ports the existing ownership changes to the new
serialization/context APIs and connects file reads to the reviewed fallible
`vortex-io` constructors. Writer edition enforcement remains enabled; the pinned
facade defaults to frozen `core:2026.08.3`, without preview or the optional Zstd
buffer edition. Historical-file compatibility is not an acceptance requirement;
fresh native files must pass complete write/reopen checks.
`publish = false` prevents publishing this local patch.
No external query engine or execution fallback is introduced.

The owning contract is
`docs/architecture/native-file-metadata-buffers-2026-10-16.md`. It covers the
named byte-buffer allocations, not structural context/segment collections,
schema field-name tables, layout child caches, statistics objects, writer
scratch, allocator bookkeeping or process RSS. Supplied parsed footers retain
their existing owners.

The subsequent structural work admits the parsed `SegmentSpec` vector before
construction. `SegmentMap` clones retain its reservation through the final
storage owner, including independent source and serializer handoffs. Its slice
access is borrowed; it does not export an unguarded shared allocation. Empty
maps allocate no table. `Footer::segment_map` returns this owned handle, while
`Footer::new` and `FileSegmentSource::open` retain their caller-owned `Arc` input
compatibility boundaries. `FileSegmentSource::open_with_segments` accepts the
owned handle. The two pinned ShardLoom raw-map clone sites use handle clones.
Buffer and file sources address base and metadata tables directly, preserving
metadata IDs and maximum alignment without a concatenation allocation.
In-memory sources use the provider's unaligned slice operation before testing
each segment's actual alignment, so a stronger whole-buffer alignment does not
panic at an otherwise valid segment offset. Realignment copies remain admitted.

The structural contract is
`docs/architecture/native-structural-provider-ownership-2026-10-21.md`. Its
segment-table scope excludes metadata locator/name allocation, schema and
statistics payloads, read contexts, writer construction/scratch, fixed wrapper
bookkeeping and process RSS. Caller-owned tables remain explicitly unadmitted.

The subsequent file-statistics extension admits both `StatsSet` and `DType`
table capacities before parsing or native writer construction. `FileStatistics`
clones share vectors and retain their reservation through the final owner;
`stats_sets` and `dtypes` borrow slices without exporting raw shared allocations.
Its existing constructors preserve caller-owned inputs. The fallible indexed
factory checks count/layout overflow, skips empty allocations and releases
partial production on error. Parsing validates cardinality before admission and
reads FlatBuffer entries directly. The writer consumes the accumulator's lazy
iterator after admission, avoiding an intermediate statistics vector. Nested
schema/scalar payloads, statistics overflow storage, accumulator state, writer
scratch and fixed bookkeeping remain separate from these table capacities.

The metadata-locator extension shares one private owner across postscript
parsing, footer/source aliases and serialization. It admits exact locator-vector
and UTF-8 name capacities before construction. Bounded borrowed-key validation
keeps the existing count, byte-length, uniqueness and diagnostic rules without
temporary owned-name collections. Incremental parsing retains that owner across
missing-input responses and retries. Native writers supply their actual session;
reserializing existing metadata retains its owner without another name copy.
Empty tables allocate no payload. The resolved-payload map and its name copies,
initial-read caches, future/serialization/sorting scratch and fixed bookkeeping
remain outside this locator/name owner. Metadata payload buffers keep their
separate existing allocator contract. Private format fixtures preserve their
original assertions with tuple-based metadata construction; new factory tests
cover exact/short grants, maximum Unicode keys and partial failure cleanup.

ShardLoom's native tests exercise the linked provider with real serialized
files, allocator refusal, exact grants, competing owners, retained clones,
malformed-input cleanup, aligned sharing and complete nested/wide values.
The standalone library test target preserves the upstream reader/writer
regressions and links the reviewed array and layout patches. Its ignored
crate-local lockfile starts from the workspace lockfile; any extra upstream
development dependencies belong to that test target only.
The nullable-root writer fixture verifies reopened metadata, whole-record values
and DTypes, logical child values, null-sensitive filtering and absent unmasked
child statistics for all-valid, mixed and all-null root validity. The separate
structural-reader correction and ShardLoom integration acceptance are tracked in
`docs/architecture/native-nullable-layout-2026-10-18.md`; the original metadata
intake did not claim to fix raw nullable-root projection.
