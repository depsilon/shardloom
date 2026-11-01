# ShardLoom ORC provider

This preserves the `orc-rust` 0.9.0 registry source with the following reviewed changes:

- Its Zstandard dependency is pinned to `=0.14.3`, and `publish = false` prevents
  publishing this local provider.
- The two `Decompressor::read` advances propagate codec errors as `std::io::Error`
  instead of panicking on malformed compressed input. Valid decoding is unchanged.
  Native adapter tests cover first/later block errors, complete compressed ORC
  readback, corrupted frames and a successful subsequent reopen.
- The writer emits recursive List/Struct schemas and streams in matching preorder,
  exact Decimal128 data/scale streams, and excludes payload beneath null parents.
  Schema and writer-size errors return before the file header; empty flushes do
  not create empty stripes. Writer memory/row/stripe observations support the
  calling adapter's resource admission, which remains outside this codec.
- Decoded batches retain the reader's declared schema, including nullability
  and metadata, instead of inferring a new schema from each batch's arrays.
- Original Arrow timestamp units are recorded in the optional ORC type attribute
  `shardloom.arrow.timestamp.unit` (`s`, `ms`, `us`, `ns`). Default reading honors
  validated annotations; explicit reader precision overrides them. Unannotated
  files retain their existing default. This preserves the value domain when a
  wide-range seconds/milliseconds/microseconds column cannot fit nanoseconds.
  The decoder rejects fractional loss and malformed/overflowing timestamp streams.
  UTC writer zones retain original-width ticks without conversion through a
  narrower calendar representation. Non-UTC offset calculation uses checked
  calendar and integer operations; an unrepresentable valid timestamp returns
  an explicit error instead of becoming null or panicking. Parent nulls remain
  null without evaluating their physical payload.
  The ORC-763 pre-epoch gap remains a deterministic writer error.
- New schema/decimal/nested encoder code is original implementation based on the
  [Apache ORC v1 specification](https://orc.apache.org/specification/ORCv1/).
  It uses the pinned provider's existing integer/boolean/compression primitives.
  No external query engine or new dependency is introduced.
- Readers may share parsed file metadata under the caller's held source
  generation. Synchronous and asynchronous stripe iteration seek to the first
  selected stripe in the validated sorted inventory and stop at the range end
  before opening streams. Synchronous reads borrow the selected metadata;
  asynchronous reads clone only that stripe across the read.
- File/stripe count agreement, required stripe fields, checked tail lengths,
  non-overlapping stripe extents and complete column-statistics indexing are
  validated. Missing file totals remain unavailable for exact-count consumers.
  Optional null/value-count presence is preserved; absent/incomplete/inverted
  integer, floating and string bounds and malformed boolean buckets cannot
  become invented zero bounds. Incomplete statistics remain conservative.
- Synchronous metadata parsing accepts a borrowed memory admission callback.
  It admits codec/buffer/protobuf overlap before growth, then retains a separate
  shape estimate for the metadata. Input buffers remain caller-owned, and the
  estimate does not claim to bound codec allocations or process RSS. The caller
  retains the final credit for as long as it retains the metadata.
  Provider tests cover missing/inconsistent/overflowing counts, stripe extents,
  metadata sharing without footer rereads, range exclusion before stream reads,
  unknown statistics and memory denial. Engine admission and tests decide which
  statistics types are usable for query pruning.
- Selected regions retain their consumed-row progress across output batches.
  A selected region larger than a batch must stop at its declared end rather
  than continue to the end of the stripe. The synchronous reader accepts a
  positive next-batch row limit without reopening its stripe or resetting
  selection progress. Tests vary limits, cross stripe boundaries, retain prior
  outputs and reject zero without consuming input. Held stripe streams and
  dictionaries remain indivisible; this API does not claim a memory bound.
- Controlled readers keep encoded stream owners and admit decompressed buffer
  capacity and replacement overlap before allocation. Physical headers, declared
  expansion limits, empty blocks and truncated framing are checked; decoder errors
  are terminal. Zlib, Snappy, Zstandard and LZ4 use credited destinations. The
  controlled LZO route rejects before its unbounded allocating API.
- Next-stripe preparation loads selected streams' first physical blocks without
  consuming logical values. Stripe footer parsing admits buffer/protobuf overlap,
  validates stream extents, encoding coverage, timezone and unsupported encryption,
  and keeps metadata credit with cloned columns.
- Decoded primitive, boolean, string, binary, list, map and union payload buffers
  reserve before allocation. Their Arrow slices retain those same credits. String
  dictionaries retain their buffer owners; expansion into the declared Utf8 output
  admits checked offsets and exact payload length before copying. Timestamp and
  decimal scratch use the same budget. Bounded integer/byte run buffers are admitted
  before constructing their decoders, including overlapping patch/skip scratch.
  Structural admission remains separate from these payload credits; neither
  establishes an allocator or process RSS bound.
- The controlled synchronous `try_build` path admits projected schema, metadata
  maps and selection/control structure before construction. Immutable root type
  clones share their child/index inventories. Stripe preparation admits checked
  type/name, stream-map, decoder graph and output-construction overlap before
  creating those owners; column clones retain the shared stripe credit. Bounds
  use actual Rust structure sizes and checked schema shapes, independent of
  batch rows. They are conservative source-derived bounds, not allocator/RSS
  measurements or minimum-memory claims. The legacy `build` API remains for
  ungoverned upstream callers; it does not acquire the new reader structure
  credit. Controlled provider row-index predicates reject explicitly; ShardLoom
  uses its separate held stripe-statistics path before payload reads.
- Controlled Zstandard decoding requires a caller-owned bounded workspace
  provider, including file metadata and stripe footers. ShardLoom supplies its
  existing Vortex static decoder with the shared allocator. The provider-only
  memory mock explicitly declines that capability; native integration tests own
  the real decoder proof. Uncontrolled provider use preserves the existing API.
  Raw Deflate uses the already locked `miniz_oxide` 0.8.9 slice decoder with
  fixed stack state and a credited complete output; it has no private heap/window.
  This direct dependency uses its Apache-2.0 option and introduces no new package
  version or copied implementation. Complete Deflate consumption is checked.
- The actual Zstandard context remains with the reader/metadata operation and
  is reused across physical blocks, so future blocks do not allocate an unseen
  context after row sizing. Every call initializes fresh one-shot state. Stream
  preparation also acquires bounded payload/context storage when a stored first
  block precedes a compressed block. Framing inspection consumes no bytes and
  does not advance future value errors; all-stored streams need no codec context.
  Escaping payload credits do not keep the reader's codec workspace alive.
- Presence-stream errors are propagated through nullable parent combination.
  Negative/overflowing lengths, invalid dictionary/union keys and truncated skipped
  strings fail explicitly. Union skips count tags and advance only the matching
  child streams. Regression observations retain the former discarded-presence-error
  and over-skipped-union failures. Skip length/presence scans use bounded stack
  scratch instead of allocating in proportion to the entire skipped range.

The source comes from the SHA-256 verified crates.io archive. The Apache-2.0 license is preserved
from the repository's recorded upstream revision.

The archive's `Cargo.lock` is omitted because the repository root lockfile owns
runtime dependency resolution. Standalone provider tests use the reviewed
`.github/provider-locks/orc-rust.lock` configuration with default features disabled.
These are codec-boundary changes; query execution remains owned by ShardLoom.
The October 31 combined batch has not yet completed validation.
