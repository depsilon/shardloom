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

The source comes from the SHA-256 verified crates.io archive. The Apache-2.0 license is preserved
from the repository's recorded upstream revision.

The archive's `Cargo.lock` is omitted because the repository root lockfile owns
runtime dependency resolution. Standalone provider tests use the reviewed
`.github/provider-locks/orc-rust.lock` configuration with default features disabled.
These are codec-boundary changes; query execution remains owned by ShardLoom.
The October 31 combined batch has not yet completed validation.
