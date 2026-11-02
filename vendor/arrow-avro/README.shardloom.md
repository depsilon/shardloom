# ShardLoom Arrow Avro continuation and resource patch

This is the Apache-2.0 `arrow-avro` 59.3.1 registry source from the SHA-256
verified archive recorded in `upstream-provenance.json`. Original license and
NOTICE texts are preserved. The archive lockfile is omitted because the root
workspace lockfile owns dependency resolution; this local package cannot publish.

The November 1 integration adds synchronous OCF next-batch controls capped by
the original positive maximum. Preparing a block validates its complete framing
and exposes the remaining row count before output allocation, without restarting
the decoder or replaying input. Record builders start empty and no longer restore
an unrelated 1024-row allocation on every flush. These allocation changes also
apply to the shared record decoder used by single-object streams; their schema
switching and value semantics are unchanged.

Regression controls reproduce and correct zero-byte record loss, silently
truncated blocks and missing sync validation. Header/block sync assembly now
preserves markers split across input buffers. Count/payload mismatches fail,
including leftover bytes, and synchronous reader errors are terminal.

An optional caller-owned block reservation covers incremental encoded storage,
compressed/decompressed overlap, growth replacement overlap and retained block
buffers. It supports the engine's admitted uncompressed, Deflate and Snappy
routes. Other reserved codecs fail explicitly; ordinary unreserved codec routes
remain available to existing provider callers. Snappy checksum truncation fails
before indexing. Controlled Deflate uses the already pinned miniz_oxide 0.8.9
core with stack-owned state and the credited output as its complete history.
Growth admits encoded/output/replacement overlap; truncated streams and trailing
members fail explicitly. Reader field order frees block bytes before returning
the block credit. Reducing rows cannot split this indivisible storage.

The separate optional `DecoderMemory` factory admits each decoded payload before
allocation. Fallible value, bitmap, offset, decimal and interval builders retain
independent credits through Arrow buffers, clones, slices and reader destruction.
Replacement storage overlaps its old credit. Dictionary values and string-view
descriptors use the same factory; error propagation precedes decoding later
fields. Controlled optional run-end gather is explicitly unsupported; ordinary
uncontrolled provider behavior remains available. This named allocation scope
is not an allocator or RSS bound. Header offsets and metadata bytes now use
the same fallible, independently credited storage; header clones share their
immutable owner and retain credits after reader destruction. Fragmented header
decoder reuse resets the complete initial state, and negative metadata lengths
fail before payload growth.

OCF schema parsing now admits temporary capacity before serde constructs its
buffered forms. The estimate counts source syntax, string bytes, depth,
concrete schema/field widths, map storage and overlapping projection forms.
Retained codec, projection, skipper, decoder and Arrow schema structures use a
separate source-shaped capacity. Named references propagate checked shapes
without expanding the graph during admission; general writer/reader resolution
includes candidate-pair and nested-copy capacity. Controlled expanded depth is
limited to 128 before recursive construction, including shallow JSON containing
a deep chain of named definitions. These bounds are conservative capacities,
not minimum-memory measurements. Raw single-object schema-store parsing and
cache containers remain caller-owned boundaries.

`schema_memory_reservation()` lets callers retain the structure credit with
schemas or batch metadata that outlive the decoder. Record payload credits
remain independent and automatic. The native adapter retains discovery credits
with its file schema and reserves its actual input buffer before construction.
It omits generated reader schemas for identity physical projections. The old
fixed Avro discovery/row envelope is removed; all these owners use the same
native pool. Sixteen native source/ownership controls pass, including operation
under 384 KiB of additional headroom and complete refunds. Whole-engine and
cross-interface acceptance remain separate.

Additional literal regressions reproduce and correct dense-union offsets leaking
across batches, string-view null loss and nested empty-record flush failure.
String views now retain the validated payload without rebuilding a string list;
nested records carry their explicit row count.

The original upstream fixtures are retained under `test/data` and
`test/arrow-testing`, with their Apache license texts and exact source identities
in `upstream-provenance.json`. Arrow release 59.3.1 resolves to
`265760646dec646fbdca5d26e4ba9f3c03a6bd91`; its Arrow-testing submodule is
`735ae7128d571398dd798d7ff004adebeb342883`. The original record source matches
the registry archive. Set `ARROW_TEST_DATA` to this package's
`test/arrow-testing/data` when running standalone upstream tests. The admitted
`--no-default-features --features deflate,snappy` library suite passes all 427
tests with none ignored or filtered. The complete optional type-feature suite
passes 448 tests, and strict provider/native Clippy passes; these are separate
from remaining engine-wide acceptance. The
`formats` provider CI group uses the committed fixture path and reviewed lock
in `.github/provider-locks/arrow-avro.lock` for this complete suite and strict
library/test lint. Existing provider configurations retain their commands.

All changes are original integration/correctness work. This adds no execution
engine, dependency upgrade or fallback. Physical geometry and shared source
integration are exercised through the workspace's pinned provider graph.

Format reference: [Apache Avro OCF specification](https://avro.apache.org/docs/1.12.0/specification/#object-container-files).
