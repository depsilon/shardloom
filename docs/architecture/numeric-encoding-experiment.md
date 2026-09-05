<!-- SPDX-License-Identifier: Apache-2.0 -->

# Isolated Numeric Encoding Experiment

The historical `vortex_encode_write_micros` field retains its name but now
reports measured inclusive provider writer wall time, including compression.
Earlier values subtracted summed concurrent compression work and are not
comparable to this corrected duration. Codec probe/storage work spans are
reported separately and are never subtracted from elapsed wall time.

Status: implementation experiment against `b3bb15adf4c0e74dac498000d78b931a9ca80674`.
This extends RFC 0044's native provider and measured-resource obligations. It does
not complete PERF-03, approve a release, or establish a performance improvement.

## Pinned Provider Finding

In Vortex 0.85.0, `vortex-layout/src/layouts/dict/writer.rs` compresses the first
chunk only to test whether its root is `Dict`. A non-dictionary result is dropped;
the original stream is sent to the alternative layout. ShardLoom's fast-load
alternative currently coalesces, buffers and writes Flat arrays without an
explicit data compressor. The Flat writer serializes its input array unchanged.

The baseline probe has a narrower scope than a fully enabled adaptive dictionary
probe. Its allowed encoding IDs come from `vortex::array::legacy_session()`;
the pinned implementation constructs an array-only session with no
`EnabledEditions`. Consequently its explicit scheme whitelist is empty. Built-in
canonical/constant decisions can still run, but this does not establish selection
of integer, float or text dictionary schemes. The regression exercises this exact
empty-whitelist policy and its discarded constant result.

Compression before repartitioning is insufficient: the pinned repartitioner
canonicalizes emitted chunk groups even when its input `canonicalize` option is
false. A selected encoding must therefore be retained after the last coalescing
stage to reach the artifact. The provider's default write strategy follows that
ordering and excludes the integer dictionary scheme from its data compressor.

## Bounded Candidate

Keep source preparation, source batch sizes, statistics zones, dictionary layout
selection, dictionary codes/values, text overrides, publication checksums, and
query operators unchanged. Change only the non-dictionary path for primitive
integer and floating-point column streams in the fast-load table strategy:

1. Existing native dictionary-layout probe with its baseline empty whitelist.
2. Existing native repartition/coalescing and canonicalization.
3. Explicit edition-restricted native BtrBlocks data compression for numeric
   streams, with integer and floating-point dictionary selection excluded.
4. Existing buffering, encoded-array validator, chunking and Flat serialization.

Non-numeric streams use their existing child strategy directly. In particular,
the candidate does not add compression to text dictionary codes or replace text
writers. Preserve an already admitted, serializable numeric encoding at the
post-coalescing compression boundary; the experiment does not claim preservation
through earlier canonicalizing stages.

Obtain the new numeric compressor and Flat validator whitelist from the actual
configured writer session, including the admitted allocator session on bounded
intake. Canonical nodes remain intrinsically permitted, matching the provider's
normalization rules. Do not insert individual encoding IDs or accept unknown
encodings to bypass validation. Keeping the original probe whitelist avoids
turning this numeric storage change into a text/dictionary selection experiment.

The native dictionary API does not return its discarded probe result, and its
input chunks differ from the coalesced storage chunks. Reusing that result is
therefore outside this experiment. Record numeric probe and final compression
work separately; do not add an answer/array cache or duplicate dictionary logic.

The umbrella crate exposes the native compressor builder and encoding whitelist
but not scheme constructors. Exclude dictionary-producing schemes by removing
the native `Dict` array ID from the compression whitelist; keep the full edition
whitelist for preservation of existing encoded arrays. The compression wrapper
uses an empty extra statistics set: the compressor computes its own required
statistics inside the measured call, while existing zone/file statistics remain
on their original routes.

## Evidence

Record monotonic elapsed work spans, call counts and logical input/output bytes
for numeric probe and final compression. Actual stored encoding roots come from
the explicit persisted-file inspection, not from probe predictions. Concurrent
service spans may overlap and are neither CPU time nor exclusive wall time.
Never derive a new encode/write duration by subtracting their sum from wall time.

Provide an explicit bounded artifact inspection operation using the persisted
layout tree, segment references and native serialized-array reader. Associate
encodings with actual field paths and distinguish data from auxiliary statistics
and dictionary-value layouts. Report segment byte attribution and any shared
segments; never infer every column's encoding from the artifact-wide layout set.
The inspection reads encoded segments and is separate from the ingest timer.

The `physical_encoding_inventory` example accepts one frozen Vortex artifact
path and emits JSON. Optional `--max-total-segment-bytes POSITIVE_BYTES` changes
only that inspection's explicit total-read limit (for example, `68719476736`
admits 64 GiB including repeated segment references). The default remains 32 GiB;
all other limits and permanent UAT guards are unchanged. Its recorded hash, open and inspection durations are
separate sequential spans. The inventory lists actual serialized array nodes
for every Flat reference, with native layout field paths, auxiliary roles and
physical segment IDs/lengths. Composite Flat arrays lacking a field layout stay
unattributed. Non-Flat segment references are explicitly listed as uninspected.
Limits bound traversal and requested encoded bytes; they do not certify RSS.
Use `--summary-only` to retain compact per-column actual encoding IDs, referenced
bytes, inspection counts, limits, source SHA and shared-byte scopes while omitting
per-reference array trees from emitted JSON. The library still inspects every
actual tree under the same limits. Rerun without that flag to reproduce full
reference detail; summary mode changes output size, not inspection coverage or
the existing UAT/log guards.
Hashing and inspection can affect cache residency, so run them outside timed
ingest/query phases and establish the intended cache policy afterward.

## Acceptance and Drop Decision

Focused tests must verify complete nullable integer/float values, signed and
unsigned extrema, constants, low-bit-width values, high-cardinality values and
existing encoded inputs. Reopen the artifact and verify actual physical encoding
trees, then run native projection/filter/count and relevant numeric aggregate or
sort consumers against literal exact expectations. Include renamed fields and a
text control column whose path is unchanged.

The main task owns all builds and measurements. Compare this candidate separately
against the frozen source on identical input: artifact bytes, complete correctness,
raw cold/hot query runs, full ingest time and full query time, with source/binary
hashes retained. Keep or drop only after measuring the storage/lifecycle tradeoff.
No new dependencies, unsafe code, query-engine fallback, changed checksum guarantee,
or temporary generated artifacts in synced source directories are permitted.
