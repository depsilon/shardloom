# ShardLoom full-domain timestamp correction

This directory contains the Apache-2.0 `vortex-datetime-parts` 0.90.0 published
source. `upstream-provenance.json` records the registry archive and every
original member hash. The archive is authoritative; its VCS metadata records
the upstream revision and dirty checkout. The upstream generated `Cargo.lock`
is recorded but omitted locally, and `publish = false` prevents publication of
this local patch.

The published archive omits the six-byte `goldenfiles/datetimeparts.metadata`
fixture required by its metadata compatibility test. The retained fixture is
copied from that exact upstream revision; its URL, Git blob and SHA-256 are
recorded as supplemental test provenance. The upstream `src/lib.rs` matches
the archive, and its metadata test remains unchanged.

ShardLoom admits the full signed-i64 timestamp storage domain independently of
the calendar provider's narrower range. The published timestamp splitter casts
the day count to i32, corrupting second and millisecond endpoints when the
normal compressor selects DateTimeParts. This patch keeps days as i64 through
scalar splitting, array splitting, scalar reads and encoded comparisons.
Seconds and subseconds remain i32, and ordinary compression still narrows each
component to its observed range. The existing canonical decoder and serialized
child-type metadata already support i64 days; no layout ID or file schema
changes. The transient uncompressed day buffer grows by four bytes per row.

The previously retired comparison/constant-part patch is not restored: the
0.90.0 archive already contains those fixes. This is an independent storage
domain correction using the existing Vortex-native provider. It adds no query
engine, external fallback or broader calendar-range claim. Regression coverage
must include split/recombine, encoded scalar access, comparisons, canonical
decoding and public nested Arrow-file preparation at both signed endpoints.

The provider's unit tests use a local assertion helper that checks types,
lengths, logical equality and recursive canonical equality without rendering
array payloads. This retains the original comparison coverage while avoiding
value-dump diagnostics in hosted security analysis. The helper and its mismatch
controls are compiled only for tests; runtime execution is unchanged.
