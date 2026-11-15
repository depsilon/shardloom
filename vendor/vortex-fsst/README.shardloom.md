# Vortex FSST buffer-table patch

This is the Apache-2.0 `vortex-fsst` 0.93.0 published source. Its canonical
decoder calls the existing Vortex view-construction loops through their admitted
table helper. Temporary buffer collections and final handle-table conversion
overlap use the caller's allocator. FSST decompression, payloads, DTypes and
native serialization retain their existing behavior.

`upstream-provenance.json` records every original registry member, the archive
checksum and dirty VCS marker. The published archive is authoritative. The
original crate lockfile is recorded but omitted; the workspace controls
resolution. `Cargo.toml.orig` is unchanged and the normalized manifest disables
publication. The reviewed upstream Apache license is included.

`goldenfiles/fsst.metadata` is supplemental test data from the recorded upstream
Git revision, omitted from the registry archive. Its Git object identity, bytes
and SHA-256 are recorded separately; the associated test source matches the
published crate exactly. The full library suite uses that fixture unchanged.

The [ownership contract](../../docs/architecture/native-varbin-metadata-resources-2026-10-16.md)
defines the finite claim and acceptance checks. No external execution engine or
new unsafe boundary is introduced. Retire this patch only after an approved
upstream provider satisfies the same ownership and complete-workflow checks.

The November 15 FSST consumer investigation adds `like_observation.rs` and a
successful-return observation in `compute/like.rs`, exported from `lib.rs` only
with debug assertions. This ShardLoom-authored diagnostic uses the existing
Vortex session extension surface and explicit caller registration to count
successful calls and logical operand rows. It does not change matching,
validation or allocation policy, adds no dependency or unsafe code, and is
absent from release builds. Its harness allocation is not provider memory
accounting. See the [investigation contract](../../docs/architecture/native-fsst-consumer-2026-11-15.md).

The candidate prefilter adds `dfa/prefilter.rs`, changes the private matcher in
`dfa/mod.rs`, and connects it in `compute/like.rs`. It admits a positive literal
contains pattern only when a necessary byte is represented by at most two
symbol codes, always retaining escapes for exact matching. A negative test over
the complete compressed backing can skip matcher construction; individual
negative rows skip exact matching. Original offset execution, row slicing,
validity and result construction remain in force. Prefix, empty, unsupported,
case-insensitive and negated behavior remains on the existing route. Original
private matcher tests pass `None` for candidate codes; new exhaustive and
construction/escape tests exercise the added route. Debug-only counters record
actual candidate rejections and exact matcher work, with no release counter cost.

The normalized manifest adds a direct dependency on the already-locked
`memchr` 2.8.3, using its MIT license option. The original manifest and original
archive/member hashes remain unchanged. This is an experimental local provider
modification pending complete correctness, resource and performance acceptance;
it does not establish a retained optimization or a writer-default change.
