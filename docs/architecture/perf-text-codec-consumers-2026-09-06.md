# Native text codec and consumer portfolio

This test-only PERF-08/PERF-09/PERF-12 packet compares executable native text
representations and their complete consumers. It adds no advisor, controller,
public option, dependency or production codec switch. The retained numeric
compression and rejected unconditional text-zoning decision remain unchanged.

The pinned Vortex 0.85 provider offers three relevant existing boundaries:

- `vortex-array/src/arrays/dict` binds each code array to its own values array.
  The experiment builds native `DictArray` objects and deliberately reverses the
  values domain in alternate source chunks. Domain construction is timed once.
- `vortex-fsst/src/compress.rs` exposes `fsst_train_compressor` and
  `fsst_compress` for native VarBin/VarBinView input, including nullable rows.
  Training and encoding are separate from file writing. Its LIKE kernel in
  `vortex-fsst/src/compute/like.rs` scans encoded symbols for admitted constant
  case-sensitive patterns and preserves validity. This does not establish that
  every ShardLoom nullable predicate dispatch stays encoded.
- `large_source_fast_zstd_text_leaf_strategy` is the actual retained source-text
  writer in `vortex_ingest.rs`. It canonicalizes and compacts native views, then
  calls `Zstd::from_var_bin_view_without_dict` at level -3 with at most 8,192
  valid values per frame. That work stays inside the retained write clock.

Each representation writes the same renamed text column and exact Int64
identifier above 2^60 through native layouts. The Zstd control uses the retained
source-text composition directly. Dict and FSST preserve their already encoded
text arrays through native Chunked/Flat leaves, while using the identical retained
numeric/table composition for other fields. All three wrap each source batch with the
existing bounded local-EOF writer. There is no new text zoning, changed numeric
compressor, cross-batch coalescing or column-footer transposition.

Fixtures cover categorical UTF8, reordered dictionary epochs and high-cardinality
Unicode, with separate nullable and nonnullable cases, empty strings and literal
pattern metacharacters. Source rows/text lengths and aggregate result cardinality
are bounded before preparation. Files use an owned temporary directory, bounded
safe native publication, explicit sync, full SHA-256 readback and native reopen.
An exact physical inventory reports every persisted Flat encoding and segment
length; it is outside query clocks and is not a device-I/O measurement.

The consumers are existing `execute_vortex_local_primitive_with_policy` calls:
COUNT(text), complete GROUP BY text COUNT(*), and contains/not-contains COUNT
WHERE. No local top-K replaces the complete group oracle. Metadata answers and
decoded paths are admitted outcomes and their actual execution evidence remains
visible. Every result is independently checked against source values outside the
query clock. Full native scalar readback separately checks all identifiers, text
and nulls. Each query call returns its complete native result report; timing does
not stop at lazy native-array return.

Source construction, native session setup, train/build preparation, writer strategy
setup, native write/publication, explicit
sync, full-hash readback, native footer reopen, retained writer-summary release,
physical inventory and exact
verification have separate elapsed spans. Reuse 1/10/100 records contain actual
query samples and cumulative arithmetic including every representation's source
and train/build work once, write/sync/hash/reopen/summary-release once, and all query calls through
that reuse count. Validation and diagnostic inventory are separately reported,
excluded from the operational lifecycle, and never described as free. That
operational field is a sum of named spans, not continuous wall time; it also
excludes source hashing and intervening bookkeeping. Separate continuous clocks
start before source preparation and end after each complete four-query round's
validation, and after all validation and owned artifact removal. They include
diagnostics, source hashing and intervening bookkeeping. The shared outer write
context and temporary-directory setup precede these per-case clocks. Reuse
means repeated full native query calls against one immutable file; there is no
cached-answer shortcut or claim of controlled cold device caches.

The existing shared allocator accounts native allocations that use it and the
retained layout-reference owners. FSST training/scratch, Zstd helpers and some
native buffer constructors allocate outside that allocator. The test therefore
reports finite fixture/preparation/file limits and the precise owned-byte scope;
it does not claim a complete provider-memory or process-RSS ceiling. Every
observable retained lease must release after the file/results/strategy owners
drop. Unknown encodings, changed dtype/values, truncated inventories, publication
failures and exceeded limits fail the case without producing a successful score.

The small correctness tests pass with one reuse, real persisted codecs and
complete native query values. A separately ignored release matrix uses
bounded fixtures and records reuse 1/10/100 with alternating/rotated codec order,
raw elapsed samples, complete-value/source hashes and persisted geometry. Root
owns serial compilation and timing. No retention or public performance claim is
authorized by the design alone.
