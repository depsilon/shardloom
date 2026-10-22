# ShardLoom native layout patches

This directory contains the reviewed Apache-2.0 `vortex-layout` 0.85.0 registry
source. `upstream-provenance.json` records the authoritative archive, original
member hashes, license and dirty upstream revision. The workspace lockfile owns
dependency resolution; the archive's crate-local lockfile is recorded but omitted.

ShardLoom changes `src/layouts/file_stats.rs` to retain bounded running native
statistics instead of one builder entry per input batch. Unknown observations
and overflow invalidate a statistic; an all-null batch does not invalidate a
known extremum. Variable-length bounds retain independent compact bytes, and
disabled statistics do not execute input columns. Nullable root structs retain
one empty statistics set per field because root validity cannot be represented
by unmasked child bounds. Native `StatsSet` merging and
scalar truncation supply the existing precision semantics.

The accumulator also exposes a lazy, exact-size statistics iterator retaining
its existing lock. Creating it allocates no statistics table. The native file
writer admits both output table capacities before producing entries, without
an intermediate vector. The existing vector-returning API remains available
for caller-owned data. Running accumulator state and independently allocated
statistics payloads retain their separate ownership scope.

File/array/layout identities, serialization, provider version and dependencies
are unchanged. The `publish = false` manifest marker prevents publication of
this local patch. No query engine or execution fallback is introduced.

The owning contract is
`docs/architecture/native-persisted-reuse-2026-10-11.md`. ShardLoom integration
must reserve retained state and footer workspace, prove owner release and
budget refusal, verify complete write/reopen values, and pass the frozen
lifecycle cost gate before claiming acceptance.

The nullable structural-reader correction in `src/layouts/struct_/reader.rs`
uses native mask expressions and a private validity partition. It preserves
whole-record DTypes, masks field access before null-sensitive expressions, and
keeps nullable predicate pruning conservative. The non-nullable partition path
is retained. Its contract and acceptance are tracked in
`docs/architecture/native-nullable-layout-2026-10-18.md`.

The `native_vortex_tests` CI lane runs this excluded crate's complete library
test target, including the accumulator and nullable structural-reader
regressions, plus strict library Clippy. It seeds the
ignored crate-local lockfile from the workspace lockfile, applies the reviewed
`vortex-array` patch and enables the provider's test harness and local runtime.
Cargo resolves additional upstream test-only dependencies for that target;
production resolution remains the root workspace's locked graph. The ordinary
ShardLoom tests separately exercise the linked provider through real native
writers, readers and resource ownership.

Five existing async trait declarations have method-local lint annotations for
`async_trait` 0.1.89's generated, redundant `must_use` attributes on boxed
futures. This permits the standalone strict Clippy target without suppressing
other warnings or changing their behavior.

Structural cache construction reserves checked backing-array capacity through
the existing session allocator before allocation. Viewed layout clones share
one cache owner; lazy reader caches retain credit until their cells are freed.
Native struct, zoned and debug chunked readers admit their per-child DType and
name-reference vectors together with the cache before producing metadata.
The indexed producer validates cardinality and releases partial values and
container credit on error. Uniform release-mode chunked readers retain only the
cache charge. The original vector-taking fallible constructor admits new cache
cells while preserving its caller-owned metadata-vector boundary.
The original caller-directed infallible constructors remain explicitly outside
allocator admission for compatibility. Name text, nested DTypes, wide-field
lookup maps, expression caches, layout-specific data other than chunk offsets,
and fixed wrapper bookkeeping are separate allocation classes.

Chunk split classification uses no backing array for uniform all/none answers.
A mixed classification admits its flag array before allocation, retains the
reservation with that array and can retry after denial. The ownership contract
and outstanding structural owners are in
`docs/architecture/native-structural-provider-ownership-2026-10-21.md`.

Chunked layouts admit their cumulative row-offset tables during deserialization
and fallible native writer construction. Independently cloned layout data shares
one immutable offset owner and its credit. Empty layouts use a static zero
boundary; child-count arithmetic, payload size and row-count accumulation are
checked. Legacy caller-directed constructors remain outside admission. Native
chunked, dictionary and flat writer paths use the admitted constructor.

Layout depth-first traversal propagates child-construction errors and terminates
that traversal. It no longer returns an apparently complete parent after denied
child admission; callers can start a fresh traversal after credit is available.
