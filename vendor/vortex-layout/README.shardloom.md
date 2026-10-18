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
