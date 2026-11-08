# Standalone patched-provider validation locks

These fifteen lockfiles freeze the current excluded provider test configurations.
`scripts/ci_provider_tests.py` copies the matching lock to each provider before
running its recorded feature/patch/target configuration with `--locked`.
The four CI groups share compatible Cargo fingerprints within each job; they
do not combine every provider into a maximal-feature graph. The buffer command
still includes integration tests and doctests.

## November 8 provider migration

The current graphs resolve Vortex 0.93.0 and Arrow/Parquet 59.3.2. RunEnd's source
and tests move into `vortex-array`, so its obsolete separate lock is removed.
Parquet adds a default-codec library and enabled-integration-test configuration,
with the patched Arrow array/data/IPC providers and both committed fixture roots.
Its `test_common` benchmark helpers are unnecessary for these targets and remain
disabled; the library's unit tests receive their normal test configuration.
Missing fixture roots fail before Cargo starts. No external engine is admitted.

The added `vortex-arrow` configuration checks its complete library tests and
strict library/test lint with the reviewed array, buffer, Arrow array/data and
FSST patches. Its scoped decimal import preserves physical storage only after
ShardLoom's ancestor-aware value validation. Its new lock adds no package identity
outside the previously reviewed root and standalone graphs.

Resolution retains unchanged packages from prior locks where possible. The
Parquet graph starts from the root lock. Relative to the exact prior standalone
graphs, 34 current development-only identities are absent; their checksum-verified archives
declare Apache-2.0, MIT or combinations of those licenses. Declared Rust minimums
do not exceed 1.95. This review does not add these development packages to the
runtime or turn optional features on. ORC retains its already-reviewed upstream
Arrow 58.4.1 development dependencies; the runtime has one Arrow 59.3.2 family.
The additional identity since the initial 33-package review is the registry
`vortex-arrow` 0.93.0 used by some standalone tests; the runtime now uses its
local path provider. This is an existing reviewed archive, not a new version.
The historical graph reviews below retain their original versions and outcomes.

The [provider migration contract](../../docs/architecture/native-provider-upgrade-2026-11-08.md)
tracks whole-engine and installed-package acceptance separately from these locks.
The [November 8 acceptance packet](../../docs/release/evidence/native-provider-upgrade-2026-11-08/README.md)
verifies passing tests and strict lint for all fifteen configurations against
their current compiled sources, direct patches, lockfiles and runner settings.
Earlier passing commands are reused only when those inputs match; provenance-only
metadata changes are verified separately. Whole-workspace and isolated installed
wheel acceptance also pass for the current graph, with hosted integration pending.

The initial October 25 resolution starts from workspace `Cargo.lock` SHA-256
`2784ccccf7f886593291618ea7cfd8494dc7405757ef0d416b030d160f473bea`, resolves
each standalone manifest with its actual CI arguments, then verifies the result
with `cargo metadata --locked`. Tests and strict Clippy pass for all eight
configurations. Root dependencies, provider source and runtime features are
unchanged. These locks are CI inputs, not replacement runtime lockfiles.

## Provenance review

The standalone graphs previously resolved additional development dependencies
on every CI run. Relative to the prior local locks, the initial frozen graphs
contain these reviewed version changes (some already match the root lock):

| Packages | Versions | Declared licenses |
| --- | --- | --- |
| aws-lc-rs / aws-lc-sys | 1.18.2 / 0.46.0 | ISC, Apache-2.0, MIT, BSD-3-Clause and MIT-0 in the manifests' combined expressions |
| const_format / const_format_proc_macros | 0.2.37 / 0.2.37 | Zlib; optional lock entries outside the selected metadata graph |
| schemars / combine | 1.2.3 / 4.6.9 | MIT |
| prost / prost-derive | 0.14.4 / 0.14.4 | Apache-2.0 |
| rstest_reuse / memmap2 | 0.7.1 / 0.9.12 | MIT OR Apache-2.0 |
| rustls-native-certs / schannel | 0.8.5 / 0.1.30 | Apache-2.0 OR ISC OR MIT / MIT |
| tokio | 1.54.1 | MIT |
| webpki-root-certs | 1.0.11 | CDLA-Permissive-2.0 certificate data |
| js-sys / wasm-bindgen-futures | 0.3.106 / 0.4.79 | MIT OR Apache-2.0 |
| wasm-bindgen and its macro, macro-support and shared crates | 0.2.129 | MIT OR Apache-2.0 |

License evidence comes from Cargo package manifests and the registry metadata
for [const_format](https://crates.io/api/v1/crates/const_format/0.2.37) and
[const_format_proc_macros](https://crates.io/api/v1/crates/const_format_proc_macros/0.2.37).
Registry checksums remain in the lockfiles. No external implementation code is
copied or modified by this change.

`colored 3.1.1` declares MPL-2.0 and is already present through the providers'
development benchmark/test graph. The completed [#1604 native job](https://github.com/depsilon/shardloom/actions/runs/40167963537/job/121092781115)
both resolves and compiles that exact version. It is absent from the workspace
runtime lock and shipped CLI/extension graph. Provider jobs retain reports and
logs, not these development executables. Its source and notices stay intact.
This scoped review preserves existing test coverage and does not widen the
runtime license allowlist. Mozilla documents file-level obligations and
[use/distribution boundaries](https://www.mozilla.org/en-US/MPL/2.0/FAQ/);
shipping these development binaries would require its own distribution review.

Update a lock only with the matching configuration arguments, inspect the
resolved dependency/license changes, and rerun that provider's tests and lint.
Do not remove development tests or add an external execution engine to simplify
resolution.

## October 31 ORC provider configuration

`orc-rust.lock` freezes the existing provider manifest with
`--no-default-features`, matching ShardLoom's synchronous codec boundary.
The `formats` CI group runs its complete library tests and strict library/test
Clippy checks. This supplements the earlier provider configurations; their
historical acceptance above is unchanged. ORC validation for the combined I/O
batch remains pending until the corresponding command receipts pass.

The resolution was seeded from the workspace lock and then resolved against the
provider's unchanged dependency declarations. Its development dependencies include
Arrow 58.4.1, Criterion, Proptest and OpenDAL; these are excluded from the shipped
runtime graph. The runtime Arrow boundary remains 59.3.1. The
[resolution review](orc-rust-review.json) records the root and standalone lock
identities and every added package/version relative to the root lock. All added
packages provide Apache-2.0, MIT or BSD-3-Clause terms in their Cargo manifests;
existing upstream license texts are preserved. No query-engine dependency or
runtime fallback is introduced.

The November 1 controlled Deflate integration adds a direct `miniz_oxide` edge
to the ORC lock at its existing pinned version. No package version changes.

## November 1 Avro provider configuration

`arrow-avro.lock` freezes the complete standalone Arrow Avro 59.3.1 library
test graph with `--no-default-features --features deflate,snappy`, matching the
admitted runtime codecs. The `formats` group runs all upstream and local library
tests, then strict library/test Clippy. Its `ARROW_TEST_DATA` points at the
committed, identity-verified fixtures under `vendor/arrow-avro/test/arrow-testing/data`;
it does not inherit a machine-specific fixture path or Vortex lint aliases.

The [Avro resolution review](arrow-avro-review.json) records all 57 package/version
entries absent from the root graph, their registry checksums and exact manifest
licenses. All provide permissive terms; the optional `libbz2-rs-sys` entry uses
the attribution-and-change-notice bzip2 license. The optional XZ entry's bundled
`COPYING` identifies liblzma as 0BSD and distinguishes the GPL/LGPL scripts and
build helpers. Those optional Bzip2/XZ codecs are disabled in this CI recipe
and the engine runtime. Their implementation is not copied into this repository.
The standalone lock is a test input; it does not widen runtime features, update
the engine's dependency graph or add a fallback engine.

## November 1 IPC provider configuration

`arrow-ipc.lock` fixes the complete Arrow IPC library tests at Arrow 59.3.1 with
`--features lz4,zstd`. The `formats` group runs these tests and strict library/test
Clippy with its own lint scope. These tests generate their fixtures locally.

The [IPC resolution review](arrow-ipc-review.json) records the 23 registry
package/version entries absent from the runtime lock, including exact archive,
manifest and bundled-license hashes. Their declared terms are Apache-2.0, MIT,
Unlicense or combinations of those permissive licenses. The resolution was
seeded from the root lock to preserve its exact Arrow versions. This standalone
development graph does not update the runtime's package versions. The runtime
adds only a direct IPC edge to its already pinned `bytes` package for retained
buffer ownership.
