# JSON And Digest Dependency Review

## Purpose

This document records ShardLoom's current JSON and digest dependency posture for optional
`shardloom-vortex` features. It is a dependency-review ledger only; it does not authorize new
runtime behavior, package publication, performance claims, or fallback execution.

## October 24, 2026 current candidate

The current candidate pins PyO3 runtime to 0.29.3 and uses the independent
`pyo3-build-config` 0.30.1 build helper only from `shardloom-python/build.rs` to call
`add_extension_module_link_args()`. The root dependency graph now aligns Vortex 0.90.0,
Arrow 59.3.1, Parquet 59.3.1 and ORC 0.9.0. This updates the compatibility-candidate
status recorded in the October 19 intake; its JSON/digest versions remain
locked at `serde_json` 1.0.154 and `sha2` 0.11.1. See the
[structured-format dependency review](structured-format-dependency-review.md) and
[controlled Vortex provider-refresh plan](../architecture/native-vortex-provider-refresh-2026-10-24.md).

Dependabot PR [1568](https://github.com/depsilon/shardloom/pull/1568) supplies the separate
build helper version included here; the PyO3 runtime stays at 0.29.3 for the Arrow/Python
bridge. PR 1568 was closed on October 24 for consolidation into this active
provider-refresh work; the replacement still requires full acceptance.

The broader migration remains bounded by existing feature and no-fallback policies;
dependency versions alone do not admit richer types, V2 scans, optional encodings,
connectors, external engines or performance claims.

Native workspace tests passed 6,071 tests across 118 binaries with zero failures and 35
ignored, and native strict workspace lint passed (`test-workspace-native-090-r4`,
`clippy-workspace-native-090-r5`). Provider rechecks and all-feature, minimal,
Rust 1.96.0 and default workspace checks pass (`remaining-validation-summary-r1`).
Installed Python/CLI proof passes 140 native integration tests without skips, including
capsule and ownership regressions. Fresh full UAT and isolated Full43 acceptance
remain pending; these checks do not complete the provider-refresh acceptance.

## October 19, 2026 patch intake

This section preserves the compatibility-candidate state at the October 19 intake; the later
current candidate is summarized above.

The conditional-investigation checkpoint consolidates Dependabot
[#1570](https://github.com/depsilon/shardloom/pull/1570) and
[#1569](https://github.com/depsilon/shardloom/pull/1569). Only the two package
versions and checksums change in `Cargo.lock`; dependency edges, workspace
requirements and feature selection are unchanged.

| Dependency | Previous lock | Current lock | License | Upstream compiler floor |
| --- | --- | --- | --- | --- |
| `serde_json` | 1.0.151 | 1.0.154 | MIT OR Apache-2.0 | Rust 1.71 |
| `sha2` | 0.11.0 | 0.11.1 | MIT OR Apache-2.0 | Rust 1.88 |

The reviewed lock targets are `serde_json = 1.0.154` and `sha2 = 0.11.1`.
The dependency freshness gate keeps exact-version checks for these reviewed
targets and records both the historical intake IDs and their superseding patch PRs.

The tagged upstream manifests establish the
[Serde JSON metadata](https://github.com/serde-rs/json/blob/v1.0.154/Cargo.toml)
and [SHA-2 metadata](https://github.com/RustCrypto/hashes/blob/sha2-v0.11.1/sha2/Cargo.toml).
Both compiler floors fit ShardLoom's declared Rust 1.96 floor.
Both downloaded crate archives match their new lockfile checksums. All four
license texts match the existing bundled bytes exactly; the Python notice
manifest updates only the two versions and archive checksums.

Serde JSON's intervening releases correct signed-zero and infinity comparison
behavior in arbitrary-precision mode, improve the position reported for invalid
UTF-8, and support order-preserving maps without the standard library. See
[1.0.152](https://github.com/serde-rs/json/releases/tag/v1.0.152),
[1.0.153](https://github.com/serde-rs/json/releases/tag/v1.0.153), and
[1.0.154](https://github.com/serde-rs/json/releases/tag/v1.0.154).
SHA-2 0.11.1 corrects AArch64 round-constant loads and updates the RISC-V backend;
its [changelog](https://github.com/RustCrypto/hashes/blob/sha2-v0.11.1/sha2/CHANGELOG.md)
also records the compiler-floor increase. Existing source-generation, spill,
sink and security digest checks exercise the dependency through ShardLoom.

The individual bot branches had failing checks at intake. Fresh current-host
workspace/native, release-script, license and clean installed-package validation
passes for the combined branch with the new lockfile; the
[checkpoint record](../architecture/native-conditional-checkpoint-2026-10-19.md)
links complete evidence. Earlier compact-index comparison and Full43 records
remain tied to their original compiled dependency graph; they are not fresh
measurements of these patch versions. Hosted PR validation remains separate.

The failing native-wheel jobs identified stale license-manifest versions; the
release-script failures identified the previous exact-version admission targets.
Those records are updated together with the lock. Regression tests confirm that
stale and unreviewed versions still fail the gate and a missing live dependency
check still blocks benchmark refresh.

The same intake leaves two separate compatibility changes open. PR
[#1568](https://github.com/depsilon/shardloom/pull/1568) advances
`pyo3-build-config` to 0.30.1 while the Python runtime and Arrow bridge remain
pinned to PyO3 0.29.3. PR
[#1513](https://github.com/depsilon/shardloom/pull/1513) introduces Arrow 60,
Parquet 59 and ORC 0.9 alongside the admitted Arrow 58/Vortex 0.85 graph.
Those require coordinated provider and binding review; see the existing
[structured-format review](structured-format-dependency-review.md). Neither
change is necessary for compact local indices or this patch intake.
The freshness gate continues to treat those two PRs as unincorporated.

## Historical Serde JSON 1.0.151 intake

- Source: Dependabot PRs <https://github.com/depsilon/shardloom/pull/1151> and
  <https://github.com/depsilon/shardloom/pull/1405>.
- Direct manifest posture: `serde_json = "1.0"` remains optional in `shardloom-vortex`.
- Workspace dependency version: `serde_json = "1.0.151"`.
- Lockfile version at that intake: `serde_json = 1.0.151`.
- `cargo info serde_json@1.0.151` reports license `MIT OR Apache-2.0`, Rust version `1.71`,
  documentation <https://docs.rs/serde_json>, repository <https://github.com/serde-rs/json>, and
  crates.io version <https://crates.io/crates/serde_json/1.0.151>.
- The release note adds `RawValue::from_string_unchecked`; ShardLoom does not use that unsafe-style
  constructor as a runtime shortcut.

## Historical Sha2 0.11.0 intake

- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1152>.
- Direct manifest posture: `sha2 = "0.11"` remains optional in `shardloom-vortex`.
- Lockfile version at that intake: `sha2 = 0.11.0`.
- `Cargo.lock` also records digest `0.11.3` for the updated SHA-2 stack.
- `cargo info sha2@0.11.0` reports license `MIT OR Apache-2.0`, Rust version `1.85`,
  documentation <https://docs.rs/sha2>, repository <https://github.com/RustCrypto/hashes>, and
  crates.io version <https://crates.io/crates/sha2/0.11.0>.

## No-Fallback Posture

- Both dependencies remain optional through `shardloom-vortex` feature gates.
- They do not add Spark, DataFusion, DuckDB, Polars, Velox, pandas, Dask, Ray, Trino, a Vortex
  query-engine integration, or any external runtime fallback.
- The update does not broaden the ShardLoom runtime claim boundary.
