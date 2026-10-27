# Structured Format And Regex Dependency Review

## Purpose

This document records the current Arrow, Parquet, and regex dependency posture for ShardLoom's
feature-gated local structured-format bridge and parsing helpers. It is a dependency-review ledger
only; it does not authorize broad adapter support, external execution fallback, package
publication, performance claims, or production readiness.

## October 24, 2026 current candidate

The root dependency graph now aligns Vortex 0.90.0 with the Arrow family and Parquet 59.3.1;
`orc-rust` is 0.9.0. The Arrow/Parquet bridge remains feature-gated, and this graph update does
not admit new formats, runtime capabilities or external-engine execution. `arrow-pyarrow` 59.3.1
resolves from the registry rather than the former workspace path patch; its unused vendor
directory is retired after installed capsule and ownership proof. See the
[controlled Vortex provider-refresh plan](../architecture/native-vortex-provider-refresh-2026-10-24.md).

Dependabot PR [1582](https://github.com/depsilon/shardloom/pull/1582) was reviewed as a
candidate set: `arrow-array`, `arrow-avro`, `arrow-flight` and `arrow-schema` 60 would split
the aligned graph and are not included. Parquet 59.3.1 and ORC 0.9.0 are integrated in this
candidate. PR 1582 was closed on October 24 for consolidation into this active
provider-refresh work, whose full acceptance remains pending;
the October 17 entry below retains PR 1513's historical review state.

## 2026-10-17 Arrow 60 / Parquet 59.3.1 Review Deferral

This section preserves the graph and deferral state at the October 17 review; the later candidate
is summarized in the current support snapshot below.

- Dependabot PR <https://github.com/depsilon/shardloom/pull/1513> remains open and is explicitly
  deferred pending this batch's disposition. No dependency or Cargo manifest changes are included
  here.
- Its proposed direct Arrow 60, Parquet 59.3.1, and ORC 0.9.0 updates would split the current
  Arrow graph across versions 58, 59 and 60. The saved PR compile logs show
  `arrow_schema::DataType` mismatches between Vortex's 58.4 and the direct 60.0
  types, plus ORC's Arrow 59.3.1 `ArrowReader<File>` not implementing the direct
  Arrow 60 `RecordBatchReader` trait. Release user-surface compilation reports
  45 errors; the MSRV library-test compilation reports 96 errors.
- Current constraints remain `arrow-array = 58.3.0`, `arrow-schema = 58.3.0`, and
  `parquet = 58.3.0`, with `arrow-pyarrow` pinned exactly to `58.3.0`,
  in the workspace, `vortex = 0.85`, and `orc-rust = 0.8.0` in
  `shardloom-vortex`. Upstream Vortex 0.85's Arrow 58 provider boundary and ORC 0.8's Arrow 58
  types do not support independently advancing the direct bridge crates.
- Evidence: [PR 1513](https://github.com/depsilon/shardloom/pull/1513),
  [Rust MSRV job](https://github.com/depsilon/shardloom/actions/runs/39038511974/job/117413083621),
  and [release user-surfaces compile job](https://github.com/depsilon/shardloom/actions/runs/39038511974/job/117413083766).
- Upstream version references: [Arrow Rust releases](https://github.com/apache/arrow-rs/releases),
  [Vortex 0.85.0](https://github.com/vortex-data/vortex/releases/tag/v0.85.0), and
  [orc-rust releases](https://github.com/datafusion-contrib/orc-rust/releases).
- Reconsider after a provider-alignment change removes the multiple Arrow type graph and the
  feature matrix compiles. This is a compatibility deferral, not a finding that the proposed
  versions are unsuitable in general.

## Current support snapshot (October 24, 2026)

- The local structured-format bridge remains feature-gated behind `universal-format-io` and
  benchmark-specific Vortex features.
- The current candidate uses Vortex 0.90.0, Arrow 59.3.1, Parquet 59.3.1 and `orc-rust` 0.9.0;
  the selected dependency versions do not by themselves broaden format support.
- `arrow-pyarrow` 59.3.1 resolves from the registry instead of a workspace path patch. The
  unused `vendor/arrow-pyarrow` directory is retired after installed capsule and ownership proof.
- PR 1582's Arrow 60 subset (`arrow-array`, `arrow-avro`, `arrow-flight` and `arrow-schema`)
  remains excluded because it would split the selected Arrow graph. Its Parquet 59.3.1 and
  ORC 0.9.0 candidates are integrated. PR 1582 is closed for consolidation; the
  provider-refresh replacement remains in acceptance.
- Native workspace tests passed 6,071 tests across 118 binaries with zero failures and 35
  ignored, and native strict workspace lint passed (`test-workspace-native-090-r4`,
  `clippy-workspace-native-090-r5`). Provider rechecks and all-feature, minimal,
  Rust 1.96.0 and default workspace checks pass (`remaining-validation-summary-r1`).
  Installed Python/CLI proof passes 140 native integration tests without skips, including
  malformed capsule names and owner lifetimes. Fresh full UAT remains pending.
- `regex` remains a ShardLoom-owned parsing and validation helper, not an execution provider.
- No Spark, DataFusion, DuckDB, Polars, Velox, pandas, Dask, Ray, Trino, Vortex query-engine
  integration or other fallback execution dependency is introduced.

The October 17 and August 29 sections below preserve their dated review decisions and graph
observations; they are not the current dependency snapshot.

## 2026-10-26 Local Parquet Query-Path Qualification

The feature-gated Parquet 59.3.1 reader now has a separate metadata-first source-demand path for
eligible ordinary relational queries over a single local Parquet file. It binds the native plan
before reading data and supplies demanded owned batches to existing Vortex operators without first
writing a complete prepared input artifact. This path uses the existing Parquet dependency and
does not change the dependency graph or broaden the admitted schema/operator boundary. Existing
prepared-artifact reuse remains available; directory intake, explicit schema overrides,
content-digest fingerprinting, explicit `prepare_once`, and explicit primitive options retain their
existing contracts. See the [source-demand contract](../architecture/native-source-demand-2026-10-26.md)
for the scope and limits. This qualification makes no performance or final-acceptance claim.

## 2026-08-29 Dependency Intake Closeout

- Closed Dependabot PRs <https://github.com/depsilon/shardloom/pull/1383>,
  <https://github.com/depsilon/shardloom/pull/1385>,
  <https://github.com/depsilon/shardloom/pull/1387>,
  <https://github.com/depsilon/shardloom/pull/1389>, and
  <https://github.com/depsilon/shardloom/pull/1391> without merging.
- Decision: keep ShardLoom's direct Arrow/Parquet bridge dependencies on `58.3.0` for now.
- Reason: current upstream Vortex `0.85.0` still declares `arrow-array ^58.3` and `parquet ^58.3`,
  while `orc-rust 0.8.0` still declares Arrow `^58.0`. Moving only ShardLoom's direct
  Arrow/Parquet bridge crates to `59.x` would create a duplicate Arrow type graph across the
  Vortex/ORC boundary and risks extra conversion/materialization in performance-sensitive local
  structured-format paths.
- Follow-up: re-admit Arrow/Parquet `59.x` or later only after a Vortex/ORC provider-alignment
  check proves a single Arrow boundary or an explicit no-regression adapter bridge.
- Dependabot grouping now batches `arrow-*`, `parquet`, and `orc-rust` as one
  `structured-format-stack` update so this compatibility decision is reviewed once.

## Runtime Utility Refresh

- Closed and absorbed Dependabot PRs <https://github.com/depsilon/shardloom/pull/1381>,
  <https://github.com/depsilon/shardloom/pull/1388>,
  <https://github.com/depsilon/shardloom/pull/1392>, and
  <https://github.com/depsilon/shardloom/pull/1394> into one cohesive dependency-intake PR.
- `futures = 0.3.34`.
- `memchr = 2.8.3`.
- `regex = 1.13.1`.
- `rustc-hash = 2.1.3`.
- Lockfile-only transitive remediation:
  - `crossbeam-epoch = 0.9.20` resolves `RUSTSEC-2026-0204` reported through the Vortex/Moka
    dependency path.
  - `chacha20 = 0.10.2` replaces the yanked `0.10.0` version reported through the Vortex/Rand
    dependency path.
- `cargo info futures@0.3.34` reports license `MIT OR Apache-2.0` and Rust version `1.71`.
- `cargo info memchr@2.8.3` reports license `Unlicense OR MIT` and Rust version `1.61`.
- `cargo info regex@1.13.1` reports license `MIT OR Apache-2.0` and Rust version `1.65`.
- `cargo info rustc-hash@2.1.3` reports license `Apache-2.0 OR MIT` and Rust version `1.77`.
- These crates remain utility dependencies inside ShardLoom-owned parse, scan, hashing, async helper,
  and upstream-provider support paths. They are not planners, query engines, residual executors, or
  fallback runtimes.

## Arrow And Parquet 59.0.0 Review Deferral

- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1224> updates
  `arrow-avro = 59.0.0`.
- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1225> updates
  `arrow-array = 59.0.0`.
- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1227> updates
  `arrow-schema = 59.0.0`.
- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1228> updates
  `parquet = 59.0.0`.
- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1229> updates
  `arrow-ipc = 59.0.0`.
- `cargo info arrow-array@59.0.0` reports license `Apache-2.0 AND MIT`, Rust version `1.85`,
  documentation <https://docs.rs/arrow-array/59.0.0>, repository
  <https://github.com/apache/arrow-rs>, and crates.io version
  <https://crates.io/crates/arrow-array/59.0.0>.
- `cargo info arrow-avro@59.0.0` reports license `Apache-2.0`, Rust version `1.85`,
  documentation <https://docs.rs/arrow-avro/59.0.0>, repository
  <https://github.com/apache/arrow-rs>, and crates.io version
  <https://crates.io/crates/arrow-avro/59.0.0>.
- `cargo info arrow-ipc@59.0.0` reports license `Apache-2.0`, Rust version `1.85`,
  documentation <https://docs.rs/arrow-ipc/59.0.0>, repository
  <https://github.com/apache/arrow-rs>, and crates.io version
  <https://crates.io/crates/arrow-ipc/59.0.0>.
- `cargo info arrow-schema@59.0.0` reports license `Apache-2.0`, Rust version `1.85`,
  documentation <https://docs.rs/arrow-schema/59.0.0>, repository
  <https://github.com/apache/arrow-rs>, and crates.io version
  <https://crates.io/crates/arrow-schema/59.0.0>.
- `cargo info parquet@59.0.0` reports license `Apache-2.0`, Rust version `1.85`, documentation
  <https://docs.rs/parquet/59.0.0>, repository <https://github.com/apache/arrow-rs>, and crates.io
  version <https://crates.io/crates/parquet/59.0.0>.

Compatibility decision:

- The Arrow/Parquet 59 Dependabot PRs were reviewed but are not admitted in the current PR.
- The closed Arrow/Parquet 59.1 Dependabot PRs were also reviewed during the 2026-08-29
  dependency-intake pass. Current crates.io latest is 59.2, so the closed PRs were stale as well as
  provider-misaligned.
- The `vortex-traditional-analytics-benchmark` feature lane still compiles through Vortex Arrow
  import/export boundaries and `orc-rust 0.8.0`, both of which require Arrow 58-compatible record
  batch and schema types for this ShardLoom integration.
- Forcing Arrow/Parquet 59 into ShardLoom directly would require an additional conversion or
  serialization boundary before the current Vortex/ORC providers align. That would add complexity
  and potential copy/materialization overhead in a performance-sensitive lane without adding
  ShardLoom runtime capability.
- Re-admit these PRs only after upstream provider alignment or an explicit no-regression adapter
  plan proves the boundary keeps Vortex-native/no-fallback evidence intact.

Runtime boundary:

- These crates remain local file-format boundary readers/writers used for compatibility-format
  smoke, troubleshooting, feature-gated local SQL source/output smoke, and traditional benchmark
  fixture preparation.
- They are not query engines, residual executors, broad production adapters, object-store/table
  runtimes, or hidden fallback paths.
- Compatibility-format input must still be normalized into ShardLoom SourceState/Vortex-prepared
  evidence before ShardLoom runtime rows claim ShardLoom execution.
- Arrow conversion remains an explicit admitted boundary for artifact preparation and local output
  compatibility only; it must not become decoded Arrow execution by default.

## Regex 1.13.1

- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1392> was absorbed into the
  2026-08-29 dependency-intake PR and updates `regex = 1.13.1`.
- `cargo info regex@1.13.1` reports license `MIT OR Apache-2.0`, Rust version `1.65`,
  documentation <https://docs.rs/regex>, repository <https://github.com/rust-lang/regex>, and
  crates.io version <https://crates.io/crates/regex/1.13.1>.
- The dependency remains a parsing and validation helper in ShardLoom-owned code paths.
- It does not add a runtime engine, query planner, external compute provider, or fallback execution
  path.

## License and no-fallback posture

- Apache-2.0, MIT, and `MIT OR Apache-2.0` licenses are compatible with the current ShardLoom
  dependency policy.
- The updates do not copy upstream implementation code into ShardLoom.
- Default builds remain lightweight for Vortex/structured-format feature gates.
- Fallback execution remains disabled.
