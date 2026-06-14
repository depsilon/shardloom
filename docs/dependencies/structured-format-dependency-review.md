# Structured Format And Regex Dependency Review

## Purpose

This document records the current Arrow, Parquet, and regex dependency posture for ShardLoom's
feature-gated local structured-format bridge and parsing helpers. It is a dependency-review ledger
only; it does not authorize broad adapter support, external execution fallback, package
publication, performance claims, or production readiness.

## Current support snapshot

- The local structured-format bridge remains feature-gated behind `universal-format-io` and
  benchmark-specific Vortex feature gates.
- `orc-rust 0.8.0` remains unchanged and still brings the Arrow 58 stack transitively for ORC.
- ShardLoom's direct Arrow and Parquet bridge dependencies remain on Arrow 58.3 / Parquet 58.3
  because Vortex 0.75 and ORC currently expose Arrow 58 provider boundaries in ShardLoom's
  feature-complete local structured-format lane.
- `regex` remains a ShardLoom-owned parsing/validation helper, not an execution provider.
- No Spark, DataFusion, DuckDB, Polars, Velox, pandas, Dask, Ray, Trino, Vortex query-engine
  integration, or other fallback execution dependency is introduced.

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
- The `vortex-traditional-analytics-benchmark` feature lane still compiles through Vortex 0.75
  Arrow import/export boundaries and `orc-rust 0.8.0`, both of which require Arrow 58-compatible
  record batch and schema types for this ShardLoom integration.
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

## Regex 1.12.4

- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1226> updates
  `regex = 1.12.4`.
- `cargo info regex@1.12.4` reports license `MIT OR Apache-2.0`, Rust version `1.65`,
  documentation <https://docs.rs/regex>, repository <https://github.com/rust-lang/regex>, and
  crates.io version <https://crates.io/crates/regex/1.12.4>.
- The dependency remains a parsing and validation helper in ShardLoom-owned code paths.
- It does not add a runtime engine, query planner, external compute provider, or fallback execution
  path.

## License and no-fallback posture

- Apache-2.0, MIT, and `MIT OR Apache-2.0` licenses are compatible with the current ShardLoom
  dependency policy.
- The updates do not copy upstream implementation code into ShardLoom.
- Default builds remain lightweight for Vortex/structured-format feature gates.
- Fallback execution remains disabled.
