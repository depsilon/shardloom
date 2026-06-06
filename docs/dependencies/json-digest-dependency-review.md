# JSON And Digest Dependency Review

## Purpose

This document records ShardLoom's current JSON and digest dependency posture for optional
`shardloom-vortex` features. It is a dependency-review ledger only; it does not authorize new
runtime behavior, package publication, performance claims, or fallback execution.

## Serde JSON 1.0.150

- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1151>.
- Direct manifest posture: `serde_json = "1.0"` remains optional in `shardloom-vortex`.
- Lockfile version: `serde_json = 1.0.150`.
- `cargo info serde_json@1.0.150` reports license `MIT OR Apache-2.0`, Rust version `1.71`,
  documentation <https://docs.rs/serde_json>, repository <https://github.com/serde-rs/json>, and
  crates.io version <https://crates.io/crates/serde_json/1.0.150>.
- The release note says non-string enum object keys are rejected.

## Sha2 0.11.0

- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1152>.
- Direct manifest posture: `sha2 = "0.11"` remains optional in `shardloom-vortex`.
- Lockfile version: `sha2 = 0.11.0`.
- `Cargo.lock` also records digest `0.11.3` for the updated SHA-2 stack.
- `cargo info sha2@0.11.0` reports license `MIT OR Apache-2.0`, Rust version `1.85`,
  documentation <https://docs.rs/sha2>, repository <https://github.com/RustCrypto/hashes>, and
  crates.io version <https://crates.io/crates/sha2/0.11.0>.

## No-Fallback Posture

- Both dependencies remain optional through `shardloom-vortex` feature gates.
- They do not add Spark, DataFusion, DuckDB, Polars, Velox, pandas, Dask, Ray, Trino, a Vortex
  query-engine integration, or any external runtime fallback.
- The update does not broaden the ShardLoom runtime claim boundary.
