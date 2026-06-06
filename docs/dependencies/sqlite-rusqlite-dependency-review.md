# SQLite/Rusqlite Dependency Review

## Purpose

This document records ShardLoom's current `rusqlite` dependency posture for the local SQLite
fixture smoke. It is a dependency-review ledger only; it does not authorize arbitrary SQL,
query pushdown, network database connectors, Vortex ingest from SQLite, production database support,
or fallback execution.

## Current support snapshot

- Direct crate: `rusqlite = 0.40.1` in `shardloom-cli`.
- Feature posture: `default-features = false`, `features = ["bundled"]`.
- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1153>.
- `cargo info rusqlite@0.40.1` reports license `MIT`, documentation
  <https://docs.rs/rusqlite/>, repository <https://github.com/rusqlite/rusqlite>, and crates.io
  version <https://crates.io/crates/rusqlite/0.40.1>.
- Immediate host-target transitive dependency for SQLite FFI: `libsqlite3-sys = 0.38.1`, license
  `MIT` per `cargo info libsqlite3-sys@0.38.1`.
- ShardLoom intentionally disables `rusqlite` default features for this fixture path. The local
  smoke does not use `prepare_cached`, and ShardLoom is not a wasm SQLite runtime, so the update
  keeps `hashlink`, `sqlite-wasm-rs`, `rsqlite-vfs`, `wasm-bindgen`, and `js-sys` out of the
  normal host-target dependency tree.

## Runtime boundary

- SQLite remains a local named-table import/export fixture smoke through
  `sqlite-local-import-export-smoke`.
- The path may open a caller-provided local SQLite file, scan a named table, write a workspace-safe
  JSONL export, and create a roundtrip local SQLite artifact with replay evidence.
- Query pushdown remains false; optional ordering is ShardLoom fixture post-scan ordering, not
  SQLite execution support.
- BLOB schemas/values remain blocked by deterministic diagnostics.
- SQLite is not a ShardLoom residual executor, query engine fallback, production connector,
  credentialed/network database surface, or Vortex ingest input.

## License and no-fallback posture

- `rusqlite` and `libsqlite3-sys` are MIT-licensed and compatible with the current Apache-2.0
  dependency policy.
- `deny.toml` already allows `MIT`.
- The update does not add Spark, DataFusion, DuckDB, Polars, Velox, pandas, Dask, Ray, Trino, a
  Vortex query-engine integration, or another forbidden runtime fallback dependency.
- The dependency is runtime-scoped only for the named local SQLite fixture smoke in
  `shardloom-cli`; it is not a broad database execution claim.

## Validation

- `cargo check -p shardloom-cli --all-targets`
- `cargo clippy -p shardloom-vortex -p shardloom-cli --all-targets -- -D warnings`
- `cargo test -p shardloom-cli --test sqlite_local_runtime_snapshots`
- `cargo tree -p shardloom-cli --edges normal --depth 3` confirms the host-target normal tree
  includes `rusqlite v0.40.1` and `libsqlite3-sys v0.38.1` without `hashlink`,
  `sqlite-wasm-rs`, `rsqlite-vfs`, `wasm-bindgen`, or `js-sys`.
