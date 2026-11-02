# SQLite/Rusqlite Dependency Review

## Purpose

This document records ShardLoom's current `rusqlite` dependency posture for the local SQLite
fixture smoke. It is a dependency-review ledger only; it does not authorize arbitrary SQL,
query pushdown, network database connectors, Vortex ingest from SQLite, production database support,
or fallback execution.

## Current support snapshot

- Direct crate: `rusqlite = 0.41.1` in `shardloom-cli`.
- Feature posture: `default-features = false`, `features = ["bundled"]`.
- Source: Dependabot PRs <https://github.com/depsilon/shardloom/pull/1153> and
  <https://github.com/depsilon/shardloom/pull/1406>, followed by the reviewed
  <https://github.com/depsilon/shardloom/pull/1636> update consolidated in #1639.
- Immediate host-target transitive dependency for SQLite FFI: `libsqlite3-sys = 0.39.1`.
- Both exact published archives declare `MIT`; their checksums, packaged manifests,
  VCS identities and full notices are recorded in the
  [November 2 intake](../architecture/dependency-intake-2026-11-02.md) and its
  [acceptance packet](../benchmarks/evidence/dependency-intake-2026-11-02/acceptance.json).
  This replaces the previously accepted 0.40.2 / 0.38.2 pair without broadening
  ShardLoom's SQLite fixture-smoke runtime scope.
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
- The November 2 complete default/native workspace tests, strict Clippy, dependency
  audit and notice validation cover the 0.41.1 / 0.39.1 pair. The freshness gate
  binds both the historical and superseding PR IDs to these exact reviewed versions;
  old or unreviewed versions remain blocked.
