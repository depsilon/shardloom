# Vortex Dependency Footprint

## Purpose
ShardLoom is auditing the upstream Vortex dependency graph before deeper integration so default
builds stay lightweight while preserving a controlled path to upstream Vortex capability work.

## Current state
- Current direct dependency in `shardloom-vortex`: optional umbrella `vortex = 0.71`.
- Latest upstream intake note: `vortex = 0.71.0` was inventoried in
  `docs/dependencies/vortex-0.71-upstream-intake.md` and
  `docs/architecture/vortex-public-api-inventory.md`.
- Umbrella `vortex` crate is still used for upstream opt-in builds.
- Default build (`default = []`) does not enable upstream Vortex.
- Existing feature-gated Vortex file/local primitive/write paths remain explicitly scoped and
  claim-gated; the version bump does not broaden runtime support.
- Fallback execution engines are not present.

## Vortex 0.71 dependency bump proof

`GAR-VORTEX-071B` updated the optional upstream Vortex dependency from `0.70` to `0.71` and refreshed
`Cargo.lock` to Vortex `0.71.0` crate family versions.

Validation recorded for the bump:

- `cargo check -p shardloom-vortex`
- `cargo check -p shardloom-vortex --features upstream-vortex`
- `cargo check -p shardloom-vortex --features vortex-file-io`
- `cargo check -p shardloom-vortex --features vortex-write`
- `cargo check -p shardloom-vortex --features vortex-local-primitives`
- `cargo check -p shardloom-vortex --features vortex-traditional-analytics-benchmark`

Compatibility fix required:

- Vortex `0.71.0` adds `DType::Union`; ShardLoom now maps that dtype to the same deterministic
  unsupported local primitive posture as other non-admitted complex dtypes.

Claim boundary:

- The bump proves optional dependency compatibility only.
- It does not admit new Vortex runtime APIs, external engines, object-store/table support,
  SQL/DataFrame support, performance claims, package claims, or production readiness.

## Dependency families observed
From `cargo tree` inspection of `shardloom-vortex` with upstream enabled:
- Vortex workspace crates: `vortex-*` crates across array/layout/file/scan/session/etc.
- Arrow interop crates: `arrow-array`, `arrow-schema`, `arrow-*` family transitively from Vortex.
- Encoding/compression crates: `vortex-alp`, `vortex-btrblocks`, `vortex-pco`, `vortex-fsst`,
  `vortex-zstd`, and related compression helpers.
- FlatBuffers/Proto/metadata crates: `flatbuffers`, `prost`, `prost-types`, `vortex-flatbuffers`,
  `vortex-proto`.
- Async/runtime/IO crates: `futures`, `tokio`, `async-*`, `vortex-io`, `vortex-file`, `vortex-scan`.
- Build/native tooling crates: proc-macro crates and observed transitive build tooling in the graph.
- WASM/WIT-related crates: none confirmed as direct ShardLoom dependencies in this change.
- Fallback-engine-like crates: none observed for
  DataFusion/Spark/DuckDB/Polars/Velox/vortex-datafusion.

## Direct fallback dependency check
Direct dependencies in `shardloom-vortex`:
- `datafusion`: absent
- `vortex-datafusion`: absent
- `spark`: absent
- `duckdb`: absent
- `polars`: absent
- `velox`: absent

## Transitive notes
- Arrow/serde/tracing/tokio-style crates appear transitively through upstream Vortex when enabled.
- ShardLoom does not add new direct dependencies on these in this PR.

## Selectivity recommendation
- Keep umbrella `vortex` as an optional feature only.
- Defer upstream dependency in default builds.
- Reserve `vortex-file` usage for future `vortex-file-io` feature work.

## Proposed feature layout
- `default = []`
- `upstream-vortex`
- `vortex-file-io`
- `vortex-object-store`
- `vortex-write`

## Do not do yet
- Do not enable object-store feature yet.
- Do not implement Vortex IO yet.
- Do not default to decode-to-Arrow.
- Do not add fallback execution engines.

## Vortex file IO feature gate

- `vortex-file-io` remains off by default.
- Default build does not perform `Vortex` file IO.
- Metadata-only local file open is gated behind `vortex-file-io`.
- Object-store IO remains disabled in this contract.
- Writes remain disabled in this contract.
- No fallback engines were introduced.
