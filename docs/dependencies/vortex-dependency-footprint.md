# Vortex Dependency Footprint

## Purpose
ShardLoom is auditing the upstream Vortex dependency graph before deeper integration so default
builds stay lightweight while preserving a controlled path to upstream Vortex capability work.

## Current state
- Current direct dependency in `shardloom-vortex`: optional umbrella `vortex = 0.74`.
- Latest upstream intake note: `vortex = 0.74.0` was reviewed in
  `docs/architecture/vortex-public-api-inventory.md`; the prior `0.73`, `0.72`, and detailed
  `0.71` release-note intake sections remain historical background.
- Umbrella `vortex` crate is still used for upstream opt-in builds.
- Default build (`default = []`) does not enable upstream Vortex.
- Existing feature-gated Vortex file/local primitive/write paths remain explicitly scoped and
  claim-gated; the version bump does not broaden runtime support.
- Fallback execution engines are not present.

## Vortex 0.74 dependency bump proof

`GAR-DEPENDENCY-INTAKE-1` incorporates Dependabot PR
[#1150](https://github.com/depsilon/shardloom/pull/1150), updating the optional upstream Vortex
dependency family from `0.73.0` to `0.74.0`.

Compatibility posture:

- `shardloom-vortex/Cargo.toml` requires optional `vortex = "0.74"`.
- `Cargo.lock` resolves the upstream Vortex crate family to `0.74.0`.
- `cargo info vortex@0.74.0` reports license `Apache-2.0` and Rust version `1.91.0`.
- Default builds still keep upstream Vortex optional and disabled by default.
- ShardLoom provider-version evidence has been refreshed from `0.73` to `0.74` for the existing
  approved/scoped Vortex evidence surfaces only.
- The upstream FFI `vx_file` scan removal in 0.74.0 is outside ShardLoom's admitted runtime
  boundary.
- No `vortex-datafusion`, DuckDB, Spark, Polars, Velox, or other external query-engine fallback
  dependency is introduced.

Claim boundary:

- The bump proves optional dependency compatibility only.
- It does not admit new Vortex runtime APIs, TurboQuant execution, vector search, GPU execution,
  external engines, object-store/table support, SQL/DataFrame support, performance claims, package
  claims, or production readiness.

## Vortex 0.73 dependency bump proof

Historical note; superseded by the Vortex 0.74 dependency bump proof above for current dependency
status.

`GAR-DEPENDENCY-INTAKE-1` incorporates Dependabot PR
[#979](https://github.com/depsilon/shardloom/pull/979), updating the optional upstream Vortex
dependency family from `0.72.0` to `0.73.0`.

Compatibility posture:

- `shardloom-vortex/Cargo.toml` requires optional `vortex = "0.73"`.
- `Cargo.lock` resolves the upstream Vortex crate family to `0.73.0`.
- `cargo info vortex@0.73.0` reports license `Apache-2.0` and Rust version `1.91.0`.
- Default builds still keep upstream Vortex optional and disabled by default.
- ShardLoom provider-version evidence has been refreshed from `0.72` to `0.73` for the existing
  approved/scoped Vortex evidence surfaces only.
- No `vortex-datafusion`, DuckDB, Spark, Polars, Velox, or other external query-engine fallback
  dependency is introduced.

Claim boundary:

- The bump proves optional dependency compatibility only.
- It does not admit new Vortex runtime APIs, TurboQuant execution, vector search, GPU execution,
  external engines, object-store/table support, SQL/DataFrame support, performance claims, package
  claims, or production readiness.

Validation recorded for the bump:

- `cargo fmt --all -- --check`
- `cargo check -p shardloom-cli --all-targets`
- `cargo check -p shardloom-cli --features vortex-write --all-targets`
- `cargo check -p shardloom-vortex --features vortex-traditional-analytics-benchmark --all-targets`
- `cargo clippy -p shardloom-vortex -p shardloom-cli --all-targets -- -D warnings`
- `cargo clippy -p shardloom-vortex -p shardloom-cli --features vortex-write --all-targets -- -D warnings`
- `cargo test -p shardloom-vortex --features vortex-write --lib vortex_compatibility`
- `cargo test -p shardloom-vortex --features vortex-write --lib vortex_compute_provider`
- `cargo test -p shardloom-vortex --features vortex-write --lib source_backed_encoded_execution::tests::reader_split_constructor_records_allowed_local_scan_effects`
- `cargo test -p shardloom-vortex --features vortex-write --lib source_backed_benchmark_matrix::tests::measured_source_backed_rows_preserve_provider_and_certificate_refs`
- `cargo test -p shardloom-vortex --features vortex-write capillary --lib`
- `cargo test -p shardloom-cli --features vortex-write --test sql_local_source_runtime_smoke vortex_ingest_smoke_writes_reopens_vortex_prepared_state`
- `cargo test -p shardloom-cli --features vortex-write --test sql_local_source_runtime_smoke vortex_ingest_smoke_minimal_certification_skips_reopen_scan`

## Vortex 0.72 dependency bump proof

Historical note; superseded by the Vortex 0.73 dependency bump proof above for current dependency
status.

The combined dependency/runtime-compatibility update incorporates the open Dependabot Vortex bump
from `0.71.0` to `0.72.0`, alongside the Parquet `58.3.0` and GitHub Actions major-version updates.

Compatibility posture:

- `shardloom-vortex/Cargo.toml` requires optional `vortex = "0.72"`.
- `Cargo.lock` resolves the upstream Vortex crate family to `0.72.0`.
- Default builds still keep upstream Vortex optional and disabled by default.
- TurboQuant was reviewed as an upstream vector-extension opportunity, but ShardLoom does not add a
  `vortex-turboquant` dependency or admit vector quantization runtime support in this update.
- Capability discovery now exposes `vortex_turboquant_vector_encoding` as blocked metadata so
  users and agents do not confuse upstream availability with ShardLoom runtime support.

Validation recorded for the bump:

- `cargo +1.91.1 check -p shardloom-vortex --features upstream-vortex`
- `cargo +1.91.1 check -p shardloom-vortex --features universal-format-io`
- `cargo +1.91.1 check -p shardloom-vortex --features vortex-file-io`
- `cargo +1.91.1 check -p shardloom-vortex --features vortex-write`
- `cargo +1.91.1 check -p shardloom-vortex --features vortex-local-primitives`
- `cargo +1.91.1 check -p shardloom-vortex --features vortex-traditional-analytics-benchmark`
- `cargo +1.91.1 test -p shardloom-vortex --features vortex-traditional-analytics-benchmark`
- `cargo +1.91.1 test -p shardloom-cli unstructured_and_adapter_capabilities_expose_report_only_matrix -- --nocapture`
- `python scripts\check_dependency_audit.py --release-gate --json-output target\dependency-audit-report.json`

Claim boundary:

- The bump proves optional dependency compatibility only.
- It does not admit new Vortex runtime APIs, TurboQuant execution, vector search, GPU execution,
  external engines, object-store/table support, SQL/DataFrame support, performance claims, package
  claims, or production readiness.

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
