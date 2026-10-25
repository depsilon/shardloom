# Vortex Dependency Footprint

## Purpose
This ledger records ShardLoom's upstream Vortex dependency footprint and preserves prior
version-specific reviews. The current candidate remains feature-gated and does not itself
broaden runtime support.

## Current state (October 24, 2026 candidate)

- The optional upstream Vortex umbrella is pinned to 0.90.0 in the workspace and inherited
  by `shardloom-vortex`; the dependency remains feature-gated.
- Eight 0.90.0 packages remain local path patches: `vortex-array`, `vortex-buffer`,
  `vortex-file`, `vortex-fsst`, `vortex-io`, `vortex-layout`, `vortex-runend` and
  `vortex-zstd`.
  `vortex-datetime-parts` now resolves from its reviewed 0.90.0 registry archive.
- The new `vortex-buffer` patch preserves explicit empty `Bytes` and Arrow owners through
  cloning, typed conversions and Arrow import/export; ordinary empty slices still detach.
  See [`vendor/vortex-buffer/README.shardloom.md`](../../vendor/vortex-buffer/README.shardloom.md).
- The workspace uses Arrow 59.3.1 and Parquet 59.3.1 with ORC 0.9.0. `arrow-pyarrow`
  59.3.1 resolves from the registry after its workspace path patch was removed; the unused
  `vendor/arrow-pyarrow` directory is retired after installed capsule and ownership proof.
- PyO3 runtime remains 0.29.3. The independent `pyo3-build-config` 0.30.1 build helper
  is called only by `shardloom-python/build.rs` to add extension-module link arguments.
- Final matched-build validation passes 6,072 native workspace tests across 118 binaries
  (35 existing opt-in ignored), 3,246 default workspace tests, native strict lint and
  formatting. Exact final all-feature, MSRV and no-default checks pass. The installed
  Python wheel passes all 140 native integration tests without skips. Public validation
  passes 5,930 cases across 39 families; semantic replay validates 5,639 actual/expected
  value files and 137 JSONL archives. The recovered modular matrix accepts 1,408 records
  across 18 cohorts. Fresh ingest produces 99,997,497 rows through the minimal digest
  path; no independent full 100-million-row readback is claimed. One isolated Full43
  completes each of 43 queries once with 381 complete rows and exact schema continuity
  at 49.625514041 engine seconds; the prior 48.701809680 observation supports no causal
  speedup or regression claim. See the [acceptance report](../benchmarks/native-vortex-provider-refresh-2026-10-24.md).
  Local runtime/UAT and portable replay acceptance are complete; PR, CI, review
  and merge remain pending. The verified notice manifest
  contains 437 registry packages, eight vendored providers, 827 file records and
  248 distinct texts.
- New versions do not automatically enable rich types, V2 scans, optional encodings,
  connectors, external engines or performance claims. The no-fallback boundary and
  existing feature gates remain in force.

See the [controlled provider-refresh plan](../architecture/native-vortex-provider-refresh-2026-10-24.md)
for migration scope and remaining acceptance work.

## Vortex 0.85 provider-capability update

Historical review; superseded for current dependency status by the October 24, 2026
candidate state above. The package/version and compatibility notes below remain as
historical evidence.

`VORTEX-UPSTREAM-CAPABILITY-INTAKE-1` incorporates the safe part of the Vortex dependency intake:
the optional upstream Vortex provider family moves from `0.75.0` to `0.85.0`, while the structured
Arrow/Parquet bridge remains Arrow 58-aligned to avoid duplicate provider types across Vortex and
ORC.

Compatibility posture:

- Root `Cargo.toml` declares workspace dependencies `vortex = "0.85"` and
  `vortex-zstd = "0.85"`; `shardloom-vortex/Cargo.toml` inherits them as optional dependencies.
- `Cargo.lock` resolves the upstream Vortex crate family to `0.85.0`.
- `cargo info vortex@0.85.0` reports license `Apache-2.0`, Rust version `1.95`, documentation
  <https://docs.rs/vortex/0.85.0>, repository <https://github.com/spiraldb/vortex>, and crates.io
  version <https://crates.io/crates/vortex/0.85.0>.
- `cargo info vortex-zstd@0.85.0` reports license `Apache-2.0`, Rust version `1.95`,
  documentation <https://docs.rs/vortex-zstd/0.85.0>, repository
  <https://github.com/spiraldb/vortex>, and crates.io version
  <https://crates.io/crates/vortex-zstd/0.85.0>.
- ShardLoom provider-version evidence continues to read from
  `shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION`; active provider-disposition field names
  now use `vortex_upstream_*` labels instead of hard-coding a release number into current report
  keys.
- Deprecated Rust type aliases preserve the previous `Vortex075*` public report/type names while
  new code uses the version-neutral `VortexUpstream*` names and the provider-version evidence field.
- Default builds still keep upstream Vortex optional and disabled by default.
- No `vortex-datafusion`, DuckDB, Spark, Polars, Velox, or other external query-engine fallback
  dependency is introduced.

0.85 migration changes retained in `shardloom-vortex`:

- Vortex scan projection/filter calls now bind optimized expressions as `BoundExpression` before
  calling `ScanBuilder::with_projection` / `with_filter`.
- Arrow import/export uses `vortex::arrow::ArrowSessionExt` session methods instead of deprecated
  non-session Arrow imports.
- Layout inventory accepts the current `DynLayout` trait surface.
- Local top-K row-reference materialization uses Vortex `StrictSortedBuffer<u64>` row-index
  selection.
- Run-end lowering imports current slot access traits and keeps unsupported nullable/nested cases
  explicit.
- Current Vortex `Map`/`Union` dtype/scalar variants are deterministically unsupported at the
  encoded-stat boundary unless a later runtime item adds support.
- Writer layout strategy derives allowed encodings from the active Vortex edition/session instead
  of depending on removed static file constants.

Claim boundary:

- This update proves optional dependency compatibility and refreshed provider-disposition evidence.
- It does not by itself admit new Vortex runtime APIs, JSON/geospatial execution, GPU/device
  execution, external engines, object-store/table support, broad SQL/DataFrame claims, performance
  claims, package claims, or production readiness.

## Vortex 0.75 dependency bump proof

Historical note; superseded by the Vortex 0.85 provider-capability update above for current
dependency status.

`PROD-V1-3A` folds in Dependabot PR
[#1223](https://github.com/depsilon/shardloom/pull/1223), updating the optional upstream Vortex
dependency family from `0.74.0` to `0.75.0`.

Compatibility posture:

- Root `Cargo.toml` declares workspace dependency `vortex = "0.75"`; `shardloom-vortex/Cargo.toml`
  inherits it as an optional dependency.
- `Cargo.lock` resolves the upstream Vortex crate family to `0.75.0`.
- `cargo info vortex@0.75.0` reports license `Apache-2.0`, Rust version `1.91.0`, documentation
  <https://docs.rs/vortex/0.75.0>, repository <https://github.com/spiraldb/vortex>, and crates.io
  version <https://crates.io/crates/vortex/0.75.0>.
- ShardLoom provider-version evidence now reads from
  `shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION`, keeping certificates, capability rows,
  scan/source admission rows, and benchmark route evidence on the same provider-version line.
- Default builds still keep upstream Vortex optional and disabled by default.
- No `vortex-datafusion`, DuckDB, Spark, Polars, Velox, or other external query-engine fallback
  dependency is introduced.

Runtime-relevant upstream 0.75 opportunities recorded for later provider-gated work:

- grouped aggregate kernels for `sum` and `count`
- explicit validity/mask execution context changes and `definitely_no_nulls` / `execute_no_nulls`
  surfaces
- layout reader context and layout reader cache work
- JSON extension Arrow import/export, WKB/geospatial extension import/export, and native Point /
  `GeoDistance` extension surfaces
- Interleave array encoding, `byte_length()` expression, binary zstd schemes, and row-oriented byte
  encoder surfaces
- branchless primitive/boolean zip kernels, mask `AllTrue`/`AllFalse` fast paths, dictionary
  validation/slice optimization, FSST symbol-table sharing, and child-layout cache performance work

Claim boundary:

- The bump proves optional dependency compatibility and source-grounded opportunity mapping only.
- It does not admit new Vortex runtime APIs, JSON/geospatial execution, GPU/device execution,
  external engines, object-store/table support, SQL/DataFrame support, performance claims, package
  claims, or production readiness.

## Vortex 0.74 dependency bump proof

Historical note; superseded by the Vortex 0.85 provider-capability update above for current
dependency status.

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

Historical note; superseded by the Vortex 0.85 provider-capability update above for current
dependency status.

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

- `cargo +<workspace-msrv-toolchain> check -p shardloom-vortex --features upstream-vortex`
- `cargo +<workspace-msrv-toolchain> check -p shardloom-vortex --features universal-format-io`
- `cargo +<workspace-msrv-toolchain> check -p shardloom-vortex --features vortex-file-io`
- `cargo +<workspace-msrv-toolchain> check -p shardloom-vortex --features vortex-write`
- `cargo +<workspace-msrv-toolchain> check -p shardloom-vortex --features vortex-local-primitives`
- `cargo +<workspace-msrv-toolchain> check -p shardloom-vortex --features vortex-traditional-analytics-benchmark`
- `cargo +<workspace-msrv-toolchain> test -p shardloom-vortex --features vortex-traditional-analytics-benchmark`
- `cargo +<workspace-msrv-toolchain> test -p shardloom-cli unstructured_and_adapter_capabilities_expose_report_only_matrix -- --nocapture`
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
