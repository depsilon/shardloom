# Vortex Upstream Dependency Review

## Purpose

This document is a historical dependency-review ledger for ShardLoom's upstream
Vortex dependency. It does not define the current executable Vortex surface.

Current runtime support is summarized in
`docs/architecture/phased-execution-plan.md` and
`docs/architecture/vortex-public-api-inventory.md`. Do not infer current
executable support from older PR-specific sections that say "this PR" or
"actual IO implemented."

## Current support snapshot

- Upstream Vortex remains optional and isolated in `shardloom-vortex`.
- The tracked direct dependency is `vortex = 0.71`.
- Vortex `0.71.0` was released on 2026-05-18, inventoried in
  `docs/dependencies/vortex-0.71-upstream-intake.md`, classified in
  `docs/architecture/vortex-public-api-inventory.md`, and admitted as an optional dependency bump
  only.
- Approved historical metadata/footer fixture IO and approved local primitive
  scan paths are recorded in `docs/architecture/phased-execution-plan.md`.
- Vortex-native array, compute, scan, source, and sink APIs may be future native
  providers only when feature-gated, version-recorded, policy-admitted, and
  certificate-backed.
- Future upstream release intake is governed by
  `docs/dependencies/vortex-upstream-release-intake-runbook.md`.
- Vortex DataFusion, DuckDB, Spark, Trino, and similar integrations remain
  baseline/reference/oracle surfaces only; they must not execute unsupported
  ShardLoom residual work.
- Fallback execution remains disabled.

## Vortex 0.71 bump update

- `shardloom-vortex` now requests optional `vortex = 0.71`.
- `Cargo.lock` records Vortex `0.71.0` crate family versions.
- The bump required one ShardLoom source compatibility update: local primitive dtype mapping now
  rejects upstream `DType::Union` deterministically instead of relying on a non-exhaustive match.
- Feature-gated compile proof covered `upstream-vortex`, `vortex-file-io`, `vortex-write`,
  `vortex-local-primitives`, and `vortex-traditional-analytics-benchmark`.
- No new Vortex runtime support claim, fallback engine, object-store/table claim, SQL/DataFrame
  claim, performance claim, or package publication claim is introduced by the bump.

## Current status

- Upstream Vortex dependency has been added to `shardloom-vortex`.
- Historical scope in the original dependency PR was compile/readiness only.
- Current executable Vortex support is tracked in the phase plan rather than in
  this historical review section.
- Fallback execution remains disabled.

## Historical dependency review

- Crate name: `vortex`
- Version requested: `0.70`
- Repository: upstream Vortex repository
- License: Apache-2.0
- Purpose: native Vortex format/toolkit integration inside `shardloom-vortex`
- Original PR scope: dependency compile/readiness only
- Public APIs used in the original PR: none (compile marker only)
- Internal APIs used: none
- Actual IO implemented by the original PR: no
- Fallback engines introduced: no
- Copied upstream code: no
- Vendored upstream code: no

## License/provenance checklist

- Upstream license identified as Apache-2.0.
- License is compatible with Apache-2.0 project policy.
- No GPL/AGPL/SSPL/BUSL/proprietary code introduced.
- No copied upstream implementation code.
- No vendored upstream code.
- Dependency usage is isolated to `shardloom-vortex`.
- No fallback execution dependency was directly added.

## Historical dependency addition status

- Upstream Vortex dependency has been added to `shardloom-vortex`.
- The original dependency PR did not implement actual Vortex IO.
- The original dependency PR did not add fallback execution.
- The original dependency PR did not add DataFusion/Spark/DuckDB/Polars/Velox.

## Follow-up required

- Identify minimal metadata inspection API.
- Identify DType mapping API.
- Identify encoding/layout mapping API.
- Add adapter tests.
- Add unsupported diagnostics.
- Avoid decode-to-Arrow default path.


## API discovery update

- Added `docs/architecture/vortex-public-api-inventory.md` to track inspected upstream public API
  areas and adapter boundaries.
- Current adapter work remains mapping/readiness only.
- No actual Vortex IO is implemented in this phase.
- No fallback execution was introduced.


## Typed DType mapping update

- Typed DType mapping is deferred in this PR because a compile-safe public typed API could not be
  confirmed in this environment.
- Public upstream typed API used: none.
- Name-based DType mapping remains available as a temporary adapter utility.
- No actual Vortex IO was implemented.
- No fallback execution was introduced.

## Typed encoding/layout mapping update

- Typed encoding mapping: deferred pending confirmed stable public upstream non-IO API surface.
- Typed layout mapping: deferred pending confirmed stable public upstream non-IO API surface.
- Public upstream API used for typed mapping: none in this PR (deferred).
- Name-based mapping remains available for planning/reporting utility.
- No actual Vortex IO implemented.
- No fallback execution introduced.

## Typed statistics mapping update

- Typed statistics mapping: deferred in this PR (`deferred_api_unclear`) because a compile-safe,
  unambiguous public upstream API surface for typed statistics mapping was not confirmed in this
  environment.
- Public upstream statistics API used: none.
- `ShardLoom` `SegmentStats` placeholder mapping remains available for planning/reporting utility.
- No actual Vortex IO implemented.
- No fallback execution introduced.

## Metadata-only IO update

- Metadata-only local `Vortex` inspection: deferred to report-only mode in this PR.
- Public upstream API used for implemented runtime metadata inspection: none (deferred).
- Scan execution implemented: no.
- Data materialization implemented: no.
- Write implemented: no.
- Object-store IO implemented: no.
- Fallback execution introduced: no.

## Dependency footprint gating update

- Upstream `vortex` dependency is now feature-gated in `shardloom-vortex` (`upstream-vortex`).
- Default workspace builds do not enable the broad upstream Vortex graph by default.
- `vortex-file-io`, `vortex-object-store`, and `vortex-write` are declared as staged feature gates
  only.
- File IO and object-store IO remain disabled by default and unimplemented.
- No DataFusion/Spark/DuckDB/Polars/Velox/vortex-datafusion direct dependencies were introduced.
- No fallback execution behavior was introduced.

## Universal-format local benchmark bridge dependencies

- `shardloom-vortex` gates local structured-file bridge support behind
  `universal-format-io`, which is pulled by the existing
  `vortex-traditional-analytics-benchmark` feature and by the scoped
  `shardloom-cli --features universal-format-io` local Parquet source smoke.
- Rust crates added under that gate:
  - `parquet 58.2.0` for local Parquet record-batch reads/writes.
  - `arrow-ipc 58.3.0` for Arrow IPC reads/writes.
  - `arrow-avro 58.3.0` for Avro reads/writes.
  - `orc-rust 0.8.0` for ORC reads/writes.
  - `arrow-array 58.3.0` and `arrow-schema 58.3.0` for Arrow boundary arrays and schemas.
  - `arrow-json 58.2.0` is reserved under the same gate for JSON/NDJSON boundary work; the current
    deterministic JSONL fixture parser remains local and narrow.
- License/provenance:
  - Apache Arrow Rust crates are Apache-2.0.
  - `orc-rust` is Apache-2.0.
  - Benchmark fixture generation uses Python `fastavro 1.12.2` in
    `benchmarks/traditional_analytics/requirements.txt`; `pip show fastavro` reports MIT license.
- Scope:
  - Default workspace builds remain lightweight and do not enable these dependencies.
  - These dependencies do not introduce Spark, DataFusion, DuckDB, Polars, Velox, Trino, Dask, Ray,
    Calcite, or `vortex-datafusion`.
  - They are file-format boundary readers/writers for local benchmark smoke, troubleshooting, and
    the feature-gated flat scalar local Parquet SQL smoke, not execution engines or fallback paths.
  - Compatibility-format input is imported into native local Vortex output before the temporary
    benchmark operator runs.
  - Production adapter certification, object-store IO, catalog/table metadata IO, distributed
    execution, SQL/DataFrame/UDF runtime, and performance/superiority claims remain separate future
    work.
