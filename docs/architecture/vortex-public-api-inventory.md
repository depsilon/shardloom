# Vortex Public API Inventory

## Purpose
This document records upstream `vortex` public APIs inspected for ShardLoom adapter work and defines narrow adapter boundaries that remain planning-only. It is explicitly not a Vortex IO implementation.

## Current dependency
- Crate: `vortex`
- Version: `0.70`
- License: Apache-2.0 (per dependency review)
- ShardLoom crate using it: `shardloom-vortex`
- Actual Vortex IO implemented: no
- Fallback execution introduced: no

## Public API discovery method
- Inspected dependency linkage and version via `shardloom-vortex/Cargo.toml` and `cargo tree -p shardloom-vortex`.
- Used compiler-safe reference checks in `shardloom-vortex` without invoking runtime IO APIs.
- Reviewed local readiness and dependency review docs.
- Documented only names/status; no copied upstream implementation code.

## Candidate API areas
### DType / logical type APIs
- Public API names discovered: not confirmed yet.
- Use now: yes, via temporary name-based mapping only.
- Stability for first adapter work: partially acceptable (name-based placeholder).
- Adapter support: planned.
- Risks: upstream typed API names may shift; avoid guessing.

### Array APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: deferred.
- Risks: coupling to internal array APIs.

### Encoding APIs
- Public API names discovered: not confirmed yet.
- Use now: yes, via temporary name-based mapping only.
- Stability: partially acceptable for placeholders.
- Adapter support: planned.
- Risks: encoding taxonomy drift.

### Layout APIs
- Public API names discovered: not confirmed yet.
- Use now: yes, via temporary name-based mapping only.
- Stability: partially acceptable for placeholders.
- Adapter support: planned.
- Risks: layout labels may change.

### Statistics APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: deferred/planned.
- Risks: stats exactness/typing details unclear.

### File/open APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: deferred.
- Risks: would imply IO implementation scope.

### Scan/source APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: deferred.
- Risks: accidental execution coupling.

### Write/sink APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: deferred.
- Risks: write semantics and metadata fidelity unknown.

### Arrow interoperability APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: unsupported for default path.
- Risks: decode-to-Arrow drift as implicit fallback.

## Adapter mapping plan
- Vortex DType -> `shardloom_core::LogicalDType`
- Vortex encoding/layout -> `shardloom_core::EncodingKind` / `shardloom_core::LayoutKind`
- Vortex statistics -> `shardloom_core::SegmentStats`
- Vortex file metadata -> `shardloom_vortex::VortexFileMetadata`
- Vortex segment metadata -> `shardloom_vortex::VortexSegmentDescriptor`
- Vortex output capability -> ShardLoom native Vortex output planning (`VortexWritePlan`)

## Do not do
- Do not implement real IO yet.
- Do not default to decode-to-Arrow.
- Do not add DataFusion/Spark/DuckDB/Polars/Velox helpers.
- Do not copy upstream implementation code.
- Do not over-couple to private/internal APIs.

## Next milestone
Implement typed DType adapter mapping only if upstream public DType APIs are clearly confirmed and compile-safe; otherwise keep ShardLoom-local placeholder mapping until API stability is better understood.


## Typed DType adapter probe

- Typed DType mapping implemented: no (deferred).
- Public upstream APIs used for typed mapping in this PR: none confirmed compile-safe in this environment.
- Added compile-safe non-IO typed mapping status/report only: yes.
- Name-based mapping remains available as a temporary planning utility: yes.
- Fallback execution introduced: no.
- Actual IO implemented: no.
- Risks: upstream public typed DType API names and constructors must be re-validated once registry/docs access is available.

## Encoding APIs and Layout APIs update (adapter probe)

- Confirmed compile-time dependency linkage to upstream `vortex` crate remains intact in `shardloom-vortex`.
- Public typed encoding/layout adapter APIs were **not** implemented in this probe because this environment could not verify stable, non-IO constructible upstream public encoding/layout types safely.
- Typed encoding mapping status: deferred (`deferred_api_unclear`).
- Typed layout mapping status: deferred (`deferred_api_unclear`).
- Name-based mapping helpers remain available for planning-time adapter labeling (`map_known_vortex_encoding_name`, `map_known_vortex_layout_name`).
- Risk: upstream public encoding/layout surfaces may change; typed mapping should only land when compile-safe constructors and stable public APIs are confirmed.
- No Vortex IO implemented in this PR.

## Typed encoding and layout adapter probe

- Typed encoding mapping implemented: no (deferred API discovery).
- Typed layout mapping implemented: no (deferred API discovery).
- Upstream typed APIs used: none in code paths.
- Only compile-safe non-IO usage added: yes.
- Name-based mapping remains as placeholder utility: yes.
- Fallback execution introduced: no.
- Actual IO implemented: no.

## Statistics APIs update (adapter probe)

- Public statistics type/API names discovered: no obvious public typed statistics structs/enums were found in the locally resolved `vortex` crate surface for this environment.
- Use in this PR: typed statistics mapping is deferred; compile-safe report/status + `ShardLoom` `SegmentStats` placeholders were added.
- Stability for first typed adapter work: unclear.
- Adapter support state: deferred (`deferred_api_unclear`).
- Risks: probing private/internal APIs would be brittle and violate the public-API-only boundary.
- No Vortex IO implemented in this PR.

## Typed statistics adapter probe

- Typed statistics mapping implemented: no (deferred).
- Upstream public APIs used for typed statistics mapping: none.
- Added compile-safe non-IO usage only: yes.
- `ShardLoom` local placeholder mapping utilities remain available: yes.
- Fallback execution introduced: no.
- Actual IO implemented: no.

## File/open APIs

- Public API names discovered: `vortex::file::VortexOpenOptions`, `vortex::file::OpenOptionsSessionExt`, and `vortex::file::VortexFile::footer` via upstream crate public re-exports.
- Used in this PR: no runtime invocation; discovery-only.
- Metadata-only open appears supported: potentially, but runtime semantics are not yet proven compile-safe for strict no-materialization guarantees in this phase.
- Actual scan/materialization avoided: yes.
- Stability for first metadata probe: deferred pending clearer adapter-safe API contract.
- Risks: async/session requirements and footer open path may read more than minimal metadata depending on upstream behavior.

## Metadata inspection APIs

- Public API names discovered: `Footer` accessors exposed through `vortex::file` module surface, plus dtype/layout/statistics references in crate docs.
- Schema/`DType` metadata access: appears possible through footer/layout APIs, but not adopted in this PR.
- Row count / length access: appears available via footer row count APIs, not adopted in this PR.
- Statistics access: appears available in footer/file statistics APIs, not adopted in this PR.
- Encoding/layout metadata access: appears available through footer/layout graph, not adopted in this PR.
- Adapter support state: deferred (`deferred_api_unclear`) for runtime metadata IO; report-only status implemented.

## Metadata-only Vortex IO probe

- Metadata-only `Vortex` IO implemented: deferred runtime IO; report-only probe implemented.
- Upstream public APIs used by runtime probe: none (deferred path).
- Only local file metadata inspection added: no runtime file inspection yet.
- Data buffers read/materialized: no.
- Object-store IO implemented: no.
- Fallback execution introduced: no.
- Actual scan execution implemented: no.
- Actual write implemented: no.

## Metadata normalization update

- Vortex metadata probe reports are now normalized into ShardLoom-facing metadata summaries.
- The summary layer does not scan, decode, materialize, or write data.
- The summary layer preserves metadata availability flags.
- Future scan/explain/estimate integration should consume this summary layer.
- No fallback execution introduced.

## Metadata planning integration update

- Vortex metadata summaries now feed `ShardLoom` scan/explain/estimate planning skeletons.
- This integration is planning-only: it does not scan, decode, materialize, or write data.
- This bridge is the near-term seam for future pruning and metadata-only explain/estimate evolution.
- No fallback execution behavior or external execution engine was introduced.

## Metadata-driven pruning update

- `Vortex` metadata summaries now feed conservative metadata-driven pruning plans.
- Pruning uses `SegmentStats` and metadata only.
- Missing statistics do not prove absence.
- No scan, decode, materialization, object-store IO, or writes are performed.
- This is the future seam for metadata-only execution and segment pruning.
- No fallback execution is introduced.

## Feature-gated upstream posture update

- Upstream Vortex dependency is now optional behind `upstream-vortex`.
- Default builds remain planning/report-only and do not require compiling upstream Vortex.
- Vortex file IO remains unimplemented and staged behind future `vortex-file-io` work.
- Object-store support remains disabled by default.
- No fallback engines were introduced.

## Metadata-only file open contract update

- Feature gate used: `vortex-file-io`.
- Public metadata-only file API integration is currently deferred when metadata-only guarantees are unclear.
- `VortexMetadataSummaryReport` integration exists as an optional field on the open report.
- No scan execution is performed.
- No data materialization is performed.
- No object-store IO is performed.
- No writes are performed.
- No fallback execution is allowed.

## Read planning update

- ShardLoom now has a metadata-driven read planning skeleton.
- It does not call upstream Vortex scan execution.
- It prepares the future scan/split boundary.
- It preserves no-decode/no-materialization/no-object-store/no-write behavior.
- No fallback execution introduced.

## Runtime task graph integration update

`ShardLoom` now bridges `Vortex` read planning into runtime task graph skeletons.
This bridge does not call upstream `Vortex` scan execution, does not read data, does not materialize values, and does not execute tasks.
No fallback execution is introduced.
