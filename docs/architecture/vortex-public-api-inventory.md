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

## Metadata-only open update

- Public APIs re-validated for this phase: `vortex::file::VortexOpenOptions`, `vortex::file::OpenOptionsSessionExt`, and `vortex::file::VortexFile::footer` as inventory targets from upstream public surfaces.
- Runtime usage posture in this phase:
  - non-Vortex targets: deterministic invalid-target report;
  - object-store URIs: deterministic unsupported/deferred report;
  - missing local `.vortex` path: deterministic file-missing report;
  - existing local file: metadata open remains `ApiDeferred` until metadata-only behavior guarantees are explicit.
- `footer`/`row_count`/`dtype` usage remains deferred for existing-file metadata open because this phase avoids guessing about data-read side effects.
- Tests exercised in this phase: missing local path, invalid target, object-store rejection, feature-disabled path, and feature-enabled deterministic reporting.
- No scan execution introduced.
- No fallback execution introduced.

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

## Adaptive sizing integration update

`ShardLoom` now exposes a bridge that maps `Vortex` read planning and runtime graph reports into adaptive sizing reports and plans.

This integration is planning-only and does not call upstream `Vortex` scan execution. It does not read data, materialize values, or execute tasks. No fallback execution behavior or external execution engine is introduced.

## Scheduler planning update

`ShardLoom` now bridges `Vortex` memory plans into scheduler queue plans.
This bridge does not call upstream `Vortex` scan execution.
This bridge does not read data.
This bridge does not execute tasks.
This bridge does not materialize values.
This bridge does not write spill files.
No fallback execution is introduced.

## Execution readiness update

`ShardLoom` now exposes a Vortex execution-readiness gate over the Vortex planning chain.
This update does not call upstream Vortex scan execution.
This update does not read data.
This update does not execute tasks.
This update does not materialize values.
This update does not write spill files.
No fallback execution was introduced.

## Metadata-only execution spike update

The current spike validates ShardLoom's end-to-end Vortex contract without invoking upstream scan execution.

The feature-gated file IO seam remains metadata/report oriented, and no fallback execution is introduced.

## Metadata-only executor update

- This executor does not call upstream Vortex scan execution.
- It consumes ShardLoom planning/readiness reports.
- It does not invoke upstream data APIs.
- No fallback execution introduced.

## Encoded-read readiness update

This contract does not call upstream Vortex scan execution. It consumes `ShardLoom` scheduler/readiness reports, does not invoke upstream data APIs, and introduces no fallback execution.

## Encoded-read executor skeleton update

- This skeleton does not invoke upstream data APIs.
- It classifies what would execute later.
- No fallback execution introduced.

## Encoded-read public API boundary update

This update adds a `ShardLoom` contract-only boundary for future `Vortex` encoded-read work.

Confirmed public upstream API surfaces from the current review include `VortexOpenOptions`, `OpenOptionsSessionExt`, and `VortexFile::footer` plus related metadata (`row_count`/`dtype`) surfaces.

Contract-usable now:
- `VortexFile::footer` (metadata-only inventory contract item).

Deferred contract surfaces:
- `VortexOpenOptions`, `OpenOptionsSessionExt`, and `row_count`/`dtype` metadata surfaces are classified as confirmed public but deferred while encoded-read execution remains blocked by default.

APIs that would start data reads:
- Scan/start-read APIs are explicitly classified under data-read and marked forbidden for now.

APIs that would decode/materialize:
- Decode/materialization related areas are classified as forbidden-for-now boundary areas.

APIs with Arrow-default behavior risk:
- Arrow interop/conversion APIs are classified with `ArrowDefaultPath` blocking risk.

Forbidden-for-now areas:
- Data read, decode, materialization, object-store IO, and write IO remain blocked.

No fallback execution is introduced by this boundary update.

- Encoded-read probe plan (`vortex-encoded-read-probe`) combines `VortexEncodedReadApiBoundaryReport` and `VortexEncodedReadReadinessReport`; it is plan-only and performs no scan execution, no data read/decode/materialization, no object-store/write/spill IO, and no fallback.

## Encoded-read spike update

- Inspected public upstream `vortex` API surface via compile-time integration and existing `vortex-file-io` path.
- Phase 8 spike is feature-gated behind `vortex-encoded-read-spike`.
- Current spike returns deterministic blocked/deferred execution when a safe no-decode/no-materialize encoded-read API path is not yet proven.
- No fixture was added in this phase.
- No decode/materialization/write/object-store/fallback behavior was introduced.


## Query primitive update

`CountAll` can now use `Vortex` metadata summaries when row counts are available.
Projection/filter primitives remain future encoded-read or predicate-kernel work.
No fallback execution was introduced.

## Predicate-count primitive update
- Query primitive layer supports metadata-filtered count planning/evaluation.
- It uses `ShardLoom` metadata summaries and conservative predicate proofs.
- Projection/filter remain future encoded-read or predicate-kernel work.
- No fallback execution introduced.


## Encoded predicate/projection primitive update
- Query primitive layer now models projection/filter work.
- These remain future encoded-read/predicate-kernel work unless metadata proves an answer.
- No fallback execution introduced.


## Query trace and work avoided update

- These reports are `ShardLoom`-native and do not call upstream `Vortex` scan execution.
- No fallback execution is introduced.


## Local execution loop update

- Introduces `ShardLoom`-native engine-loop plumbing for local `Vortex` query primitive handling.
- The loop does not call upstream `Vortex` scan execution APIs.
- No fallback execution is introduced by this update.


## Bounded local execution update

This is `ShardLoom`-native scheduling/runtime plumbing for `Vortex` local execution. It does not call upstream `Vortex` scan execution and introduces no fallback execution.


## Local engine surface update
- This is `ShardLoom`-native CLI/API plumbing.
- It does not call upstream `Vortex` scan execution.
- No fallback execution is introduced.

## Local engine metadata-open propagation update

The local engine surface preserves metadata-open report context in `VortexLocalEngineReport`, including metadata-open status and diagnostics.

This update does not introduce scans, decode, materialization, writes, object-store IO, spill IO, or fallback execution.


## Vortex write API boundary update

- Upstream `Vortex` write APIs remain deferred.
- Phase 12A.1 models write intent only.
- No actual `Vortex` write API calls.
- No object-store writes.
- No fallback execution.
