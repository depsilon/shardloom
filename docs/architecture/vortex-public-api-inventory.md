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
- Adapter support: deferred (real payload write path remains future, feature-gated, and explicitly approved).
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


## Staged output boundary update

- No upstream `Vortex` write APIs are called.
- Staged output is a `ShardLoom`-native planning contract.
- Feature-gated staged markers remain `ShardLoom`-native local workspace behavior.
- Marker writes are tiny/deterministic and are not manifests or output payload writes.
- Upstream `Vortex` write API integration remains deferred.
- Actual write execution remains deferred.


## `ShardLoom` staged workspace setup CLI

- `ShardLoom` exposes staged workspace setup via the `vortex-staged-workspace-setup` command.
- This command is `ShardLoom`-native and wraps existing staged workspace setup reporting/helpers.
- It does not use upstream `Vortex` write APIs.
- It does not write output payload files or manifests.


## `ShardLoom` staged marker write CLI

- `ShardLoom` exposes staged marker writing via `vortex-staged-marker-write`.
- The staged marker CLI is `ShardLoom`-native.
- It does not use upstream `Vortex` write APIs.
- Actual `Vortex` write execution remains deferred.
## Phase 12A.3a update
- Phase 12A.2c.2 complete.
- Phase 12A.3a current: staged manifest draft core contract (report-only, no filesystem).
- Phase 12A.3b planned: feature-gated local staged manifest draft file.
- Phase 12A.3c planned: CLI/docs integration.
- Actual output payload and file writes remain deferred.

- Staged manifest draft is `ShardLoom`-native.
- Upstream `Vortex` write APIs remain deferred.
- Actual `Vortex` writes remain deferred.

- Manifest draft file planning is `ShardLoom`-native.
- Upstream `Vortex` write APIs remain deferred.


## Staged manifest draft write posture

- Staged manifest draft file writing is `ShardLoom`-native and does not depend on upstream `Vortex` write APIs.
- Upstream `Vortex` write API integration remains deferred.


- Staged manifest CLI is `ShardLoom`-native, and upstream `Vortex` write APIs remain deferred.

## Phase 12A write API boundary closeout

- Upstream Vortex write APIs remain unused.
- ShardLoom-native staged artifacts exist.
- Vortex file output and committed manifest behavior remain deferred.


## Phase 12B commit intent note

- Commit intent remains `ShardLoom`-native in Phase 12B.1 report-only surfaces.
- Commit readiness integration remains `ShardLoom`-native and report-derived.
- Upstream `Vortex` write APIs remain deferred.
- Upstream `Vortex` write APIs remain unused.
- Commit protocol execution is not implemented in this phase.


## Commit protocol planning boundary update

- Commit protocol state machine is `ShardLoom`-native and report-only.
- Upstream `Vortex` write APIs remain deferred for commit execution phases.

## Phase 12B commit planning CLI wrappers

- `vortex-commit-marker-plan`, `vortex-commit-intent-plan`, and `vortex-commit-protocol-plan` are `ShardLoom`-native report-only `CLI` wrappers over commit planning reports.
- `vortex-commit-marker-write` is a `ShardLoom`-native `CLI` wrapper over the feature-gated local commit marker write helper.
- They provide stable text/JSON output fields for readiness validation.
- They do not execute commits, finalize manifests, write commit markers, write output payloads, or perform object-store IO.
- Upstream `Vortex` write API calls remain deferred.


## Local staged write-readiness smoke test posture

- The staged write-readiness smoke test remains ShardLoom-native and does not use upstream `Vortex` write APIs.


## Commit marker planning note

- Commit marker planning is `ShardLoom`-native.
- Commit marker writing is `ShardLoom`-native and limited to the exact feature-gated local marker artifact.
- Upstream `Vortex` write APIs remain deferred.


## APIs needed for expanded competitive gates

- CG-1 encoded read boundary
- CG-2 query primitive execution
- CG-3 real Vortex output payload
- CG-12 plan export/import boundary
- CG-13 encoding-aware execution APIs
- CG-14 runtime/adaptive metrics
- CG-15 CPU dispatch/kernel capabilities
- CG-16 execution certificate metadata
- CG-17 cache/reuse invalidation metadata
- CG-18 universal runner/comparison output

- Upstream Vortex write/read APIs must remain feature-gated.
- No upstream API should be called if it implies row materialization, Arrow conversion, object-store IO, or write execution without explicit feature gate.
- The encoded-read boundary `CLI` can model open/options/footer/metadata boundary surfaces through `VortexEncodedReadBoundaryRequest`.
- Scan/data traversal APIs remain deferred until CG-1.2+ and must stay feature-gated.

## Competitive Engine Track API alignment

- CG items are competitive success gates / roadmap tracks, not implementation phase IDs.
- External engines are baseline references only, never fallback execution.
- API evolution must preserve Vortex-native, no-fallback, explicit-boundary behavior.

- The staged write-readiness smoke test remains `ShardLoom`-native and does not use upstream `Vortex` write APIs.

## Phase 12B.4 closeout note

- Upstream Vortex write APIs remain unused after commit-marker staging.
- Manifest finalization remains ShardLoom-native and report-only for the next phase.


- Manifest finalization remains `ShardLoom`-native/report-only in Phase 12B.5a; upstream `Vortex` write APIs remain deferred.

## Finalized-manifest candidate artifact posture

- Finalized-manifest candidate writing is `ShardLoom`-native.
- Upstream `Vortex` write APIs remain deferred.
- Candidate artifact writes do not imply committed manifest state.


- Local commit execution gate is `ShardLoom`-native and report-only in Phase 12B.6.
- Upstream `Vortex` write APIs remain deferred from this gate.
- Output payload plan CLI is `ShardLoom`-native in Phase 12C.3a (complete) and remains report-only. Output payload artifact write CLI is `ShardLoom`-native in Phase 12C.3b (complete). Phase 12C.4 keeps the staged smoke test `ShardLoom`-native while real upstream `Vortex` write APIs remain deferred.


- Output payload contract is `ShardLoom`-native and report-only.
- Upstream `Vortex` write APIs remain deferred until explicit approval.
- Object-store write APIs remain deferred.

- Output payload artifact writing is `ShardLoom`-native and does not use upstream `Vortex` write APIs.
- Upstream `Vortex` write APIs for real payload writes remain deferred.


## CG-3 clarification

- Local placeholder artifact write paths are not real Vortex payload write paths.
- Upstream Vortex write APIs remain deferred in current phases.
- A future real payload write path must be feature-gated and explicitly approved before CG-3 can be treated as complete.


- CG-1.2b adds metadata/footer probe contracts only; default report construction does not inspect local file existence and scan/data traversal remains deferred.

- CG-1.2c exposes the metadata probe contract through CLI only; default path does not inspect local files and does not perform metadata/footer IO.


## CG-1.2d blocker clarification

- Re-validated public symbols: `VortexOpenOptions`, `OpenOptionsSessionExt`, `VortexFile::footer`.
- Current blocker: these metadata/footer paths require async/session invocation semantics.
- This phase intentionally avoids introducing a runtime boundary (`tokio`/executor wiring) for probe-only metadata/footer calls.
- Result: `MetadataProbeCompleted` remains unreachable in this phase; deterministic `BlockedByUnsupportedApiSurface` is preserved for existing local files under `vortex-file-io`.


## CG-1.2d.2 deterministic async/session boundary contract

- Adds report-only async/session boundary planning for local metadata/footer probes.
- Keeps actual `VortexOpenOptions`/`OpenOptionsSessionExt`/`VortexFile::footer` invocation deferred to CG-1.2d.3.
- No runtime/executor wiring is added.
- No scan/read-start, encoded reads, row reads, decode/materialization, `Arrow` conversion, object-store IO, writes, or fallback execution are added.


### CG-1.2d.3 update
- Added feature-gated async metadata/footer invocation surface for caller-provided async context only.
- No runtime/executor dependency was added by `ShardLoom`.
- Sync `VortexEncodedReadMetadataProbeReport::from_request` path remains report-only/no-IO.
- Async surface preserves no scan/read-start, no encoded-data reads, no decode/materialization, no `Arrow` conversion, no object-store IO, no writes, and no fallback execution.
- Actual public upstream `Vortex` metadata/footer invocation remains blocked by compile-unclear API shape; deterministic `blocked_by_unsupported_api_surface` diagnostics now record: `vortex::session::Session` not found, `VortexOpenOptions::new()` unavailable, and `OpenOptionsSessionExt` not usable in a compile-passing invocation path yet.


## CG-1.2d.4 update (API compile probe)
- Added feature-gated compile probe that confirms public `Vortex` symbols compile in `shardloom-vortex`: `vortex::file::VortexOpenOptions`, `vortex::file::OpenOptionsSessionExt`, `vortex::file::VortexFile`, and `vortex::session::VortexSession`.
- Production async invocation remains deterministically blocked in this phase; no metadata/footer IO is executed.

## CG-1.2d.5 update (method-shape compile probe)
- Added feature-gated method-item probes that compile-check the following public method shapes without invocation: `<VortexSession as OpenOptionsSessionExt>::open_options(&self) -> VortexOpenOptions`, `VortexOpenOptions::with_initial_read_size(self, usize) -> VortexOpenOptions`, `VortexOpenOptions::with_some_file_size(self, Option<u64>) -> VortexOpenOptions`, and `VortexFile::footer(&self) -> &Footer`.
- Remaining blocker: production path still lacks an approved compile-safe no-IO constructor policy for deterministic async invocation wiring in `ShardLoom`; invocation remains blocked by unsupported API surface.
- No runtime/executor dependency was added and no file open, metadata/footer IO, scan/read-start, decode/materialization, `Arrow` conversion, object-store IO, writes, or fallback execution was introduced.

## CG-1.2d.6 update (caller-provided session contract + open method probe)
- CG-1.2d.5 confirmed symbols/methods remain valid: `VortexOpenOptions`, `OpenOptionsSessionExt`, `VortexFile`, `VortexSession`, `open_options`, `with_initial_read_size`, `with_some_file_size`, and `footer`.
- Added caller-provided `VortexSession` invocation contract (`VortexMetadataAsyncInvocationInput<'a> { boundary, session }`) behind `vortex-file-io`; construction is contract-only and performs no IO.
- Added compile probe reference for `VortexOpenOptions::open_path` method item to identify local-path open surface without invocation.
- Production metadata/footer invocation remains blocked pending approved async runtime/IO harness and explicit invocation policy.
- No runtime/executor dependency, file open, metadata/footer IO, scan/read-start, decode/materialization, `Arrow` conversion, object-store IO, writes, or fallback execution was added.

## Test-only async metadata/footer harness policy

- Test-only async execution is allowed only in feature-gated tests.
- It must not affect production/default runtime behavior.
- It must not add fallback execution.
- It must not call scan/read-start/decode/materialization/`Arrow`/object-store/write APIs.
- A dev-dependency executor is allowed only when already present in `Cargo.lock` through the `Vortex` feature graph and when adding it introduces no new lockfile packages.
- A checked-in local `.vortex` fixture is allowed only with explicit provenance and only for metadata/footer open tests.
- Fixture generation using `Vortex` write APIs is not allowed in this phase.


## CG-2.0 query primitive boundary update
- CG-1.2 metadata/footer execution remains paused/blocked after CG-1.2d.8 due to missing repository-local `.vortex` fixture and no confirmed public no-IO `Footer` route.
- CG-2.0 is current and adds a report-only, feature-gated `Vortex` query primitive readiness boundary for count, filtered count, projection, and predicate/filter primitives.
- This boundary does not execute query primitives and remains side-effect-free.
- CG-2.1 actual count execution remains blocked until both metadata/footer readiness and an approved encoded data path exist.
- No scan/read-start, encoded data reads, row reads, decode/materialization, `Arrow` conversion, object-store `IO`, writes, or fallback execution are introduced.
- CG-1 through CG-18 remain visible and active competitive gates.

## CG-2.0b query primitive helper correctness update
- Metadata async invocation to query primitive request propagation now preserves boundary feature-gate/object-store/risk signals and no longer drops blockers from `boundary_report`.
- Query primitive error detection now includes report diagnostic severity (`Error`/`Fatal`) in addition to status/request checks.
- No upstream Vortex scan/read-start or write APIs are called by this update.
- No encoded data reads, row reads, decode/materialization, Arrow conversion, object-store IO, writes, or fallback execution are introduced.
- Optional CLI command for direct query primitive planning is deferred to CG-2.0c.

## CG-2.0c query primitive plan CLI integration
- Adds `shardloom vortex-query-primitive-plan <primitive> <dataset_uri> [flags] [--format text|json]` as a report-only/readiness-only planning command.
- Command constructs `VortexQueryPrimitiveRequest` and calls `plan_vortex_query_primitive` only; it does not execute query primitives.
- Command does not call scan/read-start APIs, does not read encoded data or rows, does not decode/materialize/Arrow-convert, does not perform object-store IO, does not write output payloads, and does not allow fallback execution.
- CG-2.1 actual count/query execution remains blocked until metadata/footer and encoded-data path readiness are both available.


## CG-1.3 invariant closeout status

- Current encoded-read and query-readiness report contracts now include cross-surface invariant coverage for no row reads, no decode/materialization, no `Arrow` default conversion, and no fallback execution.
- This is not metadata/footer IO execution and does not call scan/read-start APIs.
- CG-1.2 metadata/footer execution remains paused after CG-1.2d.8 pending fixture/no-IO `Footer` route confirmation.
- CG-2.1 actual execution remains blocked pending metadata/footer and encoded data path availability.
