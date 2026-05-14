# Vortex Public API Inventory

## Purpose
This document records upstream `vortex` public APIs inspected for ShardLoom adapter work and defines
narrow adapter boundaries. It is an API evidence inventory, not an implementation authorization
document.

Active phase status, active queue placement, and CG closeout decisions live in
`docs/architecture/phased-execution-plan.md`. This document is an API evidence inventory. It may
record what was inspected, but it does not authorize new Vortex IO or mark CG-1/CG-2/CG-3 complete.

Status words in historical sections below describe evidence recorded at the time of the original
phase note. They are not active queue state and do not override `phased-execution-plan.md`.

## Current status snapshot

- Approved historical metadata/footer path: feature-gated local metadata/footer fixture open.
- Approved local primitive scan paths: feature-gated local `.vortex` primitive paths for `CountAll`,
  `CountWhere`, `FilterPredicate`, `ProjectColumns`, and `FilterAndProject` where recorded in
  `docs/architecture/phased-execution-plan.md`.
- Prepared encoded execution surfaces: prepared encoded filter, projection, and filter-project
  evidence paths with source-bound prepared batch envelopes, reader split-ref validation, explicit
  reader-generated kernel-input admission, and narrow direct reader-chunk lowering for constants
  through `ArrayRef::as_constant()`, dictionaries through `DictArray` slots, and run-end arrays
  through `RunEnd` slots.
- Still blocked reader-chunk lowering: nullable dictionaries/RLE, sparse internals, nested/extension
  values, scalar row access, canonicalization, Arrow conversion, decoded row materialization, and
  generalized Source/Sink extraction remain deferred until phase-plan work proves
  dtype/encoding-specific no-decode mappings.
- Vortex-native provider framing: upstream Vortex array, compute, scan, source, and sink APIs may be
  native providers only when isolated in approved ShardLoom boundaries, version-recorded,
  feature-gated, policy-admitted, certificate-required, and no-fallback. Current reader-backed
  evidence carries `VortexNativeProviderBoundary` with provider kind, crate/version, API surface,
  feature gate, admission policy, certificate requirement, external-engine status, and fallback
  status.
- Scoped native Vortex admission framing: `compute-capability-matrix` and
  `vortex-count-benchmark` now expose the admitted `local_vortex_count_scalar` lane for local
  Vortex scan `CountAll` to typed scalar result evidence. This admits the exact fixture-certified
  lane only; broad source/sink/operator/workload support remains deferred.
- Compute-provider report framing: execution certificates carry `ExecutionProviderKind` fields,
  while `VortexComputeProviderReport`, `VortexComputeProviderAlignmentReport`, and
  `VortexIntegrationBoundaryReport` keep upstream Vortex-native provider boundaries distinct from
  Vortex query-engine integrations and external baselines.
- Source/Split admission framing: `vortex-api-inventory` now exposes
  `shardloom.vortex_source_split_runtime_admission.v1` for the scoped
  `local_vortex_file_scan_into_array_iter` fixture path. The proof records `vortex` version `0.70`,
  `vortex-local-primitives`, `VortexFile::scan` / `ScanBuilder` API surfaces, source/split refs,
  field-mask and predicate-ordering blockers, execution/Native I/O refs, and
  `fallback_attempted=false`. This classifies the fixture path only; generalized Source/Split,
  object-store, table/catalog, split serialization, and residual execution remain blocked.
- Segment extraction admission framing: `vortex-api-inventory` now exposes
  `shardloom.vortex_segment_extraction_admission.v1` for the `sparse_patch_fill` layout family.
  The report records the Vortex sparse layout concepts checked and keeps sparse segment extraction
  `blocked_until_segment_extraction_certificate` with deterministic diagnostics, required
  correctness/execution/Native I/O/materialization evidence, `claim_gate_status=not_claim_grade`,
  `external_engine_invoked=false`, and `fallback_attempted=false`.
- Residual boundary framing: reader-generated prepared-batch reports carry
  `VortexResidualBoundaryReport`; admitted constant/dictionary/run-end kernel inputs use
  `residual_executor=none`, while opaque/sparse/nullable/unsupported chunks use
  `residual_executor=unsupported_blocked` with `external_engine_invoked=false`, prohibited external
  fallback, and `fallback_attempted=false`.
- Source-backed expansion evidence: source-backed encoded filter/projection reports expose
  `VortexSourceBackedExpansionEvidenceReport`, linking correctness, execution-certificate, Native
  I/O certificate, benchmark-row requirement, no-fallback evidence, and benchmark/production claim
  blockers for the expansion.
- Still deferred: generalized Source/Sink API integration, object-store scan, table/catalog scan,
  broad reader wiring, writes, Arrow-default execution, GPU/device execution,
  vector/geospatial/media execution, and external query-engine integration execution.
- Prohibited: DataFusion, DuckDB, Spark, Polars, Velox, `vortex-datafusion`, or similar engines
  executing unsupported ShardLoom residual work as fallback.

## Inventory Rules

- Record the upstream `vortex` dependency version and license posture.
- Record public API discovery method.
- Classify DType, array, encoding, layout, statistics, file/open, scan/source, write/sink, and Arrow
  interoperability surfaces.
- Keep all broad scan, read-start, row-read, decode, Arrow conversion, object-store, write, and
  fallback paths explicitly blocked unless a phase-plan item authorizes them.
- Keep upstream Vortex-native providers distinct from Vortex query-engine integrations.
- Re-validate upstream public API shapes before any generalized encoded-data execution path.
- Promote any new executable API usage into `phased-execution-plan.md` before implementation.

## Dependency Snapshot
- Crate: `vortex`
- Version: `0.70`
- License: Apache-2.0 (per dependency review)
- ShardLoom crate using it: `shardloom-vortex`
- Actual Vortex IO implemented: historical metadata/footer fixture open plus approved feature-gated
  local primitive scan paths where recorded in the phase plan
- Fallback execution introduced: no

## Public API discovery method
- Inspected dependency linkage and version via `shardloom-vortex/Cargo.toml` and `cargo tree -p
  shardloom-vortex`.
- Used compiler-safe reference checks in `shardloom-vortex` without invoking runtime IO APIs.
- Reviewed local readiness and dependency review docs.
- Documented only names/status; no copied upstream implementation code.

## Candidate API areas
### DType / logical type APIs
- Public API names discovered: `vortex::array::dtype::DType::is_struct`, `DType::is_nullable`,
  `DType::to_string`, and primitive/logical type variants used for no-decode dictionary/run-end
  dtype mapping.
- Use now: yes, for local primitive scan evidence and narrow constant/dictionary/run-end
  reader-chunk kernel-input lowering.
- Stability for first adapter work: partially acceptable for feature-gated local evidence; broad
  dtype mapping remains staged.
- Adapter support: planned.
- Risks: upstream typed API names may shift; do not infer unsupported nested/extension semantics
  from string or generic dtype access.

### Array APIs
- Public API names discovered: `vortex::array::ArrayRef::len`, `ArrayRef::dtype`,
  `ArrayRef::encoding_id`, `ArrayRef::nchildren`, `ArrayRef::nbuffers`, `ArrayRef::named_children`,
  `ArrayRef::as_constant`, `ArrayRef::as_opt::<Dict>`, `ArrayRef::as_opt::<RunEnd>`, and direct host
  primitive buffers for dictionary codes/values and run ends/values.
- Use now: yes, only in feature-gated local primitive scan evidence and direct
  constant/dictionary/run-end reader-chunk kernel-input lowering where upstream APIs expose
  whole-chunk values, dictionary slots, or run-end slots without decode/materialization.
- Stability: acceptable only for narrow local evidence; broad encoded-value extraction remains
  staged.
- Adapter support: local reader chunk evidence plus constant, dictionary, and run-end lowering; real
  payload write path remains future, feature-gated, and explicitly approved.
- Risks: `scalar_at`, `execute_scalar`, canonicalization, Arrow execution, validity row reads,
  nullable dictionary/RLE handling, sparse patch/fill interpretation, and broad child traversal can
  silently become row/decode/materialization behavior if admitted without separate evidence.

### Encoding APIs
- Public API names discovered: `DictArray` slots (`codes`, `values`) and `RunEndArray` slots
  (`ends`, `values`, `offset`) for narrow no-decode local reader-chunk lowering.
- Use now: yes, for non-null host primitive dictionary and run-end arrays only; sparse and other
  encodings remain blocked.
- Stability: partially acceptable for feature-gated local evidence; broad encoding mapping remains
  staged.
- Adapter support: planned beyond the narrow local lowering path.
- Risks: encoding taxonomy drift, nullable validity handling, sparse patch/fill semantics, and
  device/non-host buffers require separate evidence before support claims.

### Layout APIs
- Public API names discovered: not confirmed yet.
- Use now: yes, via temporary name-based mapping only.
- Stability: partially acceptable for placeholders.
- Adapter support: planned; sparse patch/fill segment extraction is explicitly blocked by
  `shardloom.vortex_segment_extraction_admission.v1` until certificate-backed layout semantics,
  validity handling, and materialization/decode evidence exist.
- Risks: layout labels may change; sparse patch/fill traversal can silently become decoded,
  materialized, or canonicalized execution if admitted without separate evidence.

### Statistics APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: deferred/planned.
- Risks: stats exactness/typing details unclear.

### File/open APIs
- Public API names discovered: `vortex::file::VortexOpenOptions`,
  `vortex::file::OpenOptionsSessionExt`, `vortex::file::VortexFile::footer`, and
  `vortex::file::VortexFile::row_count`.
- Use now: yes, only in `invoke_vortex_metadata_footer_probe_with_session_async` behind
  `vortex-file-io` with a caller-provided `VortexSession`.
- Stability: acceptable for the narrow local metadata/footer fixture path.
- Adapter support: local metadata/footer only; scans, encoded data traversal, object-store IO, and
  writes remain deferred.
- Risks: broadening this path could accidentally introduce scan/read/decode/write behavior without
  CG approval.

### Scan/source APIs
- Public API names discovered: `VortexFile::scan`, `ScanBuilder::with_filter`,
  `ScanBuilder::with_projection`, `ScanBuilder::with_concurrency`, and
  `ScanBuilder::into_array_iter`.
- Use now: yes, for approved feature-gated local primitive scan paths and reader split evidence.
- Stability: acceptable only for local primitive evidence and the GAR-0042A admission proof;
  generalized Source/Sink, object-store, table/catalog, split serialization, and residual execution
  remain staged.
- Adapter support: local scan evidence plus report-only source/split admission proof.
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
Implement typed DType adapter mapping only if upstream public DType APIs are clearly confirmed and
compile-safe; otherwise keep ShardLoom-local placeholder mapping until API stability is better
understood.


## Typed DType adapter probe

- Typed DType mapping implemented: no (deferred).
- Public upstream APIs used for typed mapping in this PR: none confirmed compile-safe in this
  environment.
- Added compile-safe non-IO typed mapping status/report only: yes.
- Name-based mapping remains available as a temporary planning utility: yes.
- Fallback execution introduced: no.
- Actual IO implemented: no.
- Risks: upstream public typed DType API names and constructors must be re-validated once
  registry/docs access is available.

## Encoding APIs and Layout APIs update (adapter probe)

- Confirmed compile-time dependency linkage to upstream `vortex` crate remains intact in
  `shardloom-vortex`.
- Public typed encoding/layout adapter APIs were **not** implemented in this probe because this
  environment could not verify stable, non-IO constructible upstream public encoding/layout types
  safely.
- Typed encoding mapping status: deferred (`deferred_api_unclear`).
- Typed layout mapping status: deferred (`deferred_api_unclear`).
- Name-based mapping helpers remain available for planning-time adapter labeling
  (`map_known_vortex_encoding_name`, `map_known_vortex_layout_name`).
- Risk: upstream public encoding/layout surfaces may change; typed mapping should only land when
  compile-safe constructors and stable public APIs are confirmed.
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

- Public statistics type/API names discovered: no obvious public typed statistics structs/enums were
  found in the locally resolved `vortex` crate surface for this environment.
- Use in this PR: typed statistics mapping is deferred; compile-safe report/status + `ShardLoom`
  `SegmentStats` placeholders were added.
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

- Public API names discovered: `vortex::file::VortexOpenOptions`,
  `vortex::file::OpenOptionsSessionExt`, and `vortex::file::VortexFile::footer` via upstream crate
  public re-exports.
- Used in this PR: no runtime invocation; discovery-only.
- Metadata-only open appears supported: potentially, but runtime semantics are not yet proven
  compile-safe for strict no-materialization guarantees in this phase.
- Actual scan/materialization avoided: yes.
- Stability for first metadata probe: deferred pending clearer adapter-safe API contract.
- Risks: async/session requirements and footer open path may read more than minimal metadata
  depending on upstream behavior.

## Metadata inspection APIs

- Public API names discovered: `Footer` accessors exposed through `vortex::file` module surface,
  plus dtype/layout/statistics references in crate docs.
- Schema/`DType` metadata access: appears possible through footer/layout APIs, but not adopted in
  this PR.
- Row count / length access: appears available via footer row count APIs, not adopted in this PR.
- Statistics access: appears available in footer/file statistics APIs, not adopted in this PR.
- Encoding/layout metadata access: appears available through footer/layout graph, not adopted in
  this PR.
- Adapter support state: deferred (`deferred_api_unclear`) for runtime metadata IO; report-only
  status implemented.

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
- Broad Vortex file IO remains unimplemented.
- A narrow local metadata/footer fixture open is implemented behind `vortex-file-io` and requires a
  caller-provided async/session context.
- Object-store support remains disabled by default.
- No fallback engines were introduced.

## Metadata-only file open contract update

- Feature gate used: `vortex-file-io`.
- Public metadata-only file API integration exists only for local `.vortex` fixtures through the
  caller-session async helper.
- `VortexMetadataSummaryReport` integration exists as an optional field on the open report.
- No scan execution is performed.
- No read-start or encoded data traversal is performed.
- No data materialization is performed.
- No object-store IO is performed.
- No writes are performed.
- No fallback execution is allowed.

## Metadata-only open update

- Public APIs re-validated for this phase: `vortex::file::VortexOpenOptions`,
  `vortex::file::OpenOptionsSessionExt`, and `vortex::file::VortexFile::footer` as inventory targets
  from upstream public surfaces.
- Runtime usage posture in this phase:
  - non-Vortex targets: deterministic invalid-target report;
  - object-store URIs: deterministic unsupported/deferred report;
  - missing local `.vortex` path: deterministic file-missing report;
  - existing local fixture with caller-provided `VortexSession`: feature-gated metadata/footer open
    with `MetadataOpened` and `FooterInspected` effects.
- `footer`/`row_count`/`dtype` usage is limited to the feature-gated local fixture path.
- Sync/default metadata probe paths remain report-only/no-IO.
- Tests exercised in this phase: missing local path, invalid target, object-store rejection,
  feature-disabled path, feature-enabled deterministic reporting, and feature-gated checked-in local
  fixture open.
- No scan execution introduced.
- No read-start, encoded data traversal, row reads, decode/materialization, or `Arrow` conversion
  introduced.
- No fallback execution introduced.

## Read planning update

- ShardLoom now has a metadata-driven read planning skeleton.
- It does not call upstream Vortex scan execution.
- It prepares the future scan/split boundary.
- It preserves no-decode/no-materialization/no-object-store/no-write behavior.
- No fallback execution introduced.

## Runtime task graph integration update

`ShardLoom` now bridges `Vortex` read planning into runtime task graph skeletons.
This bridge does not call upstream `Vortex` scan execution, does not read data, does not materialize
values, and does not execute tasks.
No fallback execution is introduced.

## Adaptive sizing integration update

`ShardLoom` now exposes a bridge that maps `Vortex` read planning and runtime graph reports into
adaptive sizing reports and plans.

This integration is planning-only and does not call upstream `Vortex` scan execution. It does not
read data, materialize values, or execute tasks. No fallback execution behavior or external
execution engine is introduced.

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

The recorded spike validates ShardLoom's end-to-end Vortex contract without invoking upstream scan
execution.

The feature-gated file IO seam remains metadata/report oriented, and no fallback execution is
introduced.

## Metadata-only executor update

- This executor does not call upstream Vortex scan execution.
- It consumes ShardLoom planning/readiness reports.
- It does not invoke upstream data APIs.
- No fallback execution introduced.

## Encoded-read readiness update

This contract does not call upstream Vortex scan execution. It consumes `ShardLoom`
scheduler/readiness reports, does not invoke upstream data APIs, and introduces no fallback
execution.

## Encoded-read executor skeleton update

- This skeleton does not invoke upstream data APIs.
- It classifies what would execute later.
- No fallback execution introduced.

## Encoded-read public API boundary update

This update adds a `ShardLoom` contract-only boundary for future `Vortex` encoded-read work.

Confirmed public upstream API surfaces from the recorded review include `VortexOpenOptions`,
`OpenOptionsSessionExt`, `VortexFile::footer`, and the direct footer-backed `VortexFile::row_count`
metadata surface plus related `dtype` metadata surfaces. The CG-2.1e.2/CG-2.1e.5 classifications
also compile-check exact public data-access-adjacent symbols: `VortexFile::layout_reader`,
`LayoutReader::row_count`, `VortexFile::scan`, `ScanBuilder::into_array_stream`,
`ScanBuilder::into_array_iter`, `LayoutReader::projection_evaluation`,
`LayoutReader::filter_evaluation`, and `VortexFile::data_source`.

Contract-usable now:
- `VortexOpenOptions` + `OpenOptionsSessionExt::open_path` + `VortexFile::footer` in the
  feature-gated local metadata/footer fixture path.
- `VortexFile::row_count` as a public metadata-only footer row-count method for count planning
  evidence.
- `row_count`/`dtype` metadata summaries in the same local fixture path.

Deferred contract surfaces:
- `VortexFile::layout_reader`, `LayoutReader::row_count`, `VortexFile::scan`,
  `ScanBuilder::into_array_stream`, `ScanBuilder::into_array_iter`,
  `LayoutReader::projection_evaluation`, `LayoutReader::filter_evaluation`,
  `VortexFile::data_source`, object-store, and write surfaces remain deferred or forbidden while
  encoded-read execution remains blocked by default.

APIs that would start data reads:
- Scan/start-read APIs are explicitly classified under data-read and marked forbidden for now.

APIs that would decode/materialize:
- Decode/materialization related areas are classified as forbidden-for-now boundary areas.

APIs with Arrow-default behavior risk:
- Arrow interop/conversion APIs are classified with `ArrowDefaultPath` blocking risk.

Forbidden-for-now areas:
- Data read, decode, materialization, object-store IO, and write IO remain blocked.

No fallback execution is introduced by this boundary update.

- Encoded-read probe plan (`vortex-encoded-read-probe`) combines
  `VortexEncodedReadApiBoundaryReport` and `VortexEncodedReadReadinessReport`; it is plan-only and
  performs no scan execution, no data read/decode/materialization, no object-store/write/spill IO,
  and no fallback.

## Encoded-read spike update

- Inspected public upstream `vortex` API surface via compile-time integration and existing
  `vortex-file-io` path.
- Phase 8 spike is feature-gated behind `vortex-encoded-read-spike`.
- Recorded spike returns deterministic blocked/deferred execution when a safe
  no-decode/no-materialize encoded-read API path is not yet proven.
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

This is `ShardLoom`-native scheduling/runtime plumbing for `Vortex` local execution. It does not
call upstream `Vortex` scan execution and introduces no fallback execution.


## Local engine surface update
- This is `ShardLoom`-native CLI/API plumbing.
- It does not call upstream `Vortex` scan execution.
- No fallback execution is introduced.

## Local engine metadata-open propagation update

The local engine surface preserves metadata-open report context in `VortexLocalEngineReport`,
including metadata-open status and diagnostics.

This update does not introduce scans, decode, materialization, writes, object-store IO, spill IO, or
fallback execution.


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
- Phase 12A.3a recorded-active: staged manifest draft core contract (report-only, no filesystem).
- Phase 12A.3b planned: feature-gated local staged manifest draft file.
- Phase 12A.3c planned: CLI/docs integration.
- Actual output payload and file writes remain deferred.

- Staged manifest draft is `ShardLoom`-native.
- Upstream `Vortex` write APIs remain deferred.
- Actual `Vortex` writes remain deferred.

- Manifest draft file planning is `ShardLoom`-native.
- Upstream `Vortex` write APIs remain deferred.


## Staged manifest draft write posture

- Staged manifest draft file writing is `ShardLoom`-native and does not depend on upstream `Vortex`
  write APIs.
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

- `vortex-commit-marker-plan`, `vortex-commit-intent-plan`, and `vortex-commit-protocol-plan` are
  `ShardLoom`-native report-only `CLI` wrappers over commit planning reports.
- `vortex-commit-marker-write` is a `ShardLoom`-native `CLI` wrapper over the feature-gated local
  commit marker write helper.
- They provide stable text/JSON output fields for readiness validation.
- They do not execute commits, finalize manifests, write commit markers, write output payloads, or
  perform object-store IO.
- Upstream `Vortex` write API calls remain deferred.


## Local staged write-readiness smoke test posture

- The staged write-readiness smoke test remains ShardLoom-native and does not use upstream `Vortex`
  write APIs.


## Commit marker planning note

- Commit marker planning is `ShardLoom`-native.
- Commit marker writing is `ShardLoom`-native and limited to the exact feature-gated local marker
  artifact.
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
- CG-19 universal native I/O envelope evidence
- CG-20 SQL/operator/function/adapter/user capability evidence

- Upstream Vortex write/read APIs must remain feature-gated.
- No upstream API should be called if it implies row materialization, Arrow conversion, object-store
  IO, or write execution without explicit feature gate.
- The encoded-read boundary `CLI` can model open/options/footer/metadata boundary surfaces through
  `VortexEncodedReadBoundaryRequest`.
- Scan/data traversal APIs remain deferred until CG-1.2+ and must stay feature-gated.

## Competitive Engine Track API alignment

- CG items are competitive success gates / roadmap tracks, not implementation phase IDs.
- External engines are baseline references only, never fallback execution.
- API evolution must preserve Vortex-native, no-fallback, explicit-boundary behavior.

- The staged write-readiness smoke test remains `ShardLoom`-native and does not use upstream
  `Vortex` write APIs.

## Phase 12B.4 readiness audit note

- Upstream Vortex write APIs remain unused after commit-marker staging.
- Manifest finalization remains ShardLoom-native and report-only for the next phase.


- Manifest finalization remains `ShardLoom`-native/report-only in Phase 12B.5a; upstream `Vortex`
  write APIs remain deferred.

## Finalized-manifest candidate artifact posture

- Finalized-manifest candidate writing is `ShardLoom`-native.
- Upstream `Vortex` write APIs remain deferred.
- Candidate artifact writes do not imply committed manifest state.


- Local commit execution gate is `ShardLoom`-native and report-only in Phase 12B.6.
- Phase 12D.1 / CG-4.1 local committed-manifest execution is `ShardLoom`-native and feature-gated
  behind `vortex-staged-output-fs`.
- It copies the local finalized-manifest candidate to `_shardloom_committed_manifest.json`; no
  upstream `Vortex` commit/write API is called.
- Phase 12D.2 / CG-4.2 local committed-manifest recovery and rollback diagnostics are
  `ShardLoom`-native and report-only.
- They emit `RecoveryPlan` cleanup targets and ambiguous commit records; no committed-manifest
  deletion, cleanup execution, object-store IO, or upstream `Vortex` API is called.
- Phase 12D.3 / CG-4.3 local committed-manifest rollback cleanup execution is `ShardLoom`-native and
  feature-gated behind `vortex-staged-output-fs`.
- It deletes only `_shardloom_committed_manifest.json` after rollback-planned recovery evidence;
  finalized-manifest, commit-marker, output-payload, object-store IO, and upstream `Vortex` APIs
  remain untouched.
- Upstream `Vortex` commit/write APIs remain deferred from this gate.
- Output payload plan CLI is `ShardLoom`-native in Phase 12C.3a (complete) and remains report-only.
  Output payload artifact write CLI is `ShardLoom`-native in Phase 12C.3b (complete). Phase 12C.4
  keeps the staged smoke test `ShardLoom`-native while real upstream `Vortex` write APIs remain
  deferred. CG-3.1 introduces the first feature-gated upstream `Vortex` write call for a local
  one-row `CountAll` payload only.


- Output payload contract is `ShardLoom`-native and report-only.
- Upstream `Vortex` write APIs remain deferred until explicit approval.
- Object-store write APIs remain deferred.

- Output payload artifact writing is `ShardLoom`-native and does not use upstream `Vortex` write
  APIs.
- Native count output payload writing uses upstream `VortexSessionDefault`, `SingleThreadRuntime`,
  `WriteOptionsSessionExt`, `PrimitiveArray`, `Validity`, and `buffer!` inside `shardloom-vortex`
  under `vortex-write`.
- Upstream `Vortex` write APIs for broader payload writes remain deferred.


## CG-3 clarification

- Local placeholder artifact write paths are not real Vortex payload write paths.
- CG-3.1 approves a feature-gated local native Vortex count-result payload path only.
- Broader payload shapes, manifest commits, and object-store writes remain deferred.


- CG-1.2b adds metadata/footer probe contracts only; default report construction does not inspect
  local file existence and scan/data traversal remains deferred.

- CG-1.2c exposes the metadata probe contract through CLI only; default path does not inspect local
  files and does not perform metadata/footer IO.


## CG-1.2d blocker clarification

- Re-validated public symbols: `VortexOpenOptions`, `OpenOptionsSessionExt`, `VortexFile::footer`.
- Recorded blocker: these metadata/footer paths require async/session invocation semantics.
- This phase intentionally avoids introducing a runtime boundary (`tokio`/executor wiring) for
  probe-only metadata/footer calls.
- Result: `MetadataProbeCompleted` remains unreachable in this phase; deterministic
  `BlockedByUnsupportedApiSurface` is preserved for existing local files under `vortex-file-io`.


## CG-1.2d.2 deterministic async/session boundary contract

- Adds report-only async/session boundary planning for local metadata/footer probes.
- Keeps actual `VortexOpenOptions`/`OpenOptionsSessionExt`/`VortexFile::footer` invocation deferred
  to CG-1.2d.3.
- No runtime/executor wiring is added.
- No scan/read-start, encoded reads, row reads, decode/materialization, `Arrow` conversion,
  object-store IO, writes, or fallback execution are added.


### CG-1.2d.3 update
- Added feature-gated async metadata/footer invocation surface for caller-provided async context
  only.
- No runtime/executor dependency was added by `ShardLoom`.
- Sync `VortexEncodedReadMetadataProbeReport::from_request` path remains report-only/no-IO.
- Async surface preserves no scan/read-start, no encoded-data reads, no decode/materialization, no
  `Arrow` conversion, no object-store IO, no writes, and no fallback execution.
- At this phase, actual public upstream `Vortex` metadata/footer invocation remained blocked by
  compile-unclear API shape; CG-1.2d.9 supersedes that blocker for the approved local fixture path.


## CG-1.2d.4 update (API compile probe)
- Added feature-gated compile probe that confirms public `Vortex` symbols compile in
  `shardloom-vortex`: `vortex::file::VortexOpenOptions`, `vortex::file::OpenOptionsSessionExt`,
  `vortex::file::VortexFile`, and `vortex::session::VortexSession`.
- Production async invocation remained deterministically blocked in this phase; CG-1.2d.9 supersedes
  this for the approved local fixture path.

## CG-1.2d.5 update (method-shape compile probe)
- Added feature-gated method-item probes that compile-check the following public method shapes
  without invocation: `<VortexSession as OpenOptionsSessionExt>::open_options(&self) ->
  VortexOpenOptions`, `VortexOpenOptions::with_initial_read_size(self, usize) -> VortexOpenOptions`,
  `VortexOpenOptions::with_some_file_size(self, Option<u64>) -> VortexOpenOptions`, and
  `VortexFile::footer(&self) -> &Footer`.
- At this phase, invocation remained blocked by unsupported API surface; CG-1.2d.9 supersedes this
  for the approved local fixture path.
- No runtime/executor dependency was added and no file open, metadata/footer IO, scan/read-start,
  decode/materialization, `Arrow` conversion, object-store IO, writes, or fallback execution was
  introduced.

## CG-1.2d.6 update (caller-provided session contract + open method probe)
- CG-1.2d.5 confirmed symbols/methods remain valid: `VortexOpenOptions`, `OpenOptionsSessionExt`,
  `VortexFile`, `VortexSession`, `open_options`, `with_initial_read_size`, `with_some_file_size`,
  and `footer`.
- Added caller-provided `VortexSession` invocation contract (`VortexMetadataAsyncInvocationInput<'a>
  { boundary, session }`) behind `vortex-file-io`; construction is contract-only and performs no IO.
- Added compile probe reference for `VortexOpenOptions::open_path` method item to identify
  local-path open surface without invocation.
- Production metadata/footer invocation remained blocked pending approved async runtime/IO harness
  and explicit invocation policy; CG-1.2d.9 adds the approved local fixture invocation path.
- No runtime/executor dependency, file open, metadata/footer IO, scan/read-start,
  decode/materialization, `Arrow` conversion, object-store IO, writes, or fallback execution was
  added.

## Test-only async metadata/footer harness policy

- Test-only async execution is allowed only in feature-gated tests.
- It must not affect production/default runtime behavior.
- It must not add fallback execution.
- It must not call scan/read-start/decode/materialization/`Arrow`/object-store/write APIs.
- A dev-dependency executor is allowed only when already present in `Cargo.lock` through the
  `Vortex` feature graph and when adding it introduces no new lockfile packages.
- A checked-in local `.vortex` fixture is allowed only with explicit provenance and only for
  metadata/footer open tests.
- Fixture generation using `Vortex` write APIs is not allowed in this phase.

## CG-1.2d.9 update (local metadata/footer fixture invocation)

- Added checked-in local `.vortex` metadata/footer fixture provenance under
  `shardloom-vortex/tests/fixtures/`.
- Added feature-gated caller-session metadata/footer invocation using `VortexOpenOptions` +
  `OpenOptionsSessionExt::open_path` + `VortexFile::footer`.
- Successful local fixture invocation emits `MetadataFooterOpened` with `metadata_opened` and
  `footer_inspected` effects.
- Open failures emit deterministic `metadata_footer_open_failed` diagnostics.
- Sync/default paths remain report-only/no-IO, and non-session async helpers remain deferred.
- No scan/read-start, encoded data traversal, row reads, decode/materialization, `Arrow` conversion,
  object-store IO, writes, or fallback execution are introduced.


## CG-2.0 query primitive boundary update
- CG-1.2d.9 clears the local fixture metadata/footer invocation blocker for the caller-session
  feature-gated path.
- Historical evidence: CG-2.0 added a report-only, feature-gated `Vortex` query primitive readiness
  boundary for count, filtered count, projection, and predicate/filter primitives.
- This boundary does not execute query primitives and remains side-effect-free.
- CG-2.1c clears the metadata-footer `CountAll` blocker; CG-2.1d clears the encoded-data count
  candidate blocker while actual encoded execution remains deferred.
- No scan/read-start, encoded data reads, row reads, decode/materialization, `Arrow` conversion,
  object-store `IO`, writes, or fallback execution are introduced.
- CG-1 through CG-20 remain visible and active competitive gates.

## CG-2.0b query primitive helper correctness update
- Metadata async invocation to query primitive request propagation now preserves boundary
  feature-gate/object-store/risk signals and no longer drops blockers from `boundary_report`.
- Query primitive error detection now includes report diagnostic severity (`Error`/`Fatal`) in
  addition to status/request checks.
- No upstream Vortex scan/read-start or write APIs are called by this update.
- No encoded data reads, row reads, decode/materialization, Arrow conversion, object-store IO,
  writes, or fallback execution are introduced.
- Optional CLI command for direct query primitive planning is deferred to CG-2.0c.

## CG-2.0c query primitive plan CLI integration
- Adds `shardloom vortex-query-primitive-plan <primitive> <dataset_uri> [flags] [--format
  text|json]` as a report-only/readiness-only planning command.
- Command constructs `VortexQueryPrimitiveRequest` and calls `plan_vortex_query_primitive` only; it
  does not execute query primitives.
- Command does not call scan/read-start APIs, does not read encoded data or rows, does not
  decode/materialize/Arrow-convert, does not perform object-store IO, does not write output
  payloads, and does not allow fallback execution.
- CG-2.1+ actual non-metadata count/query execution remains blocked until encoded-data readiness
  exists for non-metadata candidates.


## CG-1.3 invariant evidence note

- Recorded encoded-read and query-readiness report contracts now include cross-surface invariant
  coverage for no row reads, no decode/materialization, no `Arrow` default conversion, and no
  fallback execution.
- Feature-gated local fixture metadata/footer invocation exists through CG-1.2d.9 and does not call
  scan/read-start APIs.
- CG-2.1c wires metadata-footer count execution; encoded-data execution remains blocked pending
  encoded data path availability for non-metadata candidates.


## CG-2.1 count readiness planning update

- CG-1.3 invariant contract tests are complete.
- CG-2.0 / CG-2.0b / CG-2.0c / CG-2.0c.1 are complete.
- Historical evidence: CG-2.1 added a report-only
  `VortexCountReadinessRequest`/`VortexCountReadinessReport` planning contract.
- Count planning distinguishes metadata-footer candidates from encoded-data-path candidates.
- Metadata-footer `CountAll` execution is wired through CG-2.1c; encoded-data count candidates are
  approved/deferred through CG-2.1d while actual encoded execution remains deferred.
- No scan/read-start, encoded-data reads, row reads, decode, materialization, `Arrow` conversion,
  object-store IO, writes, or fallback execution are introduced.
- CG-2.1b `CLI` surfacing is complete via `shardloom vortex-count-readiness-plan <candidate_source>
  <dataset_uri> [flags] [--format text|json]`.
- CG-2.1a semantic hardening is complete: `VortexCountCandidateSource::Unknown` cannot be
  readiness-complete and deterministically returns `blocked_by_unsupported_primitive` when
  feature-gated count/query-primitive-ready signals are present.
- `VortexCountReadinessReport` error detection is severity-aware across status, request diagnostics,
  and report diagnostics.
- Count readiness remains report-only and does not execute count.
- `CLI` output remains report-only/readiness-only and never executes count.
- No scan/read-start, encoded-data read, row read, decode, materialization, `Arrow` conversion,
  object-store `IO`, writes, or fallback execution are introduced.

## CG-2.1c metadata-footer CountAll execution bridge

- `VortexMetadataAsyncInvocationReport` carries a typed `VortexMetadataSummaryReport` for successful
  feature-gated local footer invocation.
- `VortexQueryPrimitiveKind::Count` no longer requires encoded-data-path readiness when
  metadata-footer readiness is available.
- `execute_vortex_count_all_from_metadata_footer_invocation` returns metadata-only local `CountAll`
  results from the typed footer summary.
- The checked-in fixture path proves `Count(20000)` from actual `VortexFile::footer` metadata.
- No scan/read-start, encoded data traversal, row reads, decode/materialization, `Arrow` conversion,
  object-store IO, writes, or fallback execution are introduced.

## CG-2.1d encoded-data CountAll candidate bridge

- `count_readiness_request_from_encoded_read_readiness_report` turns a side-effect-free future
  encoded-read readiness candidate into an encoded-data count candidate.
- `execute_vortex_count_all_from_encoded_data_candidate` returns a deferred `NeedsEncodedRead` local
  execution report for approved encoded-data count candidates.
- The helper rejects metadata-footer count readiness when the encoded-data helper is requested,
  preserving explicit candidate-source boundaries.
- This remains candidate/defer scope only: no scan/read-start, encoded data traversal, row reads,
  decode/materialization, `Arrow` conversion, object-store IO, writes, or fallback execution are
  introduced.

## CG-2.1e.1 encoded-data CountAll API gate

- `count_readiness_request_from_encoded_read_probe_report` now gates encoded-data count readiness
  through `VortexEncodedReadProbeReport`.
- The recorded public API boundary still reports scan/data-read and Arrow-default risks for actual
  data access, so encoded-data `CountAll` cannot yet become executable from scheduler/readiness
  candidates alone.
- Public API blockers from the probe are translated into count-readiness blockers before any
  execution helper is allowed to see `EncodedDataPathReady`.
- This pass intentionally performs no scan/read-start invocation, no encoded data traversal, no row
  reads, no decode/materialization, no `Arrow` conversion, no object-store IO, no writes, and no
  fallback execution.
- CG-2.1e actual encoded-data count execution remains blocked until the public Vortex data path is
  approved as no-decode/no-materialization safe.

## CG-2.1e.2 exact Vortex data-access API classification

- The encoded-read public API boundary now lists exact reviewed Vortex surfaces instead of only
  generic scan/read-start placeholders.
- Compile-checked symbols include `VortexFile::layout_reader`, `LayoutReader::row_count`,
  `VortexFile::scan`, `ScanBuilder::into_array_stream`, `LayoutReader::projection_evaluation`,
  `LayoutReader::filter_evaluation`, and `VortexFile::data_source`.
- `LayoutReader::row_count` is metadata-like layout access and does not prove encoded-data
  traversal.
- `VortexFile::scan`, `ScanBuilder::into_array_stream`, `ScanBuilder::into_array_iter`,
  `LayoutReader::projection_evaluation`, `LayoutReader::filter_evaluation`, and
  `VortexFile::data_source` remain blocked or deferred for execution because their
  no-decode/no-materialization semantics are not yet approved for ShardLoom-native count.
- Execution usability remains zero; no scan/read-start, array stream/evaluation call, encoded-data
  traversal, row read, decode/materialization, `Arrow` conversion, object-store IO, write, or
  fallback execution is introduced.

## CG-2.1e.3 named count API-boundary blockers

- Count readiness now preserves named blocked API-boundary summaries from the encoded-read probe.
- Blocked public surfaces such as `VortexFile::scan`, `ScanBuilder::into_array_stream`,
  `ScanBuilder::into_array_iter`, `LayoutReader::projection_evaluation`,
  `LayoutReader::filter_evaluation`, and `VortexFile::data_source` are visible at the
  count-readiness boundary.
- Metadata-like `LayoutReader::row_count` is not reported as an execution blocker.
- This remains report metadata only and introduces no scan/read-start invocation, array
  stream/evaluation call, encoded-data traversal, row read, decode/materialization, `Arrow`
  conversion, object-store IO, write, or fallback execution.

## CG-2.1e.4 encoded-count admission blocker guard

- Named API-boundary blockers are now enforced by count-readiness derivation and local encoded-count
  admission.
- A readiness request that still names blocked surfaces cannot become `CountReady`, even with
  `EncodedDataPathReady`.
- Local encoded-count admission rejects reports with named blockers instead of deferring them as
  execution candidates.
- This remains a guardrail only and introduces no scan/read-start invocation, array
  stream/evaluation call, encoded-data traversal, row read, decode/materialization, `Arrow`
  conversion, object-store IO, write, or fallback execution.

## CG-2.1e.5 `VortexFile::row_count` metadata-surface approval

- `VortexFile::row_count` is compile-checked as an exact public upstream method and classified as
  confirmed public `file_metadata`.
- This approval is metadata-only: the method is contract-usable for count-planning evidence because
  it is a direct footer row-count wrapper, but it is not execution-usable under the encoded-read API
  boundary.
- `LayoutReader::row_count` remains deferred because constructing a layout reader is not yet an
  approved count path, even though the method itself is metadata-like.
- `VortexFile::layout_reader`, `VortexFile::scan`, `ScanBuilder::into_array_stream`,
  `ScanBuilder::into_array_iter`, `LayoutReader::projection_evaluation`,
  `LayoutReader::filter_evaluation`, and `VortexFile::data_source` remain blocked or deferred for
  execution.
- This remains classification-only and introduces no scan/read-start invocation, array
  stream/evaluation call, encoded-data traversal, row read, decode/materialization, `Arrow`
  conversion, object-store IO, write, or fallback execution.

## CG-2.1e.6 encoded-count data-path approval boundary

- `VortexEncodedCountDataPathApprovalReport` now combines count readiness with this API inventory
  boundary before encoded-data `CountAll` can be approved for any future execution planning.
- The recorded inventory does not approve encoded-data traversal: `VortexFile::row_count` is
  metadata-only evidence, and the boundary still has zero execution-usable data paths.
- Blocked or deferred surfaces remain visible by name, including scan, stream, layout-evaluation,
  data-source, and Arrow-default boundaries.
- This remains report-only and introduces no scan/read-start invocation, array stream/evaluation
  call, encoded-data traversal, row read, decode/materialization, `Arrow` conversion, object-store
  IO, write, or fallback execution.

## CG-2.1e.7 encoded-count approval CLI surfacing

- `shardloom vortex-encoded-count-approval-plan` now exposes the encoded-count approval boundary
  from this inventory to text/JSON CLI output.
- The command keeps recorded scan, stream, layout-evaluation, data-source, and Arrow-default
  blockers visible to users and agents.
- Ready encoded-count inputs still return unsupported/non-zero while execution-usable data path
  count remains zero.
- This remains report-only and introduces no scan/read-start invocation, array stream/evaluation
  call, encoded-data traversal, row read, decode/materialization, `Arrow` conversion, object-store
  IO, write, or fallback execution.

## CG-2.1e.8 encoded-count approval local guard

- Local encoded-count planning now has a guard that consumes the approval report derived from this
  inventory.
- The recorded inventory remains blocked by that guard because scan, stream, layout-evaluation,
  data-source, and Arrow-default blockers are still present.
- A future approved inventory boundary may advance only to deferred encoded-read planning until real
  execution is separately approved.
- This remains report-only and introduces no scan/read-start invocation, array stream/evaluation
  call, encoded-data traversal, row read, decode/materialization, `Arrow` conversion, object-store
  IO, write, or fallback execution.

## CG-2.1e.9 layout-reader construction blocker hardening

- Upstream source review shows `VortexFile::layout_reader` constructs through
  `VortexFile::segment_source`, and that public method may spawn a background I/O driver.
- The API boundary now classifies `VortexFile::layout_reader` with a runtime-driver risk instead of
  treating it as risk-free metadata access.
- `LayoutReader::row_count` remains metadata-like and non-blocking by itself, but it is not
  execution-usable and cannot approve encoded-count execution while layout-reader construction is
  blocked.
- Count-readiness and encoded-count approval retain `VortexFile::layout_reader` as a named blocker
  while excluding `VortexFile::row_count` and `LayoutReader::row_count` from encoded-data execution
  evidence.
- This remains classification-only and introduces no `LayoutReader` construction, scan/read-start
  invocation, array stream/evaluation call, encoded-data traversal, row read,
  decode/materialization, `Arrow` conversion, object-store IO, write, or fallback execution.

## CG-2.1e.10 layout-driver approval boundary

- `VortexLayoutReaderDriverApprovalReport` now records the explicit approval requirements for any
  future row-count-only `LayoutReader` construction path.
- Recorded inventory remains blocked without explicit runtime-driver permission, even when
  `VortexFile::layout_reader` and `LayoutReader::row_count` are present.
- Approval is local-fixture and row-count-only: it requires caller session permission and explicit
  no-scan, no-evaluation, no-data-read, no-decode, no-materialization, no-`Arrow`, no-object-store,
  no-write, and no-fallback signals.
- Even an approved report is still report-only and records `layout_reader_constructed=false`,
  `runtime_driver_started=false`, `scan_called=false`, `data_read=false`, and
  `fallback_execution_allowed=false`.
- This introduces no `LayoutReader` construction, driver start, scan/read-start invocation, array
  stream/evaluation call, encoded-data traversal, row read, decode/materialization, `Arrow`
  conversion, object-store IO, write, or fallback execution.

## CG-2.1e.11 layout-driver approval CLI surfacing

- `shardloom vortex-layout-driver-approval-plan <signals> [--format text|json]` now exposes the
  layout-driver approval report and side-effect fields.
- The command uses the static public API inventory and caller-provided signals only; it does not
  inspect files, open Vortex data, construct `LayoutReader`, or start a driver.
- Recorded inventory remains blocked without `runtime-driver-start-allowed`; a complete approved
  signal set still reports `layout_reader_constructed=false`, `runtime_driver_started=false`,
  `scan_called=false`, `data_read=false`, and `fallback_execution_allowed=false`.
- This introduces no scan/read-start invocation, array stream/evaluation call, encoded-data
  traversal, row read, decode/materialization, `Arrow` conversion, object-store IO, write, or
  fallback execution.

## CG-2.1e.15 local fixture Vortex array scan/count proof

- `VortexFile::scan` and `ScanBuilder::into_array_iter` are now invoked only by the feature-gated
  local fixture helper `execute_vortex_count_all_from_local_scan_with_session`.
- This helper requires a caller-owned `VortexSession`, caller-owned blocking runtime, local
  `.vortex` target, and encoded-read readiness approved for future execution.
- The helper counts returned Vortex arrays with `ArrayRef::len()` and reports array count, row
  count, count result, `data_read=true`, and `upstream_scan_called=true`.
- It reports no row read, no requested decode/materialization, no `Arrow` conversion, no
  object-store IO, no writes, no spill IO, and no fallback execution.
- The static public API boundary still treats broad scan, stream, layout-evaluation, data-source,
  and object-store paths as blocked or deferred for general execution.
- This inventory update does not approve non-fixture adapters, encoded predicates, projections,
  writes, external baselines, or superiority claims.

## CG-2.1e.16 approval-gated local fixture scan/count

- The feature-gated local fixture helper now requires
  `VortexEncodedCountDataPathApprovalReport::approved()` before it can invoke `VortexFile::scan` or
  `ScanBuilder::into_array_iter`.
- Recorded public API-boundary blockers still prevent approval and therefore prevent scan
  invocation.
- Approved local fixture scan/count remains a narrow exception to the static broad API boundary, not
  a general adapter/source approval.
- This inventory update keeps non-fixture sources, object stores, encoded predicates, projections,
  writes, external baselines, and superiority claims out of scope.

## CG-2.1e.17 local fixture scan target consistency

- The feature-gated local fixture helper now requires approval target URI and encoded-read readiness
  source URI evidence to match before it can invoke `VortexFile::scan` or
  `ScanBuilder::into_array_iter`.
- Missing readiness source URI evidence or mismatched target evidence blocks before scan and keeps
  `data_read=false`, `upstream_scan_called=false`, and fallback disabled.
- The local fixture exception remains exact-source, approval-gated evidence only, not a general
  public scan/read-start approval.
- This inventory update keeps non-fixture sources, object stores, encoded predicates, projections,
  writes, external baselines, and superiority claims out of scope.

## CG-2.1e.18 local fixture scan source evidence reporting

- The feature-gated local fixture execution report now exposes scan target URI, readiness source
  URI, and source/target match status.
- These evidence fields are emitted for successful fixture count scans and for blocked approval,
  target mismatch, and object-store rejection reports.
- The evidence does not make scan/read-start broadly execution-usable; it documents the exact source
  authorization for the existing local fixture exception.
- This inventory update keeps non-fixture sources, object stores beyond deterministic rejection,
  encoded predicates, projections, writes, external baselines, and superiority claims out of scope.

## CG-2.1e.19 explicit local encoded-count execution boundary

- `vortex_encoded_read_local_scan_count_api_boundary` is a separate narrow boundary from
  `vortex_encoded_read_public_api_boundary`.
- Only `OpenOptionsSessionExt::open_path`, `VortexFile::scan`, and `ScanBuilder::into_array_iter`
  are execution-usable, and only for approved local `.vortex` `CountAll` scans that sum returned
  Vortex array lengths.
- The CLI opt-in path is `shardloom vortex-encoded-read-spike ... --execute-local-count`; the
  default spike path remains report/probe-only.
- The broad public API inventory remains conservative for general scan/read-start, adapters,
  object-store targets, encoded predicates, projections, writes, external baselines, and superiority
  claims.
- This inventory update does not approve row reads, requested decode/materialization, Arrow
  conversion, object-store IO, writes, spill IO, or fallback execution.

## CG-2.1e.20 approved local scan naming normalization

- The approved local count path now uses `local_scan` status, mode, report-field, diagnostic, and
  human-text names.
- This keeps the public inventory aligned with CG-2.1e.19: the path is local `.vortex` scan/count
  only, not checked-in fixture only.
- The layout-driver `local-fixture-only` signal remains historical and unchanged; it is not part of
  this approved local scan report surface.
- This inventory update does not approve broader scan/read-start use, adapters, object-store
  targets, encoded predicates, projections, row reads, requested decode/materialization, Arrow
  conversion, writes, spill IO, or fallback execution.

## CG-2.1e.21 approved local scan result bridge

- The approved local scan/count report is now accepted by local query-primitive execution only
  through `execute_vortex_count_all_from_approved_local_scan_result`.
- The bridge verifies the local scan report is the narrow local `.vortex` `CountAll` array-length
  path and that approval target URI, local scan target URI, and encoded-read readiness source URI
  all match.
- Local execution can report `local_encoded_count_executed` and a known count value only after the
  approved local scan report proves data was read by the local scan and proves no rows, requested
  decode/materialization, Arrow conversion, object-store IO, writes, spill IO, external effects, or
  fallback execution occurred.
- `shardloom vortex-encoded-read-spike ... --execute-local-count` now surfaces the bridged local
  execution fields for auditability.
- This inventory update does not approve broader scan/read-start use, adapters, object-store
  targets, encoded predicates, projections, row reads, requested decode/materialization, Arrow
  conversion, writes, spill IO, external baselines, or fallback execution.

## CG-2.1e.22 stable explicit local encoded count command

- `shardloom vortex-count <dataset_uri>` remains metadata-only by default.
- `shardloom vortex-count <dataset_uri> --execute-local-encoded-count <memory_gb> <max_parallelism>`
  now exposes the approved local `.vortex` `CountAll` scan/count path without routing users through
  the spike command.
- The stable command uses the same narrow public API boundary: `OpenOptionsSessionExt::open_path`,
  `VortexFile::scan`, and `ScanBuilder::into_array_iter` for local array-length counting only.
- It preserves target/source-match evidence, arrays-read and rows-counted evidence, no row reads, no
  requested decode/materialization, no Arrow conversion, no object-store IO, no writes, no spill IO,
  no external effects, and no fallback execution.
- Broader scan/read-start use, adapters, non-local sources, object-store targets, encoded
  predicates, projections, benchmarks, external baselines, and CG closeout remain out of scope.

## CG-2.2a filtered-count readiness core contract
- CG-2.1, CG-2.1a, and CG-2.1b are complete.
- CG-2.2a adds `VortexFilteredCountReadinessRequest` and `VortexFilteredCountReadinessReport`
  planning/reporting only.
- CG-2.2a.1 blocker precision helper update is complete: `filtered-count` + `PredicateProvided` maps
  to `EncodedPredicatePath` even when encoded-data-path readiness is missing; missing
  encoded-data-path reports `BlockedByMissingEncodedDataPath`; non-`filtered-count` primitives
  remain `Unknown`; metadata predicate-proof remains deferred to explicit proof contract.
- Distinguishes `VortexFilteredCountCandidateSource::MetadataPredicateProof` vs
  `::EncodedPredicatePath`.
- Metadata-proof filtered count remains explicit and opt-in via `PredicateMetadataProofReady`;
  CG-2.2c admits it to metadata-only local execution only when a matching `CountWhere` request and
  metadata summary are supplied.
- Encoded-predicate filtered-count execution is not implemented.
- No scan/read-start, predicate evaluation, encoded-data read, row read, decode, materialization,
  `Arrow` conversion, object-store IO, writes, or fallback execution are added.
- CG-2.2b CLI integration is complete via `shardloom vortex-filtered-count-readiness-plan
  <candidate_source> <dataset_uri> [flags] [--format text|json]`.
- Keep CG-1 through CG-20 visible; active status remains in `phased-execution-plan.md`.
- The command does not execute filtered count, does not evaluate predicates, does not call
  scan/read-start APIs, and performs no metadata/footer open, encoded-data read, row read,
  decode/materialization, `Arrow` conversion, object-store IO, writes, or fallback execution.
- Encoded-predicate filtered-count execution remains blocked until a real encoded predicate path
  exists.

## CG-2.2c filtered-count metadata proof local guard

- `execute_vortex_count_where_from_filtered_count_metadata_proof` accepts only
  `MetadataPredicateProof` readiness for matching `CountWhere` requests with metadata summaries.
- Metadata-proven predicates can return metadata-only count results from segment metadata through
  the local execution report, preserving no encoded-data read, no row read, no
  decode/materialization, and no fallback.
- Encoded-predicate candidates are rejected by this guard and remain future work.
- This adds no encoded predicate evaluation, scan/read-start invocation, encoded-data traversal, row
  read, decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO,
  external baseline invocation, or fallback execution.

## CG-2.2d filtered-count metadata proof report

- `VortexFilteredCountMetadataProofReport` classifies `CountWhere` plus a supplied metadata summary
  as `proof_ready`, `needs_encoded_predicate`, `missing_metadata`, or `unsupported`.
- Proof-ready reports carry the metadata-only count result and explicitly report no data read, no
  row read, no decode/materialization, no object-store IO, no write IO, and no fallback.
- Inconclusive metadata reports request encoded predicate evaluation without executing it.
- This adds no encoded predicate evaluation, scan/read-start invocation, encoded-data traversal, row
  read, decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO,
  external baseline invocation, or fallback execution.

## CG-2.3a projection readiness semantic hardening

- CG-2.2, CG-2.2a.1, and CG-2.2b are complete.
- CG-2.3a semantic hardening is complete.
- `ShardLoom` now provides projection-readiness planning/reporting contracts
  (`VortexProjectionReadinessRequest` and `VortexProjectionReadinessReport`) without projection
  execution.
- Projection-readiness distinguishes metadata/schema projection candidates from encoded-column
  projection candidates:
  - metadata/schema projection remains explicit and requires `ProjectionSupported` plus
    `MetadataFooterReady`;
  - encoded-column projection candidates require `EncodedDataPathReady`.
- The contract remains report-only: no scan/read-start, no projection application, no encoded-data
  reads, no row reads, no decode, no materialization, no `Arrow` conversion, no object-store `IO`,
  no writes, and no fallback execution.
- Keep CG-1 through CG-20 visible; active status remains in `phased-execution-plan.md`.

## CG-2.3b projection readiness CLI integration

- CG-2.3b CLI integration is complete via `shardloom vortex-projection-readiness-plan
  <candidate_source> <dataset_uri> [flags] [--format text|json]`.
- Candidate sources are `metadata-schema-projection`, `encoded-column-path`, and `unknown`.
- The command emits report-only text/JSON fields for readiness status, planning mode, projection
  readiness, candidate source, readiness signals, no-op effect fields, and
  `fallback_execution_allowed=false`.
- The command does not execute projection, apply projection, call scan/read-start APIs, read
  metadata/footer or encoded data, read rows, decode, materialize, convert to `Arrow`, perform
  object-store `IO`, write data, call upstream scans, or attempt fallback execution.
- Projection execution remains blocked until a real encoded projection path or explicit
  metadata/schema projection execution capability exists.

