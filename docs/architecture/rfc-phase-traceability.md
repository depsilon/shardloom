# RFC Phase Traceability

## Purpose

RFCs are ShardLoom's source-of-truth design documents, but they are not automatically enforced by code. The phased execution plan should explicitly reference the RFCs that govern each phase so phase work can remain aligned with approved architecture and acceptance criteria.

`docs/architecture/phased-execution-plan.md` is the source of truth for active status, active queue, completed phase ledger, deferred work, and CG closeout state. This document maps phase and CG work to governing RFCs; it may record historical traceability, but it must not introduce a competing active queue.

Status words in historical sections below describe evidence recorded at the time of the original phase note. They are not active queue state and do not override `phased-execution-plan.md`.

## How to use this document

- Before starting a new phase, check the mapped RFCs for that phase.
- Do not re-read every RFC for every PR.
- Do targeted RFC checks at phase boundaries.
- If implementation diverges from an RFC, either update the RFC, add an ADR/RFC amendment, or document the deviation.
- No fallback execution remains a global invariant.
- `docs/architecture/systems-learning-map.md` is conceptual reference material only and does not authorize dependencies or runtime fallback execution.

## Phase-to-RFC matrix

| Phase | Primary RFCs | Secondary RFCs | Must check before phase starts | Implementation notes |
| --- | --- | --- | --- | --- |
| R3.1 — repo cleanup backlog inventory and terminology/CLI audit (complete) | RFC 0012 Diagnostics, Explain, Estimate, and Capabilities; RFC 0024 Release Engineering, API Compatibility, and Packaging | RFC 0025 Competitive/no-fallback; RFC 0030 Universal API/baseline harness | docs/audit-only scope; no runtime behavior changes; no fallback execution; compatibility-preserving cleanup inventory only | Prepares future small cleanup PRs across CLI consistency, diagnostics normalization, terminology consolidation, feature-footprint/doctor centralization, and traceability drift. |
| R3.2 — CLI usage/name consistency cleanup (complete) | RFC 0012 Diagnostics, Explain, Estimate, and Capabilities; RFC 0024 Release Engineering, API Compatibility, and Packaging; RFC 0030 Universal API/baseline harness | RFC 0025 Competitive/no-fallback | docs/test/CLI compatibility cleanup only; no runtime behavior changes; no fallback execution | Verifies user-facing usage/help naming uses `shardloom`, reinforces plan/probe/write/execute distinction through focused CLI tests, and records command-registry work as backlog only. |
| R3.3 — diagnostics normalization backlog (complete) | RFC 0012 Diagnostics, Explain, Estimate, and Capabilities; RFC 0024 Release Engineering, API Compatibility, and Packaging; RFC 0030 Universal API/baseline harness | RFC 0025 Competitive/no-fallback | docs/audit-only scope; no runtime behavior; no fallback execution | Audits diagnostic normalization backlog across CLI parse/argument paths, ShardLoomError-to-Diagnostic conversion, category/status consistency, and future helper/test sequencing for targeted follow-up PRs. |
| Phase 10D — Local engine diagnostic propagation stabilization | RFC 0012 Diagnostics, Explain, Estimate, Capabilities; RFC 0018 Observability, Tracing, Profiling; RFC 0024 Release Engineering, API Compatibility, Packaging | — | stable diagnostic codes; JSON/human output compatibility; no generic diagnostics replacing root-cause diagnostics; no fallback execution | Stabilize diagnostics transport and schema compatibility for local engine surfaces before broader runtime expansion. |
| Phase 11A — Spill policy turns real (11A.1 lifecycle/cleanup contract; 11A.2 reservations/bounded memory integration; 11A.3 spill data movement; 11A.3a.2d roundtrip API complete; 11A.3a.3 CLI/docs integration recorded-active; 11A.3b bounded execution spill payload integration planned) | RFC 0014 Memory Management, Spill, and OOM Safety | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0008 Object-Store Runtime and Distributed Task Model | memory budgets; memory reservations; spill policies; spill file refs; cleanup expectations; deterministic fail-before-OOM; no fallback execution | Keep spill posture local/native and deterministic; do not add object-store spill during this phase. |
| Phase 11B.1 — Recovery context and cleanup planning integration (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety; RFC 0008 Object-Store Runtime and Distributed Task Model | task attempt identity; cleanup required; bounded spill recovery artifacts; deterministic report-only recovery planning; no fallback execution | Recovery context is planning-only and side-effect-free; classify known vs unknown cleanup artifacts explicitly. |
| Phase 11B.2 — Retry/cancellation planning integration (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety | retry eligibility; cancellation planning state; cleanup-before-retry gating; no fallback execution | Retry/cancellation outcomes remain explicit plans until execution phases. |
| Phase 11B.3a — Cleanup execution core contract, no filesystem (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety; RFC 0008 Object-Store Runtime and Distributed Task Model | cleanup execution contract; deterministic report semantics; no filesystem deletes; no fallback execution | Establishes explicit cleanup execution shape without performing cleanup side effects. |
| Phase 11B.3b — Feature-gated cleanup execution for exact synthetic spill payload files (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety; RFC 0008 Object-Store Runtime and Distributed Task Model | exact-file cleanup execution gates; `SpillPayloadFsRef` targeting; deterministic diagnostics; no fallback execution | Cleanup execution remains constrained to exact known synthetic payload files only. |
| Phase 11B.3c — Cleanup execution CLI/docs integration (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety | CLI/reporting integration for cleanup execution; deterministic machine-readable output; no fallback execution | CLI and docs integration exposes existing cleanup execution without adding new cleanup semantics. |
| Phase 11B.4a.2 — Retry gate report integration with cancellation/cleanup reports (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety | derived retry-gate planning signals from existing reports; side-effect-free planning; no fallback execution | Report-derived retry-gate planning is complete without executing retry, cleanup, or cancellation. |
| Phase 11B.4b — Retry gate CLI/docs integration (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety | explicit retry-gate CLI signal input; deterministic machine-readable output fields; no fallback execution | CLI/docs now expose retry-gate planning only; actual retry execution remains deferred. |
| Phase 11B.5a — cancellation execution gate core contract (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety; RFC 0008 Object-Store Runtime and Distributed Task Model | explicit cancellation-gate signal evaluation; planning/report-only behavior; deterministic blocked-state diagnostics; no fallback execution | Defines the cancellation gate core only; does not consume nested retry/cancellation or cleanup execution reports yet. |
| Phase 11B.5b — cancellation gate report integration (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety; RFC 0008 Object-Store Runtime and Distributed Task Model | derive cancellation-gate signals from retry/cancellation and cleanup reports; preserve side-effect-free planning; no fallback execution | Integrates actual retry/cancellation and cleanup reports into cancellation gate planning without enabling execution. |
| Phase 11B.5c — cancellation gate CLI/docs integration (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety | explicit `cancellation-gate-plan` signal input; deterministic machine-readable output fields; no fallback execution | CLI/docs expose cancellation-gate planning only; actual cancellation execution remains deferred. |
| Phase 11B.6 — recovery phase final audit before Phase 12 writes (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety; RFC 0008 Object-Store Runtime and Distributed Task Model | recovery/cleanup/retry/cancellation report coherence; stable machine-readable fields; synthetic-spill-only scope; no retry/cancellation execution; no object-store/output recovery execution; no fallback execution | Phase-boundary audit only; confirms planning/gate surfaces before Phase 12A write-intent planning. |

| Phase 12A.4 — staged output/write-readiness closeout before commit protocol (complete) | RFC 0005 Vortex-Native File IO and Output Contract | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0020 Schema Evolution, Catalog, Table Compatibility; RFC 0024 Release Engineering | staged workspace/marker/manifest-draft coherence; staged draft files are not committed manifests; output payload writes disabled; upstream Vortex write APIs deferred; object-store writes blocked; fallback disabled | Phase-boundary closeout only before Phase 12B commit protocol entry. |
| Phase 12B.1 — commit-intent core contract (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0024 Release Engineering | commit protocol starts report-only; deterministic commit-state diagnostics; no commit execution; no output payload writes; no fallback execution | Establish commit intent contract and diagnostics before any commit execution. |
| Phase 12B.1b — commit-intent readiness integration (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0024 Release Engineering | derive commit-intent readiness from staged manifest draft-file write, recovery integration, retry gate, and cancellation gate reports; missing/blocked gate reports remain explicit blockers; no commit execution; no fallback execution | Integrate report-derived readiness signals while preserving report-only commit behavior. |
| Phase 12B.1c — commit readiness integration validation closeout (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0024 Release Engineering | full workspace/feature validation matrix; side-effect-free readiness closeout; staged draft boundary preserved; machine-readable recovery/retry/cancellation blockers preserved; no commit execution; no fallback execution | Validate Phase 12B.1b integration before starting Phase 12B.2 commit protocol state machine work. |
| Phase 12B.3a — commit marker core contract (complete, report-only) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0024 Release Engineering | deterministic commit marker name/ref/content contract; explicit commit-marker readiness blockers; commit marker writes deferred; manifest finalization deferred; no committed manifest writes; no output writes; no fallback execution | Defines commit marker planning/reporting boundaries without filesystem/object-store side effects. |
| Phase 12B.3b — feature-gated local commit marker file (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0024 Release Engineering | writes only the exact local commit marker artifact under `vortex-staged-output-fs`; requires commit-marker feature-gate readiness; manifest finalization remains deferred; no committed manifest writes; no output payload writes; no upstream `Vortex` write API calls; no object-store IO; no fallback execution | Introduces the first commit-marker local artifact helper without commit protocol execution or manifest finalization. |
| Phase 12B.3c.1 — commit marker planning `CLI` (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0024 Release Engineering | `ShardLoom` `CLI` wraps `plan_vortex_commit_marker` for report-only planning; explicit feature-gate signal required for marker-ready planning; commit marker writes remain disabled in this command; manifest finalization remains deferred; no committed manifest writes; no output payload writes; no upstream `Vortex` write API calls; no object-store IO; no fallback execution | Adds machine-readable and human-readable planner surfacing without introducing side effects. |
| Phase 12B.3c.2 — commit marker write `CLI` (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0024 Release Engineering | `CLI` exposure for feature-gated local marker write helper with marker-plan feature-gate readiness required; manifest finalization remains deferred; no committed manifest writes; no output payload writes; no upstream `Vortex` write API calls; no object-store IO; no fallback execution | Completed follow-on to planning `CLI` integration that keeps commit protocol execution deferred. |
| Phase 12B.2 — commit protocol state machine (planned, report-only start) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0024 Release Engineering | deterministic commit protocol states and transitions; report-only state machine entry; commit execution still deferred; no fallback execution | Introduce commit protocol state machine contracts before any commit execution behavior. |
| Phase 12B.3c.3 — staged write-readiness smoke test includes commit marker artifact (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0024 Release Engineering | exercises staged workspace setup, staged marker write, staged manifest planning/write, commit intent/protocol planning, commit marker planning/write through `ShardLoom` `CLI`; verifies commit marker appears only after explicit write; no manifest finalization; no committed manifest writes; no output payload writes; no upstream `Vortex` write API calls; no object-store IO; no fallback execution | Completed staged-artifact closeout validation before Phase 12B.4. |
| Phase 12B.4 — commit protocol closeout before manifest finalization (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0024 Release Engineering | closeout audit for staged artifacts and commit-marker boundaries before manifest finalization starts; commit intent/protocol/marker contracts remain report-only where applicable; no fallback execution | Complete. |
| Phase 12B — Commit protocol and recovery | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0024 Release Engineering | commit states; rollback; cleanup; idempotency; ambiguous commit diagnostics; no fallback execution | Commit state machine and recovery behavior should be deterministic before broad writer expansion. |
| Phase 13A — Lakehouse table intelligence | RFC 0020 Schema Evolution, Catalog Integration, and Table Compatibility | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0019 Security | CG-9.1 schema-evolution compatibility report, CG-9.2 partition-evolution compatibility report, CG-9.3 delete/tombstone compatibility report, CG-9.4 aggregate table compatibility evidence, and CG-9.5 CDC incremental planning evidence complete; broader schema/partition/delete catalog-table metadata integration, snapshots/time travel, delete/tombstone execution, CDC execution, and metadata-loss propagation remain planned; no silent unsafe coercion; no fallback execution | Compatibility intelligence must remain explicit, typed, and safety-first. |
| Phase 13B — Layout health, clustering, compaction planning | RFC 0016 Optimizer, Adaptive Execution, Runtime Filters, Skew | RFC 0004 Manifest/Snapshot/Incremental; RFC 0014 Memory/Spill/OOM; RFC 0020 Table Compatibility | CG-9.6 layout-health report evidence and CG-9.7 compaction planning evidence complete for declared manifest metadata; small files/segments, missing stats/byte ranges, mixed formats/layouts/encodings, compaction recommendations, and future maintenance groups are report-only; no writes unless Phase 12 path is ready | Planning/reporting phase first; write-side changes must stay gated behind Phase 12 readiness. |
| Phase 14A — Object-store read planning | RFC 0008 Object-Store Runtime and Distributed Task Model | RFC 0017 Recovery; RFC 0014 Memory/Spill/OOM; RFC 0018 Observability | CG-10.1 object-store range planning and CG-10.2 request coalescing evidence complete for declared S3/GCS/ADLS byte ranges; metadata before reads; byte ranges before full files; request budgets; retry/backpressure policy; no object-store IO or writes yet; no fallback execution | Enable object-store read planning with bounded, diagnosable request behavior before distributed execution. |
| Phase 14A.3 — Object-store commit protocol planning evidence | RFC 0008 Object-Store Runtime and Distributed Task Model | RFC 0017 Recovery; RFC 0004 Manifest/Snapshot/Incremental; RFC 0028 Output Payloads/Commit/Lakehouse | CG-10.3 object-store commit protocol planning evidence records declared staging, manifest pointer, commit record, idempotency, cleanup, atomicity, object-store target, diagnostics, no-IO/no-write/no-fallback fields; commit execution, provider probes, recovery cleanup, and distributed coordination remain deferred | Define object-store commit readiness before object-store writes or distributed commit behavior. |
| Phase 14B — Distributed execution planning | RFC 0008 Object-Store Runtime and Distributed Task Model | RFC 0017 Recovery; RFC 0016 Optimizer/Adaptive; RFC 0018 Observability | CG-10.4 object-store distributed scheduling planning and CG-10.5 checkpoint/retry/idempotency planning evidence record task-shape grouping, task budgets, retry policy, checkpoint plan, idempotency keys, attempt records, cleanup policy, no coordinator/worker/task execution, no checkpoint writes, and no fallback; bounded distributed runtime remains planned | Distributed planning must preserve bounded resources and explicit retry/recovery semantics before runtime execution. |
| CG-11.1 — stable CLI/API JSON protocol foundation | RFC 0030 Universal API, Plan Portability, Import/Deployment, and External Baselines | RFC 0010 Developer Experience; RFC 0012 Diagnostics; RFC 0024 Release/API Compatibility | `CliApiJsonProtocolReport` records `OutputEnvelope` schema keys, command statuses, fallback fields, diagnostic fields, thin Python wrapper boundary, no PyO3/maturin, no Foundry requirement, no parser/runtime/probe/write/publish side effects, and no fallback execution | Establishes a stable report-only CLI JSON protocol surface for future clients before a Python package, native bindings, DataFrame API, or server/API runtime exists. |
| CG-11.2 — thin Python wrapper foundation | RFC 0030 Universal API, Plan Portability, Import/Deployment, and External Baselines | RFC 0010 Developer Experience; RFC 0012 Diagnostics; RFC 0032 Capability Surface | `PythonWrapperFoundationReport` records subprocess CLI JSON transport, initial command scope, client behavior requirements, package/native binding deferral, DataFrame/notebook/Python UDF deferral, materialization-boundary and diagnostic passthrough requirements, no probes, no parser/runtime/write/publish side effects, and no fallback execution | Establishes the first Python wrapper boundary as a future CLI JSON client contract while leaving mature Python API, packaging, notebook, DataFrame, and UDF certification to CG-20. |
| CG-12.1 — native-first plan portability report foundation | RFC 0030 Universal API, Plan Portability, Import/Deployment, and External Baselines; RFC 0022 Plan IR and Substrait-Compatible Interoperability | RFC 0010 Developer Experience; RFC 0012 Diagnostics; RFC 0024 Release/API Compatibility | `PlanPortabilityReport` records native-first direction, interop format, native plan schema version, supported/native-only/Substrait-like/lossy/unsupported/residual construct lists, metadata-loss boundaries, redaction requirement, parser/import/export/runtime/probe/read/write side-effect fields, and no fallback execution | Establishes CG-12 report-only evidence through `plan-ir`, `plan-import`, and `plan-export` without real serialization, external engine execution, filesystem/network probing, or fallback execution. |
| Ongoing — Expression and kernel engine | RFC 0021 Expression Engine and Kernel Registry | RFC 0015 Correctness, Semantics, Differential Testing, Fuzzing; RFC 0023 Extension/Plugin ABI and Sandboxing | metadata kernel; encoded kernel; partial-decode kernel; decoded reference kernel only as explicit reference/test path; deterministic kernel selection; effect boundaries; no hidden fallback | Keep kernel selection deterministic and no-fallback while preserving explicit reference-only decoded paths. |
| Ongoing — Release/API/agent stability | RFC 0024 Release Engineering, API Compatibility, Packaging | RFC 0012 Diagnostics; RFC 0018 Observability; RFC 0019 Security | CLI compatibility; JSON output compatibility; diagnostic schema stability; feature footprint; benchmark claim evidence; no fallback release check | Treat compatibility and diagnostics as continuous contracts, verified at every phase boundary. |

## RFC coverage status

Status categories:
- Implemented
- Partially implemented
- Planned
- Deferred
- Needs amendment

| RFC | RFC implementation status | Relevant phases | Notes |
| --- | --- | --- | --- |
| RFC 0001 | Partially implemented | 0-3, Ongoing | Foundational architecture and no-fallback direction established; ongoing operationalization across phases. |
| RFC 0002 | Partially implemented | 2-6, Ongoing | Core contract framing in place; implementation depth still increases by phase. |
| RFC 0003 | Partially implemented | 3-10C, Ongoing | Planning/runtime skeletons exist; deeper runtime behavior remains phased. |
| RFC 0004 | Partially implemented | 12A, 12B, 13A, 13B | Manifest/snapshot/incremental model present conceptually; CG-9.5 CDC incremental planning and CG-9.7 compaction planning evidence exist for declared metadata; advanced write/commit, CDC execution, and maintenance execution behavior remain planned. |
| RFC 0005 | Partially implemented | 12A, Ongoing | Vortex-native output contract is established; full staged write path remains planned. |
| RFC 0006 | Partially implemented | 5-10C, Ongoing | Compatibility translation contracts exist at architecture level; enforcement/reporting evolves with later phases. |
| RFC 0007 | Planned | 10B-14B | Deeper execution/runtime scaling specifics remain mostly future-phase work. |
| RFC 0008 | Partially implemented | 11A, 14A, 14A.3, 14B | CG-10.1 object-store range planning, CG-10.2 request coalescing, CG-10.3 commit protocol planning, CG-10.4 distributed scheduling planning, and CG-10.5 checkpoint/retry/idempotency planning evidence exist for declared byte ranges, commit signals, task shapes, and reliability evidence; object-store IO, retry execution, checkpoint writes, distributed runtime, and object-store commit execution remain planned. |
| RFC 0009 | Partially implemented | 2-10C, Ongoing | Core policy scaffolding exists; deeper behavior and tooling continue to mature. |
| RFC 0010 | Partially implemented | 10C, 10D, Ongoing | Developer/agent usability direction set; stable interfaces continue across release phases. |
| RFC 0011 | Deferred | Ongoing (post-core) | Modular extensibility remains intentionally deferred relative to core engine phases. |
| RFC 0012 | Partially implemented | 10C, 10D, Ongoing | Diagnostics contracts exist; stabilization and propagation are explicit upcoming checkpoints. |
| RFC 0013 | Planned | 13B+, Ongoing | Streaming/zero-copy boundary work remains mostly future-phase effort. |
| RFC 0014 | Partially implemented | 10B, 11A, 11B, 13B, 14A, CG-14 | Memory/spill/OOM policies are partially scaffolded; CG-14.1 adaptive memory boundary evidence exists, but allocator/reservation runtime and spill execution remain planned. |
| RFC 0015 | Partially implemented | Ongoing | Correctness-first posture present; deeper differential/fuzz coverage continues over time. |
| RFC 0016 | Partially implemented | 13B, 14B, CG-14, Ongoing | CG-9.6 layout-health planning, CG-9.7 compaction planning, and CG-14.1 adaptive optimizer/memory decision evidence exist; runtime adaptation, runtime filter application, and advanced optimizer behavior remain later-phase work. |
| RFC 0017 | Planned | 11A, 11B, 12B, 14A, 14B | Recovery/cancellation/commit robustness is a remaining implementation focus. |
| RFC 0018 | Partially implemented | 10D, 14A, 14B, Ongoing | Observability foundations exist; richer tracing/profiling is still phased. |
| RFC 0019 | Partially implemented | 11B, 13A, Ongoing | Security/governance guardrails exist; advanced phase-specific controls remain planned. |
| RFC 0020 | Partially implemented | 12A, 13A, 13B | CG-9.1 schema-evolution, CG-9.2 partition-evolution, CG-9.3 delete/tombstone, CG-9.4 aggregate table compatibility, CG-9.5 CDC incremental planning, CG-9.6 layout-health dependency, and CG-9.7 compaction planning dependency evidence exist for typed transitions; broader catalog/table metadata integration, delete/tombstone execution, CDC execution, compaction execution, and layout/compaction integration remain planned. |
| RFC 0021 | Partially implemented | Ongoing | Expression/kernel architecture exists in principle; full kernel coverage remains ongoing. |
| RFC 0022 | Deferred | Ongoing (interop track) | Plan interoperability direction is documented; implementation remains intentionally staged. |
| RFC 0023 | Deferred | Ongoing (extension track) | Extension/plugin ABI and sandboxing are documented but not a near-term core phase focus. |
| RFC 0024 | Partially implemented | 10D, 12A, 12B, Ongoing | Release/API compatibility policy exists; continues as a cross-phase enforcement concern. |
| RFC 0025 | Planned | CG-1 through CG-20, Ongoing | Competitive Engine Track policy is documented; implementation remains gate-specific and evidence-gated. |
| RFC 0026 | Partially implemented | CG-1, CG-2, CG-13 | Encoded-read and query-primitive readiness contracts exist; CG-13.1 encoded path selection evidence exists for count/filter/project candidates; real generalized encoded execution remains gated. |
| RFC 0027 | Partially implemented | CG-7, CG-8, CG-14, CG-15 | CG-14.1 adaptive optimizer/memory decision evidence and CG-15.1 CPU specialization report evidence exist; runtime adaptivity, CPU probing, SIMD dispatch, and specialized kernel execution remain planned. |
| RFC 0028 | Partially implemented | CG-3, CG-4, CG-9, CG-10 | Output/commit readiness contracts exist; first native count-result payload path is complete; first local committed-manifest execution path is complete; local committed-manifest recovery diagnostics and first local rollback cleanup path are complete; broader payloads, generalized recovery, table/catalog commits, and object-store commits remain incomplete. |
| RFC 0029 | Partially implemented | CG-5, CG-6, CG-16, CG-17 | CG-16.1 local encoded count certificate, CG-16.2 execution-certificate evidence surface, and CG-17.1 stateful reuse boundary report exist; broader correctness, benchmark, certificate, cache read/write/replay, and incremental execution evidence remain future gate work. |
| RFC 0030 | Partially implemented | CG-11, CG-12, CG-18 | CG-11.1 stable CLI/API JSON protocol foundation and CG-11.2 thin Python wrapper foundation exist through `CliApiJsonProtocolReport`, `PythonWrapperFoundationReport`, `api-compat-plan`, and `python-wrapper-plan`; CG-12.1 plan portability report foundation exists through `PlanPortabilityReport`, `plan-ir`, `plan-import`, and `plan-export`; real plan serialization/import/export, deployment/import, and baseline harness work remain staged. |
| RFC 0031 | Planned | CG-19 | Universal Native I/O Envelope is RFC-level only; implementation pending. |
| RFC 0032 | Planned | CG-20 | Capability certification surface is RFC-level only; implementation pending. |

## Drift policy

- If a phase needs behavior not covered by an RFC, add a small RFC amendment or ADR.
- If implementation contradicts an RFC, stop and document the decision before merging.
- If an RFC is too broad but still directionally right, reference the relevant acceptance criteria only.

## No-fallback invariant

- All phases must preserve no Spark, DataFusion, DuckDB, Polars, Velox, or fallback engine execution.
- Compatibility formats are inputs/outputs, not fallback engines.
- Unsupported behavior must fail explicitly.


## Phase 12A refinement

- 12A.2a staged output workspace core contract was recorded as active and report-only.
- 12A.2b feature-gated local staged workspace/marker behavior remains planned.
- Output payload and manifest writes remain deferred.

- 12A.1 native `Vortex` write intent core contract was recorded as active.
- 12A.2 staged output workspace contract is planned.
- Actual write execution remains deferred.
## Phase 12A.3a update
- Phase 12A.2c.2 complete.
- Phase 12A.3a recorded-active: staged manifest draft core contract (report-only, no filesystem).
- Phase 12A.3b planned: feature-gated local staged manifest draft file.
- Phase 12A.3c planned: CLI/docs integration.
- Actual output payload and file writes remain deferred.

## Phase 12 refinement

- Phase 12A closeout is complete at Phase 12A.4.
- Phase 12B.1 commit-intent core contract is complete.
- Phase 12B.1b commit readiness integration is complete.
- Phase 12B.1c validation closeout is complete.
- Phase 12B.2a.1 commit protocol state machine core contract is complete and report-only.
- Phase 12B.2a.2 commit intent report integration was recorded as active in the historical ledger.
- Commit protocol remains report-only and commit execution remains deferred.
- Commit execution remains deferred.
- Commit protocol must start report-only.
- Commit execution remains deferred.


## Competitive roadmap traceability additions

- CG-1 through CG-20 are **Competitive Engine Track** success gates and roadmap tracks.
- CG gates are not aliases for canonical implementation phase IDs (for example, they are distinct from Phase 12/13/14 implementation phases).
- Spark/Polars/DataFusion (and other external engines) are future external baseline references only.
- External engines are never runtime fallback or delegation targets.
- No fallback execution remains mandatory across all CG gates.

Competitive gate coverage:
- CG-1: encoded read boundary and real encoded reads
  - CG-1.1a encoded read boundary core contract: complete
  - CG-1.1b encoded read boundary `CLI`/docs integration: complete
  - CG-1.2a metadata/footer probe planning contract: complete
  - CG-1.2a.1 encoded-read plan diagnostics/report fields: complete
  - CG-1.2a.2 feature-gated readiness/report validation: complete
  - CG-1.2b metadata probe fixture/report integration: complete
  - CG-1.2b.1 metadata probe stability/contract closeout: complete
  - CG-1.2c metadata probe `CLI`/docs integration: complete
  - CG-1.2d.2 deterministic async/session boundary contract: recorded-active (report-only; no runtime/executor added; metadata/footer invocation deferred to CG-1.2d.3)
    - primary RFC: RFC 0026
    - secondary RFCs: RFC 0012, RFC 0016, RFC 0025, RFC 0027, RFC 0029
    - constraints: no scan/read-start, decode, materialization, Arrow conversion, object-store IO, or fallback
- CG-2: real query primitive execution over Vortex data
- CG-3: output payload write path (placeholder artifact phases support readiness only; first real local count-result Vortex payload path complete; broader payloads deferred)
- CG-4: commit protocol execution
- CG-5: correctness/differential harness
- CG-6: benchmark harness
- CG-7: physical operators/kernels
- CG-8: streaming/parallel/adaptive execution
- CG-9: lakehouse/table intelligence
- CG-10: object-store/distributed execution
- CG-11: Python/API foundation surface later
  - CG-11.1 stable CLI/API JSON protocol foundation: complete
    - primary RFC: RFC 0030
    - secondary RFCs: RFC 0010, RFC 0012, RFC 0024
    - constraints: no Python package, PyO3/maturin, DataFrame API, parser/runtime execution, filesystem/network/catalog/adapter probing, writes, package publication, or fallback
  - CG-11.2 thin Python wrapper foundation: complete
    - primary RFC: RFC 0030
    - secondary RFCs: RFC 0010, RFC 0012, RFC 0032
    - constraints: subprocess CLI JSON contract only; no Python package, native bindings, DataFrame/notebook/Python UDF runtime, parser/runtime execution, probes, writes, package publication, or fallback
- CG-12: plan portability / semantic IR
- CG-13: encoded-native compressed execution
  - CG-13.1 encoded path selection report foundation: complete
    - primary RFC: RFC 0026
    - secondary RFCs: RFC 0012, RFC 0015, RFC 0021, RFC 0025, RFC 0029, RFC 0031, RFC 0032
    - constraints: report-only path selection; no generalized encoded execution, parser, SQL execution, adapter runtime, scan/read-start API, encoded-data read, decode, materialization, Arrow conversion, object-store IO, writes, spill IO, external engine execution, production/superiority claim, or fallback
- CG-14: runtime-adaptive optimizer and execution memory
  - CG-14.1 adaptive optimizer and memory decision report foundation: complete
    - primary RFCs: RFC 0016 and RFC 0014
    - secondary RFCs: RFC 0012, RFC 0013, RFC 0015, RFC 0021, RFC 0025, RFC 0027, RFC 0029, RFC 0031, RFC 0032
    - constraints: report-only optimizer/memory decision evidence; no optimizer execution, runtime adaptation application, runtime filter build/apply, dynamic pruning execution, plan rewrite, join/aggregate/skew execution, memory allocator/reservation runtime, spill execution, object-store IO, writes, production/superiority claim, or fallback
- CG-15: CPU operator specialization
- CG-16: evidence-first execution certificates
- CG-17: stateful result reuse / incremental execution
- CG-18: universal import/deployment/baseline harness
- CG-19: universal native I/O envelope
- CG-20: world-class SQL/operator/function/adapter/user capability surface

| Phase 12B.5a — manifest finalization core contract (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental | report-only finalization contract distinguishing staged draft manifest, finalized manifest candidate, and committed manifest; requires draft/marker/protocol/schema/delete/tombstone readiness signals; no finalized manifest writes; no committed manifest writes; no output payload writes; no object-store IO; no upstream `Vortex` write API calls; no fallback execution | Complete. Finalized manifest file writing and manifest commit remain deferred. |

| Phase 12B.5b — feature-gated local finalized-manifest candidate artifact (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental | local-only finalized-manifest candidate artifact write behind `vortex-staged-output-fs`; writes exact `VortexFinalizedManifestFileRef` path only; candidate artifact is not committed manifest state; no output payload writes; no upstream `Vortex` write APIs; no object-store IO; no fallback execution; manifest commit deferred | Complete. |

| Phase 12B.6 — local commit execution gate (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental | report-only local commit execution gate over commit protocol/marker/manifest-finalization/finalized-manifest-candidate/output-payload/feature-gate signals; commit execution deferred; output payload path deferred to Phase 12C (CG-3); commit protocol execution deferred to Phase 12D (CG-4); no committed manifest writes; no object-store IO; no upstream `Vortex` write API calls; no fallback execution | Complete. |
| Phase 12C.1 — output payload write contract (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental | report-only `VortexOutputPayloadReport`/`VortexOutputPayloadRequest` contract for payload identity, content descriptor, blockers, side-effect flags, and readiness signaling; no payload file writes; no `Vortex` file writes; no upstream `Vortex` write APIs; no object-store IO; no commit execution; no fallback execution | Starts CG-3 with explicit readiness diagnostics before any write execution. |
| Phase 12C.2 — feature-gated local output payload artifact (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0004 Native Dataset Manifest, Snapshot, Incremental | feature-gated local payload artifact writing only after 12C.1 readiness contracts; commit execution remains deferred | Complete. |

| Phase 12C.3a — output payload plan CLI (complete) | RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental | CLI report-only output payload readiness planning; no artifact writes; no real `Vortex` payload writes; upstream `Vortex` write APIs remain deferred | Complete. |
| Phase 12C.3b — output payload artifact write CLI (complete) | RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental | CLI for local placeholder output payload artifact writes; default build remains feature-disabled/report-only; no real upstream `Vortex` payload writes; no manifest writes/commit execution/object-store IO | Complete readiness-only milestone. |
| Phase 12C.4 — staged smoke test includes output payload artifact (complete readiness-only) | RFC 0015 Correctness, Semantics, Differential Testing, and Fuzzing | RFC 0004 Native Dataset Manifest, Snapshot, Incremental | Extends staged CLI-driven write-readiness smoke coverage with output payload plan and placeholder artifact write; verifies no real `Vortex` payload writes, no upstream `Vortex` write API calls, no manifest/commit writes, no object-store IO, fallback disabled; this is CG-3 readiness evidence only and does not complete CG-3 | Complete readiness-only milestone; not the active implementation phase. |
| Phase 12C.5 / CG-3.1 — native count output payload write (complete) | RFC 0005 Vortex-Native File IO and Output Contract; RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0017 Fault Tolerance, Cancellation, and Recovery | feature-gated local native `Vortex` output payload write for a known `CountAll` result; writes a one-row `u64` `.vortex` payload through upstream `Vortex` writer APIs only under `vortex-write`; default builds remain report-only/feature-disabled; no manifest writes, no manifest commits, no object-store IO, no generalized output writes, and no fallback execution | Provides the first real CG-3 payload path while leaving CG-4 commit execution and broader output support deferred. |
| Phase 12D.1 / CG-4.1 — local committed-manifest execution (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0005 Vortex-Native File IO and Output Contract | feature-gated local commit execution copies `_shardloom_finalized_manifest.json` into `_shardloom_committed_manifest.json` only after commit protocol, finalized-manifest, commit-marker, output-payload, local-workspace, and feature-gate evidence; identical existing committed manifest is idempotent; differing existing committed manifest is ambiguous/blocked; no object-store IO, output payload write, upstream `Vortex` commit API, recovery execution, rollback execution, or fallback execution | Provides the first CG-4 local commit path while leaving recovery, rollback, table/catalog transaction, distributed, and object-store commit paths deferred. |
| Phase 12D.2 / CG-4.2 — local commit recovery/rollback diagnostics (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0028 Output Payloads, Finalization, Commit, and Lakehouse Semantics | report-only local committed-manifest recovery planning records recovery-not-required, rollback-required, rollback-planned, ambiguous commit, missing committed-manifest, cleanup-policy, and object-store blockers; emits `RecoveryPlan` cleanup targets and ambiguous commit records; no cleanup deletion, rollback execution, object-store IO, upstream `Vortex` commit API, retry execution, or fallback execution | Provides CG-4 rollback and ambiguous-commit diagnostics while leaving rollback cleanup execution and broader recovery deferred. |
| Phase 12D.3 / CG-4.3 — local committed-manifest rollback cleanup execution (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0012 Diagnostics, Explain, Estimate, and Capabilities | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0028 Output Payloads, Finalization, Commit, and Lakehouse Semantics | feature-gated local rollback cleanup execution consumes rollback-planned recovery evidence and removes only `_shardloom_committed_manifest.json`; default builds remain feature-disabled/report-only; finalized-manifest, commit-marker, and output-payload artifacts are preserved; no object-store IO, upstream `Vortex` commit API, generalized recovery manager, retry execution, or fallback execution | Provides the first CG-4 local rollback cleanup path while leaving generalized recovery, table/catalog transaction recovery, distributed, and object-store commit paths deferred. |


## Competitive Engine Track RFC mappings

CG items are competitive success gates, not implementation phase aliases.
External engines are baselines only.
No fallback execution.

| RFC | Competitive gates covered |
| --- | --- |
| RFC 0025 | CG-1 through CG-20 |
| RFC 0026 | CG-1, CG-2, CG-13 |
| RFC 0027 | CG-7, CG-8, CG-14, CG-15 |
| RFC 0028 | CG-3, CG-4, CG-9, CG-10 |
| RFC 0029 | CG-5, CG-6, CG-16, CG-17 |
| RFC 0030 | CG-11, CG-12, CG-18 |
| RFC 0031 | CG-19 |
| RFC 0032 | CG-20 |

- Phase 12C placeholder output payload artifact work supports CG-3 readiness only; it does not complete CG-3 by itself.
- CG-3.1 adds the first real feature-gated local native `Vortex` payload write path for a known `CountAll` result; broader payload shapes, commits, and object-store writes remain separate work.
- RFC 0026 supports CG-1.1 encoded read boundary sequencing.
- Competitive claims still require CG-5 correctness and CG-6 benchmarks before any “beats Spark/Polars/DataFusion” statement.




## R3 cleanup traceability

| Cleanup phase | Scope | Primary RFCs | Notes |
| --- | --- | --- | --- |
| R3.3a | CLI missing/unknown argument diagnostic helpers | RFC 0012, RFC 0024, RFC 0030 | Helper/test cleanup only; no broad diagnostics migration; no runtime behavior change; no fallback execution. |
| R3.3b | Unknown signal diagnostic normalization | RFC 0012, RFC 0024, RFC 0030 | Narrow helper/parser cleanup only; no broad diagnostics migration; no runtime behavior change; no fallback execution. |
| R3.3c | Output envelope command-status derivation audit | RFC 0012, RFC 0024, RFC 0030 | Output-envelope audit/tests only; no broad diagnostics migration; no runtime behavior change; no fallback execution. |
| R3.4 | Terminology consolidation backlog | RFC 0012, RFC 0013, RFC 0014, RFC 0016, RFC 0022, RFC 0024 | docs/audit only; mapping-helper backlog only; no public type renames; no runtime behavior; no fallback execution. |
| R3.5 | Feature-footprint/doctor centralization plan | RFC 0012, RFC 0018, RFC 0024, RFC 0025, RFC 0030 | docs/audit only; feature-footprint report implementation deferred; doctor/capabilities behavior unchanged; no runtime behavior; no fallback execution. |
| R3.5a | `FeatureFootprintReport` core contract | RFC 0012, RFC 0018, RFC 0024, RFC 0025, RFC 0030 | core report contract only; no probing; no `doctor`/`capabilities` behavior change; no dependency scanning; no runtime behavior; no fallback execution. |
| R3.5d | no-fallback dependency invariant tests | RFC 0024, RFC 0025, RFC 0030 | manifest/lockfile invariant tests only; no docs scan for conceptual references; no runtime behavior; no fallback execution. |


### CG-1.2d.3 update
- Added feature-gated async metadata/footer invocation surface for caller-provided async context only.
- No runtime/executor dependency was added by `ShardLoom`.
- Sync `VortexEncodedReadMetadataProbeReport::from_request` path remains report-only/no-IO.
- Async surface preserves no scan/read-start, no encoded-data reads, no decode/materialization, no `Arrow` conversion, no object-store IO, no writes, and no fallback execution.
- At this phase, actual public upstream `Vortex` metadata/footer invocation remained blocked by compile-unclear API shape; CG-1.2d.9 supersedes that blocker for the approved local fixture path.


- CG-1.2d.5 (complete): method-shape compile probes confirm public method items for `OpenOptionsSessionExt`, `VortexOpenOptions`, and `VortexFile::footer` without invocation; metadata/footer invocation remained deferred and deterministically blocked in that phase without runtime/executor wiring.

- CG-1.2d.6 (complete): caller-provided `VortexSession` invocation contract is added under `vortex-file-io` and open-method compile probing now includes `VortexOpenOptions::open_path` method-item reference; production invocation remained deterministically blocked in that phase; CG-1.2d.8 confirmed test harness ingredient limits before CG-1.2d.9 added the checked-in local fixture path.
- CG-1 through CG-20 remain active Competitive Engine Track gates; this update is CG-1.2d scope only and does not change other gate statuses.

### CG-1.2d.9 local metadata/footer invocation path
- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, and RFC 0025 Competitive/no-fallback.
- `invoke_vortex_metadata_footer_probe_with_session_async` now performs a feature-gated local `VortexOpenOptions::open_path` / `VortexFile::footer` metadata/footer invocation when the caller provides a `VortexSession` and the boundary report is `BoundaryReady`.
- A checked-in local `.vortex` fixture with provenance supplies deterministic metadata/footer open evidence for the harness.
- The invocation records only `metadata_opened` and `footer_inspected` effects; it does not call scan/read-start APIs, read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write data, or attempt fallback execution.
- Default builds, sync report paths, and non-session helpers remain report-only/deferred.
- CG-1.2d metadata/footer execution is no longer blocked for the local feature-gated fixture path; CG-1 closeout still requires an encoded data path beyond metadata/footer inspection.
- CG-1 through CG-20 remain active Competitive Engine Track gates.

## Test-only async metadata/footer harness policy

- Test-only async execution is allowed only in feature-gated tests.
- It must not affect production/default runtime behavior.
- It must not add fallback execution.
- It must not call scan/read-start/decode/materialization/`Arrow`/object-store/write APIs.
- A dev-dependency executor is allowed only when already present in `Cargo.lock` through the `Vortex` feature graph and when adding it introduces no new lockfile packages.
- A checked-in local `.vortex` fixture is allowed only with explicit provenance and only for metadata/footer open tests.
- Fixture generation using `Vortex` write APIs is not allowed in this phase.


## CG-2.0 query primitive boundary update
- CG-1.2 metadata/footer execution was paused after CG-1.2d.8; CG-1.2d.9 clears the local fixture metadata/footer invocation blocker but does not add query primitive execution.
- Historical evidence: CG-2.0 added a report-only, feature-gated `Vortex` query primitive readiness boundary for count, filtered count, projection, and predicate/filter primitives.
- This boundary does not execute query primitives and remains side-effect-free.
- CG-2.1c clears metadata-footer `CountAll` execution; encoded-data-path readiness is still required for non-metadata candidates.
- No scan/read-start, encoded data reads, row reads, decode/materialization, `Arrow` conversion, object-store `IO`, writes, or fallback execution are introduced.
- CG-1 through CG-20 remain visible and active competitive gates.

## CG-2.0b helper-correctness traceability update
- CG-2.0b closes helper correctness gaps for invocation-derived query primitive requests by preserving boundary blockers/signals and preventing misclassification as `feature_disabled` when a stronger blocker exists.
- `VortexQueryPrimitiveReport::has_errors` now treats report diagnostics with `Error`/`Fatal` severity as errors in addition to status/request diagnostics.
- This remains report-only readiness planning and introduces no execution side effects.
- CLI query primitive planning command is deferred to CG-2.0c.
- CG-2.1 execution remains blocked pending query wiring and encoded data path readiness.

## CG-2.0c query primitive plan CLI integration
- Adds `shardloom vortex-query-primitive-plan <primitive> <dataset_uri> [flags] [--format text|json]` as a report-only/readiness-only planning command.
- Command constructs `VortexQueryPrimitiveRequest` and calls `plan_vortex_query_primitive` only; it does not execute query primitives.
- Command does not call scan/read-start APIs, does not read encoded data or rows, does not decode/materialize/Arrow-convert, does not perform object-store IO, does not write output payloads, and does not allow fallback execution.
- CG-2.1+ actual non-metadata count/query execution remains blocked until encoded-data readiness exists for non-metadata candidates.


| CG-1.3 - encoded-read no-materialization / no-`Arrow` invariant evidence (complete for recorded contract surfaces) | RFC 0025 Competitive/no-fallback; RFC 0026 `Vortex` encoded-read/query-readiness boundaries | RFC 0015 Correctness/testing | Keep report-contract only outside the feature-gated CG-1.2d.9 local metadata/footer invocation; no scan/read-start; no decode/materialization/`Arrow` conversion; no object-store IO/writes; no fallback execution | Records invariant evidence for no broad row materialization and no `Arrow`-default conversion across recorded report surfaces; CG-1.2d.9 clears local metadata/footer invocation; CG-2.1 execution remains blocked pending query wiring and encoded data path readiness. |


## CG-2.1 count readiness planning update

- CG-1.3 invariant contract tests are complete.
- CG-2.0 / CG-2.0b / CG-2.0c / CG-2.0c.1 are complete.
- Historical evidence: CG-2.1 added a report-only `VortexCountReadinessRequest`/`VortexCountReadinessReport` planning contract.
- Count planning distinguishes metadata-footer candidates from encoded-data-path candidates.
- Metadata-footer `CountAll` execution is now wired through CG-2.1c; encoded-data count candidates can be approved and deferred through CG-2.1d.
- No scan/read-start, encoded-data reads, row reads, decode, materialization, `Arrow` conversion, object-store IO, writes, or fallback execution are introduced.
- CG-2.1b `CLI` surfacing is complete via `shardloom vortex-count-readiness-plan <candidate_source> <dataset_uri> [flags] [--format text|json]`.
- CG-2.1a semantic hardening is complete: `VortexCountCandidateSource::Unknown` cannot be readiness-complete and deterministically returns `blocked_by_unsupported_primitive` when feature-gated count/query-primitive-ready signals are present.
- `VortexCountReadinessReport` error detection is severity-aware across status, request diagnostics, and report diagnostics.
- Count readiness remains report-only and does not execute count.
- `CLI` output remains report-only/readiness-only and never executes count.
- No scan/read-start, encoded-data read, row read, decode, materialization, `Arrow` conversion, object-store `IO`, writes, or fallback execution are introduced.

## CG-2.1c metadata-footer CountAll execution bridge

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexMetadataAsyncInvocationReport` now carries a typed `VortexMetadataSummaryReport` when the feature-gated local footer invocation succeeds.
- `Count` query primitive readiness treats metadata-footer readiness as sufficient for metadata-only `CountAll`; encoded-data-path readiness remains required for non-metadata primitives.
- `execute_vortex_count_all_from_metadata_footer_invocation` consumes the typed summary and returns a metadata-only local execution result.
- The checked-in `metadata_footer_u64_20000.vortex` fixture proves `Count(20000)` from actual Vortex footer metadata.
- This does not call scan/read-start APIs, traverse encoded data, read rows, decode/materialize values, convert to `Arrow`, perform object-store IO, write data, or attempt fallback execution.
- CG-2 closeout still requires non-metadata count, filtered-count, projection, and encoded-data execution paths.

## CG-2.1d encoded-data CountAll candidate bridge

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `count_readiness_request_from_encoded_read_readiness_report` can promote a side-effect-free `VortexEncodedReadReadinessReport` with future encoded-read candidates into a `VortexCountCandidateSource::EncodedDataPath` request.
- `execute_vortex_count_all_from_encoded_data_candidate` accepts only count-ready encoded-data candidates and returns a deferred `NeedsEncodedRead` local execution report.
- This bridge does not execute the encoded read, does not call scan/read-start APIs, does not traverse encoded data, does not read rows, does not decode/materialize values, does not convert to `Arrow`, does not perform object-store IO or writes, and does not attempt fallback execution.
- CG-2 closeout still requires actual native encoded count execution plus filtered-count and projection execution over real Vortex data.

## CG-2.1e.1 encoded-data CountAll API-gated blocker

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `count_readiness_request_from_encoded_read_probe_report` consumes `VortexEncodedReadProbeReport` so encoded-data count readiness is gated by the public encoded-read API boundary, not only scheduler/readiness candidates.
- Recorded upstream public surfaces for data access remain blocked for actual count execution because they route through scan/data-read or array-stream/evaluation APIs that are not yet approved under ShardLoom's no-decode/no-materialization boundary.
- API boundary blockers propagate into count readiness as deterministic object-store, scan-execution, decode, materialization, Arrow-default, or write blockers.
- This pass does not execute encoded reads, call scan/read-start APIs, traverse encoded data, read rows, decode/materialize values, convert to `Arrow`, perform object-store IO or writes, or attempt fallback execution.
- CG-2.1e actual encoded-data count execution remains planned and blocked until a safe public Vortex data path is approved.

## CG-2.1e.2 exact Vortex data-access API classification

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- The encoded-read API boundary now names the exact upstream public surfaces reviewed for the next execution decision: `VortexFile::layout_reader`, `LayoutReader::row_count`, `VortexFile::scan`, `ScanBuilder::into_array_stream`, `ScanBuilder::into_array_iter`, `LayoutReader::projection_evaluation`, `LayoutReader::filter_evaluation`, and `VortexFile::data_source`.
- A feature-gated compile probe references the public Vortex method items without invoking them, preserving version compatibility evidence while keeping the default runtime side-effect-free.
- `LayoutReader::row_count` is classified as metadata-like layout access and remains not execution-usable by itself.
- Scan, array-stream, layout-evaluation, and data-source surfaces remain blocked or deferred until ShardLoom can prove no row reads, decode/materialization, `Arrow` conversion, object-store IO, writes, or fallback execution.
- CG-2.1e actual encoded-data count execution remains planned and blocked until one of these public surfaces, or an upstream-supported alternative, is approved as no-decode/no-materialization safe for ShardLoom-native count execution.

## CG-2.1e.3 named count API-boundary blockers

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `count_readiness_request_from_encoded_read_probe_report` now copies named blocked API-boundary summaries from `VortexEncodedReadProbeReport` into count readiness.
- The count-readiness boundary can now expose exact blockers such as `VortexFile::scan`, `ScanBuilder::into_array_stream`, `ScanBuilder::into_array_iter`, `LayoutReader::projection_evaluation`, `LayoutReader::filter_evaluation`, and `VortexFile::data_source`.
- Metadata-like `LayoutReader::row_count` is intentionally not carried as an execution blocker.
- This is still report metadata only: no scan/read-start invocation, array stream/evaluation call, encoded-data traversal, row read, decode/materialization, `Arrow` conversion, object-store IO, write, or fallback execution is introduced.

## CG-2.1e.4 encoded-count admission blocker guard

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- Named API-boundary blockers now participate in readiness derivation: a request with any blocker cannot produce `CountReady`, even when `EncodedDataPathReady` is present.
- `execute_vortex_count_all_from_encoded_data_candidate` also rejects readiness reports that still carry named API-boundary blockers.
- This closes the admission gap before actual encoded-count execution: exact blocked Vortex surfaces must be removed or approved before any execution helper can advance.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data, read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt fallback execution.

## CG-2.1e.5 `VortexFile::row_count` metadata-surface approval

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexFile::row_count` is now compile-checked and classified as a confirmed public metadata-only surface because upstream Vortex implements it as a footer row-count wrapper.
- This approval is intentionally narrower than encoded-data execution: `VortexFile::row_count` is contract-usable but still not execution-usable under the encoded-read API boundary.
- `LayoutReader::row_count` remains metadata-like but deferred because constructing layout readers is not yet an approved count execution path.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data, read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt fallback execution.

## CG-2.1e.6 encoded-count data-path approval boundary

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexEncodedCountDataPathApprovalReport` now consumes `VortexCountReadinessReport` and `VortexEncodedReadApiBoundaryReport` to decide whether encoded-data `CountAll` can even be approved for deferred execution planning.
- The recorded public API boundary remains blocked: `VortexFile::row_count` is metadata count evidence, but execution-usable data path count is zero and scan/stream/evaluation/data-source surfaces remain blocked or deferred.
- This pass makes the remaining blocker explicit before actual encoded-data count execution work.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data, read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt fallback execution.

## CG-2.1e.7 encoded-count approval CLI surfacing

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `shardloom vortex-encoded-count-approval-plan` now surfaces `VortexEncodedCountDataPathApprovalReport` in text/JSON CLI envelopes.
- The command is report-only: recorded public API blockers remain visible and ready encoded-data count inputs return deterministic unsupported/non-zero status until an execution-usable data path exists.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data, read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt fallback execution.

## CG-2.1e.8 encoded-count approval local guard

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `execute_vortex_count_all_from_encoded_count_data_path_approval` now requires `VortexEncodedCountDataPathApprovalReport` before local encoded-count planning can advance.
- The recorded public API boundary is rejected by this guard; a future approved boundary can only produce deferred `NeedsEncodedRead`, not actual scan/data execution.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data, read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt fallback execution.

## CG-2.1e.9 layout-reader construction blocker hardening

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexFile::layout_reader` is now a named runtime-driver blocker because upstream construction reaches `VortexFile::segment_source`, whose public contract may spawn a background I/O driver.
- `LayoutReader::row_count` remains metadata-like and non-blocking by itself, but it does not approve encoded-count execution because the construction boundary remains unapproved.
- Count-readiness and encoded-count approval preserve the layout-reader blocker by name while excluding metadata-only row-count surfaces from execution blockers.
- This pass does not construct `LayoutReader`, call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data, read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt fallback execution.

## CG-2.1e.10 layout-driver approval boundary

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexLayoutReaderDriverApprovalReport` defines the report-only gate that must approve any future `LayoutReader::row_count` use.
- The recorded public API boundary remains blocked unless local fixture scope, caller session, runtime-driver permission, row-count-only intent, no scan/evaluation/data-read/decode/materialization/Arrow/object-store/write, and no-fallback signals are explicit.
- Even approved reports construct no `LayoutReader`, start no driver, call no scan/evaluation API, read no data or rows, decode/materialize nothing, convert nothing to `Arrow`, perform no object-store IO or writes, and do not allow fallback.
- This pass adds no runtime invocation, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-2.1e.11 layout-driver approval CLI surfacing

- Primary RFC linkage: RFC 0010 Developer Experience, RFC 0012 Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `shardloom vortex-layout-driver-approval-plan <signals> [--format text|json]` exposes the layout-driver approval report for human and agent inspection.
- The command consumes only explicit signal text and the static encoded-read public API boundary report; it performs no filesystem, network, catalog, adapter, scan, evaluation, or data-read probing.
- Missing/unknown signals fail deterministically, and the recorded public API boundary remains unsupported unless runtime-driver permission is explicit.
- This pass adds no runtime invocation, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-2.1e.12 layout-approved encoded count bridge

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexEncodedCountDataPathApprovalInput` can now carry a matching `VortexLayoutReaderDriverApprovalReport`.
- Encoded-count approval can reach `approved_for_deferred_count` when count readiness is ready and the layout approval report is approved, side-effect-free, fallback-disabled, and built from the same API boundary.
- `shardloom vortex-encoded-count-approval-plan ... --layout-row-count-approved` exposes this bridge in CLI output with layout approval status and row-count path approval fields.
- This pass still performs no actual encoded-data traversal, layout-reader construction, runtime-driver startup, scan/read-start invocation, row read, decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO, external baseline invocation, or fallback execution.

## CG-2.1e.13 layout-approved local count guard

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `execute_vortex_count_all_from_encoded_count_data_path_approval` now has coverage for a layout-row-count-approved encoded-count approval report, returning only the existing deferred `NeedsEncodedRead` local plan.
- `shardloom vortex-encoded-count-approval-plan ... --layout-row-count-approved` now includes local execution status fields when approval is present, while preserving `data_read=false`.
- This pass performs no actual encoded-data traversal, layout-reader construction, runtime-driver startup, scan/read-start invocation, row read, decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO, external baseline invocation, or fallback execution.

## CG-2.1e.14 encoded-count local guard capability discovery

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexEncodedCountLocalGuardDiscoveryReport` records the static local guard contract for approved encoded-count paths without probing runtime inputs.
- `shardloom capabilities operators --format json` emits accepted approval sources, deferred local execution status, plan-only mode, no count result, no data read, no decode/materialization, no runtime execution, and no fallback.
- This pass performs no actual encoded-data traversal, layout-reader construction, runtime-driver startup, scan/read-start invocation, row read, decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO, external baseline invocation, or fallback execution.

## CG-2.1e.15 local fixture Vortex array scan/count proof

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `execute_vortex_count_all_from_local_scan_with_session` adds a feature-gated local fixture path that requires caller-owned `VortexSession`, caller-owned blocking runtime, local `.vortex` target, and encoded-read readiness approved for future execution.
- The helper calls `VortexFile::scan` and `ScanBuilder::into_array_iter` only inside the `vortex-encoded-read-spike` local fixture boundary, then counts returned Vortex arrays via `ArrayRef::len()`.
- The report records `data_read=true`, `upstream_scan_called=true`, array count, row count, and count result.
- The report records no row reads, no requested decode/materialization, no Arrow conversion, no object-store IO, no writes, no spill IO, and no fallback execution.
- The general public scan/read-start API boundary remains conservative; this pass does not approve adapters, non-fixture sources, encoded predicates, projections, object-store targets, benchmarks, external baselines, parser/runtime expansion, or superiority claims.

## CG-2.1e.16 approval-gated local fixture scan/count

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `execute_vortex_count_all_from_local_scan_with_session` now requires an approved `VortexEncodedCountDataPathApprovalReport` before the local fixture scan/count path can run.
- Recorded public API-boundary approval blockers return a blocked report before `VortexFile::scan` or `ScanBuilder::into_array_iter` is called.
- Approved reports still require encoded-read readiness, caller-owned session/runtime, and local `.vortex` scope.
- This keeps the CG-2.1e approval chain authoritative as execution begins: no row reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO, external baselines, or fallback execution are added.

## CG-2.1e.17 local fixture scan target consistency

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- The local fixture scan/count helper now derives source URI evidence from the encoded-read readiness planning chain before scan.
- Approval target URI and encoded-read readiness source URI must match exactly before `VortexFile::scan` or `ScanBuilder::into_array_iter` is called.
- Missing readiness source URI evidence or a target mismatch returns a blocked report with `data_read=false`, `upstream_scan_called=false`, and `fallback_execution_allowed=false`.
- This prevents cross-target evidence reuse while keeping the local fixture exception narrow: no row reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO, external baselines, or fallback execution are added.

## CG-2.1e.18 local fixture scan source evidence reporting

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexEncodedReadExecutionReport` now exposes local fixture scan target URI, encoded-read readiness source URI, and a source/target match flag.
- Successful local fixture reports, target-mismatch reports, object-store blocked reports, and approval-blocked reports preserve the source-evidence fields for auditability.
- These fields make the narrow fixture proof easier to validate before generalized count execution while keeping non-fixture scan/read-start approval deferred.
- No row reads, requested decode/materialization, Arrow conversion, object-store IO expansion, writes, spill IO, external baselines, or fallback execution are added.

## CG-2.1e.19 explicit local encoded-count execution boundary

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `vortex_encoded_read_local_scan_count_api_boundary` marks only `OpenOptionsSessionExt::open_path`, `VortexFile::scan`, and `ScanBuilder::into_array_iter` as execution-usable, and only for local `.vortex` `CountAll`.
- `execute_vortex_count_all_from_approved_local_scan` owns the upstream runtime/session setup for that approved local boundary while preserving encoded-count approval and source-match gates.
- `shardloom vortex-encoded-read-spike ... --execute-local-count` exposes the path as an explicit CLI opt-in and reports count result, arrays read, rows counted, scan target, readiness source, and source-match evidence.
- The broad public API boundary remains conservative; generalized encoded-data count execution, adapters, non-local sources, object-store IO, encoded predicates, projections, writes, benchmarks, external baselines, CG closeout, and fallback execution remain out of scope.
- No row reads, requested decode/materialization, Arrow conversion, object-store IO expansion, writes, spill IO, external baselines, or fallback execution are added.

## CG-2.1e.20 approved local scan naming normalization

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexEncodedReadExecutionStatus`, `VortexEncodedReadExecutionMode`, `VortexEncodedReadExecutionReport`, diagnostics, human text, and focused tests now use `local_scan` naming for the approved local count path.
- The CLI output keeps the existing `local_scan_*` fields while reading the renamed report fields.
- Historical layout-driver `local-fixture-only` input remains unchanged to avoid a public signal rename outside this cleanup scope.
- This is naming/report-surface cleanup only: generalized encoded-data count execution, adapters, non-local sources, object-store IO, encoded predicates, projections, writes, benchmarks, external baselines, CG closeout, and fallback execution remain out of scope.

## CG-2.1e.21 approved local scan result bridge

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `execute_vortex_count_all_from_approved_local_scan_result` now consumes approved encoded-count data-path evidence plus a successful approved local scan/count report and returns local execution evidence with a known `CountAll` value.
- The bridge requires enabled feature evidence, `local_scan_encoded_count_executed`, `local_scan_encoded_array_length_count`, matching approval target/readiness source URI evidence, a known count result, and `rows_counted == count_result`.
- The bridge rejects missing count results, target/source mismatches, disabled feature reports, unsuccessful scan reports, row reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO, external effects, and fallback execution.
- `shardloom vortex-encoded-read-spike ... --execute-local-count` now emits local execution status, mode, known result/value, task/data-read evidence, and side-effect/no-fallback fields alongside the local scan report.
- This is still not generalized encoded-data count execution: adapters, non-local sources, object-store IO, encoded predicates, projections, writes, benchmarks, external baselines, CG closeout, and fallback execution remain out of scope.

## CG-2.1e.22 stable explicit local encoded `CountAll` execution surface

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `shardloom vortex-count <dataset_uri>` remains metadata-only by default.
- `shardloom vortex-count <dataset_uri> --execute-local-encoded-count <memory_gb> <max_parallelism>` now explicitly opts into the same approved local `.vortex` encoded `CountAll` path that was previously reachable only through the spike command.
- The stable command reuses encoded-read readiness, encoded-count data-path approval, approved local scan/count execution, and approved local scan result bridging before reporting a known count value.
- CLI output records local scan target URI, readiness source URI, source-match evidence, arrays read, rows counted, count result, local execution status, side-effect flags, and `fallback_execution_allowed=false`.
- This does not approve broad scan/read-start execution, adapters, non-local sources, object-store IO, encoded predicates, projections, row reads, requested decode/materialization, Arrow conversion, writes, spill IO, benchmarks, external baselines, CG closeout, or fallback execution.

## CG-5.1 metadata query primitive correctness fixtures

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0012 Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `shardloom-contract-tests/tests/query_primitive_correctness.rs` adds cross-crate fixtures for metadata-backed `CountAll`, metadata-proven `CountWhere`, inconclusive predicate deferral, and projection deferral.
- The fixtures assert exact values for file row-count, segment row-count summing, metadata-proven false predicates, metadata-proven true predicates, and deferred encoded-predicate/projection cases.
- Every fixture asserts no task execution, data read, decode/materialization, object-store IO, write IO, or fallback execution.
- This pass adds no new runtime behavior, external baseline execution, benchmark claim, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-5.2 metadata query primitive edge and diagnostic fixtures

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0012 Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `shardloom-contract-tests/tests/query_primitive_correctness.rs` extends cross-crate fixtures to missing metadata, metadata-proven true predicates without segment row counts, metadata-pruned filters, projection metadata misses, unsupported primitive requests, and local missing-summary blocking.
- `shardloom-vortex/src/query_primitive.rs` corrects the `CountAll` missing-metadata reason so diagnostic evidence names the evaluated primitive.
- Every fixture asserts no task execution, data read, decode/materialization, object-store IO, write IO, spill IO, or fallback execution.
- This pass adds no new query execution behavior, external baseline execution, benchmark claim, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-5.3 correctness fixture manifest contract

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0012 Diagnostics/Capabilities, and RFC 0025 Competitive/no-fallback.
- `shardloom-core/src/correctness.rs` adds manifest fields for fixture source references, test-only reference roles, metadata row-count reference outputs, and explicit edge-case fixture families.
- `CorrectnessValidationPlan::default_foundation_plan` declares the checked-in `metadata_footer_u64_20000.vortex` fixture with `ExpectedOutcome::MetadataRowCount { row_count: 20000 }` and marks golden/reference roles as non-production execution.
- `shardloom-contract-tests/tests/correctness_fixture_manifest.rs` verifies the fixture is checked in, the row-count reference output does not require execution, null/nested/dictionary/sparse/run-length/temporal fixture families are tracked, and reference roles cannot become production fallback.
- This pass adds no query execution behavior, external baseline invocation, benchmark claim, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-5.4 external baseline oracle policy

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0025 Competitive/no-fallback, and RFC 0032 capability certification gates.
- `CorrectnessValidationPlan::default_foundation_plan` now declares Spark, DataFusion, DuckDB, Polars, and Velox as external correctness oracles only.
- `DifferentialBaseline::external_correctness_oracle` records comparison-only notes and remains fallback-disabled.
- `shardloom-contract-tests/tests/external_baseline_oracles.rs` verifies all declared baselines are present, reference-only, non-fallback-capable, and not runtime execution paths.
- This pass adds no external engine dependency, external baseline invocation, query execution behavior, benchmark claim, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-5.5 local encoded `CountAll` correctness fixture/reference-output proof

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0012 Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `ExpectedOutcome::EncodedCount { count }` records an execution-required reference output distinct from metadata-only row-count evidence.
- `CorrectnessValidationPlan::default_foundation_plan` declares `vortex-local-encoded-count-u64-20000` over the checked-in `metadata_footer_u64_20000.vortex` fixture with `ExpectedOutcome::EncodedCount { count: 20000 }`.
- `shardloom-contract-tests/tests/correctness_fixture_manifest.rs` verifies the fixture path, expected count, execution-required status, golden fixture role, and non-production reference role.
- `shardloom-vortex/src/encoded_read_executor.rs` feature-gated tests verify the approved local encoded count path and local execution bridge return the manifest count without decode/materialization/row/Arrow/object-store/write/spill/external/fallback effects.
- This pass adds no new fixture generation, decoded reference engine execution, external baseline invocation, generalized encoded-data execution, non-local adapter, object-store IO, encoded predicate, projection execution, row read, requested decode/materialization, Arrow conversion, write behavior, benchmark claim, superiority claim, or fallback execution.

## CG-16.1 local encoded `CountAll` execution certificate

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0015 Correctness/Semantics/Differential Testing, RFC 0012 Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `ExecutionCertificateInput`, `ExecutionCertificate`, and `ExecutionCertificateStatus` define the first generic CG-16 execution certificate surface in `shardloom-core`.
- Certificate evaluation requires matching expected/actual correctness output, explicit correctness-passed evidence, no fallback attempt, no fallback availability, no unsafe effect flag, and no error diagnostics before certification.
- `local_encoded_count_execution_certificate` derives a certificate from the approved local encoded count report and local execution bridge report, preserving input/output refs, fixture id, data-read flags, unsafe-effect flags, and no-fallback fields.
- `shardloom-contract-tests/tests/execution_certificate_contracts.rs` verifies certified, fallback-blocked, unsafe-effect-blocked, and diagnostic-blocked certificate outcomes.
- The feature-gated local encoded count test verifies the actual approved local `.vortex` path emits a certified, fallback-free certificate for the CG-5.5 fixture.
- This pass adds no generalized execution certificate system, native I/O certificate implementation, benchmark certificate, external baseline invocation, generalized encoded-data execution, non-local adapter, object-store IO, encoded predicate, projection execution, row read, requested decode/materialization, Arrow conversion, write behavior, benchmark claim, superiority claim, or fallback execution.

## CG-16.2 execution certificate evidence surface

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0015 Correctness/Semantics/Differential Testing, RFC 0012 Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, RFC 0031 Native I/O Certificates, and RFC 0032 capability certification requirements.
- `ExecutionCertificateEvidenceSurfaceReport` defines the report-only artifact requirements for broader CG-16 certificates.
- The report requires plan hash, input snapshot hash, output hash, selected/skipped segment traces, side-effect manifest, reproducibility metadata, correctness fixture linkage, deterministic field order, and machine-readable artifacts.
- `execution-certificate-plan` exposes stable JSON/text fields for artifact counts, per-kind counts, hash requirements, reproducibility requirements, no-evaluation status, no-runtime status, and fallback-disabled status.
- This phase adds no generalized execution certificate evaluation, benchmark certificate execution, external baseline invocation, generalized encoded-data execution, adapter runtime, object-store IO, row reads, decode/materialization, Arrow conversion, writes, spill IO, performance/superiority claim, production certification, CG closeout, or fallback behavior.

## CG-17.1 stateful reuse boundary report

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates/Stateful Reuse, RFC 0015 Correctness/Semantics/Differential Testing, RFC 0012 Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, RFC 0031 Native I/O Certificates, and RFC 0032 capability certification requirements.
- `StatefulReuseReport` defines typed cache/reuse boundaries for segment results, predicate results, encoded dictionaries, encoded filters, layout decisions, execution certificates, and incremental manifest diffs.
- The report requires deterministic keys scoped to dataset snapshot, plan hash, semantic profile, encoding/layout, and adapter fidelity before any reuse can become eligible.
- Invalidation proof requirements cover snapshot, segment, schema, partition, predicate, semantic profile, function version, adapter fidelity, and unknown-change signals with conservative rejection for unproven changes.
- `stateful-reuse-plan` exposes stable JSON/text fields for boundary counts, invalidation signal counts, correctness proof counts, invalidation proof counts, execution certificate counts, manifest-diff requirements, no-cache side-effect fields, no-runtime fields, and fallback-disabled status.
- This phase adds no cache storage, cache lookup, cache write, cache replay, incremental recompute execution, manifest-diff reads, generalized execution certificate evaluation, external baseline invocation, generalized encoded-data execution, adapter runtime, object-store IO, row reads, decode/materialization, Arrow conversion, writes, spill IO, performance/superiority claim, production certification, CG closeout, or fallback behavior.

## CG-6.1 benchmark evidence manifest

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0015 Correctness/Semantics/Differential Testing, RFC 0025 Competitive/no-fallback, and RFC 0032 capability certification gates.
- `shardloom-core/src/benchmark.rs` expands benchmark metric vocabulary for startup/runtime/write latency, peak memory, bytes read/written/decoded/avoided, materialization avoided, segments considered/pruned/metadata-answered, object-store requests, spill required/avoided, and work avoided.
- `BenchmarkPlan::default_foundation_plan` now covers CG-6 metric categories in report-only scenarios and keeps baselines comparison-only with fallback disabled.
- `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verifies required metric coverage and correctness validation mode presence before any claim can rely on the benchmark plan.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior, superiority claim, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-6.2 benchmark claim gate

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0015 Correctness/Semantics/Differential Testing, RFC 0025 Competitive/no-fallback, and RFC 0032 claim publication requirements.
- `BenchmarkClaimGate` blocks performance, superiority, cost, replacement, or best-default publication unless correctness evidence, benchmark evidence, required metrics, comparison reports, and no-fallback evidence are all present.
- `BenchmarkPlan::claim_gate` returns `evidence_missing` for the recorded report-only foundation plan because no benchmark runner or comparison report exists yet.
- `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verifies every publication input is required and fallback attempts block claims.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior, superiority claim, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-6.3 benchmark comparison report contract

- Primary RFC linkage: RFC 0029 benchmark evidence requirements, RFC 0015 correctness-before-performance requirements, RFC 0025 competitive/no-fallback guardrails, and RFC 0032 claim publication and comparison-report requirements.
- `BenchmarkComparisonReport` records expected scenario/baseline result coverage, required metric gaps, comparison-report emission, correctness evidence state, benchmark evidence state, and no-fallback state without executing benchmarks.
- `BenchmarkComparisonReport::claim_gate` treats a report as emitted while keeping performance/superiority publication blocked until correctness evidence, complete benchmark results, required metrics, and no-fallback evidence are all present.
- `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verifies missing results and unknown required metrics block claim readiness, and complete synthetic evidence can only reach claim-review readiness through explicit report fields.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior, superiority claim, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-6.4 benchmark reproducibility manifest

- Primary RFC linkage: RFC 0029 reproducible benchmark evidence requirements, RFC 0015 correctness-before-performance requirements, RFC 0025 competitive/no-fallback guardrails, and RFC 0032 benchmark evidence floor for best-default claims.
- `BenchmarkRunManifest` records dataset shape, schema, storage format, compression, engine versions, hardware profile, operating-system profile, runtime configuration, cache state, required metrics, reproduction steps, correctness evidence state, and no-fallback state.
- The recorded foundation plan remains `incomplete` because no approved benchmark runner has produced complete reproducibility metadata or benchmark results.
- `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verifies incomplete default manifests, complete synthetic reproducibility metadata, and comparison-only engine-version labels.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior, superiority claim, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-6.5 reproducibility-aware benchmark claim gate

- Primary RFC linkage: RFC 0029 reproducible benchmark evidence requirements, RFC 0015 correctness-before-performance requirements, RFC 0025 competitive/no-fallback guardrails, and RFC 0032 benchmark-gated claim publication requirements.
- `BenchmarkClaimGate` now requires reproducibility evidence in addition to correctness evidence, benchmark evidence, required metrics, comparison-report evidence, and no-fallback evidence.
- `BenchmarkEvidenceBundle` combines a `BenchmarkRunManifest` and `BenchmarkComparisonReport` into the final claim gate so complete metric rows cannot publish claims without reproducible run metadata.
- `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verifies comparison-ready reports remain blocked without reproducibility and that complete synthetic comparison/reproducibility evidence is required before the publication gate can open.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior, superiority claim, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-7.1 physical operator/kernel contract foundation

- Primary RFC linkage: RFC 0021 Expression Engine and Kernel Registry, RFC 0027 CPU Vectorized Kernels/Runtime Adaptivity, RFC 0014 Memory/Spill/OOM Safety, RFC 0025 competitive/no-fallback guardrails, and RFC 0032 operator certification requirements.
- `PhysicalOperatorPlan::cg7_foundation` declares report-only filter, project, and count-aggregate operator contracts with required metadata/encoded kernel blockers.
- `PhysicalKernelRequirement` rejects decoded-reference kernels as production native evidence, and `PhysicalOperatorContract` keeps native planning blocked while required kernels are missing.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` verifies missing-kernel blockers, reference-only rejection, synthetic native-readiness without execution, and no-fallback invariants.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, external engine invocation, or fallback execution.

## CG-7.2 physical operator capability discovery

- Primary RFC linkage: RFC 0012 diagnostics/capabilities, RFC 0021 kernel registry requirements, RFC 0025 no-fallback guardrails, and RFC 0032 operator coverage/certification discovery requirements.
- `PhysicalOperatorPlan` now carries a stable schema version and readiness-count helpers for ready, missing-kernel, and unsupported operator contract states.
- `shardloom capabilities operators` includes the physical operator plan schema/version, plan id, operator count, readiness count, missing-kernel count, unsupported count, fallback-disabled flag, and runtime-execution=false flag.
- `shardloom-cli/tests/capability_discovery_snapshots.rs` locks the operator capability JSON field order and verifies the CG-7 physical operator blockers remain agent-readable.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, external engine invocation, or fallback execution.

## CG-7.3 physical kernel registry plan

- Primary RFC linkage: RFC 0021 kernel registry requirements, RFC 0027 native kernel specialization roadmap, RFC 0012 diagnostics/capabilities, RFC 0025 no-fallback guardrails, and RFC 0032 operator certification requirements.
- `PhysicalKernelRegistryPlan` derives required native kernel slots from the CG-7 foundation physical operator plan and records present, missing, and reference-only-rejected slot counts.
- `shardloom kernel-registry` exposes the physical kernel registry schema/version, registry id, required slot count, present count, missing count, reference-only rejection count, runtime-execution=false, and fallback-disabled fields.
- `shardloom-cli/tests/kernel_registry_snapshots.rs` verifies the kernel-registry JSON fields remain stable and agent-readable.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, external engine invocation, or fallback execution.

## CG-7.4 physical kernel admission gate

- Primary RFC linkage: RFC 0021 kernel registry requirements, RFC 0015 correctness-first requirements, RFC 0014 memory/OOM safety, RFC 0025 no-fallback guardrails, RFC 0029 benchmark evidence rules, and RFC 0032 operator certification requirements.
- `PhysicalKernelAdmissionReport` records required/candidate kernel kind, correctness evidence, benchmark evidence, memory-safety evidence, fallback state, and admission status for a physical kernel slot.
- Reference-only kernels, unsupported kernels, kind mismatches, fallback attempts, missing correctness evidence, and missing memory-safety evidence cannot mark a slot present.
- Registry admission can proceed before production claims when benchmark evidence is missing, but production readiness requires benchmark evidence in addition to correctness, memory, and no-fallback proof.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` verifies blocked reference/fallback/missing-evidence states and the registry-ready versus production-ready distinction.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, external engine invocation, or fallback execution.

## CG-7.5 physical operator execution profiles

- Primary RFC linkage: RFC 0021 expression/kernel execution modes, RFC 0027 native kernel specialization roadmap, RFC 0014 memory/materialization safety, RFC 0025 no-fallback guardrails, and RFC 0032 operator certification requirements.
- `PhysicalOperatorExecutionProfileMatrix::cg7_foundation` declares metadata-only, encoded-native, hybrid-native, and native-decoded execution levels for filter, project, and count-aggregate operator profiles.
- Foundation profiles reject test-reference-only and unsupported execution levels and keep row materialization, Arrow conversion, and fallback execution disabled.
- `shardloom capabilities operators` includes execution-profile schema/version and counts for profile, reference-only, row-materialization, Arrow-conversion, and fallback paths.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` and `shardloom-cli/tests/capability_discovery_snapshots.rs` verify the profile contracts and capability discovery fields.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, external engine invocation, or fallback execution.

## CG-7.6 physical kernel selection gate

- Primary RFC linkage: RFC 0021 kernel selection requirements, RFC 0027 native kernel specialization roadmap, RFC 0012 deterministic diagnostics, RFC 0025 no-fallback guardrails, and RFC 0032 operator certification requirements.
- `PhysicalKernelSelectionReport` validates operator profile availability, requested execution level, and required native kernel slot presence before a physical kernel can be selected.
- Selection rejects missing operator profiles, disallowed execution levels, and missing required slots while preserving runtime-execution=false and fallback-disabled flags.
- A synthetic present-kernel registry can reach `ready_for_admission_review` for planning evidence, but selection still does not execute kernels or read data.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` verifies missing-slot, rejected-level, missing-profile, and synthetic ready-selection states.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, external engine invocation, or fallback execution.

## CG-7.7 physical operator planning certificate

- Primary RFC linkage: RFC 0021 kernel selection and registry requirements, RFC 0027 physical operator/kernel roadmap, RFC 0012 deterministic diagnostics, RFC 0025 no-fallback guardrails, RFC 0029 evidence gating, and RFC 0032 operator certification requirements.
- `PhysicalOperatorPlanningCertificate` summarizes physical operator readiness, registry slot state, selection gate state, admission gate state, fallback-attempt evidence, and production-claim readiness in one report-only certificate.
- Certificates distinguish operator-plan blockers, registry blockers, selection blockers, admission blockers, native-planning readiness, and production certification readiness.
- Production certification remains separate from native planning readiness and requires benchmark evidence; runtime execution remains disabled even when certificate evidence is ready.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` verifies foundation blockers, synthetic native-planning readiness, production-certification separation, and fallback-attempt blocking.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, external engine invocation, or fallback execution.

## CG-7.8 Vortex query primitive physical-operator bridge

- Primary RFC linkage: RFC 0021 physical/kernel selection requirements, RFC 0027 operator/kernel roadmap, RFC 0012 deterministic diagnostics, RFC 0025 no-fallback guardrails, and RFC 0032 operator coverage/certification requirements.
- `VortexPhysicalOperatorBridgeReport` lowers Vortex `CountAll`, `CountWhere`, `ProjectColumns`, `FilterPredicate`, and `FilterAndProject` requests into CG-7 physical operator plans.
- The bridge attaches a `PhysicalOperatorPlanningCertificate` so CG-2 query primitives expose CG-7 operator/kernel blockers before any kernel implementation is accepted.
- Unsupported Vortex query primitives lower to an unsupported physical operator instead of fallback execution.
- `shardloom-vortex/src/physical_operator_bridge.rs` verifies count/filter/project mappings, physical operator order, side-effect-free behavior, and no-fallback diagnostics.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, external engine invocation, or fallback execution.

## CG-7.9 execution-level kernel requirements

- Primary RFC linkage: RFC 0021 deterministic kernel selection requirements, RFC 0027 metadata/encoded/hybrid/native-decoded operator levels, RFC 0012 deterministic diagnostics, RFC 0025 no-fallback guardrails, and RFC 0032 operator certification requirements.
- `PhysicalOperatorExecutionProfile::required_kernel_kinds_for_level` makes kernel selection requirements depend on the requested execution level.
- Metadata-only selection now requires only metadata kernels, encoded-native selection requires metadata and encoded kernels, hybrid-native selection also requires partial-decode capability, and native-decoded selection requires metadata plus partial-decode capability.
- `PhysicalKernelSelectionReport` stores the level-specific required kernel kinds and emits missing-slot blockers for absent level-specific slots.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` verifies metadata-only readiness without encoded blockers and hybrid partial-decode missing-slot diagnostics.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, external engine invocation, or fallback execution.

## CG-7.10 metadata-result physical operator bridge

- Primary RFC linkage: RFC 0021 metadata kernel selection requirements, RFC 0026 metadata/query primitive bridge, RFC 0027 physical operator/kernel roadmap, RFC 0012 deterministic diagnostics, RFC 0025 no-fallback guardrails, and RFC 0032 operator certification requirements.
- `plan_vortex_query_primitive_result_physical_operators` maps already metadata-answered Vortex query primitive results to metadata-only physical operator plans.
- Metadata-answered `CountAll`, `CountWhere`, and `FilterPredicate` results can mark metadata kernel requirements present for count/filter physical operators without executing kernels.
- Physical planning certificate admission remains blocked until separate correctness, memory-safety, benchmark, and no-fallback evidence is supplied.
- Non-metadata results keep the original missing-kernel blockers, and unsupported primitives remain unsupported instead of fallback execution.
- `shardloom-vortex/src/physical_operator_bridge.rs` verifies metadata count/filter readiness, non-metadata blocker preservation, side-effect-free behavior, and no-fallback flags.
- This pass adds no new query execution behavior, kernel implementation, encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, external engine invocation, or fallback execution.

## CG-7.11 metadata bridge admission evidence

- Primary RFC linkage: RFC 0021 kernel admission and selection requirements, RFC 0026 metadata/query primitive bridge, RFC 0027 physical operator/kernel roadmap, RFC 0012 deterministic diagnostics, RFC 0025 no-fallback guardrails, RFC 0029 benchmark evidence gating, and RFC 0032 operator certification requirements.
- `plan_vortex_query_primitive_result_physical_operators_with_evidence` lets already metadata-answered Vortex query primitive results supply explicit correctness, benchmark, memory-safety, and no-fallback admission evidence to the attached physical planning certificate.
- Metadata-result bridge defaults remain conservative: the existing `plan_vortex_query_primitive_result_physical_operators` path still emits missing evidence and blocked admission until evidence is supplied.
- Correctness plus memory-safety evidence can advance metadata-only count/filter bridges to `ready_for_native_planning`; benchmark evidence is still required before `production_certified` can appear.
- Any attempted fallback evidence blocks admission and keeps runtime execution and fallback execution disabled.
- This pass adds no new query execution behavior, kernel implementation, encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, external baseline invocation, or fallback execution.

## CG-7.12 metadata-only physical kernel report

- Primary RFC linkage: RFC 0021 metadata kernel requirements, RFC 0026 metadata/query primitive bridge, RFC 0027 physical operator/kernel roadmap, RFC 0012 deterministic diagnostics, RFC 0025 no-fallback guardrails, RFC 0029 evidence gating, and RFC 0032 operator certification requirements.
- `evaluate_vortex_metadata_physical_kernels` consumes an already metadata-answered Vortex primitive result plus a matching physical-operator bridge report.
- Metadata-only count/filter kernel reports require the bridge certificate to be ready for native planning; the default missing-evidence bridge remains blocked.
- Metadata `CountAll`/`CountWhere` reports surface count-aggregate and filter/count-aggregate operator coverage; metadata `FilterPredicate` reports surface filter operator coverage.
- Blocked reports remain deterministic, side-effect-free, and fallback-disabled.
- This pass adds no encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO, external baseline invocation, or fallback execution.

## CG-7.13 metadata physical kernel CLI surfacing

- Primary RFC linkage: RFC 0012 deterministic capability/diagnostic discovery, RFC 0021 kernel selection and metadata-kernel requirements, RFC 0025 no-fallback guardrails, RFC 0029 evidence gating, and RFC 0032 operator certification discovery requirements.
- `shardloom vortex-metadata-physical-kernel-plan <primitive> <dataset_uri> <metadata_value>` exposes metadata-only physical kernel reports for count, filtered-count, and filter metadata values.
- The command requires explicit `--correctness-evidence` and `--memory-safe` evidence before returning success; `--benchmark-evidence` upgrades the attached certificate to production-certified, and `--fallback-attempted` blocks admission.
- JSON/text output includes certificate status, metadata kernel count, evidence flags, data-read/decode/materialization/IO fields, side-effect-free status, and fallback-disabled status.
- This pass adds no encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO, external baseline invocation, or fallback execution.

## CG-7.14 metadata kernel capability discovery

- Primary RFC linkage: RFC 0012 capability discovery, RFC 0021 kernel registry and metadata-kernel contracts, RFC 0025 no-fallback guardrails, RFC 0029 correctness/benchmark evidence, and RFC 0032 operator certification discovery.
- `shardloom capabilities operators` now reports the metadata physical kernel report schema, supported metadata primitives, contextual-only status, correctness/memory/benchmark evidence requirements, and no runtime/fallback/IO effects.
- `shardloom kernel-registry` reports the same metadata physical kernel discovery fields while preserving the global registry counts as missing until actual native kernel slots are implemented and admitted.
- The discovery surface makes already metadata-answered count/filter capability visible to humans and agents without claiming encoded-native, hybrid, production-certified, or global runtime kernel readiness.
- This pass adds no encoded-data traversal, scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO, external baseline invocation, or fallback execution.


## CG-2.2a filtered-count readiness core contract
- CG-2.1, CG-2.1a, and CG-2.1b are complete.
- CG-2.2a adds `VortexFilteredCountReadinessRequest` and `VortexFilteredCountReadinessReport` planning/reporting only.
- CG-2.2a.1 blocker precision helper update is complete: `filtered-count` + `PredicateProvided` maps to `EncodedPredicatePath` even when encoded-data-path readiness is missing; missing encoded-data-path reports `BlockedByMissingEncodedDataPath`; non-`filtered-count` primitives remain `Unknown`; metadata predicate-proof remains deferred to explicit proof contract.
- Distinguishes `VortexFilteredCountCandidateSource::MetadataPredicateProof` vs `::EncodedPredicatePath`.
- Metadata-proof filtered count remains explicit and opt-in via `PredicateMetadataProofReady`; CG-2.2c admits it to metadata-only local execution only when a matching `CountWhere` request and metadata summary are supplied.
- Encoded-predicate filtered-count execution is not implemented.
- No scan/read-start, predicate evaluation, encoded-data read, row read, decode, materialization, `Arrow` conversion, object-store IO, writes, or fallback execution are added.
- CG-2.2b CLI integration is complete via `shardloom vortex-filtered-count-readiness-plan <candidate_source> <dataset_uri> [flags] [--format text|json]`.
- Keep CG-1 through CG-20 visible; active status remains in `phased-execution-plan.md`.
- The command does not execute filtered count, does not evaluate predicates, does not call scan/read-start APIs, and performs no metadata/footer open, encoded-data read, row read, decode/materialization, `Arrow` conversion, object-store IO, writes, or fallback execution.
- Encoded-predicate filtered-count execution remains blocked until a real encoded predicate path exists.

## CG-2.2c filtered-count metadata proof local guard

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex query-readiness boundaries.
- `execute_vortex_count_where_from_filtered_count_metadata_proof` accepts only `MetadataPredicateProof` readiness for matching `CountWhere` requests with metadata summaries.
- Metadata-proven predicates can return metadata-only count results from segment metadata through the local execution report, preserving no encoded-data read, no row read, no decode/materialization, and no fallback.
- Encoded-predicate candidates are rejected by this guard and remain future work.
- This pass adds no encoded predicate evaluation, scan/read-start invocation, encoded-data traversal, row read, decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO, external baseline invocation, or fallback execution.

## CG-2.2d filtered-count metadata proof report

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex query-readiness boundaries.
- `VortexFilteredCountMetadataProofReport` classifies `CountWhere` plus a supplied metadata summary as `proof_ready`, `needs_encoded_predicate`, `missing_metadata`, or `unsupported`.
- Proof-ready reports carry the metadata-only count result and explicitly report no data read, no row read, no decode/materialization, no object-store IO, no write IO, and no fallback.
- Inconclusive metadata reports request encoded predicate evaluation without executing it.
- This pass adds no encoded predicate evaluation, scan/read-start invocation, encoded-data traversal, row read, decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO, external baseline invocation, or fallback execution.

## CG-2.3a projection readiness semantic hardening

- CG-2.2, CG-2.2a.1, and CG-2.2b are complete.
- CG-2.3a semantic hardening is complete.
- `ShardLoom` now provides projection-readiness planning/reporting contracts (`VortexProjectionReadinessRequest` and `VortexProjectionReadinessReport`) without projection execution.
- Projection-readiness distinguishes metadata/schema projection candidates from encoded-column projection candidates:
  - metadata/schema projection remains explicit and requires `ProjectionSupported` plus `MetadataFooterReady`;
  - encoded-column projection candidates require `EncodedDataPathReady`.
- The contract remains report-only: no scan/read-start, no projection application, no encoded-data reads, no row reads, no decode, no materialization, no `Arrow` conversion, no object-store `IO`, no writes, and no fallback execution.
- Keep CG-1 through CG-20 visible; active status remains in `phased-execution-plan.md`.

## CG-2.3b projection readiness CLI integration

- `ShardLoom` now exposes projection-readiness planning through `shardloom vortex-projection-readiness-plan <candidate_source> <dataset_uri> [flags] [--format text|json]`.
- Candidate sources are `metadata-schema-projection`, `encoded-column-path`, and `unknown`.
- CLI flags surface existing readiness signals, including feature-gate, query-primitive readiness, metadata/footer readiness, encoded-data-path readiness, projection primitive/provided/supported/unsupported, object-store target, decode/materialization/Arrow/write/scan risks, and fallback-policy blocking.
- The command emits deterministic text/JSON fields for status, mode, projection readiness, candidate source, readiness signals, no-op effect fields, and `fallback_execution_allowed=false`.
- Focused CLI tests cover missing/invalid arguments, unknown options, bare `json`/`text` rejection, metadata-schema readiness, encoded-column readiness, unknown-source blocking, missing encoded path blocking, unsupported projection blocking, JSON output dispatch, and report-only field invariants.
- The command does not execute projection, apply projection, call scan/read-start APIs, read metadata/footer or encoded data, read rows, decode, materialize, convert to `Arrow`, perform object-store `IO`, write data, call upstream scans, or attempt fallback execution.
- CG-2.1+ actual primitive execution remains deferred until real metadata/footer and encoded-data execution paths are approved.

## R5 systems-learning vocabulary traceability

- RFC 0008: `SplitSource`, `TaskLease`, `PlacementHint`, `IntermediateArtifactRef`, `RecoveryStrategy`.
- RFC 0012: `PushdownProofReport`, `LoweringTraceReport`, `TaskGranularityReport`, `RuntimeFilterReport`, `PlannedVsActualOperatorProfile`, `PlanPortabilityReport`.
- RFC 0016: `OptimizerDecisionKind`, runtime filter lifecycle, pushdown proof, split/fuse/coalesce decisions.
- RFC 0018: `OperatorProfile`, planned-vs-actual runtime reporting, `system.*` introspection surfaces.
- RFC 0022: `PlanPortabilityReport`, Substrait-like portability/loss boundary.
- RFC 0011: SQL frontend parse/bind/validate boundary.



## R5.2 additions (docs/RFC-only)

| RFC | Competitive gate mapping | RFC linkage | Notes |
| --- | --- | --- | --- |
| RFC 0031 | CG-19 | RFC 0013; RFC 0008; RFC 0012; RFC 0016; RFC 0018 | docs/RFC-only in this pass; no runtime behavior or dependency changes. |
| RFC 0032 | CG-20 | RFC 0011; RFC 0012; RFC 0015; RFC 0021; RFC 0022; RFC 0023; RFC 0029; RFC 0030 | docs/RFC-only in this pass; no runtime behavior or dependency changes. |


## R5.3 — capability coverage and certification deepening
- Scope: docs/RFC-only deepening pass.
- RFC 0031 deeper contracts map primarily to CG-19, with related trace links to RFC 0008, RFC 0012, RFC 0013, RFC 0016, and RFC 0018.
- RFC 0032 deeper contracts map primarily to CG-20, with related trace links to RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, and RFC 0030.
- This phase adds no runtime/parser/adapter/dependency/fallback behavior.

## R5.3.1 RFC consistency fixes (docs-only)

- RFC 0031 transition semantics corrected so metadata-first planning can continue from `metadata_only` into encoded states when metadata is insufficient.
- RFC 0032 claim evidence semantics corrected to distinguish emitted evidence fields from progressively required-pass fields.
- Docs-only update; no runtime behavior, dependency, parser, execution, adapter, or fallback changes.

## R5.3.2 docs-wide CG-19/CG-20 consistency pass (docs-only)

- RFC 0025 keeps CG-19 and CG-20 inside the primary Competitive Engine Track list.
- RFC 0031 requires per-source/sink-path `NativeIoCertificate` evidence instead of a single run-level certificate.
- RFC 0032 uses neutral claim-stage labels until correctness and benchmark evidence authorize superiority or best-default claims.
- RFC 0032 treats decoded reference behavior as test-only/reference evidence unless a native execution tier is explicitly certified.
- Downstream architecture docs keep CG-1 through CG-20 visible.
- This phase adds no runtime/parser/adapter/dependency/fallback behavior.

## R5.4 capability certification sequencing (docs-only)

- `docs/architecture/capability-certification-sequencing.md` splits CG-20 into implementation-ready batches before code or dependency work.
- Sequencing maps SQL coverage, operator coverage, function coverage, adapter certification, semantic profiles, migration compatibility, workload constitution, scorecards, and CI snapshots to explicit acceptance boundaries.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no runtime/parser/adapter/dependency/fallback behavior.

## R5.4.1 core capability matrix contracts

- `shardloom-core/src/certification.rs` adds report-only CG-20 contract shapes for SQL coverage, operator coverage, function coverage, adapter certification, semantic profiles, migration compatibility, and best-choice scorecards.
- `CapabilityCertificationReport::contract_only()` emits planned foundation matrices with `fallback_attempted=false`.
- `test_reference_only` evidence is modeled as non-production certification evidence and cannot satisfy production claim helpers.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, adapter runtime, function registry, operator kernel, dependency, external probing, or fallback behavior.

## R5.4.2 capability discovery surface

- `shardloom-cli/src/main.rs` exposes report-only CG-20 discovery through `shardloom capabilities <scope>`.
- Implemented scopes: `sql`, `functions`, `operators`, `adapters`, `semantic-profiles`, `migration`, and `certification`.
- Broader user-surface scopes such as `data-etl`, `python`, `unstructured-media`, `universal-adapters`, `api-surfaces`, `observability`, `deployment`, `extensions`, and `security-governance` remain planned until report-only contracts and snapshot coverage are added.
- `shardloom capabilities` without a scope remains the existing engine-level capability summary.
- Discovery output includes stable output-envelope fields for scope, schema version, fallback-disabled status, fallback-attempted status, and side-effect/probe flags.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, adapter runtime, function registry, operator kernel, dependency, filesystem/network/catalog probing, external-engine probing, or fallback behavior.

## R5.4.2a capability certification snapshot tests

- `shardloom-contract-tests/tests/capability_certification_snapshots.rs` locks the planned CG-20 matrix names, schema versions, and unsupported/default statuses.
- `shardloom-cli/tests/capability_discovery_snapshots.rs` locks scoped `shardloom capabilities <scope>` JSON field names and report-only probe flags.
- Snapshot tests cover SQL, operator, function, adapter, semantic profile, migration, and best-choice scorecard surfaces.
- Certification report output is checked against `FeatureFootprintReport` no-probe expectations where the contracts overlap.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, adapter runtime, function registry, operator kernel, dependency, filesystem/network/catalog probing, external-engine probing, or fallback behavior.

## R5.4.3 SQL frontend sequencing

- RFC 0032 now defines the SQL frontend stage ladder from `declared_only` through `benchmarked_certified`.
- `SqlFrontendReport` records parser, binder, semantic-profile, catalog, function, lowering, unsupported-construct, materialization, SQL coverage snapshot, diagnostics, dependency, runtime, and fallback fields.
- Parse-only status is explicitly not execution support, planning support, binding support, or semantic conformance.
- Native logical lowering must reject unsupported residuals instead of carrying them toward fallback execution.
- Native physical lowering must declare decode/materialization, ordering, partitioning, memory, spill, and sink requirements.
- Parser dependency approval remains deferred to an explicit dependency/RFC pass.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, adapter runtime, function registry, operator kernel, dependency, filesystem/network/catalog probing, external-engine probing, or fallback behavior.

## R5.4.4 operator and function certification sequencing

- RFC 0032 now defines operator certification transition meaning from `unsupported` through `production_certified`.
- `OperatorCertificationReport` records family, status, semantic profile, representation states, memory certification, materialization/order/partition requirements, correctness, semantic conformance, benchmark, diagnostics, report refs, and fallback status.
- Operator production certification requires correctness, semantic conformance, memory/spill safety, diagnostics, benchmark evidence, and no-fallback invariants.
- RFC 0032 now defines function certification status meaning using the shared `CapabilityCertificationStatus` vocabulary.
- `FunctionCertificationReport` records names, aliases, group, types, null behavior, determinism, volatility, effects, encoded/selection-vector/streaming/spill support, materialization, semantic profile, correctness, semantic conformance, benchmark, diagnostics, and fallback status.
- `test_reference_only` cannot satisfy production certification for operators or functions.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, adapter runtime, function registry, operator kernel, dependency, filesystem/network/catalog probing, external-engine probing, or fallback behavior.

## R5.4.5 adapter certification sequencing

- RFC 0032 now maps adapter maturity A0-A7 to evidence requirements from declared-only through benchmarked/certified.
- RFC 0032 defines adapter pushdown and residual-expression boundaries for exact, exact-with-residual, conservative false-positive, unsupported, and unsafe-rejected behavior.
- RFC 0032 expands adapter certification with source/sink report refs, fidelity report refs, native I/O certificate refs, metadata/statistics/fidelity loss, commit/recovery semantics, side effects, and diagnostics.
- RFC 0031 now links source capability, sink requirement, adapter fidelity, and native I/O certificate evidence to adapter certification.
- External source pushdown is proof-backed source behavior, not hidden fallback execution.
- Adapter certification remains workload/path scoped and cannot be inferred from external baseline availability.
- Primary RFC linkage: RFC 0031 and RFC 0032.
- Related RFCs: RFC 0008, RFC 0012, RFC 0013, RFC 0015, RFC 0016, RFC 0018, RFC 0021, RFC 0022, RFC 0029, and RFC 0030.
- This phase adds no SQL parser, SQL execution, adapter runtime, object-store IO, file-format dependency, catalog dependency, external-engine probing, or fallback behavior.

## R5.4.6 semantic profile and migration sequencing

- RFC 0032 now defines `SemanticProfileReport` fields, semantic dimension statuses, profile-specific evidence, and compatibility-profile boundaries.
- RFC 0032 states that Spark-compatible, DataFusion-compatible, Postgres-like, ANSI-strict, and ShardLoom-native profiles are semantics contracts, not execution modes.
- RFC 0032 now defines `MigrationCompatibilityReport` fields for supported constructs, unsupported constructs, semantic differences, function differences, adapter differences, materialization requirements, rewrite suggestions, evidence labels, diagnostics, and fallback status.
- RFC 0032 now defines performance/cost delta estimate fields with evidence labels and uncertainty, and blocks unsupported gain claims.
- RFC 0032 now defines Vortex conversion payback fields for source conversion scope, cost, benefit, uncertainty, and recommendation.
- External engines remain comparison, fixture, and migration baselines only.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0016, RFC 0021, RFC 0022, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, migration analyzer runtime, compatibility execution mode, adapter runtime, external-engine dependency, external-engine probing, benchmark claim, or fallback behavior.

## R5.4.7 workload constitution and scorecard sequencing

- RFC 0032 now defines `WorkloadConstitution` fields for workload categories, query patterns, data source profiles, sink target profiles, semantic profiles, SQL/operator/function/adapter requirements, API surfaces, scale shape, objectives, budgets, fixtures, benchmarks, migration sources, evidence refs, diagnostics, and fallback status.
- RFC 0032 now defines `WorkloadCategoryEvidence` entries tying each category to required coverage, correctness tests, benchmark scenarios, native I/O certificates, unsupported budgets, materialization budgets, evidence status, and diagnostics.
- RFC 0032 now defines `BestChoiceScorecard` fields, dimension statuses, dimension entries, optional/deferred weighting rules, mandatory dimension behavior, and claim publication requirements.
- RFC 0032 now defines `BestDefaultCertificationDossier` fields, minimum evidence floor, disqualifiers, and publication decisions for best-default-engine claims.
- Best-default certification remains workload-scoped and blocked by missing correctness, benchmark, semantic, adapter, native I/O, memory/spill, observability, migration, deployment, dependency-policy, or no-fallback evidence.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0010, RFC 0011, RFC 0012, RFC 0013, RFC 0014, RFC 0015, RFC 0016, RFC 0021, RFC 0023, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, benchmark implementation, certification runtime, migration analyzer runtime, compatibility execution mode, adapter runtime, external-engine dependency, external-engine probing, superiority claim, or fallback behavior.

## R5.4.8 CI and snapshot sequencing

- RFC 0032 now defines `CapabilitySurfaceSnapshot` fields for schema versions, field keys, entry keys, status counts, certification counts, no-probe flags, external-engine invocation flags, diagnostics, and fallback status.
- RFC 0032 now defines snapshot kinds for diagnostics, capability discovery, SQL, operators, functions, adapters, semantic profiles, migration compatibility, workload constitutions, scorecards, best-default dossiers, world-class sufficiency, feature footprint, and no-fallback invariants.
- RFC 0032 now defines `CapabilityDriftPolicy` fields plus allowed and blocked snapshot changes.
- RFC 0032 separates docs-only, report-only, correctness-gated, benchmark-gated, and release-gated CI levels.
- Snapshot execution remains deterministic, side-effect-free, report-only, no-probe, and no-fallback.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0012, RFC 0015, RFC 0024, RFC 0025, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, benchmark implementation, certification runtime, migration analyzer runtime, compatibility execution mode, adapter runtime, new tests, external-engine dependency, external-engine probing, superiority claim, or fallback behavior.

## R5.4.9 RFC sufficiency hardening pass

- RFC 0025 now defines the canonical best-default evidence gate for CG-20 claims.
- RFC 0031 now defines CG-19 sufficiency gates and disqualifiers for per-source/sink-path native I/O certificate evidence.
- RFC 0032 now defines `WorldClassSufficiencyReport` fields, sufficiency decisions, invariants, disqualifiers, and explicit implementation deferrals.
- Best-default and world-class claims now require workload-scoped links across `WorkloadConstitution`, `BestChoiceScorecard`, `BestDefaultCertificationDossier`, `WorldClassSufficiencyReport`, CG-5 correctness, CG-6 benchmark, CG-16 execution certificate, CG-19 native I/O certificate, capability snapshots, dependency policy, and no-fallback evidence.
- Primary RFC linkage: RFC 0025, RFC 0031, and RFC 0032.
- Related RFCs: RFC 0008, RFC 0012, RFC 0013, RFC 0015, RFC 0016, RFC 0018, RFC 0021, RFC 0023, RFC 0029, and RFC 0030.
- This phase adds no SQL parser, SQL execution, benchmark implementation, certification runtime, migration analyzer runtime, compatibility execution mode, adapter runtime, dependency, external-engine probing, superiority claim, or fallback behavior.

## R5.4.10 user-surface RFC hardening

- RFC 0032 now defines `ApiSurfaceReport`, `ObservabilityCertificationReport`, `DeploymentReadinessReport`, `ExtensionCapabilityReport`, and `SecurityGovernanceReport` as CG-20 certification evidence surfaces.
- Capability discovery now has explicit response fields and statuses for supported, partially supported, planned, disabled, feature/config gated, materialization gated, external-effect gated, dependency-review gated, unsupported, and unsafe-rejected entries.
- API/client/server maturity now covers CLI JSON, Rust, Python, DataFrame/query builder, SQL file, config/job, agent, notebook, HTTP/gRPC, FlightSQL-like, JDBC/ODBC, and BI/dashboard surfaces without implying execution delegation.
- Extension and UDF certification now covers runtime kind, type/null/effect metadata, sandboxing, permissions, credentials, resource limits, materialization boundaries, redaction/audit policy, license/provenance, and no-execution inspection behavior.
- Observability, deployment, security/governance, and extension-safety evidence now feed workload constitutions, best-choice scorecards, best-default dossiers, and capability snapshots.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0010, RFC 0011, RFC 0012, RFC 0018, RFC 0019, RFC 0023, RFC 0024, RFC 0030, and RFC 0031.
- This phase adds no runtime behavior, SQL parser, SQL execution, API implementation, server implementation, UDF/plugin runtime, adapter runtime, dependency, external probing, superiority claim, or fallback behavior.

## R5.4.11 architecture document ownership cleanup

- `phased-execution-plan.md` now explicitly owns active status, active queue, completed phase ledger, deferred work, and CG closeout state.
- Supporting architecture docs now identify whether they are traceability maps, sequencing ledgers, cleanup backlogs, inventories, reference maps, or vocabulary references.
- Cleanup and sequencing docs use checklist/completed-ledger structure where status tracking is meaningful.
- Conceptual and reference docs use structured maps and guardrails rather than misleading completion checklists.
- Primary RFC linkage: RFC 0012, RFC 0024, RFC 0025, and RFC 0030.
- Related RFCs: RFC 0010, RFC 0011, RFC 0013, RFC 0014, RFC 0017, RFC 0018, RFC 0019, RFC 0020, RFC 0031, and RFC 0032.
- This phase adds no runtime behavior, parser, execution, API implementation, server implementation, UDF/plugin runtime, adapter runtime, dependency, external probing, superiority claim, CG closeout, or fallback behavior.

## R5.4.12 common data/ETL and Python/media surface expansion

- RFC 0032 now defines CG-20 coverage for common data/ETL surfaces beyond SQL, including ingestion, schema contracts, data quality, cleaning, transformation, enrichment, incremental state, write/export, partition/layout behavior, bounded streaming, memory/spill, lineage/provenance, governance, and pipeline observability.
- RFC 0032 now places mature Python wrapper/API, DataFrame/query-builder, notebook, Python UDF, and Python packaging certification under CG-20 user capability, starting with a thin stable CLI/API JSON client and requiring explicit diagnostics and materialization boundaries.
- RFC 0032 now clarifies that CG-11 supplies API/protocol foundation while CG-20 owns mature Python and user-capability certification.
- RFC 0032 now expands universal adapter coverage to partitioned datasets, compressed wrappers, relational/warehouse sources, event/API/SaaS sources, Python/notebook surfaces, and unstructured/media references.
- RFC 0032 now defines unstructured/media capability boundaries for typed references, extracted text/chunks/metadata, extractor provenance, redaction, effect permissions, materialization costs, and unsupported diagnostics.
- Workload constitutions, scorecards, best-default dossiers, and sufficiency reports now include data/ETL, Python, and unstructured/media evidence where those surfaces are in scope.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0010, RFC 0011, RFC 0012, RFC 0013, RFC 0018, RFC 0019, RFC 0023, RFC 0030, and RFC 0031.
- This phase adds no runtime behavior, parser, SQL execution, Python package, adapter runtime, media runtime, OCR/LLM/embedding dependency, external probing, superiority claim, or fallback behavior.

## R5.4.13 README roadmap source-of-truth cleanup

- README now acts as a stable project entry point rather than a mutable implementation-status ledger.
- README points active implementation state to `docs/architecture/phased-execution-plan.md`.
- README preserves the no-fallback policy and evidence-gated claim rule.
- README names CG-20 user-capability surfaces such as SQL, Python/API, DataFrame/query builder, notebook, UDF, common ETL, universal adapters, and unstructured/media workflows without claiming implementation completion.
- Primary RFC linkage: RFC 0025 and RFC 0032.
- Related RFCs: RFC 0012, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no runtime behavior, parser, execution, adapter runtime, Python package, media runtime, dependency change, benchmark claim, superiority claim, or fallback behavior.

## CG-13.1 encoded path selection report foundation

- `shardloom-vortex/src/encoded_path_selection.rs` adds a report-only `VortexEncodedExecutionPathSelectionReport` for CG-13 count/filter/project encoded-native candidate selection.
- The report composes existing physical operator profiles, encoded count discovery, encoded predicate discovery, selection-vector filter discovery, and encoded projection evidence into one agent-readable artifact.
- `vortex-encoded-path-selection-plan` exposes the report through stable CLI JSON/text output with selected execution levels, evidence sources, decode/materialization avoided counts, selection-vector preservation, and explicit no-work/no-fallback fields.
- The path selection report does not read data, decode arrays, materialize values, read rows, convert to Arrow, touch object stores, write, spill, execute runtime paths, invoke external engines, or allow fallback.
- Primary RFC linkage: RFC 0026 and RFC 0021.
- Related RFCs: RFC 0012, RFC 0015, RFC 0025, RFC 0029, RFC 0031, and RFC 0032.
- This phase adds no generalized encoded execution, scan/read-start API, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, CG closeout, or fallback behavior.

## CG-14.1 adaptive optimizer and memory decision report foundation

- `shardloom-plan/src/optimizer.rs` adds `AdaptiveOptimizerMemoryReport`, a report-only contract for runtime-adaptive optimizer and execution-memory decision evidence.
- The report records deferred optimizer rules for conservative runtime filters, dynamic pruning proof gates, and memory/spill-aware planning boundaries.
- The report surfaces candidate adaptive decisions for memory pressure and runtime-filter availability while explicitly recording that no runtime adaptation is applied.
- `optimizer-adaptive-memory-plan` exposes deterministic JSON/text fields for rule counts, conservative runtime-filter counts, adaptive decision counts, skew signal representation, memory/spill proof requirements, side-effect boundaries, and no-fallback status.
- Primary RFC linkage: RFC 0016 and RFC 0014.
- Related RFCs: RFC 0012, RFC 0013, RFC 0015, RFC 0021, RFC 0025, RFC 0027, RFC 0029, RFC 0031, and RFC 0032.
- This phase adds no optimizer execution, cost-model execution, runtime-filter build/apply, dynamic pruning execution, plan rewrite, join/aggregate/skew execution, allocator/reservation runtime, spill execution, object-store IO, writes, benchmark claim, production/superiority claim, CG closeout, or fallback behavior.

## CG-15.1 CPU operator specialization report foundation

- `shardloom-core/src/cpu_specialization.rs` adds `CpuOperatorSpecializationReport`, a report-only contract for commodity CPU operator specialization candidates.
- The report records filter, project, count-aggregate, aggregate, sort, and join operator/kernel candidates with SIMD, cache-aware, branch-reduced, encoded-layout-aware, and selection-vector-aware classes.
- The report requires correctness evidence, benchmark evidence, CPU feature guards, portable native baselines, and deterministic dispatch before any runtime specialization or performance claim.
- `cpu-specialization-plan` exposes deterministic JSON/text fields for candidate counts, operator/kernel order, evidence gates, CPU probing/dispatch status, unsafe/GPU/FPGA requirements, side-effect boundaries, and no-fallback status.
- Primary RFC linkage: RFC 0027 and RFC 0021.
- Related RFCs: RFC 0012, RFC 0015, RFC 0025, RFC 0029, RFC 0031, and RFC 0032.
- This phase adds no CPU feature probing, runtime dispatch, unsafe SIMD implementation, operator execution, kernel implementation, data reads, decode/materialization, Arrow conversion, object-store IO, writes, spill IO, benchmark execution, performance/superiority claim, production certification, CG closeout, or fallback behavior.

## CG-7.15 local encoded `CountAll` physical kernel evidence

- `shardloom-vortex/src/encoded_count_physical_kernel.rs` adds a contextual encoded-native physical-kernel report for the approved local encoded `CountAll` path.
- The report consumes existing local scan evidence, local execution evidence, and the CG-16 execution certificate; it does not open files, scan, decode, materialize, convert to Arrow, touch object stores, write, spill, invoke external baselines, or fallback on its own.
- `capabilities operators` and `kernel-registry` now surface the encoded count physical kernel as contextual/report-only discovery.
- The feature-gated local encoded count fixture test now verifies correctness evidence, execution certificate evidence, and encoded physical-kernel evidence for the same `CountAll` result.
- Primary RFC linkage: RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0012, RFC 0015, RFC 0016, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, filtered-count execution, projection execution, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, superiority claim, CG closeout, or fallback behavior.

## CG-7.16 local encoded `CountAll` CLI evidence surfacing

- `shardloom vortex-count <dataset_uri> --execute-local-encoded-count <memory_gb> <max_parallelism>` now surfaces fixture-matched CG-16 execution certificate evidence and CG-7.15 encoded physical-kernel evidence in the stable command output.
- Certification is emitted only when the executed local target matches the repository's `vortex-local-encoded-count-u64-20000` correctness fixture source ref; arbitrary local `.vortex` count execution remains usable without claiming fixture certification.
- The command output records fixture match status, certificate status, correctness/no-fallback fields, encoded physical-kernel status, safe-native-kernel evidence, and production-claim-disabled status.
- Primary RFC linkage: RFC 0010, RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0015, RFC 0016, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, filtered-count execution, projection execution, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, CG closeout, or fallback behavior.

## CG-7.17 encoded count aggregate kernel admission bridge

- `VortexEncodedCountKernelAdmissionReport` maps safe encoded count physical-kernel evidence into the CG-7 `PhysicalKernelAdmissionReport` gate for the count-aggregate encoded kernel slot.
- Safe evidence can make the encoded slot registry-ready, but benchmark evidence remains missing so production certification and any superiority/best-choice claims remain blocked.
- `vortex-count --execute-local-encoded-count` surfaces encoded count kernel admission fields when fixture certification is available.
- `capabilities operators` and `kernel-registry` expose admission discovery fields without probing files, executing kernels, or registering a global runtime kernel.
- Primary RFC linkage: RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0015, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, filtered-count execution, projection execution, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, broad count/aggregate closeout, CG closeout, or fallback behavior.

## CG-7.18 metadata filter kernel admission bridge

- `VortexMetadataFilterKernelAdmissionReport` maps safe metadata-only filter physical-kernel evidence into the CG-7 `PhysicalKernelAdmissionReport` gate for the filter metadata-kernel slot.
- Safe evidence can make the metadata filter slot registry-ready, but benchmark evidence remains missing so production certification and any superiority/best-choice claims remain blocked.
- `vortex-metadata-physical-kernel-plan filter` surfaces metadata filter kernel admission fields when explicit correctness and memory evidence is supplied.
- `capabilities operators` and `kernel-registry` expose admission discovery fields without probing files, executing kernels, or registering a global runtime kernel.
- Primary RFC linkage: RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0015, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, encoded predicate execution, filtered-count execution, projection execution, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, broad filter-kernel closeout, CG closeout, or fallback behavior.

## CG-7.19 metadata projection kernel admission bridge

- `VortexMetadataProjectionKernelAdmissionReport` maps safe metadata-schema projection readiness into the CG-7 `PhysicalKernelAdmissionReport` gate for the project metadata-kernel slot.
- Safe evidence can make the project metadata slot registry-ready, but benchmark evidence remains missing so production certification and any superiority/best-choice claims remain blocked.
- `capabilities operators` and `kernel-registry` expose admission discovery fields without probing files, executing kernels, or registering a global runtime kernel.
- Primary RFC linkage: RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0015, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, encoded projection execution, projection execution, row reads, requested decode/materialization, Arrow conversion, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, broad projection-kernel closeout, CG closeout, or fallback behavior.

## CG-7.20 metadata count aggregate kernel admission bridge

- `VortexMetadataCountKernelAdmissionReport` maps safe metadata-only `CountAll` and metadata-proof `CountWhere` physical-kernel evidence into the CG-7 `PhysicalKernelAdmissionReport` gate for the count-aggregate metadata-kernel slot.
- Safe evidence can make the count-aggregate metadata slot registry-ready, but benchmark evidence remains missing so production certification and any superiority/best-choice claims remain blocked.
- `vortex-metadata-physical-kernel-plan count` and `vortex-metadata-physical-kernel-plan filtered-count` surface metadata count admission fields when explicit correctness and memory evidence is supplied.
- `capabilities operators` and `kernel-registry` expose admission discovery fields without probing files, executing kernels, or registering a global runtime kernel.
- RFC 0031 wording now records the upstream Vortex Scan API source/sink/split/range-I/O lessons as a design reference for CG-19, while preserving ShardLoom-native envelopes and no-fallback execution boundaries.
- The systems-learning map now records Vortex blog lessons around lazy operators, IO/write surfaces, GPU/device paths, nested/list support, wide-table work, and benchmark visibility as design references only.
- Primary RFC linkage: RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0015, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, encoded aggregate execution, count execution beyond existing metadata/local paths, row reads, requested decode/materialization, Arrow conversion, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, broad count/aggregate closeout, CG closeout, or fallback behavior.

## CG-7.21 execution-level coverage discovery

- `PhysicalOperatorExecutionProfileMatrix` now exposes stable counts for distinct native execution levels and per-level operator profile support.
- `capabilities operators` and `kernel-registry` surface metadata-only, encoded-native, hybrid-native, and native-decoded execution-level counts for the CG-7 operator profile set.
- Reference-only execution remains rejected, and row materialization, Arrow conversion, runtime execution, and fallback execution remain disabled in discovery output.
- This closes the CG-7 metadata/encoded/hybrid execution-level checklist item as a coverage/discovery contract only; broad filter, projection, count/aggregate, and expression-evaluation kernels remain open.
- Primary RFC linkage: RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0015, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, filter execution, projection execution, aggregate execution, row reads, requested decode/materialization, Arrow conversion, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, broad operator-kernel closeout, CG closeout, or fallback behavior.

## CG-7.22 encoded segment predicate evaluation foundation

- `shardloom-core::encoded` now defines encoded predicate evaluation report/status contracts that evaluate predicates as far as segment metadata allows.
- Metadata-proven all/none predicates emit selection vectors without data reads, decode, materialization, row reads, Arrow conversion, object-store IO, writes, spill IO, runtime fallback, or external effects.
- Inconclusive predicates report `needs_encoded_values` instead of silently decoding or claiming filter execution.
- Vortex metadata summaries can emit per-segment encoded predicate evaluation reports for the filter operator path.
- `capabilities operators` and `kernel-registry` expose report-only encoded predicate evaluation discovery fields.
- Primary RFC linkage: RFC 0012, RFC 0015, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no scan/read-start path, broad encoded-data execution, broad filter execution, projection execution, aggregate execution, row reads, requested decode/materialization, Arrow conversion, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, broad operator-kernel closeout, CG closeout, or fallback behavior.

## CG-7.23 selection-vector filter kernel evidence

- `shardloom-vortex` now evaluates contextual selection-vector filter-kernel evidence from successful encoded predicate evaluation reports.
- Safe reports can mark the encoded filter kernel slot registry-ready, while benchmark evidence remains required before production certification or any superiority claim.
- Inconclusive predicates remain blocked as `needs_encoded_values` and do not decode, materialize, convert to Arrow, execute fallback, or claim broad filter execution.
- `capabilities operators` and `kernel-registry` expose selection-vector filter-kernel discovery and admission fields.
- Primary RFC linkage: RFC 0012, RFC 0015, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no scan/read-start path, generalized encoded-data execution, encoded-value predicate execution, broad filter execution, projection execution, aggregate execution, row reads, requested decode/materialization, Arrow conversion, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, broad operator-kernel closeout, CG closeout, or fallback behavior.

## CG-7.24 encoded projection kernel evidence

- `shardloom-vortex` now admits safe encoded-column projection readiness into the encoded project kernel slot.
- Safe reports can mark the encoded project kernel slot registry-ready, while benchmark evidence remains required before production certification or any superiority claim.
- Missing encoded-column readiness blocks admission and does not read encoded data, decode, materialize, convert to Arrow, execute fallback, or claim broad projection execution.
- `capabilities operators` and `kernel-registry` expose encoded projection-kernel admission fields.
- Primary RFC linkage: RFC 0012, RFC 0015, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no scan/read-start path, generalized encoded-data execution, encoded-value projection execution, broad projection execution, aggregate execution, row reads, requested decode/materialization, Arrow conversion, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, broad operator-kernel closeout, CG closeout, or fallback behavior.

## CG-7.25 count/aggregate kernel closeout

- CG-7 count/aggregate kernel coverage is complete for the declared CG-7 scope through existing encoded `CountAll` physical-kernel evidence/admission and metadata `CountAll`/`CountWhere` count-aggregate admission.
- `shardloom-vortex/src/encoded_count_physical_kernel.rs` provides local encoded `CountAll` physical-kernel evidence and encoded count-aggregate admission.
- `shardloom-vortex/src/metadata_physical_kernel.rs` provides metadata-only `CountAll` and metadata-proof `CountWhere` count-aggregate admission.
- `capabilities operators`, `kernel-registry`, `vortex-count --execute-local-encoded-count`, and `vortex-metadata-physical-kernel-plan` expose the count-aggregate evidence chain.
- With filter, projection, count-aggregate, execution-level, and encoded segment evaluation checklist items complete, CG-7 is marked complete in the phase plan.
- Primary RFC linkage: RFC 0012, RFC 0015, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no scan/read-start path, new count execution, new aggregate execution, generalized encoded-data execution, row reads, requested decode/materialization, Arrow conversion, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, CG-2 closeout, or fallback behavior.

## CG-8.1 streaming plan discovery surface

- `streaming-plan` is now listed in the public CLI usage surface.
- `streaming-plan --format json` emits stable plan fields for mode/status, source kind/capability/zero-decode, sink kind/capability/encoded acceptance/materialization/metadata preservation, backpressure, memory policy, best work level, runtime execution, and fallback status.
- Vortex-native targets surface zero-decode planning with encoded-preserving sink requirements and no materialization boundary.
- Compatibility targets surface materialization-required boundaries and metadata-preservation loss without treating the compatibility sink as fallback execution.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0021, RFC 0031, and RFC 0032.
- This phase adds no stream execution, task execution, read-start API, row reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, CG-8 closeout, or fallback behavior.

## CG-8.2 adaptive sizing, memory, scheduler, and bounded execution evidence surface

- `vortex-adaptive-sizing --format json` now emits stable fields for adaptive sizing status/mode, segment input count, planned task count, split decisions, coalesce candidates, estimate blockers, keep-single decisions, metadata-only decisions, split/coalesce policy, and target/min/max task bytes.
- `vortex-memory-plan --format json` now emits stable fields for memory bridge status/mode, memory budget bytes, spill policy, task memory-safety counts, spill-required counts, spill-plan count, side-effect flags, and fallback status.
- `vortex-schedule-plan --format json` now emits stable fields for scheduler status/mode, max parallelism, bounded batch counts, scheduled/metadata/blocked/unsupported task counts, bounded parallelism enforcement, future-action status, side-effect flags, and fallback status.
- `vortex-bounded-local-exec --format json` now emits stable fields for bounded execution status/mode, local execution status/mode, completed/deferred/blocked counts, decision count, max parallelism, side-effect flags, result-known status, and fallback status.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0021, RFC 0031, and RFC 0032.
- This phase adds no stream execution, new task execution, new read-start API, row reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO, dynamic sizing feedback execution, benchmark claim, production/superiority claim, CG-8 closeout, or fallback behavior.

## CG-8.3 bounded backpressure planning surface

- `BackpressurePlanInput` and `BackpressurePlanReport` now model bounded-memory backpressure planning with status/mode, max parallelism, max in-flight chunks, max buffered bytes, optional estimated chunk bytes, side-effect flags, diagnostics, and fallback status.
- `plan_backpressure` derives a bounded `BackpressurePolicy` from required bounded memory and max parallelism; missing budgets and zero parallelism fail explicitly.
- `backpressure-plan --format json` exposes stable backpressure fields for agents and CI without executing streams or tasks.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0031, and RFC 0032.
- This phase adds no stream execution, task execution, read-start API, row reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO, dynamic sizing feedback execution, benchmark claim, production/superiority claim, CG-8 closeout, or fallback behavior.

## CG-8.4 dynamic sizing feedback planning surface

- `DynamicSizingFeedbackInput` and `DynamicSizingFeedbackReport` now model advisory feedback signals for target-task-byte adjustment with status/mode, signal counts, current/recommended policy, side-effect flags, diagnostics, and fallback status.
- `plan_dynamic_sizing_feedback` treats memory-pressure and too-large-task signals as safer target-reduction evidence, too-small-task and object-store-throttling signals as target-increase evidence, mixed signals as safer reduction, and no signals as no feedback.
- `sizing-feedback-plan --format json` exposes stable fields for feedback status/mode, signal counts, current/recommended target bytes, unchanged execution effects, and fallback-disabled evidence.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0031, and RFC 0032.
- This phase adds no stream execution, task execution, feedback application, read-start API, row reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO, benchmark claim, production/superiority claim, CG-8 closeout, or fallback behavior.

## CG-8.5 encoded streaming-batch planning surface

- `EncodedStreamingBatchPlanInput` and `EncodedStreamingBatchPlanReport` now model encoded streaming-batch planning with representation state, zero-decode status, bounded parallelism, bounded memory, backpressure, materialization boundary, diagnostics, and side-effect flags.
- `plan_encoded_streaming_batches` preserves Vortex-encoded batch representation for native Vortex source/sink plans, reports compatibility-sink materialization boundaries, and blocks object-store byte-range sources until object-store streaming IO lands.
- `streaming-batch-plan --format json` exposes stable fields for batch status, source/sink kind, representation, zero-decode, encoded preservation, max parallelism, memory, backpressure, materialization, side-effect flags, and fallback-disabled evidence.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0031, and RFC 0032.
- This phase adds no stream execution, task execution, read-start API, encoded data reads, row reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO, dynamic sizing feedback application, benchmark claim, production/superiority claim, CG-8 closeout, or fallback behavior.

## CG-8.6 bounded metadata/no-op local task execution

- `VortexBoundedExecutionMode::MetadataOnly` and `VortexBoundedExecutionMode::NoOp` now report task execution because their bounded decisions complete local work.
- `VortexBoundedExecutionReport::tasks_executed` derives from completed metadata-only or no-op bounded decisions, while data-read, decode, materialization, object-store, write, spill, external-effect, and fallback fields remain false.
- Local-engine reports now propagate nested local and bounded effect flags so `tasks_executed=true` is visible when bounded metadata/no-op work completes.
- Policy-disabled metadata tasks stay `ReadyButNoExecutableTasks`, keep `tasks_executed=false`, and remain side-effect-free.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0021, RFC 0031, and RFC 0032.
- This phase adds no stream runtime execution, bounded parallel encoded/read execution, read-start API, encoded data reads, row reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO, dynamic sizing feedback execution, benchmark claim, production/superiority claim, CG-8 closeout, or fallback behavior.

## CG-8.7 approved local encoded streaming-batch runtime evidence

- `VortexStreamingBatchRuntimeReport` records schema, status, mode, representation, zero-decode, bounded-memory, backpressure, source-match, batch-count, row-count, count-result, side-effect, diagnostic, and no-fallback fields for the approved local encoded count path.
- Runtime evidence requires an already planned zero-decode Vortex streaming-batch source/sink path with no materialization boundary.
- Runtime evidence requires a successful approved local scan encoded-count execution report, and the streaming source URI must match the local scan target URI.
- Unsafe reports are rejected if they include decode, materialization, row reads, Arrow conversion, object-store IO, writes, spill, external effects, fallback, or source mismatch.
- Stable `vortex-count --execute-local-encoded-count` now surfaces streaming-batch runtime evidence beside existing local execution, execution-certificate, physical-kernel, and kernel-admission evidence.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0021, RFC 0029, RFC 0031, and RFC 0032.
- This phase adds no broad streaming runtime execution for arbitrary query plans, bounded parallel encoded/read execution, new scan/read-start API, new encoded data read path beyond the approved local count scan, filtered-count/projection execution, row reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO, dynamic sizing feedback execution, benchmark claim, production/superiority claim, CG-8 closeout, or fallback behavior.

## CG-9.1 schema evolution compatibility evidence

- `SchemaEvolutionCompatibilityReport` records compatibility level, safe/unsafe change counts, field-id requirements, projection/cast/default requirements, metadata-loss reporting, read/write support, no-IO fields, diagnostics, and fallback-disabled evidence.
- `evaluate_schema_evolution_compatibility` compares typed schema definitions for add, drop, rename, safe widening, narrowing, nullability, field-identity, and metadata changes without touching catalogs, data files, object stores, writes, or execution paths.
- Safe rename evidence requires stable field IDs; possible renames without field IDs are rejected deterministically with no fallback attempted.
- `schema-plan evolution` surfaces representative add-nullable, rename-with-id, rename-without-id, drop-field, widen, and narrow scenarios for human and agent-facing compatibility evidence.
- Primary RFC linkage: RFC 0020 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0019, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no catalog access, table metadata reads, object-store IO, data reads, writes, commits, external table-format implementation, partition evolution, delete/tombstone execution, CDC execution, layout-health execution, compaction execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-9.2 partition evolution compatibility evidence

- `PartitionEvolutionCompatibilityReport` records compatibility level, partition changes, preserved/added/dropped/transform/reorder/unsafe counts, partition-router requirements, metadata-rewrite requirements, repartition requirements, read/write support, no-IO fields, diagnostics, and fallback-disabled evidence.
- `evaluate_partition_evolution_compatibility` compares typed partition specs for add, drop, transform-change, reorder, and unknown-transform transitions without touching catalogs, table metadata, data files, object stores, writes, repartition execution, or fallback paths.
- Known partition changes surface routing, metadata rewrite, or repartition requirements instead of pretending the old and new specs are interchangeable.
- Unknown partition transforms are rejected deterministically with no fallback attempted.
- `table-compat-plan partition-evolution` surfaces representative same, add-field, change-transform, drop-field, reorder, and unknown-transform scenarios for human and agent-facing compatibility evidence.
- Primary RFC linkage: RFC 0020 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0019, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no catalog access, table metadata reads, object-store IO, data reads, writes, commits, external table-format implementation, delete/tombstone execution, CDC execution, layout-health execution, compaction execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-9.3 delete/tombstone compatibility evidence

- `DeleteTombstoneCompatibilityReport` records source/target delete model, compatibility level, preservation flags, native handling requirements, metadata-loss reporting, unsupported/unsafe counts, read/write support, no-IO fields, diagnostics, and fallback-disabled evidence.
- `evaluate_delete_tombstone_compatibility` compares declared delete/tombstone models without touching catalogs, table metadata, data files, object stores, delete files, tombstone filters, writes, or fallback paths.
- Initial compatibility is limited to `none` and `file_level_delete`. Segment tombstones, row-level deletes, position deletes, equality deletes, external table metadata, metadata-loss transitions, and unknown models are blocked behind explicit native rules.
- `table-compat-plan delete-semantics` surfaces representative none, file-level, file-to-none, segment-tombstone, row-level, position-delete, equality-delete, external-table-metadata, and unknown scenarios for human and agent-facing compatibility evidence.
- Primary RFC linkage: RFC 0020 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0017, RFC 0019, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no catalog access, table metadata reads, object-store IO, data reads, writes, commits, external table-format implementation, delete-file application, tombstone filtering, row-delete execution, position-delete execution, equality-delete execution, CDC execution, layout-health execution, compaction execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-9.4 table compatibility evidence aggregation

- `TableCompatibilityReport` aggregates schema-evolution, partition-evolution, and delete/tombstone compatibility reports while retaining side-effect and no-fallback flags.
- Aggregate read/write support is blocked when any nested report has errors or reports unsupported behavior.
- Nested diagnostics from schema, partition, and delete/tombstone reports are surfaced through `table-compat-plan aggregate`.
- `table-compat-plan aggregate` surfaces representative compatible, schema-blocked, partition-blocked, and delete-blocked scenarios for human and agent-facing compatibility evidence.
- Primary RFC linkage: RFC 0020 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0017, RFC 0019, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no catalog access, table metadata reads, object-store IO, data reads, writes, commits, external table-format implementation, delete-file application, tombstone filtering, row-delete execution, position-delete execution, equality-delete execution, CDC execution, layout-health execution, compaction execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-9.5 CDC incremental planning evidence

- `CdcIncrementalPlanningReport` records declared `ChangeSet`, incremental-plan, CDC event, status, count, requirement, diagnostic, side-effect, and no-fallback evidence.
- `evaluate_cdc_incremental_planning` routes append-only and metadata-only CDC summaries as plan-only evidence when a source/target snapshot pair exists.
- Updates, deletes, tombstones, schema changes, partition changes, unknown events, unknown segment changes, and missing snapshot pairs are rejected until native compatibility evidence exists.
- `incremental-plan cdc` surfaces representative append-only, metadata-only, delete, upsert, schema-change, partition-change, missing-from-snapshot, and unknown scenarios for human and agent-facing planning evidence.
- Primary RFC linkage: RFC 0004, RFC 0020, and RFC 0025.
- Related RFCs: RFC 0012, RFC 0015, RFC 0017, RFC 0019, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no catalog access, table metadata reads, object-store IO, data reads, writes, commits, external table-format implementation, delete-file application, tombstone filtering, row-delete execution, position-delete execution, equality-delete execution, CDC execution, layout-health execution, compaction execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-9.6 layout health planning evidence

- `LayoutHealthReport` records declared manifest, policy, issue, status, count, requirement, diagnostic, side-effect, compaction-execution-disabled, and no-fallback evidence.
- `evaluate_layout_health` detects small files, small segments, missing statistics, missing byte ranges, mixed formats, mixed encodings, mixed layouts, and non-native data-file evidence from already-declared manifest metadata.
- `layout-health-plan` surfaces representative healthy, small-files, missing-stats, mixed-layout, and empty scenarios for human and agent-facing planning evidence.
- Empty manifests are rejected; compaction recommendations are planning evidence only and do not run maintenance or writes.
- Primary RFC linkage: RFC 0016, RFC 0020, and RFC 0025.
- Related RFCs: RFC 0004, RFC 0008, RFC 0012, RFC 0014, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no layout-reader construction, catalog access, table metadata reads, object-store IO, data reads, writes, commits, external table-format implementation, delete-file application, tombstone filtering, row-delete execution, position-delete execution, equality-delete execution, CDC execution, compaction execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-9.7 compaction planning evidence

- `CompactionPlanningReport` records layout-health input, policy, status, action, count, blocker, estimated group, side-effect, compaction-execution-disabled, and no-fallback evidence.
- `evaluate_compaction_planning` consumes declared manifest metadata through `LayoutHealthReport` and emits future maintenance recommendations only when small-file/small-segment candidates have sufficient metadata and layout evidence.
- Missing statistics or byte ranges block recommendation emission behind metadata refresh/index requirements.
- Mixed formats, mixed encodings, mixed layouts, and non-native data files block recommendation emission behind layout or adapter-fidelity review.
- `compaction-plan` surfaces representative healthy, small-files, missing-stats, mixed-layout, and empty scenarios for human and agent-facing planning evidence.
- Empty manifests are rejected; recommendations are not executable tasks, write intents, commit intents, catalog updates, object-store operations, or compaction execution.
- Primary RFC linkage: RFC 0016, RFC 0020, and RFC 0025.
- Related RFCs: RFC 0004, RFC 0008, RFC 0012, RFC 0014, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no layout-reader construction, catalog access, table metadata reads, object-store IO, data reads, writes, commits, external table-format implementation, delete-file application, tombstone filtering, row-delete execution, position-delete execution, equality-delete execution, CDC execution, compaction execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-10.1 object-store range planning evidence

- `ObjectStoreRangePlanningReport` records declared manifest, policy, status, request-shape, count, blocker, estimated byte, side-effect, full-file-read-disallowed, object-store-IO-disabled, and no-fallback evidence.
- `plan_object_store_ranges` emits request shapes only from already-declared S3/GCS/ADLS segment byte ranges.
- Empty manifests, local/non-object-store inputs, missing byte ranges, invalid ranges, and oversized ranges are blocked with deterministic diagnostics.
- Missing byte ranges do not silently degrade into full-file reads; `full_file_read_allowed=false` remains explicit.
- `object-store-range-plan` surfaces representative s3-ranges, missing-ranges, local-file, invalid-range, oversized-range, and empty scenarios for human and agent-facing planning evidence.
- Primary RFC linkage: RFC 0008 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0014, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no object-store IO, file IO, data reads, row reads, decode/materialization, Arrow conversion, request execution, retry execution, network probing, writes, commits, distributed execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-10.2 object-store request coalescing evidence

- `ObjectStoreRequestCoalescingReport` records uncoalesced and coalesced range reports, decisions, status, request reduction, estimated bytes, side-effect, object-store-IO-disabled, and no-fallback evidence.
- `plan_object_store_request_coalescing` compares request-shape plans without executing reads, retries, provider probes, or network calls.
- Coalescing is blocked whenever range planning is blocked by missing byte ranges, invalid ranges, request-budget violations, or non-object-store input evidence.
- `object-store-coalesce-plan` surfaces representative s3-ranges and missing-ranges scenarios for human and agent-facing planning evidence.
- Request reduction is planning evidence only, not a benchmark, latency, or superiority claim.
- Primary RFC linkage: RFC 0008 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0014, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no object-store IO, file IO, data reads, row reads, decode/materialization, Arrow conversion, request execution, retry execution, network probing, writes, commits, distributed execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-10.3 object-store commit protocol planning evidence

- `ObjectStoreCommitProtocolReport` records declared commit-protocol input, status, diagnostics, object-store target status, unmet readiness evidence, no-IO/no-write side-effect flags, and no-fallback evidence.
- `plan_object_store_commit_protocol` validates declared staging prefix, manifest pointer update, commit record, idempotency key, cleanup plan, and atomicity evidence without executing commits or contacting storage.
- Non-object-store targets, missing staging, missing manifest pointer evidence, missing commit record, missing idempotency, missing cleanup, and missing atomicity evidence are blocked with deterministic diagnostics.
- `object-store-commit-plan` surfaces representative ready, missing-staging, missing-idempotency, missing-atomicity, and local-file scenarios for human and agent-facing planning evidence.
- Commit protocol readiness is planning evidence only; object-store writes, provider-specific atomicity, recovery cleanup, and distributed commit coordination remain separate gates.
- Primary RFC linkage: RFC 0008 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0014, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no object-store IO, file IO, data reads, row reads, decode/materialization, Arrow conversion, request execution, retry execution, network probing, writes, commit execution, cleanup execution, distributed execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-10.4 object-store distributed scheduling planning evidence

- `ObjectStoreDistributedSchedulingReport` records request coalescing input, scheduling policy, status, task-shape plans, diagnostics, task counts, retry/checkpoint/idempotency requirements, no coordinator/worker/task-execution flags, and no-fallback evidence.
- `plan_object_store_distributed_scheduling` groups successful coalesced object-store requests into stable task ids without starting a coordinator, starting workers, executing tasks, or contacting storage.
- Blocked coalescing, empty requests, task-budget overflow, and invalid policy limits are rejected with deterministic diagnostics.
- `object-store-schedule-plan` surfaces representative s3-ranges, multi-task, missing-ranges, task-budget, and invalid-policy scenarios for human and agent-facing planning evidence.
- Scheduling evidence records checkpoint/retry/idempotency requirements, but the actual checkpoint/retry/idempotency readiness gate remains separate before distributed execution can be considered.
- Primary RFC linkage: RFC 0008 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0014, RFC 0016, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no object-store IO, file IO, data reads, row reads, decode/materialization, Arrow conversion, request execution, retry execution, checkpoint writes, network probing, writes, commit execution, cleanup execution, coordinator runtime, worker runtime, task execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-10.5 object-store checkpoint/retry/idempotency planning evidence

- `ObjectStoreCheckpointRetryReport` records distributed scheduling input, reliability evidence flags, status, diagnostics, task counts, retryable task counts, planned checkpoint/attempt record counts, no retry/checkpoint/cleanup execution flags, and no-fallback evidence.
- `plan_object_store_checkpoint_retry` requires successful distributed scheduling plus declared retry policy, checkpoint plan, idempotency keys, attempt records, and cleanup policy before readiness.
- Blocked scheduling, missing retry policy, missing checkpoint plan, missing idempotency keys, missing attempt records, and missing cleanup policy are rejected with deterministic diagnostics.
- `object-store-checkpoint-retry-plan` surfaces representative ready, missing-retry, missing-checkpoint, missing-idempotency, missing-attempt, missing-cleanup, and blocked-scheduling scenarios for human and agent-facing planning evidence.
- Checkpoint/retry/idempotency readiness is planning evidence only; retry execution, checkpoint writes, attempt record writes, cleanup execution, and distributed runtime remain separate gates.
- Primary RFC linkage: RFC 0008 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0014, RFC 0016, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no object-store IO, file IO, data reads, row reads, decode/materialization, Arrow conversion, request execution, retry execution, checkpoint writes, attempt record writes, network probing, writes, commit execution, cleanup execution, coordinator runtime, worker runtime, task execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback behavior.
