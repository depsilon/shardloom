# RFC Phase Traceability

## Purpose

RFCs are ShardLoom's source-of-truth design documents, but they are not automatically enforced by code. The phased execution plan should explicitly reference the RFCs that govern each phase so phase work can remain aligned with approved architecture and acceptance criteria.

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
| Phase 11A — Spill policy turns real (11A.1 lifecycle/cleanup contract; 11A.2 reservations/bounded memory integration; 11A.3 spill data movement; 11A.3a.2d roundtrip API complete; 11A.3a.3 CLI/docs integration current; 11A.3b bounded execution spill payload integration planned) | RFC 0014 Memory Management, Spill, and OOM Safety | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0008 Object-Store Runtime and Distributed Task Model | memory budgets; memory reservations; spill policies; spill file refs; cleanup expectations; deterministic fail-before-OOM; no fallback execution | Keep spill posture local/native and deterministic; do not add object-store spill during this phase. |
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
| Phase 13A — Lakehouse table intelligence | RFC 0020 Schema Evolution, Catalog Integration, and Table Compatibility | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0019 Security | schema evolution; field identity; catalog compatibility; partition/spec metadata; snapshots/time travel; delete/tombstone semantics; metadata loss reporting; no silent unsafe coercion; no fallback execution | Compatibility intelligence must remain explicit, typed, and safety-first. |
| Phase 13B — Layout health, clustering, compaction planning | RFC 0016 Optimizer, Adaptive Execution, Runtime Filters, Skew | RFC 0004 Manifest/Snapshot/Incremental; RFC 0014 Memory/Spill/OOM; RFC 0020 Table Compatibility | small files/segments; overpartitioning/underpartitioning; clustering hints; compaction recommendations; work-avoided estimates; no writes unless Phase 12 path is ready | Planning/reporting phase first; write-side changes must stay gated behind Phase 12 readiness. |
| Phase 14A — Object-store read planning | RFC 0008 Object-Store Runtime and Distributed Task Model | RFC 0017 Recovery; RFC 0014 Memory/Spill/OOM; RFC 0018 Observability | object-store capability gates; metadata before reads; byte ranges before full files; request budgets; retry/backpressure policy; no object-store writes yet; no fallback execution | Enable object-store read planning with bounded, diagnosable request behavior before distributed execution. |
| Phase 14B — Distributed execution planning | RFC 0008 Object-Store Runtime and Distributed Task Model | RFC 0017 Recovery; RFC 0016 Optimizer/Adaptive; RFC 0018 Observability | coordinator/worker concepts; task attempts; retry/idempotency; checkpointing; bounded resources; no Spark/DataFusion fallback | Distributed planning must preserve bounded resources and explicit retry/recovery semantics. |
| Ongoing — Expression and kernel engine | RFC 0021 Expression Engine and Kernel Registry | RFC 0015 Correctness, Semantics, Differential Testing, Fuzzing; RFC 0023 Extension/Plugin ABI and Sandboxing | metadata kernel; encoded kernel; partial-decode kernel; decoded reference kernel only as explicit reference/test path; deterministic kernel selection; effect boundaries; no hidden fallback | Keep kernel selection deterministic and no-fallback while preserving explicit reference-only decoded paths. |
| Ongoing — Release/API/agent stability | RFC 0024 Release Engineering, API Compatibility, Packaging | RFC 0012 Diagnostics; RFC 0018 Observability; RFC 0019 Security | CLI compatibility; JSON output compatibility; diagnostic schema stability; feature footprint; benchmark claim evidence; no fallback release check | Treat compatibility and diagnostics as continuous contracts, verified at every phase boundary. |

## RFC coverage status

Status categories:
- Implemented
- Partially implemented
- Planned
- Deferred
- Needs amendment

| RFC | Current status | Relevant phases | Notes |
| --- | --- | --- | --- |
| RFC 0001 | Partially implemented | 0-3, Ongoing | Foundational architecture and no-fallback direction established; ongoing operationalization across phases. |
| RFC 0002 | Partially implemented | 2-6, Ongoing | Core contract framing in place; implementation depth still increases by phase. |
| RFC 0003 | Partially implemented | 3-10C, Ongoing | Planning/runtime skeletons exist; deeper runtime behavior remains phased. |
| RFC 0004 | Partially implemented | 12A, 12B, 13A, 13B | Manifest/snapshot/incremental model present conceptually; advanced write/commit behavior remains planned. |
| RFC 0005 | Partially implemented | 12A, Ongoing | Vortex-native output contract is established; full staged write path remains planned. |
| RFC 0006 | Partially implemented | 5-10C, Ongoing | Compatibility translation contracts exist at architecture level; enforcement/reporting evolves with later phases. |
| RFC 0007 | Planned | 10B-14B | Deeper execution/runtime scaling specifics remain mostly future-phase work. |
| RFC 0008 | Planned | 11A, 14A, 14B | Object-store runtime/distributed model intentionally deferred until later phases. |
| RFC 0009 | Partially implemented | 2-10C, Ongoing | Core policy scaffolding exists; deeper behavior and tooling continue to mature. |
| RFC 0010 | Partially implemented | 10C, 10D, Ongoing | Developer/agent usability direction set; stable interfaces continue across release phases. |
| RFC 0011 | Deferred | Ongoing (post-core) | Modular extensibility remains intentionally deferred relative to core engine phases. |
| RFC 0012 | Partially implemented | 10C, 10D, Ongoing | Diagnostics contracts exist; stabilization and propagation are explicit upcoming checkpoints. |
| RFC 0013 | Planned | 13B+, Ongoing | Streaming/zero-copy boundary work remains mostly future-phase effort. |
| RFC 0014 | Planned | 10B, 11A, 11B, 13B, 14A | Memory/spill/OOM policies are partially scaffolded but not fully realized. |
| RFC 0015 | Partially implemented | Ongoing | Correctness-first posture present; deeper differential/fuzz coverage continues over time. |
| RFC 0016 | Planned | 13B, 14B, Ongoing | Advanced optimizer/adaptive behavior remains later-phase work. |
| RFC 0017 | Planned | 11A, 11B, 12B, 14A, 14B | Recovery/cancellation/commit robustness is a remaining implementation focus. |
| RFC 0018 | Partially implemented | 10D, 14A, 14B, Ongoing | Observability foundations exist; richer tracing/profiling is still phased. |
| RFC 0019 | Partially implemented | 11B, 13A, Ongoing | Security/governance guardrails exist; advanced phase-specific controls remain planned. |
| RFC 0020 | Planned | 12A, 13A, 13B | Schema/table compatibility intelligence remains primarily a future phase domain. |
| RFC 0021 | Partially implemented | Ongoing | Expression/kernel architecture exists in principle; full kernel coverage remains ongoing. |
| RFC 0022 | Deferred | Ongoing (interop track) | Plan interoperability direction is documented; implementation remains intentionally staged. |
| RFC 0023 | Deferred | Ongoing (extension track) | Extension/plugin ABI and sandboxing are documented but not a near-term core phase focus. |
| RFC 0024 | Partially implemented | 10D, 12A, 12B, Ongoing | Release/API compatibility policy exists; continues as a cross-phase enforcement concern. |
| RFC 0025 | Planned | CG-1 through CG-20, Ongoing | Competitive Engine Track policy is documented; implementation remains gate-specific and evidence-gated. |
| RFC 0026 | Partially implemented | CG-1, CG-2, CG-13 | Encoded-read and query-primitive readiness contracts exist; real execution remains gated. |
| RFC 0027 | Planned | CG-7, CG-8, CG-14, CG-15 | CPU/vectorized/runtime adaptivity scope remains future implementation. |
| RFC 0028 | Partially implemented | CG-3, CG-4, CG-9, CG-10 | Output/commit readiness contracts exist; real payload and commit execution remain incomplete. |
| RFC 0029 | Planned | CG-5, CG-6, CG-16, CG-17 | Correctness, benchmark, certificate, and reuse evidence remain future gate work. |
| RFC 0030 | Planned | CG-11, CG-12, CG-18 | API, portability, deployment, and baseline harness work remains staged. |
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

- 12A.2a staged output workspace core contract is current and report-only.
- 12A.2b feature-gated local staged workspace/marker behavior remains planned.
- Output payload and manifest writes remain deferred.

- 12A.1 native `Vortex` write intent core contract is current.
- 12A.2 staged output workspace contract is planned.
- Actual write execution remains deferred.
## Phase 12A.3a update
- Phase 12A.2c.2 complete.
- Phase 12A.3a current: staged manifest draft core contract (report-only, no filesystem).
- Phase 12A.3b planned: feature-gated local staged manifest draft file.
- Phase 12A.3c planned: CLI/docs integration.
- Actual output payload and file writes remain deferred.

## Phase 12 refinement

- Phase 12A closeout is complete at Phase 12A.4.
- Phase 12B.1 commit-intent core contract is complete.
- Phase 12B.1b commit readiness integration is complete.
- Phase 12B.1c validation closeout is complete.
- Phase 12B.2a.1 commit protocol state machine core contract is complete and report-only.
- Phase 12B.2a.2 commit intent report integration is current.
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
  - CG-1.2d.2 deterministic async/session boundary contract: current (report-only; no runtime/executor added; metadata/footer invocation deferred to CG-1.2d.3)
    - primary RFC: RFC 0026
    - secondary RFCs: RFC 0012, RFC 0016, RFC 0025, RFC 0027, RFC 0029
    - constraints: no scan/read-start, decode, materialization, Arrow conversion, object-store IO, or fallback
- CG-2: real query primitive execution over Vortex data
- CG-3: output payload write path (placeholder artifact phases support readiness only; completion requires real executable Vortex payload writes with evidence)
- CG-4: commit protocol execution
- CG-5: correctness/differential harness
- CG-6: benchmark harness
- CG-7: physical operators/kernels
- CG-8: streaming/parallel/adaptive execution
- CG-9: lakehouse/table intelligence
- CG-10: object-store/distributed execution
- CG-11: Python/API surface later
- CG-12: plan portability / semantic IR
- CG-13: encoded-native compressed execution
- CG-14: runtime-adaptive optimizer and execution memory
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
| Phase 12C.4 — staged smoke test includes output payload artifact (complete readiness-only) | RFC 0015 Correctness, Semantics, Differential Testing, and Fuzzing | RFC 0004 Native Dataset Manifest, Snapshot, Incremental | Extends staged CLI-driven write-readiness smoke coverage with output payload plan and placeholder artifact write; verifies no real `Vortex` payload writes, no upstream `Vortex` write API calls, no manifest/commit writes, no object-store IO, fallback disabled; this is CG-3 readiness evidence only and does not complete CG-3 | Complete readiness-only milestone; not the current implementation phase. |


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

- Phase 12C placeholder output payload artifact work supports CG-3 readiness only; it does not complete CG-3.
- CG-3 completion requires a real Vortex output payload write implementation plus evidence.
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
- Actual public upstream `Vortex` metadata/footer invocation remains blocked by compile-unclear API shape; deterministic `blocked_by_unsupported_api_surface` diagnostics now record: `vortex::session::Session` not found, `VortexOpenOptions::new()` unavailable, and `OpenOptionsSessionExt` not usable in a compile-passing invocation path yet.


- CG-1.2d.5 (complete): method-shape compile probes confirm public method items for `OpenOptionsSessionExt`, `VortexOpenOptions`, and `VortexFile::footer` without invocation; metadata/footer invocation remains deferred and deterministically blocked without runtime/executor wiring.

- CG-1.2d.6 (complete): caller-provided `VortexSession` invocation contract is added under `vortex-file-io` and open-method compile probing now includes `VortexOpenOptions::open_path` method-item reference; production invocation remains deterministically blocked; CG-1.2d.8 confirms test harness ingredient limits (no local `.vortex` fixture and no confirmed no-IO `Footer` construction route), so metadata/footer execution stays open/paused without production runtime changes.
- CG-1 through CG-20 remain active Competitive Engine Track gates; this update is CG-1.2d scope only and does not change other gate statuses.

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
- CG-1 through CG-20 remain visible and active competitive gates.

## CG-2.0b helper-correctness traceability update
- CG-2.0b closes helper correctness gaps for invocation-derived query primitive requests by preserving boundary blockers/signals and preventing misclassification as `feature_disabled` when a stronger blocker exists.
- `VortexQueryPrimitiveReport::has_errors` now treats report diagnostics with `Error`/`Fatal` severity as errors in addition to status/request diagnostics.
- This remains report-only readiness planning and introduces no execution side effects.
- CLI query primitive planning command is deferred to CG-2.0c.
- CG-2.1 execution remains blocked pending metadata/footer and encoded data path readiness.

## CG-2.0c query primitive plan CLI integration
- Adds `shardloom vortex-query-primitive-plan <primitive> <dataset_uri> [flags] [--format text|json]` as a report-only/readiness-only planning command.
- Command constructs `VortexQueryPrimitiveRequest` and calls `plan_vortex_query_primitive` only; it does not execute query primitives.
- Command does not call scan/read-start APIs, does not read encoded data or rows, does not decode/materialize/Arrow-convert, does not perform object-store IO, does not write output payloads, and does not allow fallback execution.
- CG-2.1 actual count/query execution remains blocked until metadata/footer and encoded-data path readiness are both available.


| CG-1.3 — encoded-read no-materialization / no-`Arrow` invariant closeout (complete for current contract surfaces) | RFC 0025 Competitive/no-fallback; RFC 0026 `Vortex` encoded-read/query-readiness boundaries | RFC 0015 Correctness/testing | Keep report-contract only; no metadata/footer IO execution; no scan/read-start; no decode/materialization/`Arrow` conversion; no object-store IO/writes; no fallback execution | Closes invariant gates for no broad row materialization and no `Arrow`-default conversion across current report surfaces; CG-1.2d.8 metadata/footer execution remains paused; CG-2.1 execution remains blocked pending metadata/footer and encoded data path readiness. |


## CG-2.1 count readiness planning update

- CG-1.3 invariant contract tests are complete.
- CG-2.0 / CG-2.0b / CG-2.0c / CG-2.0c.1 are complete.
- CG-2.1 is current with a report-only `VortexCountReadinessRequest`/`VortexCountReadinessReport` planning contract.
- Count planning distinguishes metadata-footer candidates from encoded-data-path candidates.
- Count execution remains blocked until real metadata/footer or encoded-data-path readiness exists.
- No scan/read-start, encoded-data reads, row reads, decode, materialization, `Arrow` conversion, object-store IO, writes, or fallback execution are introduced.
- CG-2.1b `CLI` surfacing is complete via `shardloom vortex-count-readiness-plan <candidate_source> <dataset_uri> [flags] [--format text|json]`.
- CG-2.1a semantic hardening is complete: `VortexCountCandidateSource::Unknown` cannot be readiness-complete and deterministically returns `blocked_by_unsupported_primitive` when feature-gated count/query-primitive-ready signals are present.
- `VortexCountReadinessReport` error detection is severity-aware across status, request diagnostics, and report diagnostics.
- Count readiness remains report-only and does not execute count.
- `CLI` output remains report-only/readiness-only and never executes count.
- No scan/read-start, encoded-data read, row read, decode, materialization, `Arrow` conversion, object-store `IO`, writes, or fallback execution are introduced.


## CG-2.2a filtered-count readiness core contract
- CG-2.1, CG-2.1a, and CG-2.1b are complete.
- CG-2.2a adds `VortexFilteredCountReadinessRequest` and `VortexFilteredCountReadinessReport` planning/reporting only.
- CG-2.2a.1 blocker precision helper update is complete: `filtered-count` + `PredicateProvided` maps to `EncodedPredicatePath` even when encoded-data-path readiness is missing; missing encoded-data-path reports `BlockedByMissingEncodedDataPath`; non-`filtered-count` primitives remain `Unknown`; metadata predicate-proof remains deferred to explicit proof contract.
- Distinguishes `VortexFilteredCountCandidateSource::MetadataPredicateProof` vs `::EncodedPredicatePath`.
- Metadata-proof filtered count remains blocked until a dedicated predicate-proof contract is introduced.
- Filtered-count execution is not implemented.
- No scan/read-start, predicate evaluation, encoded-data read, row read, decode, materialization, `Arrow` conversion, object-store IO, writes, or fallback execution are added.
- CG-2.2b CLI integration is complete via `shardloom vortex-filtered-count-readiness-plan <candidate_source> <dataset_uri> [flags] [--format text|json]`.
- Keep CG-1 through CG-20 visible and current.
- The command does not execute filtered count, does not evaluate predicates, does not call scan/read-start APIs, and performs no metadata/footer open, encoded-data read, row read, decode/materialization, `Arrow` conversion, object-store IO, writes, or fallback execution.
- Filtered-count execution remains blocked until a real encoded predicate path or explicit metadata predicate proof execution capability exists; metadata-proof remains explicit and opt-in via `PredicateMetadataProofReady`.

## CG-2.3a projection readiness semantic hardening

- CG-2.2, CG-2.2a.1, and CG-2.2b are complete.
- CG-2.3 is current in CG-2.3a semantic hardening; CG-2.3b `CLI` is next/deferred.
- `ShardLoom` now provides projection-readiness planning/reporting contracts (`VortexProjectionReadinessRequest` and `VortexProjectionReadinessReport`) without projection execution.
- Projection-readiness distinguishes metadata/schema projection candidates from encoded-column projection candidates:
  - metadata/schema projection remains explicit and requires `ProjectionSupported` plus `MetadataFooterReady`;
  - encoded-column projection candidates require `EncodedDataPathReady`.
- The contract remains report-only: no scan/read-start, no projection application, no encoded-data reads, no row reads, no decode, no materialization, no `Arrow` conversion, no object-store `IO`, no writes, and no fallback execution.
- Keep CG-1 through CG-20 visible and current.

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
- `shardloom capabilities` without a scope remains the existing engine-level capability summary.
- Discovery output includes stable output-envelope fields for scope, schema version, fallback-disabled status, fallback-attempted status, and side-effect/probe flags.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, adapter runtime, function registry, operator kernel, dependency, filesystem/network/catalog probing, external-engine probing, or fallback behavior.
