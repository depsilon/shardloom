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
- CG-2.0 is current and adds a report-only, feature-gated `Vortex` query primitive readiness boundary for count, filtered count, projection, and predicate/filter primitives.
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


| CG-1.3 - encoded-read no-materialization / no-`Arrow` invariant closeout (complete for current contract surfaces) | RFC 0025 Competitive/no-fallback; RFC 0026 `Vortex` encoded-read/query-readiness boundaries | RFC 0015 Correctness/testing | Keep report-contract only outside the feature-gated CG-1.2d.9 local metadata/footer invocation; no scan/read-start; no decode/materialization/`Arrow` conversion; no object-store IO/writes; no fallback execution | Closes invariant gates for no broad row materialization and no `Arrow`-default conversion across current report surfaces; CG-1.2d.9 clears local metadata/footer invocation; CG-2.1 execution remains blocked pending query wiring and encoded data path readiness. |


## CG-2.1 count readiness planning update

- CG-1.3 invariant contract tests are complete.
- CG-2.0 / CG-2.0b / CG-2.0c / CG-2.0c.1 are complete.
- CG-2.1 is current with a report-only `VortexCountReadinessRequest`/`VortexCountReadinessReport` planning contract.
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
- Current upstream public surfaces for data access remain blocked for actual count execution because they route through scan/data-read or array-stream/evaluation APIs that are not yet approved under ShardLoom's no-decode/no-materialization boundary.
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
- The current public API boundary remains blocked: `VortexFile::row_count` is metadata count evidence, but execution-usable data path count is zero and scan/stream/evaluation/data-source surfaces remain blocked or deferred.
- This pass makes the remaining blocker explicit before actual encoded-data count execution work.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data, read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt fallback execution.

## CG-2.1e.7 encoded-count approval CLI surfacing

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `shardloom vortex-encoded-count-approval-plan` now surfaces `VortexEncodedCountDataPathApprovalReport` in text/JSON CLI envelopes.
- The command is report-only: current public API blockers remain visible and ready encoded-data count inputs return deterministic unsupported/non-zero status until an execution-usable data path exists.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data, read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt fallback execution.

## CG-2.1e.8 encoded-count approval local guard

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `execute_vortex_count_all_from_encoded_count_data_path_approval` now requires `VortexEncodedCountDataPathApprovalReport` before local encoded-count planning can advance.
- The current public API boundary is rejected by this guard; a future approved boundary can only produce deferred `NeedsEncodedRead`, not actual scan/data execution.
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
- The current public API boundary remains blocked unless local fixture scope, caller session, runtime-driver permission, row-count-only intent, no scan/evaluation/data-read/decode/materialization/Arrow/object-store/write, and no-fallback signals are explicit.
- Even approved reports construct no `LayoutReader`, start no driver, call no scan/evaluation API, read no data or rows, decode/materialize nothing, convert nothing to `Arrow`, perform no object-store IO or writes, and do not allow fallback.
- This pass adds no runtime invocation, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-2.1e.11 layout-driver approval CLI surfacing

- Primary RFC linkage: RFC 0010 Developer Experience, RFC 0012 Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `shardloom vortex-layout-driver-approval-plan <signals> [--format text|json]` exposes the layout-driver approval report for human and agent inspection.
- The command consumes only explicit signal text and the static encoded-read public API boundary report; it performs no filesystem, network, catalog, adapter, scan, evaluation, or data-read probing.
- Missing/unknown signals fail deterministically, and the current public API boundary remains unsupported unless runtime-driver permission is explicit.
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

## CG-6.1 benchmark evidence manifest

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0015 Correctness/Semantics/Differential Testing, RFC 0025 Competitive/no-fallback, and RFC 0032 capability certification gates.
- `shardloom-core/src/benchmark.rs` expands benchmark metric vocabulary for startup/runtime/write latency, peak memory, bytes read/written/decoded/avoided, materialization avoided, segments considered/pruned/metadata-answered, object-store requests, spill required/avoided, and work avoided.
- `BenchmarkPlan::default_foundation_plan` now covers CG-6 metric categories in report-only scenarios and keeps baselines comparison-only with fallback disabled.
- `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verifies required metric coverage and correctness validation mode presence before any claim can rely on the benchmark plan.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior, superiority claim, dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-6.2 benchmark claim gate

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0015 Correctness/Semantics/Differential Testing, RFC 0025 Competitive/no-fallback, and RFC 0032 claim publication requirements.
- `BenchmarkClaimGate` blocks performance, superiority, cost, replacement, or best-default publication unless correctness evidence, benchmark evidence, required metrics, comparison reports, and no-fallback evidence are all present.
- `BenchmarkPlan::claim_gate` returns `evidence_missing` for the current report-only foundation plan because no benchmark runner or comparison report exists yet.
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
- The current foundation plan remains `incomplete` because no approved benchmark runner has produced complete reproducibility metadata or benchmark results.
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
- Metadata-proof filtered count remains blocked until a dedicated predicate-proof contract is introduced.
- Filtered-count execution is not implemented.
- No scan/read-start, predicate evaluation, encoded-data read, row read, decode, materialization, `Arrow` conversion, object-store IO, writes, or fallback execution are added.
- CG-2.2b CLI integration is complete via `shardloom vortex-filtered-count-readiness-plan <candidate_source> <dataset_uri> [flags] [--format text|json]`.
- Keep CG-1 through CG-20 visible and current.
- The command does not execute filtered count, does not evaluate predicates, does not call scan/read-start APIs, and performs no metadata/footer open, encoded-data read, row read, decode/materialization, `Arrow` conversion, object-store IO, writes, or fallback execution.
- Filtered-count execution remains blocked until a real encoded predicate path or explicit metadata predicate proof execution capability exists; metadata-proof remains explicit and opt-in via `PredicateMetadataProofReady`.

## CG-2.3a projection readiness semantic hardening

- CG-2.2, CG-2.2a.1, and CG-2.2b are complete.
- CG-2.3a semantic hardening is complete.
- `ShardLoom` now provides projection-readiness planning/reporting contracts (`VortexProjectionReadinessRequest` and `VortexProjectionReadinessReport`) without projection execution.
- Projection-readiness distinguishes metadata/schema projection candidates from encoded-column projection candidates:
  - metadata/schema projection remains explicit and requires `ProjectionSupported` plus `MetadataFooterReady`;
  - encoded-column projection candidates require `EncodedDataPathReady`.
- The contract remains report-only: no scan/read-start, no projection application, no encoded-data reads, no row reads, no decode, no materialization, no `Arrow` conversion, no object-store `IO`, no writes, and no fallback execution.
- Keep CG-1 through CG-20 visible and current.

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
