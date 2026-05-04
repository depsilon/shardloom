# RFC Phase Traceability

## Purpose

RFCs are ShardLoom's source-of-truth design documents, but they are not automatically enforced by code. The phased execution plan should explicitly reference the RFCs that govern each phase so phase work can remain aligned with approved architecture and acceptance criteria.

## How to use this document

- Before starting a new phase, check the mapped RFCs for that phase.
- Do not re-read every RFC for every PR.
- Do targeted RFC checks at phase boundaries.
- If implementation diverges from an RFC, either update the RFC, add an ADR/RFC amendment, or document the deviation.
- No fallback execution remains a global invariant.

## Phase-to-RFC matrix

| Phase | Primary RFCs | Secondary RFCs | Must check before phase starts | Implementation notes |
| --- | --- | --- | --- | --- |
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
| Phase 11B.6 — recovery phase final audit before Phase 12 writes (current) | RFC 0017 Fault Tolerance, Cancellation, and Recovery | RFC 0014 Memory Management, Spill, and OOM Safety; RFC 0008 Object-Store Runtime and Distributed Task Model | recovery/cleanup/retry/cancellation report coherence; stable machine-readable fields; synthetic-spill-only scope; no retry/cancellation execution; no object-store/output recovery execution; no fallback execution | Phase-boundary audit only; confirms planning/gate surfaces before Phase 12A write-intent planning. |

| Phase 12A — Native Vortex write intent to staged output | RFC 0005 Vortex-Native File IO and Output Contract | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0020 Schema Evolution, Catalog, Table Compatibility; RFC 0024 Release Engineering | Vortex output highest fidelity; write intent and staged-output planning first; schema compatibility; metadata preservation; recovery/commit diagnostics continuity; unknown schema/delete/tombstone semantics block writes; no object-store write in Phase 12A; no fallback execution | Preserve native Vortex write fidelity and stage/validate before commit semantics. |
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
