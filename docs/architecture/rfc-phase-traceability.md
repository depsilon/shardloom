# RFC Phase Traceability

## Purpose

RFCs are ShardLoom's source-of-truth design documents, but they are not automatically enforced by
code. The phased execution plan should explicitly reference the RFCs that govern each phase so phase
work can remain aligned with approved architecture and acceptance criteria.

`docs/architecture/phased-execution-plan.md` is the source of truth for the Planned queue, deferred
work, and CG closeout ordering. `docs/architecture/phased-execution-completed-ledger.md` is the
source of truth for detailed completed session and historical phase provenance. This document maps
phase and CG work to governing RFCs; it may record historical traceability, but it must not
introduce a competing current queue.

Status words in historical sections below describe evidence recorded at the time of the original
phase note. They are not active queue state and do not override `phased-execution-plan.md`.

## How to use this document

- Before starting a new phase, check the mapped RFCs for that phase.
- Do not re-read every RFC for every PR.
- Do targeted RFC checks at phase boundaries.
- If implementation diverges from an RFC, either update the RFC, add an ADR/RFC amendment, or
  document the deviation.
- No fallback execution remains a global invariant.
- `docs/architecture/systems-learning-map.md` is conceptual reference material only and does not
  authorize dependencies or runtime fallback execution.

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
| CG-12.4 — native plan import/export serialization | RFC 0030 Universal API, Plan Portability, Import/Deployment, and External Baselines; RFC 0022 Plan IR and Substrait-Compatible Interoperability | RFC 0010 Developer Experience; RFC 0012 Diagnostics; RFC 0024 Release/API Compatibility | `NativePlanDocument` records deterministic `shardloom.native_plan.v1` serialization/import/export payloads and `plan-import native` reports imported plan ID/node count without execution | Establishes native-plan portability payload evidence without file IO, external format parsers, imported-plan execution, external engines, dependencies, or fallback execution. |
| CG-12.5 — imported-plan capability execution gate | RFC 0030 Universal API, Plan Portability, Import/Deployment, and External Baselines; RFC 0022 Plan IR and Substrait-Compatible Interoperability | RFC 0010 Developer Experience; RFC 0012 Diagnostics; RFC 0024 Release/API Compatibility; RFC 0031 Native I/O Envelope; RFC 0032 Capability Surface | `ImportedPlanCapabilityGateReport` maps imported native plan nodes and boundaries to required SQL/operator/function/adapter/native-I/O/execution-certificate certification surfaces and keeps execution blocked under contract-only evidence | Establishes the imported-plan execution safety gate without executing imported plans, probing files/network/catalog/adapters, reading/writing data, invoking external engines, adding dependencies, or allowing fallback. |
| CG-18.1 — universal harness report foundation | RFC 0030 Universal API, Plan Portability, Import/Deployment, and External Baselines | RFC 0010 Developer Experience; RFC 0012 Diagnostics; RFC 0024 Release/API Compatibility; RFC 0029 Benchmark Evidence | `UniversalHarnessReport` records CLI JSON runner fields, package/import, deployment profile, optional Foundry example, external baseline runner, comparison dataset, portability-check surfaces, external-only Spark/DataFusion/Polars baseline requirements, no probes, no runtime execution, no publish, and no fallback execution | Establishes CG-18 report-only evidence through `universal-harness-plan` without import/deployment execution, Foundry invocation, external baseline execution, comparison dataset materialization, probing, or fallback execution. |
| CG-19.1 — native I/O envelope report foundation | RFC 0031 Universal Native I/O Envelope | RFC 0012 Diagnostics; RFC 0013 Streaming/Zero-Copy/Boundary Interoperability; RFC 0008 Object-Store Runtime; RFC 0016 Optimizer/Adaptive Execution; RFC 0018 Observability | `NativeIoEnvelopeReport` records RFC 0031 contract surfaces, representation state contracts, transition examples, per-source/sink-path certificate requirements, decoded Arrow normalization prohibition, materialization boundary requirements, no probes, no reads, no decode/materialization, no writes, and no fallback execution | Establishes CG-19 report-only evidence through `native-io-envelope-plan` without source/sink runtime emission, adapter runtime, data reads, decode/materialization, Arrow conversion, object-store I/O, writes, spill I/O, or fallback execution. |
| CG-21 implementation lanes — user data workflow and ETL surface | RFC 0033 User Data Workflow and ETL Surface | RFC 0032 Capability Surface; RFC 0031 Native I/O; RFC 0030 API/Deployment; RFC 0029 Correctness/Benchmarks/Certificates; RFC 0012 Diagnostics | install/import; context/capability API; source/sink registry; DataFrame/query builder; SQL workflow; pandas/Arrow/NumPy boundaries; data contracts; local structured adapters; output/commit UX; object-store UX; table/catalog UX; observability; benchmark/migration UX; notebooks; UDFs; unstructured/media; governance; workload scorecards | Priority 4 completed local workflow/report lanes for import, context, capability, source/sink planning, query-builder planning, local adapter posture, output/remote blockers, and quickstart proof. Mature DataFrame methods (`profile`, `collect`, `to_pandas`, `to_arrow`, `write_vortex`, `write_parquet`), SQL, joins, aggregations, windows, schema/data-quality APIs, object-store/table runtime, production ETL certification, package publication, external engine fallback, and claims remain blocked unless named in Planned follow-up slices. |
| CG-22 implementation lanes — three-engine certified data execution fabric | RFC 0034 Three-Engine Certified Data Execution Fabric | RFC 0033 User Workflow; RFC 0031 Native I/O; RFC 0029 Certificates; RFC 0017 Recovery; RFC 0016 Optimizer/Adaptive; RFC 0014 Memory/Spill | engine-mode contracts; per-engine capability matrix; live source/change contract; narrow in-memory live prototype; hybrid base/delta overlay; Vortex micro-segment flush; compaction/layout planner; Python/API engine UX; state/checkpoint/freshness certification | Priority 5 completed report/fixture lanes for engine-mode contracts, per-engine matrix, live source/change contracts, narrow in-memory live fixture, hybrid overlay certification, Vortex micro-segment/layout-health, and Python/API engine UX. Production live/hybrid runtime, broker/state-store dependencies, object-store execution, freshness/exactly-once claims, external engine invocation, and fallback remain blocked until workload certification evidence is present. |
| CG-23 implementation lanes — REST, event, and remote API surface | RFC 0035 REST, Event, and Remote API Surface | RFC 0033 User Workflow; RFC 0034 Engine Fabric; RFC 0031 Native I/O; RFC 0029 Certificates; RFC 0019 Security; RFC 0018 Observability | REST/OpenAPI contract; discovery server; plan/explain/validate API; async query lifecycle; result delivery/spooling; live/hybrid event API; security/governance policy; Flight/ADBC bridge; MCP agent API | Priority 6 completed contract/report-only lanes for OpenAPI/AsyncAPI contract, discovery mode report, plan preview, lifecycle/result delivery, live/hybrid event posture, security/governance evidence, agent API, and columnar data-plane standards. No HTTP listener, remote execution, Flight/ADBC transport, external broker, data-plane runtime bridge, dependency expansion, fallback, or API production claim is authorized by these lanes alone. |
| Priority 7 - CG-21/CG-22/CG-23 integrated certification closeout | RFC 0033 User Data Workflow and ETL Surface; RFC 0034 Three-Engine Certified Data Execution Fabric; RFC 0035 REST, Event, and Remote API Surface | RFC 0029 Correctness, Benchmark, Certificates, and State Reuse; RFC 0031 Universal Native I/O Envelope; RFC 0032 Capability Surface; RFC 0039 Typed Command Result Envelope | workflow unsupported diagnostics; cross-CG capability parity; workload certification dossiers; `claim-gate-closeout` | P7.0 and P7.1 complete the report-only workflow unsupported and cross-CG capability parity surfaces. P7.2 adds workload-scoped certification dossiers that index correctness, benchmark, execution-certificate, Native I/O, capability, workflow, engine, and API evidence without runtime effects. P7.3 adds `claim-gate-closeout`, which summarizes allowed report/local fixture claims, blocked production/API/package/benchmark claims, and out-of-scope integration claims while preserving no-runtime, no-effect, no-fallback, and no-broad-claim posture before Priority 8/9 evidence exists. |
| Priority 7.4 - claim-grade compute-engine completion | RFC 0014 Memory Management; RFC 0015 Correctness and Semantics; RFC 0016 Optimizer/Adaptive Execution; RFC 0021 Expression Engine; RFC 0025 Competitive/no-fallback; RFC 0029 Certificates and Benchmarks; RFC 0031 Native I/O; RFC 0033 User Workflow; RFC 0038 Top-Level Plan/Execution Facade; RFC 0040 Benchmark Suite | RFC 0012 Diagnostics; RFC 0018 Observability; RFC 0024 Release Engineering; RFC 0032 Capability Surface; RFC 0041 Feature/Build Matrix; RFC 0042 Vortex Runtime Utilization | `compute-capability-matrix`; operator-family ladder; ShardLoomNative semantic conformance suite; DataFrame/SQL unsupported parity; execution artifact richness; measured source-backed benchmark rows; benchmark taxonomy execution; sink/write/replay proof; first workload-certified compute workflow; local scheduler/runtime; memory/spill operator maturity; Vortex layout/write advisor feedback | P7.4.1 through P7.4.7 now close the local claim-grade compute-engine completion layer: compute capability and operator-family matrices, semantic/API parity, artifact-rich execution results, measured source-backed fixture rows, local taxonomy benchmark coverage, sink/write/replay proof, first workload-certified `local_vortex_analytics_v1` workflow, scheduler/runtime and memory/spill evidence, and report-only Vortex layout/write advisor feedback. P7.4.4 closes the benchmark/source-backed parent scope by combining expanded local fixture profiles, deterministic unsupported/blocked coverage rows, selected local `--claim-readiness-rerun` evidence, separated coverage/timing tables, and claim-grade versus not-claim-grade classification. External engines remain reference/oracle/baseline-only; broad SQL/DataFrame/performance/Spark-displacement/production, object-store/table/catalog, general JSON, and general incremental-state claims remain blocked until separate workload constitutions have correctness, benchmark, certificate, Native I/O, materialization, and no-fallback evidence. |

P7.4.4 expanded-scenario update: the ShardLoom traditional analytics lane now executes the
base-schema expanded taxonomy scenarios `filter + projection + limit`, `multi-key group by`,
`join + aggregate`, `row number window`, `partition pruning`, `many-small-files scan`,
`null-heavy aggregate`, `high-cardinality string group/distinct`, and `top-N per group` through the
local Vortex import/replay/result-sink path. The update preserves Native I/O certificate, runtime
execution certificate, materialization/decode-boundary, and `fallback_attempted=false` evidence.
Dirty CSV `clean/cast/filter/write` now executes through the same local Vortex path with
dirty-column clean/cast/filter evidence; dirty-CSV `malformed timestamp / dirty CSV` and
`nested JSON field scan` now execute through the same local Vortex path for generated fixture
coverage. CDC-overlay `small change over large base` now executes through the same local Vortex
path using an explicit generated delta sidecar imported into a separate Vortex artifact. Broader
incremental-state execution, table/catalog/object-store partition-pruning, object-store multi-file,
and general JSON execution claims remain separately blocked.

P7.4.4 closeout sequence: the selected local comparative taxonomy rerun now uses
`--claim-readiness-rerun`, preserves coverage rows separately from timing rows, and promotes rows
only when the artifact contains workload-scoped correctness, benchmark, execution-certificate,
Native I/O, materialization/decode, and no-fallback evidence. Managed platforms remain design
references only.
Unsupported ShardLoom expanded taxonomy scenarios should emit deterministic unsupported or blocked
rows with `fallback_attempted=false` and `external_engine_invoked=false`, not crash and not delegate
to external engines. The harness now records `claim_gate_status`,
`claim_grade_requirements_met`, `claim_grade_missing_evidence`, and `timing_row_claim_grade` in the
coverage table so fixture-smoke, not-claim-grade, claim-grade, unsupported, blocked, and external
baseline rows are machine-distinguishable. Result-sink rows also expose
`scenario_compute_millis`, `computed_result_sink_write_millis`, `computed_result_sink_bytes`, and
`write_timing_present` so certified local write timing is separated from scenario compute timing.
The P7.4.4 harness also provides `--claim-readiness-rerun` for the selected local comparative rerun
and requires stable correctness digests across at least three iterations before a ShardLoom timing
row can set `reproducible_benchmark_row=true` and `timing_row_claim_grade=true`.
With those classifications and deterministic unsupported rows in place, P7.4.4 is complete for the
local taxonomy/source-backed benchmark closeout scope; broad object-store, table/catalog,
incremental-state, general JSON, SQL/DataFrame, and performance/public-displacement claims remain
separate blocked work.

P7.4.6 update: the feature-gated `local_vortex_analytics_v1` workflow now records local
task-graph scheduler refs, scheduled/completed task counts, bounded queue/backpressure status,
retry and cancellation gate status, memory reservation request/grant/release counts,
fail-before-OOM status, operator spill claim blockers, and a runtime execution certificate in
`traditional-analytics-run --verify-native-replay --write-result-vortex` output. This is scoped to
the certified local workflow; broad SQL/DataFrame runtime, distributed/object-store scheduling,
native spill IO, and large-workload claims remain blocked until later evidence-bearing slices.

P7.4.7 update: the same workflow now emits `TraditionalVortexLayoutAdvisorReport` evidence with
workload constitution id, benchmark/coverage refs, runtime certificate ref, Native I/O refs,
source/result size basis, scheduler ref, chunk row/byte recommendation, encoding/statistics/
dictionary/cluster recommendations, micro-segment flush policy, compaction trigger, read/write
tradeoff, measured/simulated/blocked evidence counts, and no-claim/no-write/no-fallback status. The
advisor is report-only and does not execute layout rewrites, compaction, object-store writes, or
external engines.

P7.4.8/P7.4.9 follow-up sequence: the P7.4 closeout now has a corrective benchmark-semantics lane
before release-readiness work proceeds. P7.4.8 maps to RFC 0029 benchmark evidence, RFC 0031 Native
I/O, RFC 0039 typed command/result envelopes, RFC 0040 benchmark suite hardening, and RFC 0042
Vortex runtime utilization. It must make execution-mode and timing-scope fields explicit across
benchmark rows, CLI envelopes, and Python accessors so compatibility-import-certified timings are
not confused with pure operator compute or prepared/native Vortex query timings. P7.4.9 maps to the
same RFC set plus RFC 0016 optimizer/adaptive execution and the Vortex-first provider rules: it may
add prepared/native Vortex benchmark lanes, expose fused/filter-project-limit or Scan API blockers,
and include native microbenchmark rows, but it must not use external engines as fallback or publish
performance/superiority claims.

P7.4.8/P7.4.9 completion update: benchmark artifacts now carry execution-mode fields and stage
timing attribution, and `shardloom-vortex`/`shardloom-prepared-vortex` prepared-native rows are
reported under requested CSV/JSONL/Parquet/Arrow IPC/Avro/ORC source-format rows instead of a
synthetic standalone `.vortex` format row. Prepared artifacts record preparation timing, refs,
digests, and source Native I/O status separately from scenario timing. Filter/project/limit rows
emit explicit fusion status and blockers when the temporary benchmark operator still materializes
Vortex-derived arrays. `docs/architecture/compute-engine-flow-reference.md` is now the canonical
flow reference for future changes in this lane: user request -> policy/capability admission ->
explicit execution mode -> provider admission -> result/result sink -> evidence -> claim gate. It
also records that current native Vortex rows can still use temporary ShardLoom operators until
encoded/native evidence matures, while external engines remain benchmark baselines only and never
fallback execution.

P7.5 follow-up sequence: `docs/architecture/compute-engine-flow-overhaul-review.md` records the
repo-alignment review for the compute-engine flow reference. P7.5 maps to RFC 0029 benchmark
evidence, RFC 0031 Native I/O, RFC 0039 typed command/result envelopes, RFC 0040 benchmark suite
hardening, RFC 0042 Vortex runtime utilization, RFC 0033 user workflows, and RFC 0035 future API
surface parity. It must promote execution-mode selection, prepared Vortex artifact reuse,
typed-envelope evidence, mode-aware capability rows, source-backed/native provider admission,
prepared/native result-sink proof, stage attribution, Python/REST parity, and format-preparation
matrices without adding fallback execution or public performance claims.

P7.5.1 completion update: execution-mode admission now has a shared core report,
`ShardLoomExecutionModeSelectionReport`, used by traditional compatibility and native/prepared
Vortex benchmark runners. The report records requested/selected mode, reason, family, source
format, workload constitution, timing-scope flags, certification/result-sink policy,
provider-availability facts, stable unsupported diagnostics, blocker id, required future evidence,
claim-gate status, and no-fallback/no-external-engine fields. This closes the shared mode-selection
contract while leaving typed-envelope artifact routing, prepared artifact lifecycle, mode-aware
capabilities, and broader provider admission to later P7.5 slices.

P7.5.2 completion update: typed CLI envelopes now emit inline
`execution_mode_selection_report` and `compute_flow_evidence` artifacts whenever execution-mode
fields are present. Legacy flat fields remain compatible, but Python can prefer typed artifacts for
mode selection and compute-flow evidence. Missing compute-flow slots are represented as
`evidence_incomplete`, keeping artifact richness explicit without inventing new execution behavior.

P7.5.3 completion update: prepared Vortex artifacts now have explicit lifecycle evidence in
traditional compatibility and native/prepared Vortex reports: refs, digests where generated,
workspace, reuse eligibility, lifecycle status, source Native I/O status, and cleanup policy.
Python exposes `PreparedVortexArtifacts` and prepare/reuse helpers over the existing no-fallback CLI
commands. Cleanup remains caller-owned and explicit; no hidden destructive cleanup or external
engine fallback is added.

P7.5.4 completion update: `compute-capability-matrix` is mode-aware. Rows now expose
`execution_mode`, `claim_gate_status`, and `vortex_native_claim_allowed`, and the matrix includes
an explicit unsupported `direct_compatibility_transient` row with stable diagnostic
`SL_UNSUPPORTED_DIRECT_COMPATIBILITY_TRANSIENT`, blocker `p75.direct_transient.executor_missing`,
required future ShardLoom-native evidence, and no fallback/no external-engine evidence.

P7.5.5 completion update: traditional analytics compatibility/native/prepared rows now expose
provider-admission evidence. The local Vortex scan/source boundary is reported as the admitted
provider surface, while residual operators remain explicitly ShardLoom-native/materialized or
blocked. Filter/project/limit fusion is not claimed; it carries a stable fusion blocker until true
fusion evidence exists.

P7.5.6 completion update: prepared/native Vortex rows can now opt into caller-owned result-sink
proof through `traditional-analytics-vortex-run --workspace <dir> --write-result-vortex`. The path
reuses the certified Vortex result writer/replay verifier, emits result Native I/O certificate refs,
keeps sink write timing separate from operator timing, records commit/cleanup status, and leaves
claim promotion blocked when result-sink evidence is missing. No external writer, query engine, or
fallback path is introduced.

P7.5.7 completion update: the local comparative benchmark harness now separates ShardLoom build
time, CLI process wall time, Python harness overhead, prepared-artifact setup, query/operator
timing, and result-sink write timing where feasible. It keeps the per-scenario CLI runner and
documents the future persistent-runner requirements in
`docs/architecture/benchmark-persistent-runner-decision.md`; build and preparation are not relabeled
as pure compute.

P7.5.8 completion update: Python request methods and result views now use the shared
execution-mode selection report for explicit `auto`/prepared/native/compatibility modes, claim
gates, blockers, and result-sink evidence. `docs/architecture/execution-mode-protocol-parity.md`
defines the future REST/OpenAPI parity contract so REST uses the same enum, selected-mode reason,
claim gates, blockers, and no-fallback fields instead of inventing a separate control vocabulary.

P7.5.9 completion update: the traditional analytics benchmark artifact now includes
`format_preparation_matrix` rows for ShardLoom compatibility and prepared/native Vortex paths. The
matrix separates source read, compatibility parse, compatibility-to-Vortex import, Vortex
write/reopen/scan, operator compute, optional result sink, and total runtime by source format while
recording `native_execution_format=vortex`.

| Priority 1.7 - top-level plan/execution facade catch-up | RFC 0038 Top-Level Plan and Execution Facade | RFC 0026 Encoded Native Reads; RFC 0029 Correctness/Benchmarks/Execution Certificates; RFC 0031 Native I/O Envelope | typed plan variants for current local/prepared/source-backed/reader-backed Vortex primitive surfaces; `ShardLoomExecutionResult`; provider kind/API surface/version; result/artifact/certificate refs; inline artifacts; evidence slots; lifecycle status; residual boundaries; no-op success prohibition | Replaces unreleased placeholder facade shapes and now preserves rich provider evidence through the P7.4.3 top-level result/envelope path. No SQL/DataFrame runtime, object-store runtime, writes, external engine invocation, or fallback execution is authorized by this row. |
| Priority 2.5 - Vortex upstream alignment and compatibility hardening | RFC 0031 Universal Native I/O Envelope; RFC 0026 Encoded Native Reads | RFC 0012 Diagnostics; RFC 0015 Correctness; RFC 0018 Observability; RFC 0021 Expression/Kernel Registry; RFC 0032 Capability Surface | Vortex compatibility matrix; Scan API compatibility report; composite pushdown matrix; execute-step evidence; device residency; extension types; streaming sink certificates; IO backend evidence; telemetry facet; compression advisor; integrity/encryption; PyVortex interop; Vortex benchmark interop | Docs/report-only unless later promoted; no new Vortex dependency surface, runtime behavior, GPU/vector/geospatial/object-store/write/streaming/benchmark claim, external query-engine integration execution, or fallback is authorized by this row. |
| Priority 2.6 - Vortex compute-provider alignment | RFC 0031 Universal Native I/O Envelope; RFC 0029 Correctness/Benchmarks/Execution Certificates | RFC 0002 No Fallback/Vortex I/O; RFC 0012 Diagnostics; RFC 0018 Observability; RFC 0021 Expression/Kernel Registry; RFC 0025 Competitive Track | Vortex-native execution provider terminology; ExecutionProviderKind; VortexComputeProviderReport; ResidualBoundaryReport; residual_executor; VortexIntegrationBoundaryReport; current Vortex API inventory snapshot | Clarifies that ShardLoom is standalone from external query-engine fallback, not isolated from upstream Vortex compute. Upstream Vortex APIs may be native providers only when policy-admitted and certificate-backed; Vortex query-engine integrations remain baseline/reference/oracle-only. |
| Priority 2.6.5 - Vortex runtime utilization audit and execution-spine hardening | RFC 0042 Vortex Runtime Utilization and Execution Spine | RFC 0031 Universal Native I/O Envelope; RFC 0040 Benchmark Suite and Platform-Learning Hardening; RFC 0041 Feature/Build Matrix and Crate Posture | VortexCapabilityUtilizationReport; VortexRuntimeUtilizationAuditReport; VortexScanExecutionSpineReport; VortexFieldMaskEvidence; VortexPredicateOrderingEvidence; VortexLayoutAdvisorReport; VortexArrayExecutionCertificate; ShardLoomSessionModelReport | Report-only Vortex-first utilization lane. It records array/execution-layer/Scan/layout/I/O/session/device/extension/benchmark evidence requirements without authorizing new upstream Vortex API calls, runtime expansion, external engine invocation, or fallback execution. |
| Priority 2.7 - source-backed benchmark matrix and benchmark-suite overhaul | RFC 0040 Benchmark Suite and Platform-Learning Hardening | RFC 0009 Benchmark Methodology; RFC 0015 Correctness; RFC 0025 Competitive/no-fallback; RFC 0029 Evidence; RFC 0031 Native I/O | source-backed correctness/benchmark matrix; `BenchmarkSuiteCatalogReport`; `BenchmarkConstitutionRequirementReport`; `benchmarks/common/scenario_catalog.json`; executable local taxonomy runner; scenario taxonomy; dataset-profile matrix; `BenchmarkEnginePluginContract`; coverage-table rows; benchmark-constitution result metadata; platform-inspired neutral capability reports | Report/code surfaces define the local-first suite catalog and source-backed benchmark matrix, and the runnable `traditional_analytics` harness now emits taxonomy metadata, benchmark constitutions, generated dataset profile metadata, and support/coverage rows separate from timings. P7.4.4 adds explicit fixture-smoke measurement rows for eligible source-backed matrix lanes and expands generated local fixtures across wide, very-wide, null-heavy, many/few file-shape, date-partitioned, poorly/well-clustered, schema-drift, dirty CSV, nested JSON, CDC overlay, skewed, and high-cardinality shapes. It also adds opt-in local scenario rows for partition pruning, many-small-files scan, null-heavy aggregate, high-cardinality string group/distinct, top-N per group, clean/cast/filter/write, malformed timestamp cleanup, small-change-over-large-base, and nested JSON field scan. Benchmarks remain local/platform-neutral by default; Photon/Fabric/Snowflake and other managed systems are design references only, not benchmark dependencies or fallback execution paths. Full comparative benchmark reruns, remaining ShardLoom-native expanded-scenario support, source-backed claim-grade promotion, and performance claims remain blocked until later release-readiness work. |
| Priority 2.8 - crate-level posture cleanup | RFC 0041 Feature/Build Matrix and Crate Posture | RFC 0024 Release Engineering; RFC 0026 Encoded Native Reads; RFC 0031 Native I/O; RFC 0038 Facade | crate docs/export posture for executable, report-only, blocked, future, and prohibited-fallback surfaces; `docs/architecture/crate-posture-public-exports.md` | Crate docs and public export posture are now documented for core, plan, exec, vortex, and CLI. Docs/posture cleanup only; no runtime expansion, dependency expansion, external engine invocation, or fallback execution. |
| Priority 3.5 / CG-14 - memory runtime hardening gate | RFC 0014 Memory Management, Spill, and OOM Safety | RFC 0016 Optimizer/Adaptive Execution; RFC 0017 Fault Tolerance/Recovery; RFC 0018 Observability; RFC 0024 Release Engineering; RFC 0025 Competitive/no-fallback; RFC 0029 Certificates | `MemoryRuntimeHardeningGateReport`; existing memory admission, operator spill declarations, spill reservation, spill lifecycle, and dynamic runtime-promotion report refs; blocked runtime chunk sizing, adaptive parallelism, reservation release, pressure reaction, native spill read/write, spill cleanup, allocator integration, large-workload claim closeout | Report-only gate; no allocator runtime, resource-derived chunk sizing runtime, adaptive parallelism runtime, reservation release runtime, pressure reaction runtime, native spill IO, cleanup execution, object-store IO, data reads, writes, claim publication, external engine invocation, or fallback execution is authorized by this row. |
| Priority 3.5 / CG-17 - stateful reuse promotion gate | RFC 0029 Correctness, Benchmark, Execution Certificates, and Stateful Reuse | RFC 0004 Native Dataset Manifest/Snapshot/Incremental; RFC 0015 Correctness/Semantics; RFC 0016 Optimizer/Adaptive Execution; RFC 0025 Competitive/no-fallback; RFC 0031 Native I/O Envelope | `StatefulReusePromotionGateReport`; existing `stateful-reuse-plan` and `incremental-plan cdc` refs; blocked stable reuse keys, key digest/scope, manifest-diff inputs, invalidation decision matrix, cache safety, state certificate schema, execution-certificate linkage, Native I/O linkage, reuse benchmark constitution, incremental recompute execution, and production reuse claim closeout | Report-only gate; no cache read/write/replay, manifest-diff reads, incremental recompute execution, state-certificate claim, reuse/incremental performance claim, data reads, writes, external engine invocation, or fallback execution is authorized by this row. |
| Priority 3.5 - workspace feature/build validation matrix | RFC 0041 Feature/Build Matrix and Crate Posture | RFC 0024 Release Engineering; RFC 0030 API/Deployment/Baselines; RFC 0036 Foundry Integration Pack | `WorkspaceFeatureBuildMatrixReport`; default/all/no-default feature checks; Vortex feature combinations; packaging/deployment; benchmark extras; future Foundry optional package surfaces; feature-disabled unsupported diagnostics; `docs/architecture/workspace-feature-build-matrix.md` | Matrix rows and release blockers are now code/doc surfaces. Release-readiness validation only; no release publication, runtime expansion, dependency expansion, external engine invocation, or fallback execution. |
| Priority 3.5 / CG-18 - universal import/deployment/baseline harness maturity | RFC 0030 Universal API, Plan Portability, Import/Deployment, and External Baselines | RFC 0024 Release Engineering; RFC 0029 Benchmark Evidence; RFC 0036 Foundry Integration Pack; RFC 0041 Feature/Build Matrix and Crate Posture | `UniversalHarnessReport`; local/CI/container/optional Foundry/optional benchmark harness environment rows; Spark/DataFusion/Polars/DuckDB/Dask/pandas optional baseline environments; `docs/architecture/universal-import-deployment-baseline-harness.md`; `universal-harness-plan` JSON fields for harness environment and baseline order | Closes the CG-18 planning/maturity surface. No harness execution, package publication, container publication, Foundry invocation, benchmark execution, comparison dataset materialization, external engine invocation, runtime dependency expansion, or fallback execution is authorized by this row. |
| Priority 3.6 - RFC coverage follow-through before broader user/runtime expansion | RFC 0010 Developer Experience; RFC 0011 Modular Extensibility; RFC 0020 Schema/Catalog/Table Compatibility; RFC 0022 Native Plan IR/Interop; RFC 0023 Extension/Plugin ABI | RFC 0012 Diagnostics; RFC 0019 Security/Governance; RFC 0024 Release Engineering; RFC 0030 Universal API/Plan Portability; RFC 0037 Client/Wrapper Architecture | `RfcCoverageFollowThroughReport`; `rfc-coverage-followthrough-plan`; `docs/architecture/rfc-coverage-followthrough.md`; explicit rows for deterministic agent-facing surfaces, effect/materialization metadata, table/catalog evidence separation, imported-plan gates, and extension manifest/sandbox evidence | Closes Priority 3.6 as a report-only coverage gate. Runtime expansion, parser expansion, adapter expansion, dependency expansion, imported-plan execution, extension execution, external effects, external engine invocation, and fallback execution remain blocked until later evidence-bearing lanes. |
| Priority 3.7 - evidence, policy, workload, and protocol hardening | RFC 0032 Capability Surface; RFC 0033 User Workflow; RFC 0034 Engine Fabric; RFC 0035 REST/Event/API | RFC 0010 Developer/Agent Experience; RFC 0012 Diagnostics; RFC 0018 Observability; RFC 0019 Security/Governance; RFC 0024 Release Engineering; RFC 0029 Certificates | EvidenceArtifactEnvelope; EvidenceArtifactSafety; ShardLoomExecutionPolicy; QueryLifecycleContract; ProtocolSurfaceParityReport; starter WorkloadConstitution catalog; ShardLoomNative semantic floor; StandardsDependencyDecision; BenchmarkConstitution; RustPerformanceProfileEvidence | Cross-surface docs/report-only hardening; no runtime, parser, adapter, server, package publication, benchmark execution, external engine invocation, dependency, or fallback execution is authorized by this row. |
| Priority 3.8 - client and wrapper surface architecture | RFC 0037 Client/Wrapper/SDK/Ecosystem Surface | RFC 0030 Universal API/Deployment/Baselines; RFC 0032 Capability Surface; RFC 0035 REST/Event/API; RFC 0036 Foundry Integration Pack; Priority 3.7 evidence/policy hardening | canonical protocol schemas; transports; client core; wrapper maturity W0-W7; language SDK registry; ecosystem wrapper registry; MCP posture; Flight/ADBC/JDBC/ODBC posture; WrapperCapabilityReport; ProtocolSurfaceParityReport; golden contract fixtures | Defines one protocol and many thin wrappers. No generated client, DB-API, SQLAlchemy, Ibis, dbt, Airflow, Dagster, Prefect, MCP, Flight, ADBC, BI connector, runtime behavior, dependency, external engine invocation, or fallback execution is authorized by this row alone. |
| Priority 3.9 - typed command/result envelope and CLI modularity | RFC 0039 Typed Command/Result Envelope and CLI Modularity | RFC 0030 Universal API/Deployment/Baselines; RFC 0035 REST/Event/API; RFC 0037 Client/Wrapper Surface; Priority 3.7 evidence/policy hardening | `shardloom.output.v2`; typed result/result_refs/artifacts/artifact_refs/certificates/policy/lifecycle/capability_snapshot payload slots; Python typed-payload parsing; `shardloom-cli/src/typed_envelope.rs`; `shardloom-cli/src/command_family.rs`; `shardloom-cli/src/cli_output.rs`; `shardloom-cli/src/status_capabilities.rs`; `shardloom-cli/src/input_planning.rs`; `shardloom-cli/src/rest_api_planning.rs`; `shardloom-cli/src/packaging_deployment.rs`; `shardloom-cli/src/benchmark_planning.rs`; `shardloom-cli/src/benchmark_runtime.rs`; `shardloom-cli/src/operational_hardening.rs`; `shardloom-cli/src/diagnostics.rs`; `shardloom-cli/src/evidence_certificates.rs`; `shardloom-cli/src/workflow_planning.rs`; `shardloom-cli/src/engine_runtime_planning.rs`; `shardloom-cli/src/extension_planning.rs`; `shardloom-cli/src/prepared_source_backed_execution.rs`; `shardloom-cli/src/vortex_primitive_execution.rs`; `shardloom-cli/src/vortex_planning.rs`; shared CLI routing for common policy/lifecycle/capability fields; conservative typed ref routing for explicit result/artifact/certificate refs; typed-envelope contract snapshots; remaining command-family typed migration; modular CLI handlers; shared renderer; remaining golden JSON fixtures | The typed-envelope foundation, common field routing, explicit ref routing, first shared CLI modules, command-family lifecycle taxonomy, centralized CLI renderer/error emitter, status/capabilities handler split, input planning handler split, REST/API planning handler split, packaging/deployment handler split, benchmark planning handler split, benchmark runtime handler split including `vortex-count-benchmark`, operational hardening/security handler split, diagnostics handler split, evidence/certificate planning handler split, workflow/table planning handler split including schema and table-compatibility routing, engine/runtime planning handler split, extension/UDF planning handler split, prepared/source-backed encoded-read probe/spike handler split, Vortex primitive `vortex-count`, `vortex-count-where`, `vortex-project`, `vortex-filter`, `vortex-filter-project`, `vortex-run`, `vortex-local-exec`, `vortex-bounded-local-exec`, and `vortex-query-trace` handler splits, Vortex planning `vortex-metadata-plan`, `vortex-pruning-plan`, `vortex-metadata-probe`, and `vortex-api-inventory` handler splits, and typed-envelope contract snapshots for success/error/unsupported/blocked/evidence-incomplete/source-backed/benchmark/Foundry-adjacent reports are implemented while the legacy `fields` mirror remains temporary. P7.4.3 adds artifact-rich top-level execution result envelopes and Python result views. Remaining command-family result migration, certified runtime execution fixture, missing-binary protocol parity fixture, concrete Foundry boundary report fixture, and additional physical handler splits are still active. No REST server, wrapper ecosystem implementation, runtime expansion, new benchmark behavior, correctness harness execution, runtime certificate emission beyond admitted current provider reports, dataset read outside explicit benchmark command contracts, task execution, catalog probe, materialization, write, credential resolution, secret loading, profile collection, extension dynamic loading, UDF execution, external service invocation, external effect execution, external engine invocation, or fallback execution is authorized by this row. |
| P8.0 - security, vulnerability, exploit, and supply-chain hardening | RFC 0043 Security/Vulnerability/Exploit/Supply-Chain Hardening | RFC 0019 Security/Governance; RFC 0024 Release Engineering; RFC 0035 REST/Event/API; RFC 0036 Foundry Integration Pack; RFC 0041 Feature/Build Matrix | `SecurityThreatModelReport`; `DependencyAuditReport`; `SupplyChainReleaseEvidence`; `RuntimeInputSafetyReport`; `WorkspacePathSafetyReport`; `EvidenceArtifactSafetyReport`; `VulnerabilityResponseReport`; `SECURITY.md`; `docs/security/threat-model.md`; `docs/security/supply-chain-response.md`; `docs/security/runtime-exploit-regression-suite.md`; `docs/release/release-provenance-dry-run.md`; `docs/security/open-source-security-posture.md`; `docs/security/release-security-gate.md`; `docs/release/known-unsupported-paths.md`; dependency/advisory gates; malicious-input/path-safety/redaction tests; SBOM/checksum/provenance workflow hardening; CodeQL; OpenSSF Scorecard; Dependabot | P8.0 is a release security gate inserted before public release readiness. P8.0A and P8.0B add the RFC, threat model, expanded security policy, and supply-chain response docs. P8.0C adds the hard dependency/advisory gate contract: `scripts/check_dependency_audit.py --release-gate`, `shardloom.dependency_audit_report.v1`, runtime no-fallback dependency checks, and benchmark-only external baseline classification. P8.0D adds report-level `RuntimeInputSafetyReport`, `WorkspacePathSafetyReport`, and `EvidenceArtifactSafetyReport` contracts plus Rust regression tests for malformed inputs, invalid UTF-8, oversized/deeply nested blockers, path traversal, outside-workspace outputs, symlink/hardlink policy, credential redaction, deterministic diagnostics, and no-fallback invariants. P8.0E adds `scripts/release_provenance_dry_run.py`, local SBOM JSON generation for Rust/Python/CLI artifacts, `checksums.sha256`, `SupplyChainReleaseEvidence`, workflow policy snapshots, release-dry-run integration, and the SHA-pinning-or-waiver rule for third-party publish actions before real publication. P8.0F adds CodeQL, Scorecard, Dependabot, maintainer-setting docs, and `shardloom.open_source_security_posture_report.v1`. P8.0G adds `scripts/check_release_security_gate.py`, `shardloom.release_security_gate_report.v1`, known unsupported paths, and fail-closed release claim gating over P8.0 evidence. P8.4 hard release-readiness gate is complete; current release-claim follow-up lives in the GAR-0043 hard release-readiness validators and publication rehearsal slices. No package publication, release tags, secrets, runtime dependencies, external engine invocation, or fallback execution is authorized. |
| Priority 8 - general availability and external proof-of-use | RFC 0024 Release Engineering; RFC 0030 Universal API/Deployment/Baselines; RFC 0036 Foundry Integration Pack; RFC 0043 Security/Vulnerability/Exploit/Supply-Chain Hardening | RFC 0010 Developer Experience; RFC 0012 Diagnostics; RFC 0019 Security/Governance; RFC 0029 Certificates; RFC 0032 Capability Surface; RFC 0041 Feature/Build Matrix | public package identities; Conda CLI/Python/metapackage proof; PyPI-friendly Python package; GitHub release artifacts; checksums; SBOM; attestations; clean environment proof; public first-10-minutes proof; hard release-readiness gate; external smoke/benchmark/Foundry examples; public docs; P8.0 security evidence | Late-stage release/distribution lane only. P8.1 and P8.2 now have source-local dry-run proof: local CLI build, Python wheel/sdist build, clean venv wheel install, deterministic CLI resolution through `SHARDLOOM_BIN`, first-10-minutes smoke commands, benchmark smoke, and transcript fields that prove no publication, tag, secret, fallback runtime dependency, or external runtime dependency was added. P8.3 adds self-contained local Python, local Vortex benchmark, and Foundry-lightweight examples with environment files, input fixtures, expected outputs, certificate field snapshots, known limitations, and baseline-boundary docs. P8.0 adds the release security contract. P8.4 adds `scripts/check_release_readiness.py` and `shardloom.hard_release_readiness_gate.v1`, which fail closed until full validation, feature/build matrix, clean Conda proof, benchmark, dependency audit, provenance, security, and known-unsupported-path evidence is attached. Package publication, release tags, OCI pushes, crates.io publication, feedstock submission, Marketplace publication, runtime expansion, dependency expansion, and fallback execution remain human-approved and evidence-gated. |
| Priority 9 - Foundry integration pack and platform availability | RFC 0036 Foundry Integration Pack | RFC 0019 Security/Governance; RFC 0024 Release Engineering; RFC 0030 Universal API/Deployment/Baselines; RFC 0031 Native I/O; RFC 0033 User Workflow; RFC 0035 REST/Event/API | shardloom-foundry; FoundryExecutionContext; dataset source/sink; transactions/branches/builds; virtual tables; external compute boundary; Data Health bridge; lineage; schedules; Data Connection; S3-compatible datasets; media sets; `FoundryMediaSetSource`; `FoundryVirtualMediaSetSource`; `FoundryMediaSetSink`; `FoundryMediaExtractionBoundaryReport`; `FoundryModelCallBoundaryReport`; `FoundryEmbeddingBoundaryReport`; `FoundryAipLogicBoundaryReport`; `FoundryUnstructuredWorkflowCertificate`; Ontology/AIP/Functions; BYOC; Compute Modules; Marketplace starter product; Foundry proof-of-use certification | Foundry is optional integration, not a core engine gate. Virtual tables and external compute are governed handles or baseline/oracle/migration boundaries; ShardLoom-native execution requires staged/native data plus certificates. Priority 9 now has `docs/foundry/integration-pack-readiness.md`, `docs/foundry/proof-of-use-certification.md`, and `scripts/foundry_proof_of_use.py`, which emit local proof for import/CLI resolution, no-dataset smoke, explicit staged dataset path, local Vortex execution smoke, certificate output, benchmark metrics output, and no-fallback/external-compute boundaries. This does not publish `shardloom-foundry`, invoke Foundry, or certify real Foundry platform execution; no Foundry/Snowflake/Databricks/BigQuery/Spark compute pushdown may be reported as ShardLoom execution. |
| Ongoing — Expression and kernel engine | RFC 0021 Expression Engine and Kernel Registry | RFC 0015 Correctness, Semantics, Differential Testing, Fuzzing; RFC 0023 Extension/Plugin ABI and Sandboxing | metadata kernel; encoded kernel; partial-decode kernel; decoded reference kernel only as explicit reference/test path; deterministic kernel selection; effect boundaries; no hidden fallback | Keep kernel selection deterministic and no-fallback while preserving explicit reference-only decoded paths. |
| Ongoing — Release/API/agent stability | RFC 0024 Release Engineering, API Compatibility, Packaging | RFC 0012 Diagnostics; RFC 0018 Observability; RFC 0019 Security | CLI compatibility; JSON output compatibility; diagnostic schema stability; feature footprint; benchmark claim evidence; no fallback release check | Treat compatibility and diagnostics as continuous contracts, verified at every phase boundary. |

## RFC coverage status

Status categories:
- Implemented
- Partially implemented
- Accepted
- Accepted as contract; implementation deferred
- Planned
- Deferred
- Needs amendment

| RFC | RFC implementation status | Relevant phases | Notes |
| --- | --- | --- | --- |
| RFC 0001 | Partially implemented | 0-3, Ongoing | Foundational architecture and no-fallback direction established; GAR-0001A-A adds a report-only SQL/DataFrame planner-readiness matrix through CLI capability fields and Python capability accessors, with deterministic diagnostics, `claim_gate_status=not_claim_grade`, no parser/binder/planner/runtime execution, no external engine, and no fallback. GAR-0001A-B adds `global-architecture-gate`, a release/readiness runtime-claim gate for distributed, object-store, and lakehouse claims with deterministic blockers, required evidence refs, `claim_gate_status=not_claim_grade`, no credentials, no I/O, no runtime execution, no external engine, and no fallback. Executable SQL/DataFrame runtime, distributed runtime, object-store execution, lakehouse output, and engine-replacement claims remain gated by later evidence. |
| RFC 0002 | Partially implemented | 2-6, Ongoing | Core contract framing in place; implementation depth still increases by phase. |
| RFC 0003 | Partially implemented | 3-10C, Ongoing | Planning/runtime skeletons exist. GAR-0003-A adds a report-only `vortex_segment_extraction_admission_ref` and `shardloom.vortex_segment_extraction_admission.v1` through `vortex-api-inventory`, explicitly blocking sparse patch/fill segment extraction with deterministic diagnostics, required correctness/execution/Native I/O/materialization/no-fallback evidence, and `claim_gate_status=not_claim_grade`. GAR-0003-B adds `shardloom.materialization_policy.v1`, Python typed accessors, and benchmark `materialization_policy_ref` coverage so encoded-native, residual-native, materialized-temporary, and unsupported paths expose decode/materialization posture and claim boundaries. Broad production Vortex segment extraction and wider operator coverage remain phased. |
| RFC 0004 | Partially implemented | 12A, 12B, 13A, 13B | Manifest/snapshot/incremental model present conceptually; CG-9.5 CDC incremental planning and CG-9.7 compaction planning evidence exist for declared metadata; advanced write/commit, CDC execution, and maintenance execution behavior remain planned. |
| RFC 0005 | Partially implemented | 12A, Ongoing | Vortex-native output contract is established; full staged write path remains planned. |
| RFC 0006 | Partially implemented | 5-10C, GAR-0006-A, Ongoing | Metadata-only and pruning contracts exist for narrow paths. GAR-0006-A adds compute-capability-matrix coverage rows for predicate, DType, null, nested, and statistics families, including support status, fixture/evidence gaps, deterministic unsupported diagnostics, `claim_gate_status=not_claim_grade`, `fallback_attempted=false`, and `external_engine_invoked=false`. Claim-grade broad predicate/DType/null/nested/statistics runtime coverage remains planned. |
| RFC 0007 | Planned | 10B-14B | Deeper execution/runtime scaling specifics remain mostly future-phase work. |
| RFC 0008 | Partially implemented | 11A, 14A, 14A.3, 14B, GAR-0008-A, GAR-0008-B | CG-10.1 object-store range planning, CG-10.2 request coalescing, CG-10.3 commit protocol planning, CG-10.4 distributed scheduling planning, and CG-10.5 checkpoint/retry/idempotency planning evidence exist for declared byte ranges, commit signals, task shapes, and reliability evidence. GAR-0008-A adds `shardloom.object_store_byte_range_provider_gate.v1` through `object-store-request-plan` and `cg10-object-store-runtime-gate`, explicitly blocking byte-range provider runtime until provider capability policy, credential-effect policy, request-budget policy, retry policy, idempotency-key contract, execution certificate, Native I/O certificate, and benchmark evidence exist. GAR-0008-B adds `shardloom.object_store_runtime_blocker_matrix.v1` for coordinator start, worker start, task execution, checkpoint writes, retry attempts, cleanup execution, and commit-record writes, with `SL_OBJECT_STORE_UNSUPPORTED`, row-specific blockers, no I/O, no fallback, and no external engine invocation. Object-store IO, provider probes, credential resolution, retry execution, checkpoint writes, distributed runtime, and object-store commit execution remain planned. |
| RFC 0009 | Partially implemented | 2-10C, Ongoing | Core policy scaffolding exists; deeper behavior and tooling continue to mature. |
| RFC 0010 | Partially implemented | 10C, 10D, Priority 3.6, Ongoing | Developer/agent usability direction is represented by CLI/Python/agent protocol surfaces and `RfcCoverageFollowThroughReport`; every new CLI, Python, future REST, capability, diagnostic, benchmark, and certificate surface must remain deterministic, machine-readable, human-readable, side-effect-explicit, and safe for import/discovery/dry-run workflows before execution/write permissions. |
| RFC 0011 | Accepted as contract; implementation deferred | Priority 3.6, Ongoing (post-core), CG-21P | Modular extensibility is accepted as the boundary contract for SQL, UDFs, unstructured/media, LLM/API/model effects, embeddings, vector operations, and agent-facing extension discovery. `RfcCoverageFollowThroughReport` now records typed/effect/materialization metadata and sandbox/governance/correctness/certificate prerequisites; runtime implementation remains deferred and must preserve explicit effect/materialization/cost/redaction/certificate/no-fallback boundaries. |
| RFC 0012 | Partially implemented | 10C, 10D, Ongoing | Diagnostics contracts exist; stabilization and propagation are explicit upcoming checkpoints. |
| RFC 0013 | Planned | 13B+, Ongoing | Streaming/zero-copy boundary work remains mostly future-phase effort. |
| RFC 0014 | Partially implemented | 10B, 11A, 11B, 13B, 14A, CG-14 | Memory/spill/OOM policies are partially scaffolded; CG-14.1 adaptive memory boundary evidence, runtime memory reservation admission evidence, operator memory/spill declaration gate evidence, and the CG-14 memory runtime hardening gate exist. Allocator runtime, resource-derived runtime chunk sizing, adaptive parallelism runtime, reservation release runtime, pressure reaction runtime, native spill read/write, spill cleanup execution, and large-workload claim publication remain planned and evidence-gated. |
| RFC 0015 | Partially implemented | Ongoing | Correctness-first posture present; deeper differential/fuzz coverage continues over time. |
| RFC 0016 | Partially implemented | 13B, 14B, CG-14, Ongoing | CG-9.6 layout-health planning, CG-9.7 compaction planning, and CG-14.1 adaptive optimizer/memory decision evidence exist; runtime adaptation, runtime filter application, and advanced optimizer behavior remain later-phase work. |
| RFC 0017 | Planned | 11A, 11B, 12B, 14A, 14B | Recovery/cancellation/commit robustness is a remaining implementation focus. |
| RFC 0018 | Partially implemented | 10D, 14A, 14B, Ongoing | Observability foundations exist; richer tracing/profiling is still phased. |
| RFC 0019 | Partially implemented | 11B, 13A, Ongoing | Security/governance guardrails exist; advanced phase-specific controls remain planned. |
| RFC 0020 | Partially implemented | 12A, 13A, 13B, Priority 3.6 | CG-9.1 schema-evolution, CG-9.2 partition-evolution, CG-9.3 delete/tombstone, CG-9.4 aggregate table compatibility, CG-9.5 CDC incremental planning, CG-9.6 layout-health dependency, CG-9.7 compaction planning dependency evidence, and `RfcCoverageFollowThroughReport` exist for typed transitions and table/catalog promotion gates; broader catalog/table metadata integration, delete/tombstone execution, CDC execution, compaction execution, layout/compaction integration, update/delete/merge claims, and write/commit certification remain planned. |
| RFC 0021 | Partially implemented | Ongoing | Expression/kernel architecture exists in principle; full kernel coverage remains ongoing. |
| RFC 0022 | Partially implemented | Priority 3.6, Ongoing (interop track) | Native plan serialization/import/export, imported-plan capability gates, and `RfcCoverageFollowThroughReport` exist as dependency-free safety surfaces. Imported-plan execution, non-native format parsers/exporters, Substrait-like dependency adoption, external-engine bridging, and fallback execution remain blocked. |
| RFC 0023 | Partially implemented | Priority 3.6, Ongoing (extension track) | Extension manifests, inspection reports, sandbox-policy vocabulary, and `RfcCoverageFollowThroughReport` exist as inspection-only surfaces. Plugin/UDF execution, unsafe Python/external extension behavior, extension code loading, signing enforcement, resource-limit enforcement, dependency expansion, and fallback execution remain blocked. |
| RFC 0024 | Partially implemented | 10D, 12A, 12B, Ongoing | Release/API compatibility policy exists; continues as a cross-phase enforcement concern. |
| RFC 0025 | Planned | CG-1 through CG-23, Ongoing | Competitive Engine Track policy is documented; CG-21 is the user data workflow and ETL surface, CG-22 is the three-engine certified data execution fabric, and CG-23 is the REST, event, and remote API surface; implementation remains gate-specific and evidence-gated. |
| RFC 0026 | Partially implemented | CG-1, CG-2, CG-13 | Encoded-read and query-primitive readiness contracts exist; CG-13.1 encoded path selection evidence exists for count/filter/project candidates; GAR-0026-A adds a scoped prepared/native `filter + projection + limit` residual-native Vortex scan path with filter/projection pushdown and bounded top-N state; GAR-0026-B adds a scoped prepared/native `group by aggregation` residual-native Vortex scan path with projection pushdown and grouped residual state; GAR-0026-C extends that pattern to scoped prepared/native `multi-key group by` with composite-key residual state; GAR-0026-D adds scoped prepared/native `hash join` with projected dimension/fact scans and bounded residual join state; GAR-0026-E adds scoped prepared/native `join + aggregate` with projected dimension/fact scans, fact-side filter pushdown, and bounded residual grouped aggregation; GAR-0026-F adds scoped prepared/native `top-N per group` with projected fact scans and bounded residual per-group ranking state; GAR-0026-G adds scoped prepared/native `row number window` with projected fact scans and bounded residual rank-1 state; GAR-0026-H adds scoped prepared/native `high-cardinality string group/distinct` with projected fact scans and residual string grouping state; GAR-0026-I adds scoped prepared/native `partition pruning` with projected `event_date`/`metric` scans, local date-range filter pushdown, and residual scalar aggregation; GAR-0026-J adds scoped prepared/native `sort and top-k` with projected `id`/`metric` scans and bounded residual global top-k state; real generalized encoded execution and production compressed-execution claims remain gated. |
| RFC 0027 | Partially implemented | CG-7, CG-8, CG-14, CG-15 | CG-14.1 adaptive optimizer/memory decision evidence and CG-15.1 CPU specialization report evidence exist; GAR-0027-A adds side-effect-free host CPU feature probing plus a blocked filter/encoded vector-kernel admission diagnostic; runtime adaptivity, SIMD dispatch, and specialized kernel execution remain planned. |
| RFC 0028 | Partially implemented | CG-3, CG-4, CG-9, CG-10 | Output/commit readiness contracts exist; first native count-result payload path is complete; first local committed-manifest execution path is complete; local committed-manifest recovery diagnostics and first local rollback cleanup path are complete; broader payloads, generalized recovery, table/catalog commits, and object-store commits remain incomplete. |
| RFC 0029 | Partially implemented | CG-5, CG-6, CG-16, CG-17 | CG-16.1 local encoded count certificate, CG-16.2 execution-certificate evidence surface, CG-17.1 stateful reuse boundary report, and the CG-17 promotion gate for stable reuse keys/invalidation/manifest-diff/state-certificate/reuse-benchmark blockers exist; broader correctness, benchmark, certificate, cache read/write/replay, and incremental execution evidence remain future gate work. |
| RFC 0030 | Partially implemented | CG-11, CG-12, CG-18 | CG-11.1 stable CLI/API JSON protocol foundation, CG-11.2 thin Python wrapper foundation, and CG-11.3/CG-11.4 source-tree Python client surfaces exist through `CliApiJsonProtocolReport`, `PythonWrapperFoundationReport`, `api-compat-plan`, `python-wrapper-plan`, and the zero-dependency `python/` client; CG-12.1 plan portability report foundation, CG-12.4 native plan serialization, and CG-12.5 imported-plan capability gate exist through `PlanPortabilityReport`, `NativePlanDocument`, `ImportedPlanCapabilityGateReport`, `plan-ir`, `plan-import native`, and `plan-export native`; CG-18.1 plus the Priority 3.5 CG-18 harness maturity surface exist through `UniversalHarnessReport`, `universal-harness-plan`, and `docs/architecture/universal-import-deployment-baseline-harness.md`; imported-plan execution, non-native format parsers/exporters, harness execution, package/container publication, Foundry invocation, baseline runner execution, and comparison dataset materialization remain staged. |
| RFC 0031 | Partially implemented | CG-19 | CG-19.1 native I/O envelope report exists through `NativeIoEnvelopeReport` and `native-io-envelope-plan`; GAR-0031A adds a source/sink coverage matrix for local Vortex, compatibility import, object-store/range-read, table/catalog, streaming, unstructured/media, and external-adapter families with support status, certificate refs, deterministic unsupported diagnostics, blockers, future evidence, no-fallback fields, and claim boundaries. Source/sink runtime certificate emission, adapter runtime, reads, decode/materialization, writes, object-store I/O, and fallback remain absent outside existing certified local lanes. |
| RFC 0032 | Partially implemented | CG-20 | CG-20.1 world-class sufficiency reporting exists through `WorldClassSufficiencyReport` and `world-class-sufficiency-plan`; CG-20.2 user-surface capability discovery exposes report-only `capabilities` scopes for ETL, Python, DataFrame/notebook, UDF, universal/event/API adapters, unstructured/media, API, observability, deployment, extension, and security/governance dimensions; GAR-0001A-A adds a SQL/DataFrame planner-readiness matrix to `capabilities sql`, `capabilities dataframe`, and Python `CapabilityView`; CG-20.3 adds Python live ETL smoke helpers; CG-20.7 adds a feature-gated local compatibility-file-to-Vortex bridge for CSV, JSONL, Parquet, Arrow IPC, Avro, and ORC; CG-20.11 adds local Conda recipe scaffolds for the CLI/Python/metapackage split; P7.4.2 adds the first ShardLoomNative semantic conformance fixture suite. Real SQL, operators, functions, production adapters, full semantic conformance coverage, migration analyzers, mature Python/API, DataFrame/notebook, UDF, production ETL, universal-adapter certification, unstructured/media, Conda build/install publication, correctness, benchmark, and best-default certification evidence remain staged. |
| RFC 0033 | Partially implemented | CG-21 | Defines the user data workflow and ETL surface: install/import, capability discovery, local structured ETL, DataFrame/query-builder, SQL, pandas/Arrow boundaries, data contracts, transformations, joins, aggregations, windows, incremental ETL, writes, object stores, table/catalog UX, remote inputs, logs/events, unstructured/media references, UDFs, observability, migration, benchmarks, governance, notebooks, deployment, adapter maturity, lane sequencing, MVP scope, unsupported diagnostics, and certification disqualifiers. Priority 4 implements local workflow/report and quickstart proof surfaces. P7/Audit-F9 adds report-only unsupported diagnostics and Python helper parity for `from_pandas`, Arrow table/IPC input and output boundaries, NumPy/Python-object materialization, schema discovery/description/validation, data-quality summary/quarantine, and notebook preview/display. P7.4.2 extends parity to `with_column`, `group_by`, `agg`, `sort`, `limit`, and SQL parse/bind/plan/execute diagnostics. Mature DataFrame/SQL/pandas/Arrow/write/object-store/table runtime and production ETL certification remain blocked or report-only until later evidence-bearing slices. |
| RFC 0034 | Accepted | CG-22 | Defines the three-engine certified data execution fabric: batch, live, and hybrid engine modes under one importable UX; engine selection reports; boundedness, update mode, and output mode vocabulary; freshness, state, delta overlay, hot/cold contribution, and continuous view certificates; engine-specific lowering; per-engine capability matrices; hot/warm/cold storage layers; NoSQL-inspired analytical state; roadmap phases; non-goals; certification blockers; and no-fallback boundaries. No runtime behavior, dependencies, fallback execution, or claims are authorized by the RFC alone. |
| RFC 0035 | Partially implemented | CG-23 | Defines the REST, event, and remote API surface: REST control plane, data plane, event plane, capability/discovery endpoints, plan/explain/dry-run endpoints, async query lifecycle, result delivery policies, problem+json errors, engine-aware request behavior, live/hybrid event APIs, API maturity ladder, OpenAPI/AsyncAPI/CloudEvents/OpenTelemetry/OpenLineage/Flight/ADBC/MCP references, security/governance policy, certification blockers, and no-fallback boundaries. Priority 6 implements contract/report-only lanes through API-A9 columnar data-plane standards, while API-A2 remains no-listener discovery reporting and API-A10 production-certified workload API remains blocked until workload-scoped evidence exists. |
| RFC 0036 | Partially implemented | Priority 8, Priority 9 | Defines the optional Foundry Integration Pack and availability surface: Conda-first/PyPI-friendly/GitHub-release-backed distribution, provenance artifacts, external proof examples, shardloom-foundry helper package, Foundry maturity ladder, datasets, transactions, branches, incremental runs, Data Health, lineage, schedules, Data Connection, S3-compatible dataset access, virtual tables, external compute boundary reports, Iceberg posture, media sets, `FoundryMediaSetSource`, `FoundryVirtualMediaSetSource`, `FoundryMediaSetSink`, `FoundryMediaExtractionBoundaryReport`, `FoundryModelCallBoundaryReport`, `FoundryEmbeddingBoundaryReport`, `FoundryAipLogicBoundaryReport`, `FoundryUnstructuredWorkflowCertificate`, Ontology/AIP/Functions/model/scenario surfaces, BYOC, Compute Modules, Marketplace starter products, governance, and Foundry benchmark schemas. Priority 9 now adds local Foundry integration-pack readiness docs and `shardloom.foundry_proof_of_use_report.v1` for source-checkout proof. Full Foundry package/platform installation, invocation, external compute execution, virtual-table native execution, dependency expansion, fallback execution, and platform claims remain unauthorized until real Foundry evidence exists. |
| RFC 0037 | Accepted | Priority 3.8 | Defines the client/wrapper/SDK/ecosystem integration surface: one canonical protocol, transport adapters, client core, language SDKs, Python ecosystem wrappers, workflow wrappers, MCP posture, Flight/ADBC/JDBC/ODBC/BI posture, wrapper maturity W0-W7, `WrapperCapabilityReport`, `ProtocolSurfaceParityReport`, golden contract fixtures, and wrapper no-fallback invariants. No wrapper implementation, generated client, API server, data-plane bridge, dependency, external engine invocation, or fallback execution is authorized by the RFC alone. |
| RFC 0038 | Partially implemented | Priority 1.7 | Top-level plan variants, `ShardLoomExecutionResult`, provider-neutral blocked dispatch, `ShardLoomExecutionProvider`, and Vortex provider-side dispatch now exist for local primitive and prepared/source-backed/reader-backed encoded report surfaces. P7.4.3 preserves provider reports as artifact-rich execution results with provider version, lifecycle, inline artifact, evidence-slot, certificate, Native I/O, materialization, residual-boundary, representation-transition, source/split, and no-fallback fields. SQL/DataFrame runtime, object-store runtime, writes, external engine invocation, fallback execution, and legacy facade compatibility remain unauthorized. |
| RFC 0039 | Partially implemented | Priority 3.9 | Defines the typed command/result/evidence envelope and CLI modularity overhaul. Current implementation adds `shardloom.output.v2`, typed result/result_refs/artifacts/artifact_refs/certificates/policy/lifecycle/capability_snapshot slots, API/Python protocol reporting, Python typed-payload parsing, shared CLI routing for common policy/lifecycle/capability fields, conservative typed ref routing for explicit result/artifact/certificate refs, the shared `typed_envelope` and `cli_output` CLI modules, `command_family` lifecycle taxonomy, the status/capabilities, REST/API planning, packaging/deployment, benchmark planning, benchmark runtime including `vortex-count-benchmark`, operational hardening/security, diagnostics, evidence/certificate planning, workflow/table planning, engine/runtime planning, extension/UDF planning, prepared/source-backed encoded-read, and first Vortex primitive execution and planning handler modules, including `vortex-count-where`, `vortex-project`, `vortex-filter`, `vortex-filter-project`, `vortex-run`, `vortex-local-exec`, `vortex-bounded-local-exec`, `vortex-query-trace`, `vortex-metadata-plan`, `vortex-pruning-plan`, `vortex-metadata-probe`, and `vortex-api-inventory`, and typed-envelope contract snapshots while retaining `fields` as a temporary legacy mirror. P7.4.3 adds artifact-rich top-level execution result envelopes and Python result views for current provider evidence. Remaining command-family result migration, remaining certified-runtime/missing-binary/Foundry-boundary golden fixtures, and additional physical handler splits remain planned. No REST server, wrapper ecosystem implementation, benchmark execution, runtime expansion, extension dynamic loading, UDF execution, external service invocation, external engine invocation, or fallback execution is authorized by the RFC alone. |
| RFC 0040 | Partially implemented | Priority 2.7 | Defines the benchmark-suite and platform-learning hardening lane: local-first benchmark suite catalog, scenario taxonomy, dataset profiles, benchmark constitutions, plugin-based local engines, coverage tables, and neutral capability reports inspired by Photon/Fabric/Snowflake lessons. Current implementation adds `BenchmarkSuiteCatalogReport`, `BenchmarkConstitutionRequirementReport`, local optional engine plugin contracts, coverage rows, the source-backed matrix report surface, fixture-smoke measured source-backed matrix rows, the machine-readable scenario catalog, taxonomy/constitution metadata in the executable local analytics runner, opt-in taxonomy-extra local scenarios, generated dataset profile metadata including wide/very-wide/null-heavy/date-partitioned/clustered shapes, and support/coverage table output separate from timings. Managed platforms are design references only; full comparative benchmark reruns, source-backed claim-grade promotion, performance claims, managed-platform benchmark lanes, credentials, new managed dependencies, external engine fallback, and fallback execution remain unauthorized. |
| RFC 0041 | Partially implemented | Priority 2.8, Priority 3.5 | Defines the workspace feature/build validation matrix and crate-level posture cleanup: default/all/no-default feature checks, key Vortex/package/benchmark/Foundry feature combinations, feature-disabled unsupported diagnostics, build evidence fields, and crate docs/export posture for executable, report-only, blocked, future, and prohibited-fallback surfaces. Current implementation completes the crate-posture docs/export cleanup and adds `WorkspaceFeatureBuildMatrixReport` plus matrix documentation. Public release/package claims remain blocked until the release environment records passing matrix evidence. No package publication, dependency expansion, runtime expansion, object-store execution, external engine invocation, or fallback execution is authorized by the RFC alone. |
| RFC 0042 | Partially implemented | Priority 2.6.5, Priority 2.7 | Defines Vortex runtime utilization and execution-spine hardening: utilization reporting across arrays, execution layers, Scan Source/Sink/Split, field masks, predicate ordering, layouts, I/O, sessions/registries, device residency, extension types, benchmark discipline, and Vortex integrations as baselines only. Current implementation adds report/code surfaces, tests, the GAR-0042A `vortex-api-inventory` source/split admission proof for the scoped local Vortex scan fixture path, and the GAR-0042B layout/write/device/object-store/managed-platform boundary matrix carried through benchmark and claim-gate metadata. Generalized Source/Split runtime, field-mask/predicate-ordering evidence, layout/write runtime evidence, object-store runtime I/O, GPU/device execution, managed-platform benchmark execution, external engine invocation, and fallback execution remain unauthorized. |
| RFC 0043 | Partially implemented | P8.0, P8.4, Priority 9 | Defines security, vulnerability, exploit, and supply-chain hardening for release and platform readiness: SEC-0 through SEC-9, malicious input and workspace safety, evidence artifact safety, dependency/advisory gates, SBOM/checksum/provenance/attestation requirements, vulnerability disclosure, compromised package response, Scorecard/CodeQL/Dependabot posture, and no-fallback security blockers. Current implementation completes P8.0 by adding the RFC, threat model, expanded security policy, supply-chain response doc, dependency audit release-gate report, report-level runtime input/workspace/evidence artifact safety contracts, local SBOM/checksum/provenance dry-run generation, PyPI workflow policy snapshots, CodeQL, Scorecard, Dependabot, open-source security posture docs, known unsupported paths, release security gate aggregation, and contract tests. P8.4 remains planned for the full hard release-readiness gate across all release evidence. |

## Drift policy

- If a phase needs behavior not covered by an RFC, add a small RFC amendment or ADR.
- If implementation contradicts an RFC, stop and document the decision before merging.
- If an RFC is too broad but still directionally right, reference the relevant acceptance criteria
  only.

## No-fallback invariant

- All phases must preserve no Spark, DataFusion, DuckDB, Polars, Velox, or fallback engine
  execution.
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

- CG-1 through CG-23 are **Competitive Engine Track** success gates and roadmap tracks.
- CG gates are not aliases for canonical implementation phase IDs (for example, they are distinct
  from Phase 12/13/14 implementation phases).
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
  - CG-1.2d.2 deterministic async/session boundary contract: recorded-active (report-only; no
    runtime/executor added; metadata/footer invocation deferred to CG-1.2d.3)
    - primary RFC: RFC 0026
    - secondary RFCs: RFC 0012, RFC 0016, RFC 0025, RFC 0027, RFC 0029
    - constraints: no scan/read-start, decode, materialization, Arrow conversion, object-store IO,
      or fallback
- CG-2: real query primitive execution over Vortex data
- CG-3: output payload write path (placeholder artifact phases support readiness only; first real
  local count-result Vortex payload path complete; broader payloads deferred)
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
    - constraints: no Python package, PyO3/maturin, DataFrame API, parser/runtime execution,
      filesystem/network/catalog/adapter probing, writes, package publication, or fallback
  - CG-11.2 thin Python wrapper foundation: complete
    - primary RFC: RFC 0030
    - secondary RFCs: RFC 0010, RFC 0012, RFC 0032
    - constraints: source-tree subprocess CLI JSON client only; no native bindings,
      DataFrame/notebook/Python UDF runtime, parser/runtime execution, probes, writes, package
      publication, or fallback
  - CG-11.3 source-tree Python CLI JSON client package: complete
    - primary RFC: RFC 0030
    - secondary RFCs: RFC 0010, RFC 0012, RFC 0024, RFC 0032
    - constraints: zero-dependency source-tree Python package only; no package publication, native
      extension, PyO3/maturin, DataFrame/notebook/Python UDF runtime, parser/runtime execution
      during import, external engine execution, or fallback
  - CG-11.4 Python live ETL client helpers and advisory optimization hooks: active
    - primary RFC: RFC 0030
    - secondary RFCs: RFC 0010, RFC 0012, RFC 0016, RFC 0024, RFC 0029, RFC 0031, RFC 0032
    - constraints: explicit CLI JSON invocations only; no package publication, native extension,
      DataFrame/notebook/Python UDF runtime, SQL parser/execution, production adapter runtime,
      object-store IO, writes, external engine execution, or fallback
- CG-12: plan portability / semantic IR
  - CG-12.4 native plan import/export serialization: complete
    - primary RFC: RFC 0030
    - secondary RFCs: RFC 0010, RFC 0012, RFC 0022, RFC 0024
    - constraints: in-memory ShardLoom-native serialization only; no file IO, external format
      parser, imported-plan execution, external engine execution, dependency, or fallback
  - CG-12.5 imported-plan capability execution gate: complete
    - primary RFC: RFC 0030
    - secondary RFCs: RFC 0010, RFC 0012, RFC 0022, RFC 0024, RFC 0031, RFC 0032
    - constraints: gate/report only; no imported-plan execution, parser, file IO,
      network/catalog/adapter probing, reads, writes, external engine execution, dependency, or
      fallback
- CG-13: encoded-native compressed execution
  - CG-13.1 encoded path selection report foundation: complete
    - primary RFC: RFC 0026
    - secondary RFCs: RFC 0012, RFC 0015, RFC 0021, RFC 0025, RFC 0029, RFC 0031, RFC 0032
    - constraints: report-only path selection; no generalized encoded execution, parser, SQL
      execution, adapter runtime, scan/read-start API, encoded-data read, decode, materialization,
      Arrow conversion, object-store IO, writes, spill IO, external engine execution,
      production/superiority claim, or fallback
- CG-14: runtime-adaptive optimizer and execution memory
  - CG-14.1 adaptive optimizer and memory decision report foundation: complete
    - primary RFCs: RFC 0016 and RFC 0014
    - secondary RFCs: RFC 0012, RFC 0013, RFC 0015, RFC 0021, RFC 0025, RFC 0027, RFC 0029, RFC
      0031, RFC 0032
    - constraints: report-only optimizer/memory decision evidence; no optimizer execution, runtime
      adaptation application, runtime filter build/apply, dynamic pruning execution, plan rewrite,
      join/aggregate/skew execution, memory allocator/reservation runtime, spill execution,
      object-store IO, writes, production/superiority claim, or fallback
- CG-15: CPU operator specialization
- CG-16: evidence-first execution certificates
- CG-17: stateful result reuse / incremental execution
- CG-18: universal import/deployment/baseline harness
- CG-19: universal native I/O envelope
- CG-20: world-class SQL/operator/function/adapter/user capability surface
- CG-21: user data workflow and ETL surface
- CG-22: three-engine certified data execution fabric
- CG-23: REST, event, and remote API surface

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
| Priority 3 / CG-4 — broader commit execution promotion gate (complete) | RFC 0017 Fault Tolerance, Cancellation, and Recovery; RFC 0028 Output Payloads, Finalization, Commit, and Lakehouse Semantics | RFC 0004 Native Dataset Manifest, Snapshot, Incremental; RFC 0031 Universal Native I/O Envelope; RFC 0036 Foundry Integration Pack | `CommitExecutionPromotionGateReport` names local, generalized local sink, object-store, table/catalog, native source/sink, Foundry dataset transaction, and live/hybrid checkpoint commit surfaces; existing local commit/rollback paths remain narrow; broader commit promotion is blocked behind output manifest, sink requirement, materialization/fidelity, idempotency, recovery/rollback, ambiguous-commit, backend atomicity, table/catalog transaction, credential/effect policy, execution-certificate, Native I/O, and no-fallback evidence | Complete report-only gate; no runtime execution, write IO, object-store IO, catalog IO, external effects, claim publication, or fallback execution. |


| Priority 3 / CG-20 - broad user capability promotion gate (complete) | RFC 0032 World-Class SQL, Operators, Functions, Adapters, and User Capability; RFC 0033 User Data Workflow and ETL Surface; RFC 0037 Client, Wrapper, SDK, and Ecosystem Integration Surface | RFC 0030 Universal API, Plan Portability, Import/Deployment Baselines; RFC 0031 Universal Native I/O Envelope; RFC 0035 REST, Event, and Remote API Surface; RFC 0036 Foundry Integration Pack | `UserCapabilityPromotionGateReport` names broad SQL frontend, DataFrame query-builder, notebook, UDF/plugin, unstructured/media, universal adapter, event/API adapter, adapter read/write/commit, semantic-profile conformance, workload-certified closeout, and best-default dossier publication surfaces; existing world-class sufficiency, Python wrapper, input adapter registry, and unstructured workflow boundary contracts remain report-only evidence | Complete report-only gate; no SQL parsing/execution, DataFrame runtime, notebook runtime, UDF/plugin execution, OCR/transcription/embedding/LLM call, adapter runtime, external API call, catalog probe, object-store IO, write IO, claim publication, external engine invocation, or fallback execution. |
| Priority 3 / CG-20 - approximate aggregate/sketch function admission gate (complete) | RFC 0032 World-Class SQL, Operators, Functions, Adapters, and User Capability; RFC 0021 Expression Engine and Kernel Registry; RFC 0029 Correctness, Benchmark, Certificates, and State Reuse | RFC 0015 Correctness, Semantics, Differential Testing, and Fuzzing; RFC 0031 Universal Native I/O Envelope; RFC 0033 User Data Workflow and ETL Surface | `ApproxSketchFunctionGateReport` names canonical `approx_count_distinct`, incumbent aliases, grouped approximate distinct, partial sketch construction/merge, serialization/deserialization, stable hash/seed metadata, error/confidence model, value semantics, encoded dictionary/run-length/selection-vector strategies, partial-decode materialization boundaries, exact-reference comparison, and benchmark/certificate/Native I/O closeout surfaces | Complete report-only gate; no function registry mutation, sketch-state runtime, grouped aggregate runtime, sketch serialization runtime, encoded sketch execution, partial-decode execution, materialization without report, generic sketch dependency, claim publication, external engine invocation, or fallback execution. |

## Competitive Engine Track RFC mappings

CG items are competitive success gates, not implementation phase aliases.
External engines are baselines only.
No fallback execution.

| RFC | Competitive gates covered |
| --- | --- |
| RFC 0025 | CG-1 through CG-23 |
| RFC 0026 | CG-1, CG-2, CG-13 |
| RFC 0027 | CG-7, CG-8, CG-14, CG-15 |
| RFC 0028 | CG-3, CG-4, CG-9, CG-10 |
| RFC 0029 | CG-5, CG-6, CG-16, CG-17 |
| RFC 0030 | CG-11, CG-12, CG-18 |
| RFC 0031 | CG-19 |
| RFC 0032 | CG-20 |
| RFC 0033 | CG-21 |
| RFC 0034 | CG-22 |
| RFC 0035 | CG-23 |

- Phase 12C placeholder output payload artifact work supports CG-3 readiness only; it does not
  complete CG-3 by itself.
- CG-3.1 adds the first real feature-gated local native `Vortex` payload write path for a known
  `CountAll` result; broader payload shapes, commits, and object-store writes remain separate work.
- RFC 0026 supports CG-1.1 encoded read boundary sequencing.
- Competitive claims still require CG-5 correctness and CG-6 benchmarks before any “beats
  Spark/Polars/DataFusion” statement.




## R3 cleanup traceability

| Cleanup phase | Scope | Primary RFCs | Notes |
| --- | --- | --- | --- |
| R3.3a | CLI missing/unknown argument diagnostic helpers | RFC 0012, RFC 0024, RFC 0030 | Helper/test cleanup only; no broad diagnostics migration; no runtime behavior change; no fallback execution. |
| R3.3b | Unknown signal diagnostic normalization | RFC 0012, RFC 0024, RFC 0030 | Narrow helper/parser cleanup only; no broad diagnostics migration; no runtime behavior change; no fallback execution. |
| R3.3c | Output envelope command-status derivation audit | RFC 0012, RFC 0024, RFC 0030 | Output-envelope audit/tests only; no broad diagnostics migration; no runtime behavior change; no fallback execution. |
| GAR-0012-A | Diagnostic category and helper normalization | RFC 0012, RFC 0010, RFC 0033 | Workflow unsupported command family now routes invalid-input, unsupported-feature, materialization, object-store, and no-fallback diagnostics through helper-backed categories; Python envelope tests preserve those categories; no output-envelope redesign, runtime behavior, object-store IO, materialization, external engines, or fallback execution. |
| R3.4 | Terminology consolidation backlog | RFC 0012, RFC 0013, RFC 0014, RFC 0016, RFC 0022, RFC 0024 | docs/audit only; mapping-helper backlog only; no public type renames; no runtime behavior; no fallback execution. |
| R3.5 | Feature-footprint/doctor centralization plan | RFC 0012, RFC 0018, RFC 0024, RFC 0025, RFC 0030 | docs/audit only; feature-footprint report implementation deferred; doctor/capabilities behavior unchanged; no runtime behavior; no fallback execution. |
| R3.5a | `FeatureFootprintReport` core contract | RFC 0012, RFC 0018, RFC 0024, RFC 0025, RFC 0030 | core report contract only; no probing; no `doctor`/`capabilities` behavior change; no dependency scanning; no runtime behavior; no fallback execution. |
| R3.5d | no-fallback dependency invariant tests | RFC 0024, RFC 0025, RFC 0030 | manifest/lockfile invariant tests only; no docs scan for conceptual references; no runtime behavior; no fallback execution. |


### CG-1.2d.3 update
- Added feature-gated async metadata/footer invocation surface for caller-provided async context
  only.
- No runtime/executor dependency was added by `ShardLoom`.
- Sync `VortexEncodedReadMetadataProbeReport::from_request` path remains report-only/no-IO.
- Async surface preserves no scan/read-start, no encoded-data reads, no decode/materialization, no
  `Arrow` conversion, no object-store IO, no writes, and no fallback execution.
- At this phase, actual public upstream `Vortex` metadata/footer invocation remained blocked by
  compile-unclear API shape; CG-1.2d.9 supersedes that blocker for the approved local fixture path.


- CG-1.2d.5 (complete): method-shape compile probes confirm public method items for
  `OpenOptionsSessionExt`, `VortexOpenOptions`, and `VortexFile::footer` without invocation;
  metadata/footer invocation remained deferred and deterministically blocked in that phase without
  runtime/executor wiring.

- CG-1.2d.6 (complete): caller-provided `VortexSession` invocation contract is added under
  `vortex-file-io` and open-method compile probing now includes `VortexOpenOptions::open_path`
  method-item reference; production invocation remained deterministically blocked in that phase;
  CG-1.2d.8 confirmed test harness ingredient limits before CG-1.2d.9 added the checked-in local
  fixture path.
- CG-1 through CG-20 remain active Competitive Engine Track gates; this update is CG-1.2d scope only
  and does not change other gate statuses.

### CG-1.2d.9 local metadata/footer invocation path
- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, and RFC 0025
  Competitive/no-fallback.
- `invoke_vortex_metadata_footer_probe_with_session_async` now performs a feature-gated local
  `VortexOpenOptions::open_path` / `VortexFile::footer` metadata/footer invocation when the caller
  provides a `VortexSession` and the boundary report is `BoundaryReady`.
- A checked-in local `.vortex` fixture with provenance supplies deterministic metadata/footer open
  evidence for the harness.
- The invocation records only `metadata_opened` and `footer_inspected` effects; it does not call
  scan/read-start APIs, read rows, decode/materialize, convert to `Arrow`, perform object-store IO,
  write data, or attempt fallback execution.
- Default builds, sync report paths, and non-session helpers remain report-only/deferred.
- CG-1.2d metadata/footer execution is no longer blocked for the local feature-gated fixture path;
  CG-1 closeout still requires an encoded data path beyond metadata/footer inspection.
- CG-1 through CG-20 remain active Competitive Engine Track gates.

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


## CG-2.0 query primitive boundary update
- CG-1.2 metadata/footer execution was paused after CG-1.2d.8; CG-1.2d.9 clears the local fixture
  metadata/footer invocation blocker but does not add query primitive execution.
- Historical evidence: CG-2.0 added a report-only, feature-gated `Vortex` query primitive readiness
  boundary for count, filtered count, projection, and predicate/filter primitives.
- This boundary does not execute query primitives and remains side-effect-free.
- CG-2.1c clears metadata-footer `CountAll` execution; encoded-data-path readiness is still required
  for non-metadata candidates.
- No scan/read-start, encoded data reads, row reads, decode/materialization, `Arrow` conversion,
  object-store `IO`, writes, or fallback execution are introduced.
- CG-1 through CG-20 remain visible and active competitive gates.

## CG-2.0b helper-correctness traceability update
- CG-2.0b closes helper correctness gaps for invocation-derived query primitive requests by
  preserving boundary blockers/signals and preventing misclassification as `feature_disabled` when a
  stronger blocker exists.
- `VortexQueryPrimitiveReport::has_errors` now treats report diagnostics with `Error`/`Fatal`
  severity as errors in addition to status/request diagnostics.
- This remains report-only readiness planning and introduces no execution side effects.
- CLI query primitive planning command is deferred to CG-2.0c.
- CG-2.1 execution remains blocked pending query wiring and encoded data path readiness.

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


| CG-1.3 - encoded-read no-materialization / no-`Arrow` invariant evidence (complete for recorded contract surfaces) | RFC 0025 Competitive/no-fallback; RFC 0026 `Vortex` encoded-read/query-readiness boundaries | RFC 0015 Correctness/testing | Keep report-contract only outside the feature-gated CG-1.2d.9 local metadata/footer invocation; no scan/read-start; no decode/materialization/`Arrow` conversion; no object-store IO/writes; no fallback execution | Records invariant evidence for no broad row materialization and no `Arrow`-default conversion across recorded report surfaces; CG-1.2d.9 clears local metadata/footer invocation; CG-2.1 execution remains blocked pending query wiring and encoded data path readiness. |


## CG-2.1 count readiness planning update

- CG-1.3 invariant contract tests are complete.
- CG-2.0 / CG-2.0b / CG-2.0c / CG-2.0c.1 are complete.
- Historical evidence: CG-2.1 added a report-only
  `VortexCountReadinessRequest`/`VortexCountReadinessReport` planning contract.
- Count planning distinguishes metadata-footer candidates from encoded-data-path candidates.
- Metadata-footer `CountAll` execution is now wired through CG-2.1c; encoded-data count candidates
  can be approved and deferred through CG-2.1d.
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

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexMetadataAsyncInvocationReport` now carries a typed `VortexMetadataSummaryReport` when the
  feature-gated local footer invocation succeeds.
- `Count` query primitive readiness treats metadata-footer readiness as sufficient for metadata-only
  `CountAll`; encoded-data-path readiness remains required for non-metadata primitives.
- `execute_vortex_count_all_from_metadata_footer_invocation` consumes the typed summary and returns
  a metadata-only local execution result.
- The checked-in `metadata_footer_u64_20000.vortex` fixture proves `Count(20000)` from actual Vortex
  footer metadata.
- This does not call scan/read-start APIs, traverse encoded data, read rows, decode/materialize
  values, convert to `Arrow`, perform object-store IO, write data, or attempt fallback execution.
- CG-2 closeout still requires non-metadata count, filtered-count, projection, and encoded-data
  execution paths.

## CG-2.1d encoded-data CountAll candidate bridge

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `count_readiness_request_from_encoded_read_readiness_report` can promote a side-effect-free
  `VortexEncodedReadReadinessReport` with future encoded-read candidates into a
  `VortexCountCandidateSource::EncodedDataPath` request.
- `execute_vortex_count_all_from_encoded_data_candidate` accepts only count-ready encoded-data
  candidates and returns a deferred `NeedsEncodedRead` local execution report.
- This bridge does not execute the encoded read, does not call scan/read-start APIs, does not
  traverse encoded data, does not read rows, does not decode/materialize values, does not convert to
  `Arrow`, does not perform object-store IO or writes, and does not attempt fallback execution.
- CG-2 closeout still requires actual native encoded count execution plus filtered-count and
  projection execution over real Vortex data.

## CG-2.1e.1 encoded-data CountAll API-gated blocker

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `count_readiness_request_from_encoded_read_probe_report` consumes `VortexEncodedReadProbeReport`
  so encoded-data count readiness is gated by the public encoded-read API boundary, not only
  scheduler/readiness candidates.
- Recorded upstream public surfaces for data access remain blocked for actual count execution
  because they route through scan/data-read or array-stream/evaluation APIs that are not yet
  approved under ShardLoom's no-decode/no-materialization boundary.
- API boundary blockers propagate into count readiness as deterministic object-store,
  scan-execution, decode, materialization, Arrow-default, or write blockers.
- This pass does not execute encoded reads, call scan/read-start APIs, traverse encoded data, read
  rows, decode/materialize values, convert to `Arrow`, perform object-store IO or writes, or attempt
  fallback execution.
- CG-2.1e actual encoded-data count execution remains planned and blocked until a safe public Vortex
  data path is approved.

## CG-2.1e.2 exact Vortex data-access API classification

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- The encoded-read API boundary now names the exact upstream public surfaces reviewed for the next
  execution decision: `VortexFile::layout_reader`, `LayoutReader::row_count`, `VortexFile::scan`,
  `ScanBuilder::into_array_stream`, `ScanBuilder::into_array_iter`,
  `LayoutReader::projection_evaluation`, `LayoutReader::filter_evaluation`, and
  `VortexFile::data_source`.
- A feature-gated compile probe references the public Vortex method items without invoking them,
  preserving version compatibility evidence while keeping the default runtime side-effect-free.
- `LayoutReader::row_count` is classified as metadata-like layout access and remains not
  execution-usable by itself.
- Scan, array-stream, layout-evaluation, and data-source surfaces remain blocked or deferred until
  ShardLoom can prove no row reads, decode/materialization, `Arrow` conversion, object-store IO,
  writes, or fallback execution.
- CG-2.1e actual encoded-data count execution remains planned and blocked until one of these public
  surfaces, or an upstream-supported alternative, is approved as no-decode/no-materialization safe
  for ShardLoom-native count execution.

## CG-2.1e.3 named count API-boundary blockers

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `count_readiness_request_from_encoded_read_probe_report` now copies named blocked API-boundary
  summaries from `VortexEncodedReadProbeReport` into count readiness.
- The count-readiness boundary can now expose exact blockers such as `VortexFile::scan`,
  `ScanBuilder::into_array_stream`, `ScanBuilder::into_array_iter`,
  `LayoutReader::projection_evaluation`, `LayoutReader::filter_evaluation`, and
  `VortexFile::data_source`.
- Metadata-like `LayoutReader::row_count` is intentionally not carried as an execution blocker.
- This is still report metadata only: no scan/read-start invocation, array stream/evaluation call,
  encoded-data traversal, row read, decode/materialization, `Arrow` conversion, object-store IO,
  write, or fallback execution is introduced.

## CG-2.1e.4 encoded-count admission blocker guard

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- Named API-boundary blockers now participate in readiness derivation: a request with any blocker
  cannot produce `CountReady`, even when `EncodedDataPathReady` is present.
- `execute_vortex_count_all_from_encoded_data_candidate` also rejects readiness reports that still
  carry named API-boundary blockers.
- This closes the admission gap before actual encoded-count execution: exact blocked Vortex surfaces
  must be removed or approved before any execution helper can advance.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data,
  read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt
  fallback execution.

## CG-2.1e.5 `VortexFile::row_count` metadata-surface approval

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexFile::row_count` is now compile-checked and classified as a confirmed public metadata-only
  surface because upstream Vortex implements it as a footer row-count wrapper.
- This approval is intentionally narrower than encoded-data execution: `VortexFile::row_count` is
  contract-usable but still not execution-usable under the encoded-read API boundary.
- `LayoutReader::row_count` remains metadata-like but deferred because constructing layout readers
  is not yet an approved count execution path.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data,
  read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt
  fallback execution.

## CG-2.1e.6 encoded-count data-path approval boundary

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexEncodedCountDataPathApprovalReport` now consumes `VortexCountReadinessReport` and
  `VortexEncodedReadApiBoundaryReport` to decide whether encoded-data `CountAll` can even be
  approved for deferred execution planning.
- The recorded public API boundary remains blocked: `VortexFile::row_count` is metadata count
  evidence, but execution-usable data path count is zero and scan/stream/evaluation/data-source
  surfaces remain blocked or deferred.
- This pass makes the remaining blocker explicit before actual encoded-data count execution work.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data,
  read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt
  fallback execution.

## CG-2.1e.7 encoded-count approval CLI surfacing

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `shardloom vortex-encoded-count-approval-plan` now surfaces
  `VortexEncodedCountDataPathApprovalReport` in text/JSON CLI envelopes.
- The command is report-only: recorded public API blockers remain visible and ready encoded-data
  count inputs return deterministic unsupported/non-zero status until an execution-usable data path
  exists.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data,
  read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt
  fallback execution.

## CG-2.1e.8 encoded-count approval local guard

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `execute_vortex_count_all_from_encoded_count_data_path_approval` now requires
  `VortexEncodedCountDataPathApprovalReport` before local encoded-count planning can advance.
- The recorded public API boundary is rejected by this guard; a future approved boundary can only
  produce deferred `NeedsEncodedRead`, not actual scan/data execution.
- This pass does not call scan/read-start APIs, array stream/evaluation APIs, traverse encoded data,
  read rows, decode/materialize, convert to `Arrow`, perform object-store IO, write, or attempt
  fallback execution.

## CG-2.1e.9 layout-reader construction blocker hardening

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexFile::layout_reader` is now a named runtime-driver blocker because upstream construction
  reaches `VortexFile::segment_source`, whose public contract may spawn a background I/O driver.
- `LayoutReader::row_count` remains metadata-like and non-blocking by itself, but it does not
  approve encoded-count execution because the construction boundary remains unapproved.
- Count-readiness and encoded-count approval preserve the layout-reader blocker by name while
  excluding metadata-only row-count surfaces from execution blockers.
- This pass does not construct `LayoutReader`, call scan/read-start APIs, array stream/evaluation
  APIs, traverse encoded data, read rows, decode/materialize, convert to `Arrow`, perform
  object-store IO, write, or attempt fallback execution.

## CG-2.1e.10 layout-driver approval boundary

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexLayoutReaderDriverApprovalReport` defines the report-only gate that must approve any future
  `LayoutReader::row_count` use.
- The recorded public API boundary remains blocked unless local fixture scope, caller session,
  runtime-driver permission, row-count-only intent, no
  scan/evaluation/data-read/decode/materialization/Arrow/object-store/write, and no-fallback signals
  are explicit.
- Even approved reports construct no `LayoutReader`, start no driver, call no scan/evaluation API,
  read no data or rows, decode/materialize nothing, convert nothing to `Arrow`, perform no
  object-store IO or writes, and do not allow fallback.
- This pass adds no runtime invocation, dependency, parser, adapter runtime, object-store IO, write
  behavior, or fallback execution.

## CG-2.1e.11 layout-driver approval CLI surfacing

- Primary RFC linkage: RFC 0010 Developer Experience, RFC 0012 Diagnostics/Capabilities, RFC 0025
  Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `shardloom vortex-layout-driver-approval-plan <signals> [--format text|json]` exposes the
  layout-driver approval report for human and agent inspection.
- The command consumes only explicit signal text and the static encoded-read public API boundary
  report; it performs no filesystem, network, catalog, adapter, scan, evaluation, or data-read
  probing.
- Missing/unknown signals fail deterministically, and the recorded public API boundary remains
  unsupported unless runtime-driver permission is explicit.
- This pass adds no runtime invocation, dependency, parser, adapter runtime, object-store IO, write
  behavior, or fallback execution.

## CG-2.1e.12 layout-approved encoded count bridge

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `VortexEncodedCountDataPathApprovalInput` can now carry a matching
  `VortexLayoutReaderDriverApprovalReport`.
- Encoded-count approval can reach `approved_for_deferred_count` when count readiness is ready and
  the layout approval report is approved, side-effect-free, fallback-disabled, and built from the
  same API boundary.
- `shardloom vortex-encoded-count-approval-plan ... --layout-row-count-approved` exposes this bridge
  in CLI output with layout approval status and row-count path approval fields.
- This pass still performs no actual encoded-data traversal, layout-reader construction,
  runtime-driver startup, scan/read-start invocation, row read, decode/materialization, Arrow
  conversion, object-store IO, write behavior, spill IO, external baseline invocation, or fallback
  execution.

## CG-2.1e.13 layout-approved local count guard

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex
  encoded-read/query-readiness boundaries.
- `execute_vortex_count_all_from_encoded_count_data_path_approval` now has coverage for a
  layout-row-count-approved encoded-count approval report, returning only the existing deferred
  `NeedsEncodedRead` local plan.
- `shardloom vortex-encoded-count-approval-plan ... --layout-row-count-approved` now includes local
  execution status fields when approval is present, while preserving `data_read=false`.
- This pass performs no actual encoded-data traversal, layout-reader construction, runtime-driver
  startup, scan/read-start invocation, row read, decode/materialization, Arrow conversion,
  object-store IO, write behavior, spill IO, external baseline invocation, or fallback execution.

## CG-2.1e.14 encoded-count local guard capability discovery

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex
  encoded-read/query-readiness boundaries.
- `VortexEncodedCountLocalGuardDiscoveryReport` records the static local guard contract for approved
  encoded-count paths without probing runtime inputs.
- `shardloom capabilities operators --format json` emits accepted approval sources, deferred local
  execution status, plan-only mode, no count result, no data read, no decode/materialization, no
  runtime execution, and no fallback.
- This pass performs no actual encoded-data traversal, layout-reader construction, runtime-driver
  startup, scan/read-start invocation, row read, decode/materialization, Arrow conversion,
  object-store IO, write behavior, spill IO, external baseline invocation, or fallback execution.

## CG-2.1e.15 local fixture Vortex array scan/count proof

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `execute_vortex_count_all_from_local_scan_with_session` adds a feature-gated local fixture path
  that requires caller-owned `VortexSession`, caller-owned blocking runtime, local `.vortex` target,
  and encoded-read readiness approved for future execution.
- The helper calls `VortexFile::scan` and `ScanBuilder::into_array_iter` only inside the
  `vortex-encoded-read-spike` local fixture boundary, then counts returned Vortex arrays via
  `ArrayRef::len()`.
- The report records `data_read=true`, `upstream_scan_called=true`, array count, row count, and
  count result.
- The report records no row reads, no requested decode/materialization, no Arrow conversion, no
  object-store IO, no writes, no spill IO, and no fallback execution.
- The general public scan/read-start API boundary remains conservative; this pass does not approve
  adapters, non-fixture sources, encoded predicates, projections, object-store targets, benchmarks,
  external baselines, parser/runtime expansion, or superiority claims.

## CG-2.1e.16 approval-gated local fixture scan/count

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex
  encoded-read/query-readiness boundaries.
- `execute_vortex_count_all_from_local_scan_with_session` now requires an approved
  `VortexEncodedCountDataPathApprovalReport` before the local fixture scan/count path can run.
- Recorded public API-boundary approval blockers return a blocked report before `VortexFile::scan`
  or `ScanBuilder::into_array_iter` is called.
- Approved reports still require encoded-read readiness, caller-owned session/runtime, and local
  `.vortex` scope.
- This keeps the CG-2.1e approval chain authoritative as execution begins: no row reads, requested
  decode/materialization, Arrow conversion, object-store IO, writes, spill IO, external baselines,
  or fallback execution are added.

## CG-2.1e.17 local fixture scan target consistency

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex
  encoded-read/query-readiness boundaries.
- The local fixture scan/count helper now derives source URI evidence from the encoded-read
  readiness planning chain before scan.
- Approval target URI and encoded-read readiness source URI must match exactly before
  `VortexFile::scan` or `ScanBuilder::into_array_iter` is called.
- Missing readiness source URI evidence or a target mismatch returns a blocked report with
  `data_read=false`, `upstream_scan_called=false`, and `fallback_execution_allowed=false`.
- This prevents cross-target evidence reuse while keeping the local fixture exception narrow: no row
  reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO,
  external baselines, or fallback execution are added.

## CG-2.1e.18 local fixture scan source evidence reporting

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex
  encoded-read/query-readiness boundaries.
- `VortexEncodedReadExecutionReport` now exposes local fixture scan target URI, encoded-read
  readiness source URI, and a source/target match flag.
- Successful local fixture reports, target-mismatch reports, object-store blocked reports, and
  approval-blocked reports preserve the source-evidence fields for auditability.
- These fields make the narrow fixture proof easier to validate before generalized count execution
  while keeping non-fixture scan/read-start approval deferred.
- No row reads, requested decode/materialization, Arrow conversion, object-store IO expansion,
  writes, spill IO, external baselines, or fallback execution are added.

## CG-2.1e.19 explicit local encoded-count execution boundary

- Primary RFC linkage: RFC 0005 Vortex-Native File IO and Output Contract, RFC 0012
  Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC 0015 Correctness/testing, RFC
  0025 Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `vortex_encoded_read_local_scan_count_api_boundary` marks only `OpenOptionsSessionExt::open_path`,
  `VortexFile::scan`, and `ScanBuilder::into_array_iter` as execution-usable, and only for local
  `.vortex` `CountAll`.
- `execute_vortex_count_all_from_approved_local_scan` owns the upstream runtime/session setup for
  that approved local boundary while preserving encoded-count approval and source-match gates.
- `shardloom vortex-encoded-read-spike ... --execute-local-count` exposes the path as an explicit
  CLI opt-in and reports count result, arrays read, rows counted, scan target, readiness source, and
  source-match evidence.
- The broad public API boundary remains conservative; generalized encoded-data count execution,
  adapters, non-local sources, object-store IO, encoded predicates, projections, writes, benchmarks,
  external baselines, CG closeout, and fallback execution remain out of scope.
- No row reads, requested decode/materialization, Arrow conversion, object-store IO expansion,
  writes, spill IO, external baselines, or fallback execution are added.

## CG-2.1e.20 approved local scan naming normalization

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex
  encoded-read/query-readiness boundaries.
- `VortexEncodedReadExecutionStatus`, `VortexEncodedReadExecutionMode`,
  `VortexEncodedReadExecutionReport`, diagnostics, human text, and focused tests now use
  `local_scan` naming for the approved local count path.
- The CLI output keeps the existing `local_scan_*` fields while reading the renamed report fields.
- Historical layout-driver `local-fixture-only` input remains unchanged to avoid a public signal
  rename outside this cleanup scope.
- This is naming/report-surface cleanup only: generalized encoded-data count execution, adapters,
  non-local sources, object-store IO, encoded predicates, projections, writes, benchmarks, external
  baselines, CG closeout, and fallback execution remain out of scope.

## CG-2.1e.21 approved local scan result bridge

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex
  encoded-read/query-readiness boundaries.
- `execute_vortex_count_all_from_approved_local_scan_result` now consumes approved encoded-count
  data-path evidence plus a successful approved local scan/count report and returns local execution
  evidence with a known `CountAll` value.
- The bridge requires enabled feature evidence, `local_scan_encoded_count_executed`,
  `local_scan_encoded_array_length_count`, matching approval target/readiness source URI evidence, a
  known count result, and `rows_counted == count_result`.
- The bridge rejects missing count results, target/source mismatches, disabled feature reports,
  unsuccessful scan reports, row reads, requested decode/materialization, Arrow conversion,
  object-store IO, writes, spill IO, external effects, and fallback execution.
- `shardloom vortex-encoded-read-spike ... --execute-local-count` now emits local execution status,
  mode, known result/value, task/data-read evidence, and side-effect/no-fallback fields alongside
  the local scan report.
- This is still not generalized encoded-data count execution: adapters, non-local sources,
  object-store IO, encoded predicates, projections, writes, benchmarks, external baselines, CG
  closeout, and fallback execution remain out of scope.

## CG-2.1e.22 stable explicit local encoded `CountAll` execution surface

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex
  encoded-read/query-readiness boundaries.
- `shardloom vortex-count <dataset_uri>` remains metadata-only by default.
- `shardloom vortex-count <dataset_uri> --execute-local-encoded-count <memory_gb> <max_parallelism>`
  now explicitly opts into the same approved local `.vortex` encoded `CountAll` path that was
  previously reachable only through the spike command.
- The stable command reuses encoded-read readiness, encoded-count data-path approval, approved local
  scan/count execution, and approved local scan result bridging before reporting a known count
  value.
- CLI output records local scan target URI, readiness source URI, source-match evidence, arrays
  read, rows counted, count result, local execution status, side-effect flags, and
  `fallback_execution_allowed=false`.
- This does not approve broad scan/read-start execution, adapters, non-local sources, object-store
  IO, encoded predicates, projections, row reads, requested decode/materialization, Arrow
  conversion, writes, spill IO, benchmarks, external baselines, CG closeout, or fallback execution.

## CG-2.1e.23 generalized encoded primitive execution gate

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0026 Vortex
  encoded-read/query-readiness boundaries, RFC 0029 Correctness/Benchmarks/Execution Certificates,
  RFC 0031 Universal Native I/O Envelope, and RFC 0032 World-Class Capability Surface.
- `shardloom-vortex/src/generalized_encoded_primitive_gate.rs` adds
  `VortexGeneralizedEncodedPrimitiveGateReport`, a report-only gate for direct count, filtered
  count, and projection primitive execution readiness.
- `shardloom vortex-generalized-encoded-primitive-gate` emits stable text/JSON fields for primitive
  state, current evidence, required next evidence, blockers, side-effect flags, and no-fallback
  evidence.
- The gate records that only explicit local `.vortex` `CountAll` execution is proven; metadata-proof
  filtered count and projection readiness remain distinct from encoded predicate/projection
  execution.
- Generalized count/filter/project execution remains blocked until public data-path approval,
  encoded predicate/projection paths, selection-vector pipeline proof, native I/O certificates,
  execution certificates, correctness fixtures, and benchmark evidence exist.
- This phase adds no broad scan/read-start approval, generalized encoded-data execution, encoded
  predicate execution, projection execution, adapter runtime, non-local source read, object-store
  IO, row read, requested decode/materialization, Arrow conversion, write IO, spill IO,
  benchmark/superiority claim, CG-1/CG-2/CG-13 closeout, external engine invocation, or fallback
  execution.

## CG-2.1e.24 / CG-13.4 local encoded `CountAll` target policy evidence

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0026 Vortex
  encoded-read/query-readiness boundaries, and RFC 0029 Correctness/Benchmarks/Execution
  Certificates.
- `shardloom vortex-count <dataset_uri> --execute-local-encoded-count <memory_gb> <max_parallelism>`
  now emits target-policy evidence for the explicit local encoded `CountAll` path.
- The target policy distinguishes `known_fixture_certified`, `local_vortex_uncertified`, and
  `blocked` so arbitrary local `.vortex` targets can execute under the narrow feature gate without
  being mistaken for fixture-certified evidence.
- Non-fixture local count output remains explicitly uncertified and reports required
  correctness-fixture and benchmark evidence before any production, CG-2, or CG-13 closeout claim.
- `shardloom-cli` now forwards the `vortex-encoded-read-spike` feature to `shardloom-vortex`,
  allowing the stable CLI command to be validated directly under the same local-only encoded-read
  gate.
- This phase adds no generalized encoded filter/projection execution, adapter runtime, non-local
  source read, object-store IO, row read, requested decode/materialization, Arrow conversion, write
  IO, spill IO, benchmark/superiority claim, CG-2/CG-13 closeout, external engine invocation, or
  fallback execution.

## CG-2.1e.25-CG-2.1e.27 / CG-13.6-CG-13.8 local Vortex primitive execution

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0021 Expression/Kernel Registry, RFC 0025 Competitive/no-fallback,
  RFC 0026 Vortex encoded-read/query-readiness boundaries, RFC 0029 Correctness/Benchmarks/Execution
  Certificates, RFC 0031 Universal Native I/O Envelope, and RFC 0032 World-Class Capability Surface.
- `shardloom-vortex/src/local_primitives.rs` and `shardloom-vortex/src/local_engine.rs` expose the
  feature-gated local `.vortex` primitive surface for `count`, `count-where:<predicate>`,
  `filter:<predicate>`, and `project:<columns>`.
- CG-2.1e.25 established the executable local primitive surface while preserving honest
  materialization-boundary reporting for temporary decoded paths.
- CG-2.1e.26 tightened effect reporting so metadata predicates, validity predicates, and schema-only
  projection do not falsely report decode/materialization.
- CG-2.1e.27 / CG-13.8 moves supported local filter/project/count-where primitives onto upstream
  Vortex scan filter/projection expressions and records filter/projection pushdown evidence in
  runtime and CLI output.
- `benchmarks/traditional_analytics/run.py` surfaces the new pushdown fields in ShardLoom native
  microbenchmark rows so comparison reports distinguish scan-pushdown evidence from traditional
  compatibility-file rows that still use temporary operators.
- These phases keep mature SQL/DataFrame/API/adapters, generalized encoded operator certification,
  non-local source support, object-store IO, row reads, Arrow conversion, writes, spill IO,
  distributed execution, benchmark/superiority claims, CG-2 closeout, CG-13 closeout, external
  engine invocation, and fallback execution out of scope.

## CG-2.1e.34 / CG-13.18 / CG-16.13 / CG-19.15 generalized local `CountAll` execution evidence

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0026 Vortex
  encoded-read/query-readiness boundaries, RFC 0029 Correctness/Benchmarks/Execution Certificates,
  and RFC 0031 Universal Native I/O Envelope.
- The explicit local `.vortex` `CountAll` path is validated against copied/non-fixture local Vortex
  files, not only checked-in fixture paths.
- Copied/non-fixture local `.vortex` targets remain execution-allowed under
  `local_vortex_uncertified` and emit certified local Native I/O evidence when the encoded-count
  runtime evidence is side-effect-free.
- Correctness fixtures, execution certificates, physical-kernel evidence, kernel admission,
  production claims, CG-2 closeout, and CG-13 closeout remain unavailable for non-fixture targets
  until CG-5 and CG-6 evidence exists.
- The local-engine why report now asks for broader CG-5/CG-6 evidence and future native-adapter
  expansion instead of implying local CountAll execution is still fixture-only.
- This phase adds no generalized encoded filter/projection execution, adapter runtime, non-local
  source read, object-store IO, row read, requested decode/materialization, Arrow conversion, write
  IO, spill IO, benchmark/superiority claim, CG-2/CG-13 closeout, external engine invocation, or
  fallback execution.

## CG-2.2h / CG-13.19 / CG-19.16 generalized local filter execution surface

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0021 Expression/Kernel Registry, RFC 0025 Competitive/no-fallback,
  RFC 0026 Vortex encoded-read/query-readiness boundaries, RFC 0029 Correctness/Benchmarks/Execution
  Certificates, and RFC 0031 Universal Native I/O Envelope.
- `shardloom-vortex/src/generalized_filter_execution.rs` adds a reusable report around feature-gated
  local CountWhere and FilterPredicate scan-pushdown execution.
- The surface validates copied/non-fixture local `.vortex` filter and count-where requests, emits
  selected-row and selection-vector guarantee evidence, and attaches certified Native I/O evidence
  when the local scan is side-effect-free.
- The generalized primitive gate now distinguishes local filter scan-pushdown evidence from
  still-blocked broad encoded-value predicate kernels, so local CountAll is no longer the only
  executable local primitive evidence.
- Correctness certificates, physical-kernel production admission, broad encoded-value predicate
  kernels, non-local sources, object-store reads, adapters, projection/filter-project
  generalization, SQL/DataFrame runtime, writes, spill, benchmark claims, CG-2 closeout, and CG-13
  closeout remain out of scope.
- This phase adds no new reader, parser, dependency, external engine invocation, or fallback
  execution.

## CG-2.3e / CG-13.20 / CG-19.17 generalized local projection/filter-project execution surface

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0021 Expression/Kernel Registry, RFC 0025 Competitive/no-fallback,
  RFC 0026 Vortex encoded-read/query-readiness boundaries, RFC 0029 Correctness/Benchmarks/Execution
  Certificates, and RFC 0031 Universal Native I/O Envelope.
- `shardloom-vortex/src/generalized_projection_execution.rs` adds a reusable report around
  feature-gated local ProjectColumns and FilterAndProject scan-pushdown execution.
- The surface validates copied/non-fixture local `.vortex` project and filter-project requests,
  emits projected-column and encoded-projection guarantee evidence, emits selection-vector evidence
  for filter-project, and attaches certified Native I/O evidence when the local scan is
  side-effect-free.
- The generalized primitive gate now distinguishes local projection scan-pushdown evidence from
  still-blocked broad encoded projection kernels, so projection is no longer represented as
  readiness-only.
- Correctness certificates, physical-kernel production admission, broad encoded projection kernels,
  broad encoded-value predicate kernels, non-local sources, object-store reads, adapters,
  SQL/DataFrame runtime, writes, spill, benchmark claims, CG-2 closeout, and CG-13 closeout remain
  out of scope.
- This phase adds no new reader, parser, dependency, external engine invocation, or fallback
  execution.

## CG-5.11 / CG-16.14 generalized local primitive fixture certificates

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0029
  Correctness/Benchmarks/Execution Certificates, and RFC 0031 Universal Native I/O Envelope.
- `shardloom-vortex/src/local_primitives.rs` now exposes reusable checked-in fixture matching for
  local primitive request/report pairs instead of keeping that evidence path only in CLI helpers.
- Generalized filter and projection reports attach optional CG-16 execution certificates when
  CountWhere, FilterPredicate, ProjectColumns, or FilterAndProject requests exactly match checked-in
  local primitive fixture evidence.
- Copied/non-fixture local `.vortex` paths remain execution-allowed and Native-I/O-certified but
  explicitly uncertified for correctness and production claims.
- This phase adds no new fixture family, decoded reference artifact, broad encoded-value
  predicate/projection kernel, non-local source, adapter runtime, object-store IO, SQL/DataFrame
  runtime, write behavior, spill, benchmark claim, external engine invocation, or fallback
  execution.

## CG-2.2i / CG-7.26 / CG-13.21 encoded-value predicate kernel foundation

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0021 Expression/Kernel Registry, RFC 0025
  Competitive/no-fallback, RFC 0026 Vortex encoded-read/query-readiness boundaries, RFC 0029
  Correctness/Benchmarks/Execution Certificates, and RFC 0031 Universal Native I/O Envelope.
- `shardloom-core/src/encoded.rs` adds native encoded-value predicate evaluation for constant,
  dictionary-coded, and run-length encoded batches.
- The kernel emits `SelectionVector::All`, `SelectionVector::None`, or sparse
  `SelectionVector::Indices` without row reads, decoded row materialization, Arrow conversion,
  object-store IO, writes, spill, or fallback execution.
- Dictionary and run-length tests cover sparse selection vectors; constant-null tests cover null
  predicate behavior; mismatch tests keep unsupported/type/row-count cases deterministic and
  no-fallback.
- `shardloom-vortex/src/encoded_predicate_evaluation.rs` now counts sparse selection-vector segment
  reports in the aggregate predicate report.
- This phase does not wire the kernel to Vortex readers/adapters, broaden non-local sources, add
  SQL/DataFrame/runtime adapters, write outputs, rerun benchmarks, certify production claims, close
  CG-2/CG-13, or add fallback execution.

## CG-2.2j / CG-7.27 / CG-13.22 encoded-value predicate bridge to Vortex filter evidence

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0021 Expression/Kernel Registry, RFC 0025
  Competitive/no-fallback, RFC 0026 Vortex encoded-read/query-readiness boundaries, RFC 0029
  Correctness/Benchmarks/Execution Certificates, and RFC 0031 Universal Native I/O Envelope.
- `shardloom-vortex/src/encoded_predicate_evaluation.rs` adds
  `evaluate_vortex_encoded_value_predicate_batch`, a no-reader bridge from explicit encoded segment
  metadata plus encoded-value batches into the normal Vortex encoded predicate aggregate report.
- Sparse encoded-value selections now flow into the existing selection-vector filter-kernel evidence
  path, so dictionary encoded-value predicates can produce safe native filter-kernel evidence
  without local scan-pushdown.
- Unsupported encoded-value type mismatches remain deterministic, side-effect-free, and no-fallback.
- This phase does not open Vortex files, wire readers/adapters, broaden non-local sources, add
  SQL/DataFrame runtime, write outputs, spill, rerun benchmarks, certify production claims, close
  CG-2/CG-13, or add fallback execution.

## CG-2.2k / CG-7.28 / CG-13.23 multi-segment encoded-value filter evidence

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0021 Expression/Kernel Registry, RFC 0025
  Competitive/no-fallback, RFC 0026 Vortex encoded-read/query-readiness boundaries, RFC 0029
  Correctness/Benchmarks/Execution Certificates, and RFC 0031 Universal Native I/O Envelope.
- `shardloom-vortex/src/encoded_predicate_evaluation.rs` adds `VortexEncodedValuePredicateBatch` and
  `evaluate_vortex_encoded_value_predicate_batches`, a reusable no-reader target for aggregating
  prepared segment/value batches into the Vortex encoded predicate report.
- Multi-segment encoded-value evidence can now combine constant, dictionary, and run-length batch
  selections and feed complete selection vectors into the existing Vortex selection-vector
  filter-kernel evidence path.
- Empty prepared-batch inputs block deterministically with no fallback instead of pretending broad
  reader wiring exists.
- This phase does not open Vortex files, wire readers/adapters, broaden non-local sources, add
  SQL/DataFrame runtime, write outputs, spill, rerun benchmarks, certify production claims, close
  CG-2/CG-13, or add fallback execution.

## CG-2.2l / CG-7.30 / CG-13.25 / CG-19.18 generalized prepared encoded filter execution

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0021 Expression/Kernel Registry, RFC 0025
  Competitive/no-fallback, RFC 0026 Vortex encoded-read/query-readiness boundaries, RFC 0029
  Correctness/Benchmarks/Execution Certificates, and RFC 0031 Universal Native I/O Envelope.
- `shardloom-vortex/src/generalized_encoded_filter_execution.rs` adds
  `execute_vortex_generalized_filter_from_encoded_value_batches`, an execution-level target for
  prepared Vortex encoded-value batches.
- The report composes encoded predicate evaluation, selection-vector filter-kernel evidence,
  filter-kernel admission, selected-row evidence, side-effect evidence, no-fallback fields, and a
  CG-19 `cg19.prepared_encoded_filter.native_io` certificate.
- Safe prepared encoded filter execution records `vortex_encoded->selection_vector_encoded` without
  row reads, decode, materialization, Arrow conversion, object-store IO, writes, spill, external
  effects, or fallback execution.
- Empty prepared-batch inputs and encoding mismatches block deterministically and emit blocked
  Native I/O evidence rather than pretending reader/adapters exist.
- The generalized encoded primitive gate now distinguishes prepared encoded filter evidence from
  local scan-pushdown-only evidence.
- This phase does not open Vortex files, wire readers/adapters, broaden non-local sources, add
  SQL/DataFrame runtime, write outputs, spill, rerun benchmarks, certify production claims, close
  CG-2/CG-13/CG-19, or add fallback execution.

## CG-2.3f / CG-7.29 / CG-13.24 prepared encoded projection/filter-project evidence

- Primary RFC linkage: RFC 0013 Streaming/zero-decode boundaries, RFC 0015 Correctness/testing, RFC
  0021 Expression/Kernel Registry, RFC 0025 Competitive/no-fallback, RFC 0026 Vortex
  encoded-read/query-readiness boundaries, RFC 0029 Correctness/Benchmarks/Execution Certificates,
  and RFC 0031 Universal Native I/O Envelope.
- `shardloom-vortex/src/encoded_projection_execution.rs` adds
  `VortexPreparedEncodedProjectionColumn` and `evaluate_vortex_prepared_encoded_projection`, a
  no-reader projection target for explicitly supplied encoded column batches.
- Prepared projection evidence preserves encoded batches for requested columns without row reads,
  decode, materialization, Arrow conversion, object-store IO, writes, spill, or fallback.
- Filter-project composition can carry safe selection-vector filter-kernel evidence from CG-7.28
  while preserving encoded projected batches.
- Missing projected columns and unsafe filter-kernel evidence block deterministically with no
  fallback instead of pretending broad reader wiring exists.
- This phase does not open Vortex files, wire readers/adapters, broaden non-local sources, add
  SQL/DataFrame runtime, write outputs, spill, rerun benchmarks, certify production claims, close
  CG-2/CG-13, or add fallback execution.

## CG-2.3g / CG-7.31 / CG-13.26 / CG-19.19 generalized prepared encoded projection/filter-project execution

- Primary RFC linkage: RFC 0013 Streaming/zero-decode boundaries, RFC 0015 Correctness/testing, RFC
  0021 Expression/Kernel Registry, RFC 0025 Competitive/no-fallback, RFC 0026 Vortex
  encoded-read/query-readiness boundaries, RFC 0029 Correctness/Benchmarks/Execution Certificates,
  and RFC 0031 Universal Native I/O Envelope.
- `shardloom-vortex/src/generalized_encoded_projection_execution.rs` adds
  `execute_vortex_generalized_projection_from_encoded_projection_batches`, an execution-level target
  for prepared Vortex encoded projection batches and optional safe filter-kernel evidence.
- The report composes prepared projection evidence, optional filter-project selection-vector
  evidence, selected-row/projected-row evidence, side-effect evidence, no-fallback fields, and a
  CG-19 `cg19.prepared_encoded_projection.native_io` certificate.
- Safe prepared encoded projection records `vortex_encoded->vortex_encoded`; safe prepared encoded
  filter-project records `vortex_encoded->selection_vector_encoded` without row reads, decode,
  materialization, Arrow conversion, object-store IO, writes, spill, external effects, or fallback
  execution.
- Missing requested columns block deterministically and emit blocked Native I/O evidence rather than
  pretending reader/adapters exist.
- The generalized encoded primitive gate now distinguishes prepared encoded projection evidence from
  local projection scan-pushdown-only evidence.
- This phase does not open Vortex files, wire readers/adapters, broaden non-local sources, add
  SQL/DataFrame runtime, write outputs, spill, rerun benchmarks, certify production claims, close
  CG-2/CG-13/CG-19, or add fallback execution.

## CG-16.15 prepared encoded execution certificate surfacing

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0029
  Correctness/Benchmarks/Execution Certificates, RFC 0031 Universal Native I/O Envelope, and RFC
  0032 capability certification.
- `shardloom-vortex/src/generalized_encoded_filter_execution.rs` now attaches a CG-16
  `ExecutionCertificate` to prepared encoded filter execution reports.
- `shardloom-vortex/src/generalized_encoded_projection_execution.rs` now attaches a CG-16
  `ExecutionCertificate` to prepared encoded projection and filter-project execution reports.
- Safe prepared execution paths emit `evidence_incomplete` execution certificates with actual
  row-count evidence, no unsafe effects, and `fallback_attempted=false` until CG-5
  fixtures/reference outputs certify correctness.
- Unsafe prepared evidence, missing encoded batches, missing requested columns, or failed Native I/O
  evidence emits blocked execution certificates instead of widening runtime behavior.
- This phase does not add CG-5 fixture families, decoded-reference artifacts, reader/adapters,
  non-local sources, object-store IO, SQL/DataFrame runtime, writes, spill, benchmark reruns,
  production certification, or fallback execution.

## CG-5.12 / CG-16.16 prepared encoded correctness fixtures

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0029
  Correctness/Benchmarks/Execution Certificates, RFC 0031 Universal Native I/O Envelope, and RFC
  0032 capability certification.
- `shardloom-core/src/correctness.rs` adds generated golden fixtures for prepared encoded filter,
  prepared encoded projection, and prepared encoded filter-project evidence.
- `shardloom-contract-tests/tests/correctness_fixture_manifest.rs` and
  `correctness_differential_harness.rs` update the fixture inventory and harness counts so the
  prepared encoded reference outputs are visible to CG-5.
- Prepared encoded filter execution certificates now certify the exact dictionary/run-length fixture
  shape with five selected rows.
- Prepared encoded projection and filter-project execution certificates now certify the exact
  dictionary projection and selection-vector filter-project fixture shapes.
- This phase does not add broad edge-case fixture families, decoded-reference artifacts, external
  oracle execution, reader/adapters, non-local sources, object-store IO, SQL/DataFrame runtime,
  writes, spill, benchmark reruns, production certification, or fallback execution.

## CG-5.13 decoded-reference artifact coverage for prepared encoded primitives

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0029
  Correctness/Benchmarks/Execution Certificates, RFC 0031 Universal Native I/O Envelope, and RFC
  0032 capability certification.
- `ReferenceArtifact` records test-only decoded-reference output metadata with expected output,
  semantic profile, materialization-boundary label, `execution_performed=false`, and
  `fallback_attempted=false`.
- `CorrectnessFixture` now carries reference artifacts separately from reference roles so
  decoded-reference artifacts can be counted without implying decoded-reference execution.
- The prepared encoded filter, projection, and filter-project fixtures now attach decoded-reference
  row-output artifacts for their deterministic expected row counts.
- `CorrectnessValidationPlan`, `CorrectnessDifferentialHarnessReport`, `correctness-plan`, and
  `correctness-harness-plan` surface decoded-reference artifact count, artifact id order, and
  incomplete coverage status.
- Decoded-reference output coverage remains incomplete until every executable fixture family has
  appropriate reference artifacts; property/fuzz and benchmark claim gates remain blocked.
- This phase adds no decoded-reference execution, external oracle execution, data reads,
  reader/adapters, non-local sources, object-store IO, SQL/DataFrame runtime, writes, spill,
  benchmark reruns, production certification, superiority claim, or fallback execution.

## CG-5.14 complete decoded-reference artifact coverage for executable fixtures

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0029
  Correctness/Benchmarks/Execution Certificates, RFC 0031 Universal Native I/O Envelope, and RFC
  0032 capability certification.
- The local encoded `CountAll` fixture, checked-in struct `CountAll` fixture, and checked-in struct
  count-where/filter/project/filter-project fixtures now attach test-only decoded-reference
  artifacts.
- Current executable fixture families now have decoded-reference artifact coverage through
  `CorrectnessValidationPlan::decoded_reference_output_coverage_complete`.
- `correctness-harness-plan` no longer lists `decoded_reference_outputs` as a blocked surface, while
  property/fuzz and benchmark claim gates remain blocked.
- This phase adds no decoded-reference execution, external oracle execution, data reads,
  reader/adapters, non-local sources, object-store IO, SQL/DataFrame runtime, writes, spill,
  benchmark reruns, production certification, superiority claim, or fallback execution.

## CG-5.15 generated edge-case executable fixture matrix

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0021 Expression/Kernel Registry, RFC 0025
  Competitive/no-fallback, RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0031
  Universal Native I/O Envelope, and RFC 0032 capability certification.
- `CorrectnessValidationPlan::default_foundation_plan` now includes generated executable fixtures
  for empty input, single-row projection, all-null filter, mixed-null sparse selection, duplicate
  low-cardinality filter, high-cardinality projection, sorted dictionary filter-project, unsorted
  run-length filter-project, and temporal filter cases.
- Every generated edge-case executable fixture has deterministic expected output and a test-only
  decoded-reference artifact with `execution_performed=false` and `fallback_attempted=false`.
- `correctness-plan` and `correctness-harness-plan` now surface 31 total fixtures, 19 golden
  fixtures, 18 decoded-reference artifacts, and 18 executable expected outputs while keeping
  property/fuzz and benchmark claim gates blocked.
- This phase adds no source-backed generated data files, property/fuzz execution, external oracle
  execution, decoded-reference execution, data reads, reader/adapters, non-local sources,
  object-store IO, SQL/DataFrame runtime, writes, spill, benchmark reruns, production certification,
  superiority claim, or fallback execution.

## CG-5.16 generated property/fuzz fixture metadata

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0021 Expression/Kernel Registry, RFC 0025
  Competitive/no-fallback, RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0031
  Universal Native I/O Envelope, and RFC 0032 capability certification.
- `CorrectnessValidationPlan::default_foundation_plan` now includes generated property fixtures for
  encoded filter selection-vector consistency, encoded projection row-order preservation, and
  encoded filter-project composition.
- The CG-5 aggregate plan includes reproducible fuzz seeds for encoded filter selection vectors,
  encoded projection ordering, and encoded filter-project composition.
- `correctness-harness-plan` now surfaces 34 total fixtures, 3 generated property fixtures, 3 fuzz
  seeds, and only `benchmark_claim_gate` as the remaining blocked aggregate surface.
- This phase adds no property/fuzz execution, source-backed generated data files, external oracle
  execution, decoded-reference execution, data reads, reader/adapters, non-local sources,
  object-store IO, SQL/DataFrame runtime, writes, spill, benchmark reruns, production certification,
  superiority claim, or fallback execution.

## CG-5.17 source-backed edge fixture and external-oracle artifact metadata

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0029
  Correctness/Benchmarks/Execution Certificates, RFC 0031 Universal Native I/O Envelope, and RFC
  0032 capability certification.
- The executable edge-case fixture families now reference the checked-in
  `docs/fixtures/correctness/source-backed-edge-fixtures.json` manifest without reading or executing
  it.
- `ExternalOracleResultArtifact` declares comparison-only result artifact slots for each
  source-backed edge fixture across Spark, DataFusion, DuckDB, Polars, pandas, Dask, and Velox.
- `CorrectnessDifferentialHarnessReport`, `correctness-plan`, and `correctness-harness-plan` surface
  source-backed edge fixture counts, external-oracle artifact counts, artifact status order, and
  test-only/no-fallback fields.
- The aggregate harness now distinguishes `source_backed_edge_fixtures` and
  `external_oracle_result_artifacts` surfaces while keeping only `benchmark_claim_gate` blocked.
- This phase adds no external oracle execution, property/fuzz execution, decoded-reference
  execution, data reads, reader/adapters, non-local sources, object-store IO, SQL/DataFrame runtime,
  writes, spill, benchmark reruns, production certification, superiority claim, or fallback
  execution.

## CG-5.18 claim-gate execution blockers for declared evidence

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0029
  Correctness/Benchmarks/Execution Certificates, RFC 0031 Universal Native I/O Envelope, and RFC
  0032 capability certification.
- `CorrectnessDifferentialHarnessReport` now surfaces `external_oracle_result_populated_count`,
  `external_oracle_results_populated`, `property_fuzz_execution_performed`, and
  `benchmark_claim_blocker_order`.
- At this phase, the benchmark claim gate remains blocked by unresolved fixture expectations,
  declared-but-unpopulated external-oracle result artifacts, and unperformed property/fuzz
  execution.
- Declared external-oracle artifacts remain comparison-only and non-executed; their presence no
  longer risks claim completion once fixture expectations are filled in later.
- This phase adds no external oracle execution, property/fuzz execution, decoded-reference
  execution, data reads, reader/adapters, non-local sources, object-store IO, SQL/DataFrame runtime,
  writes, spill, benchmark reruns, production certification, superiority claim, or fallback
  execution.

## CG-5.19 deferred fixture-family blockers

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0029
  Correctness/Benchmarks/Execution Certificates, RFC 0031 Universal Native I/O Envelope, and RFC
  0032 capability certification.
- `ExpectedOutcome` now distinguishes explicit deferred fixture-family requirements from truly
  `NotYetDefined` expectations.
- `CorrectnessValidationPlan`, `CorrectnessDifferentialHarnessReport`, `correctness-plan`, and
  `correctness-harness-plan` now surface deferred fixture-family count and fixture ID order.
- The current foundation plan reports zero `NotYetDefined` fixtures and eight explicit deferred
  fixture-family requirements.
- The benchmark claim gate remains blocked by `deferred_fixture_families`, declared-but-unpopulated
  external-oracle result artifacts, and unperformed property/fuzz execution.
- This phase adds no decoded-reference execution, external oracle execution, property/fuzz
  execution, data reads, reader/adapters, non-local sources, object-store IO, SQL/DataFrame runtime,
  writes, spill, benchmark reruns, production certification, superiority claim, or fallback
  execution.

## CG-5.20 deferred fixture-family artifact slots

- Primary RFC linkage: RFC 0015 Correctness/testing, RFC 0025 Competitive/no-fallback, RFC 0029
  Correctness/Benchmarks/Execution Certificates, RFC 0031 Universal Native I/O Envelope, and RFC
  0032 capability certification.
- `DeferredFixtureFamilyArtifact` now declares required evidence slots for each deferred fixture
  family, including required fixture-manifest refs, required decoded-reference refs, status,
  semantic profile, materialization-boundary label, and no-execution/no-fallback fields.
- `CorrectnessValidationPlan`, `CorrectnessDifferentialHarnessReport`, `correctness-plan`, and
  `correctness-harness-plan` now surface deferred fixture-family artifact count, populated count,
  populated status, artifact ID order, status order, and test-only status.
- The aggregate harness now treats `deferred_fixture_family_artifacts` as a distinct evidence
  surface and keeps it blocked while artifact slots are declared but unpopulated.
- The benchmark claim gate now reports `deferred_fixture_family_artifacts_not_populated` instead of
  the broader `deferred_fixture_families` blocker, while external-oracle population and
  property/fuzz execution blockers remain.
- This phase adds no decoded-reference execution, external oracle execution, property/fuzz
  execution, data reads, reader/adapters, non-local sources, object-store IO, SQL/DataFrame runtime,
  writes, spill, benchmark reruns, production certification, superiority claim, or fallback
  execution.

## RFC/Vortex provider alignment drift cleanup

- Primary RFC linkage: RFC 0002 No Fallback/Vortex I/O, RFC 0031 Universal Native I/O Envelope, RFC
  0032 Capability Surface, RFC 0033 User Workflow, RFC 0034 Engine Fabric, and RFC 0035
  REST/Event/API.
- The historical phase-plan cleanup moved then-active docs-session evidence into the completed
  ledger. The current phase plan now uses only Planned and Completed as operational sections.
- RFC 0002 now clarifies that ShardLoom is standalone from external query-engine fallback while
  upstream Vortex array, compute, scan, source, and sink APIs may be native providers when approved,
  feature-gated, version-recorded, policy-admitted, and certificate-backed.
- The Vortex Scan API skill prompt now preserves ShardLoom-owned admission, policy, diagnostics,
  certificate, and capability semantics instead of implying low-level upstream Vortex
  scan/source/sink providers are forbidden.
- The Vortex upstream dependency review now labels old PR-era fields as historical and points
  readers to the phase plan plus Vortex public API inventory for current executable support.
- The current RFC/phase mapping already covers the newly supplied CG-21 user workflow, CG-22
  batch/live/hybrid, CG-23 remote API, operational evidence/policy/workload/protocol hardening, and
  Vortex upstream/provider alignment items; no new CG lane is introduced.
- This phase adds no runtime behavior, dependency, Vortex API call, reader/writer, benchmark
  execution, package publication, external engine invocation, superiority claim, or fallback
  execution.

## CG-5.1 metadata query primitive correctness fixtures

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0012
  Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex
  encoded-read/query-readiness boundaries.
- `shardloom-contract-tests/tests/query_primitive_correctness.rs` adds cross-crate fixtures for
  metadata-backed `CountAll`, metadata-proven `CountWhere`, inconclusive predicate deferral, and
  projection deferral.
- The fixtures assert exact values for file row-count, segment row-count summing, metadata-proven
  false predicates, metadata-proven true predicates, and deferred encoded-predicate/projection
  cases.
- Every fixture asserts no task execution, data read, decode/materialization, object-store IO, write
  IO, or fallback execution.
- This pass adds no new runtime behavior, external baseline execution, benchmark claim, dependency,
  parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-5.2 metadata query primitive edge and diagnostic fixtures

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0012
  Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex
  encoded-read/query-readiness boundaries.
- `shardloom-contract-tests/tests/query_primitive_correctness.rs` extends cross-crate fixtures to
  missing metadata, metadata-proven true predicates without segment row counts, metadata-pruned
  filters, projection metadata misses, unsupported primitive requests, and local missing-summary
  blocking.
- `shardloom-vortex/src/query_primitive.rs` corrects the `CountAll` missing-metadata reason so
  diagnostic evidence names the evaluated primitive.
- Every fixture asserts no task execution, data read, decode/materialization, object-store IO, write
  IO, spill IO, or fallback execution.
- This pass adds no new query execution behavior, external baseline execution, benchmark claim,
  dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-5.3 correctness fixture manifest contract

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0029
  Correctness/Benchmarks/Execution Certificates, RFC 0012 Diagnostics/Capabilities, and RFC 0025
  Competitive/no-fallback.
- `shardloom-core/src/correctness.rs` adds manifest fields for fixture source references, test-only
  reference roles, metadata row-count reference outputs, and explicit edge-case fixture families.
- `CorrectnessValidationPlan::default_foundation_plan` declares the checked-in
  `metadata_footer_u64_20000.vortex` fixture with `ExpectedOutcome::MetadataRowCount { row_count:
  20000 }` and marks golden/reference roles as non-production execution.
- `shardloom-contract-tests/tests/correctness_fixture_manifest.rs` verifies the fixture is checked
  in, the row-count reference output does not require execution,
  null/nested/dictionary/sparse/run-length/temporal fixture families are tracked, and reference
  roles cannot become production fallback.
- This pass adds no query execution behavior, external baseline invocation, benchmark claim,
  dependency, parser, adapter runtime, object-store IO, write behavior, or fallback execution.

## CG-5.4 external baseline oracle policy

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0029
  Correctness/Benchmarks/Execution Certificates, RFC 0025 Competitive/no-fallback, and RFC 0032
  capability certification gates.
- `CorrectnessValidationPlan::default_foundation_plan` now declares Spark, DataFusion, DuckDB,
  Polars, pandas, Dask, and Velox as external correctness oracles only.
- `DifferentialBaseline::external_correctness_oracle` records comparison-only notes and remains
  fallback-disabled.
- `shardloom-contract-tests/tests/external_baseline_oracles.rs` verifies all declared baselines are
  present, reference-only, non-fallback-capable, and not runtime execution paths.
- This pass adds no external engine dependency, external baseline invocation, query execution
  behavior, benchmark claim, parser, adapter runtime, object-store IO, write behavior, or fallback
  execution.

## CG-5.5 local encoded `CountAll` correctness fixture/reference-output proof

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0029
  Correctness/Benchmarks/Execution Certificates, RFC 0012 Diagnostics/Capabilities, RFC 0025
  Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `ExpectedOutcome::EncodedCount { count }` records an execution-required reference output distinct
  from metadata-only row-count evidence.
- `CorrectnessValidationPlan::default_foundation_plan` declares
  `vortex-local-encoded-count-u64-20000` over the checked-in `metadata_footer_u64_20000.vortex`
  fixture with `ExpectedOutcome::EncodedCount { count: 20000 }`.
- `shardloom-contract-tests/tests/correctness_fixture_manifest.rs` verifies the fixture path,
  expected count, execution-required status, golden fixture role, and non-production reference role.
- `shardloom-vortex/src/encoded_read_executor.rs` feature-gated tests verify the approved local
  encoded count path and local execution bridge return the manifest count without
  decode/materialization/row/Arrow/object-store/write/spill/external/fallback effects.
- This pass adds no new fixture generation, decoded reference engine execution, external baseline
  invocation, generalized encoded-data execution, non-local adapter, object-store IO, encoded
  predicate, projection execution, row read, requested decode/materialization, Arrow conversion,
  write behavior, benchmark claim, superiority claim, or fallback execution.

## CG-5.6 correctness coverage inventory surfacing

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0012
  Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, RFC 0029
  Correctness/Benchmarks/Execution Certificates, and RFC 0032 capability certification gates.
- `CorrectnessValidationPlan` now exposes fixture ID order, semantic-area order, edge-case order,
  reference-role order, source-backed fixture count, golden fixture count, executable
  expected-output count, not-yet-defined gap count, diagnostic/unsupported expectation counts,
  required foundation edge-case coverage, and test-only/no-fallback helpers.
- `shardloom correctness-plan` now emits these fields in deterministic text/JSON output so humans
  and agents can see which CG-5 fixture families are present versus still only planned.
- Contract and CLI snapshot tests verify required null, nested, dictionary, sparse-validity,
  run-length, temporal, and unsupported-plan-shape fixture families are tracked, while reference
  roles and external baselines remain non-production and fallback-free.
- This pass adds no new query execution, decoded reference execution, external baseline invocation,
  fixture generation, parser, adapter runtime, object-store IO, write IO, benchmark/superiority
  claim, or fallback execution.

## CG-5.7 correctness/differential harness aggregate surface

- Primary RFC linkage: RFC 0015 Correctness/Semantics/Differential Testing, RFC 0012
  Diagnostics/Capabilities, RFC 0025 Competitive/no-fallback, RFC 0029
  Correctness/Benchmarks/Execution Certificates, and RFC 0032 capability certification gates.
- `CorrectnessDifferentialHarnessReport` aggregates fixture manifest coverage, golden/reference
  coverage, decoded-reference output gaps, external oracle policy, semantic edge-case coverage,
  unsupported diagnostic fixtures, property/fuzz gaps, and benchmark claim blockers.
- `correctness-harness-plan` exposes stable JSON/text fields for surface order, planned/blocked
  surface counts, required validation modes, missing validation modes, baseline engine order,
  no-execution boundaries, no-fallback boundaries, and production-claim blockers.
- The external correctness oracle inventory now includes Spark, DataFusion, DuckDB, Polars, pandas,
  Dask, and Velox as comparison-only baselines, never runtime fallback engines.
- Contract and CLI snapshot tests verify the aggregate remains side-effect-free and that
  decoded-reference execution, external engine execution, data reads, object-store IO, writes,
  production claims, and fallback execution are disabled.
- This pass adds no decoded-reference execution, external engine invocation, query execution,
  fixture generation, parser, adapter runtime, object-store IO, write IO, benchmark/superiority
  claim, production certification, or fallback execution.

## CG-16.1 local encoded `CountAll` execution certificate

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0015
  Correctness/Semantics/Differential Testing, RFC 0012 Diagnostics/Capabilities, RFC 0025
  Competitive/no-fallback, and RFC 0026 Vortex encoded-read/query-readiness boundaries.
- `ExecutionCertificateInput`, `ExecutionCertificate`, and `ExecutionCertificateStatus` define the
  first generic CG-16 execution certificate surface in `shardloom-core`.
- Certificate evaluation requires matching expected/actual correctness output, explicit
  correctness-passed evidence, no fallback attempt, no fallback availability, no unsafe effect flag,
  and no error diagnostics before certification.
- `local_encoded_count_execution_certificate` derives a certificate from the approved local encoded
  count report and local execution bridge report, preserving input/output refs, fixture id,
  data-read flags, unsafe-effect flags, and no-fallback fields.
- `shardloom-contract-tests/tests/execution_certificate_contracts.rs` verifies certified,
  fallback-blocked, unsafe-effect-blocked, and diagnostic-blocked certificate outcomes.
- The feature-gated local encoded count test verifies the actual approved local `.vortex` path emits
  a certified, fallback-free certificate for the CG-5.5 fixture.
- This pass adds no generalized execution certificate system, native I/O certificate implementation,
  benchmark certificate, external baseline invocation, generalized encoded-data execution, non-local
  adapter, object-store IO, encoded predicate, projection execution, row read, requested
  decode/materialization, Arrow conversion, write behavior, benchmark claim, superiority claim, or
  fallback execution.

## CG-16.2 execution certificate evidence surface

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0015
  Correctness/Semantics/Differential Testing, RFC 0012 Diagnostics/Capabilities, RFC 0025
  Competitive/no-fallback, RFC 0031 Native I/O Certificates, and RFC 0032 capability certification
  requirements.
- `ExecutionCertificateEvidenceSurfaceReport` defines the report-only artifact requirements for
  broader CG-16 certificates.
- The report requires plan hash, input snapshot hash, output hash, selected/skipped segment traces,
  side-effect manifest, reproducibility metadata, correctness fixture linkage, deterministic field
  order, and machine-readable artifacts.
- `execution-certificate-plan` exposes stable JSON/text fields for artifact counts, per-kind counts,
  hash requirements, reproducibility requirements, no-evaluation status, no-runtime status, and
  fallback-disabled status.
- This phase adds no generalized execution certificate evaluation, benchmark certificate execution,
  external baseline invocation, generalized encoded-data execution, adapter runtime, object-store
  IO, row reads, decode/materialization, Arrow conversion, writes, spill IO, performance/superiority
  claim, production certification, CG closeout, or fallback behavior.

## CG-17.1 stateful reuse boundary report

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates/Stateful Reuse, RFC
  0015 Correctness/Semantics/Differential Testing, RFC 0012 Diagnostics/Capabilities, RFC 0025
  Competitive/no-fallback, RFC 0031 Native I/O Certificates, and RFC 0032 capability certification
  requirements.
- `StatefulReuseReport` defines typed cache/reuse boundaries for segment results, predicate results,
  encoded dictionaries, encoded filters, layout decisions, execution certificates, and incremental
  manifest diffs.
- The report requires deterministic keys scoped to dataset snapshot, plan hash, semantic profile,
  encoding/layout, and adapter fidelity before any reuse can become eligible.
- Invalidation proof requirements cover snapshot, segment, schema, partition, predicate, semantic
  profile, function version, adapter fidelity, and unknown-change signals with conservative
  rejection for unproven changes.
- `stateful-reuse-plan` exposes stable JSON/text fields for boundary counts, invalidation signal
  counts, correctness proof counts, invalidation proof counts, execution certificate counts,
  manifest-diff requirements, no-cache side-effect fields, no-runtime fields, and fallback-disabled
  status.
- This phase adds no cache storage, cache lookup, cache write, cache replay, incremental recompute
  execution, manifest-diff reads, generalized execution certificate evaluation, external baseline
  invocation, generalized encoded-data execution, adapter runtime, object-store IO, row reads,
  decode/materialization, Arrow conversion, writes, spill IO, performance/superiority claim,
  production certification, CG closeout, or fallback behavior.

## CG-17 stateful reuse promotion gate

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates/Stateful Reuse, RFC
  0004 Native Dataset Manifest/Snapshot/Incremental, RFC 0015 Correctness/Semantics/Differential
  Testing, RFC 0016 Optimizer/Adaptive Execution, RFC 0025 Competitive/no-fallback, RFC 0031
  Native I/O Certificates, and RFC 0032 capability certification requirements.
- `StatefulReusePromotionGateReport` records existing `stateful-reuse-plan` and `incremental-plan
  cdc` contract evidence while blocking stable reuse keys, key digest/scope, manifest-diff input
  evidence, invalidation decision matrix, cache-safety policy, state-certificate schema,
  execution-certificate linkage, Native I/O linkage, reuse benchmark constitution, incremental
  recompute execution, and production reuse claim closeout.
- `cg17-stateful-reuse-gate` exposes stable JSON/text fields for surface counts, existing evidence
  counts, blocked surface counts, surface order, existing report refs, required evidence flags,
  cache/runtime side-effect flags, claim flags, external-engine flags, and fallback-disabled status.
- This phase adds no stable reuse-key derivation runtime, cache storage, cache lookup, cache write,
  cache replay, manifest-diff reads, state certificate issuance, incremental recompute execution,
  external baseline invocation, adapter runtime, object-store IO, row reads, decode/materialization,
  Arrow conversion, writes, spill IO, performance/superiority claim, production certification, or
  fallback behavior.

## CG-18.1 universal harness report

- Primary RFC linkage: RFC 0030 Universal API/Plan Portability/Import/Deployment/Baselines, RFC 0010
  Developer Experience, RFC 0012 Diagnostics/Capabilities, RFC 0024 Release/API Compatibility, RFC
  0029 benchmark evidence, RFC 0025 Competitive/no-fallback, and RFC 0032 capability certification
  requirements.
- `UniversalHarnessReport` defines the report-only surface for the CLI JSON runner contract,
  package/import guidance, deployment profile guidance, optional Foundry examples, external baseline
  harnesses, comparison report datasets, and portability checks.
- The report requires stable runner contract fields for command, schema version, exit code, status,
  diagnostics, fallback status, side effects, output artifacts, and metrics.
- External baseline requirements cover Spark, DataFusion, and Polars as external-only comparison
  harnesses with engine version, workload id, fixture id, command/transform, correctness result,
  benchmark metric, and comparison report requirements.
- `universal-harness-plan` exposes stable JSON/text fields for harness surface counts, external
  baseline counts, runner field order, surface order, baseline order, requirement fields, no-probe
  fields, no-execution fields, no-publish fields, and fallback-disabled status.
- This phase adds no package import, deployment execution, Foundry invocation, external baseline
  runner execution, comparison dataset materialization, parser execution, plan import/export
  serialization, filesystem/network/catalog/adapter probing, read/write IO, performance/superiority
  claim, production certification, CG closeout, or fallback behavior.

## CG-19.1 native I/O envelope report

- Primary RFC linkage: RFC 0031 Universal Native I/O Envelope, RFC 0013 Streaming/Zero-Copy/Boundary
  Interoperability, RFC 0012 Diagnostics/Capabilities, RFC 0008 Object-Store Runtime, RFC 0016
  Optimizer/Adaptive Execution, RFC 0018 Observability, RFC 0025 Competitive/no-fallback, and RFC
  0032 capability certification requirements.
- `NativeIoEnvelopeReport` defines the report-only surface for native work envelope, native work
  stream, native result stream, source capability, source pushdown, sink requirement, adapter
  fidelity, materialization boundary, and native I/O certificate contracts.
- Representation state contracts distinguish metadata-only, pruned, Vortex-encoded, foreign-encoded,
  selection-vector-encoded, partially decoded, decoded-columnar, materialized-row, external-effect,
  and unsupported states.
- Transition examples require materialization boundary evidence for decode/materialization
  transitions and use `any->unsupported` for capability proof failures.
- Per-source/sink-path certificate requirements cover source capability, source pushdown,
  representation transitions, sink requirements, adapter fidelity, materialization boundaries, side
  effects, diagnostics, and `fallback_attempted=false`.
- `native-io-envelope-plan` exposes stable JSON/text fields for contract counts, representation
  state counts, transition example counts, certificate path counts, order fields, requirement
  fields, side-effect fields, and fallback-disabled status.
- This phase adds no source/sink runtime certificate emission, adapter runtime, parser execution,
  filesystem/network/catalog/adapter probing, data reads, decode/materialization, row reads, Arrow
  conversion, object-store I/O, writes, spill I/O, performance/superiority claim, production
  certification, CG closeout, or fallback behavior.

## CG-6.1 benchmark evidence manifest

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0015
  Correctness/Semantics/Differential Testing, RFC 0025 Competitive/no-fallback, and RFC 0032
  capability certification gates.
- `shardloom-core/src/benchmark.rs` expands benchmark metric vocabulary for startup/runtime/write
  latency, peak memory, bytes read/written/decoded/avoided, materialization avoided, segments
  considered/pruned/metadata-answered, object-store requests, spill required/avoided, and work
  avoided.
- `BenchmarkPlan::default_foundation_plan` now covers CG-6 metric categories in report-only
  scenarios and keeps baselines comparison-only with fallback disabled.
- `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verifies required metric coverage
  and correctness validation mode presence before any claim can rely on the benchmark plan.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior,
  superiority claim, dependency, parser, adapter runtime, object-store IO, write behavior, or
  fallback execution.

## CG-6.2 benchmark claim gate

- Primary RFC linkage: RFC 0029 Correctness/Benchmarks/Execution Certificates, RFC 0015
  Correctness/Semantics/Differential Testing, RFC 0025 Competitive/no-fallback, and RFC 0032 claim
  publication requirements.
- `BenchmarkClaimGate` blocks performance, superiority, cost, replacement, or best-default
  publication unless correctness evidence, benchmark evidence, required metrics, comparison reports,
  and no-fallback evidence are all present.
- `BenchmarkPlan::claim_gate` returns `evidence_missing` for the recorded report-only foundation
  plan because no benchmark runner or comparison report exists yet.
- `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verifies every publication input
  is required and fallback attempts block claims.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior,
  superiority claim, dependency, parser, adapter runtime, object-store IO, write behavior, or
  fallback execution.

## CG-6.3 benchmark comparison report contract

- Primary RFC linkage: RFC 0029 benchmark evidence requirements, RFC 0015
  correctness-before-performance requirements, RFC 0025 competitive/no-fallback guardrails, and RFC
  0032 claim publication and comparison-report requirements.
- `BenchmarkComparisonReport` records expected scenario/baseline result coverage, required metric
  gaps, comparison-report emission, correctness evidence state, benchmark evidence state, and
  no-fallback state without executing benchmarks.
- `BenchmarkComparisonReport::claim_gate` treats a report as emitted while keeping
  performance/superiority publication blocked until correctness evidence, complete benchmark
  results, required metrics, and no-fallback evidence are all present.
- `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verifies missing results and
  unknown required metrics block claim readiness, and complete synthetic evidence can only reach
  claim-review readiness through explicit report fields.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior,
  superiority claim, dependency, parser, adapter runtime, object-store IO, write behavior, or
  fallback execution.

## CG-6.4 benchmark reproducibility manifest

- Primary RFC linkage: RFC 0029 reproducible benchmark evidence requirements, RFC 0015
  correctness-before-performance requirements, RFC 0025 competitive/no-fallback guardrails, and RFC
  0032 benchmark evidence floor for best-default claims.
- `BenchmarkRunManifest` records dataset shape, schema, storage format, compression, engine
  versions, hardware profile, operating-system profile, runtime configuration, cache state, required
  metrics, reproduction steps, correctness evidence state, and no-fallback state.
- The recorded foundation plan remains `incomplete` because no approved benchmark runner has
  produced complete reproducibility metadata or benchmark results.
- `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verifies incomplete default
  manifests, complete synthetic reproducibility metadata, and comparison-only engine-version labels.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior,
  superiority claim, dependency, parser, adapter runtime, object-store IO, write behavior, or
  fallback execution.

## CG-6.5 reproducibility-aware benchmark claim gate

- Primary RFC linkage: RFC 0029 reproducible benchmark evidence requirements, RFC 0015
  correctness-before-performance requirements, RFC 0025 competitive/no-fallback guardrails, and RFC
  0032 benchmark-gated claim publication requirements.
- `BenchmarkClaimGate` now requires reproducibility evidence in addition to correctness evidence,
  benchmark evidence, required metrics, comparison-report evidence, and no-fallback evidence.
- `BenchmarkEvidenceBundle` combines a `BenchmarkRunManifest` and `BenchmarkComparisonReport` into
  the final claim gate so complete metric rows cannot publish claims without reproducible run
  metadata.
- `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verifies comparison-ready reports
  remain blocked without reproducibility and that complete synthetic comparison/reproducibility
  evidence is required before the publication gate can open.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior,
  superiority claim, dependency, parser, adapter runtime, object-store IO, write behavior, or
  fallback execution.

## CG-6.6 benchmark coverage inventory surfacing

- Primary RFC linkage: RFC 0029 benchmark evidence requirements, RFC 0015
  correctness-before-performance requirements, RFC 0025 competitive/no-fallback guardrails, RFC 0012
  diagnostics/capability reporting, and RFC 0032 benchmark-gated claim publication requirements.
- `BenchmarkPlan` now exposes deterministic scenario, workload class, correctness-validation,
  required-metric, foundation-metric, baseline-engine, external-baseline, expected-result,
  metric-family, benchmark-execution, and no-fallback inventory helpers.
- `benchmark-plan` now emits stable JSON/text fields for scenario names, workload classes,
  correctness-validation modes, metric-family coverage, baseline engine order, expected result
  slots, claim-gate evidence state, and comparison-only/no-fallback boundaries.
- `shardloom-cli/tests/benchmark_plan_snapshots.rs` and
  `shardloom-contract-tests/tests/benchmark_evidence_manifest.rs` verify the inventory fields
  without executing benchmarks or invoking external baselines.
- This pass adds no benchmark runner, external baseline invocation, query execution behavior,
  performance/superiority/best-default claim, dependency, parser, adapter runtime, object-store IO,
  write behavior, or fallback execution.

## CG-6.7 traditional analytics benchmark plan

- Primary RFC linkage: RFC 0029 benchmark evidence requirements, RFC 0015
  correctness-before-performance requirements, RFC 0025 competitive/no-fallback guardrails, RFC 0012
  diagnostics/capability reporting, and RFC 0032 world-class user capability requirements.
- `BenchmarkPlan::traditional_analytics_plan` records conventional dataframe/SQL benchmark scenarios
  for CSV/file ingest, selective filters, group-by aggregation, sort/top-k, and hash joins.
- Traditional analytics baselines include `ShardLoom`, pandas, Polars, DuckDB, Spark/PySpark,
  DataFusion, and Dask as comparison targets only.
- `benchmark-plan traditional-analytics` exposes the scenario/baseline/metric inventory without
  running benchmarks or adding external dependencies.
- This phase adds no external baseline invocation, pandas/Polars/DuckDB/Spark/DataFusion/Dask
  dependency, SQL parser, dataframe API, adapter runtime, benchmark claim, superiority claim,
  production certification, or fallback execution.

## CG-6.8 local encoded count benchmark runner

- Primary RFC linkage: RFC 0029 benchmark evidence requirements, RFC 0015
  correctness-before-performance requirements, RFC 0025 competitive/no-fallback guardrails, RFC 0026
  Vortex encoded-read/query-readiness boundaries, and RFC 0032 benchmark-gated claim publication
  requirements.
- `shardloom vortex-count-benchmark <dataset_uri> <memory_gb> <max_parallelism> [--iterations <n>]`
  runs the approved local encoded `CountAll` path repeatedly and emits ShardLoom timing/count/effect
  metrics.
- The runner records Spark, DataFusion, Polars, pandas, DuckDB, and Dask as required external
  comparison results, but does not invoke them; comparison and claim gates remain blocked until
  external baseline results and reproducibility evidence exist.
- The local runner preserves the existing local encoded count guardrails: no decode/materialization,
  no row reads, no Arrow conversion, no object-store IO, no writes, no spill IO, no external engine
  invocation, and no fallback.
- This phase adds no broad benchmark suite execution, external baseline runner, generalized encoded
  filter/project execution, SQL parser, dataframe API, adapter runtime,
  performance/superiority/best-default claim, production certification, or fallback execution.

## CG-6.9 traditional analytics external benchmark harness

- Primary RFC linkage: RFC 0029 benchmark evidence requirements, RFC 0015
  correctness-before-performance requirements, RFC 0025 competitive/no-fallback guardrails, RFC 0009
  benchmark methodology, RFC 0012 diagnostics/capability reporting, and RFC 0032 world-class user
  capability requirements.
- `benchmarks/traditional_analytics/run.py` creates deterministic local CSV fact/dimension data and
  runs `csv/file ingest`, `selective filter`, `group by aggregation`, `sort and top-k`, `hash join`,
  `wide projection`, `distinct count`, and optional scale-stress skewed-join and multi-stage ETL
  scenarios independently per engine and scenario, including separate `spark-default` and
  `spark-local-tuned` rows.
- The harness emits machine-readable JSON and human-readable Markdown reports with fairness
  parameters, engine availability/version, scenario timing matrix, fastest-row table, ASCII timing
  bars, correctness digests, unsupported/failure rows, raw metrics, environment metadata, and
  limitations.
- ShardLoom is included as a first-class engine row; failures are captured per scenario and do not
  block pandas, Polars, DuckDB, Spark/PySpark, DataFusion, or Dask baselines from running.
- ShardLoom native encoded microbenchmarks and universal-I/O/compatibility-to-Vortex evidence lanes
  are included so current Vortex-native capability and remaining encoded-operator gaps are visible
  in the same report.
- External engines remain benchmark-only tooling. They are not Cargo dependencies, runtime
  dependencies, ShardLoom execution delegates, or fallback engines.
- This phase adds no ShardLoom SQL parser, dataframe API, adapter runtime, production dependency,
  broad claim publication, superiority claim, best-default claim, or fallback execution.

## CG-6.10 ShardLoom traditional analytics universal-I/O smoke row

- Primary RFC linkage: RFC 0031 native work envelopes/native I/O certificates, RFC 0029 benchmark
  evidence requirements, RFC 0015 correctness-before-performance requirements, RFC 0025
  competitive/no-fallback guardrails, and RFC 0032 user-capability benchmark-gated claim
  requirements.
- `shardloom traditional-analytics-run <scenario> <fact_input> <dim_input> --workspace <dir>
  [--input-format auto|csv|jsonl|parquet|arrow-ipc|avro|orc]` is feature-gated behind
  `vortex-traditional-analytics-benchmark` and is used by the traditional analytics harness for
  ShardLoom rows.
- The feature-gated path parses deterministic benchmark fixtures as benchmark-only local
  compatibility source adapters, writes local Vortex files with upstream Vortex writer APIs isolated
  in `shardloom-vortex`, reopens those files, scans through upstream Vortex, and runs temporary
  benchmark operators over Vortex-derived arrays.
- ShardLoom rows must emit native work envelope, native work stream, native result stream, native
  I/O certificate, compatibility source adapter, compatibility-to-Vortex import, Vortex
  write/read/scan, auto sizing/partitioning, and materialization-boundary evidence fields before the
  harness accepts them.
- The current traditional analytics path explicitly reports decode/materialization for temporary
  operators. It is universal-I/O smoke evidence, not mature encoded-native
  SQL/operator/function/adapter coverage.
- This phase adds no SQL parser, DataFrame API, production CSV adapter, object-store IO, Arrow
  conversion, row-read path, external engine dependency, performance/superiority/best-default claim,
  or fallback execution.

## CG-6.19 benchmark/competitive claim evidence aggregate surface

- Primary RFC linkage: RFC 0009 benchmark methodology, RFC 0012 diagnostics/capability reporting,
  RFC 0015 correctness-before-performance requirements, RFC 0025 competitive/no-fallback guardrails,
  RFC 0029 benchmark/reproducibility/claim evidence requirements, and RFC 0032 benchmark-gated
  capability claim requirements.
- `BenchmarkClaimEvidenceReport` aggregates benchmark-plan coverage, required metrics, missing
  result rows, missing external comparison rows, reproducibility metadata gaps, claim-gate state,
  baseline no-fallback policy, and publication flags into one deterministic report.
- `benchmark-claim-evidence-plan [foundation|traditional-analytics]` exposes stable JSON/text fields
  for surface order, planned/blocked surfaces, scenario order, metric order, baseline order,
  expected/missing result counts, manifest/comparison statuses, claim-gate evidence states, and
  no-execution/no-fallback fields.
- Performance, superiority, cost, replacement, and best-default claims remain blocked until
  correctness evidence, benchmark result rows, external comparison rows, reproducible run metadata,
  and no-fallback evidence are all present.
- This phase adds no benchmark runner, external baseline invocation, query execution, parser,
  dataframe API, adapter runtime, object-store IO, data reads, writes, dependency,
  performance/superiority/best-default claim, production certification, or fallback execution.

## CG-6.20 traditional analytics storage expansion

- Primary RFC linkage: RFC 0029 benchmark evidence requirements, RFC 0031 native I/O certificates,
  RFC 0032 world-class user capability/adapters requirements, RFC 0015
  correctness-before-performance requirements, and RFC 0025 competitive/no-fallback guardrails.
- `benchmarks/traditional_analytics/run.py` now writes Parquet copies of deterministic benchmark
  fixtures with `pyarrow` and runs CSV/Parquet rows for engines that support those formats.
- The harness adds a separate `shardloom-vortex` engine lane that prepares local native `.vortex`
  inputs before scenario timing and then invokes `shardloom traditional-analytics-vortex-run` for
  the same scenario labels.
- Unsupported format rows are recorded explicitly, so ShardLoom Parquet gaps and non-Vortex engine
  native-Vortex gaps are visible without aborting JSON/Markdown artifact generation.
- ShardLoom native Vortex rows emit a certified native-Vortex source certificate while still
  reporting temporary-operator materialization boundaries.
- This phase adds no production Parquet adapter, SQL parser, DataFrame API, external engine
  dependency, object-store IO, mature encoded operator certification,
  performance/superiority/best-default claim, or fallback execution.

## CG-12.4 native plan import/export serialization

- Primary RFC linkage: RFC 0030 Universal API/Plan Portability/Import/Deployment/Baselines, RFC 0022
  Plan IR interoperability, RFC 0010 Developer Experience, RFC 0012 Diagnostics, and RFC 0024
  release/API compatibility.
- `NativePlanDocument` now serializes to and imports from a deterministic `shardloom.native_plan.v1`
  payload with plan ID, schema version, layer, node kind, capability, and boundary fields.
- `plan-export native` emits a serialized native plan payload and reports
  `portability_status=serialized` while keeping runtime execution, external engine execution, file
  IO, probes, writes, and fallback disabled.
- `plan-import native <payload>` validates the native payload and reports
  `portability_status=imported`, imported plan ID, and imported node count without executing the
  imported plan.
- Non-native formats remain unsupported/validation-only until explicit parsers/exporters are
  approved.
- The CG-12.4 pass adds no imported-plan execution gate, SQL parser, external format parser,
  filesystem/network/catalog/adapter probing, object-store IO, write IO, dependency, external engine
  execution, or fallback execution.

## CG-12.5 imported-plan capability execution gate

- Primary RFC linkage: RFC 0030 Universal API/Plan Portability/Import/Deployment/Baselines, RFC 0022
  Plan IR interoperability, RFC 0010 Developer Experience, RFC 0012 Diagnostics, RFC 0024
  release/API compatibility, RFC 0031 native I/O certificate coverage, and RFC 0032 capability
  surface certification.
- `ImportedPlanCapabilityGateReport` maps each imported native plan node and boundary to required
  certification surfaces before any imported plan can be treated as execution-eligible.
- `plan-import native <payload>` now reports the gate schema version, status,
  required/certified/missing certification surfaces, unsupported/effect counts, execution-allowed
  flag, and no-parser/no-probe/no-runtime/no-external-engine/no-read/no-write/no-fallback fields.
- Contract-only capability certification keeps current imported native plans blocked as
  `blocked_missing_capability_evidence`; effectful imported plans block as `blocked_effect_boundary`
  before capability evidence can authorize them.
- Real imported-plan execution remains staged until the gate can consume certified
  SQL/operator/function/adapter/native-I/O/execution-certificate evidence for a declared workload.
- This pass adds no imported-plan execution, SQL parser, external format parser,
  filesystem/network/catalog/adapter probing, data reads, write IO, dependency, external engine
  execution, or fallback execution.

## CG-7.1 physical operator/kernel contract foundation

- Primary RFC linkage: RFC 0021 Expression Engine and Kernel Registry, RFC 0027 CPU Vectorized
  Kernels/Runtime Adaptivity, RFC 0014 Memory/Spill/OOM Safety, RFC 0025 competitive/no-fallback
  guardrails, and RFC 0032 operator certification requirements.
- `PhysicalOperatorPlan::cg7_foundation` declares report-only filter, project, and count-aggregate
  operator contracts with required metadata/encoded kernel blockers.
- `PhysicalKernelRequirement` rejects decoded-reference kernels as production native evidence, and
  `PhysicalOperatorContract` keeps native planning blocked while required kernels are missing.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` verifies missing-kernel
  blockers, reference-only rejection, synthetic native-readiness without execution, and no-fallback
  invariants.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal,
  scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO,
  write behavior, external engine invocation, or fallback execution.

## CG-7.2 physical operator capability discovery

- Primary RFC linkage: RFC 0012 diagnostics/capabilities, RFC 0021 kernel registry requirements, RFC
  0025 no-fallback guardrails, and RFC 0032 operator coverage/certification discovery requirements.
- `PhysicalOperatorPlan` now carries a stable schema version and readiness-count helpers for ready,
  missing-kernel, and unsupported operator contract states.
- `shardloom capabilities operators` includes the physical operator plan schema/version, plan id,
  operator count, readiness count, missing-kernel count, unsupported count, fallback-disabled flag,
  and runtime-execution=false flag.
- `shardloom-cli/tests/capability_discovery_snapshots.rs` locks the operator capability JSON field
  order and verifies the CG-7 physical operator blockers remain agent-readable.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal,
  scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO,
  write behavior, external engine invocation, or fallback execution.

## CG-7.3 physical kernel registry plan

- Primary RFC linkage: RFC 0021 kernel registry requirements, RFC 0027 native kernel specialization
  roadmap, RFC 0012 diagnostics/capabilities, RFC 0025 no-fallback guardrails, and RFC 0032 operator
  certification requirements.
- `PhysicalKernelRegistryPlan` derives required native kernel slots from the CG-7 foundation
  physical operator plan and records present, missing, and reference-only-rejected slot counts.
- `shardloom kernel-registry` exposes the physical kernel registry schema/version, registry id,
  required slot count, present count, missing count, reference-only rejection count,
  runtime-execution=false, and fallback-disabled fields.
- `shardloom-cli/tests/kernel_registry_snapshots.rs` verifies the kernel-registry JSON fields remain
  stable and agent-readable.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal,
  scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO,
  write behavior, external engine invocation, or fallback execution.

## CG-7.4 physical kernel admission gate

- Primary RFC linkage: RFC 0021 kernel registry requirements, RFC 0015 correctness-first
  requirements, RFC 0014 memory/OOM safety, RFC 0025 no-fallback guardrails, RFC 0029 benchmark
  evidence rules, and RFC 0032 operator certification requirements.
- `PhysicalKernelAdmissionReport` records required/candidate kernel kind, correctness evidence,
  benchmark evidence, memory-safety evidence, fallback state, and admission status for a physical
  kernel slot.
- Reference-only kernels, unsupported kernels, kind mismatches, fallback attempts, missing
  correctness evidence, and missing memory-safety evidence cannot mark a slot present.
- Registry admission can proceed before production claims when benchmark evidence is missing, but
  production readiness requires benchmark evidence in addition to correctness, memory, and
  no-fallback proof.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` verifies blocked
  reference/fallback/missing-evidence states and the registry-ready versus production-ready
  distinction.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal,
  scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO,
  write behavior, external engine invocation, or fallback execution.

## CG-7.5 physical operator execution profiles

- Primary RFC linkage: RFC 0021 expression/kernel execution modes, RFC 0027 native kernel
  specialization roadmap, RFC 0014 memory/materialization safety, RFC 0025 no-fallback guardrails,
  and RFC 0032 operator certification requirements.
- `PhysicalOperatorExecutionProfileMatrix::cg7_foundation` declares metadata-only, encoded-native,
  hybrid-native, and native-decoded execution levels for filter, project, and count-aggregate
  operator profiles.
- Foundation profiles reject test-reference-only and unsupported execution levels and keep row
  materialization, Arrow conversion, and fallback execution disabled.
- `shardloom capabilities operators` includes execution-profile schema/version and counts for
  profile, reference-only, row-materialization, Arrow-conversion, and fallback paths.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` and
  `shardloom-cli/tests/capability_discovery_snapshots.rs` verify the profile contracts and
  capability discovery fields.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal,
  scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO,
  write behavior, external engine invocation, or fallback execution.

## CG-7.6 physical kernel selection gate

- Primary RFC linkage: RFC 0021 kernel selection requirements, RFC 0027 native kernel specialization
  roadmap, RFC 0012 deterministic diagnostics, RFC 0025 no-fallback guardrails, and RFC 0032
  operator certification requirements.
- `PhysicalKernelSelectionReport` validates operator profile availability, requested execution
  level, and required native kernel slot presence before a physical kernel can be selected.
- Selection rejects missing operator profiles, disallowed execution levels, and missing required
  slots while preserving runtime-execution=false and fallback-disabled flags.
- A synthetic present-kernel registry can reach `ready_for_admission_review` for planning evidence,
  but selection still does not execute kernels or read data.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` verifies missing-slot,
  rejected-level, missing-profile, and synthetic ready-selection states.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal,
  scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO,
  write behavior, external engine invocation, or fallback execution.

## CG-7.7 physical operator planning certificate

- Primary RFC linkage: RFC 0021 kernel selection and registry requirements, RFC 0027 physical
  operator/kernel roadmap, RFC 0012 deterministic diagnostics, RFC 0025 no-fallback guardrails, RFC
  0029 evidence gating, and RFC 0032 operator certification requirements.
- `PhysicalOperatorPlanningCertificate` summarizes physical operator readiness, registry slot state,
  selection gate state, admission gate state, fallback-attempt evidence, and production-claim
  readiness in one report-only certificate.
- Certificates distinguish operator-plan blockers, registry blockers, selection blockers, admission
  blockers, native-planning readiness, and production certification readiness.
- Production certification remains separate from native planning readiness and requires benchmark
  evidence; runtime execution remains disabled even when certificate evidence is ready.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` verifies foundation
  blockers, synthetic native-planning readiness, production-certification separation, and
  fallback-attempt blocking.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal,
  scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO,
  write behavior, external engine invocation, or fallback execution.

## CG-7.8 Vortex query primitive physical-operator bridge

- Primary RFC linkage: RFC 0021 physical/kernel selection requirements, RFC 0027 operator/kernel
  roadmap, RFC 0012 deterministic diagnostics, RFC 0025 no-fallback guardrails, and RFC 0032
  operator coverage/certification requirements.
- `VortexPhysicalOperatorBridgeReport` lowers Vortex `CountAll`, `CountWhere`, `ProjectColumns`,
  `FilterPredicate`, and `FilterAndProject` requests into CG-7 physical operator plans.
- The bridge attaches a `PhysicalOperatorPlanningCertificate` so CG-2 query primitives expose CG-7
  operator/kernel blockers before any kernel implementation is accepted.
- Unsupported Vortex query primitives lower to an unsupported physical operator instead of fallback
  execution.
- `shardloom-vortex/src/physical_operator_bridge.rs` verifies count/filter/project mappings,
  physical operator order, side-effect-free behavior, and no-fallback diagnostics.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal,
  scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO,
  write behavior, external engine invocation, or fallback execution.

## CG-7.9 execution-level kernel requirements

- Primary RFC linkage: RFC 0021 deterministic kernel selection requirements, RFC 0027
  metadata/encoded/hybrid/native-decoded operator levels, RFC 0012 deterministic diagnostics, RFC
  0025 no-fallback guardrails, and RFC 0032 operator certification requirements.
- `PhysicalOperatorExecutionProfile::required_kernel_kinds_for_level` makes kernel selection
  requirements depend on the requested execution level.
- Metadata-only selection now requires only metadata kernels, encoded-native selection requires
  metadata and encoded kernels, hybrid-native selection also requires partial-decode capability, and
  native-decoded selection requires metadata plus partial-decode capability.
- `PhysicalKernelSelectionReport` stores the level-specific required kernel kinds and emits
  missing-slot blockers for absent level-specific slots.
- `shardloom-contract-tests/tests/physical_operator_kernel_contracts.rs` verifies metadata-only
  readiness without encoded blockers and hybrid partial-decode missing-slot diagnostics.
- This pass adds no kernel implementation, query execution behavior, encoded-data traversal,
  scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO,
  write behavior, external engine invocation, or fallback execution.

## CG-7.10 metadata-result physical operator bridge

- Primary RFC linkage: RFC 0021 metadata kernel selection requirements, RFC 0026 metadata/query
  primitive bridge, RFC 0027 physical operator/kernel roadmap, RFC 0012 deterministic diagnostics,
  RFC 0025 no-fallback guardrails, and RFC 0032 operator certification requirements.
- `plan_vortex_query_primitive_result_physical_operators` maps already metadata-answered Vortex
  query primitive results to metadata-only physical operator plans.
- Metadata-answered `CountAll`, `CountWhere`, and `FilterPredicate` results can mark metadata kernel
  requirements present for count/filter physical operators without executing kernels.
- Physical planning certificate admission remains blocked until separate correctness, memory-safety,
  benchmark, and no-fallback evidence is supplied.
- Non-metadata results keep the original missing-kernel blockers, and unsupported primitives remain
  unsupported instead of fallback execution.
- `shardloom-vortex/src/physical_operator_bridge.rs` verifies metadata count/filter readiness,
  non-metadata blocker preservation, side-effect-free behavior, and no-fallback flags.
- This pass adds no new query execution behavior, kernel implementation, encoded-data traversal,
  scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO,
  write behavior, external engine invocation, or fallback execution.

## CG-7.11 metadata bridge admission evidence

- Primary RFC linkage: RFC 0021 kernel admission and selection requirements, RFC 0026 metadata/query
  primitive bridge, RFC 0027 physical operator/kernel roadmap, RFC 0012 deterministic diagnostics,
  RFC 0025 no-fallback guardrails, RFC 0029 benchmark evidence gating, and RFC 0032 operator
  certification requirements.
- `plan_vortex_query_primitive_result_physical_operators_with_evidence` lets already
  metadata-answered Vortex query primitive results supply explicit correctness, benchmark,
  memory-safety, and no-fallback admission evidence to the attached physical planning certificate.
- Metadata-result bridge defaults remain conservative: the existing
  `plan_vortex_query_primitive_result_physical_operators` path still emits missing evidence and
  blocked admission until evidence is supplied.
- Correctness plus memory-safety evidence can advance metadata-only count/filter bridges to
  `ready_for_native_planning`; benchmark evidence is still required before `production_certified`
  can appear.
- Any attempted fallback evidence blocks admission and keeps runtime execution and fallback
  execution disabled.
- This pass adds no new query execution behavior, kernel implementation, encoded-data traversal,
  scan/read-start API calls, row reads, decode/materialization, Arrow conversion, object-store IO,
  write behavior, external baseline invocation, or fallback execution.

## CG-7.12 metadata-only physical kernel report

- Primary RFC linkage: RFC 0021 metadata kernel requirements, RFC 0026 metadata/query primitive
  bridge, RFC 0027 physical operator/kernel roadmap, RFC 0012 deterministic diagnostics, RFC 0025
  no-fallback guardrails, RFC 0029 evidence gating, and RFC 0032 operator certification
  requirements.
- `evaluate_vortex_metadata_physical_kernels` consumes an already metadata-answered Vortex primitive
  result plus a matching physical-operator bridge report.
- Metadata-only count/filter kernel reports require the bridge certificate to be ready for native
  planning; the default missing-evidence bridge remains blocked.
- Metadata `CountAll`/`CountWhere` reports surface count-aggregate and filter/count-aggregate
  operator coverage; metadata `FilterPredicate` reports surface filter operator coverage.
- Blocked reports remain deterministic, side-effect-free, and fallback-disabled.
- This pass adds no encoded-data traversal, scan/read-start API calls, row reads,
  decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO, external
  baseline invocation, or fallback execution.

## CG-7.13 metadata physical kernel CLI surfacing

- Primary RFC linkage: RFC 0012 deterministic capability/diagnostic discovery, RFC 0021 kernel
  selection and metadata-kernel requirements, RFC 0025 no-fallback guardrails, RFC 0029 evidence
  gating, and RFC 0032 operator certification discovery requirements.
- `shardloom vortex-metadata-physical-kernel-plan <primitive> <dataset_uri> <metadata_value>`
  exposes metadata-only physical kernel reports for count, filtered-count, and filter metadata
  values.
- The command requires explicit `--correctness-evidence` and `--memory-safe` evidence before
  returning success; `--benchmark-evidence` upgrades the attached certificate to
  production-certified, and `--fallback-attempted` blocks admission.
- JSON/text output includes certificate status, metadata kernel count, evidence flags,
  data-read/decode/materialization/IO fields, side-effect-free status, and fallback-disabled status.
- This pass adds no encoded-data traversal, scan/read-start API calls, row reads,
  decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO, external
  baseline invocation, or fallback execution.

## CG-7.14 metadata kernel capability discovery

- Primary RFC linkage: RFC 0012 capability discovery, RFC 0021 kernel registry and metadata-kernel
  contracts, RFC 0025 no-fallback guardrails, RFC 0029 correctness/benchmark evidence, and RFC 0032
  operator certification discovery.
- `shardloom capabilities operators` now reports the metadata physical kernel report schema,
  supported metadata primitives, contextual-only status, correctness/memory/benchmark evidence
  requirements, and no runtime/fallback/IO effects.
- `shardloom kernel-registry` reports the same metadata physical kernel discovery fields while
  preserving the global registry counts as missing until actual native kernel slots are implemented
  and admitted.
- The discovery surface makes already metadata-answered count/filter capability visible to humans
  and agents without claiming encoded-native, hybrid, production-certified, or global runtime kernel
  readiness.
- This pass adds no encoded-data traversal, scan/read-start API calls, row reads,
  decode/materialization, Arrow conversion, object-store IO, write behavior, spill IO, external
  baseline invocation, or fallback execution.


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

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex query-readiness
  boundaries.
- `execute_vortex_count_where_from_filtered_count_metadata_proof` accepts only
  `MetadataPredicateProof` readiness for matching `CountWhere` requests with metadata summaries.
- Metadata-proven predicates can return metadata-only count results from segment metadata through
  the local execution report, preserving no encoded-data read, no row read, no
  decode/materialization, and no fallback.
- Encoded-predicate candidates are rejected by this guard and remain future work.
- This pass adds no encoded predicate evaluation, scan/read-start invocation, encoded-data
  traversal, row read, decode/materialization, Arrow conversion, object-store IO, write behavior,
  spill IO, external baseline invocation, or fallback execution.

## CG-2.2d filtered-count metadata proof report

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0025 Competitive/no-fallback, and RFC 0026 Vortex query-readiness
  boundaries.
- `VortexFilteredCountMetadataProofReport` classifies `CountWhere` plus a supplied metadata summary
  as `proof_ready`, `needs_encoded_predicate`, `missing_metadata`, or `unsupported`.
- Proof-ready reports carry the metadata-only count result and explicitly report no data read, no
  row read, no decode/materialization, no object-store IO, no write IO, and no fallback.
- Inconclusive metadata reports request encoded predicate evaluation without executing it.
- This pass adds no encoded predicate evaluation, scan/read-start invocation, encoded-data
  traversal, row read, decode/materialization, Arrow conversion, object-store IO, write behavior,
  spill IO, external baseline invocation, or fallback execution.

## CG-2.2e / CG-13.5 count-where selection-vector filter evidence surfacing

- Primary RFC linkage: RFC 0012 Diagnostics/Capabilities, RFC 0013 Streaming/Zero-Copy Boundary, RFC
  0015 Correctness/testing, RFC 0021 Expression/kernel registry, RFC 0025 Competitive/no-fallback,
  RFC 0026 Vortex query-readiness boundaries, and RFC 0029 Correctness/benchmarks/certificates.
- `shardloom vortex-count-where` now surfaces encoded predicate evaluation evidence,
  selection-vector filter-kernel evidence, and encoded filter admission fields in text/JSON output.
- Metadata-proven predicates can report selection-vector evidence and registry-ready encoded filter
  admission while keeping benchmark evidence, production claims, CG-2 closeout, and CG-13 closeout
  disabled.
- Inconclusive predicates still report the encoded-value-kernel blocker instead of attempting hidden
  reads or fallback execution.
- This phase adds no encoded value reads, generalized filtered-count execution, projection
  execution, adapter runtime, non-local source read, object-store IO, row read, requested
  decode/materialization, Arrow conversion, write IO, spill IO, benchmark/superiority claim, CG-2
  closeout, CG-13 closeout, external engine invocation, or fallback execution.

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

- `ShardLoom` now exposes projection-readiness planning through `shardloom
  vortex-projection-readiness-plan <candidate_source> <dataset_uri> [flags] [--format text|json]`.
- Candidate sources are `metadata-schema-projection`, `encoded-column-path`, and `unknown`.
- CLI flags surface existing readiness signals, including feature-gate, query-primitive readiness,
  metadata/footer readiness, encoded-data-path readiness, projection
  primitive/provided/supported/unsupported, object-store target,
  decode/materialization/Arrow/write/scan risks, and fallback-policy blocking.
- The command emits deterministic text/JSON fields for status, mode, projection readiness, candidate
  source, readiness signals, no-op effect fields, and `fallback_execution_allowed=false`.
- Focused CLI tests cover missing/invalid arguments, unknown options, bare `json`/`text` rejection,
  metadata-schema readiness, encoded-column readiness, unknown-source blocking, missing encoded path
  blocking, unsupported projection blocking, JSON output dispatch, and report-only field invariants.
- The command does not execute projection, apply projection, call scan/read-start APIs, read
  metadata/footer or encoded data, read rows, decode, materialize, convert to `Arrow`, perform
  object-store `IO`, write data, call upstream scans, or attempt fallback execution.
- CG-2.1+ actual primitive execution remains deferred until real metadata/footer and encoded-data
  execution paths are approved.

## R5 systems-learning vocabulary traceability

- RFC 0008: `SplitSource`, `TaskLease`, `PlacementHint`, `IntermediateArtifactRef`,
  `RecoveryStrategy`.
- RFC 0012: `PushdownProofReport`, `LoweringTraceReport`, `TaskGranularityReport`,
  `RuntimeFilterReport`, `PlannedVsActualOperatorProfile`, `PlanPortabilityReport`.
- RFC 0016: `OptimizerDecisionKind`, runtime filter lifecycle, pushdown proof, split/fuse/coalesce
  decisions.
- RFC 0018: `OperatorProfile`, planned-vs-actual runtime reporting, `system.*` introspection
  surfaces.
- RFC 0022: `PlanPortabilityReport`, Substrait-like portability/loss boundary.
- RFC 0011: SQL frontend parse/bind/validate boundary.



## R5.2 additions (docs/RFC-only)

| RFC | Competitive gate mapping | RFC linkage | Notes |
| --- | --- | --- | --- |
| RFC 0031 | CG-19 | RFC 0013; RFC 0008; RFC 0012; RFC 0016; RFC 0018 | docs/RFC-only in this pass; no runtime behavior or dependency changes. |
| RFC 0032 | CG-20 | RFC 0011; RFC 0012; RFC 0015; RFC 0021; RFC 0022; RFC 0023; RFC 0029; RFC 0030 | docs/RFC-only in this pass; no runtime behavior or dependency changes. |


## R5.3 — capability coverage and certification deepening
- Scope: docs/RFC-only deepening pass.
- RFC 0031 deeper contracts map primarily to CG-19, with related trace links to RFC 0008, RFC 0012,
  RFC 0013, RFC 0016, and RFC 0018.
- RFC 0032 deeper contracts map primarily to CG-20, with related trace links to RFC 0011, RFC 0012,
  RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, and RFC 0030.
- This phase adds no runtime/parser/adapter/dependency/fallback behavior.

## R5.3.1 RFC consistency fixes (docs-only)

- RFC 0031 transition semantics corrected so metadata-first planning can continue from
  `metadata_only` into encoded states when metadata is insufficient.
- RFC 0032 claim evidence semantics corrected to distinguish emitted evidence fields from
  progressively required-pass fields.
- Docs-only update; no runtime behavior, dependency, parser, execution, adapter, or fallback
  changes.

## R5.3.2 docs-wide CG-19/CG-20 consistency pass (docs-only)

- RFC 0025 keeps CG-19 and CG-20 inside the primary Competitive Engine Track list.
- RFC 0031 requires per-source/sink-path `NativeIoCertificate` evidence instead of a single
  run-level certificate.
- RFC 0032 uses neutral claim-stage labels until correctness and benchmark evidence authorize
  superiority or best-default claims.
- RFC 0032 treats decoded reference behavior as test-only/reference evidence unless a native
  execution tier is explicitly certified.
- Downstream architecture docs keep CG-1 through CG-20 visible.
- This phase adds no runtime/parser/adapter/dependency/fallback behavior.

## R5.4 capability certification sequencing (docs-only)

- `docs/architecture/capability-certification-sequencing.md` splits CG-20 into implementation-ready
  batches before code or dependency work.
- Sequencing maps SQL coverage, operator coverage, function coverage, adapter certification,
  semantic profiles, migration compatibility, workload constitution, scorecards, and CI snapshots to
  explicit acceptance boundaries.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and
  RFC 0031.
- This phase adds no runtime/parser/adapter/dependency/fallback behavior.

## R5.4.1 core capability matrix contracts

- `shardloom-core/src/certification.rs` adds report-only CG-20 contract shapes for SQL coverage,
  operator coverage, function coverage, adapter certification, semantic profiles, migration
  compatibility, and best-choice scorecards.
- `CapabilityCertificationReport::contract_only()` emits planned foundation matrices with
  `fallback_attempted=false`.
- `test_reference_only` evidence is modeled as non-production certification evidence and cannot
  satisfy production claim helpers.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and
  RFC 0031.
- This phase adds no SQL parser, SQL execution, adapter runtime, function registry, operator kernel,
  dependency, external probing, or fallback behavior.

## R5.4.2 capability discovery surface

- `shardloom-cli/src/main.rs` exposes report-only CG-20 discovery through `shardloom capabilities
  <scope>`.
- Implemented scopes: `sql`, `functions`, `operators`, `adapters`, `semantic-profiles`, `migration`,
  and `certification`.
- Broader user-surface scopes such as `data-etl`, `python`, `unstructured-media`,
  `universal-adapters`, `api-surfaces`, `observability`, `deployment`, `extensions`, and
  `security-governance` remain planned until report-only contracts and snapshot coverage are added.
- `shardloom capabilities` without a scope remains the existing engine-level capability summary.
- Discovery output includes stable output-envelope fields for scope, schema version,
  fallback-disabled status, fallback-attempted status, and side-effect/probe flags.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and
  RFC 0031.
- This phase adds no SQL parser, SQL execution, adapter runtime, function registry, operator kernel,
  dependency, filesystem/network/catalog probing, external-engine probing, or fallback behavior.

## R5.4.2a capability certification snapshot tests

- `shardloom-contract-tests/tests/capability_certification_snapshots.rs` locks the planned CG-20
  matrix names, schema versions, and unsupported/default statuses.
- `shardloom-cli/tests/capability_discovery_snapshots.rs` locks scoped `shardloom capabilities
  <scope>` JSON field names and report-only probe flags.
- Snapshot tests cover SQL, operator, function, adapter, semantic profile, migration, and
  best-choice scorecard surfaces.
- Certification report output is checked against `FeatureFootprintReport` no-probe expectations
  where the contracts overlap.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and
  RFC 0031.
- This phase adds no SQL parser, SQL execution, adapter runtime, function registry, operator kernel,
  dependency, filesystem/network/catalog probing, external-engine probing, or fallback behavior.

## R5.4.2b user-surface capability discovery

- `shardloom capabilities data-etl`, `python`, `dataframe`, `notebook`, `udfs`,
  `universal-adapters`, `event-api-saas-adapters`, `unstructured-media`, `api-surfaces`,
  `observability`, `deployment`, `extensions`, and `security-governance` now expose report-only
  CG-20 user-surface scopes.
- Each scope maps to the corresponding `WorldClassSufficiencyReport` dimension and emits required
  evidence gates, surface component labels, production-claim blocking, best-default publication
  blocking, fallback status, and no-probe/no-runtime fields.
- `shardloom-cli/tests/capability_discovery_snapshots.rs` locks field ordering, scope names,
  report-only invariants, and selected dimension mappings.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0010, RFC 0011, RFC 0012, RFC 0013, RFC 0018, RFC 0019, RFC 0023, RFC 0030, and
  RFC 0031.
- This phase adds no SQL parser, SQL execution, Python package, DataFrame runtime, notebook runtime,
  UDF/plugin runtime, adapter runtime, media extraction, filesystem/network/catalog/adapter probing,
  data reads, object-store IO, writes, external-engine execution, superiority claim, best-default
  publication, or fallback behavior.

## R5.4.3 SQL frontend sequencing

- RFC 0032 now defines the SQL frontend stage ladder from `declared_only` through
  `benchmarked_certified`.
- `SqlFrontendReport` records parser, binder, semantic-profile, catalog, function, lowering,
  unsupported-construct, materialization, SQL coverage snapshot, diagnostics, dependency, runtime,
  and fallback fields.
- Parse-only status is explicitly not execution support, planning support, binding support, or
  semantic conformance.
- Native logical lowering must reject unsupported residuals instead of carrying them toward fallback
  execution.
- Native physical lowering must declare decode/materialization, ordering, partitioning, memory,
  spill, and sink requirements.
- Parser dependency approval remains deferred to an explicit dependency/RFC pass.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, adapter runtime, function registry, operator kernel,
  dependency, filesystem/network/catalog probing, external-engine probing, or fallback behavior.

## R5.4.4 operator and function certification sequencing

- RFC 0032 now defines operator certification transition meaning from `unsupported` through
  `production_certified`.
- `OperatorCertificationReport` records family, status, semantic profile, representation states,
  memory certification, materialization/order/partition requirements, correctness, semantic
  conformance, benchmark, diagnostics, report refs, and fallback status.
- Operator production certification requires correctness, semantic conformance, memory/spill safety,
  diagnostics, benchmark evidence, and no-fallback invariants.
- RFC 0032 now defines function certification status meaning using the shared
  `CapabilityCertificationStatus` vocabulary.
- `FunctionCertificationReport` records names, aliases, group, types, null behavior, determinism,
  volatility, effects, encoded/selection-vector/streaming/spill support, materialization, semantic
  profile, correctness, semantic conformance, benchmark, diagnostics, and fallback status.
- `test_reference_only` cannot satisfy production certification for operators or functions.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and
  RFC 0031.
- This phase adds no SQL parser, SQL execution, adapter runtime, function registry, operator kernel,
  dependency, filesystem/network/catalog probing, external-engine probing, or fallback behavior.

## R5.4.4a approximate aggregate and sketch function roadmap

- RFC 0032 now defines approximate aggregate/sketch functions as a CG-20 function family, starting
  with canonical `approx_count_distinct(col)` and incumbent-compatible aliases such as
  `approx_distinct` and `approx_n_unique` where semantic profiles allow them.
- Certification requires ungrouped and grouped distinct support, partial sketch construction,
  associative merge, deterministic serialization/deserialization, sketch version/hash-seed metadata,
  declared error bounds, exact-reference comparison fixtures, null/type handling, and diagnostics.
- Encoded-aware sketch strategy evidence is required for dictionary, run-length, validity,
  selection-vector, and partial-decode cases before ShardLoom can claim differentiated encoded
  execution.
- Production certification remains gated by CG-5 correctness fixtures, CG-6 benchmarks/error
  distributions, CG-7 aggregate-state admission, CG-13 representation evidence, CG-16 execution
  certificates, CG-19 Native I/O certificates, and `fallback_attempted=false`.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0015, RFC 0021, RFC 0029, and RFC 0031.
- This phase adds no SQL parser, SQL execution, function registry, sketch implementation, operator
  kernel, dependency, benchmark claim, production certification, external-engine probing, or
  fallback behavior.

## R5.4.5 adapter certification sequencing

- RFC 0032 now maps adapter maturity A0-A7 to evidence requirements from declared-only through
  benchmarked/certified.
- RFC 0032 defines adapter pushdown and residual-expression boundaries for exact,
  exact-with-residual, conservative false-positive, unsupported, and unsafe-rejected behavior.
- RFC 0032 expands adapter certification with source/sink report refs, fidelity report refs, native
  I/O certificate refs, metadata/statistics/fidelity loss, commit/recovery semantics, side effects,
  and diagnostics.
- RFC 0031 now links source capability, sink requirement, adapter fidelity, and native I/O
  certificate evidence to adapter certification.
- External source pushdown is proof-backed source behavior, not hidden fallback execution.
- Adapter certification remains workload/path scoped and cannot be inferred from external baseline
  availability.
- Primary RFC linkage: RFC 0031 and RFC 0032.
- Related RFCs: RFC 0008, RFC 0012, RFC 0013, RFC 0015, RFC 0016, RFC 0018, RFC 0021, RFC 0022, RFC
  0029, and RFC 0030.
- This phase adds no SQL parser, SQL execution, adapter runtime, object-store IO, file-format
  dependency, catalog dependency, external-engine probing, or fallback behavior.

## R5.4.6 semantic profile and migration sequencing

- RFC 0032 now defines `SemanticProfileReport` fields, semantic dimension statuses, profile-specific
  evidence, and compatibility-profile boundaries.
- RFC 0032 states that Spark-compatible, DataFusion-compatible, Postgres-like, ANSI-strict, and
  ShardLoom-native profiles are semantics contracts, not execution modes.
- RFC 0032 now defines `MigrationCompatibilityReport` fields for supported constructs, unsupported
  constructs, semantic differences, function differences, adapter differences, materialization
  requirements, rewrite suggestions, evidence labels, diagnostics, and fallback status.
- RFC 0032 now defines performance/cost delta estimate fields with evidence labels and uncertainty,
  and blocks unsupported gain claims.
- RFC 0032 now defines Vortex conversion payback fields for source conversion scope, cost, benefit,
  uncertainty, and recommendation.
- External engines remain comparison, fixture, and migration baselines only.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0011, RFC 0012, RFC 0015, RFC 0016, RFC 0021, RFC 0022, RFC 0029, RFC 0030, and
  RFC 0031.
- This phase adds no SQL parser, SQL execution, migration analyzer runtime, compatibility execution
  mode, adapter runtime, external-engine dependency, external-engine probing, benchmark claim, or
  fallback behavior.

## R5.4.7 workload constitution and scorecard sequencing

- RFC 0032 now defines `WorkloadConstitution` fields for workload categories, query patterns, data
  source profiles, sink target profiles, semantic profiles, SQL/operator/function/adapter
  requirements, API surfaces, scale shape, objectives, budgets, fixtures, benchmarks, migration
  sources, evidence refs, diagnostics, and fallback status.
- RFC 0032 now defines `WorkloadCategoryEvidence` entries tying each category to required coverage,
  correctness tests, benchmark scenarios, native I/O certificates, unsupported budgets,
  materialization budgets, evidence status, and diagnostics.
- RFC 0032 now defines `BestChoiceScorecard` fields, dimension statuses, dimension entries,
  optional/deferred weighting rules, mandatory dimension behavior, and claim publication
  requirements.
- RFC 0032 now defines `BestDefaultCertificationDossier` fields, minimum evidence floor,
  disqualifiers, and publication decisions for best-default-engine claims.
- Best-default certification remains workload-scoped and blocked by missing correctness, benchmark,
  semantic, adapter, native I/O, memory/spill, observability, migration, deployment,
  dependency-policy, or no-fallback evidence.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0010, RFC 0011, RFC 0012, RFC 0013, RFC 0014, RFC 0015, RFC 0016, RFC 0021, RFC
  0023, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, benchmark implementation, certification runtime,
  migration analyzer runtime, compatibility execution mode, adapter runtime, external-engine
  dependency, external-engine probing, superiority claim, or fallback behavior.

## R5.4.8 CI and snapshot sequencing

- RFC 0032 now defines `CapabilitySurfaceSnapshot` fields for schema versions, field keys, entry
  keys, status counts, certification counts, no-probe flags, external-engine invocation flags,
  diagnostics, and fallback status.
- RFC 0032 now defines snapshot kinds for diagnostics, capability discovery, SQL, operators,
  functions, adapters, semantic profiles, migration compatibility, workload constitutions,
  scorecards, best-default dossiers, world-class sufficiency, feature footprint, and no-fallback
  invariants.
- RFC 0032 now defines `CapabilityDriftPolicy` fields plus allowed and blocked snapshot changes.
- RFC 0032 separates docs-only, report-only, correctness-gated, benchmark-gated, and release-gated
  CI levels.
- Snapshot execution remains deterministic, side-effect-free, report-only, no-probe, and
  no-fallback.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0012, RFC 0015, RFC 0024, RFC 0025, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, benchmark implementation, certification runtime,
  migration analyzer runtime, compatibility execution mode, adapter runtime, new tests,
  external-engine dependency, external-engine probing, superiority claim, or fallback behavior.

## R5.4.9 RFC sufficiency hardening pass

- RFC 0025 now defines the canonical best-default evidence gate for CG-20 claims.
- RFC 0031 now defines CG-19 sufficiency gates and disqualifiers for per-source/sink-path native I/O
  certificate evidence.
- RFC 0032 now defines `WorldClassSufficiencyReport` fields, sufficiency decisions, invariants,
  disqualifiers, and explicit implementation deferrals.
- Best-default and world-class claims now require workload-scoped links across
  `WorkloadConstitution`, `BestChoiceScorecard`, `BestDefaultCertificationDossier`,
  `WorldClassSufficiencyReport`, CG-5 correctness, CG-6 benchmark, CG-16 execution certificate,
  CG-19 native I/O certificate, capability snapshots, dependency policy, and no-fallback evidence.
- Primary RFC linkage: RFC 0025, RFC 0031, and RFC 0032.
- Related RFCs: RFC 0008, RFC 0012, RFC 0013, RFC 0015, RFC 0016, RFC 0018, RFC 0021, RFC 0023, RFC
  0029, and RFC 0030.
- This phase adds no SQL parser, SQL execution, benchmark implementation, certification runtime,
  migration analyzer runtime, compatibility execution mode, adapter runtime, dependency,
  external-engine probing, superiority claim, or fallback behavior.

## R5.4.10 user-surface RFC hardening

- RFC 0032 now defines `ApiSurfaceReport`, `ObservabilityCertificationReport`,
  `DeploymentReadinessReport`, `ExtensionCapabilityReport`, and `SecurityGovernanceReport` as CG-20
  certification evidence surfaces.
- Capability discovery now has explicit response fields and statuses for supported, partially
  supported, planned, disabled, feature/config gated, materialization gated, external-effect gated,
  dependency-review gated, unsupported, and unsafe-rejected entries.
- API/client/server maturity now covers CLI JSON, Rust, Python, DataFrame/query builder, SQL file,
  config/job, agent, notebook, HTTP/gRPC, FlightSQL-like, JDBC/ODBC, and BI/dashboard surfaces
  without implying execution delegation.
- Extension and UDF certification now covers runtime kind, type/null/effect metadata, sandboxing,
  permissions, credentials, resource limits, materialization boundaries, redaction/audit policy,
  license/provenance, and no-execution inspection behavior.
- Observability, deployment, security/governance, and extension-safety evidence now feed workload
  constitutions, best-choice scorecards, best-default dossiers, and capability snapshots.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0010, RFC 0011, RFC 0012, RFC 0018, RFC 0019, RFC 0023, RFC 0024, RFC 0030, and
  RFC 0031.
- This phase adds no runtime behavior, SQL parser, SQL execution, API implementation, server
  implementation, UDF/plugin runtime, adapter runtime, dependency, external probing, superiority
  claim, or fallback behavior.

## R5.4.11 architecture document ownership cleanup

- `phased-execution-plan.md` established the active queue/status ownership model; current detailed
  completed-session provenance is now split into `phased-execution-completed-ledger.md`.
- Supporting architecture docs now identify whether they are traceability maps, sequencing ledgers,
  cleanup backlogs, inventories, reference maps, or vocabulary references.
- Cleanup and sequencing docs use checklist/completed-ledger structure where status tracking is
  meaningful.
- Conceptual and reference docs use structured maps and guardrails rather than misleading completion
  checklists.
- Primary RFC linkage: RFC 0012, RFC 0024, RFC 0025, and RFC 0030.
- Related RFCs: RFC 0010, RFC 0011, RFC 0013, RFC 0014, RFC 0017, RFC 0018, RFC 0019, RFC 0020, RFC
  0031, and RFC 0032.
- This phase adds no runtime behavior, parser, execution, API implementation, server implementation,
  UDF/plugin runtime, adapter runtime, dependency, external probing, superiority claim, CG closeout,
  or fallback behavior.

## R5.4.12 common data/ETL and Python/media surface expansion

- RFC 0032 now defines CG-20 coverage for common data/ETL surfaces beyond SQL, including ingestion,
  schema contracts, data quality, cleaning, transformation, enrichment, incremental state,
  write/export, partition/layout behavior, bounded streaming, memory/spill, lineage/provenance,
  governance, and pipeline observability.
- RFC 0032 now places mature Python wrapper/API, DataFrame/query-builder, notebook, Python UDF, and
  Python packaging certification under CG-20 user capability, starting with a thin stable CLI/API
  JSON client and requiring explicit diagnostics and materialization boundaries.
- RFC 0032 now clarifies that CG-11 supplies API/protocol foundation while CG-20 owns mature Python
  and user-capability certification.
- RFC 0032 now expands universal adapter coverage to partitioned datasets, compressed wrappers,
  relational/warehouse sources, event/API/SaaS sources, Python/notebook surfaces, and
  unstructured/media references.
- RFC 0032 now defines unstructured/media capability boundaries for typed references, extracted
  text/chunks/metadata, extractor provenance, redaction, effect permissions, materialization costs,
  and unsupported diagnostics.
- Workload constitutions, scorecards, best-default dossiers, and sufficiency reports now include
  data/ETL, Python, and unstructured/media evidence where those surfaces are in scope.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0010, RFC 0011, RFC 0012, RFC 0013, RFC 0018, RFC 0019, RFC 0023, RFC 0030, and
  RFC 0031.
- This phase adds no runtime behavior, parser, SQL execution, Python package, adapter runtime, media
  runtime, OCR/LLM/embedding dependency, external probing, superiority claim, or fallback behavior.

## CG-20.1 world-class sufficiency report foundation

- `shardloom-core/src/certification.rs` adds `WorldClassSufficiencyReport`,
  `WorldClassSufficiencyDecision`, `WorldClassSufficiencyStatus`, and
  `WorldClassSufficiencyDimensionKind`.
- The report turns RFC 0032's best-default sufficiency fields into required dimensions for SQL,
  operators, functions, adapters, semantic profiles, migration, common data/ETL, Python/API,
  DataFrame/query builder, notebook, UDF/plugin, universal adapters, event/API/SaaS adapters,
  unstructured/media, observability, deployment, extension safety, security/governance, native I/O
  certificates, execution certificates, correctness, semantic conformance, benchmarks, memory/spill,
  capability snapshots, scorecards, dossiers, and no-fallback integrity.
- `world-class-sufficiency-plan` exposes deterministic JSON/text fields for dimension counts,
  dimension order, key evidence statuses, certificate coverage, rates, blocking gaps, baseline
  references, publication decision, and no-fallback/no-side-effect evidence.
- All required dimensions default to `evidence_insufficient`; best-default publication remains
  `not_certified` and not allowed.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0010, RFC 0011, RFC 0012, RFC 0013, RFC 0015, RFC 0016, RFC 0018, RFC 0019, RFC
  0021, RFC 0022, RFC 0023, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no SQL parser, SQL execution, DataFrame runtime, Python package, UDF runtime,
  adapter runtime, function registry implementation, operator kernels, source/sink runtime
  certificate emission, filesystem/network/catalog/adapter probing, data reads,
  decode/materialization, row reads, Arrow conversion, object-store IO, writes, spill IO, package
  publication, performance claim, superiority claim, best-default publication, or fallback behavior.

## CG-20.2 user-surface capability discovery

- `shardloom-cli/src/main.rs` maps broad CG-20 user-surface `capabilities` scopes to
  `WorldClassSufficiencyReport` dimensions.
- New scopes: `data-etl`, `python`, `dataframe`, `notebook`, `udfs`, `universal-adapters`,
  `event-api-saas-adapters`, `unstructured-media`, `api-surfaces`, `observability`, `deployment`,
  `extensions`, and `security-governance`.
- Each scope exposes its dimension status, required correctness/semantic/benchmark evidence,
  adapter/native-I/O/execution-certificate/capability-snapshot gates, planned surface components,
  no-fallback fields, and blocked production/best-default claim fields.
- Snapshot coverage locks the user-surface field keys, scope values, report-only flags, and selected
  dimension mappings.
- Primary RFC linkage: RFC 0032.
- Related RFCs: RFC 0010, RFC 0011, RFC 0012, RFC 0013, RFC 0018, RFC 0019, RFC 0023, RFC 0030, and
  RFC 0031.
- This phase adds no SQL parser, SQL execution, Python package, DataFrame runtime, notebook runtime,
  UDF/plugin runtime, adapter runtime, media extraction, filesystem/network/catalog/adapter probing,
  data reads, decode/materialization, row reads, Arrow conversion, object-store IO, writes,
  external-engine execution, superiority claim, best-default publication, or fallback execution.

## CG-11.4 / CG-20.3 Python live ETL client helpers

- `python/src/shardloom/client.py` exposes explicit source-tree client methods for
  `traditional-analytics-run`, `traditional-analytics-vortex-run`, `live_etl_smoke`,
  `dynamic-work-shaping-plan`, `sizing-feedback-plan`, `benchmark-plan`, and
  `benchmark-claim-evidence-plan`.
- `ShardLoomClient.from_repo()` provides opt-in local source-tree binary discovery for
  `target/release/shardloom` and `target/debug/shardloom` while preserving explicit binary and
  `SHARDLOOM_BIN` overrides.
- Importing the package remains side-effect-free; local binary discovery and runtime commands happen
  only when a caller creates a client and invokes a method.
- Successful planning/evidence envelopes remain inspectable even when they carry error-severity
  diagnostics for blockers; nonzero CLI results and `error`/`unsupported` statuses still raise
  through `ShardLoomCommandError` by default.
- `python/examples/live_etl_smoke.py` documents the current CSV-to-Vortex and native Vortex live ETL
  smoke surfaces without representing them as mature SQL, DataFrame, adapter, UDF, or production ETL
  certification.
- `PythonWrapperFoundationReport` expands the initial command scope to include native Vortex ETL
  smoke, dynamic sizing/work-shaping advisory reports, and benchmark evidence plan discovery.
- Primary RFC linkage: RFC 0030 and RFC 0032.
- Related RFCs: RFC 0010, RFC 0012, RFC 0016, RFC 0024, RFC 0029, and RFC 0031.
- This phase adds no package publication, native binding, PyO3/maturin, DataFrame runtime, notebook
  runtime, Python UDF runtime, SQL parser/execution, production adapter runtime, object-store IO,
  writes, external-engine execution, superiority claim, best-default publication, or fallback
  behavior.

## R5.4.13 README roadmap source-of-truth cleanup

- README now acts as a stable project entry point rather than a mutable implementation-status
  ledger.
- README points active implementation state to `docs/architecture/phased-execution-plan.md`.
- README preserves the no-fallback policy and evidence-gated claim rule.
- README names CG-20 user-capability surfaces such as SQL, Python/API, DataFrame/query builder,
  notebook, UDF, common ETL, universal adapters, and unstructured/media workflows without claiming
  implementation completion.
- Primary RFC linkage: RFC 0025 and RFC 0032.
- Related RFCs: RFC 0012, RFC 0029, RFC 0030, and RFC 0031.
- This phase adds no runtime behavior, parser, execution, adapter runtime, Python package, media
  runtime, dependency change, benchmark claim, superiority claim, or fallback behavior.

## CG-21/CG-22/CG-23 broader synthesis cleanup

- README now reflects the longer-term product direction: complete user data workflows,
  batch/live/hybrid engine modes, and REST/event/remote API surfaces, while keeping support claims
  evidence-gated.
- `docs/architecture/phased-execution-plan.md` promotes CG-21A through CG-21R, CG-22A through
  CG-22I, and CG-23A through CG-23I into the Planned queue so future sessions can move items through
  Planned and Completed without maintaining a second current-status list.
- `docs/architecture/canonical-terminology.md` defines the new user-workflow, engine-fabric, and
  remote-API terms introduced by RFC 0033, RFC 0034, and RFC 0035.
- `docs/architecture/systems-learning-map.md` adds technique-transfer guidance for user workflows,
  dynamic/continuous table semantics, state/changelog/checkpoint systems, streaming/batch lake
  formats, API/event standards, telemetry, lineage, and agent surfaces.
- `docs/architecture/incumbent-gap-opportunity-map.md` adds the new CG-21, CG-22, and CG-23
  opportunities to the incumbent gap map.
- `docs/architecture/capability-certification-sequencing.md` clarifies that CG-20 remains the
  capability surface while CG-21/22/23 add workflow, engine-mode, and remote API layers.
- `docs/architecture/universal-input-contract.md` clarifies how universal inputs feed CG-21 user
  workflows, CG-22 engine selection, and CG-23 remote discovery without authorizing probes or
  fallback execution.
- `AGENTS.md` now routes broad user workflow, batch/live/hybrid, REST/event API, remote result
  delivery, lineage/governance export, and agent API work through RFC 0033, RFC 0034, and RFC 0035
  before implementation.
- Primary RFC linkage: RFC 0033, RFC 0034, and RFC 0035.
- Related RFCs: RFC 0025, RFC 0032, RFC 0031, RFC 0030, RFC 0029, RFC 0019, RFC 0018, RFC 0017, RFC
  0016, RFC 0014, and RFC 0012.
- This phase adds no runtime behavior, HTTP server, dependency, package publication, reader, writer,
  SQL/DataFrame execution, UDF runtime, live/hybrid runtime, object-store IO, catalog access,
  benchmark execution, superiority claim, best-default claim, or fallback behavior.

## Cross-RFC platform hardening coverage audit

- `docs/architecture/phased-execution-plan.md` now includes a Planned cross-RFC platform hardening
  and release-readiness lane so older governing RFC themes are visible in the implementation
  sequence instead of only in historical traceability.
- The lane explicitly covers RFC 0014 memory/spill/OOM safety, RFC 0017 fault
  tolerance/cancellation/recovery/idempotency, RFC 0018 observability/tracing/profiling/debug
  bundles, RFC 0019 security/secrets/governance/data-egress/agent safety, and RFC 0024 release
  engineering/API compatibility/packaging.
- It also promotes the remaining cross-cutting CG tracks CG-15 CPU operator specialization, CG-17
  stateful reuse/incremental execution, and CG-18 universal import/deployment/baseline harness into
  Planned so they can be pulled into Active deliberately.
- A follow-through Planned lane also names RFC 0010 developer/agent UX, RFC 0011 modular
  extensibility, RFC 0020 schema/catalog/table compatibility, RFC 0022 native-first plan
  IR/interoperability, and RFC 0023 extension/plugin sandboxing so those themes remain visible
  before broader user/runtime expansion.
- Primary RFC linkage: RFC 0014, RFC 0017, RFC 0018, RFC 0019, RFC 0024, RFC 0010, RFC 0011, RFC
  0020, RFC 0022, and RFC 0023.
- Related RFCs: RFC 0004, RFC 0008, RFC 0012, RFC 0015, RFC 0016, RFC 0021, RFC 0025, RFC 0027, RFC
  0029, RFC 0030, RFC 0032, RFC 0033, RFC 0034, and RFC 0035.
- This audit adds no runtime behavior, dependency, reader, writer, object-store IO, server/API
  runtime, tracing/exporter integration, UDF execution, package publication, benchmark rerun,
  superiority claim, best-default claim, or fallback behavior.

## CG-20.11 Conda package split recipe scaffolds

- `packaging/conda/shardloom-cli/meta.yaml` defines the platform-specific Rust CLI package scaffold.
- `packaging/conda/shardloom-python/meta.yaml` defines the pure Python `noarch: python` wrapper
  package scaffold.
- `packaging/conda/shardloom/meta.yaml` defines the optional one-command metapackage scaffold
  depending only on the CLI and Python wrapper packages.
- `packaging/conda/README.md` records that these are local recipe scaffolds, not feedstock or
  publication artifacts.
- `PythonWrapperFoundationReport` and `world-class-sufficiency-plan` now surface `recipe_scaffolded`
  Conda status while keeping clean package build/install/publication certification blocked.
- `shardloom-contract-tests/tests/conda_packaging_recipes.rs` verifies the package split and absence
  of Spark/DataFusion/DuckDB/Polars/pandas/Dask/Velox runtime dependency lines in the recipes.
- Primary RFC linkage: RFC 0032, RFC 0033, RFC 0024, RFC 0030, RFC 0010, and RFC 0025.
- This phase adds no Conda package build, feedstock, package publication, release tag, source
  archive hash, SBOM/signing, external-engine dependency, runtime behavior,
  SQL/DataFrame/UDF/adapter implementation, or fallback execution.

## CG-13.1 encoded path selection report foundation

- `shardloom-vortex/src/encoded_path_selection.rs` adds a report-only
  `VortexEncodedExecutionPathSelectionReport` for CG-13 count/filter/project encoded-native
  candidate selection.
- The report composes existing physical operator profiles, encoded count discovery, encoded
  predicate discovery, selection-vector filter discovery, and encoded projection evidence into one
  agent-readable artifact.
- `vortex-encoded-path-selection-plan` exposes the report through stable CLI JSON/text output with
  selected execution levels, evidence sources, decode/materialization avoided counts,
  selection-vector preservation, and explicit no-work/no-fallback fields.
- The path selection report does not read data, decode arrays, materialize values, read rows,
  convert to Arrow, touch object stores, write, spill, execute runtime paths, invoke external
  engines, or allow fallback.
- Primary RFC linkage: RFC 0026 and RFC 0021.
- Related RFCs: RFC 0012, RFC 0015, RFC 0025, RFC 0029, RFC 0031, and RFC 0032.
- This phase adds no generalized encoded execution, scan/read-start API, parser, SQL execution,
  adapter runtime, object-store IO, writes, spill IO, benchmark claim, production/superiority claim,
  CG closeout, or fallback behavior.

## CG-14.1 adaptive optimizer and memory decision report foundation

- `shardloom-plan/src/optimizer.rs` adds `AdaptiveOptimizerMemoryReport`, a report-only contract for
  runtime-adaptive optimizer and execution-memory decision evidence.
- The report records deferred optimizer rules for conservative runtime filters, dynamic pruning
  proof gates, and memory/spill-aware planning boundaries.
- The report surfaces candidate adaptive decisions for memory pressure and runtime-filter
  availability while explicitly recording that no runtime adaptation is applied.
- `optimizer-adaptive-memory-plan` exposes deterministic JSON/text fields for rule counts,
  conservative runtime-filter counts, adaptive decision counts, skew signal representation,
  memory/spill proof requirements, side-effect boundaries, and no-fallback status.
- Primary RFC linkage: RFC 0016 and RFC 0014.
- Related RFCs: RFC 0012, RFC 0013, RFC 0015, RFC 0021, RFC 0025, RFC 0027, RFC 0029, RFC 0031, and
  RFC 0032.
- This phase adds no optimizer execution, cost-model execution, runtime-filter build/apply, dynamic
  pruning execution, plan rewrite, join/aggregate/skew execution, allocator/reservation runtime,
  spill execution, object-store IO, writes, benchmark claim, production/superiority claim, CG
  closeout, or fallback behavior.

## CG-15.1 CPU operator specialization report foundation

- `shardloom-core/src/cpu_specialization.rs` adds `CpuOperatorSpecializationReport`, a report-only
  contract for commodity CPU operator specialization candidates.
- The report records filter, project, count-aggregate, aggregate, sort, and join operator/kernel
  candidates with SIMD, cache-aware, branch-reduced, encoded-layout-aware, and
  selection-vector-aware classes.
- The report requires correctness evidence, benchmark evidence, CPU feature guards, portable native
  baselines, and deterministic dispatch before any runtime specialization or performance claim.
- `cpu-specialization-plan` exposes deterministic JSON/text fields for candidate counts,
  operator/kernel order, evidence gates, host CPU architecture/features, filter/encoded
  vector-kernel admission status, dispatch status, unsafe/GPU/FPGA requirements, side-effect
  boundaries, and no-fallback status.
- Primary RFC linkage: RFC 0027 and RFC 0021.
- Related RFCs: RFC 0012, RFC 0015, RFC 0025, RFC 0029, RFC 0031, and RFC 0032.
- This phase adds side-effect-free CPU feature probing and a blocked filter/encoded admission
  diagnostic only. It adds no runtime dispatch, unsafe SIMD implementation, operator execution,
  kernel implementation, data reads, decode/materialization, Arrow conversion, object-store IO,
  writes, spill IO, benchmark execution, performance/superiority claim, production certification,
  CG closeout, or fallback behavior.

## CG-7.15 local encoded `CountAll` physical kernel evidence

- `shardloom-vortex/src/encoded_count_physical_kernel.rs` adds a contextual encoded-native
  physical-kernel report for the approved local encoded `CountAll` path.
- The report consumes existing local scan evidence, local execution evidence, and the CG-16
  execution certificate; it does not open files, scan, decode, materialize, convert to Arrow, touch
  object stores, write, spill, invoke external baselines, or fallback on its own.
- `capabilities operators` and `kernel-registry` now surface the encoded count physical kernel as
  contextual/report-only discovery.
- The feature-gated local encoded count fixture test now verifies correctness evidence, execution
  certificate evidence, and encoded physical-kernel evidence for the same `CountAll` result.
- Primary RFC linkage: RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0012, RFC 0015, RFC 0016, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, filtered-count execution,
  projection execution, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO,
  benchmark claim, superiority claim, CG closeout, or fallback behavior.

## CG-7.16 local encoded `CountAll` CLI evidence surfacing

- `shardloom vortex-count <dataset_uri> --execute-local-encoded-count <memory_gb> <max_parallelism>`
  now surfaces fixture-matched CG-16 execution certificate evidence and CG-7.15 encoded
  physical-kernel evidence in the stable command output.
- Certification is emitted only when the executed local target matches the repository's
  `vortex-local-encoded-count-u64-20000` correctness fixture source ref; arbitrary local `.vortex`
  count execution remains usable without claiming fixture certification.
- The command output records fixture match status, certificate status, correctness/no-fallback
  fields, encoded physical-kernel status, safe-native-kernel evidence, and production-claim-disabled
  status.
- Primary RFC linkage: RFC 0010, RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0015, RFC 0016, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, filtered-count execution,
  projection execution, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO,
  benchmark claim, production/superiority claim, CG closeout, or fallback behavior.

## CG-7.17 encoded count aggregate kernel admission bridge

- `VortexEncodedCountKernelAdmissionReport` maps safe encoded count physical-kernel evidence into
  the CG-7 `PhysicalKernelAdmissionReport` gate for the count-aggregate encoded kernel slot.
- Safe evidence can make the encoded slot registry-ready, but benchmark evidence remains missing so
  production certification and any superiority/best-choice claims remain blocked.
- `vortex-count --execute-local-encoded-count` surfaces encoded count kernel admission fields when
  fixture certification is available.
- `capabilities operators` and `kernel-registry` expose admission discovery fields without probing
  files, executing kernels, or registering a global runtime kernel.
- Primary RFC linkage: RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0015, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, filtered-count execution,
  projection execution, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO,
  benchmark claim, production/superiority claim, broad count/aggregate closeout, CG closeout, or
  fallback behavior.

## CG-7.18 metadata filter kernel admission bridge

- `VortexMetadataFilterKernelAdmissionReport` maps safe metadata-only filter physical-kernel
  evidence into the CG-7 `PhysicalKernelAdmissionReport` gate for the filter metadata-kernel slot.
- Safe evidence can make the metadata filter slot registry-ready, but benchmark evidence remains
  missing so production certification and any superiority/best-choice claims remain blocked.
- `vortex-metadata-physical-kernel-plan filter` surfaces metadata filter kernel admission fields
  when explicit correctness and memory evidence is supplied.
- `capabilities operators` and `kernel-registry` expose admission discovery fields without probing
  files, executing kernels, or registering a global runtime kernel.
- Primary RFC linkage: RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0015, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, encoded predicate
  execution, filtered-count execution, projection execution, parser, SQL execution, adapter runtime,
  object-store IO, writes, spill IO, benchmark claim, production/superiority claim, broad
  filter-kernel closeout, CG closeout, or fallback behavior.

## CG-7.19 metadata projection kernel admission bridge

- `VortexMetadataProjectionKernelAdmissionReport` maps safe metadata-schema projection readiness
  into the CG-7 `PhysicalKernelAdmissionReport` gate for the project metadata-kernel slot.
- Safe evidence can make the project metadata slot registry-ready, but benchmark evidence remains
  missing so production certification and any superiority/best-choice claims remain blocked.
- `capabilities operators` and `kernel-registry` expose admission discovery fields without probing
  files, executing kernels, or registering a global runtime kernel.
- Primary RFC linkage: RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0015, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, encoded projection
  execution, projection execution, row reads, requested decode/materialization, Arrow conversion,
  parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim,
  production/superiority claim, broad projection-kernel closeout, CG closeout, or fallback behavior.

## CG-7.20 metadata count aggregate kernel admission bridge

- `VortexMetadataCountKernelAdmissionReport` maps safe metadata-only `CountAll` and metadata-proof
  `CountWhere` physical-kernel evidence into the CG-7 `PhysicalKernelAdmissionReport` gate for the
  count-aggregate metadata-kernel slot.
- Safe evidence can make the count-aggregate metadata slot registry-ready, but benchmark evidence
  remains missing so production certification and any superiority/best-choice claims remain blocked.
- `vortex-metadata-physical-kernel-plan count` and `vortex-metadata-physical-kernel-plan
  filtered-count` surface metadata count admission fields when explicit correctness and memory
  evidence is supplied.
- `capabilities operators` and `kernel-registry` expose admission discovery fields without probing
  files, executing kernels, or registering a global runtime kernel.
- RFC 0031 wording now records the upstream Vortex Scan API source/sink/split/range-I/O lessons as a
  design reference for CG-19, while preserving ShardLoom-native envelopes and no-fallback execution
  boundaries.
- The systems-learning map now records Vortex blog lessons around lazy operators, IO/write surfaces,
  GPU/device paths, nested/list support, wide-table work, and benchmark visibility as design
  references only.
- Primary RFC linkage: RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0015, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, encoded aggregate
  execution, count execution beyond existing metadata/local paths, row reads, requested
  decode/materialization, Arrow conversion, parser, SQL execution, adapter runtime, object-store IO,
  writes, spill IO, benchmark claim, production/superiority claim, broad count/aggregate closeout,
  CG closeout, or fallback behavior.

## CG-7.21 execution-level coverage discovery

- `PhysicalOperatorExecutionProfileMatrix` now exposes stable counts for distinct native execution
  levels and per-level operator profile support.
- `capabilities operators` and `kernel-registry` surface metadata-only, encoded-native,
  hybrid-native, and native-decoded execution-level counts for the CG-7 operator profile set.
- Reference-only execution remains rejected, and row materialization, Arrow conversion, runtime
  execution, and fallback execution remain disabled in discovery output.
- This closes the CG-7 metadata/encoded/hybrid execution-level checklist item as a
  coverage/discovery contract only; broad filter, projection, count/aggregate, and
  expression-evaluation kernels remain open.
- Primary RFC linkage: RFC 0012, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0015, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no new runtime path, generalized encoded-data execution, filter execution,
  projection execution, aggregate execution, row reads, requested decode/materialization, Arrow
  conversion, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark
  claim, production/superiority claim, broad operator-kernel closeout, CG closeout, or fallback
  behavior.

## CG-7.22 encoded segment predicate evaluation foundation

- `shardloom-core::encoded` now defines encoded predicate evaluation report/status contracts that
  evaluate predicates as far as segment metadata allows.
- Metadata-proven all/none predicates emit selection vectors without data reads, decode,
  materialization, row reads, Arrow conversion, object-store IO, writes, spill IO, runtime fallback,
  or external effects.
- Inconclusive predicates report `needs_encoded_values` instead of silently decoding or claiming
  filter execution.
- Vortex metadata summaries can emit per-segment encoded predicate evaluation reports for the filter
  operator path.
- `capabilities operators` and `kernel-registry` expose report-only encoded predicate evaluation
  discovery fields.
- Primary RFC linkage: RFC 0012, RFC 0015, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no scan/read-start path, broad encoded-data execution, broad filter execution,
  projection execution, aggregate execution, row reads, requested decode/materialization, Arrow
  conversion, parser, SQL execution, adapter runtime, object-store IO, writes, spill IO, benchmark
  claim, production/superiority claim, broad operator-kernel closeout, CG closeout, or fallback
  behavior.

## CG-7.23 selection-vector filter kernel evidence

- `shardloom-vortex` now evaluates contextual selection-vector filter-kernel evidence from
  successful encoded predicate evaluation reports.
- Safe reports can mark the encoded filter kernel slot registry-ready, while benchmark evidence
  remains required before production certification or any superiority claim.
- Inconclusive predicates remain blocked as `needs_encoded_values` and do not decode, materialize,
  convert to Arrow, execute fallback, or claim broad filter execution.
- `capabilities operators` and `kernel-registry` expose selection-vector filter-kernel discovery and
  admission fields.
- Primary RFC linkage: RFC 0012, RFC 0015, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no scan/read-start path, generalized encoded-data execution, encoded-value
  predicate execution, broad filter execution, projection execution, aggregate execution, row reads,
  requested decode/materialization, Arrow conversion, parser, SQL execution, adapter runtime,
  object-store IO, writes, spill IO, benchmark claim, production/superiority claim, broad
  operator-kernel closeout, CG closeout, or fallback behavior.

## CG-7.24 encoded projection kernel evidence

- `shardloom-vortex` now admits safe encoded-column projection readiness into the encoded project
  kernel slot.
- Safe reports can mark the encoded project kernel slot registry-ready, while benchmark evidence
  remains required before production certification or any superiority claim.
- Missing encoded-column readiness blocks admission and does not read encoded data, decode,
  materialize, convert to Arrow, execute fallback, or claim broad projection execution.
- `capabilities operators` and `kernel-registry` expose encoded projection-kernel admission fields.
- Primary RFC linkage: RFC 0012, RFC 0015, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no scan/read-start path, generalized encoded-data execution, encoded-value
  projection execution, broad projection execution, aggregate execution, row reads, requested
  decode/materialization, Arrow conversion, parser, SQL execution, adapter runtime, object-store IO,
  writes, spill IO, benchmark claim, production/superiority claim, broad operator-kernel closeout,
  CG closeout, or fallback behavior.

## CG-7.25 count/aggregate kernel closeout

- CG-7 count/aggregate kernel coverage is complete for the declared CG-7 scope through existing
  encoded `CountAll` physical-kernel evidence/admission and metadata `CountAll`/`CountWhere`
  count-aggregate admission.
- `shardloom-vortex/src/encoded_count_physical_kernel.rs` provides local encoded `CountAll`
  physical-kernel evidence and encoded count-aggregate admission.
- `shardloom-vortex/src/metadata_physical_kernel.rs` provides metadata-only `CountAll` and
  metadata-proof `CountWhere` count-aggregate admission.
- `capabilities operators`, `kernel-registry`, `vortex-count --execute-local-encoded-count`, and
  `vortex-metadata-physical-kernel-plan` expose the count-aggregate evidence chain.
- With filter, projection, count-aggregate, execution-level, and encoded segment evaluation
  checklist items complete, CG-7 is marked complete in the phase plan.
- Primary RFC linkage: RFC 0012, RFC 0015, RFC 0021, RFC 0025, and RFC 0029.
- Related RFCs: RFC 0013, RFC 0016, RFC 0027, RFC 0031, and RFC 0032.
- This phase adds no scan/read-start path, new count execution, new aggregate execution, generalized
  encoded-data execution, row reads, requested decode/materialization, Arrow conversion, parser, SQL
  execution, adapter runtime, object-store IO, writes, spill IO, benchmark claim,
  production/superiority claim, CG-2 closeout, or fallback behavior.

## CG-8.1 streaming plan discovery surface

- `streaming-plan` is now listed in the public CLI usage surface.
- `streaming-plan --format json` emits stable plan fields for mode/status, source
  kind/capability/zero-decode, sink kind/capability/encoded acceptance/materialization/metadata
  preservation, backpressure, memory policy, best work level, runtime execution, and fallback
  status.
- Vortex-native targets surface zero-decode planning with encoded-preserving sink requirements and
  no materialization boundary.
- Compatibility targets surface materialization-required boundaries and metadata-preservation loss
  without treating the compatibility sink as fallback execution.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0021, RFC 0031, and RFC 0032.
- This phase adds no stream execution, task execution, read-start API, row reads, requested
  decode/materialization, Arrow conversion, object-store IO, writes, spill IO, benchmark claim,
  production/superiority claim, CG-8 closeout, or fallback behavior.

## CG-8.2 adaptive sizing, memory, scheduler, and bounded execution evidence surface

- `vortex-adaptive-sizing --format json` now emits stable fields for adaptive sizing status/mode,
  segment input count, planned task count, split decisions, coalesce candidates, estimate blockers,
  keep-single decisions, metadata-only decisions, split/coalesce policy, and target/min/max task
  bytes.
- `vortex-memory-plan --format json` now emits stable fields for memory bridge status/mode, memory
  budget bytes, spill policy, task memory-safety counts, spill-required counts, spill-plan count,
  side-effect flags, and fallback status.
- `vortex-schedule-plan --format json` now emits stable fields for scheduler status/mode, max
  parallelism, bounded batch counts, scheduled/metadata/blocked/unsupported task counts, bounded
  parallelism enforcement, future-action status, side-effect flags, and fallback status.
- `vortex-bounded-local-exec --format json` now emits stable fields for bounded execution
  status/mode, local execution status/mode, completed/deferred/blocked counts, decision count, max
  parallelism, side-effect flags, result-known status, and fallback status.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0021, RFC 0031, and RFC 0032.
- This phase adds no stream execution, new task execution, new read-start API, row reads, requested
  decode/materialization, Arrow conversion, object-store IO, writes, spill IO, dynamic sizing
  feedback execution, benchmark claim, production/superiority claim, CG-8 closeout, or fallback
  behavior.

## CG-8.3 bounded backpressure planning surface

- `BackpressurePlanInput` and `BackpressurePlanReport` now model bounded-memory backpressure
  planning with status/mode, max parallelism, max in-flight chunks, max buffered bytes, optional
  estimated chunk bytes, side-effect flags, diagnostics, and fallback status.
- `plan_backpressure` derives a bounded `BackpressurePolicy` from required bounded memory and max
  parallelism; missing budgets and zero parallelism fail explicitly.
- `backpressure-plan --format json` exposes stable backpressure fields for agents and CI without
  executing streams or tasks.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0031, and RFC 0032.
- This phase adds no stream execution, task execution, read-start API, row reads, requested
  decode/materialization, Arrow conversion, object-store IO, writes, spill IO, dynamic sizing
  feedback execution, benchmark claim, production/superiority claim, CG-8 closeout, or fallback
  behavior.

## CG-8.4 dynamic sizing feedback planning surface

- `DynamicSizingFeedbackInput` and `DynamicSizingFeedbackReport` now model advisory feedback signals
  for target-task-byte adjustment with status/mode, signal counts, current/recommended policy,
  side-effect flags, diagnostics, and fallback status.
- `plan_dynamic_sizing_feedback` treats memory-pressure and too-large-task signals as safer
  target-reduction evidence, too-small-task and object-store-throttling signals as target-increase
  evidence, mixed signals as safer reduction, and no signals as no feedback.
- `sizing-feedback-plan --format json` exposes stable fields for feedback status/mode, signal
  counts, current/recommended target bytes, unchanged execution effects, and fallback-disabled
  evidence.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0031, and RFC 0032.
- This phase adds no stream execution, task execution, feedback application, read-start API, row
  reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO,
  benchmark claim, production/superiority claim, CG-8 closeout, or fallback behavior.

## CG-8.5 encoded streaming-batch planning surface

- `EncodedStreamingBatchPlanInput` and `EncodedStreamingBatchPlanReport` now model encoded
  streaming-batch planning with representation state, zero-decode status, bounded parallelism,
  bounded memory, backpressure, materialization boundary, diagnostics, and side-effect flags.
- `plan_encoded_streaming_batches` preserves Vortex-encoded batch representation for native Vortex
  source/sink plans, reports compatibility-sink materialization boundaries, and blocks object-store
  byte-range sources until object-store streaming IO lands.
- `streaming-batch-plan --format json` exposes stable fields for batch status, source/sink kind,
  representation, zero-decode, encoded preservation, max parallelism, memory, backpressure,
  materialization, side-effect flags, and fallback-disabled evidence.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0031, and RFC 0032.
- This phase adds no stream execution, task execution, read-start API, encoded data reads, row
  reads, requested decode/materialization, Arrow conversion, object-store IO, writes, spill IO,
  dynamic sizing feedback application, benchmark claim, production/superiority claim, CG-8 closeout,
  or fallback behavior.

## CG-8.6 bounded metadata/no-op local task execution

- `VortexBoundedExecutionMode::MetadataOnly` and `VortexBoundedExecutionMode::NoOp` now report task
  execution because their bounded decisions complete local work.
- `VortexBoundedExecutionReport::tasks_executed` derives from completed metadata-only or no-op
  bounded decisions, while data-read, decode, materialization, object-store, write, spill,
  external-effect, and fallback fields remain false.
- Local-engine reports now propagate nested local and bounded effect flags so `tasks_executed=true`
  is visible when bounded metadata/no-op work completes.
- Policy-disabled metadata tasks stay `ReadyButNoExecutableTasks`, keep `tasks_executed=false`, and
  remain side-effect-free.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0021, RFC 0031, and RFC 0032.
- This phase adds no stream runtime execution, bounded parallel encoded/read execution, read-start
  API, encoded data reads, row reads, requested decode/materialization, Arrow conversion,
  object-store IO, writes, spill IO, dynamic sizing feedback execution, benchmark claim,
  production/superiority claim, CG-8 closeout, or fallback behavior.

## CG-8.7 approved local encoded streaming-batch runtime evidence

- `VortexStreamingBatchRuntimeReport` records schema, status, mode, representation, zero-decode,
  bounded-memory, backpressure, source-match, batch-count, row-count, count-result, side-effect,
  diagnostic, and no-fallback fields for the approved local encoded count path.
- Runtime evidence requires an already planned zero-decode Vortex streaming-batch source/sink path
  with no materialization boundary.
- Runtime evidence requires a successful approved local scan encoded-count execution report, and the
  streaming source URI must match the local scan target URI.
- Unsafe reports are rejected if they include decode, materialization, row reads, Arrow conversion,
  object-store IO, writes, spill, external effects, fallback, or source mismatch.
- Stable `vortex-count --execute-local-encoded-count` now surfaces streaming-batch runtime evidence
  beside existing local execution, execution-certificate, physical-kernel, and kernel-admission
  evidence.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0021, RFC 0029, RFC 0031, and RFC 0032.
- This phase adds no broad streaming runtime execution for arbitrary query plans, bounded parallel
  encoded/read execution, new scan/read-start API, new encoded data read path beyond the approved
  local count scan, filtered-count/projection execution, row reads, requested
  decode/materialization, Arrow conversion, object-store IO, writes, spill IO, dynamic sizing
  feedback execution, benchmark claim, production/superiority claim, CG-8 closeout, or fallback
  behavior.

## CG-8.8 dynamic work shaping aggregate surface

- `DynamicWorkShapingReport` aggregates adaptive sizing policy, runtime feedback signals,
  target-task policy, bounded-memory backpressure, scheduler queue policy, runtime-application
  blockers, benchmark evidence blockers, and no-fallback policy into one deterministic report.
- `dynamic-work-shaping-plan [balanced|memory-pressure|object-store-throttled|small-tasks]` exposes
  stable JSON/text fields for profile, surface order, blocked surfaces, feedback status/mode, signal
  counts, target task bytes, backpressure status/mode, bounded memory, side-effect boundaries, and
  no-fallback status.
- Current profiles remain report-only. Runtime feedback-loop application, live policy mutation, and
  benchmark evidence remain explicit blockers before dynamic shaping can affect real execution.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0018, RFC 0025, and RFC 0027.
- Related RFCs: RFC 0008, RFC 0017, RFC 0029, RFC 0031, and RFC 0032.
- This phase adds no live feedback-loop execution, policy mutation, broad streaming runtime, bounded
  parallel encoded/read execution, read-start API, encoded data reads, row reads, requested
  decode/materialization, Arrow conversion, object-store IO, writes, spill IO, benchmark claim,
  production/superiority claim, CG-8 closeout, or fallback behavior.

## CG-8.9 dynamic runtime promotion gate

- `DynamicRuntimePromotionGateReport` records the promotion boundary for dynamic sizing feedback
  application, bounded parallel encoded reads, source-backed reader split parallelism, scheduler
  requeue, bounded backpressure runtime, memory/spill reservation runtime, object-store request
  budgeting, and benchmark/certificate closeout.
- `cg8-runtime-promotion-gate` exposes stable JSON/text fields for existing narrow local evidence,
  blocked runtime surfaces, required metrics/policy/memory/spill/backpressure/cancellation/certificate
  evidence, side-effect boundaries, large-workload claim blockers, and no-fallback status.
- Existing local streaming scan, bounded metadata/no-op, and local filter-project bounded scan
  evidence remain narrow evidence only. Runtime policy mutation and broader parallel source-backed
  read execution stay blocked until workload-scoped correctness, benchmark, execution-certificate,
  Native I/O, memory/spill, scheduler, and no-fallback evidence exists.
- Primary RFC linkage: RFC 0013, RFC 0014, RFC 0016, RFC 0017, RFC 0018, RFC 0025, RFC 0027, and
  RFC 0029.
- Related RFCs: RFC 0008, RFC 0031, RFC 0032, RFC 0034, and RFC 0035.
- This phase adds no runtime feedback application, bounded parallel encoded/source-backed read
  runtime, scheduler requeue, object-store request execution, task execution, data read,
  materialization, write IO, spill IO, policy mutation, large-workload claim, external engine
  fallback, or fallback execution.

## CG-9.0 catalog/table metadata integration gate

- `CatalogMetadataIntegrationGateReport` records the promotion boundary for snapshot/manifest
  metadata reads, catalog table resolution, table metadata reads, partition metadata reads,
  delete/tombstone metadata reads, CDC metadata reads, table-format dependency admission,
  commit/recovery metadata binding, and metadata cache invalidation.
- `cg9-catalog-metadata-gate` exposes stable JSON/text fields for existing report-only
  table-intelligence and catalog-ref skeleton evidence, blocked metadata-integration surfaces,
  required catalog/snapshot/schema/partition/delete/dependency/credential/effect/materialization
  evidence, certificate blockers, side-effect boundaries, claim blockers, and no-fallback status.
- Existing `TableIntelligenceReport`, schema/partition/delete/table compatibility, CDC/layout/
  compaction planning, and `CatalogRef` skeleton evidence remain report-only evidence. Catalog
  resolution, table metadata reads, external table-format dependency activation, credential
  resolution, metadata-cache runtime, and metadata-integration claims stay blocked until
  workload-scoped correctness, benchmark, execution-certificate, Native I/O, policy, dependency,
  and no-fallback evidence exists.
- Primary RFC linkage: RFC 0004, RFC 0017, RFC 0019, RFC 0020, RFC 0025, and RFC 0028.
- Related RFCs: RFC 0008, RFC 0012, RFC 0016, RFC 0024, RFC 0029, RFC 0031, RFC 0032, RFC 0033,
  and RFC 0036.
- This phase adds no catalog IO, table metadata IO, object-store IO, data reads, writes,
  credential resolution, table-format dependency activation, metadata-cache runtime,
  metadata-integration claim, external engine fallback, or fallback execution.

## CG-9.1 schema evolution compatibility evidence

- `SchemaEvolutionCompatibilityReport` records compatibility level, safe/unsafe change counts,
  field-id requirements, projection/cast/default requirements, metadata-loss reporting, read/write
  support, no-IO fields, diagnostics, and fallback-disabled evidence.
- `evaluate_schema_evolution_compatibility` compares typed schema definitions for add, drop, rename,
  safe widening, narrowing, nullability, field-identity, and metadata changes without touching
  catalogs, data files, object stores, writes, or execution paths.
- Safe rename evidence requires stable field IDs; possible renames without field IDs are rejected
  deterministically with no fallback attempted.
- `schema-plan evolution` surfaces representative add-nullable, rename-with-id, rename-without-id,
  drop-field, widen, and narrow scenarios for human and agent-facing compatibility evidence.
- Primary RFC linkage: RFC 0020 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0019, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no catalog access, table metadata reads, object-store IO, data reads, writes,
  commits, external table-format implementation, partition evolution, delete/tombstone execution,
  CDC execution, layout-health execution, compaction execution, parser work, SQL execution, adapter
  runtime, benchmark claim, production/superiority claim, or fallback behavior.

## CG-9.2 partition evolution compatibility evidence

- `PartitionEvolutionCompatibilityReport` records compatibility level, partition changes,
  preserved/added/dropped/transform/reorder/unsafe counts, partition-router requirements,
  metadata-rewrite requirements, repartition requirements, read/write support, no-IO fields,
  diagnostics, and fallback-disabled evidence.
- `evaluate_partition_evolution_compatibility` compares typed partition specs for add, drop,
  transform-change, reorder, and unknown-transform transitions without touching catalogs, table
  metadata, data files, object stores, writes, repartition execution, or fallback paths.
- Known partition changes surface routing, metadata rewrite, or repartition requirements instead of
  pretending the old and new specs are interchangeable.
- Unknown partition transforms are rejected deterministically with no fallback attempted.
- `table-compat-plan partition-evolution` surfaces representative same, add-field, change-transform,
  drop-field, reorder, and unknown-transform scenarios for human and agent-facing compatibility
  evidence.
- Primary RFC linkage: RFC 0020 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0019, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no catalog access, table metadata reads, object-store IO, data reads, writes,
  commits, external table-format implementation, delete/tombstone execution, CDC execution,
  layout-health execution, compaction execution, parser work, SQL execution, adapter runtime,
  benchmark claim, production/superiority claim, or fallback behavior.

## CG-9.3 delete/tombstone compatibility evidence

- `DeleteTombstoneCompatibilityReport` records source/target delete model, compatibility level,
  preservation flags, native handling requirements, metadata-loss reporting, unsupported/unsafe
  counts, read/write support, no-IO fields, diagnostics, and fallback-disabled evidence.
- `evaluate_delete_tombstone_compatibility` compares declared delete/tombstone models without
  touching catalogs, table metadata, data files, object stores, delete files, tombstone filters,
  writes, or fallback paths.
- Initial compatibility is limited to `none` and `file_level_delete`. Segment tombstones, row-level
  deletes, position deletes, equality deletes, external table metadata, metadata-loss transitions,
  and unknown models are blocked behind explicit native rules.
- `table-compat-plan delete-semantics` surfaces representative none, file-level, file-to-none,
  segment-tombstone, row-level, position-delete, equality-delete, external-table-metadata, and
  unknown scenarios for human and agent-facing compatibility evidence.
- Primary RFC linkage: RFC 0020 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0017, RFC 0019, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no catalog access, table metadata reads, object-store IO, data reads, writes,
  commits, external table-format implementation, delete-file application, tombstone filtering,
  row-delete execution, position-delete execution, equality-delete execution, CDC execution,
  layout-health execution, compaction execution, parser work, SQL execution, adapter runtime,
  benchmark claim, production/superiority claim, or fallback behavior.

## CG-9.4 table compatibility evidence aggregation

- `TableCompatibilityReport` aggregates schema-evolution, partition-evolution, and delete/tombstone
  compatibility reports while retaining side-effect and no-fallback flags.
- Aggregate read/write support is blocked when any nested report has errors or reports unsupported
  behavior.
- Nested diagnostics from schema, partition, and delete/tombstone reports are surfaced through
  `table-compat-plan aggregate`.
- `table-compat-plan aggregate` surfaces representative compatible, schema-blocked,
  partition-blocked, and delete-blocked scenarios for human and agent-facing compatibility evidence.
- Primary RFC linkage: RFC 0020 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0017, RFC 0019, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no catalog access, table metadata reads, object-store IO, data reads, writes,
  commits, external table-format implementation, delete-file application, tombstone filtering,
  row-delete execution, position-delete execution, equality-delete execution, CDC execution,
  layout-health execution, compaction execution, parser work, SQL execution, adapter runtime,
  benchmark claim, production/superiority claim, or fallback behavior.

## CG-9.5 CDC incremental planning evidence

- `CdcIncrementalPlanningReport` records declared `ChangeSet`, incremental-plan, CDC event, status,
  count, requirement, diagnostic, side-effect, and no-fallback evidence.
- `evaluate_cdc_incremental_planning` routes append-only and metadata-only CDC summaries as
  plan-only evidence when a source/target snapshot pair exists.
- Updates, deletes, tombstones, schema changes, partition changes, unknown events, unknown segment
  changes, and missing snapshot pairs are rejected until native compatibility evidence exists.
- `incremental-plan cdc` surfaces representative append-only, metadata-only, delete, upsert,
  schema-change, partition-change, missing-from-snapshot, and unknown scenarios for human and
  agent-facing planning evidence.
- Primary RFC linkage: RFC 0004, RFC 0020, and RFC 0025.
- Related RFCs: RFC 0012, RFC 0015, RFC 0017, RFC 0019, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no catalog access, table metadata reads, object-store IO, data reads, writes,
  commits, external table-format implementation, delete-file application, tombstone filtering,
  row-delete execution, position-delete execution, equality-delete execution, CDC execution,
  layout-health execution, compaction execution, parser work, SQL execution, adapter runtime,
  benchmark claim, production/superiority claim, or fallback behavior.

## CG-9.6 layout health planning evidence

- `LayoutHealthReport` records declared manifest, policy, issue, status, count, requirement,
  diagnostic, side-effect, compaction-execution-disabled, and no-fallback evidence.
- `evaluate_layout_health` detects small files, small segments, missing statistics, missing byte
  ranges, mixed formats, mixed encodings, mixed layouts, and non-native data-file evidence from
  already-declared manifest metadata.
- `layout-health-plan` surfaces representative healthy, small-files, missing-stats, mixed-layout,
  and empty scenarios for human and agent-facing planning evidence.
- Empty manifests are rejected; compaction recommendations are planning evidence only and do not run
  maintenance or writes.
- Primary RFC linkage: RFC 0016, RFC 0020, and RFC 0025.
- Related RFCs: RFC 0004, RFC 0008, RFC 0012, RFC 0014, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and
  RFC 0032.
- This phase adds no layout-reader construction, catalog access, table metadata reads, object-store
  IO, data reads, writes, commits, external table-format implementation, delete-file application,
  tombstone filtering, row-delete execution, position-delete execution, equality-delete execution,
  CDC execution, compaction execution, parser work, SQL execution, adapter runtime, benchmark claim,
  production/superiority claim, or fallback behavior.

## CG-9.7 compaction planning evidence

- `CompactionPlanningReport` records layout-health input, policy, status, action, count, blocker,
  estimated group, side-effect, compaction-execution-disabled, and no-fallback evidence.
- `evaluate_compaction_planning` consumes declared manifest metadata through `LayoutHealthReport`
  and emits future maintenance recommendations only when small-file/small-segment candidates have
  sufficient metadata and layout evidence.
- Missing statistics or byte ranges block recommendation emission behind metadata refresh/index
  requirements.
- Mixed formats, mixed encodings, mixed layouts, and non-native data files block recommendation
  emission behind layout or adapter-fidelity review.
- `compaction-plan` surfaces representative healthy, small-files, missing-stats, mixed-layout, and
  empty scenarios for human and agent-facing planning evidence.
- Empty manifests are rejected; recommendations are not executable tasks, write intents, commit
  intents, catalog updates, object-store operations, or compaction execution.
- Primary RFC linkage: RFC 0016, RFC 0020, and RFC 0025.
- Related RFCs: RFC 0004, RFC 0008, RFC 0012, RFC 0014, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and
  RFC 0032.
- This phase adds no layout-reader construction, catalog access, table metadata reads, object-store
  IO, data reads, writes, commits, external table-format implementation, delete-file application,
  tombstone filtering, row-delete execution, position-delete execution, equality-delete execution,
  CDC execution, compaction execution, parser work, SQL execution, adapter runtime, benchmark claim,
  production/superiority claim, or fallback behavior.

## CG-10.1 object-store range planning evidence

- `ObjectStoreRangePlanningReport` records declared manifest, policy, status, request-shape, count,
  blocker, estimated byte, side-effect, full-file-read-disallowed, object-store-IO-disabled, and
  no-fallback evidence.
- `plan_object_store_ranges` emits request shapes only from already-declared S3/GCS/ADLS segment
  byte ranges.
- Empty manifests, local/non-object-store inputs, missing byte ranges, invalid ranges, and oversized
  ranges are blocked with deterministic diagnostics.
- Missing byte ranges do not silently degrade into full-file reads; `full_file_read_allowed=false`
  remains explicit.
- `object-store-range-plan` surfaces representative s3-ranges, missing-ranges, local-file,
  invalid-range, oversized-range, and empty scenarios for human and agent-facing planning evidence.
- Primary RFC linkage: RFC 0008 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0014, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no object-store IO, file IO, data reads, row reads, decode/materialization, Arrow
  conversion, request execution, retry execution, network probing, writes, commits, distributed
  execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority
  claim, or fallback behavior.

## CG-10.2 object-store request coalescing evidence

- `ObjectStoreRequestCoalescingReport` records uncoalesced and coalesced range reports, decisions,
  status, request reduction, estimated bytes, side-effect, object-store-IO-disabled, and no-fallback
  evidence.
- `plan_object_store_request_coalescing` compares request-shape plans without executing reads,
  retries, provider probes, or network calls.
- Coalescing is blocked whenever range planning is blocked by missing byte ranges, invalid ranges,
  request-budget violations, or non-object-store input evidence.
- `object-store-coalesce-plan` surfaces representative s3-ranges and missing-ranges scenarios for
  human and agent-facing planning evidence.
- Request reduction is planning evidence only, not a benchmark, latency, or superiority claim.
- Primary RFC linkage: RFC 0008 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0014, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no object-store IO, file IO, data reads, row reads, decode/materialization, Arrow
  conversion, request execution, retry execution, network probing, writes, commits, distributed
  execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority
  claim, or fallback behavior.

## CG-10.3 object-store commit protocol planning evidence

- `ObjectStoreCommitProtocolReport` records declared commit-protocol input, status, diagnostics,
  object-store target status, unmet readiness evidence, no-IO/no-write side-effect flags, and
  no-fallback evidence.
- `plan_object_store_commit_protocol` validates declared staging prefix, manifest pointer update,
  commit record, idempotency key, cleanup plan, and atomicity evidence without executing commits or
  contacting storage.
- Non-object-store targets, missing staging, missing manifest pointer evidence, missing commit
  record, missing idempotency, missing cleanup, and missing atomicity evidence are blocked with
  deterministic diagnostics.
- `object-store-commit-plan` surfaces representative ready, missing-staging, missing-idempotency,
  missing-atomicity, and local-file scenarios for human and agent-facing planning evidence.
- Commit protocol readiness is planning evidence only; object-store writes, provider-specific
  atomicity, recovery cleanup, and distributed commit coordination remain separate gates.
- Primary RFC linkage: RFC 0008 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0014, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and RFC 0032.
- This phase adds no object-store IO, file IO, data reads, row reads, decode/materialization, Arrow
  conversion, request execution, retry execution, network probing, writes, commit execution, cleanup
  execution, distributed execution, parser work, SQL execution, adapter runtime, benchmark claim,
  production/superiority claim, or fallback behavior.

## CG-10.4 object-store distributed scheduling planning evidence

- `ObjectStoreDistributedSchedulingReport` records request coalescing input, scheduling policy,
  status, task-shape plans, diagnostics, task counts, retry/checkpoint/idempotency requirements, no
  coordinator/worker/task-execution flags, and no-fallback evidence.
- `plan_object_store_distributed_scheduling` groups successful coalesced object-store requests into
  stable task ids without starting a coordinator, starting workers, executing tasks, or contacting
  storage.
- Blocked coalescing, empty requests, task-budget overflow, and invalid policy limits are rejected
  with deterministic diagnostics.
- `object-store-schedule-plan` surfaces representative s3-ranges, multi-task, missing-ranges,
  task-budget, and invalid-policy scenarios for human and agent-facing planning evidence.
- Scheduling evidence records checkpoint/retry/idempotency requirements, but the actual
  checkpoint/retry/idempotency readiness gate remains separate before distributed execution can be
  considered.
- Primary RFC linkage: RFC 0008 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0014, RFC 0016, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and
  RFC 0032.
- This phase adds no object-store IO, file IO, data reads, row reads, decode/materialization, Arrow
  conversion, request execution, retry execution, checkpoint writes, network probing, writes, commit
  execution, cleanup execution, coordinator runtime, worker runtime, task execution, parser work,
  SQL execution, adapter runtime, benchmark claim, production/superiority claim, or fallback
  behavior.

## CG-10.5 object-store checkpoint/retry/idempotency planning evidence

- `ObjectStoreCheckpointRetryReport` records distributed scheduling input, reliability evidence
  flags, status, diagnostics, task counts, retryable task counts, planned checkpoint/attempt record
  counts, no retry/checkpoint/cleanup execution flags, and no-fallback evidence.
- `plan_object_store_checkpoint_retry` requires successful distributed scheduling plus declared
  retry policy, checkpoint plan, idempotency keys, attempt records, and cleanup policy before
  readiness.
- Blocked scheduling, missing retry policy, missing checkpoint plan, missing idempotency keys,
  missing attempt records, and missing cleanup policy are rejected with deterministic diagnostics.
- `object-store-checkpoint-retry-plan` surfaces representative ready, missing-retry,
  missing-checkpoint, missing-idempotency, missing-attempt, missing-cleanup, and blocked-scheduling
  scenarios for human and agent-facing planning evidence.
- Checkpoint/retry/idempotency readiness is planning evidence only; retry execution, checkpoint
  writes, attempt record writes, cleanup execution, and distributed runtime remain separate gates.
- Primary RFC linkage: RFC 0008 and RFC 0025.
- Related RFCs: RFC 0004, RFC 0012, RFC 0014, RFC 0016, RFC 0017, RFC 0018, RFC 0028, RFC 0031, and
  RFC 0032.
- This phase adds no object-store IO, file IO, data reads, row reads, decode/materialization, Arrow
  conversion, request execution, retry execution, checkpoint writes, attempt record writes, network
  probing, writes, commit execution, cleanup execution, coordinator runtime, worker runtime, task
  execution, parser work, SQL execution, adapter runtime, benchmark claim, production/superiority
  claim, or fallback behavior.

## CG-10.6 object-store/distributed runtime promotion gate

- `ObjectStoreRuntimePromotionGateReport` records the promotion boundary for object-store
  byte-range reads, request-coalescing runtime, coordinator startup, worker startup, distributed
  task execution, checkpoint writes, retry execution, cleanup execution, object-store commit
  execution, provider credential runtime, and benchmark/certificate closeout.
- `cg10-object-store-runtime-gate` exposes stable JSON/text fields for existing object-store
  request planner, range planning, coalescing, distributed scheduling, checkpoint/retry, and commit
  protocol evidence, blocked runtime surfaces, required provider/request-budget/scheduler/
  reliability/atomicity/credential/benchmark/certificate evidence, side-effect boundaries, runtime
  claim blockers, and no-fallback status.
- Existing object-store request planning surfaces remain report-only evidence. Byte-range reads,
  full-file reads, object-store IO, distributed coordinator/worker startup, task execution,
  checkpoint/retry/cleanup/commit execution, credential resolution, and runtime claims stay blocked
  until workload-scoped correctness, benchmark, execution-certificate, Native I/O, policy,
  provider, and no-fallback evidence exists.
- Primary RFC linkage: RFC 0008, RFC 0014, RFC 0016, RFC 0017, RFC 0018, RFC 0025, and RFC 0028.
- Related RFCs: RFC 0004, RFC 0012, RFC 0029, RFC 0031, RFC 0032, RFC 0034, and RFC 0035.
- This phase adds no byte-range reads, full-file reads, object-store IO, data reads, writes,
  coordinator startup, worker startup, task execution, checkpoint writes, retry execution, cleanup
  execution, object-store commit execution, credential resolution, runtime claim, external engine
  fallback, or fallback execution.
