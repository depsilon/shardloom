# ShardLoom Phased Execution Plan

## Current status

- Production-grade engine completion estimate: approximately 37–38%.
- Architecture/control-plane completion estimate: approximately 97%.
- CG implementation paused for roadmap/docs/refactor synchronization.
- Last completed CG implementation: CG-1.2c metadata probe CLI/docs integration.
- Next implementation after docs sync: CG-1.2d actual feature-gated local metadata/footer IO, if feature/toolchain validates; otherwise continue to contract-level planning.



## Immediate refactor/docs queue before continuing CG implementation

### R0 — Roadmap/docs synchronization
- update current status to CG-1.2c/CG-1.2d boundary
- ensure Competitive Engine Track CG-1 through CG-18 is complete and current
- remove stale Phase 12C.4 current wording
- remove duplicate/stale Phase 12 rows
- align traceability with PRs 169–174
- keep Foundry only under CG-18 optional deployment/comparison

### R1 — Systems learning map
- add systems-learning-map.md
- incorporate Trino/Dask/Ray/DuckDB/Calcite/Acero/Substrait lessons
- no new deps
- no fallback engines

### R2 — Native contract vocabulary amendments
- PushdownProof / PushdownGuarantee
- RuntimeFilterLifecycle
- LoweringTrace
- TaskGranularityPolicy
- IntermediateArtifactRef taxonomy
- ResourceVector / PlacementHint
- RecoveryStrategy distinction
- OperatorProfile / planned-vs-actual profile
- PipelineBreakerKind
- PlanPortabilityReport
- system introspection virtual datasets

### R3 — Existing repo cleanup backlog
- traceability doc dedupe/stale-row cleanup
- CLI usage/name consistency audit
- diagnostics normalization backlog
- terminology consolidation backlog
- feature-footprint/doctor centralization backlog

### R4 — Resume CG implementation
- CG-1.2d actual feature-gated local metadata/footer IO only if feature/toolchain validates
- CG-2.1 real count primitive over actual Vortex data

## Competitive engine target

- ShardLoom’s long-term goal is to beat Spark, Polars, DataFusion, and Arrow-adjacent execution stacks for Vortex-native lakehouse workloads.
- ShardLoom will not do this through fallback or delegation.
- Spark, Polars, DataFusion, DuckDB, Velox, and vortex-datafusion remain disallowed as runtime fallback engines.
- Baselines may be used later for correctness/differential testing and benchmarking only.
- Arrow conversion remains explicit boundary behavior, not the default execution path.

## Competitive engine success gates

1. Real encoded read path
   - CG-1.1a complete
   - CG-1.1b complete
   - CG-1.2a complete
   - CG-1.2a.1 complete
   - CG-1.2a.2 complete
   - CG-1.2b complete
   - CG-1.2b.1 complete
   - CG-1.2c complete
   - CG-1.2d planned
   - feature-gated local encoded read API boundary
   - segment/chunk/byte-range descriptors
   - no broad row materialization
   - no Arrow-default conversion
2. Real query primitive execution over actual Vortex data
   - actual count
   - actual filtered count
   - actual projection
   - actual predicate/filter primitive
   - encoded-first selection vectors
   - decode only when explicitly allowed
3. Actual output payload write path
   - CG-3 placeholder artifact path complete but real Vortex payload path incomplete.
   - local output payload artifact path complete (readiness/scaffolding only)
   - real Vortex output payload path planned
   - native Vortex output fidelity required for CG-3 completion
   - no committed state until commit protocol
   - no object-store writes initially
   - completion rule: CG-3 remains incomplete until ShardLoom can produce a real Vortex-native output payload for at least one supported workload through a feature-gated, no-fallback execution path
   - local placeholder artifacts are readiness artifacts only and must not be counted as actual payload-write completion
   - the real path must be ShardLoom-owned and write either native Vortex payloads or explicitly approved native Vortex-compatible output
   - the real path must not call Spark/DataFusion/Polars/DuckDB/Velox as fallback
   - the real path must not use Arrow conversion as the default execution path
   - competitive claims still require CG-5 correctness and CG-6 benchmark evidence
4. Commit protocol execution
   - local-first
   - feature-gated
   - idempotent
   - recoverable
   - rollback/ambiguous commit reports
   - no object-store commit until later
5. Correctness/differential tests
   - golden Vortex fixtures
   - reference outputs
   - null/nested/dictionary/sparse/run-length/temporal edge cases
   - Spark/Polars/DataFusion external baselines only, never fallback
6. Benchmarks
   - runtime
   - peak memory
   - bytes read/written
   - decode avoided
   - materialization avoided
   - segments skipped
   - work avoided
   - spill required/avoided
   - startup latency
   - query runtime
   - write/commit latency
7. Physical operator/kernel layer
   - filter kernel
   - projection kernel
   - count/aggregate kernel
   - metadata/encoded/hybrid execution levels
   - expression evaluation over encoded segments
8. Streaming and parallel adaptive execution
   - streaming encoded batches
   - bounded parallel local execution
   - adaptive split/coalesce
   - dynamic sizing feedback loop
   - backpressure
   - memory/spill-aware scheduler
9. Lakehouse/table intelligence
   - schema evolution
   - partition evolution
   - delete/tombstone semantics
   - CDC/incremental planning
   - layout health
   - compaction planning
10. Object-store and distributed execution
   - object-store range planning
   - request coalescing
   - object-store commit protocol
   - distributed scheduling
   - checkpoint/retry/idempotency
11. Python/API surface later
   - thin Python wrapper over CLI JSON first
   - Foundry-friendly later
   - no PyO3/maturin unless explicitly approved
   - no Spark fallback
12. Plan portability / semantic IR
   - native-first plan portability reports
   - explicit unsupported/lossy/residual construct reporting
   - no import/export execution side effects
13. Encoded-native compressed execution
   - encoding-aware execution path selection
   - decode-avoided proof/report requirements
14. Runtime-adaptive optimizer and execution memory
   - adaptive decisions with deterministic diagnostics
   - bounded-memory-safe adaptation boundaries
15. CPU operator specialization
   - commodity CPU vectorized specialization is first-class
   - no external engine fallback for specialization
16. Evidence-first execution certificates
   - plan/input/output evidence artifacts for reproducibility
   - deterministic, machine-readable certificate surfaces
17. Stateful result reuse / incremental execution
   - typed cache/reuse boundaries
   - explicit invalidation and correctness proof signals
18. Universal import/deployment/baseline harness
   - universal runner contracts and portability checks
   - external baseline harnesses are comparison-only
   - Foundry optional deployment/comparison example only

## Cross-cutting epic legend

**Epic A — DecisionTrace / WhyReport**  
Purpose: explain every planner/runtime/memory/spill/scheduler/write/object-store decision.

**Epic B — WorkAvoidedReport**  
Purpose: quantify avoided rows, bytes, segments, decode, materialization, object-store requests, spill, and fallback.

**Epic C — LayoutHealthReport**  
Purpose: detect small files, small segments, overpartitioning, underpartitioning, clustering/compaction opportunities.

**Epic D — FeatureFootprintReport**  
Purpose: show compiled features, enabled adapters, Vortex gates, object-store/write/spill gates, fallback-engine absence.

**Epic E — EffectBudgetReport**  
Purpose: track API/LLM/embedding/vector side effects, estimated cost, approvals, caching, redaction, retry policy.

**Epic F — Agent Contract Pack**  
Purpose: stable JSON schemas, diagnostic codes, suggested next steps, examples, deterministic command contracts.

**Epic G — Table Intelligence Layer**  
Purpose: snapshots, schema evolution, partition evolution, deletes/tombstones, CDC, catalog compatibility.

**Epic H — Object Store Request Planner**  
Purpose: range budgets, request coalescing, manifest-first planning, retry/latency policy.

**Epic I — Correctness and Differential Harness**  
Purpose: fuzzing, edge cases, golden fixtures, semantic differential checks.

**Epic J — Benchmark and Competitive Claims**  
Purpose: compare against Spark/DataFusion/Polars/DuckDB as benchmarks only, never fallback.

**Epic K — Dynamic Work Shaping**  
Purpose: adaptive split/coalesce, bounded parallelism, streaming/materialization choice, memory/spill reservation, object-store range shaping, worker partitioning.

## Full phase checklist

### Phase 0 — Project setup, licensing, naming, repo foundation — Complete

Checklist:
- Repository/workspace foundation complete.
- Apache-2.0 posture complete.
- Governance/contribution/security posture seeded.
- Initial command surface seeded.

Epic coverage:
- Epic F — Agent Contract Pack: seeded by command/diagnostic expectations.
- Epic D — FeatureFootprintReport: seeded by workspace/feature posture.

Missed or weak coverage:
- No dedicated feature-footprint command yet.
- No full agent contract schema pack yet.

Next obligations:
- Preserve license/governance/docs stability as new crates/features are added.

### Phase 1 — RFCs, skills, architecture docs, no-fallback policy — Complete

Checklist:
- RFCs 0001–0024 present.
- Skills docs present.
- No-fallback policy established.
- Vortex-native posture established.

Epic coverage:
- Epic A — DecisionTrace / WhyReport: concept introduced.
- Epic B — WorkAvoidedReport: concept introduced.
- Epic F — Agent Contract Pack: CLI/diagnostic expectations seeded.
- Epic I — Correctness and Differential Harness: correctness RFC seeded.

Missed or weak coverage:
- RFC adherence is mapped now, but not automatically enforced.
- No automated RFC acceptance checker.

Next obligations:
- Use rfc-phase-traceability.md at phase boundaries, not every PR.

### Phase 2 — Core domain contracts — Complete

Checklist:
- Diagnostics.
- Output envelope.
- Dataset/source/format contracts.
- Encoded metadata/statistics.
- Schema/security/release/extension foundations.
- Universal input/output contracts.

Epic coverage:
- Epic D — FeatureFootprintReport: capability/doctor foundation.
- Epic E — EffectBudgetReport: effect/security foundations.
- Epic F — Agent Contract Pack: diagnostics/output foundation.
- Epic G — Table Intelligence Layer: schema/catalog seeds.

Missed or weak coverage:
- FeatureFootprintReport is not a full command/report yet.
- EffectBudgetReport not concrete yet beyond foundations.

Next obligations:
- Keep stable diagnostic and output fields as CLI grows.

### Phase 3 — Plan/runtime skeletons — Complete

Checklist:
- Scan/explain/estimate skeletons.
- Runtime task graph skeletons.
- Adaptive sizing primitives.
- Streaming/materialization boundary primitives.
- Parallelism/resource budget primitives.
- Memory/OOM/spill skeletons.
- Recovery/cancellation skeletons.

Epic coverage:
- Epic A — DecisionTrace: explain/estimate foundation.
- Epic K — Dynamic Work Shaping: adaptive sizing, parallelism, streaming, materialization boundary primitives seeded.
- Epic I — Correctness and Differential Harness: plan-only/no-side-effect tests seeded.

Missed or weak coverage:
- Dynamic Work Shaping is present, but not yet fully runtime-active.
- Streaming is modeled, not deeply executed.

Next obligations:
- Ensure Phase 10–14 turn dynamic sizing/streaming/parallelism from models into real behavior incrementally.

### Phase 4 — Vortex adapter foundation — Complete

Checklist:
- Upstream Vortex feature gates.
- Public API inventory.
- Adapter readiness.
- DType/encoding/layout/statistics probes.
- Metadata summary/planning/pruning foundations.

Epic coverage:
- Epic D — FeatureFootprintReport: Vortex feature posture seeded.
- Epic B — WorkAvoidedReport: segment pruning counters seeded.
- Epic A — DecisionTrace: adapter blockers/diagnostics seeded.
- Epic H — Object Store Request Planner: explicitly deferred.

Missed or weak coverage:
- Hidden/bidi warnings have appeared repeatedly; future docs PRs should scan them.
- Feature footprint still scattered across commands/docs.

Next obligations:
- Preserve default lightweight build and feature-gated upstream Vortex graph.

### Phase 5 — Universal input/output contracts and Vortex planning chain — Complete

Checklist:
- Universal input source/adapter contract.
- Input planning bridge.
- Native Vortex input bridge.
- Output/fidelity/translation contracts.
- Vortex metadata/read/runtime bridge chain.

Epic coverage:
- Epic F — Agent Contract Pack: CLI/JSON expectations expanded.
- Epic G — Table Intelligence Layer: schema/catalog compatibility foundation.
- Epic K — Dynamic Work Shaping: Vortex planning chain starts shaping tasks.

Missed or weak coverage:
- Universal adapters remain mostly contracts.
- Output fidelity reporting exists but native write is not real yet.

Next obligations:
- Do not turn compatibility inputs into fallback engines.

### Phase 6 — Execution gates and executor skeletons — Complete

Checklist:
- Execution readiness gate.
- Dry-run contract.
- Metadata-only executor skeleton.
- Encoded-read readiness contract.
- Encoded-read executor skeleton.
- Side-effect flags preserved.

Epic coverage:
- Epic A — DecisionTrace: readiness/gate reasons seeded.
- Epic E — EffectBudgetReport: side-effect flags seeded.
- Epic F — Agent Contract Pack: dry-run/feature-disabled outputs.
- Epic D — FeatureFootprintReport: executor feature gates exposed.

Missed or weak coverage:
- EffectBudgetReport still not concrete for API/LLM/vector effects.
- Feature footprint still not centralized.

Next obligations:
- All future executor paths must pass readiness/probe gates.

### Phase 7A — Encoded-read probe plan contract — Complete

Checklist:
- Encoded-read API boundary.
- Encoded-read probe plan.
- Probe-only CLI path.
- No data read/decode/materialization.

Epic coverage:
- Epic A — DecisionTrace: encoded-read blockers seeded.
- Epic D — FeatureFootprintReport: encoded-read gates documented.
- Epic K — Dynamic Work Shaping: probe candidates classified.

Missed or weak coverage:
- Probe plan does not execute.
- API uncertainty remains documented rather than solved.

Next obligations:
- Keep unsafe public Vortex scan/read APIs blocked unless proven safe.

### Phase 7B — Feature-gated local Vortex metadata-only open — Complete

Checklist:
- vortex-file-io metadata-only open transition.
- Feature-disabled default behavior.
- Local-file-only metadata behavior or deterministic deferral.
- No scan/decode/materialization/write/object-store/fallback.

Epic coverage:
- Epic B — WorkAvoidedReport: row-count metadata enables count primitive.
- Epic H — Object Store Request Planner: object-store explicitly rejected/deferred.
- Epic D — FeatureFootprintReport: vortex-file-io feature status exposed.

Missed or weak coverage:
- Real fixture coverage is limited/deferred where unsafe.
- Object-store remains intentionally unsupported.

Next obligations:
- Keep metadata-open diagnostics preserved through local engine.

### Phase 8 — First controlled encoded-read execution spike — Complete

Checklist:
- vortex-encoded-read-spike feature gate.
- Safe/deferred encoded-read spike reporting.
- No decode/materialization/object-store/write/spill/fallback.

Epic coverage:
- Epic A — DecisionTrace: encoded-read blockers visible.
- Epic D — FeatureFootprintReport: encoded-read-spike feature visibility.
- Epic J — Benchmark and Competitive Claims: explicitly still not ready.

Missed or weak coverage:
- Actual encoded read remains constrained/deferred when unsafe.
- No competitive claims yet.

Next obligations:
- Do not benchmark/claim until meaningful execution exists.

### Phase 9A — Minimal query primitives and metadata count — Complete

Checklist:
- CountAll primitive.
- Metadata-only row-count answer.
- Projection/filter primitive modeling.
- vortex-count CLI.

Epic coverage:
- Epic B — WorkAvoidedReport: metadata-count opportunity seeded.
- Epic F — Agent Contract Pack: query primitive CLI path.
- Epic K — Dynamic Work Shaping: primitives route work by required data level.

Missed or weak coverage:
- WorkAvoidedReport was not concrete until Phase 9D.

Next obligations:
- Continue surfacing work avoided for query answers.

### Phase 9B — Metadata-filtered count / predicate-count primitive — Complete

Checklist:
- CountWhere primitive.
- Tiny predicate grammar.
- Conservative metadata predicate proofs.
- No selectivity guessing.
- vortex-count-where CLI.

Epic coverage:
- Epic B — WorkAvoidedReport: segments/rows avoided should be tracked.
- Epic I — Correctness and Differential Harness: predicate proof edge cases expanded.
- Epic A — DecisionTrace: metadata proof reason paths seeded.

Missed or weak coverage:
- No full differential harness yet.
- Predicate grammar intentionally tiny.

Next obligations:
- Expand correctness coverage as predicate kernel work grows.

### Phase 9C — Encoded predicate/projection primitive — Complete

Checklist:
- Encoded projection planning.
- Encoded predicate planning.
- Filter/project primitive routing.
- vortex-project CLI.
- vortex-filter CLI.
- No broad scan/decode/materialization.

Epic coverage:
- Epic K — Dynamic Work Shaping: encoded-read candidate routing.
- Epic A — DecisionTrace: metadata proof vs encoded-read requirement.
- Epic I — Correctness and Differential Harness: predicate/projection semantic tests.

Missed or weak coverage:
- Encoded predicate execution is not full runtime execution yet.
- No materialization.

Next obligations:
- Keep projection/filter as encoded candidates unless execution gates prove safe.

### Phase 9D — WorkAvoidedReport and DecisionTrace for query primitives — Complete

Checklist:
- VortexQueryDecisionTrace.
- VortexWorkAvoidedReport.
- VortexQueryPrimitiveAnalysisReport.
- vortex-query-trace CLI.

Epic coverage:
- Epic A — DecisionTrace: first concrete implementation.
- Epic B — WorkAvoidedReport: first concrete implementation.
- Epic F — Agent Contract Pack: explainable query primitive output.

Missed or weak coverage:
- Work avoided metrics are conservative; unknown bytes remain unknown.
- Spill/object-store avoided metrics not concrete yet.

Next obligations:
- Extend work-avoided metrics into spill and object-store phases.

### Phase 10A — Local execution loop skeleton — Complete

Checklist:
- Metadata/no-op local execution report.
- Local execution steps.
- Query primitive analysis attached.
- Encoded-read-required paths deferred.

Epic coverage:
- Epic A — DecisionTrace: execution step reasons.
- Epic K — Dynamic Work Shaping: local loop begins consuming query decisions.
- Epic F — Agent Contract Pack: stable execution report shape.

Missed or weak coverage:
- No real encoded reads.
- No real task parallelism.

Next obligations:
- Preserve metadata/no-op behavior as bounded scheduling grows.

### Phase 10B — Memory-safe bounded scheduling — Complete

Checklist:
- Bounded execution policy.
- memory_gb handling.
- max_parallelism handling.
- encoded-read work deferred.
- no row reads/decode/materialization.

Epic coverage:
- Epic K — Dynamic Work Shaping: local bounded scheduling active.
- Epic A — DecisionTrace: scheduling/memory why.
- Epic B — WorkAvoidedReport: memory/materialization avoided context.

Missed or weak coverage:
- Bounded execution does not yet execute parallel data tasks.
- Spill integration was not real yet.

Next obligations:
- Connect spill decisions without allowing unsafe query data spill.

### Phase 10C — Local engine CLI/API surface — Complete

Checklist:
- vortex-run.
- Local engine request/report.
- Wraps query primitive + local execution + bounded execution.
- Stable user-facing local engine surface.

Epic coverage:
- Epic F — Agent Contract Pack: local engine command shape.
- Epic D — FeatureFootprintReport: feature fields appear in reports.
- Epic A — DecisionTrace: surfaced through engine report.

Missed or weak coverage:
- FeatureFootprintReport still not centralized.
- Some specialized commands remain alongside vortex-run.

Next obligations:
- Keep vortex-run machine-readable fields stable.

### Phase 10D — Local engine diagnostic propagation stabilization — Complete

Checklist:
- Preserves VortexMetadataOpenReport context.
- Preserves missing-file/invalid-target/object-store/feature-disabled diagnostics.
- Prevents generic MissingMetadata masking.

Epic coverage:
- Epic A — DecisionTrace: root-cause preservation.
- Epic F — Agent Contract Pack: deterministic diagnostics.
- Epic I — Correctness Harness: diagnostic masking tests.

Missed or weak coverage:
- Diagnostic lineage could be formalized further later.

Next obligations:
- Do not collapse upstream reports into generic statuses.

### Phase 11A.1 — Spill temp-path lifecycle and cleanup contract — Complete

Checklist:
- Spill lifecycle statuses.
- Spill workspace id/path.
- Cleanup plan/action contract.
- spill-lifecycle CLI.
- No spill payload movement.

Epic coverage:
- Epic K — Dynamic Work Shaping: spill lifecycle available.
- Epic A — DecisionTrace: spill lifecycle reasons.
- Epic I — Correctness Harness: cleanup safety tests.

Missed or weak coverage:
- No payload movement yet.
- No query integration.

Next obligations:
- Keep lifecycle separate from payload movement.

### Phase 11A.2a — Spill reservation lifecycle integration visibility — Complete

Checklist:
- Reservation/lifecycle visibility surfaced.
- CLI output fields improved.
- No payload movement.

Epic coverage:
- Epic K — Dynamic Work Shaping: reservation visibility.
- Epic A — DecisionTrace: reservation blockers visible.
- Epic D — FeatureFootprintReport: spill feature states visible.

Missed or weak coverage:
- Visibility did not yet perform actionable reservation planning.

Next obligations:
- Ensure reservation outputs are machine-readable.

### Phase 11A.2b — Actionable spill reservation planning — Complete

Checklist:
- Spill reservation integration report.
- Memory/bounded execution spill helpers.
- spill-reservation-plan CLI.
- Unknown estimates remain unknown.
- No spill payload movement.

Epic coverage:
- Epic K — Dynamic Work Shaping: spill-aware planning active.
- Epic A — DecisionTrace: spill reservation why.
- Epic B — WorkAvoidedReport: future spill avoided/required metrics.

Missed or weak coverage:
- Spill payload movement still synthetic and separate.
- No query spill integration yet.

Next obligations:
- Connect bounded execution to spill payload only after synthetic path is proven.

### Phase 11A.3a.1 — Spill payload core contract, no filesystem — Complete

Checklist:
- SpillPayloadId.
- SpillPayloadRef.
- SyntheticSpillPayload.
- SpillPayloadPlanReport.
- No filesystem IO.

Epic coverage:
- Epic I — Correctness Harness: payload identity/metadata tests.
- Epic K — Dynamic Work Shaping: payload contract foundation.

Missed or weak coverage:
- No filesystem behavior yet.

Next obligations:
- Add filesystem behavior incrementally behind feature gates.

### Phase 11A.3a.2a — Spill payload filesystem feature gate + path/ref contract — Complete

Checklist:
- spill-payload-fs feature.
- SpillPayloadPath.
- SpillPayloadFsRef.
- Fs plan report.
- No filesystem IO yet.

Epic coverage:
- Epic D — FeatureFootprintReport: spill-payload-fs gate visible.
- Epic K — Dynamic Work Shaping: spill payload path contract.

Missed or weak coverage:
- Feature exists but did not yet perform writes/reads.

Next obligations:
- Keep feature-disabled default behavior safe.

### Phase 11A.3a.2b — Feature-gated synthetic spill payload write-only API — Complete

Checklist:
- write_spill_payload.
- Feature-disabled default path.
- Feature-gated synthetic local payload write.
- No read/cleanup/CLI.

Epic coverage:
- Epic K — Dynamic Work Shaping: first payload write primitive.
- Epic I — Correctness Harness: write-only safety tests.
- Epic D — FeatureFootprintReport: write behavior behind gate.

Missed or weak coverage:
- No read verification yet.

Next obligations:
- Only write exact synthetic payload file.

### Phase 11A.3a.2c — Feature-gated synthetic spill payload read/verify API — Complete

Checklist:
- read_spill_payload.
- Exact synthetic payload ref reads.
- Optional length/checksum verification.
- No roundtrip/cleanup/CLI.

Epic coverage:
- Epic K — Dynamic Work Shaping: first payload read primitive.
- Epic I — Correctness Harness: read/verification tests.

Missed or weak coverage:
- No combined roundtrip yet.

Next obligations:
- Ensure read only exact path and preserve checksum diagnostics.

### Phase 11A.3a.2d — Feature-gated synthetic spill payload roundtrip + cleanup API — Complete

Checklist:
- roundtrip_spill_payload.
- write + read + verify composition.
- Optional cleanup of exact payload file.
- No CLI yet.

Epic coverage:
- Epic K — Dynamic Work Shaping: first local payload lifecycle.
- Epic I — Correctness Harness: roundtrip/cleanup tests.
- Epic A — DecisionTrace: spill roundtrip status/reporting.

Missed or weak coverage:
- No CLI exposure yet.
- No bounded execution integration yet.

Next obligations:
- Expose safely via CLI before integrating into engine paths.

### Phase 11A.3a.3 — CLI/docs integration for spill payload roundtrip — Complete

Checklist:
- spill-payload-roundtrip CLI.
- Default feature-disabled safe report.
- Feature-gated synthetic write/read/cleanup.
- Stable envelope fields.

Epic coverage:
- Epic F — Agent Contract Pack: spill payload CLI contract.
- Epic D — FeatureFootprintReport: feature-enabled/disabled surface.
- Epic A — DecisionTrace: spill payload operational reporting.

Missed or weak coverage:
- CLI is for synthetic payload only.
- Not query/Vortex spill.

Next obligations:
- Before Phase 11A.3b, assert synthetic support is not permission to spill query data.

### Phase 11A.3a.4 — All-phase epic coverage and roadmap synchronization — Complete

Checklist:
- Update all-phase checklist.
- Add/refresh epic coverage for every phase.
- Identify missed/weak coverage.
- Add next obligations per phase.
- Scan hidden/bidi control chars.

Epic coverage:
- All epics A–K reviewed across phases.
- Prevents chat-only roadmap drift.

Missed or weak coverage:
- This is docs-only; no runtime behavior.

Next obligations:
- Proceed to Phase 11A.3b after this lands.

### Phase 11A.3b — Bounded execution spill payload integration — Complete (first pass)

Checklist:
- Bounded execution can request synthetic spill payload path.
- Memory pressure can request spill payload reservation.
- Synthetic payload only at first.
- No Vortex/query data spill until explicitly safe.

Epic coverage:
- Epic K — Dynamic Work Shaping: spill-aware execution path.
- Epic A — DecisionTrace: memory pressure → reservation → payload reason chain.
- Epic B — WorkAvoidedReport: spill avoided/required metrics.
- Epic F — Agent Contract Pack: machine-readable bounded spill fields.

Must include:
- reservation_required.
- reservation_status.
- payload_write_allowed.
- payload_written.
- payload_read.
- cleanup_performed.
- spill_data_is_synthetic.
- fallback_execution_allowed=false.

### Phase 11A.3b.1 — Bounded spill status propagation stabilization — Complete

Checklist:
- Preserve nested `SpillPayloadRoundTripReport` status propagation.
- Do not advertise `PayloadRoundTripAvailable` unless synthetic write/read verification succeeds.
- Do not downgrade blocked reservation statuses to `PayloadPlanReady`.
- Keep synthetic spill support distinct from query/`Vortex` data spill.

Epic coverage:
- Epic A — DecisionTrace: nested roundtrip and reservation blockers remain explicit.
- Epic F — Agent Contract Pack: machine-readable status correctness.
- Epic I — Correctness Harness: regression checks for blocked/deferred propagation.

### Phase 11B — Recovery, cancellation, retry — Current

### Phase 11B.1 — Recovery context and cleanup planning integration — Complete

### Phase 11B.2 — Retry/cancellation planning integration — Complete

### Phase 11B.3a — Cleanup execution core contract, no filesystem — Complete

### Phase 11B.3b — Feature-gated cleanup execution for known synthetic artifacts — Complete

### Phase 11B.3c — Cleanup execution CLI/reporting integration — Complete

### Phase 11B.4a.1 — Retry execution gate signal/effect core — Complete

### Phase 11B.4a.2 — Retry gate integration with cancellation/cleanup reports — Complete

### Phase 11B.4b — Retry gate CLI/docs integration — Complete

### Phase 11B.4b.1 — Retry gate CLI argument validation stabilization — Complete

### Phase 11B.5 — Retry/cancellation execution integration — Current

### Phase 11B.5a — Cancellation execution gate core contract (planning/report-only) — Complete

### Phase 11B.5b — Cancellation gate integration with retry/cancellation + cleanup reports — Complete

### Phase 11B.5c — Cancellation gate CLI/docs integration — Complete

### Phase 11B.6 — Recovery phase final audit before Phase 12 writes — Current

Checklist:
- Verify recovery, cleanup, retry, and cancellation contracts remain coherent and non-contradictory.
- Verify machine-readable fields remain stable and deterministic.
- Verify synthetic spill support is not treated as permission to spill query/`Vortex` data.
- Verify object-store recovery and output recovery remain blocked/deferred.
- Verify fallback execution remains disabled and explicitly reported.
- Define Phase 12A entry criteria before native write intent work begins.

Epic coverage:
- Epic A — DecisionTrace: contract and blocker coherence across recovery surfaces.
- Epic F — Agent Contract Pack: stable machine-readable gate/report fields.
- Epic I — Correctness Harness: guardrail checks for no-execution/no-fallback recovery posture.

Must include:
- no retry/cancellation execution.
- no object-store recovery execution.
- no output write behavior.
- no fallback execution.

### Phase 11 closeout criteria

- spill lifecycle/reservation/payload path complete for synthetic artifacts.
- recovery cleanup planning complete.
- retry/cancellation planning complete.
- retry/cancellation gates complete.
- no actual retry execution.
- no object-store recovery.
- no output writes.
- fallback disabled.

### Phase 12A — Native Vortex write intent to staged output — Planned

Checklist:
- Vortex write plan.
- Staged local output.
- Output fidelity verification.
- No object-store write yet.

Epic coverage:
- Epic G — Table Intelligence Layer: commit metadata foundation.
- Epic B — WorkAvoidedReport: native output/fidelity benefits.
- Epic A — DecisionTrace: write/stage why.

Must include:
- No output commit without explicit write intent.
- No metadata loss without report.

### Phase 12B — Commit protocol and recovery — Planned

Checklist:
- Manifest update.
- Commit record.
- Rollback.
- Ambiguous commit recovery.

Epic coverage:
- Epic G — Table Intelligence Layer: snapshots/commit state.
- Epic A — DecisionTrace: write/commit timeline.
- Epic I — Correctness Harness: commit/recovery edge cases.

Must include:
- idempotency.
- cleanup.
- ambiguous commit diagnostics.

### Phase 13A — Lakehouse table intelligence — Planned

Checklist:
- Snapshots/time travel.
- Schema evolution/enforcement.
- Hidden partitioning.
- Partition evolution.
- Delete/tombstone semantics.
- CDC/incremental planning.

Epic coverage:
- Epic G — Table Intelligence Layer: core implementation.
- Epic C — LayoutHealthReport: partition/layout diagnostics.
- Epic I — Correctness Harness: delete/tombstone semantics.

Must include:
- Unknown delete models block execution.
- Metadata loss reported.
- Compatibility formats are not fallback engines.

### Phase 13B — Layout health / clustering / compaction planning — Planned

Checklist:
- Small-file/segment detection.
- Overpartitioning/underpartitioning.
- Clustering hints.
- Compaction recommendations.
- No writes unless Phase 12 path is ready.

Epic coverage:
- Epic C — LayoutHealthReport: full implementation.
- Epic B — WorkAvoidedReport: layout opportunity metrics.
- Epic K — Dynamic Work Shaping: layout-driven split/coalesce.

Must include:
- no compaction writes unless write path is ready.
- recommendations only until commit/write phases are safe.

### Phase 14A — Object-store read planning — Planned

Checklist:
- Object-store capability gate.
- Request budgets.
- Byte-range planning.
- Request coalescing.
- Retry/latency policy.
- No writes.

Epic coverage:
- Epic H — Object Store Request Planner: first implementation.
- Epic A — DecisionTrace: object-store request why.
- Epic B — WorkAvoidedReport: requests/bytes avoided.
- Epic K — Dynamic Work Shaping: range/request shaping.

Must include:
- request budgets.
- no object-store writes.
- no fallback engines.

### Phase 14B — Distributed execution planning — Planned

Checklist:
- Worker/task partitioning.
- Distributed scheduling.
- Checkpointing.
- Object-store-aware recovery.
- No Spark/DataFusion fallback.

Epic coverage:
- Epic A — DecisionTrace: distributed scheduling why.
- Epic J — Benchmark and Competitive Claims: distributed comparisons begin.
- Epic K — Dynamic Work Shaping: worker partitioning.
- Epic I — Correctness Harness: distributed retry/idempotency tests.

Must include:
- coordinator/worker concepts.
- checkpoint/retry/idempotency.
- no fallback execution.

## Epic coverage matrix

| Epic | Covered phases | First concrete implementation | Current status | Weak spots | Next obligation |
| --- | --- | --- | --- | --- | --- |
| Epic A — DecisionTrace / WhyReport | 1, 3-14 | Phase 9D | Active | spill/query integration needs full reason chain. | Phase 11A.3b must explain memory pressure → reservation → synthetic payload path. |
| Epic B — WorkAvoidedReport | 1, 4, 7B, 9A-14 | Phase 9D | Active | spill avoided/required metrics not concrete. | Phase 11A.3b should add spill-required/spill-deferred work metrics if possible. |
| Epic C — LayoutHealthReport | 13A-13B | Planned in Phase 13B | Planned | not started. | Phase 13B. |
| Epic D — FeatureFootprintReport | 0, 2, 4, 6-11A, 14A | Phases 6-7 reporting surfaces | Active | no centralized feature-footprint/doctor report yet. | bounded spill reports and future doctor command should expose spill feature states. |
| Epic E — EffectBudgetReport | 2, 6, 11B+ | Seeded in core contracts | Seeded | no concrete API/LLM/vector budgets yet. | revisit before effectful inputs/extensions. |
| Epic F — Agent Contract Pack | 0-2, 5-14 | Phase 10C local engine surfaces | Active | command schemas are stable but not centrally versioned. | bounded spill report needs stable machine-readable fields. |
| Epic G — Table Intelligence Layer | 2, 5, 12-13 | Planned for Phase 12A/13A | Planned | table intelligence not started. | Phase 12/13. |
| Epic H — Object Store Request Planner | 4, 7B, 14A | Planned for Phase 14A | Deferred | object-store request planner not started. | Phase 14A. |
| Epic I — Correctness and Differential Harness | 1, 3, 9B-14 | Early semantic/test scaffolding pre-9D | Active | no large differential harness yet. | Phase 11A.3b tests must prove synthetic spill is not query data spill. |
| Epic J — Benchmark and Competitive Claims | 8, 14B | Not started | Not ready | no benchmark claims yet. | only after meaningful local/distributed execution. |
| Epic K — Dynamic Work Shaping | 3, 5-14 | Phase 10B bounded scheduling | Active | dynamic sizing/parallelism/spill shaping not fully tied to payload path. | Phase 11A.3b. |


## Phase 12A update

- Phase 11B.6 is complete.
- Phase 12A.1a (write-intent commit-protocol blocker stabilization) is complete.
- Phase 12A.2a (staged output workspace core contract, report-only) is complete.
- Phase 12A.2b.1a (feature gate and setup request/status scaffolding) is complete.
- Phase 12A.2b.1b (staged workspace setup report/helper behavior) is complete.
- Phase 12A.2b.1c (staged workspace setup path/effect correctness) is complete.
- Phase 12A.2b.2 (feature-gated staged-output marker file) is complete.
- Phase 12A.2c.1 (staged output workspace setup CLI/docs integration) is complete.
- Phase 12A.2c.2 (staged marker CLI/docs integration) is current.
- Phase 12A.3 (staged manifest draft/report-only contract) is planned.
## Phase 12A.3a update
- Phase 12A.2c.2 complete.
- Phase 12A.3a current: staged manifest draft core contract (report-only, no filesystem).
- Phase 12A.3b planned: feature-gated local staged manifest draft file.
- Phase 12A.3c planned: CLI/docs integration.
- Actual output payload and file writes remain deferred.

## Phase 12A.3b.1 update
- Phase 12A.3a complete.
- Phase 12A.3b.1 current: staged manifest draft file path/content contract (report-only, no filesystem writes).
- Phase 12A.3b.2 planned.
- Phase 12A.3c planned.

## Phase 12A.3b.2 update

- Phase 12A.3b.1 complete.
- Phase 12A.3b.2a current: staged manifest file write contract (report-only, no filesystem writes).
- Phase 12A.3b.2b planned: feature-gated local draft-file write execution.
- Phase 12A.3c planned.


## Phase 12A.3c update
- Phase 12A.3b.2b complete.
- Phase 12A.3c complete: staged manifest CLI/docs integration.
- Phase 12A.4 complete: staged output/write-readiness closeout before commit protocol.
- Phase 12B.1 complete: commit-intent core contract (report-only) before commit execution.
- Phase 12B.1b complete: commit-intent report integrations for retry/cancellation/recovery inputs.
- Phase 12B.1c complete: commit readiness integration validation closeout.
- Phase 12B.2a.1 complete: commit protocol state machine core contract (report-only, explicit signals only).
- Phase 12B.2a.2 complete: commit protocol report integration with commit-intent reports.
- Phase 12B.2a.3 complete: commit intent/protocol CLI wrappers.
- Phase 12B.2a.4 complete: local staged write-readiness smoke test.
- Phase 12B.3a complete: commit marker core contract (report-only, no filesystem writes).
- Phase 12B.3a.1b complete: competitive roadmap identifier normalization and commit-marker feature-gate readiness requirement.
- Phase 12B.3b complete: feature-gated local commit marker file write, no manifest finalization.
- Phase 12B.3c.1 complete: commit marker planning `CLI`/docs integration.
- Phase 12B.3c.2 complete: commit marker write `CLI` (feature-gated local-only path).
- Phase 12B.3c.3 complete: staged write-readiness smoke test includes the commit marker artifact.
- Phase 12B.4 complete: commit protocol closeout before manifest finalization.
- Phase 12B.5a complete: manifest finalization core contract, no filesystem writes.
- Phase 12B.5b complete: feature-gated local finalized-manifest candidate artifact write (no commit execution).
- Phase 12B.5c complete: manifest finalization CLI/docs integration.
- Phase 12B.6 complete: local commit execution gate (report-only; blocks on output payload readiness).
- Phase 12C.1 complete: actual local output payload write contract (report-only; no filesystem writes).
- Phase 12C.2 complete: feature-gated local output payload artifact write.
- Phase 12C.3a complete: output payload plan CLI (report-only).
- Phase 12C.3b complete: output payload artifact write CLI.
- Phase 12C.4 current: staged smoke test includes output payload planning and placeholder artifact writing.
- Phase 12D planned: commit protocol execution (CG-4).
- Phase 12C planned: output payload write path (CG-3).
- Phase 12D planned: commit protocol execution (CG-4).

Phase 12B.2a.4 note:
- First local staged write-readiness smoke test exercises staged workspace setup, staged marker write, staged manifest draft-file write, commit intent planning, and commit protocol planning.
- This is not output payload writing.
- This is not committed manifest writing.
- This is not upstream `Vortex` write execution.

### Phase 12B.2 entry criteria

- Commit intent core and readiness integration are complete.
- Commit intent remains report-only.
- Staged manifest draft file remains a draft artifact, not committed state.
- Recovery/retry/cancellation blockers are machine-readable.
- Commit protocol state machine must start report-only.
- Commit execution remains deferred.
- Manifest finalization remains deferred.
- Output data writes remain disabled.
- Upstream Vortex write APIs remain deferred.
- Object-store writes remain disabled.
- Fallback execution remains disabled.

### Phase 12A closeout criteria

- Native Vortex write intent exists but does not execute writes.
- Staged workspace setup exists and is feature-gated.
- Staged marker exists and is feature-gated.
- Staged manifest draft file write exists and is feature-gated.
- Staged manifest draft file is not a committed manifest.
- Output payload writes remain disabled.
- Upstream Vortex write APIs remain deferred.
- Object-store writes remain disabled.
- Commit protocol is not implemented yet.
- Fallback execution remains disabled.

### Phase 12B closeout criteria before manifest finalization

- Commit intent planning is complete and report-only.
- Commit protocol state machine is complete and report-only.
- Commit protocol CLI is complete and report-only.
- Commit marker planning is complete.
- Commit marker write is feature-gated and local-only.
- Staged write-readiness smoke test includes commit marker artifact.
- Staged manifest draft files are not committed manifests.
- Commit markers do not finalize manifests.
- Output payload writes remain disabled.
- Upstream Vortex write APIs remain unused.
- Object-store writes remain disabled.
- Fallback execution remains disabled.

Entry criteria for the next implementation phase:

- Manifest finalization must start with report-only contract.
- Manifest finalization must distinguish draft, finalized, and committed states.
- Manifest finalization must not write output payloads.
- Manifest finalization must not call upstream Vortex write APIs unless explicitly feature-gated and approved later.
- Commit execution remains deferred until finalization contract is stable.


## Competitive Engine Track (CG)

### CG-1 — Real encoded read path
- encoded read API boundary
- feature-gated local encoded read fixture
- no broad row materialization
- no Arrow-default conversion
- CG-1.1a encoded read boundary core contract: complete
- CG-1.1b encoded read boundary `CLI`/docs integration: complete
- CG-1.2a feature-gated local encoded-read fixture contract: complete
- CG-1.2b feature-gated local metadata/footer probe: complete
- CG-1.2b.1 metadata probe default no-IO / feature-gate stabilization: complete
- CG-1.2c CLI/docs integration for metadata/footer probe: current
- CG-1.2d feature-gated metadata/footer IO execution path: planned
- CG-2.1 query primitive execution contract: planned

### CG-2 — Real query primitive execution over actual Vortex data
- count
- filtered count
- projection
- predicate/filter primitive
- CG-2.1 initial real query primitive execution: planned

### CG-3 — Actual output payload write path
- local placeholder artifact path complete
- real Vortex output payload path planned

### CG-4 — Commit protocol execution
- local-first commit execution
- idempotency/recovery
- no object-store commit yet

### CG-5 — Correctness/differential tests
- golden Vortex fixtures
- Spark/Polars/DataFusion external baselines only
- no fallback execution

### CG-6 — Benchmarks
- runtime
- peak memory
- bytes read/written
- decode avoided
- materialization avoided
- work avoided

### CG-7 — Physical operator/kernel layer
- commodity CPU vectorized execution contract
- encoded batch/segment operator API
- selection-vector execution
- SIMD/cache-friendly kernels
- no GPU/FPGA requirement
- no fallback engines

### CG-8 — Streaming/parallel/adaptive execution
- streaming encoded batches
- bounded parallel local execution
- adaptive split/coalesce
- backpressure
- memory/spill-aware scheduler

### CG-9 — Lakehouse/table intelligence
- schema evolution
- partition evolution
- delete/tombstone semantics
- CDC/incremental planning
- layout health
- compaction planning

### CG-10 — Object-store/distributed execution
- object-store read planning
- byte-range coalescing
- object-store commit protocol
- distributed scheduling
- checkpoint/retry/idempotency

### CG-11 — Python/API surface later
- thin Python wrapper over CLI JSON first
- stable command schema
- no PyO3/maturin unless explicitly approved
- no Spark fallback

### CG-12 — Plan portability and semantic IR boundary
- ShardLoom plan export contract
- optional Substrait-like export/import validation
- residual unsupported plan reporting
- no external engine execution
- no fallback execution

### CG-13 — Encoded-native compressed execution
- dictionary-aware predicates
- RLE/run-end-aware predicates
- bitpack/bytebool-aware predicates
- FSST/string-encoded predicate path
- ALP/fastlanes numeric path
- direct count/filter/project over encoded segments
- decode-avoided proof/report

### CG-14 — Runtime-adaptive optimizer and execution memory
- runtime filter reordering
- segment-level adaptive pruning
- dynamic split/coalesce from observed throughput
- memory-pressure-aware operator switching
- spill-risk-aware query shaping
- workload profile cache

### CG-15 — Operator specialization and commodity CPU dispatch
- CPU feature detection/report
- SIMD-specialized count/filter/project kernels
- branchless predicate paths
- cacheline-aware segment iteration
- vector-width-specific benchmark reports
- no GPU/FPGA requirement

### CG-16 — Evidence-first execution certificates
- plan hash
- input snapshot/manifest hash
- selected/skipped segments
- bytes read/avoided
- decode/materialization avoided
- side effects performed
- reproducibility metadata

### CG-17 — Stateful result reuse and incremental execution
- segment-result cache
- predicate-result cache
- encoded dictionary/filter cache
- incremental recompute from manifest diffs
- cache invalidation proof

### CG-18 — Universal import, deployment, and external baseline harness
- universal CLI JSON runner contract
- package/import guidance independent of Foundry
- optional Foundry transform examples
- Spark baseline runner, external only
- Polars baseline runner, external only
- DataFusion baseline runner, external only
- stable comparison report dataset
- no runtime fallback



## Competitive Engine Track RFC backing

- RFC 0025 through RFC 0030 now define the expanded Competitive Engine Track.
- CG-1 remains the next implementation path after this docs PR.
- CG-18 is universal import/deployment/baseline harness, with Foundry only as an optional deployment/comparison example.
- Competitive gates are success gates and do not replace canonical implementation phase IDs.
