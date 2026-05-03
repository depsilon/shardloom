# ShardLoom Phased Execution Plan

## Current status

- Production engine completion estimate: approximately 30%.
- Architecture/control-plane completion estimate: approximately 95%.
- Current phase: Phase 11A.3a.2b — feature-gated synthetic local spill payload write-only API.
- Next major phase: Phase 11A.3a.2c — feature-gated synthetic local spill payload read API.

## Phase checklist

### Phase 0 — Project setup, licensing, naming, repo foundation — Complete

Checklist:
- Repository created.
- Apache-2.0 license and NOTICE present.
- Governance, security, contribution docs present.
- Initial RFC process established.

Cross-cutting epics:
- FeatureFootprintReport: not started.
- Agent Contract Pack: seeded by docs, not complete.

### Phase 1 — RFCs, skills, architecture docs, no-fallback policy — Complete

Checklist:
- RFCs 0001–0024 present.
- Skills docs present.
- No-fallback policy established.
- Vortex-native posture established.

Cross-cutting epics:
- DecisionTrace: concept introduced.
- WorkAvoidedReport: concept introduced.
- Agent Contract Pack: CLI/diagnostic expectations seeded.

### Phase 2 — Core domain contracts — Complete

Checklist:
- diagnostics
- output envelope
- dataset/source/format
- encoded metadata/statistics
- schema/security/release/extension foundations
- universal input/output contract

Cross-cutting epics:
- FeatureFootprintReport: capability/doctor foundation.
- EffectBudgetReport: effect/security/credential foundations.
- Agent Contract Pack: diagnostics/output foundation.

### Phase 3 — Plan/runtime skeletons — Complete

Checklist:
- scan/explain/estimate skeletons
- task graph skeletons
- sizing skeletons
- memory/OOM/spill skeletons
- recovery/cancellation skeletons
- plan-only/no-side-effect invariants

Cross-cutting epics:
- DecisionTrace: explain/estimate foundation.
- WorkAvoidedReport: not implemented.
- Memory/OOM trace: planned.

### Phase 4 — Vortex adapter foundation — Complete

Checklist:
- upstream Vortex feature-gating
- public API inventory
- adapter readiness
- DType/encoding/layout/statistics mapping probes
- metadata summary/planning/pruning foundations

Cross-cutting epics:
- WorkAvoidedReport: segment pruning counters seeded.
- FeatureFootprintReport: Vortex feature posture seeded.
- Object Store Request Planner: deferred.

### Phase 5 — Universal input/output contracts and Vortex planning chain — Complete

Checklist:
- universal input source/adapter contract
- input planning bridge
- native Vortex input bridge
- output/fidelity/translation contracts
- Vortex metadata/read/runtime bridge chain

Cross-cutting epics:
- Agent Contract Pack: CLI/json expectations expanded.
- Table Intelligence Layer: schema/catalog compatibility foundation.

### Phase 6 — Execution gates and executor skeletons — Complete

Checklist:
- execution readiness gate
- dry-run contract
- metadata-only executor skeleton
- encoded-read readiness contract
- encoded-read executor skeleton
- side-effect flags preserved

Cross-cutting epics:
- DecisionTrace: gate reasons seeded.
- EffectBudgetReport: side-effect flags seeded.
- WorkAvoidedReport: planned.

### Phase 7A — Encoded-read probe plan contract — Complete

Checklist:
- encoded-read API boundary
- encoded-read probe plan
- probe-only CLI path
- no data read/decode/materialization

Cross-cutting epics:
- DecisionTrace: probe blockers seeded.
- FeatureFootprintReport: encoded-read gates documented.

### Phase 7B — Feature-gated local Vortex metadata-only open — Complete

Checklist:
- vortex-file-io metadata-only open transition
- feature-disabled default behavior
- local-file-only metadata behavior or deterministic deferral
- no scan/decode/materialization/write/object-store/fallback

Cross-cutting epics:
- WorkAvoidedReport: row-count metadata enables count primitive.
- Object Store Request Planner: object-store explicitly rejected/deferred.

### Phase 8 — First controlled encoded-read execution spike — Complete

Checklist:
- vortex-encoded-read-spike feature gate
- safe/deferred encoded-read spike reporting
- no decode/materialization/object-store/write/spill/fallback
- deterministic blocked/deferred behavior when public API path unsafe

Cross-cutting epics:
- DecisionTrace: encoded-read blockers visible.
- Benchmark Claims: still not ready.

### Phase 9A — Minimal query primitives and metadata count — Complete

Checklist:
- CountAll primitive
- metadata-only row-count answer
- projection/filter primitive modeling
- vortex-count CLI

Cross-cutting epics:
- WorkAvoidedReport: should start attaching work avoided to count.
- Agent Contract Pack: query primitive CLI/json path.

### Phase 9B — Metadata-filtered count / predicate-count primitive — Complete

Checklist:
- CountWhere primitive
- tiny predicate grammar
- conservative metadata predicate proofs
- no selectivity guessing
- vortex-count-where CLI

Cross-cutting epics:
- WorkAvoidedReport: segments pruned / rows avoided should be tracked next.
- Correctness Harness: predicate proof edge cases should expand.

### Phase 9C — Encoded predicate/projection primitive — Current

Checklist:
- encoded projection planning
- encoded predicate planning
- filter/project primitive routing
- vortex-project CLI
- vortex-filter CLI
- no broad scan execution
- no decode/materialization

Cross-cutting epics:
- WorkAvoidedReport: add early work-avoided counters for projection/filter.
- DecisionTrace: explain metadata proof vs encoded-read requirement.
- Correctness Harness: predicate/projection semantic tests.

### Phase 9D — WorkAvoidedReport and DecisionTrace for query primitives — Complete

Checklist:
- report segments considered/pruned
- rows counted from metadata
- bytes avoided when known
- decode avoided
- materialization avoided
- reason codes for metadata answer vs encoded-read requirement
- CLI/json output fields

Cross-cutting epics:
- WorkAvoidedReport: first concrete implementation.
- DecisionTrace: first concrete implementation.

### Phase 10A — Local execution loop skeleton — Current

Checklist:
- execute scheduled no-op/metadata tasks
- execute safe encoded-read candidate only if feature and readiness allow
- task status transitions
- no object-store
- no writes
- no spill

Cross-cutting epics:
- DecisionTrace: task state transition reasons.
- EffectBudgetReport: no effectful tasks yet.

### Phase 10B — Memory-safe bounded scheduling — Planned

Checklist:
- concurrency limits
- memory reservations
- reduce parallelism on pressure
- needs-estimate blockers
- no OOM guessing

Cross-cutting epics:
- DecisionTrace: scheduling/memory why.
- WorkAvoidedReport: memory avoided / materialization avoided.

### Phase 10C — Local engine CLI/API surface — Planned

Checklist:
- local run command
- stable JSON result schema
- deterministic errors
- no fallback engines
- local Vortex-only execution

Cross-cutting epics:
- Agent Contract Pack: stable engine command schema.
- FeatureFootprintReport: doctor/capabilities output.

### Phase 11A — Spill policy turns real — Planned

Checklist:
- spill requirement detection
- temporary spill path planning
- spill file lifecycle
- cleanup
- no object-store spill yet

Cross-cutting epics:
- DecisionTrace: spill why.
- Recovery trace: spill cleanup.

### Phase 11B — Recovery, cancellation, retry — Planned

Checklist:
- cancellation propagation
- retry decisions
- partial output cleanup
- ambiguous task/output state handling

Cross-cutting epics:
- DecisionTrace: retry/recovery why.
- Agent Contract Pack: recovery diagnostics.

### Phase 12A — Native Vortex write intent to staged output — Planned

Checklist:
- Vortex write plan
- staged local output
- no object-store write yet
- output fidelity verification

Cross-cutting epics:
- Table Intelligence Layer: commit metadata foundation.
- WorkAvoidedReport: write materialization avoided if native.

### Phase 12B — Commit protocol and recovery — Planned

Checklist:
- manifest update
- commit record
- rollback
- ambiguous commit recovery

Cross-cutting epics:
- Table Intelligence Layer: snapshots.
- Recovery trace: write/commit timeline.

### Phase 13A — Lakehouse table intelligence — Planned

Checklist:
- snapshots/time travel
- schema evolution/enforcement
- hidden partitioning
- partition evolution
- delete/tombstone semantics
- CDC/incremental planning

Cross-cutting epics:
- Table Intelligence Layer: core implementation.
- LayoutHealthReport: partition/layout diagnostics.

### Phase 13B — Layout health / clustering / compaction planning — Planned

Checklist:
- small-file/segment detection
- overpartitioning/underpartitioning
- clustering hints
- compaction recommendations
- no writes unless Phase 12 path ready

Cross-cutting epics:
- LayoutHealthReport: full implementation.
- WorkAvoidedReport: layout opportunity metrics.

### Phase 14A — Object-store read planning — Planned

Checklist:
- object-store capability gate
- request budgets
- range request planning
- request coalescing
- retry/latency policy
- no writes

Cross-cutting epics:
- Object Store Request Planner: first implementation.
- DecisionTrace: object-store request why.

### Phase 14B — Distributed execution planning — Planned

Checklist:
- worker/task partitioning
- checkpointing
- distributed scheduling
- object-store-aware recovery
- no Spark/DataFusion fallback

Cross-cutting epics:
- DecisionTrace: distributed scheduling why.
- Benchmark Claims: distributed comparisons begin.

## Cross-cutting epic tracker

| Epic | Purpose | First phase | Current status | Next concrete action |
| --- | --- | --- | --- | --- |
| DecisionTrace / WhyReport | Explain causal planning/execution decisions. | Phase 1 (concept), Phase 9D (implementation start). | Concept seeded, hooks partial. | Implement query-primitive reason codes in Phase 9D. |
| WorkAvoidedReport | Quantify work avoided across metadata/pruning/execution. | Phase 1 (concept), Phase 9D (implementation start). | Concept seeded; counters partial. | Add segments/rows/decode/materialization avoided fields in CLI/JSON. |
| LayoutHealthReport | Detect and explain layout quality issues and opportunities. | Phase 13B. | Not started. | Define stable report schema and rule set. |
| FeatureFootprintReport | Show compiled/runtime feature footprint and gate posture. | Phase 0-2 foundations. | Foundation only. | Add doctor/capabilities feature inventory output in Phase 10C. |
| EffectBudgetReport | Classify and budget effectful operations. | Phase 2/6 foundations. | Foundation only. | Add dry-run effect budget schema before effectful execution phases. |
| Agent Contract Pack | Deterministic machine-readable agent contract surfaces. | Phase 0-2 foundations. | Seeded, not complete. | Stabilize local engine JSON schema in Phase 10C. |
| Table Intelligence Layer | Snapshot/evolution/delete/CDC semantics in native planning. | Phase 5 foundations, Phase 13A core. | Foundation only. | Implement snapshot + schema evolution planning contracts. |
| Object Store Request Planner | Plan request/range/retry behavior before object-store IO. | Phase 7B deferral, Phase 14A start. | Deferred by policy. | Specify request-budget and coalescing contracts pre-implementation. |
| Correctness and Differential Harness | Protect semantics across encoded/native paths. | Phase 9B expansion. | Partial targeted coverage. | Expand predicate/projection differential fixtures in Phase 9C/9D. |
| Benchmark and Competitive Claims | Reproducible competitive benchmarking and cost/work-avoided claims. | Phase 8 awareness, Phase 14B distributed comparatives. | Not ready for claims. | Prepare reproducible benchmark harness tied to Epic B metrics. |

## Rules

- No fallback engines.
- Upstream Vortex stays feature-gated.
- Default build stays lightweight.
- No Arrow-default path.
- No object-store/write/spill until explicit phase.
- Every phase must preserve diagnostics and side-effect flags.
- Reviews happen at phase boundaries, not after every small PR.
- Missing metadata/statistics/estimates must never be treated as proof.
- Delete/tombstone semantics must never be silently ignored.
- Benchmark claims require evidence.


- Phase 9C: complete.
- Phase 9D: complete (DecisionTrace + WorkAvoidedReport for query primitives).
- Phase 10A: current (local execution loop skeleton; metadata/no-op only).


## Phase 10B update

- Phase 10A is complete.
- Phase 10B is now the current phase, focused on memory-safe bounded scheduling for local execution.


## Phase 10C update
- Phase 10B is complete.
- Phase 10C is current and introduces the local engine CLI/API surface.

## Phase 10 status update

- Phase 10C is complete (`vortex-run` local engine surface landed in PR #90).
- Phase 10D is the current stabilization phase for local engine diagnostic propagation.
- Phase 11A.3a.1 is complete.
- Phase 11A.3a.2a is complete.
- Phase 11A.3a.2b is current.
- Phase 11A.3a.2c is planned.
- Phase 11A.3a.2d is planned.
- Phase 11A.3a.3 is planned.

## RFC traceability

- Reference: `docs/architecture/rfc-phase-traceability.md`.
- Each phase has mapped RFCs.
- Check mapped RFCs before starting a phase.
- Reviews happen at phase boundaries, not after every small PR.
