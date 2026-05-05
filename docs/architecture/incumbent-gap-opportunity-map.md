# Incumbent Gap Opportunity Map

## Summary

ShardLoom is not trying to clone Spark, DataFusion, Arrow, Iceberg, Delta, or Hudi.

ShardLoom should be the Vortex-native, no-fallback, metadata-first engine that explains every decision, avoids work by default, and is safe for humans and LLM agents to operate.

ShardLoom should combine:
- Vortex-native physical efficiency.
- ShardLoom-native planning, runtime, memory policy, and scheduling.
- Deterministic diagnostics.
- Metadata-first execution posture.
- No fallback engines.
- Agent-friendly CLI/API behavior.

## Spark pain points and ShardLoom opportunities

| Pain point | ShardLoom response | Phase/epic owner |
| --- | --- | --- |
| Operational heaviness across cluster setup, JVM/runtime posture, and environment packaging. | Keep early phases local-first and bounded; expose deterministic readiness gates before distributed complexity. | Phase 10A-10C, then Phase 14B; Epic F (Agent Contract Pack). |
| Configuration and tuning burden for memory, shuffle, and adaptive knobs. | Move to opinionated, diagnosable defaults with explicit blockers for missing estimates instead of hidden tuning debt. | Phase 10B and Phase 11A; Epic A (DecisionTrace). |
| Adaptive behavior can be powerful but hard to reason about. | Emit DecisionTrace/WhyReport for adaptive, memory, scheduler, pruning, and execution decisions. | Early hooks in Phase 9D; broader rollout from Phase 10+. |
| Shuffle, memory, and spill surprises. | Enforce readiness checks, reservation-aware scheduling, spill-required blockers, and deterministic failure paths. | Phase 10B-11A; Epic A + Epic I. |
| Late failures after expensive work is already started. | Shift failure left via deterministic blockers before touching data when contracts are unmet. | Phase 9D-11B; Epic A + Epic F. |
| Object-store execution complexity (requests, ranges, latency, retries, consistency). | Introduce Object Store Request Planner with request budgets, range coalescing, manifest-first planning, and explicit retry policy. | Planning contracts in Phase 10-11; implementation in Phase 14A; Epic H. |
| Small-file and partition-quality degradation over time. | Add LayoutHealthReport with diagnostics for small files/segments, partition skew, clustering drift, and compaction opportunities. | Phase 13A-13B; Epic C. |
| Dependency/runtime environment complexity. | Preserve lightweight default build and feature-gated optional capabilities, with feature footprint transparency. | Ongoing; Epic D (FeatureFootprintReport). |
| Difficult LLM/agent integration due to unstable text and implicit behavior. | Provide stable JSON output schemas, deterministic codes, and suggested remediation steps. | Phase 10C onward; Epic F. |

## DataFusion pain points and ShardLoom opportunities

Common DataFusion adoption pain is not query expressiveness, but productization burden around runtime, memory policy, diagnostics, and integration layers.

ShardLoom response:
- Keep a ShardLoom-native runtime/task graph and scheduling model.
- Keep ShardLoom-owned memory/OOM/spill policy and explicit readiness gates.
- Provide stable diagnostics and output envelope contracts for CLI/API and agents.
- Preserve Vortex-native execution policy rather than becoming Arrow-first by default.
- Provide an agent-friendly integration layer with deterministic machine-readable output.

Phase/epic ownership:
- Runtime/task graph: Phase 10A-10B.
- Memory/OOM/spill policy: Phase 10B-11A.
- Stable diagnostics/output envelope: Phase 10C + Epic F.
- Vortex-native policy and no-fallback posture: continuous across all phases.

## Arrow pain points and ShardLoom opportunities

Arrow is excellent as an interchange and memory boundary, but it is not itself a complete execution policy, table runtime, or diagnostic contract.

ShardLoom response:
- Use Arrow as an explicit boundary, not as default internal execution substrate.
- Preserve native Vortex as highest-fidelity internal and persistence path.
- Report fidelity and metadata-loss explicitly at translation boundaries.
- Include Arrow conversion avoided and decode avoided in WorkAvoidedReport as available.

Phase/epic ownership:
- Boundary and fidelity reporting foundations: Phase 5 and Phase 12A.
- Work-avoided instrumentation: Phase 9D onward; Epic B.

## Iceberg / Delta / Hudi value props to borrow

Iceberg/Delta/Hudi value props are compatibility/table-management concepts, not fallback execution engines.

| Value prop | ShardLoom-native interpretation | Compatibility relationship | Phase owner |
| --- | --- | --- | --- |
| Snapshots and time travel | Native snapshot-aware planning contracts and deterministic snapshot selection diagnostics. | Compatibility adapters should map foreign snapshot semantics into ShardLoom contracts. | Phase 13A; Epic G. |
| Schema evolution | Typed evolution checks with deterministic compatibility diagnostics. | Read/import compatibility requires explicit mapping and loss reporting. | Phase 13A; Epic G. |
| Schema enforcement | Enforce contracts before execution; reject ambiguous/unsafe reads. | Compatibility schemas are accepted only through explicit adapters. | Phase 13A; Epic G + Epic F. |
| Hidden partitioning | Preserve partition semantics in metadata planning and explain output. | Compatibility partition metadata is mapped, not executed externally. | Phase 13A; Epic G + Epic C. |
| Partition evolution | Version-aware partition planning and diagnostics for mixed layouts. | Compatibility evolution metadata must be normalized. | Phase 13A; Epic G. |
| Manifests/transaction logs | Manifest-first planning and deterministic commit/recovery contracts. | Logs/manifests are interoperability inputs, not execution delegation. | Phase 12B-14A; Epic G + Epic H. |
| CDC/incremental queries | Explicit incremental planning primitives with correctness-first semantics. | CDC models map through adapter contracts with explicit unsupported codes. | Phase 13A; Epic G + Epic I. |
| Upserts/deletes/tombstones | Delete/tombstone semantics modeled explicitly; unknown delete models block execution. | Compatibility delete semantics must be fully understood or blocked. | Phase 13A; Epic G + Epic I. |
| Clustering/compaction | Layout-health-driven planning recommendations first, writes only in write-capable phases. | Compatibility clustering metadata feeds diagnostics where available. | Phase 13B; Epic C + Epic G. |
| Indexing/data skipping | Statistics and metadata pruning with conservative proof rules. | Compatibility index metadata may inform planning with explicit confidence. | Phase 9B onward; Epic B + Epic I. |
| Catalog compatibility | Capability-gated catalog adapters with stable diagnostics. | Catalog integration is explicit and optional. | Phase 13A-14A; Epic D + Epic G. |
| Rollback/recovery | Deterministic rollback and ambiguous-commit recovery protocols. | Compatibility commit models mapped via explicit adapter semantics. | Phase 12B + 11B; Epic G + Epic I. |
| Multi-engine interoperability | Translation contracts and diagnostics without fallback execution. | Interop is import/export compatibility, not engine delegation. | Phase 5/12/13; Epic F + Epic G. |

Guardrails:
- Delete/tombstone semantics must never be silently ignored.
- Unknown delete models must block execution.
- Metadata loss must be reported.
- Default build must stay lightweight.

## LLM / API / embedding / vector integration pain points

Effectful integrations introduce correctness and operations risks:
- UDF/API/LLM/embedding/vector calls are effectful.
- Cost/latency/privacy/retry behavior can dominate pipeline safety.
- Non-determinism and cache policy must be explicit.
- Redaction, approvals, and credential scope must be machine-checkable.
- Agents need deterministic dry-run estimates before execution.

ShardLoom response:
- EffectBudgetReport for estimated side effects, cost envelope, and policy gates.
- Effect-level classification in plan/explain/estimate output.
- Dry-run estimates and explicit enablement contracts.
- Credential-scope declarations and audit records.
- Stable JSON output for agent tooling.

Phase/epic ownership:
- Foundations in Phase 2 and Phase 6 contracts.
- Deeper implementation in later modular extensibility phases.
- Primary epic owner: Epic E (EffectBudgetReport), with Epic F support.

## ShardLoom differentiators

- Vortex-native input and output.
- No fallback engines.
- Metadata-first execution.
- Encoded predicates with late materialization posture.
- Deterministic readiness gates.
- WorkAvoided reporting.
- Decision traces / WhyReport visibility.
- Side-effect flags across planning/execution surfaces.
- Feature-gated dependency footprint.
- Agent-friendly stable JSON outputs.
- Safe failure before touching data when contracts are unmet.

## Cross-cutting epics

### Epic A — DecisionTrace / WhyReport
- Causal explanations for pruning, sizing, scheduling, memory, spill, execution, and output decisions.
- Should answer: "Why did ShardLoom do this?"

### Epic B — WorkAvoidedReport
- Quantify segments/rows/bytes/decode/materialization/object-store/spill avoided.
- Must be visible in CLI and JSON outputs.

### Epic C — LayoutHealthReport
- Detect small files, small segments, overpartitioning, underpartitioning, stale clustering, and compaction opportunities.
- Supports lakehouse value props without fallback execution.

### Epic D — FeatureFootprintReport
- Show compiled features, enabled adapters, Vortex gates, object-store/write gates, and fallback-engine absence.
- Supports doctor/capabilities workflows.

### Epic E — EffectBudgetReport
- Track API/LLM/embedding/vector side effects, estimated cost, approvals, caching, redaction, and retry policy.

### Epic F — Agent Contract Pack
- Stable JSON schemas, suggested next steps, examples, deterministic diagnostic codes, and repo integration templates.

### Epic G — Table Intelligence Layer
- Snapshots, partition evolution, schema evolution, deletes/tombstones, and CDC/incremental planning.

### Epic H — Object Store Request Planner
- Request/range budgets, coalescing, manifest-first planning, and retry/latency policy.

### Epic I — Correctness and Differential Harness
- Differential tests, fuzzing, golden metadata/probe fixtures, and semantic edge cases.

### Epic J — Benchmark and Competitive Claims
- Compare against Spark/DataFusion/Polars/DuckDB only as benchmark oracles, never as fallback execution.
- Track work avoided and cost avoided with reproducible methodology.

## Do not do

- Do not add fallback engines.
- Do not make Arrow the default internal execution path.
- Do not hide metadata/fidelity loss.
- Do not silently ignore deletes/tombstones.
- Do not broaden the default dependency graph.
- Do not execute effectful inputs without explicit enablement.
- Do not implement object-store/write/spill execution before the relevant phase.
- Do not claim benchmark wins without reproducible evidence.


### Phase 9D reporting milestone
- `WorkAvoidedReport` first concrete implementation lands in Phase 9D.
- `DecisionTrace` first concrete implementation lands in Phase 9D.
- This is the first user-visible "why" and "work avoided" report for query primitives.


### Phase 10A local execution loop skeleton starts
- `ShardLoom` introduces a first engine-loop-shaped local path for `Vortex` query primitives.
- Initial loop behavior is metadata/no-op only and side-effect free.
- Encoded reads remain deferred in this phase; no scan/decode/materialization/write/object-store/spill/fallback execution occurs.


## Phase 10B bounded scheduling

Memory-safe bounded scheduling starts in Phase 10B and translates memory/parallelism contracts into deterministic execute/defer/block decisions.


## Phase 10C local engine surface
- `vortex-run` introduces a user-facing local engine command/API.
- The command remains no-fallback and side-effect-safe.


## Spill progression

Spill support should become real in phases: first lifecycle/cleanup contracts, then memory reservation integration, then spill data movement.

## Epic coverage across phases

ShardLoom is using epics A–K as cross-cutting product/engine guardrails across the full phased roadmap.

Spark/DataFusion/Arrow/lakehouse gaps are not single-phase concerns; they recur across planning, runtime, memory/spill, diagnostics, output, and compatibility phases.

Spill/OOM pain points now have lifecycle, reservation, synthetic payload, and CLI coverage in phased form.

The remaining spill gap is bounded execution integration and recovery, starting with Phase 11A.3b and continuing into Phase 11B. Phase 11B begins addressing Spark-like spill/recovery/OOM pain by surfacing cleanup requirements before retry. Phase 11B.2 further addresses late-failure pain by reporting retry/cancellation eligibility before executing anything. Phase 11B.3c now exposes controlled cleanup execution for known synthetic artifacts through CLI/docs integration, improving cleanup safety without broad retry/commit recovery execution yet. Phase 11B.4b adds a deterministic `retry-gate-plan` CLI for humans and agents before any retry execution exists. Phase 11B.5c adds a deterministic `cancellation-gate-plan` CLI for humans and agents before any cancellation execution exists. Phase 11B.6 closes the recovery/spill planning loop with a phase-boundary audit that preserves no-execution, no-object-store-write, and no-fallback contracts. Phase 12 begins write/commit value props only through staged, recoverable, no-fallback contracts.


- Phase 11A.3b connects dynamic work shaping to synthetic spill payload capability while keeping object-store writes, output dataset writes, and fallback execution disabled.


## Phase 12 kickoff

Phase 12 begins native `Vortex` write/commit value propositions with safe write-intent planning before any execution behavior.

## Phase 12A staged output closeout gap note

- Phase 12A establishes safe staged write-readiness artifacts before commit protocol.
- Remaining gap before write-value claims: commit protocol, manifest finalization, and output payload writing.


## Competitive engine target

- The product goal is to beat Spark, Polars, DataFusion, and Arrow-adjacent execution stacks for Vortex-native lakehouse workloads.
- ShardLoom’s wedge is Vortex-native fidelity, less decode/materialization, explicit memory/spill/recovery behavior, safer staged writes/commits, deterministic diagnostics, and measured performance wins.
- Baseline engines may be used only as external correctness/benchmark references, not runtime dependencies, fallback engines, or runtime delegation targets.
- Superiority claims require CG-5 (correctness/differential) and CG-6 (benchmark) gates.
- The architecture is promising, but performance claims must wait for real encoded reads, real query execution, real output payload writes, correctness, and benchmarks.

### Spark gaps ShardLoom targets

- Reduce avoidable decode/materialization for Vortex-native workloads.
- Make memory/spill/recovery behavior explicit and diagnosable.
- Preserve deterministic readiness and failure contracts.

### Polars gaps ShardLoom targets

- Improve encoded-first planning and zero-decode posture for Vortex-native execution.
- Keep staged write/commit semantics explicit and recoverable.
- Keep deterministic machine-readable diagnostics for agent workflows.

### DataFusion gaps ShardLoom targets

- Keep ShardLoom-native runtime, memory, and spill policy without fallback delegation.
- Keep Arrow conversion explicit at boundaries rather than default execution.
- Improve explicit unsupported-path diagnostics and capability gating.

### Arrow-adjacent gaps ShardLoom targets

- Treat Arrow as an interoperability boundary, not internal default execution.
- Preserve Vortex-native fidelity and metadata richness end-to-end.
- Measure and report decode/materialization/work avoided with reproducible benchmarks.

## Phase 12B closeout status

- Phase 12B now closes local staged commit-marker readiness.
- Remaining competitive gaps before write claims are manifest finalization, actual output payload writes, real encoded reads, correctness, and benchmarks.


- Phase 12B now includes a local commit execution gate contract, but it intentionally blocks on missing output payload readiness.
- This keeps CG-3 (output payload write path) and CG-4 (commit protocol execution) explicit and deferred.
