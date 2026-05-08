# Incumbent Gap Opportunity Map

## Purpose

This document maps incumbent-system pain points into ShardLoom-native design opportunities. Active phase status, active queue placement, and CG closeout decisions live in `docs/architecture/phased-execution-plan.md`.

ShardLoom is not trying to clone Spark, DataFusion, Arrow, Iceberg, Delta, or Hudi. It should be the Vortex-native, no-fallback, metadata-first engine that explains every decision, avoids work by default, and is safe for humans and LLM agents to operate.

## ShardLoom Response Themes

- Vortex-native physical efficiency.
- ShardLoom-native planning, runtime, memory policy, and scheduling.
- Deterministic diagnostics.
- Metadata-first execution posture.
- No fallback engines.
- Agent-friendly CLI/API behavior.

## Spark Gap Map

- Operational heaviness
  - Response: local-first, bounded phases before distributed complexity.
  - Owning roadmap area: runtime skeleton, CG-10, CG-18, deployment readiness.
- Configuration and tuning burden
  - Response: opinionated defaults, memory/spill diagnostics, explicit blockers.
  - Owning roadmap area: CG-14, memory/spill, DecisionTrace.
- Adaptive behavior opacity
  - Response: DecisionTrace and why-reports for pruning, sizing, scheduling, memory, and execution decisions.
  - Owning roadmap area: CG-14, CG-16, observability certification.
- Shuffle, memory, and spill surprises
  - Response: reservation-aware scheduling, spill-required blockers, deterministic fail-before-OOM paths.
  - Owning roadmap area: CG-8, CG-14, memory/spill, correctness harness.
- Late failures
  - Response: fail left through deterministic readiness gates before touching data.
  - Owning roadmap area: CG-5, CG-16, diagnostics.
- Object-store complexity
  - Response: request budgets, range coalescing, manifest-first planning, retry policy.
  - Owning roadmap area: CG-10, object-store runtime.
- Small-file and partition-quality degradation
  - Response: LayoutHealthReport and compaction recommendations before write-side behavior.
  - Owning roadmap area: CG-9, CG-13, CG-17.
- Runtime/dependency environment complexity
  - Response: lightweight default build, explicit feature footprint, no-fallback dependency checks.
  - Owning roadmap area: CG-18, deployment readiness.
- Agent integration friction
  - Response: stable JSON schemas, deterministic diagnostics, suggested remediation steps.
  - Owning roadmap area: CG-11, CG-16, CG-20.

## DataFusion Gap Map

- Productization burden
  - Response: ShardLoom-owned runtime/task graph, scheduling model, diagnostics, and API surface.
- Memory policy burden
  - Response: native memory/OOM/spill policy with explicit readiness gates.
- Arrow-first habits
  - Response: Arrow as boundary/interchange, not default internal execution substrate.
- Extensibility without certification
  - Response: typed UDF/plugin/function/operator reports with no-fallback and materialization evidence.

## Arrow Gap Map

- Interchange can obscure execution policy
  - Response: Arrow is an explicit boundary, not the internal execution substrate.
- Fidelity loss can be hidden
  - Response: translation boundaries report fidelity and metadata loss.
- Decode/copy work can be invisible
  - Response: WorkAvoidedReport should expose decode, conversion, and materialization avoided.

## Lakehouse Value-Prop Map

- Snapshots and time travel
  - Native interpretation: snapshot-aware planning contracts and deterministic snapshot diagnostics.
- Schema evolution and enforcement
  - Native interpretation: typed evolution checks and stable compatibility diagnostics.
- Hidden partitioning and partition evolution
  - Native interpretation: normalized partition descriptors and explicit mixed-layout diagnostics.
- Manifests and transaction logs
  - Native interpretation: manifest-first planning and deterministic commit/recovery contracts.
- CDC, upserts, deletes, and tombstones
  - Native interpretation: explicit change/delete semantics that block when unknown.
- Clustering, compaction, indexing, and data skipping
  - Native interpretation: layout health, segment statistics, and conservative proof rules.
- Catalog compatibility
  - Native interpretation: capability-gated catalog adapters with stable diagnostics.
- Multi-engine interoperability
  - Native interpretation: import/export compatibility, not execution delegation.

## Effectful Workload Map

- UDF/API/LLM/embedding/vector calls declare effect level.
- Cost, latency, privacy, retry, and cache policy are explicit.
- Redaction, approvals, and credential scopes are machine-checkable.
- Agents can dry-run estimates before effectful execution.
- Effectful work never runs during explain, estimate, doctor, capabilities, or report-only planning.

## Cross-Cutting Epic Map

- Epic A - DecisionTrace / WhyReport
  - Causal explanations for pruning, sizing, scheduling, memory, spill, execution, and output decisions.
- Epic B - WorkAvoidedReport
  - Quantify segments, rows, bytes, decode, materialization, object-store requests, and spill avoided.
- Epic C - LayoutHealthReport
  - Detect small files, small segments, partitioning issues, stale clustering, and compaction opportunities.
- Epic D - FeatureFootprintReport
  - Show compiled features, enabled adapters, Vortex gates, object-store/write gates, and fallback-engine absence.
- Epic E - EffectBudgetReport
  - Track side effects, estimated cost, approvals, caching, redaction, and retry policy.
- Epic F - Agent Contract Pack
  - Stable JSON schemas, next-step hints, deterministic diagnostic codes, and repo integration templates.
- Epic G - Table Intelligence Layer
  - Snapshots, partition evolution, schema evolution, deletes/tombstones, and CDC/incremental planning.
- Epic H - Object Store Request Planner
  - Request/range budgets, coalescing, manifest-first planning, and retry/latency policy.
- Epic I - Correctness and Differential Harness
  - Differential tests, fuzzing, golden metadata/probe fixtures, and semantic edge cases.
- Epic J - Benchmark and Competitive Claims
  - Compare against external engines only as benchmark/correctness oracles, never as fallback execution.
- Epic K - Dynamic Work Shaping
  - Adjust task granularity, memory shape, and scheduling policy without changing query semantics.

## Competitive Gate Rationale

- CG-13 encoded-native compressed execution proves direct predicate/filter/project behavior over compressed layouts.
- CG-14 runtime adaptivity keeps plans robust under throughput shifts and memory pressure.
- CG-15 CPU specialization captures commodity CPU vectorized paths without requiring GPU/FPGA acceleration.
- CG-16 execution certificates make plan/input hashes, skipped work, and side effects reproducible and auditable.
- CG-17 stateful reuse reduces repeated work through explicit invalidation guarantees.
- CG-18 universal import/deployment/baseline harness stays universal-first and treats Foundry as optional deployment/comparison context only.
- CG-19 universal native I/O prevents common adapters from erasing encoded-native representation state.
- CG-20 capability certification makes SQL, operators, functions, adapters, APIs, observability, deployment, migration, and security part of best-default evidence.

## Guardrails

- Do not add fallback engines.
- Do not make Arrow the default internal execution path.
- Do not hide metadata/fidelity loss.
- Do not silently ignore deletes/tombstones.
- Do not broaden the default dependency graph without explicit approval.
- Do not execute effectful inputs without explicit enablement.
- Do not implement object-store/write/spill execution before the relevant phase.
- Do not claim benchmark wins, superiority, replacement, or best-default status without CG-5 correctness and CG-6 benchmark evidence.
- Do not infer CG completion from this reference map; use `docs/architecture/phased-execution-plan.md`.
