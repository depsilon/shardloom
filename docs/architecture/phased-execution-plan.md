# ShardLoom Phased Execution Plan

## How to maintain this file
- One active session checklist should be updated per PR/session.
- Detailed phase history should stay below the active queue.
- Do not duplicate "current" status in multiple places.
- Do not use stale percentage estimates.
- CG-1 through CG-20 remain competitive gates, not replacement phase IDs.
- External engines are baselines only, never fallback execution.
- For RFC-level phase mapping details, use `docs/architecture/rfc-phase-traceability.md`.

## Active Session Checklist
- [x] Session label: R5.4.7 workload constitution and scorecard sequencing
  - Current cleanup/implementation step: Scope best-default-engine certification to declared workloads, evidence refs, mandatory dimensions, and publication gates.
  - Primary files:
    - `docs/architecture/capability-certification-sequencing.md`
    - `docs/architecture/phased-execution-plan.md`
    - `docs/architecture/rfc-phase-traceability.md`
    - `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
  - Scope: Docs/RFC sequencing only.
  - Explicitly not included: Runtime behavior, SQL parser, SQL execution, DataFrame API, benchmark implementation, certification runtime, migration analyzer runtime, compatibility execution mode, adapter runtime, function registry, operator kernels, dependencies, external engine probing, filesystem/network/catalog probing, fallback execution, superiority claims.
  - Validation required:
    - `cargo fmt --all -- --check`
    - `cargo clippy --workspace --all-targets -- -D warnings`
    - `cargo test --workspace --all-targets`
  - Completion notes: RFC 0032 now defines workload constitution records, category evidence, scorecard dimensions, best-default certification dossier fields, disqualifiers, and publication gates.

## Current Queue
- [x] Next immediate step: R5.3.2 docs-wide CG-19/CG-20 consistency pass
  - Why: Keep CG-19/CG-20 canonical across RFCs, phase docs, agent instructions, and Vortex planning docs before additional queue movement.
  - Files:
    - `AGENTS.md`
    - `docs/rfcs/0025-competitive-engine-track-no-fallback-replacement.md`
    - `docs/rfcs/0031-universal-native-io-envelope.md`
    - `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
    - `docs/architecture/phased-execution-plan.md`
    - `docs/architecture/rfc-phase-traceability.md`
    - `docs/architecture/repo-cleanup-backlog.md`
    - `docs/architecture/vortex-adapter-integration-plan.md`
    - `docs/architecture/vortex-public-api-inventory.md`
  - Acceptance:
    - CG-1 through CG-20 are the canonical competitive gate range everywhere this pass touches.
    - RFC 0032 claim stages do not make superiority or best-default claims before CG-5/CG-6 evidence.
    - RFC 0031 result-stream certificate fields satisfy per-source/sink-path certificate requirements.
    - No runtime behavior, dependency, parser, adapter, or fallback changes.
  - Blockers:
    - None known.

- [x] Follow-up: R5.4 capability certification sequencing
  - Why: Convert CG-20's broad RFC surface into a batchable roadmap before implementation starts.
  - Files:
    - `docs/architecture/capability-certification-sequencing.md`
    - `docs/architecture/phased-execution-plan.md`
    - `docs/architecture/rfc-phase-traceability.md`
  - Acceptance:
    - SQL/operator/function/adapter/semantic/migration/certification work is split into explicit docs-only batches.
    - No runtime behavior, dependency, parser, adapter, or fallback changes.
  - Blockers:
    - None known.

- [x] Follow-up: R5.4.1 core capability matrix contracts
  - Why: Establish machine-readable report shapes before capability discovery, parser, adapter, or operator work.
  - Files:
    - `shardloom-core/src/certification.rs`
    - `shardloom-core/src/lib.rs`
    - `shardloom-contract-tests/tests/no_fallback_invariants.rs`
    - `docs/architecture/capability-certification-sequencing.md`
    - `docs/architecture/phased-execution-plan.md`
    - `docs/architecture/rfc-phase-traceability.md`
  - Acceptance:
    - SQL/operator/function/adapter/semantic/migration/scorecard matrices have core contract shapes.
    - Planned entries are not supported or certified.
    - `test_reference_only` cannot satisfy production certification.
    - `fallback_attempted=false` invariants are represented in contracts and tests.
    - No parser/runtime/adapter/dependency/fallback behavior is added.
  - Blockers:
    - None known.

- [x] Follow-up: R5.4.2 capability discovery surface
  - Why: Expose the planned CG-20 certification report through deterministic, side-effect-free capability discovery before adding feature implementation.
  - Files:
    - `shardloom-cli/src/main.rs`
    - `shardloom-core/src/certification.rs`
    - `shardloom-contract-tests/tests/`
    - `docs/architecture/capability-certification-sequencing.md`
  - Acceptance:
    - CLI discovery emits report-only text/JSON for capability certification.
    - Command execution performs no filesystem, network, catalog, adapter, parser, or runtime probing.
    - Planned entries remain planned, not supported.
    - `fallback_attempted=false` and fallback disabled remain explicit.
  - Blockers:
    - None known.

- [x] Follow-up: R5.4.2a capability certification snapshot tests
  - Why: Lock the generated empty/planned matrices and CLI discovery fields before filling in real SQL/operator/function/adapter coverage.
  - Files:
    - `shardloom-cli/src/main.rs`
    - `shardloom-core/src/certification.rs`
    - `shardloom-contract-tests/tests/`
  - Acceptance:
    - Snapshot-style contract tests cover generated planned matrices.
    - Snapshot-style contract tests cover capability discovery field names.
    - Tests do not execute external engines or probe filesystem/network/catalog/adapter state.
  - Blockers:
    - None known.

- [x] Follow-up: R5.4.3 SQL frontend sequencing
  - Why: Define SQL parse/bind/lower stages and unsupported diagnostics before adding any parser dependency or SQL runtime behavior.
  - Files:
    - `docs/architecture/capability-certification-sequencing.md`
    - `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
    - `docs/architecture/rfc-phase-traceability.md`
  - Acceptance:
    - SQL remains a frontend into ShardLoom-native planning.
    - Parser dependency decisions remain deferred to an explicit dependency/RFC pass.
    - No SQL parser, SQL execution, runtime behavior, adapter runtime, dependency, or fallback behavior is added.
  - Blockers:
    - None known.

- [x] Follow-up: R5.4.4 operator and function certification sequencing
  - Why: Define operator/function status transitions and metadata obligations before native kernels, registries, or execution expansion.
  - Files:
    - `docs/architecture/capability-certification-sequencing.md`
    - `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
    - `docs/architecture/rfc-phase-traceability.md`
  - Acceptance:
    - Operator certification distinguishes test/reference, native decoded, encoded-capable, streaming, spill, distributed, benchmarked, and production-certified states.
    - Function certification defines metadata obligations for types, null behavior, determinism, volatility, effects, encoded capability, materialization, tests, and benchmarks.
    - No function registry, operator kernel, execution behavior, dependency, or fallback behavior is added.
  - Blockers:
    - None known.

- [x] Follow-up: R5.4.5 adapter certification sequencing
  - Why: Define adapter maturity, pushdown, source/sink, fidelity, and no-fallback boundaries before adapter runtime work.
  - Files:
    - `docs/architecture/capability-certification-sequencing.md`
    - `docs/rfcs/0031-universal-native-io-envelope.md`
    - `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
    - `docs/architecture/rfc-phase-traceability.md`
  - Acceptance:
    - Adapter maturity levels A0-A7 are mapped to source/sink capability evidence.
    - Pushdown, residual, metadata/fidelity loss, encoded preservation, streaming, object-store-range, read/write/commit, and native I/O certificate boundaries are explicit.
    - External source pushdown is proof-backed source behavior, not hidden fallback execution.
    - No adapter runtime, object-store IO, file-format dependency, catalog dependency, execution behavior, or fallback behavior is added.
  - Blockers:
    - None known.

- [x] Follow-up: R5.4.6 semantic profile and migration sequencing
  - Why: Define semantic-profile and migration-report contracts before compatibility modes, migration analyzers, or external baseline comparisons expand.
  - Files:
    - `docs/architecture/capability-certification-sequencing.md`
    - `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
    - `docs/architecture/rfc-phase-traceability.md`
  - Acceptance:
    - Semantic profile dimensions are tied to status/evidence fields.
    - Migration reports distinguish supported constructs, unsupported constructs, semantic differences, function differences, adapter differences, materialization requirements, rewrite suggestions, evidence labels, and Vortex conversion payback.
    - External engines remain comparison and migration baselines only.
    - No compatibility execution mode, migration analyzer runtime, external engine dependency, benchmark claim, or fallback behavior is added.
  - Blockers:
    - None known.

- [x] Follow-up: R5.4.7 workload constitution and scorecard sequencing
  - Why: Scope best-default-engine certification to declared workloads and evidence-backed scorecard dimensions.
  - Files:
    - `docs/architecture/capability-certification-sequencing.md`
    - `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
    - `docs/architecture/rfc-phase-traceability.md`
  - Acceptance:
    - Workload constitution record shape, categories, evidence refs, and certification boundaries are documented.
    - Scorecard dimensions distinguish certified, partially certified, not certified, and evidence-insufficient states.
    - Claim-level scorecard publication requirements prevent unsupported superiority/default-engine claims.
    - No benchmark implementation, certification runtime, external engine dependency, or fallback behavior is added.
  - Blockers:
    - None known.

- [ ] Follow-up: R5.4.8 CI and snapshot sequencing
  - Why: Prevent capability report and best-default scorecard drift once the report contracts exist.
  - Files:
    - `docs/architecture/capability-certification-sequencing.md`
    - `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
    - `docs/architecture/rfc-phase-traceability.md`
    - `shardloom-contract-tests/tests/`
  - Acceptance:
    - Future snapshot categories are documented for SQL, operators, functions, adapters, semantic profiles, migration compatibility, best-choice scorecards, diagnostics, and no-fallback invariants.
    - Snapshot checks remain report-only and do not execute external engines or probe filesystem/network/catalog state.
    - Benchmark gates remain separate from docs-only/report-only work.
  - Blockers:
    - None known.

- [ ] Follow-up: R5.4.9 RFC sufficiency hardening pass
  - Why: Re-read the CG-19/CG-20 RFC set against the best-default-engine bar before moving back into implementation.
  - Files:
    - `docs/rfcs/0025-competitive-engine-track-no-fallback-replacement.md`
    - `docs/rfcs/0031-universal-native-io-envelope.md`
    - `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
    - `docs/architecture/phased-execution-plan.md`
    - `docs/architecture/rfc-phase-traceability.md`
  - Acceptance:
    - Missing best-default evidence contracts, acceptance criteria, disqualifiers, and no-fallback boundaries are added or explicitly deferred.
    - Superiority, performance, migration, adapter, and compatibility language remains evidence-scoped.
    - No runtime behavior, dependency, benchmark implementation, external engine probing, or fallback behavior is added.
  - Blockers:
    - None known.

- [ ] Follow-up: CG-2.3b projection readiness CLI integration
  - Why: Core projection-readiness contract exists; CLI surfacing remains deferred.
  - Files:
    - `shardloom-cli/src/main.rs`
    - `shardloom-vortex/src/projection_readiness.rs`
    - `shardloom-contract-tests/tests/`
  - Acceptance:
    - CLI emits report-only JSON/text.
    - No projection execution.
    - No scan/read-start, decode, materialization, Arrow conversion, writes, object-store IO, or fallback execution.
  - Blockers:
    - Sequenced behind active docs/refactor queue unless explicitly overridden.

## Cleanup / Refactor Queue
- [x] R0 Roadmap/docs synchronization
- [x] R1 Systems learning map
- [x] R2 Native contract vocabulary amendments
- [x] R2.1 RFC-level systems-learning contracts and traceability formatting
- [x] R3.1 Repo cleanup backlog inventory and terminology/CLI audit
- [x] R3.2 CLI usage/name consistency cleanup
- [x] R3.3 Diagnostics normalization backlog
- [x] R3.3a CLI missing/unknown argument diagnostic helpers
- [x] R3.3b Unknown signal diagnostic normalization
- [x] R3.3c Output envelope command-status derivation audit
- [x] R3.4 Terminology consolidation backlog
- [x] R3.5 Feature-footprint/doctor centralization plan
- [x] R3.5a FeatureFootprintReport core contract
- [x] R3.5d No-fallback dependency invariant tests
- [x] R5.1 Systems-learning contract pass
- [x] R5.2 Competitive track extension to CG-19/CG-20
- [x] R5.3 RFC 0031/0032 deepening
- [x] R5.3.1 RFC consistency fixes
  - Why: finalize docs-only consistency pass before resuming queue priorities.
  - Acceptance: docs-only consistency updates with no new implementation claims.
- [x] R5.3.2 Docs-wide CG-19/CG-20 consistency pass
  - Why: eliminate remaining CG-18-range drift, evidence-claim wording drift, and RFC 0031/0032 contract inconsistencies.
  - Acceptance: docs-only consistency updates with no new implementation claims.
  - Local validation status:
    - docs scans passed for duplicate headings, hidden/bidi controls, stale CG-18-range drift, decoded-reference drift, and `git diff --check`
    - Rust validation passed with toolchain `1.91.1`
- [x] R5.4 Capability certification sequencing
  - Why: convert CG-20 from broad RFC surface into a batchable implementation roadmap before adding code or dependencies.
  - Acceptance: docs-only sequencing for SQL/operator/function/adapter/semantic/migration/certification surfaces with no parser, adapter, runtime, or fallback implementation.
- [x] R5.4.1 Core capability matrix contracts
  - Why: add core report-only certification contracts before CLI discovery and feature expansion.
  - Acceptance: report-only core contracts and no-fallback invariants with no parser, runtime, adapter, dependency, or fallback behavior.
  - Local validation status:
    - focused `shardloom-core` certification tests passed
    - focused no-fallback invariant test passed
    - full Rust validation passed with toolchain `1.91.1`
- [x] R5.4.2 Capability discovery surface
  - Why: expose CG-20 planned certification surfaces through deterministic CLI discovery before feature implementation.
  - Acceptance: report-only scoped `capabilities` CLI surfaces with no parser, runtime, adapter, dependency, external probing, or fallback behavior.
  - Local validation status:
    - focused CLI capability discovery tests passed
    - full Rust validation passed with toolchain `1.91.1`
- [x] R5.4.2a Capability certification snapshot tests
  - Why: lock generated planned matrices and CLI discovery field names before capability coverage expands.
  - Acceptance: snapshot-style tests cover planned matrices, no-probe defaults, FeatureFootprint alignment, scoped field keys, and report-only discovery invariants.
  - Local validation status:
    - focused core certification snapshot tests passed
    - focused CLI discovery snapshot tests passed
- [x] R5.4.3 SQL frontend sequencing
  - Why: define parse/bind/lower stage boundaries before parser dependency or runtime behavior.
  - Acceptance: SQL frontend stages, report fields, unsupported diagnostic requirements, semantic profile boundaries, and parser dependency approval policy are documented.
  - Local validation status:
    - full Rust validation passed with toolchain `1.91.1`
- [x] R5.4.4 Operator and function certification sequencing
  - Why: define certification transitions before native kernels, registries, or execution expansion.
  - Acceptance: operator/function transition rules, report fields, test-reference boundaries, materialization/fallback fields, and correctness/benchmark gates are documented.
  - Local validation status:
    - full Rust validation passed with toolchain `1.91.1`
- [x] R5.4.5 Adapter certification sequencing
  - Why: define adapter maturity and source/sink/pushdown/fidelity boundaries before adapter runtime work.
  - Acceptance: adapter maturity A0-A7, source capability, sink requirement, pushdown exactness, residual, fidelity loss, encoded preservation, support fields, native I/O certificate linkage, and external source pushdown boundaries are documented.
  - Local validation status:
    - full Rust validation passed with toolchain `1.91.1`
- [x] R5.4.6 Semantic profile and migration sequencing
  - Why: define semantic-profile and migration-report evidence before compatibility modes, migration analyzers, or external baseline comparisons expand.
  - Acceptance: semantic profile reports, dimension statuses, compatibility-profile boundaries, migration supported/unsupported construct fields, semantic/function/adapter differences, rewrite suggestions, evidence-labeled performance/cost deltas, and Vortex conversion payback fields are documented.
  - Local validation status:
    - full Rust validation passed with toolchain `1.91.1`
- [x] R5.4.7 Workload constitution and scorecard sequencing
  - Why: scope best-default-engine certification to declared workloads and evidence-backed scorecard dimensions.
  - Acceptance: workload constitution fields, category evidence, scorecard dimensions, optional weighting rules, best-default certification dossier fields, disqualifiers, and publication gates are documented.
  - Local validation status:
    - full Rust validation passed with toolchain `1.91.1`

## Implementation Phase Queue
- [ ] R4 Resume CG implementation (planned)
  - Why: Resume implementation once active docs/refactor queue is current or explicitly overridden.
  - Acceptance:
    - Maintain no-fallback, explicit diagnostics, and Vortex-native I/O posture.
- [ ] CG-1.2d actual feature-gated local metadata/footer IO path (planned)
  - Why: progress real encoded-read readiness after docs alignment.
  - Acceptance:
    - Feature-gated only.
    - No runtime fallback/delegation.
- [ ] CG-2.1 actual count primitive over actual Vortex data (planned)
  - Why: progress from report-only readiness to real primitive execution.
  - Acceptance:
    - Explicit gating and deterministic unsupported diagnostics where not ready.

## Competitive Engine Gates CG-1 through CG-20

Status legend:
- **[x] complete**
- **[ ] current/planned**
- **[~] blocked/deferred**

- [ ] CG-1 — Real encoded read path (**current**)
  - [x] CG-1.1a encoded read boundary core contract
  - [x] CG-1.1b CLI/docs integration
  - [x] CG-1.2a/1.2b/1.2c planning, fixture, and metadata probe/report integration
  - [~] CG-1.2d metadata/footer invocation execution remains deferred/blocked by approved-safe invocation constraints
  - Required capabilities:
    - feature-gated local encoded read API boundary
    - segment/chunk/byte-range descriptors
    - deterministic diagnostics for unsupported boundary states
  - Guardrails:
    - no broad row materialization
    - no Arrow-default conversion
    - no fallback execution

- [ ] CG-2 — Real query primitive execution over actual Vortex data (**current**)
  - [x] report-only readiness planning for:
    - count
    - filtered count
    - projection readiness
    - predicate/filter primitive readiness
  - [~] CG-2.1+ actual primitive execution remains deferred
  - [~] CG-2.3b projection readiness CLI integration deferred by queue sequencing
  - Required capabilities for completion:
    - encoded-first selection vectors
    - decode only when explicitly allowed
    - deterministic blocked diagnostics when metadata/footer or encoded-path readiness is missing

- [ ] CG-3 — Actual output payload write path (**planned, incomplete**)
  - [x] placeholder/local artifact readiness path exists
  - [~] real Vortex output payload path remains incomplete
  - Completion rule:
    - CG-3 remains incomplete until ShardLoom can produce at least one real Vortex-native output payload for a supported workload through a feature-gated, no-fallback path.
  - Guardrails:
    - placeholder artifacts do **not** count as completion
    - no committed state until commit protocol phase requirements are satisfied
    - no object-store writes initially

- [ ] CG-4 — Commit protocol execution (**planned**)
  - [x] report-only planning/state-machine and marker/finalization readiness contracts
  - [~] real commit execution remains deferred
  - Required capabilities for completion:
    - local-first idempotent commit execution
    - recoverable rollback/ambiguous-commit diagnostics
    - deterministic commit-state transitions

- [ ] CG-5 — Correctness/differential tests (**planned**)
  - Expected evidence:
    - golden Vortex fixtures
    - decoded reference outputs
    - null/nested/dictionary/sparse/run-length/temporal edge-case coverage
    - external engine baselines used only as correctness oracles (never runtime fallback)

- [ ] CG-6 — Benchmarks (**planned**)
  - Expected evidence:
    - runtime latency and startup latency
    - peak memory and spill-required/avoided reporting
    - bytes read/written and segments skipped
    - decode/materialization/work-avoided evidence
  - Guardrail:
    - no superiority claims before CG-5 and CG-6 are satisfied

- [ ] CG-7 — Physical operator/kernel layer (**planned**)
  - Scope:
    - filter/projection/count-aggregate kernels
    - metadata/encoded/hybrid execution levels
    - expression evaluation over encoded segments

- [ ] CG-8 — Streaming/parallel/adaptive execution (**planned**)
  - Scope:
    - streaming encoded batches
    - bounded parallel local execution
    - adaptive split/coalesce
    - backpressure and memory/spill-aware scheduling

- [ ] CG-9 — Lakehouse/table intelligence (**planned**)
  - Scope:
    - schema evolution and partition evolution
    - delete/tombstone semantics
    - CDC/incremental planning
    - layout-health and compaction planning

- [ ] CG-10 — Object-store/distributed execution (**planned**)
  - Scope:
    - object-store range planning and request coalescing
    - object-store commit protocol
    - distributed scheduling with checkpoint/retry/idempotency

- [ ] CG-11 — Python/API surface later (**planned**)
  - Scope:
    - thin Python wrapper over stable CLI JSON first
    - Foundry-friendly integration later
    - no PyO3/maturin unless explicitly approved

- [ ] CG-12 — Plan portability / semantic IR (**planned**)
  - Scope:
    - native-first plan portability reports
    - explicit unsupported/lossy/residual construct reporting
    - no import/export execution side effects

- [ ] CG-13 — Encoded-native compressed execution (**planned**)
  - Scope:
    - encoding-aware execution-path selection
    - direct count/filter/project over encoded segments
    - decode-avoided proof/report requirements

- [ ] CG-14 — Runtime-adaptive optimizer and execution memory (**planned**)
  - Scope:
    - adaptive decisions with deterministic diagnostics
    - conservative runtime filters and pruning
    - bounded-memory-safe adaptation boundaries

- [ ] CG-15 — CPU operator specialization (**planned**)
  - Scope:
    - commodity CPU vectorized specialization
    - SIMD/cache-aware operator paths
    - no fallback engines for specialization

- [ ] CG-16 — Evidence-first execution certificates (**planned**)
  - Scope:
    - plan/input/output evidence artifacts
    - reproducibility metadata
    - deterministic machine-readable certificate surfaces

- [ ] CG-17 — Stateful result reuse / incremental execution (**planned**)
  - Scope:
    - typed cache/reuse boundaries
    - explicit invalidation rules and correctness-proof signals

- [ ] CG-18 — Universal import/deployment/baseline harness (**planned**)
  - Scope:
    - universal runner contracts and portability checks
    - external baseline harnesses are comparison-only
    - Foundry remains optional deployment/comparison only

- [ ] CG-19 — Universal Native I/O Envelope (**planned**)
  - [x] RFC 0031 contract deepening complete
  - [~] implementation pending
  - Scope:
    - preserve representation state, pushdown evidence, materialization boundaries, and sink constraints without default decode

- [ ] CG-20 — World-Class SQL/operator/function/adapter/user capability surface (**planned**)
  - [x] RFC 0032 contract deepening complete
  - [~] implementation pending
  - Scope:
    - capability certification surface across SQL/operators/functions/adapters/semantic profiles and migration/certification reporting


## Competitive Engine Gate Detailed Checklist Ledger

Use this section for attributable CG substeps. Keep each item as a checkbox so progress remains session-updateable without losing provenance detail.

### CG-1 detailed checklist
- [x] CG-1.1a encoded read boundary core contract
- [x] CG-1.1b encoded read boundary CLI/docs integration
- [x] CG-1.2a feature-gated local encoded-read fixture contract
- [x] CG-1.2a.1 encoded-read plan diagnostics/report field closeout
- [x] CG-1.2a.2 feature-gated readiness/report validation
- [x] CG-1.2b feature-gated local metadata/footer probe contract
- [x] CG-1.2b.1 metadata probe default no-IO / feature-gate stabilization
- [x] CG-1.2c metadata/footer probe CLI/docs integration
- [x] CG-1.3 no-broad-materialization/no-Arrow-default invariant closeout (report-contract scope)
- [~] CG-1.2d metadata/footer invocation execution path remains deferred/blocked
- [ ] CG-1 closeout requires approved-safe metadata/footer invocation evidence

### CG-2 detailed checklist
- [x] CG-2.0 query primitive readiness boundary (report-only)
- [x] CG-2.0b helper correctness and blocker-preservation closeout
- [x] CG-2.0c query primitive plan CLI integration
- [x] CG-2.1 count readiness planning contract (report-only)
- [x] CG-2.1a count readiness semantic hardening
- [x] CG-2.1b count readiness CLI integration
- [x] CG-2.2a filtered-count readiness core contract
- [x] CG-2.2a.1 filtered-count blocker precision hardening
- [x] CG-2.2b filtered-count readiness CLI integration
- [x] CG-2.3a projection readiness semantic hardening
- [~] CG-2.3b projection readiness CLI integration deferred by queue sequencing
- [~] CG-2.1+ actual query primitive execution remains deferred
- [ ] CG-2 closeout requires real count/filtered-count/projection execution over actual Vortex data

### CG-3 detailed checklist
- [x] CG-3 contract/readiness scaffolding represented in phase-12 planning artifacts
- [x] local placeholder payload artifact readiness path
- [x] output-payload plan CLI (report-only)
- [x] output-payload artifact write CLI (placeholder/local scope)
- [~] staged smoke evidence is readiness-only and does not complete CG-3
- [~] real Vortex-native output payload write path incomplete
- [ ] CG-3 closeout requires at least one real feature-gated Vortex payload write path

### CG-4 detailed checklist
- [x] commit-intent core contract
- [x] commit-intent readiness integration
- [x] commit marker planning and local marker artifact boundaries
- [x] finalized-manifest candidate contract/artifact boundaries
- [x] local commit execution gate (report-only)
- [ ] local-first commit execution
- [ ] feature-gated commit execution path
- [ ] idempotent commit behavior
- [ ] recoverable commit behavior
- [ ] rollback/ambiguous commit reports
- [ ] no object-store commit until later phases
- [~] real commit execution remains deferred

### CG-5 detailed checklist
- [ ] golden Vortex fixtures
- [ ] reference outputs
- [ ] null/nested/dictionary/sparse/run-length/temporal edge-case coverage
- [ ] Spark/Polars/DataFusion external baselines only, never fallback

### CG-6 detailed checklist
- [ ] runtime benchmarks
- [ ] peak-memory benchmarks
- [ ] bytes read/written benchmarks
- [ ] decode-avoided evidence
- [ ] materialization-avoided evidence
- [ ] segments-skipped evidence
- [ ] work-avoided evidence
- [ ] spill-required/avoided evidence
- [ ] startup latency benchmarks
- [ ] query runtime benchmarks
- [ ] write/commit latency benchmarks

### CG-7 detailed checklist
- [ ] filter kernel
- [ ] projection kernel
- [ ] count/aggregate kernel
- [ ] metadata/encoded/hybrid execution levels
- [ ] expression evaluation over encoded segments

### CG-8 detailed checklist
- [ ] streaming encoded batches
- [ ] bounded parallel local execution
- [ ] adaptive split/coalesce
- [ ] dynamic sizing feedback loop
- [ ] backpressure
- [ ] memory/spill-aware scheduler

### CG-9 detailed checklist
- [ ] schema evolution
- [ ] partition evolution
- [ ] delete/tombstone semantics
- [ ] CDC/incremental planning
- [ ] layout health
- [ ] compaction planning

### CG-10 detailed checklist
- [ ] object-store range planning
- [ ] request coalescing
- [ ] object-store commit protocol
- [ ] distributed scheduling
- [ ] checkpoint/retry/idempotency

### CG-11 detailed checklist
- [ ] thin Python wrapper over CLI JSON first
- [ ] Foundry-friendly later
- [ ] no PyO3/maturin unless explicitly approved
- [ ] no Spark fallback

### CG-12 detailed checklist
- [ ] native-first plan portability reports
- [ ] explicit unsupported/lossy/residual construct reporting
- [ ] no import/export execution side effects

### CG-13 detailed checklist
- [ ] encoding-aware execution path selection
- [ ] decode-avoided proof/report requirements

### CG-14 detailed checklist
- [ ] adaptive decisions with deterministic diagnostics
- [ ] bounded-memory-safe adaptation boundaries

### CG-15 detailed checklist
- [ ] commodity CPU vectorized specialization is first-class
- [ ] no external engine fallback for specialization

### CG-16 detailed checklist
- [ ] plan/input/output evidence artifacts for reproducibility
- [ ] deterministic, machine-readable certificate surfaces

### CG-17 detailed checklist
- [ ] typed cache/reuse boundaries
- [ ] explicit invalidation and correctness proof signals

### CG-18 detailed checklist
- [ ] universal runner contracts and portability checks
- [ ] external baseline harnesses are comparison-only
- [ ] Foundry optional deployment/comparison example only

### CG-19 detailed checklist
- [x] RFC 0031 contract deepening complete
- [~] implementation pending
- [ ] preserve representation state, pushdown evidence, materialization boundaries, and sink constraints without default decode

### CG-20 detailed checklist
- [x] RFC 0032 contract deepening complete
- [~] implementation pending
- [ ] capability certification surface implementation across SQL/operators/functions/adapters/semantic profiles/migration reporting

### CG attribution and evidence notes
- [ ] When moving any detailed item to complete, link the implementing PR/commit and validating tests in the completion note.
- [ ] Do not mark CG-3 complete from placeholder artifacts.
- [ ] Do not make superiority claims before CG-5 and CG-6 are satisfied.
- [ ] Keep external engines baseline-only for comparison/correctness/benchmarks, never runtime fallback.

## Cross-cutting Epics
- [ ] Epic A — DecisionTrace / WhyReport
- [ ] Epic B — WorkAvoidedReport
- [ ] Epic C — LayoutHealthReport
- [ ] Epic D — FeatureFootprintReport
- [ ] Epic E — EffectBudgetReport
- [ ] Epic F — Agent Contract Pack
- [ ] Epic G — Table Intelligence Layer
- [ ] Epic H — Object Store Request Planner
- [ ] Epic I — Correctness and Differential Harness
- [ ] Epic J — Benchmark and Competitive Claims
- [ ] Epic K — Dynamic Work Shaping

## Completed Phase Ledger
- [x] Phase 0 — Project setup, licensing, naming, repo foundation
- [x] Phase 1 — RFCs, skills, architecture docs, no-fallback policy
- [x] Phase 2 — Core domain contracts
- [x] Phase 3 — Plan/runtime skeletons
- [x] Phase 4 — Vortex adapter foundation
- [x] Phase 5 — Universal input/output contracts and Vortex planning chain
- [x] Phase 6 — Execution gates and executor skeletons
- [x] Phase 7A — Encoded-read probe plan contract
- [x] Phase 7B — Feature-gated local Vortex metadata-only open
- [x] Phase 8 — First controlled encoded-read execution spike
- [x] Phase 9A/9B/9C/9D — Query primitive and work-avoidance planning layers
- [x] Phase 10A/10B/10C/10D — Runtime/streaming/diagnostic skeleton and stabilization layers
- [x] Phase 11A/11B (planning-focused milestones listed in traceability)
- [x] Phase 12A/12B/12C planning-and-readiness milestones listed in traceability
  - Note: Phase-12 placeholder artifacts are readiness-only and do not imply CG-3 completion.

## Deferred / Blocked Work
- [~] CG-1.2 metadata/footer execution path remains blocked pending approved-safe invocation inputs and harness constraints.
- [~] CG-2.1 actual execution remains blocked pending metadata/footer and encoded data path readiness.
- [~] CG-3 real Vortex payload writes remain deferred; placeholder artifact paths are not completion evidence.
- [~] CG-2.3b projection CLI integration deferred by queue sequencing (not capability claim).

## Guardrails
- No Spark/DataFusion/Polars/DuckDB/Velox/vortex-datafusion fallback execution or delegation.
- Unsupported behavior must fail explicitly with deterministic diagnostics.
- Vortex is native input and highest-fidelity native output.
- Compatibility outputs are translation/export targets, not execution fallback.
- Keep docs/cleanup queue visible when active; do not skip directly to CG work by default.
- Preserve both canonical phase IDs and CG gate visibility; do not treat CG IDs as replacements.
- Keep Foundry under CG-18 as optional deployment/comparison context only.
- Competitive claims require CG-5 correctness and CG-6 benchmarks.
