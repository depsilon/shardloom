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
- [x] Session label: CG-2.1e.14 encoded-count local guard capability discovery
  - Current cleanup/implementation step: Surface the layout-approved encoded-count local guard in capability discovery without enabling execution.
  - Primary files:
    - `shardloom-vortex/src/local_execution.rs`
    - `shardloom-vortex/src/lib.rs`
    - `shardloom-cli/src/main.rs`
    - `shardloom-cli/tests/capability_discovery_snapshots.rs`
    - `docs/architecture/phased-execution-plan.md`
    - `docs/architecture/rfc-phase-traceability.md`
  - Scope: Static, report-only capability fields for the encoded-count local guard, including accepted approval sources, deferred status, result-known state, and no-read/no-decode/no-fallback effects.
  - Explicitly not included: Actual encoded-data traversal, scan/read-start APIs, layout-reader construction, runtime-driver startup, row reads, decode/materialization, Arrow conversion, object-store IO, writes, spill IO, external baseline execution, fallback execution, benchmarks, SQL/API/adapter expansion, or superiority claims.
  - Validation required:
    - `cargo fmt --all -- --check`
    - `cargo clippy --workspace --all-targets -- -D warnings`
    - `cargo test --workspace --all-targets`
  - Completion notes: `shardloom capabilities operators` now exposes the encoded-count local guard as report-only discovery evidence; execution remains disabled and deferred.

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

- [x] Follow-up: R5.4.8 CI and snapshot sequencing
  - Why: Prevent capability report and best-default scorecard drift once the report contracts exist.
  - Files:
    - `docs/architecture/capability-certification-sequencing.md`
    - `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
    - `docs/architecture/rfc-phase-traceability.md`
  - Acceptance:
    - Future snapshot categories are documented for SQL, operators, functions, adapters, semantic profiles, migration compatibility, best-choice scorecards, world-class sufficiency, diagnostics, and no-fallback invariants.
    - Snapshot checks remain report-only and do not execute external engines or probe filesystem/network/catalog state.
    - Benchmark gates remain separate from docs-only/report-only work.
  - Blockers:
    - None known.

- [x] Follow-up: R5.4.9 RFC sufficiency hardening pass
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
  - Local validation status:
    - docs hygiene scans passed for duplicate headings, hidden/bidi controls, claim-language review, and `git diff --check`
    - full Rust validation passed with toolchain `1.91.1`
  - Blockers:
    - None known.

- [x] Follow-up: CG-2.3b projection readiness CLI integration
  - Why: Core projection-readiness contract exists; CLI surfacing remains deferred.
  - Files:
    - `shardloom-cli/src/main.rs`
    - `shardloom-vortex/src/projection_readiness.rs`
    - `shardloom-contract-tests/tests/`
  - Acceptance:
    - CLI emits report-only JSON/text.
    - No projection execution.
    - No scan/read-start, decode, materialization, Arrow conversion, writes, object-store IO, or fallback execution.
  - Local validation status:
    - focused `shardloom-cli` projection-readiness tests passed
    - full Rust validation passed with toolchain `1.91.1`
  - Blockers:
    - None known.

- [x] Follow-up: R5.4.10 user-surface RFC hardening
  - Why: SQL/operator/function/adapter details are deep enough for current planning, but best-default certification also needs explicit API, BI/server, observability, deployment, extension, and security/governance evidence.
  - Files:
    - `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
    - `docs/architecture/capability-certification-sequencing.md`
    - `docs/architecture/phased-execution-plan.md`
    - `docs/architecture/rfc-phase-traceability.md`
    - `docs/architecture/systems-learning-map.md`
    - `docs/architecture/canonical-terminology.md`
  - Acceptance:
    - RFC 0032 has concrete field-level contracts for API surface maturity, observability certification, deployment readiness, extension capability, and security/governance reports.
    - Capability discovery and scorecard/dossier evidence include these user-surface dimensions.
    - Scope remains docs/RFC-only with no runtime behavior, dependencies, server/API implementation, UDF/plugin runtime, external probing, or fallback execution.
  - Local validation status:
    - docs hygiene scans passed for duplicate headings, hidden/bidi controls, claim-language review, and `git diff --check`
    - full Rust validation passed with toolchain `1.91.1`
  - Blockers:
    - None known.

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
- [x] R5.4.8 CI and snapshot sequencing
  - Why: prevent capability report and best-default scorecard drift once report contracts exist.
  - Acceptance: capability surface snapshot fields, snapshot kinds, drift-policy fields, allowed/blocked changes, CI gate levels, and no-probe snapshot boundaries are documented.
  - Local validation status:
    - full Rust validation passed with toolchain `1.91.1`
- [x] R5.4.9 RFC sufficiency hardening pass
  - Why: make the CG-19/CG-20 RFC set explicit enough to govern best-default-engine claims before implementation resumes.
  - Acceptance: best-default evidence gate, CG-19 sufficiency gates, `WorldClassSufficiencyReport`, disqualifiers, explicit deferrals, and no-fallback boundaries are documented.
  - Local validation status:
    - docs hygiene scans passed for duplicate headings, hidden/bidi controls, claim-language review, and `git diff --check`
    - full Rust validation passed with toolchain `1.91.1`
- [x] R5.4.10 User-surface RFC hardening
  - Why: ensure CG-20 best-default certification includes last-mile user/product surfaces, not only SQL/operators/functions/adapters.
  - Acceptance: API/client/server maturity, observability certification, deployment readiness, extension safety, security/governance controls, capability discovery statuses, scorecard dimensions, and dossier evidence are field-level RFC contracts.
  - Local validation status:
    - docs hygiene scans passed for duplicate headings, hidden/bidi controls, claim-language review, and `git diff --check`
    - full Rust validation passed with toolchain `1.91.1`

## Implementation Phase Queue
- [x] R4 Resume CG implementation
  - Why: Resume implementation once active docs/refactor queue is current or explicitly overridden.
  - Acceptance:
    - Maintain no-fallback, explicit diagnostics, and Vortex-native I/O posture.
- [x] CG-1.2d actual feature-gated local metadata/footer IO path
  - Why: progress real encoded-read readiness after docs alignment.
  - Acceptance:
    - Feature-gated only.
    - No runtime fallback/delegation.
    - Local metadata/footer open is caller-session driven and does not call scan/read-start, decode/materialization, object-store IO, writes, or fallback execution.
- [x] CG-2.1c metadata-footer `CountAll` primitive over actual Vortex fixture metadata
  - Why: progress from report-only readiness to real primitive execution.
  - Acceptance:
    - Explicit gating and deterministic unsupported diagnostics where not ready.
    - Count result comes from typed metadata/footer summary, not scan/read-start or encoded data traversal.
- [x] CG-2.1d encoded-data count candidate path
  - Why: progress non-metadata count candidates after metadata-footer count is wired.
  - Acceptance:
    - Explicit encoded-data boundary approval before any encoded data traversal.
    - Local execution reports `NeedsEncodedRead` for approved encoded-data count candidates without executing the read.
- [x] CG-2.1e.1 encoded-data count API-gated blocker
  - Why: prevent approved future encoded-read candidates from being treated as executable count paths until the public Vortex data API is safe under ShardLoom's no-decode/no-materialization boundary.
  - Acceptance:
    - Count readiness can consume `VortexEncodedReadProbeReport`.
    - Public scan/data-read/decode/materialization/Arrow/object-store/write blockers are reflected as count-readiness blockers.
    - No scan/read-start invocation, encoded-data traversal, row read, decode/materialization, Arrow conversion, object-store IO, write, or fallback execution is added.
- [x] CG-2.1e.2 exact Vortex data-access API classification
  - Why: replace generic scan/read-start blocker language with compile-checked public Vortex surface names before considering an execution path.
  - Acceptance:
    - The encoded-read public API boundary lists the exact `VortexFile`, `LayoutReader`, and `ScanBuilder` surfaces reviewed.
    - `LayoutReader::row_count` is classified as metadata-like and not execution-usable.
    - Scan, array-stream, evaluation, and data-source surfaces remain blocked or deferred with deterministic risks.
    - No scan/read-start invocation, encoded-data traversal, row read, decode/materialization, Arrow conversion, object-store IO, write, or fallback execution is added.
- [x] CG-2.1e.3 named count API-boundary blockers
  - Why: preserve exact blocked Vortex surface names when API-boundary evidence reaches count readiness.
  - Acceptance:
    - Count readiness requests carry named API-boundary blocker summaries from the encoded-read probe.
    - Blocked scan/stream/evaluation/data-source surfaces are visible at the count-readiness boundary.
    - Metadata-like layout row-count access is not reported as a count execution blocker.
    - No scan/read-start invocation, encoded-data traversal, row read, decode/materialization, Arrow conversion, object-store IO, write, or fallback execution is added.
- [x] CG-2.1e.4 encoded-count admission blocker guard
  - Why: make named API-boundary blockers enforcement inputs, not only explanatory metadata.
  - Acceptance:
    - Count readiness cannot become `CountReady` while named API-boundary blockers are present.
    - Local encoded-count admission rejects readiness reports that carry named API-boundary blockers.
    - No scan/read-start invocation, encoded-data traversal, row read, decode/materialization, Arrow conversion, object-store IO, write, or fallback execution is added.
- [x] CG-2.1e.5 `VortexFile::row_count` metadata-surface approval
  - Why: distinguish the safe public footer-backed row-count method from layout/scan/evaluation data paths.
  - Acceptance:
    - `VortexFile::row_count` is compile-checked and classified as confirmed public metadata.
    - `VortexFile::row_count` is contract-usable but not execution-usable.
    - `LayoutReader::row_count` remains metadata-like but deferred because constructing layout readers is not yet an approved count path.
    - No scan/read-start invocation, encoded-data traversal, row read, decode/materialization, Arrow conversion, object-store IO, write, or fallback execution is added.
- [x] CG-2.1e.6 encoded-count data-path approval boundary
  - Why: prevent metadata count evidence from being mistaken for approval to traverse encoded data.
  - Acceptance:
    - `VortexEncodedCountDataPathApprovalReport` consumes count readiness and encoded-read API boundary reports.
    - Current public API boundary blocks encoded-data count approval because execution-usable data path count remains zero and scan/stream/evaluation surfaces remain blocked.
    - `VortexFile::row_count` is reported as metadata count evidence only.
    - No scan/read-start invocation, encoded-data traversal, row read, decode/materialization, Arrow conversion, object-store IO, write, or fallback execution is added.
- [x] CG-2.1e.7 encoded-count approval CLI surfacing
  - Why: make the encoded-count approval blocker queryable by humans and agents without writing Rust.
  - Acceptance:
    - `shardloom vortex-encoded-count-approval-plan <candidate_source> <dataset_uri> [flags] [--format text|json]` emits approval status and side-effect flags.
    - Current public API boundary returns a deterministic unsupported/non-zero result for encoded-data count approval even when readiness flags are present.
    - CLI output remains report-only and includes `fallback_execution_allowed=false`.
    - No scan/read-start invocation, encoded-data traversal, row read, decode/materialization, Arrow conversion, object-store IO, write, or fallback execution is added.
- [x] CG-2.1e.8 encoded-count approval local guard
  - Why: prevent local encoded-count execution helpers from advancing unless the explicit approval report is approved.
  - Acceptance:
    - `execute_vortex_count_all_from_encoded_count_data_path_approval` consumes `VortexEncodedCountDataPathApprovalReport`.
    - The current public API boundary returns unsupported from the guard.
    - A future approved boundary can only return deferred `NeedsEncodedRead`, not actual execution.
    - No scan/read-start invocation, encoded-data traversal, row read, decode/materialization, Arrow conversion, object-store IO, write, or fallback execution is added.
- [x] CG-2.1e.9 layout-reader construction blocker hardening
  - Why: prevent `LayoutReader::row_count` from being mistaken for an approved encoded-count path when the only public construction route goes through `VortexFile::segment_source`.
  - Acceptance:
    - `VortexFile::layout_reader` is classified with a runtime-driver blocking risk.
    - `LayoutReader::row_count` remains metadata-like, non-blocking by itself, and not execution-usable.
    - Count-readiness and encoded-count approval reports preserve `VortexFile::layout_reader` as a named API-boundary blocker while excluding metadata-only row-count surfaces from execution blockers.
    - No `LayoutReader` construction, scan/read-start invocation, encoded-data traversal, row read, decode/materialization, Arrow conversion, object-store IO, write, or fallback execution is added.
- [x] CG-2.1e.10 layout-driver approval boundary
  - Why: make the runtime-driver approval decision explicit before any future row-count-only layout reader path can be wired.
  - Acceptance:
    - `VortexLayoutReaderDriverApprovalReport` consumes the encoded-read API boundary report.
    - Current public API boundary blocks without explicit runtime-driver approval.
    - A synthetic approved report still performs no construction, scan, evaluation, data read, decode/materialization, Arrow conversion, object-store IO, write, or fallback execution.
    - Approval requires local fixture scope, caller session, runtime-driver permission, layout-row-count-only intent, and explicit no-scan/no-evaluation/no-read/no-decode/no-materialization/no-Arrow/no-object-store/no-write/no-fallback signals.
- [x] CG-2.1e.11 layout-driver approval CLI surfacing
  - Why: make the layout-driver approval boundary queryable by humans and agents before any runtime path is considered.
  - Acceptance:
    - `shardloom vortex-layout-driver-approval-plan <signals> [--format text|json]` emits the approval report.
    - Missing, empty, duplicate, and unknown signal handling is deterministic.
    - Current public API boundary blocks without explicit runtime-driver permission.
    - A full approved signal set still performs no construction, driver start, scan, evaluation, data read, row read, decode/materialization, Arrow conversion, object-store IO, write, or fallback execution.
- [x] CG-2.1e.14 encoded-count local guard capability discovery
  - Why: make the new deferred local guard visible in operator capability discovery before real encoded execution can be claimed.
  - Acceptance:
    - `shardloom capabilities operators --format json` emits an encoded-count local guard discovery block.
    - The discovery block records accepted approval sources, `needs_encoded_read`, `plan_only`, no count result, no data read, no decode/materialization, no runtime execution, and no fallback.
    - The discovery surface remains static/report-only and does not construct layout readers, start runtime drivers, scan, evaluate, read rows, decode/materialize, convert to Arrow, touch object stores, write, spill, invoke external baselines, or fallback.
- [ ] CG-2.1e encoded-data count execution path (planned)
  - Why: turn the approved encoded-data count candidate into actual native encoded execution after the public Vortex data path is approved.
  - Acceptance:
    - Real encoded data traversal is feature-gated, local-fixture scoped first, and still avoids rows, decode/materialization, Arrow conversion, object-store IO, writes, and fallback.

## Competitive Engine Gates CG-1 through CG-20

Status legend:
- **[x] complete**
- **[ ] current/planned**
- **[~] blocked/deferred**

- [ ] CG-1 — Real encoded read path (**current**)
  - [x] CG-1.1a encoded read boundary core contract
  - [x] CG-1.1b CLI/docs integration
  - [x] CG-1.2a/1.2b/1.2c planning, fixture, and metadata probe/report integration
  - [x] CG-1.2d metadata/footer invocation path exists for feature-gated local fixtures with caller-provided async/session context
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
  - [x] CG-2.1c metadata-footer `CountAll` execution bridge over checked-in Vortex fixture metadata
  - [x] CG-2.1d encoded-data `CountAll` candidate bridge to deferred local execution
  - [x] CG-2.1e.1 encoded-data `CountAll` API-gated blocker through encoded-read probe
  - [x] CG-2.1e.2 exact Vortex data-access API classification
  - [x] CG-2.1e.3 named count API-boundary blockers
  - [x] CG-2.1e.4 encoded-count admission blocker guard
  - [x] CG-2.1e.5 `VortexFile::row_count` metadata-surface approval
  - [x] CG-2.1e.6 encoded-count data-path approval boundary
  - [x] CG-2.1e.7 encoded-count approval CLI surfacing
  - [x] CG-2.1e.8 encoded-count approval local guard
  - [x] CG-2.1e.9 layout-reader construction blocker hardening
  - [x] CG-2.1e.10 layout-driver approval boundary
  - [x] CG-2.1e.11 layout-driver approval CLI surfacing
  - [x] CG-2.1e.12 layout-approved encoded count bridge
  - [x] CG-2.1e.13 layout-approved local count guard
  - [x] CG-2.1e.14 encoded-count local guard capability discovery
  - [~] CG-2.1+ non-metadata primitive execution remains deferred pending actual encoded-data execution
  - [x] CG-2.3b projection readiness CLI integration
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
  - [x] CG-5.1 metadata query primitive correctness fixtures
  - [x] CG-5.2 metadata query primitive edge and diagnostic fixtures
  - [x] CG-5.3 correctness fixture manifest contract
  - [x] CG-5.4 external baseline oracle policy
  - Expected evidence:
    - golden Vortex fixtures
    - decoded reference outputs
    - null/nested/dictionary/sparse/run-length/temporal edge-case coverage
    - external engine baselines used only as correctness oracles (never runtime fallback)

- [ ] CG-6 — Benchmarks (**planned**)
  - [x] CG-6.1 benchmark evidence manifest
  - [x] CG-6.2 benchmark claim gate
  - [x] CG-6.3 benchmark comparison report contract
  - [x] CG-6.4 benchmark reproducibility manifest
  - [x] CG-6.5 reproducibility-aware benchmark claim gate
  - Expected evidence:
    - runtime latency and startup latency
    - peak memory and spill-required/avoided reporting
    - bytes read/written and segments skipped
    - decode/materialization/work-avoided evidence
  - Guardrail:
    - no superiority claims before CG-5 and CG-6 are satisfied

- [ ] CG-7 — Physical operator/kernel layer (**planned**)
  - [x] CG-7.1 physical operator/kernel contract foundation
  - [x] CG-7.2 physical operator capability discovery
  - [x] CG-7.3 physical kernel registry plan
  - [x] CG-7.4 physical kernel admission gate
  - [x] CG-7.5 physical operator execution profiles
  - [x] CG-7.6 physical kernel selection gate
  - [x] CG-7.7 physical operator planning certificate
  - [x] CG-7.8 Vortex query primitive physical-operator bridge
  - [x] CG-7.9 execution-level kernel requirements
  - [x] CG-7.10 metadata-result physical operator bridge
  - [x] CG-7.11 metadata bridge admission evidence
  - [x] CG-7.12 metadata-only physical kernel report
  - [x] CG-7.13 metadata physical kernel CLI surfacing
  - [x] CG-7.14 metadata kernel capability discovery
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
- [x] CG-1.2d metadata/footer invocation execution path for local fixtures
- [ ] CG-1 closeout still requires an encoded data path beyond metadata/footer inspection

### CG-2 detailed checklist
- [x] CG-2.0 query primitive readiness boundary (report-only)
- [x] CG-2.0b helper correctness and blocker-preservation closeout
- [x] CG-2.0c query primitive plan CLI integration
- [x] CG-2.1 count readiness planning contract (report-only)
- [x] CG-2.1a count readiness semantic hardening
- [x] CG-2.1b count readiness CLI integration
- [x] CG-2.1c metadata-footer `CountAll` execution bridge over actual Vortex fixture metadata
- [x] CG-2.1d encoded-data `CountAll` candidate bridge to deferred local execution
- [x] CG-2.1e.1 encoded-data `CountAll` API-gated blocker through encoded-read probe
- [x] CG-2.1e.2 exact Vortex data-access API classification
- [x] CG-2.1e.3 named count API-boundary blockers
- [x] CG-2.1e.4 encoded-count admission blocker guard
- [x] CG-2.1e.5 `VortexFile::row_count` metadata-surface approval
- [x] CG-2.1e.6 encoded-count data-path approval boundary
- [x] CG-2.1e.7 encoded-count approval CLI surfacing
- [x] CG-2.1e.8 encoded-count approval local guard
- [x] CG-2.1e.9 layout-reader construction blocker hardening
- [x] CG-2.1e.10 layout-driver approval boundary
- [x] CG-2.1e.11 layout-driver approval CLI surfacing
- [x] CG-2.1e.12 layout-approved encoded count bridge
- [x] CG-2.1e.13 layout-approved local count guard
- [x] CG-2.1e.14 encoded-count local guard capability discovery
- [x] CG-2.2a filtered-count readiness core contract
- [x] CG-2.2a.1 filtered-count blocker precision hardening
- [x] CG-2.2b filtered-count readiness CLI integration
- [x] CG-2.3a projection readiness semantic hardening
- [x] CG-2.3b projection readiness CLI integration
- [~] CG-2.1+ non-metadata query primitive execution remains deferred pending actual encoded-data execution
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
- [x] CG-5.1 metadata query primitive correctness fixtures
- [x] CG-5.2 metadata query primitive edge and diagnostic fixtures
- [~] golden Vortex fixtures
- [~] reference outputs
- [~] null/nested/dictionary/sparse/run-length/temporal edge-case coverage
- [x] CG-5.3 correctness fixture manifest contract
- [x] CG-5.4 Spark/Polars/DataFusion external baselines only, never fallback

### CG-6 detailed checklist
- [x] CG-6.1 benchmark evidence manifest
- [x] CG-6.2 benchmark claim gate
- [x] CG-6.3 benchmark comparison report contract
- [x] CG-6.4 benchmark reproducibility manifest
- [x] CG-6.5 reproducibility-aware benchmark claim gate
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
- [x] CG-7.1 physical operator/kernel contract foundation
- [x] CG-7.2 physical operator capability discovery
- [x] CG-7.3 physical kernel registry plan
- [x] CG-7.4 physical kernel admission gate
- [x] CG-7.5 physical operator execution profiles
- [x] CG-7.6 physical kernel selection gate
- [x] CG-7.7 physical operator planning certificate
- [x] CG-7.8 Vortex query primitive physical-operator bridge
- [x] CG-7.9 execution-level kernel requirements
- [x] CG-7.10 metadata-result physical operator bridge
- [x] CG-7.11 metadata bridge admission evidence
- [x] CG-7.12 metadata-only physical kernel report
- [x] CG-7.13 metadata physical kernel CLI surfacing
- [x] CG-7.14 metadata kernel capability discovery
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
- [x] CG-19 sufficiency gates and per-path certificate disqualifiers documented
- [~] implementation pending
- [ ] preserve representation state, pushdown evidence, materialization boundaries, and sink constraints without default decode

### CG-20 detailed checklist
- [x] RFC 0032 contract deepening complete
- [x] World-class sufficiency report, best-default dossier linkage, and claim disqualifiers documented
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
- [x] CG-1.2 metadata/footer execution path has a feature-gated local fixture invocation helper.
- [x] CG-2.1 metadata-footer count execution bridge consumes the local fixture footer summary.
- [x] CG-2.1d encoded-data count candidate bridge can defer approved count candidates to `NeedsEncodedRead`.
- [x] CG-2.1e.1 encoded-data count readiness is now gated by the encoded-read probe and preserves public API blockers.
- [x] CG-2.1e.2 exact Vortex data-access API classification keeps scan/stream/evaluation surfaces blocked for execution.
- [x] CG-2.1e.3 count readiness now names exact blocked API-boundary surfaces from the encoded-read probe.
- [x] CG-2.1e.4 local encoded-count admission rejects reports with named API-boundary blockers.
- [x] CG-2.1e.5 `VortexFile::row_count` is approved as metadata-only public API evidence.
- [x] CG-2.1e.6 encoded-count data-path approval blocks current traversal until an execution-usable public data path exists.
- [x] CG-2.1e.7 encoded-count approval CLI exposes the current blocker without execution.
- [x] CG-2.1e.8 local encoded-count approval guard rejects current blockers before deferred execution planning.
- [x] CG-2.1e.9 layout-reader construction remains blocked by runtime-driver risk; layout row count alone is not encoded-count execution evidence.
- [x] CG-2.1e.10 layout-driver approval is explicit and report-only before any future row-count-only layout reader path.
- [x] CG-2.1e.11 layout-driver approval CLI exposes the report-only boundary with deterministic signals and no side effects.
- [x] CG-2.1e.12 layout-approved encoded count bridge lets encoded-count approval consume a matching, side-effect-free layout-row-count-only approval report while keeping actual layout-reader construction and data reads disabled.
- [x] CG-2.1e.13 layout-approved local count guard feeds the approved report into local execution as deferred `NeedsEncodedRead` planning while preserving no-read/no-decode/no-fallback effects.
- [x] CG-2.1e.14 encoded-count local guard capability discovery exposes the deferred guard in `capabilities operators` with static no-read/no-decode/no-fallback evidence.
- [x] CG-5.1 metadata query primitive correctness fixtures cover supported metadata answers and deferred unsupported paths without side effects.
- [x] CG-5.2 metadata query primitive edge and diagnostic fixtures cover missing/unsupported metadata primitive paths without side effects.
- [x] CG-5.3 correctness fixture manifest declares initial golden fixture/reference output and required edge-case fixture families without execution.
- [x] CG-5.4 external baseline oracle policy declares comparison-only baselines and blocks runtime fallback.
- [x] CG-6.1 benchmark evidence manifest covers required metric categories without running benchmarks.
- [x] CG-6.2 benchmark claim gate blocks publication without correctness, benchmark, comparison, metric, and no-fallback evidence.
- [x] CG-6.3 benchmark comparison report contract records missing scenario/baseline results and metric gaps without running benchmarks or invoking external baselines.
- [x] CG-6.4 benchmark reproducibility manifest records dataset, engine-version, hardware, OS, runtime, cache, reproduction-step, correctness, and no-fallback metadata requirements before any benchmark evidence can count.
- [x] CG-6.5 reproducibility-aware benchmark claim gate blocks publication unless comparison evidence and reproducible run metadata are both present.
- [x] CG-7.1 physical operator/kernel contract foundation declares filter, projection, and count aggregate kernel blockers without implementing kernels or execution.
- [x] CG-7.2 physical operator capability discovery exposes missing-kernel/readiness counts through `shardloom capabilities operators` without executing operators or probing runtime inputs.
- [x] CG-7.3 physical kernel registry plan exposes required native kernel slots through `shardloom kernel-registry` without registering kernels or executing runtime paths.
- [x] CG-7.4 physical kernel admission gate blocks reference-only kernels, fallback attempts, missing correctness evidence, and missing memory-safety evidence before a native kernel slot can be marked present.
- [x] CG-7.5 physical operator execution profiles declare metadata/encoded/hybrid/native-decoded levels for foundation operators while blocking reference-only, row-materialized, Arrow, and fallback paths.
- [x] CG-7.6 physical kernel selection gate rejects disallowed execution levels and missing kernel slots before any physical kernel can be selected.
- [x] CG-7.7 physical operator planning certificate summarizes operator, registry, selection, and admission evidence while keeping runtime execution disabled.
- [x] CG-7.8 Vortex query primitive physical-operator bridge lowers count/filter/project primitives into physical operator plans and certificates without executing kernels.
- [x] CG-7.9 execution-level kernel requirements separate metadata-only, encoded-native, hybrid-native, and native-decoded kernel blockers.
- [x] CG-7.10 metadata-result physical operator bridge maps existing metadata answers to metadata-only physical operator readiness while keeping admission evidence separate.
- [x] CG-7.11 metadata bridge admission evidence lets already metadata-answered bridges reach native planning or production certificate states only when explicit correctness, memory-safety, benchmark, and no-fallback evidence is supplied.
- [x] CG-7.12 metadata-only physical kernel report evaluates certificate-gated metadata count/filter physical kernel reports over already metadata-answered primitive results without data reads, decode, materialization, IO, or fallback execution.
- [x] CG-7.13 metadata physical kernel CLI surfacing exposes the report through `vortex-metadata-physical-kernel-plan` with explicit evidence flags and side-effect/no-fallback fields.
- [x] CG-7.14 metadata kernel capability discovery surfaces contextual metadata physical kernel availability and evidence requirements in `capabilities operators` and `kernel-registry` without treating those contextual reports as global runtime kernels.
- [~] CG-2.1+ non-metadata execution remains blocked pending actual encoded data execution.
- [~] CG-3 real Vortex payload writes remain deferred; placeholder artifact paths are not completion evidence.

## Guardrails
- No Spark/DataFusion/Polars/DuckDB/Velox/vortex-datafusion fallback execution or delegation.
- Unsupported behavior must fail explicitly with deterministic diagnostics.
- Vortex is native input and highest-fidelity native output.
- Compatibility outputs are translation/export targets, not execution fallback.
- Keep docs/cleanup queue visible when active; do not skip directly to CG work by default.
- Preserve both canonical phase IDs and CG gate visibility; do not treat CG IDs as replacements.
- Keep Foundry under CG-18 as optional deployment/comparison context only.
- Competitive claims require CG-5 correctness and CG-6 benchmarks.
