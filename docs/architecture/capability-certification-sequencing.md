# Capability Certification Sequencing

## Purpose

This document turns CG-20 from a broad capability RFC into a batchable implementation roadmap.

CG-20 is not permission to add SQL parsing, adapters, execution kernels, external dependencies, or fallback behavior in one pass. It is the certification structure that lets ShardLoom grow toward mature user capability while preserving Vortex-native, no-fallback execution.

## Scope

This sequencing covers:

- SQL coverage reporting.
- Operator coverage reporting.
- Function coverage reporting.
- Adapter certification reporting.
- Semantic profile reporting.
- Migration compatibility reporting.
- Workload constitution and best-choice scorecards.
- Capability discovery and CI snapshot direction.

## Non-goals

- No SQL parser implementation.
- No SQL execution implementation.
- No DataFrame API implementation.
- No adapter runtime implementation.
- No function registry implementation.
- No operator/kernel implementation.
- No dependency additions.
- No fallback execution.
- No superiority claims before CG-5 correctness and CG-6 benchmark evidence.

## Sequencing principles

- Contract before implementation.
- Capability discovery before broad feature work.
- Native execution status before production certification.
- Test/reference evidence before production claims.
- Deterministic unsupported diagnostics before partial implementation.
- Matrix snapshots before large coverage expansion.
- External systems remain baselines, migration references, inputs, or sinks only; never runtime fallback engines.

## R5.4.0 Contract ownership inventory

Goal: decide which crate or future crate owns each CG-20 contract before adding code.

Checklist:

- [x] Map `SqlCoverageMatrix` to a future SQL frontend owner.
- [x] Map `OperatorCoverageMatrix` to plan/exec/kernel owners.
- [x] Map `FunctionCoverageMatrix` to function/kernel owners.
- [x] Map `AdapterCertificationReport` to adapter/native I/O owners.
- [x] Map `SemanticProfile` to SQL/plan/function owners.
- [x] Map `MigrationCompatibilityReport` to plan portability and migration owners.
- [x] Map `BestChoiceScorecard` to capability/certification owners.
- [x] Document which contracts belong in `shardloom-core` first.
- [x] Document which contracts should wait for future crates.

R5.4.1 outcome:

- `shardloom-core` owns the first report-only contract shapes because they are cross-cutting, side-effect-free, and needed by CLI/API/reporting surfaces.
- Future SQL/frontend, plan/exec/kernel, adapter/native I/O, and migration crates own population of those reports as real capabilities land.
- No new crate is added solely for naming clarity.

Acceptance:

- Every CG-20 contract has a clear owner.
- No new crate is added solely for naming clarity.
- No runtime behavior changes.

## R5.4.1 Capability matrix contracts

Goal: define machine-readable report shapes before feature implementation.

Checklist:

- [x] Define SQL coverage matrix fields.
- [x] Define operator coverage matrix fields.
- [x] Define function coverage matrix fields.
- [x] Define adapter certification matrix fields.
- [x] Define semantic profile matrix fields.
- [x] Define migration compatibility matrix fields.
- [x] Define best-choice scorecard fields.
- [x] Define evidence status vocabulary shared across matrices.
- [x] Define `fallback_attempted=false` invariants across every matrix.

R5.4.1 outcome:

- `CapabilityCertificationStatus` provides the shared `unsupported`, `planned`, `partial`, `test_reference_only`, `native`, `certified`, and `blocked` vocabulary.
- `CapabilityCertificationReport::contract_only()` returns planned foundation matrices with `fallback_attempted=false`.
- `SqlCoverageTier` and `OperatorCertificationStatus` keep test/reference evidence from satisfying production certification.

Acceptance:

- Matrices distinguish `unsupported`, `planned`, `partial`, `test_reference_only`, `native`, `certified`, and `blocked`.
- Planned features are never reported as supported.
- Test/reference tiers cannot satisfy production certification.

## R5.4.2 Capability discovery surface

Goal: make future user and agent discovery deterministic before capabilities expand.

Future commands:

- `shardloom capabilities sql`
- `shardloom capabilities functions`
- `shardloom capabilities operators`
- `shardloom capabilities adapters`
- `shardloom capabilities semantic-profiles`
- `shardloom capabilities migration`
- `shardloom capabilities certification`

Checklist:

- [x] Decide whether these are subcommands under existing `capabilities` or separate commands.
- [x] Define stable JSON fields before text output.
- [x] Include unsupported reasons and next-step hints.
- [x] Include `fallback_execution_allowed=false`.
- [x] Include effect-safe behavior: no filesystem, network, catalog, or adapter probing by default.
- [x] Link output shape to `FeatureFootprintReport` where appropriate.

R5.4.2 outcome:

- Scoped capability discovery uses existing `shardloom capabilities <scope>` commands instead of adding separate top-level commands.
- Implemented scopes: `sql`, `functions`, `operators`, `adapters`, `semantic-profiles`, `migration`, and `certification`.
- Existing `shardloom capabilities` remains the engine-level capability summary.
- JSON output uses stable output-envelope fields including `scope`, `schema_version`, `fallback_execution_allowed=false`, `fallback_attempted=false`, and side-effect/probe flags.
- Discovery is report-only and does not parse SQL, execute runtime work, probe adapters, inspect catalogs, read files, perform network I/O, or infer capability from external baseline availability.

Acceptance:

- Capability commands are safe, side-effect-free, and deterministic.
- Capability discovery does not infer runtime availability from external baseline availability.

R5.4.2a outcome:

- Snapshot-style contract tests lock the planned SQL/operator/function/adapter/semantic/migration/scorecard matrices.
- Snapshot-style CLI tests lock the `shardloom capabilities <scope>` JSON field names for `sql`, `functions`, `operators`, `adapters`, `semantic-profiles`, `migration`, and `certification`.
- Certification and `FeatureFootprintReport` no-probe contracts are checked together for engine-version alignment, fallback-disabled state, empty diagnostics, and absence of generated timestamps.
- Tests do not execute external engines, probe filesystem/network/catalog/adapter state, or imply support for planned capabilities.

## R5.4.3 SQL frontend sequencing

Goal: keep SQL as a frontend into ShardLoom-native planning.

Checklist:

- [x] Define parse-only stage.
- [x] Define bind/validate stage.
- [x] Define native logical-plan lowering stage.
- [x] Define native physical-plan lowering stage.
- [x] Define unsupported SQL diagnostics.
- [x] Define semantic profile selection.
- [x] Define SQL coverage snapshot output.
- [x] Define dependency approval policy before adding any parser dependency.

R5.4.3 outcome:

- RFC 0032 now defines `SqlFrontendStage` from `declared_only` through `benchmarked_certified`.
- `SqlFrontendReport` records parser, binder, semantic-profile, catalog, function, operator-lowering, unsupported-construct, materialization, SQL coverage snapshot, diagnostic, dependency, runtime, and fallback fields.
- Parse-only capability is explicitly not execution support.
- Bind/validate must fail closed when catalog, type, function, or semantic-profile requirements are unknown.
- Native logical and physical lowering must reject unsupported residuals and declare materialization/order/partition/memory/spill/sink requirements.
- Parser dependency approval remains deferred to a later dependency/RFC pass.
- No SQL parser, SQL execution, adapter runtime, dependency, or fallback behavior is added.

Acceptance:

- SQL does not own execution.
- Unsupported SQL fails explicitly.
- Parser dependency decisions are deferred to an explicit dependency/RFC pass.

## R5.4.4 Operator and function certification sequencing

Goal: expand capability breadth without hiding execution maturity.

Checklist:

- [x] Define operator-family certification statuses.
- [x] Define per-operator memory/spill flags.
- [x] Define function-family certification statuses.
- [x] Define function metadata fields for null behavior, determinism, volatility, effects, types, encoded capability, and materialization.
- [x] Define `test_reference_only` evidence boundaries.
- [x] Define native decoded, encoded-capable, compressed-native, streaming, spill, distributed, benchmarked, and production-certified transitions.
- [x] Link operator/function status to correctness and benchmark gates.

R5.4.4 outcome:

- RFC 0032 now defines operator certification transition meaning from `unsupported` through `production_certified`.
- `OperatorCertificationReport` fields cover family, status, semantic profile, representation states, memory certification, materialization/order/partition requirements, correctness, semantic conformance, benchmark, diagnostics, report refs, and fallback status.
- Operator production certification requires correctness, semantic conformance, memory/spill safety, diagnostics, benchmark evidence, and no-fallback invariants.
- RFC 0032 now defines function certification status meaning using the shared `CapabilityCertificationStatus` vocabulary.
- `FunctionCertificationReport` fields cover names, aliases, group, types, null behavior, determinism, volatility, effects, encoded/selection-vector/streaming/spill support, materialization, semantic profile, correctness, semantic conformance, benchmarks, diagnostics, and fallback status.
- `test_reference_only` cannot satisfy production certification for operators or functions.
- Performance or superiority claims remain blocked without CG-5 correctness evidence, CG-6 benchmark evidence, and `fallback_attempted=false`.
- No function registry, operator kernel, execution behavior, dependency, or fallback behavior is added.

Acceptance:

- `test_reference_only` never appears as production-capable.
- Every production-capable operator/function declares materialization and fallback status.

## R5.4.5 Adapter certification sequencing

Goal: make common adapters useful and certifiable without turning them into fallback execution paths.

Checklist:

- [x] Define adapter maturity levels A0-A7.
- [x] Define source capability reports.
- [x] Define sink requirement reports.
- [x] Define pushdown exactness statuses.
- [x] Define residual expression reporting.
- [x] Define metadata and fidelity loss reporting.
- [x] Define encoded-representation preservation reporting.
- [x] Define read/write/commit/streaming/object-store-range support fields.
- [x] Define external source pushdown as proof-backed source behavior, not hidden execution.

R5.4.5 outcome:

- RFC 0032 now maps adapter maturity A0-A7 to evidence requirements from declared-only through benchmarked/certified.
- RFC 0032 now states that adapter maturity is workload/path scoped and cannot be inferred from lower-level reports.
- RFC 0032 now defines adapter pushdown and residual-expression boundaries, including exact, exact-with-residual, conservative false-positive, unsupported, and unsafe-rejected behavior.
- RFC 0032 now expands adapter certification with source/sink report refs, fidelity report refs, native I/O certificate refs, metadata/statistics/fidelity loss, commit/recovery semantics, side effects, and diagnostics.
- RFC 0031 now links source capability, sink requirement, adapter fidelity, and native I/O certificate evidence to adapter certification.
- External source pushdown is explicitly proof-backed source behavior, not hidden fallback execution.
- No adapter runtime, object-store IO, file-format dependency, catalog dependency, execution behavior, or fallback behavior is added.

Acceptance:

- Adapters can provide data, metadata, pushdown, or output targets.
- Adapters cannot execute ShardLoom plan fragments as fallback.
- Every adapter path can emit native I/O certificate evidence when CG-19 implementation exists.

## R5.4.6 Semantic profile and migration sequencing

Goal: make migration from incumbent engines measurable and explicit.

Checklist:

- [x] Define semantic profile behavior dimensions.
- [x] Define Spark-compatible, DataFusion-compatible, Postgres-like, ANSI-strict, and ShardLoom-native profile status fields.
- [x] Define migration report fields for supported constructs.
- [x] Define migration report fields for unsupported constructs.
- [x] Define semantic-difference reporting.
- [x] Define function-difference reporting.
- [x] Define adapter-difference reporting.
- [x] Define rewrite suggestion shape.
- [x] Define performance/cost delta estimates with uncertainty and evidence labels.
- [x] Define Vortex conversion payback estimate shape.

R5.4.6 outcome:

- RFC 0032 now defines `SemanticProfileReport` fields, semantic dimension statuses, profile-specific evidence, and compatibility-profile boundaries.
- Semantic compatibility profiles are report contracts, not execution modes.
- Spark, DataFusion, and Postgres-like semantics remain comparison and migration baselines only.
- RFC 0032 now defines `MigrationCompatibilityReport` fields for supported constructs, unsupported constructs, semantic differences, function differences, adapter differences, materialization requirements, rewrite suggestions, evidence labels, and diagnostics.
- RFC 0032 now defines performance/cost delta estimate fields with evidence labels and uncertainty instead of unsupported gain claims.
- RFC 0032 now defines Vortex conversion payback fields for source conversion scope, cost, benefit, uncertainty, and recommendation.
- No compatibility execution mode, migration analyzer runtime, external engine dependency, benchmark claim, or fallback behavior is added.

Acceptance:

- Migration reports do not promise gains without evidence.
- Semantic differences are explicit before execution.
- External engines remain comparison and migration baselines only.

## R5.4.7 Workload constitution and scorecard sequencing

Goal: scope best-default-engine certification to declared workloads.

Checklist:

- [x] Define workload constitution record shape.
- [x] Define workload categories and required coverage evidence.
- [x] Define scorecard dimension weights as optional/deferred.
- [x] Define scorecard dimensions for correctness, performance, cost, memory safety, SQL coverage, function coverage, operator coverage, adapter coverage, API usability, observability, migration ease, deployment ease, and no-fallback integrity.
- [x] Define claim-level requirements for scorecard publication.

R5.4.7 outcome:

- RFC 0032 now defines `WorkloadConstitution` fields that scope certification to declared workload categories, source/sink profiles, semantic profiles, SQL/operator/function/adapter requirements, scale shape, objectives, budgets, fixtures, benchmarks, migration sources, and evidence refs.
- RFC 0032 now defines `WorkloadCategoryEvidence` entries tying each category to required coverage, correctness tests, benchmark scenarios, native I/O certificates, unsupported budgets, materialization budgets, and evidence status.
- RFC 0032 now defines `BestChoiceScorecard` fields, scorecard dimension statuses, dimension evidence entries, optional/deferred weighting rules, mandatory dimension behavior, and claim publication gates.
- RFC 0032 now defines a `BestDefaultCertificationDossier` evidence bundle and disqualifiers for best-default claims.
- Best-default certification remains workload-scoped, evidence-backed, and blocked by missing correctness, benchmark, adapter, native I/O, semantic, observability, or no-fallback evidence.
- No benchmark implementation, certification runtime, external engine dependency, or fallback behavior is added.

Acceptance:

- Scorecards can say "not certified" clearly.
- Best-default-engine certification is workload-scoped and evidence-backed.
- Missing benchmarks block claim publication.

## R5.4.8 CI and snapshot sequencing

Goal: prevent capability surface drift once contracts exist.

Future checks:

- [ ] no-fallback dependency invariant.
- [ ] diagnostic schema snapshot.
- [ ] SQL coverage snapshot.
- [ ] operator coverage snapshot.
- [ ] function coverage snapshot.
- [ ] adapter certification snapshot.
- [ ] semantic profile snapshot.
- [ ] migration compatibility snapshot.
- [ ] best-choice scorecard snapshot.

Acceptance:

- Coverage drift is visible in CI once report contracts exist.
- Snapshot checks do not execute external engines.
- Benchmark gates remain separate from docs-only and report-only work.

## First implementation batches after R5.4

Recommended order:

1. Add core report contracts only. **Complete in R5.4.1.**
2. Add side-effect-free capability report generation with empty/planned matrices. **Foundation report available in R5.4.1; CLI discovery remains next.**
3. Add CLI capability discovery for report-only surfaces. **Complete in R5.4.2.**
4. Add snapshot tests for generated empty/planned matrices. **Complete in R5.4.2a.**
5. Sequence SQL frontend stages before parser/runtime work. **Next: R5.4.3.**
6. Add SQL/operator/function/adapter coverage entries incrementally as real implementation appears.

Do not start with a parser, adapter runtime, or kernel implementation before the report contracts and capability discovery surfaces exist.
