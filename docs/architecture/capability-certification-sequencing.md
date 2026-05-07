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

- [ ] Decide whether these are subcommands under existing `capabilities` or separate commands.
- [ ] Define stable JSON fields before text output.
- [ ] Include unsupported reasons and next-step hints.
- [ ] Include `fallback_execution_allowed=false`.
- [ ] Include effect-safe behavior: no filesystem, network, catalog, or adapter probing by default.
- [ ] Link output shape to `FeatureFootprintReport` where appropriate.

Acceptance:

- Capability commands are safe, side-effect-free, and deterministic.
- Capability discovery does not infer runtime availability from external baseline availability.

## R5.4.3 SQL frontend sequencing

Goal: keep SQL as a frontend into ShardLoom-native planning.

Checklist:

- [ ] Define parse-only stage.
- [ ] Define bind/validate stage.
- [ ] Define native logical-plan lowering stage.
- [ ] Define native physical-plan lowering stage.
- [ ] Define unsupported SQL diagnostics.
- [ ] Define semantic profile selection.
- [ ] Define SQL coverage snapshot output.
- [ ] Define dependency approval policy before adding any parser dependency.

Acceptance:

- SQL does not own execution.
- Unsupported SQL fails explicitly.
- Parser dependency decisions are deferred to an explicit dependency/RFC pass.

## R5.4.4 Operator and function certification sequencing

Goal: expand capability breadth without hiding execution maturity.

Checklist:

- [ ] Define operator-family certification statuses.
- [ ] Define per-operator memory/spill flags.
- [ ] Define function-family certification statuses.
- [ ] Define function metadata fields for null behavior, determinism, volatility, effects, types, encoded capability, and materialization.
- [ ] Define `test_reference_only` evidence boundaries.
- [ ] Define native decoded, encoded-capable, compressed-native, streaming, spill, distributed, benchmarked, and production-certified transitions.
- [ ] Link operator/function status to correctness and benchmark gates.

Acceptance:

- `test_reference_only` never appears as production-capable.
- Every production-capable operator/function declares materialization and fallback status.

## R5.4.5 Adapter certification sequencing

Goal: make common adapters useful and certifiable without turning them into fallback execution paths.

Checklist:

- [ ] Define adapter maturity levels A0-A7.
- [ ] Define source capability reports.
- [ ] Define sink requirement reports.
- [ ] Define pushdown exactness statuses.
- [ ] Define residual expression reporting.
- [ ] Define metadata and fidelity loss reporting.
- [ ] Define encoded-representation preservation reporting.
- [ ] Define read/write/commit/streaming/object-store-range support fields.
- [ ] Define external source pushdown as proof-backed source behavior, not hidden execution.

Acceptance:

- Adapters can provide data, metadata, pushdown, or output targets.
- Adapters cannot execute ShardLoom plan fragments as fallback.
- Every adapter path can emit native I/O certificate evidence when CG-19 implementation exists.

## R5.4.6 Semantic profile and migration sequencing

Goal: make migration from incumbent engines measurable and explicit.

Checklist:

- [ ] Define semantic profile behavior dimensions.
- [ ] Define Spark-compatible, DataFusion-compatible, Postgres-like, ANSI-strict, and ShardLoom-native profile status fields.
- [ ] Define migration report fields for supported constructs.
- [ ] Define migration report fields for unsupported constructs.
- [ ] Define semantic-difference reporting.
- [ ] Define function-difference reporting.
- [ ] Define adapter-difference reporting.
- [ ] Define rewrite suggestion shape.
- [ ] Define performance/cost delta estimates with uncertainty and evidence labels.
- [ ] Define Vortex conversion payback estimate shape.

Acceptance:

- Migration reports do not promise gains without evidence.
- Semantic differences are explicit before execution.
- External engines remain comparison and migration baselines only.

## R5.4.7 Workload constitution and scorecard sequencing

Goal: scope best-default-engine certification to declared workloads.

Checklist:

- [ ] Define workload constitution record shape.
- [ ] Define workload categories and required coverage evidence.
- [ ] Define scorecard dimension weights as optional/deferred.
- [ ] Define scorecard dimensions for correctness, performance, cost, memory safety, SQL coverage, function coverage, operator coverage, adapter coverage, API usability, observability, migration ease, deployment ease, and no-fallback integrity.
- [ ] Define claim-level requirements for scorecard publication.

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
3. Add CLI capability discovery for report-only surfaces.
4. Add snapshot tests for generated empty/planned matrices.
5. Add SQL/operator/function/adapter coverage entries incrementally as real implementation appears.

Do not start with a parser, adapter runtime, or kernel implementation before the report contracts and capability discovery surfaces exist.
