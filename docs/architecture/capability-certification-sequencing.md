# Capability Certification Sequencing

## Purpose

This document turns CG-20 from a broad capability RFC into a batchable implementation roadmap.

CG-20 is not permission to add SQL parsing, adapters, execution kernels, external dependencies, or
fallback behavior in one pass. It is the certification structure that lets ShardLoom grow toward
mature user capability while preserving Vortex-native, no-fallback execution.

Active implementation status, active queue placement, and CG closeout decisions live in
`docs/architecture/phased-execution-plan.md`. This document is the supporting sequencing ledger for
CG-20 capability certification.

## Scope

This sequencing covers:

- SQL coverage reporting.
- Operator coverage reporting.
- Function coverage reporting.
- Adapter certification reporting.
- Semantic profile reporting.
- Migration compatibility reporting.
- Common data/ETL capability reporting.
- Python wrapper/API certification.
- Unstructured media capability reporting.
- Workload constitution and best-choice scorecards.
- Capability discovery and CI snapshot direction.

## Boundary with CG-21, CG-22, and CG-23

CG-20 remains the capability-certification surface for SQL, operators,
functions, adapters, semantic profiles, migration, Python/API, UDFs,
unstructured/media, observability, deployment, extension safety, and
security/governance. CG-21, CG-22, and CG-23 build on that surface instead of
replacing it:

- CG-21 turns capability breadth into complete user workflow certification:
  install/import, discover, read, validate, transform, write, explain, certify,
  benchmark, and diagnose.
- CG-22 adds ShardLoom-native batch/live/hybrid engine-mode contracts beneath
  the user workflow, including boundedness, update mode, output mode,
  freshness, state, delta overlay, hot/cold, and continuous-view evidence.
- CG-23 exposes the same workflow and engine evidence remotely through REST
  control-plane contracts, event-plane contracts, result delivery policies,
  lineage/governance exports, and agent-safe API surfaces.

Implementation order for those gates now lives in the Planned section of
`docs/architecture/phased-execution-plan.md`. This document should not become a
second queue for CG-21, CG-22, or CG-23 work.

## Non-goals

- No SQL parser implementation.
- No SQL execution implementation.
- No DataFrame API implementation.
- No Python wrapper implementation.
- No adapter runtime implementation.
- No unstructured media runtime implementation.
- No OCR, LLM, embedding, vector, image, audio, or video dependency additions.
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
- External systems remain baselines, migration references, inputs, or sinks only; never runtime
  fallback engines.

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

- `shardloom-core` owns the first report-only contract shapes because they are cross-cutting,
  side-effect-free, and needed by CLI/API/reporting surfaces.
- Future SQL/frontend, plan/exec/kernel, adapter/native I/O, and migration crates own population of
  those reports as real capabilities land.
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

- `CapabilityCertificationStatus` provides the shared `unsupported`, `planned`, `partial`,
  `test_reference_only`, `native`, `certified`, and `blocked` vocabulary.
- `CapabilityCertificationReport::contract_only()` returns planned foundation matrices with
  `fallback_attempted=false`.
- `SqlCoverageTier` and `OperatorCertificationStatus` keep test/reference evidence from satisfying
  production certification.

Acceptance:

- Matrices distinguish `unsupported`, `planned`, `partial`, `test_reference_only`, `native`,
  `certified`, and `blocked`.
- Planned features are never reported as supported.
- Test/reference tiers cannot satisfy production certification.

## R5.4.2 Capability discovery surface

Goal: make future user and agent discovery deterministic before capabilities expand.

Future commands:

- `shardloom capabilities sql`
- `shardloom capabilities functions`
- `shardloom capabilities operators`
- `shardloom capabilities adapters`
- `shardloom capabilities universal-adapters`
- `shardloom capabilities event-api-saas-adapters`
- `shardloom capabilities data-etl`
- `shardloom capabilities python`
- `shardloom capabilities dataframe`
- `shardloom capabilities notebook`
- `shardloom capabilities udfs`
- `shardloom capabilities unstructured-media`
- `shardloom capabilities api-surfaces`
- `shardloom capabilities observability`
- `shardloom capabilities deployment`
- `shardloom capabilities extensions`
- `shardloom capabilities security-governance`
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

- Scoped capability discovery uses existing `shardloom capabilities <scope>` commands instead of
  adding separate top-level commands.
- Implemented certification scopes: `sql`, `functions`, `operators`, `adapters`,
  `semantic-profiles`, `migration`, and `certification`.
- Implemented user-surface scopes: `data-etl`, `python`, `dataframe`, `notebook`, `udfs`,
  `universal-adapters`, `event-api-saas-adapters`, `unstructured-media`, `api-surfaces`,
  `observability`, `deployment`, `extensions`, and `security-governance`.
- Existing `shardloom capabilities` remains the engine-level capability summary.
- JSON output uses stable output-envelope fields including `scope`, `schema_version`,
  `fallback_execution_allowed=false`, `fallback_attempted=false`, and side-effect/probe flags.
- Discovery is report-only and does not parse SQL, execute runtime work, probe adapters, inspect
  catalogs, read files, perform network I/O, or infer capability from external baseline
  availability.

Acceptance:

- Capability commands are safe, side-effect-free, and deterministic.
- Capability discovery does not infer runtime availability from external baseline availability.

R5.4.2a outcome:

- Snapshot-style contract tests lock the planned
  SQL/operator/function/adapter/semantic/migration/scorecard matrices.
- Snapshot-style CLI tests lock the `shardloom capabilities <scope>` JSON field names for `sql`,
  `functions`, `operators`, `adapters`, `semantic-profiles`, `migration`, and `certification`.
- Certification and `FeatureFootprintReport` no-probe contracts are checked together for
  engine-version alignment, fallback-disabled state, empty diagnostics, and absence of generated
  timestamps.
- Tests do not execute external engines, probe filesystem/network/catalog/adapter state, or imply
  support for planned capabilities.

R5.4.2b outcome:

- User-surface capability discovery maps broad CG-20 scopes to `WorldClassSufficiencyReport`
  dimensions.
- Each user-surface scope emits evidence requirements, surface component labels, no-probe/no-runtime
  flags, and production/best-default claim blockers.
- Snapshot-style CLI tests lock user-surface field keys, scope names, report-only flags, and
  representative dimension mappings.
- User-surface discovery remains evidence-only; it does not implement Python, DataFrame, notebook,
  UDF, ETL, adapter, media, API/server, deployment, extension, security, parser, runtime, probe, or
  fallback behavior.

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
- `SqlFrontendReport` records parser, binder, semantic-profile, catalog, function,
  operator-lowering, unsupported-construct, materialization, SQL coverage snapshot, diagnostic,
  dependency, runtime, and fallback fields.
- Parse-only capability is explicitly not execution support.
- Bind/validate must fail closed when catalog, type, function, or semantic-profile requirements are
  unknown.
- Native logical and physical lowering must reject unsupported residuals and declare
  materialization/order/partition/memory/spill/sink requirements.
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
- [x] Define function metadata fields for null behavior, determinism, volatility, effects, types,
      encoded capability, and materialization.
- [x] Define `test_reference_only` evidence boundaries.
- [x] Define native decoded, encoded-capable, compressed-native, streaming, spill, distributed,
      benchmarked, and production-certified transitions.
- [x] Link operator/function status to correctness and benchmark gates.

R5.4.4 outcome:

- RFC 0032 now defines operator certification transition meaning from `unsupported` through
  `production_certified`.
- `OperatorCertificationReport` fields cover family, status, semantic profile, representation
  states, memory certification, materialization/order/partition requirements, correctness, semantic
  conformance, benchmark, diagnostics, report refs, and fallback status.
- Operator production certification requires correctness, semantic conformance, memory/spill safety,
  diagnostics, benchmark evidence, and no-fallback invariants.
- RFC 0032 now defines function certification status meaning using the shared
  `CapabilityCertificationStatus` vocabulary.
- `FunctionCertificationReport` fields cover names, aliases, group, types, null behavior,
  determinism, volatility, effects, encoded/selection-vector/streaming/spill support,
  materialization, semantic profile, correctness, semantic conformance, benchmarks, diagnostics, and
  fallback status.
- `test_reference_only` cannot satisfy production certification for operators or functions.
- Performance or superiority claims remain blocked without CG-5 correctness evidence, CG-6 benchmark
  evidence, and `fallback_attempted=false`.
- No function registry, operator kernel, execution behavior, dependency, or fallback behavior is
  added.

Acceptance:

- `test_reference_only` never appears as production-capable.
- Every production-capable operator/function declares materialization and fallback status.

## R5.4.4a Approximate aggregate and sketch function sequencing

Goal: make DataFusion/Polars-style approximate aggregate capability a
certifiable ShardLoom-native function lane instead of a scalar-only shortcut.

Checklist:

- [x] Define approximate distinct aliases and the canonical
  `approx_count_distinct(col)` surface.
- [x] Require ungrouped and grouped approximate distinct coverage.
- [x] Require partial sketch construction, merge, serialization,
  deserialization, sketch versioning, hash-seed metadata, and error-bound
  evidence.
- [x] Require encoded-aware sketch strategy evidence for dictionary,
  run-length, validity, selection-vector, and partial-decode cases.
- [x] Link approximate/sketch production certification to CG-5, CG-6, CG-7,
  CG-13, CG-16, and CG-19 evidence.
- [ ] Implement approximate aggregate function registry entries.
- [ ] Implement sketch state, merge, serialization, and encoded-aware update
  kernels.
- [ ] Add exact-reference fixtures, error-distribution benchmarks, and
  execution/Native I/O certificates.

R5.4.4a outcome:

- RFC 0032 now treats approximate aggregates as a first-class CG-20 function
  family with explicit grouped aggregation, sketch-state, error-bound,
  serialization, merge, null/type, encoded-layout, correctness, benchmark, and
  certificate requirements.
- DataFusion and Polars are compatibility baselines for naming and user
  expectations only; they are not runtime dependencies or fallback engines.
- No function registry, sketch implementation, operator kernel, dependency,
  benchmark claim, production certification, or fallback behavior is added.

Acceptance:

- Approximate/sketch functions remain `planned` or `evidence_insufficient`
  until grouped execution, mergeable serialized state, exact-reference
  comparisons, error bounds, benchmark evidence, and no-fallback certificates
  are available.

## R5.4.5 Adapter certification sequencing

Goal: make common adapters useful and certifiable without turning them into fallback execution
paths.

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

- RFC 0032 now maps adapter maturity A0-A7 to evidence requirements from declared-only through
  benchmarked/certified.
- RFC 0032 now states that adapter maturity is workload/path scoped and cannot be inferred from
  lower-level reports.
- RFC 0032 now defines adapter pushdown and residual-expression boundaries, including exact,
  exact-with-residual, conservative false-positive, unsupported, and unsafe-rejected behavior.
- RFC 0032 now expands adapter certification with source/sink report refs, fidelity report refs,
  native I/O certificate refs, metadata/statistics/fidelity loss, commit/recovery semantics, side
  effects, and diagnostics.
- RFC 0031 now links source capability, sink requirement, adapter fidelity, and native I/O
  certificate evidence to adapter certification.
- External source pushdown is explicitly proof-backed source behavior, not hidden fallback
  execution.
- No adapter runtime, object-store IO, file-format dependency, catalog dependency, execution
  behavior, or fallback behavior is added.

Acceptance:

- Adapters can provide data, metadata, pushdown, or output targets.
- Adapters cannot execute ShardLoom plan fragments as fallback.
- Every adapter path can emit native I/O certificate evidence when CG-19 implementation exists.

## R5.4.6 Semantic profile and migration sequencing

Goal: make migration from incumbent engines measurable and explicit.

Checklist:

- [x] Define semantic profile behavior dimensions.
- [x] Define Spark-compatible, DataFusion-compatible, Postgres-like, ANSI-strict, and
      ShardLoom-native profile status fields.
- [x] Define migration report fields for supported constructs.
- [x] Define migration report fields for unsupported constructs.
- [x] Define semantic-difference reporting.
- [x] Define function-difference reporting.
- [x] Define adapter-difference reporting.
- [x] Define rewrite suggestion shape.
- [x] Define performance/cost delta estimates with uncertainty and evidence labels.
- [x] Define Vortex conversion payback estimate shape.

R5.4.6 outcome:

- RFC 0032 now defines `SemanticProfileReport` fields, semantic dimension statuses, profile-specific
  evidence, and compatibility-profile boundaries.
- Semantic compatibility profiles are report contracts, not execution modes.
- Spark, DataFusion, and Postgres-like semantics remain comparison and migration baselines only.
- RFC 0032 now defines `MigrationCompatibilityReport` fields for supported constructs, unsupported
  constructs, semantic differences, function differences, adapter differences, materialization
  requirements, rewrite suggestions, evidence labels, and diagnostics.
- RFC 0032 now defines performance/cost delta estimate fields with evidence labels and uncertainty
  instead of unsupported gain claims.
- RFC 0032 now defines Vortex conversion payback fields for source conversion scope, cost, benefit,
  uncertainty, and recommendation.
- No compatibility execution mode, migration analyzer runtime, external engine dependency, benchmark
  claim, or fallback behavior is added.

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
- [x] Define scorecard dimensions for correctness, performance, cost, memory safety, SQL coverage,
      function coverage, operator coverage, adapter coverage, API usability, observability,
      migration ease, deployment ease, and no-fallback integrity.
- [x] Define claim-level requirements for scorecard publication.

R5.4.7 outcome:

- RFC 0032 now defines `WorkloadConstitution` fields that scope certification to declared workload
  categories, source/sink profiles, semantic profiles, SQL/operator/function/adapter requirements,
  scale shape, objectives, budgets, fixtures, benchmarks, migration sources, and evidence refs.
- RFC 0032 now defines `WorkloadCategoryEvidence` entries tying each category to required coverage,
  correctness tests, benchmark scenarios, native I/O certificates, unsupported budgets,
  materialization budgets, and evidence status.
- RFC 0032 now defines `BestChoiceScorecard` fields, scorecard dimension statuses, dimension
  evidence entries, optional/deferred weighting rules, mandatory dimension behavior, and claim
  publication gates.
- RFC 0032 now defines a `BestDefaultCertificationDossier` evidence bundle and disqualifiers for
  best-default claims.
- Best-default certification remains workload-scoped, evidence-backed, and blocked by missing
  correctness, benchmark, adapter, native I/O, semantic, observability, or no-fallback evidence.
- No benchmark implementation, certification runtime, external engine dependency, or fallback
  behavior is added.

Acceptance:

- Scorecards can say "not certified" clearly.
- Best-default-engine certification is workload-scoped and evidence-backed.
- Missing benchmarks block claim publication.

## R5.4.8 CI and snapshot sequencing

Goal: prevent capability surface drift once contracts exist.

Future checks:

- [x] no-fallback dependency invariant.
- [x] diagnostic schema snapshot.
- [x] SQL coverage snapshot.
- [x] operator coverage snapshot.
- [x] function coverage snapshot.
- [x] adapter certification snapshot.
- [x] semantic profile snapshot.
- [x] migration compatibility snapshot.
- [x] workload constitution snapshot.
- [x] best-choice scorecard snapshot.
- [x] best-default dossier snapshot.
- [x] world-class sufficiency snapshot.

R5.4.8 outcome:

- RFC 0032 now defines `CapabilitySurfaceSnapshot` fields for schema versions, field keys, entry
  keys, status counts, certification counts, no-probe flags, external-engine invocation flags,
  diagnostics, and fallback status.
- RFC 0032 now defines snapshot kinds for diagnostics, capability discovery, SQL, operators,
  functions, adapters, semantic profiles, migration compatibility, workload constitutions,
  scorecards, best-default dossiers, world-class sufficiency, feature footprint, and no-fallback
  invariants.
- RFC 0032 now defines `CapabilityDriftPolicy` fields and allowed/blocked snapshot changes.
- RFC 0032 now separates docs-only, report-only, correctness-gated, benchmark-gated, and
  release-gated CI levels.
- Snapshot execution remains deterministic, side-effect-free, report-only, no-probe, and
  no-fallback.
- No new tests, benchmark gates, runtime behavior, dependency, external engine probing, or fallback
  behavior are added in this docs-only sequencing pass.

Acceptance:

- Coverage drift is visible in CI once report contracts exist.
- Snapshot checks do not execute external engines.
- Benchmark gates remain separate from docs-only and report-only work.

## R5.4.9 RFC sufficiency hardening

Goal: make the CG-19/CG-20 RFC set explicit enough to govern best-default-engine claims without
relying on scattered prose.

Checklist:

- [x] Add a canonical best-default evidence gate to RFC 0025.
- [x] Add CG-19 per-path native I/O certificate sufficiency gates and disqualifiers to RFC 0031.
- [x] Add `WorldClassSufficiencyReport` fields, decisions, invariants, disqualifiers, and explicit
      deferrals to RFC 0032.
- [x] Tie world-class sufficiency to workload constitutions, best-choice scorecards, best-default
      dossiers, CG-16 execution certificates, CG-19 native I/O certificates, CG-5 correctness, CG-6
      benchmarks, snapshots, and no-fallback evidence.

R5.4.9 outcome:

- RFC 0025 now blocks best-default, best-choice, replacement, superiority, faster, cheaper, and
  world-class claims without a workload-scoped evidence chain.
- RFC 0031 now defines native I/O certificate fields and sufficiency gates for source/sink paths,
  representation preservation, pushdown proof, sink requirements, fidelity loss, materialization,
  object-store/streaming/commit semantics, and no-fallback boundaries.
- RFC 0032 now defines `WorldClassSufficiencyReport` as the final CG-20 sufficiency decision surface
  and records explicit deferrals for parser, adapter, object-store, catalog, benchmark, baseline,
  and execution work.
- Capability sufficiency remains docs/RFC-only in this pass.
- No runtime behavior, SQL parser, SQL execution, adapter runtime, benchmark implementation,
  dependency, external-engine probing, or fallback behavior is added.

Acceptance:

- "Best" and "world-class" language is evidence-scoped and workload-scoped.
- Missing evidence downgrades publication to `not_certified` or `partial_for_workload`.
- External engines remain baseline evidence only, never runtime execution.

## R5.4.10 User-surface RFC hardening

Goal: make the final CG-20 RFC detailed enough to cover the user-facing surfaces that determine
whether ShardLoom can become the best default engine choice, not only a strong SQL/operator/adapter
implementation.

Checklist:

- [x] Define API/client/server surface families and maturity levels.
- [x] Define `ApiSurfaceReport` fields for CLI, Rust, Python, DataFrame/query builder, agent,
      notebook, BI, and service surfaces.
- [x] Define capability discovery response fields and statuses that expose support, partial support,
      planned work, feature/config requirements, materialization, external effects, dependency
      review, unsupported, and unsafe-rejected states.
- [x] Define `ExtensionCapabilityReport` fields for UDF/plugin runtime kind, metadata, effects,
      sandboxing, permissions, resource limits, materialization, license/provenance, diagnostics,
      and no-fallback behavior.
- [x] Define `ObservabilityCertificationReport` fields for explain, estimate, profile/analyze,
      operator/kernel profile, certificate visibility, work-avoided/decode/materialization metrics,
      redaction, and agent-readable output.
- [x] Define `DeploymentReadinessReport` fields for packaging, configuration, resource limits,
      object-store posture, server posture, reproducibility, compatibility, license/provenance,
      security scans, and runbooks.
- [x] Define `SecurityGovernanceReport` fields for credentials, permissions, external effects,
      destructive operations, redaction, audit, data classification, plugin sandboxing, adapter
      secret boundaries, and agent safety.
- [x] Add these user-surface dimensions to workload constitutions, scorecards, best-default
      dossiers, and capability-snapshot kinds.

R5.4.10 outcome:

- RFC 0032 no longer leaves API/BI/server access, UDF/plugin safety, observability, deployment, and
  security/governance as shallow roadmap placeholders.
- CG-20 best-default certification requires evidence for user-facing surfaces as well as SQL,
  operators, functions, adapters, semantics, migration, correctness, benchmarks, and native I/O.
- Capability discovery remains deterministic and no-probe by default, with explicit status values
  for planned, disabled, feature/config-gated, materialization-gated, effect-gated,
  dependency-review-gated, unsupported, and unsafe-rejected entries.
- No API implementation, server implementation, UDF/plugin runtime, SQL parser, adapter runtime,
  dependency, external probing, or fallback behavior is added.

Acceptance:

- Best-default certification cannot ignore API ergonomics, observability, deployment,
  security/governance, or extension safety.
- User-surface reports remain workload-scoped, evidence-backed, and no-fallback.
- Docs stay aligned with RFC 0010, RFC 0011, RFC 0018, RFC 0019, RFC 0023, RFC 0024, RFC 0030, and
  RFC 0032.

## R5.4.12 Common data/ETL and Python/media surface expansion

Goal: make CG-20 broad enough for world-class common analytical and ETL adoption, including
Python-first workflows, UDF enrichment, universal adapters, and unstructured/media data handling.

Checklist:

- [x] Define common data/ETL coverage beyond SQL clauses.
- [x] Define common data/ETL coverage families for ingestion, schema contracts, cleaning/quality,
      transformation, enrichment, incremental state, write/export, and pipeline operations.
- [x] Define `DataEtlCoverageEntry` fields for per-capability
      source/sink/operator/function/adapter/Python/UDF/unstructured requirements.
- [x] Define `DataEtlCapabilityReport` fields for ingestion, transformation, cleaning, data quality,
      incremental processing, writes/exports, partition/layout behavior, state/checkpoint behavior,
      bounded streaming, memory/spill, lineage/provenance, orchestration, and pipeline
      observability.
- [x] Define Python wrapper/API ownership under CG-20 with a thin CLI/API JSON wrapper first.
- [x] Define `PythonSurfaceReport` fields for wrapper mode, protocol versions,
      DataFrame/query-builder status, notebook support, materialization/export boundaries, UDF
      boundaries, wheel/sdist build readiness, Conda package split, fresh-environment smoke status,
      deterministic missing-binary diagnostics, optional benchmark extras, diagnostics, and
      no-fallback behavior.
- [x] Clarify that CG-11 may establish API/protocol foundations while CG-20 owns mature Python
      wrapper, DataFrame/query-builder, notebook, Python UDF, and packaging certification.
- [x] Expand universal adapter roadmap to relational/warehouse sources, event/API/SaaS sources,
      partitioned datasets, compressed wrappers, and unstructured/media source references.
- [x] Define unstructured/media capability boundaries for typed references, extracted
      text/chunks/metadata, extractor provenance, redaction, effect permissions, and materialization
      cost.
- [x] Add ETL/Python/unstructured-media dimensions to workload constitutions, scorecards,
      best-default dossiers, sufficiency status, and disqualifiers.

R5.4.12 outcome:

- RFC 0032 now treats SQL as one part of common data/ETL support, not the whole CG-20 surface.
- Python wrapper/API work is explicitly owned by CG-20 user capability, starting with a thin stable
  JSON client and preserving no-fallback/materialization diagnostics.
- CG-11 is the API/protocol foundation gate; CG-20 is the mature Python/user-capability
  certification gate.
- UDFs, Python UDFs, unstructured media extraction, OCR/LLM/embedding/vector paths, and external
  APIs remain explicit effectful/materialization boundaries until later certified native paths
  exist.
- Universal adapters include tabular files, table/lakehouse metadata, object stores, catalogs,
  relational/warehouse sources, event/API/SaaS sources, client/server bridges, Python/notebook
  surfaces, and unstructured/media references.
- No parser, Python package, adapter runtime, media runtime, OCR/LLM/embedding dependency, execution
  behavior, external probing, or fallback behavior is added.

Acceptance:

- Best-default certification cannot ignore ETL, Python, unstructured/media, or universal-adapter
  evidence when those surfaces are in scope for the workload constitution.
- Python and unstructured/media surfaces do not hide materialization, external effects, credentials,
  or unsupported behavior.
- Adapter and source pushdown rules continue to distinguish proof-backed source behavior from
  fallback execution.

## First implementation batches after R5.4

Recommended order:

- [x] Add core report contracts only. Complete in R5.4.1.
- [x] Add side-effect-free capability report generation with empty/planned matrices. Foundation
      report available in R5.4.1.
- [x] Add CLI capability discovery for report-only surfaces. Complete in R5.4.2.
- [x] Add snapshot tests for generated empty/planned matrices. Complete in R5.4.2a.
- [x] Sequence SQL frontend stages before parser/runtime work. Complete in R5.4.3.
- [x] Sequence operator/function certification. Complete in R5.4.4.
- [x] Sequence adapter certification. Complete in R5.4.5.
- [x] Sequence semantic profile and migration reporting. Complete in R5.4.6.
- [x] Sequence workload constitution, scorecards, and sufficiency evidence. Complete in R5.4.7
      through R5.4.9.
- [x] Harden user-surface certification for API, BI/server, observability, deployment, extension
      safety, and security/governance. Complete in R5.4.10.
- [x] Expand common data/ETL, Python wrapper, universal adapter, and unstructured/media coverage.
      Complete in R5.4.12.
- [ ] Add SQL/operator/function/adapter/API/observability/deployment coverage entries incrementally
      as real implementation appears.
- [ ] Add report-only discovery scopes for data/ETL, Python, unstructured media, universal adapters,
      API surfaces, observability, deployment, extensions, and security/governance after the current
      scoped capability surface is stable.

Do not start with a parser, adapter runtime, or kernel implementation before the report contracts
and capability discovery surfaces exist.
