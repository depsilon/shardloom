# RFC 0032: World-Class SQL, Operator, Function, Adapter, and User Capability Surface

## Summary
This RFC defines CG-20 as the final user-capability certification gate for ShardLoom. It expands competitive scope beyond narrow Vortex acceleration into user-visible capability breadth, common data/ETL coverage, universal adapter fitness, and certified workload fitness.

## Motivation
Real users choose engines for end-to-end capability: SQL/function/operator breadth, adapters, Python ergonomics, UDFs, unstructured and media data handling, semantics, APIs, migration ergonomics, diagnostics, and certification confidence.

## Goals
- Define capability-certification contracts for SQL/operators/functions/adapters/user surfaces.
- Define common data and ETL capability expectations for ingestion, cleaning, transformation, incremental processing, writes, and export.
- Define Python wrapper/API certification as a first-class CG-20 user surface.
- Define UDF/plugin and unstructured media capability expectations without weakening side-effect and materialization boundaries.
- Define maturity ladders and conformance scorecards.
- Preserve no-fallback execution constraints.

## Non-goals
- no SQL parser implementation in this PR
- no DataFusion/Spark/Trino/DuckDB/Polars/Velox fallback
- no external engine execution
- no SQL execution delegation
- no adapter runtime implementation
- no mature Python API, DataFrame, notebook, Python UDF, package publication, or
  native binding implementation
- no unstructured media runtime implementation
- no OCR, LLM, embedding, vector, image, audio, or video processing dependency additions
- no broad dependency additions

## CG-20 definition
CG-20 is the final user-capability gate that defines evidence required before ShardLoom can be certified as the best default engine for declared workload constitutions. It is not only a fast subset-executor gate.

## Common data and ETL capability scope
CG-20 must cover the common work users expect from a serious local analytical and ETL engine. SQL support is necessary but insufficient; users also need Python-first ergonomics, safe UDFs, common source/sink adapters, unstructured data intake, pipeline diagnostics, and migration paths.

Minimum common data/ETL surface:
- tabular file ingestion and export
- local filesystem and object-store source/sink paths
- schema discovery, schema evolution, and type coercion diagnostics
- data contracts for required columns, types, nullability, uniqueness, ordering, partitioning, and freshness
- projection, filtering, cleaning, deduplication, enrichment, joins, aggregations, windows, sorts, limits, and set operations
- reshaping operations such as rename, cast, parse, explode/unnest, flatten, pivot/unpivot where supported, and nested-field projection
- data quality handling for rejected rows, quarantine outputs, invalid records, parse failures, duplicate keys, and constraint violations
- incremental recompute, CDC-like change intake, merge/delete/update where table semantics support it
- pipeline state, checkpoints, watermarks, idempotency keys, and replay boundaries where incremental or streaming workloads require them
- partition discovery, partition pruning, compaction planning, and layout health reporting
- batch and bounded-streaming modes with backpressure and memory/spill declarations
- orchestration boundaries for dry-run, plan, execute, retry, cancel, resume, and certify flows
- lineage, provenance, audit, redaction, credential, and data-retention reporting where adapters or unstructured inputs require governance
- explain, estimate, profile/analyze, and capability discovery for every supported pipeline stage
- explicit materialization, fidelity-loss, metadata-loss, and external-effect reporting
- deterministic unsupported diagnostics and rewrite suggestions
- workload-scoped correctness, semantic conformance, benchmark, adapter, and no-fallback evidence

Common data/ETL coverage families:
- `ingestion`: files, partitioned datasets, object stores, relational sources, warehouses, event/log sources, API sources, unstructured references, and Vortex-native inputs.
- `schema_contracts`: discovery, declared schema, type coercion, nullability, evolution, nested types, compatibility, and rejected-schema diagnostics.
- `cleaning_quality`: parsing, normalization, deduplication, missing-value handling, constraint checks, quarantine, rejected-record reporting, and data-quality metrics.
- `transformation`: projection, casts, derived columns, string/date/time/numeric transforms, nested-field transforms, explode/unnest, joins, aggregates, windows, sorts, limits, and set operations.
- `enrichment`: lookup joins, UDF enrichment, external API enrichment, LLM/model enrichment, embedding enrichment, and explicit effect/materialization boundaries.
- `incremental_state`: snapshots, CDC/change sets, watermarks, checkpoints, replay, idempotency, reuse, merge/update/delete, and stateful recompute.
- `write_export`: append, overwrite, partitioned write, merge/upsert/delete where table semantics allow it, compatibility export, commit/recovery, and sink fidelity.
- `pipeline_operations`: dry run, explain, estimate, profile/analyze, retry, cancellation, cleanup, certification, and machine-readable diagnostics.

`DataEtlCoverageEntry` fields:
- `family`
- `capability_name`
- `status`
- `source_requirements`
- `sink_requirements`
- `operator_requirements`
- `function_requirements`
- `adapter_requirements`
- `python_api_requirements`
- `udf_extension_requirements`
- `unstructured_media_requirements`
- `semantic_profile_requirements`
- `memory_spill_requirements`
- `state_checkpoint_requirements`
- `materialization_requirement`
- `external_effect_requirement`
- `diagnostic_requirements`
- `correctness_evidence_ref`
- `benchmark_evidence_ref`
- `native_io_certificate_refs`
- `execution_certificate_refs`
- `fallback_attempted=false`

`DataEtlCapabilityReport` fields:
- `report_id`
- `workload_constitution_ref`
- `pipeline_surface_status`
- `coverage_entries`
- `ingestion_capabilities`
- `transformation_capabilities`
- `cleaning_capabilities`
- `data_quality_capabilities`
- `schema_contract_status`
- `join_aggregate_window_capabilities`
- `incremental_recompute_capabilities`
- `cdc_merge_delete_update_capabilities`
- `write_export_capabilities`
- `partition_layout_capabilities`
- `batch_mode_status`
- `bounded_streaming_status`
- `memory_spill_status`
- `schema_evolution_status`
- `state_checkpoint_status`
- `orchestration_boundary_status`
- `lineage_provenance_refs`
- `quarantine_policy`
- `data_contract_status`
- `credential_effect_boundary_status`
- `source_adapter_refs`
- `sink_adapter_refs`
- `native_io_certificate_refs`
- `execution_certificate_refs`
- `materialization_boundaries`
- `fidelity_loss_reports`
- `unsupported_operations`
- `rewrite_suggestions`
- `correctness_status`
- `benchmark_status`
- `diagnostics`
- `fallback_attempted=false`

ETL certification boundaries:
- A pipeline stage is not supported merely because one SQL clause or function exists; source, operator, memory, sink, and observability evidence must line up for the declared workload.
- Cleaning and enrichment UDFs must carry function/extension metadata and materialization/effect boundaries.
- Incremental, merge, delete, update, compaction, and commit paths require explicit table/adapter semantics and commit/recovery evidence.
- Object-store pipelines require request-budget, range-read, retry, idempotency, and cleanup diagnostics.
- Unstructured extraction, OCR, LLM calls, embedding generation, and external APIs are effectful operations unless a later native path is certified.
- Python convenience APIs cannot certify an ETL capability unless the underlying native plan, adapters, diagnostics, materialization boundaries, and evidence reports are certified for that workload.
- A pipeline with mixed structured and unstructured inputs must certify both the tabular path and the unstructured/media reference or extraction path.

## Capability certification surface
Contract names:
- `CompetitiveClaimLevel`
- `SqlCoverageMatrix`
- `OperatorCoverageMatrix`
- `FunctionCoverageMatrix`
- `OperatorCertificationStatus`
- `AdapterCertificationReport`
- `SourceCapabilityReport`
- `SourcePushdownReport`
- `SinkRequirementReport`
- `SemanticProfile`
- `MigrationCompatibilityReport`
- `BestChoiceScorecard`
- `WorkloadConstitution`
- `WorldClassSufficiencyReport`
- `DataEtlCapabilityReport`
- `UniversalAdapterCatalog`
- `PythonSurfaceReport`
- `UnstructuredMediaCapabilityReport`
- `ApiSurfaceReport`
- `ObservabilityCertificationReport`
- `DeploymentReadinessReport`
- `ExtensionCapabilityReport`
- `SecurityGovernanceReport`

## World-class sufficiency gate
CG-20 is complete only when the capability surface can prove that ShardLoom is the best default option for a declared workload constitution. The proof must be explicit, machine-readable, workload-scoped, and reversible to `not_certified` when evidence drifts.

`WorldClassSufficiencyReport` fields:
- `report_id`
- `workload_constitution_ref`
- `claim_level`
- `sql_surface_status`
- `operator_surface_status`
- `function_surface_status`
- `adapter_surface_status`
- `semantic_profile_status`
- `migration_surface_status`
- `data_etl_surface_status`
- `python_surface_status`
- `unstructured_media_surface_status`
- `universal_adapter_catalog_status`
- `api_surface_status`
- `observability_surface_status`
- `deployment_surface_status`
- `extension_surface_status`
- `security_governance_status`
- `native_io_certificate_coverage`
- `execution_certificate_coverage`
- `correctness_evidence_status`
- `semantic_conformance_status`
- `benchmark_evidence_status`
- `memory_spill_status`
- `unsupported_rate`
- `materialization_rate`
- `performance_regression_budget_status`
- `scorecard_ref`
- `best_default_dossier_ref`
- `capability_snapshot_refs`
- `external_baseline_refs`
- `known_limits`
- `blocking_gaps`
- `publication_decision`
- `diagnostics`
- `fallback_attempted=false`

Required sufficiency decisions:
- `not_certified`: required evidence is missing, stale, planned-only, or blocked.
- `partial_for_workload`: at least one optional or scoped workload area is certified, but one or more mandatory dimensions, categories, or source/sink paths are not.
- `sufficient_for_workload`: all mandatory workload requirements have certified evidence, but public language must remain workload-scoped.
- `best_default_candidate`: sufficient for the workload and benchmark-backed against declared baselines, but pending release/publication approval.
- `best_default_certified`: correctness-backed, benchmark-backed, adapter-certified, native-I/O-certified, migration-documented, no-fallback-certified, and approved for the declared workload only.

Sufficiency invariants:
- No single subsystem can satisfy CG-20 by itself. SQL breadth, Python usability, ETL coverage, operator breadth, function breadth, adapters, unstructured/media data handling, semantics, migration, API ergonomics, observability, deployment posture, extension safety, security/governance, correctness, benchmarks, native I/O certificates, execution certificates, and no-fallback integrity must all be represented.
- Planned, parsed-only, test-reference-only, migration-analysis-only, or benchmark-label-only entries cannot count as production support.
- Optional workload categories must be explicitly marked optional or out of scope before they can be excluded from a sufficiency decision.
- Missing evidence downgrades the publication decision instead of weakening required fields.
- External engines may appear in `external_baseline_refs`, but never as execution availability.
- Public "best", "world-class", "superiority", "replacement", "faster", or "cheaper" language must be derived from the sufficiency decision and cite the declared workload scope.

Disqualifiers:
- Missing CG-5 correctness evidence for any mandatory workload category.
- Missing CG-6 benchmark evidence for any performance, cost, superiority, replacement, or best-default statement.
- Missing CG-16 execution certificate evidence for a supported execution path.
- Missing CG-19 native I/O certificate evidence for a required source/sink path.
- Any hidden fallback, delegated execution, or external engine runtime dependency.
- Planned-only SQL/operator/function/adapter/API/observability/deployment/extension/security entries presented as supported.
- Required Python, ETL, unstructured media, or universal-adapter surfaces presented as supported before certification evidence exists.
- Unsupported constructs without deterministic diagnostics and rewrite guidance where possible.
- Unreported materialization, metadata loss, fidelity loss, semantic difference, memory spill gap, or object-store limitation.
- Snapshot drift that changes support status without matching RFC, correctness, benchmark, dependency, and migration evidence.

Explicit deferrals:
- This RFC does not approve a SQL parser dependency, adapter runtime dependency, object-store client dependency, catalog dependency, Python package dependency, media/OCR/LLM/embedding dependency, benchmark runner, client/server dependency, UDF/plugin runtime dependency, external baseline invocation, or execution implementation.
- Future parser, adapter, object-store, catalog, Python, media/OCR/LLM/embedding, benchmark, client/server, UDF/plugin, and external-effect dependencies require their own dependency, license, provenance, no-fallback, and capability-snapshot review.
- CG-20 sufficiency may define required fields before their implementation exists; unimplemented fields must report `not_certified`, `planned`, `unsupported`, or `evidence_insufficient`.

## Competitive claim ladder
`CompetitiveClaimLevel`:
- L0 planning only
- L1 Vortex-native metadata/filter/project/count capability candidate
- L2 local analytical SQL capability candidate for supported operators
- L3 adapter-certified capability candidate across Vortex/Parquet/Arrow/local/object-store
- L4 lakehouse pipeline capability candidate for Spark-style jobs
- L5 broad user-capability parity candidate with DataFusion local SQL
- L6 broad user-capability parity candidate with Spark analytical SQL/pipeline workflows
- L7 best-default-engine certification for declared workload constitution

### Required evidence for each L0-L7 claim
Every level must emit a claim evidence record with fields:
- `correctness`
- `semantic_conformance`
- `benchmark`
- `adapter_certification`
- `fallback_attempted`
- `unsupported_rate`
- `performance_regression_budget`
- `capability_report`
- `comparison_report`

Each evidence field must carry one of:
- `required_passed`
- `required_failed`
- `not_applicable`
- `deferred`
- `not_run`

Progressive requirements:
- L0:
  - correctness required
  - capability report required
  - benchmark/comparison may be `not_applicable` or `deferred`
  - adapter certification may be `not_applicable`
- L1-L2:
  - correctness required
  - semantic conformance required
  - unsupported-rate budget required
  - benchmark may be `deferred` until CG-6 only when no performance, comparison, superiority, or best-default claim is emitted
- L3-L4:
  - adapter certification required
  - comparison reporting required
  - benchmark required for superiority claims
- L5-L7:
  - benchmark evidence required
  - performance regression budget required
  - full comparison reporting required
  - adapter certification required where adapters are part of workload constitution

Cross-level invariants:
- `fallback_attempted=false` is required at every level.
- Claim fields must be emitted even when not applicable so automation can distinguish `not_applicable` from missing data.
- Any output label or public claim containing superiority, best, beat, faster, cheaper, or replacement language requires CG-5 correctness evidence, CG-6 benchmark evidence, and `benchmark=required_passed`.

## SQL coverage tiers
`SqlCoverageMatrix` tiers:
- S0 unsupported
- S1 parsed only
- S2 bound/validated
- S3 native logical plan
- S4 native physical plan
- S5 native decoded execution path, or decoded reference evidence marked `test_only`
- S6 encoded-capable native path
- S7 benchmarked and certified

## SQL frontend sequencing contract
SQL support must advance through explicit frontend stages before any construct can be represented as native execution capability.

`SqlFrontendStage`:
- `declared_only`: the construct appears in the roadmap but has no parser behavior.
- `parse_only`: SQL text can be parsed into syntax structure only; no catalog, type, function, or execution semantics are implied.
- `bound_validated`: names, types, functions, and semantic profile are validated against ShardLoom capability contracts.
- `native_logical_plan`: the construct lowers into ShardLoom-native logical plan IR with unsupported residuals rejected.
- `native_physical_plan`: the construct has a planned native physical representation with materialization boundaries declared.
- `native_execution_ready`: the construct can execute through ShardLoom-native runtime paths without external engine delegation.
- `encoded_capable`: the construct can preserve encoded or selection-vector-aware execution when inputs support it.
- `benchmarked_certified`: correctness, semantic conformance, and benchmark evidence satisfy the relevant claim level.

`SqlFrontendReport` fields:
- `sql_input_ref`
- `parser_status`
- `binder_status`
- `semantic_profile`
- `catalog_resolution_status`
- `function_resolution_status`
- `operator_lowering_status`
- `unsupported_constructs`
- `unsupported_reasons`
- `rewrite_suggestions`
- `materialization_boundaries`
- `capability_report_ref`
- `sql_coverage_snapshot_ref`
- `diagnostics`
- `parser_dependency_status`
- `runtime_execution=false` unless an explicit later execution phase enables native execution
- `fallback_attempted=false`

Stage boundaries:
- `parse_only` must not be reported as support for execution, planning, binding, or semantic conformance.
- `bound_validated` must fail closed when catalog, function, type, or semantic-profile requirements are unknown.
- `native_logical_plan` must reject unsupported SQL residuals instead of carrying them to fallback execution.
- `native_physical_plan` must declare decode/materialization, ordering, partitioning, memory, spill, and sink requirements.
- `native_execution_ready` requires ShardLoom-native runtime support. External engines remain baselines or test oracles only.
- `benchmarked_certified` requires correctness, semantic conformance, benchmark evidence, comparison reports, and `fallback_attempted=false`.
- Any stage transition must be reflected in deterministic SQL coverage snapshot output before broader capability claims are updated.

Parser dependency policy:
- No parser dependency is approved by this RFC section.
- A future parser dependency requires license/provenance review, dependency-footprint review, and an RFC or dependency approval pass.
- Parser libraries may only build syntax/frontend structures; they must not add execution, optimization, catalog, adapter, or fallback behavior.
- Parser failures and unsupported constructs must emit deterministic diagnostics rather than attempting fallback execution.

Unsupported SQL diagnostics must include:
- `feature`
- `stage`
- `semantic_profile`
- `reason`
- `unsupported_construct`
- `rewrite_suggestion`
- `capability_report_ref`
- `fallback_attempted=false`

## SQL surface minimum roadmap
- `SELECT`
- `WITH / CTE`
- `FROM table/subquery`
- `WHERE`
- `projection aliases`
- `GROUP BY`
- `HAVING`
- `ORDER BY`
- `LIMIT / OFFSET`
- `DISTINCT`
- `CASE WHEN`
- `casts`
- `scalar functions`
- `aggregate functions`
- `window functions`
- `subqueries`
- `joins`
- `set operations`
- `CREATE TABLE AS SELECT`
- `INSERT`
- `MERGE / UPDATE / DELETE` where table semantics support it
- `EXPLAIN`
- `ANALYZE / PROFILE`

## Operator coverage matrix
`OperatorCoverageMatrix` tracks operator semantics, representation-state support, and certification status by workload profile and claim level.

Operator families:
- `scan`
- `filter`
- `project`
- `limit`
- `top_k`
- `sort`
- `aggregate`
- `hash_aggregate`
- `sort_aggregate`
- `window`
- `join`
- `hash_join`
- `sort_merge_join`
- `broadcast_join`
- `semi_join`
- `anti_join`
- `range_join`
- `set_union`
- `set_intersect`
- `set_except`
- `repartition`
- `shuffle_exchange`
- `write`
- `commit`
- `compact`
- `merge`
- `delete`

## Operator certification status
`OperatorCertificationStatus` values:
- `unsupported`
- `planned`
- `parsed`
- `planned_native`
- `test_reference_only`
- `native_decoded`
- `encoded_capable`
- `compressed_native`
- `streaming_capable`
- `spill_capable`
- `distributed_capable`
- `benchmarked`
- `production_certified`

`test_reference_only` is correctness or benchmark evidence only. It is not a production execution tier and cannot satisfy production-capability certification without a native execution status such as `native_decoded`, `encoded_capable`, or stronger.

### Operator certification transition rules
Operator certification must advance monotonically through evidence-backed states. A higher status may be reported only for the declared workload profile, semantic profile, input representation states, and sink requirements covered by evidence.

Transition meaning:
- `unsupported`: no parser, planner, or native capability is promised.
- `planned`: roadmap entry only; no execution or planning support is implied.
- `parsed`: a frontend can recognize the construct, but no native plan support is implied.
- `planned_native`: native plan and execution design exists, but no executable native path is certified.
- `test_reference_only`: correctness or benchmark fixture evidence exists; this is never production execution support.
- `native_decoded`: a ShardLoom-native decoded-columnar path exists and declares any decode/materialization boundary.
- `encoded_capable`: the operator can preserve encoded or selection-vector-aware execution for the declared representation states.
- `compressed_native`: the operator can work against compressed/native physical representation without normalizing to decoded Arrow.
- `streaming_capable`: the operator declares streaming behavior, backpressure behavior, and bounded chunk semantics.
- `spill_capable`: the operator declares memory reservation, spill trigger, spill format, cleanup, and deterministic OOM-safe failure behavior.
- `distributed_capable`: the operator declares task partitioning, exchange/shuffle artifacts where required, retry/idempotency, and cleanup semantics.
- `benchmarked`: reproducible benchmark evidence exists for the declared workload profile.
- `production_certified`: correctness, semantic conformance, memory/spill safety, diagnostics, benchmark evidence, and no-fallback invariants are all satisfied.

`OperatorCertificationReport` fields:
- `operator_family`
- `status`
- `semantic_profile`
- `supported_input_representation_states`
- `output_representation_state`
- `memory_certification`
- `materialization_requirement`
- `ordering_requirement`
- `partitioning_requirement`
- `correctness_status`
- `semantic_conformance_status`
- `benchmark_status`
- `diagnostics_status`
- `capability_report_ref`
- `comparison_report_ref`
- `fallback_attempted=false`

Production certification boundaries:
- `test_reference_only`, `native_decoded`, and `benchmarked` are insufficient by themselves for production certification.
- Any transition to `native_decoded` must report the decode/materialization boundary.
- Any transition to `encoded_capable` or stronger must name the representation states it preserves.
- Any transition to `spill_capable` must include memory and cleanup semantics.
- Any transition to `distributed_capable` must include exchange, retry/idempotency, and cleanup semantics.
- Any superiority, best-choice, replacement, faster, or cheaper claim requires CG-5 correctness evidence, CG-6 benchmark evidence, and `fallback_attempted=false`.

## Memory/spill certification per operator
Every operator declaration should include:
- `streaming`
- `bounded_memory`
- `spillable`
- `requires_full_materialization`
- `requires_shuffle`
- `oom_safe`

## Function coverage matrix
`FunctionCoverageMatrix` tracks scalar/aggregate/window/table/UDF support with null/type determinism and certification evidence.

Function groups:
- `comparison`
- `boolean`
- `math`
- `numeric`
- `decimal`
- `string`
- `regex`
- `binary`
- `date`
- `time`
- `timestamp`
- `interval`
- `timezone`
- `conditional`
- `null handling`
- `casts`
- `hashing`
- `encoding-aware predicates`
- `aggregates`
- `approximate aggregates`
- `statistical aggregates`
- `window functions`
- `array/list functions`
- `struct functions`
- `map functions`
- `json functions`
- `uuid/id functions`
- `table functions`
- `metadata functions`
- `system/introspection functions`
- `vector functions`
- `effectful functions`

### Function metadata contract
Each function record should include:
- `name`
- `aliases`
- `category`
- `input types`
- `output type`
- `null behavior`
- `determinism`
- `volatility`
- `effect level`
- `encoded capability`
- `selection vector support`
- `streaming support`
- `spill support`
- `materialization requirement`
- `semantic profile`
- `compatibility notes`
- `test status`
- `benchmark status`

### Function certification transition rules
Function certification uses the shared `CapabilityCertificationStatus` vocabulary plus function metadata. A function group can be marked higher than `planned` only when every included function record has explicit metadata and evidence for the declared semantic profile.

Function status meaning:
- `unsupported`: the function or group is not available and should fail with deterministic diagnostics.
- `planned`: roadmap entry only.
- `partial`: some functions or signatures are available, but capability discovery must list gaps.
- `test_reference_only`: reference fixtures or comparison evidence exist, but no production native implementation is certified.
- `native`: ShardLoom-native implementation exists for declared signatures and semantic profile.
- `certified`: correctness, semantic conformance, benchmark evidence where relevant, and no-fallback invariants are satisfied.
- `blocked`: implementation is blocked by unresolved semantics, dependency policy, materialization risk, or effect-safety concerns.

`FunctionCertificationReport` fields:
- `name`
- `aliases`
- `group`
- `status`
- `input_types`
- `output_type`
- `null_behavior`
- `determinism`
- `volatility`
- `effect_level`
- `encoded_capability`
- `selection_vector_support`
- `streaming_support`
- `spill_support`
- `materialization_requirement`
- `semantic_profile`
- `correctness_status`
- `semantic_conformance_status`
- `benchmark_status`
- `compatibility_notes`
- `diagnostics`
- `fallback_attempted=false`

Function certification boundaries:
- Function groups must not hide unsupported signatures behind a group-level support label.
- `test_reference_only` cannot satisfy production certification.
- `native` requires a ShardLoom-native implementation, not an external engine call.
- `certified` requires correctness and semantic conformance; benchmark evidence is required before any performance or superiority claim.
- Effectful functions must declare effect level, dry-run behavior, permissions, cost/timeout risks, and materialization requirements.
- Any function requiring row materialization or decoded columnar input must report that requirement instead of silently normalizing execution.
- Capability discovery must expose function aliases and semantic-profile differences before migration or compatibility claims are emitted.

## Semantic compatibility profiles
`SemanticProfile` values:
- ShardLoomNative
- SparkCompatible
- DataFusionCompatible
- PostgresLike
- AnsiStrict

Each profile must define:
- `null ordering`
- `division behavior`
- `cast behavior`
- `timestamp/timezone behavior`
- `decimal behavior`
- `NaN behavior`
- `string collation`
- `case sensitivity`
- `identifier quoting`
- `overflow behavior`
- `aggregate empty-input behavior`
- `window frame defaults`

### Semantic profile report fields
`SemanticProfileReport` records profile coverage and evidence before a query, migration report, or compatibility claim relies on those semantics.

Required fields:
- `profile_id`
- `profile_version`
- `profile_status`
- `dimension_statuses`
- `evidence_basis`
- `baseline_system`
- `baseline_version`
- `compatibility_scope`
- `sql_constructs_covered`
- `function_groups_covered`
- `operator_families_covered`
- `adapter_paths_covered`
- `known_differences`
- `unsupported_semantics`
- `requires_rewrite`
- `test_status`
- `differential_baseline_refs`
- `diagnostics`
- `fallback_attempted=false`

`SemanticDimensionStatus` values:
- `undefined`
- `documented`
- `test_reference_only`
- `native_defined`
- `compatibility_mapped`
- `differential_tested`
- `certified`

Profile-specific evidence:
- `ShardLoomNative` defines ShardLoom's default semantics and must document every dimension before production certification.
- `AnsiStrict` maps dimensions to the chosen ANSI SQL interpretation and must report unsupported or intentionally divergent behavior.
- `SparkCompatible` records Spark SQL semantic deltas as migration evidence only; it is not permission to execute through Spark.
- `DataFusionCompatible` records DataFusion semantic deltas as migration evidence only; it is not permission to execute through DataFusion.
- `PostgresLike` records Postgres-style SQL behavior where useful for user expectations and compatibility analysis.

Semantic profile boundaries:
- A compatibility profile is a semantics contract, not an execution mode.
- External engines are comparison baselines, fixture sources, or migration references only.
- Missing or `undefined` dimensions block compatibility certification for affected constructs.
- Query planning must surface semantic differences before certified execution when the requested profile differs from `ShardLoomNative`.
- Function, operator, adapter, and SQL coverage reports must reference the semantic profile used for certification.
- Semantic compatibility does not imply benchmark, superiority, or production certification by itself.
- `fallback_attempted=false` remains required for every profile report.

## Adapter ecosystem and certification
`AdapterCertificationReport` maturity levels:
- A0 declared only
- A1 capability discovery
- A2 schema/metadata discovery
- A3 read support
- A4 pushdown support
- A5 write support
- A6 commit/recovery support
- A7 benchmarked/certified

### Adapter maturity evidence
Adapter maturity levels must represent evidence, not aspirations.

- A0 `declared_only`: adapter identity, source/sink kind, and non-goals are documented. No read, write, pushdown, commit, streaming, or object-store behavior is implied.
- A1 `capability_discovery`: adapter can emit a deterministic capability report without probing external systems by default.
- A2 `schema_metadata_discovery`: adapter can describe schema and metadata availability with explicit diagnostics and no hidden data reads.
- A3 `read_support`: adapter can produce a `NativeWorkStream` or source envelope with declared representation state and no fallback execution.
- A4 `pushdown_support`: adapter can emit `SourcePushdownReport` with exactness, proof basis, residual expression, and unsafe-rejection reasons.
- A5 `write_support`: adapter can consume a `NativeResultStream` with explicit sink requirements, materialization boundaries, and metadata/fidelity reporting.
- A6 `commit_recovery_support`: adapter can describe commit, idempotency, rollback/recovery, cleanup, and side-effect boundaries.
- A7 `benchmarked_certified`: correctness, semantic, fidelity, benchmark, and native I/O certificate evidence exists for the declared workload profile.

Maturity invariants:
- Higher maturity must not be inferred from a lower-level report.
- A3 read support does not imply pushdown support.
- A5 write support does not imply table commit/recovery support.
- A7 benchmarked/certified is workload-scoped and cannot be used for undeclared source/sink paths.
- `fallback_attempted=false` is required at every maturity level.

### AdapterCertificationReport fields
- `adapter_id`
- `adapter_version`
- `source_kind`
- `sink_kind`
- `schema_discovery_status`
- `metadata_discovery_status`
- `statistics_availability`
- `pushdown_capabilities`
- `pushdown_exactness`
- `residual_required`
- `encoded_representation_preserved`
- `materialization_required`
- `metadata_loss`
- `read_supported`
- `write_supported`
- `commit_supported`
- `streaming_supported`
- `object_store_range_supported`
- `correctness_status`
- `benchmark_status`
- `fallback_attempted=false`

Additional certification fields:
- `maturity_level`
- `source_capability_report_ref`
- `source_pushdown_report_ref`
- `sink_requirement_report_ref`
- `adapter_fidelity_report_ref`
- `native_io_certificate_refs`
- `residual_expression`
- `metadata_preserved`
- `statistics_preserved`
- `fidelity_loss`
- `commit_semantics`
- `recovery_semantics`
- `side_effects`
- `diagnostics`

### Source pushdown statuses
- `exact`
- `exact_with_residual`
- `conservative_may_include_false_positives`
- `unsupported`
- `unsafe_rejected`

### Adapter pushdown and fidelity boundaries
Pushdown is source behavior with proof, not fallback execution. An adapter may use source-native capabilities only when the accepted operation, guarantee, and residual expression are visible in `SourcePushdownReport`.

Rules:
- `exact`: source-side filtering/projection is semantically equivalent for the declared semantic profile.
- `exact_with_residual`: accepted source operations are exact, and a residual expression must still run natively in ShardLoom.
- `conservative_may_include_false_positives`: source-side filtering may include extra rows but must not exclude valid rows; native residual execution is required.
- `unsupported`: no pushdown is applied, and native planning must either proceed without pushdown or fail explicitly.
- `unsafe_rejected`: requested pushdown is rejected because semantics, ordering, side effects, or metadata are unsafe.

Residual expression reporting:
- Residuals must be typed and tied to the semantic profile used for source pushdown.
- Residuals must not be executed by an external engine as a hidden fallback.
- Residuals must appear in explain/diagnostic/certificate surfaces before execution is certified.

Metadata and fidelity reporting:
- `metadata_loss` must distinguish dropped schema metadata, statistics, ordering, partitioning, nullability, field identity, layout hints, and source-specific physical metadata.
- `fidelity_loss` must distinguish representation loss, semantic loss risk, precision/coercion risk, commit semantic loss, and sink compatibility loss.
- Encoded preservation must distinguish `vortex_encoded`, `foreign_encoded`, `selection_vector_encoded`, partial decode, decoded columnar, and row materialization.
- Materialization required by a sink or adapter must emit a `MaterializationBoundaryReport` through CG-19 certificate evidence.

Read/write/commit/streaming/object-store fields:
- `read_supported` means the adapter can produce native envelopes for declared source paths.
- `write_supported` means the adapter can consume native result envelopes for declared sink paths.
- `commit_supported` means the adapter can describe side effects, idempotency, recovery, cleanup, and commit visibility.
- `streaming_supported` means the adapter can participate in bounded streaming with backpressure semantics.
- `object_store_range_supported` means byte/range planning is supported with request-budget and retry semantics.

### External source pushdown rules
External systems may provide data, metadata, and proof-backed pushdown. They must not execute ShardLoom residual plans as fallback.

Allowed:
- source metadata discovery with explicit capability reports
- exact or conservative source-side pushdown with proof
- external table/catalog metadata import
- sink writes when explicitly planned and certified

Disallowed:
- delegating unsupported ShardLoom operators to Spark, DataFusion, DuckDB, Polars, Velox, Trino, Dask, Ray, Calcite, or another engine
- hiding remote SQL/query execution behind an adapter without `SourcePushdownReport`
- treating external baseline availability as adapter maturity
- reporting adapter certification without native I/O certificate coverage for each source/sink path

## Common adapters roadmap

### Native
- Vortex source
- Vortex sink
- Vortex manifest/snapshot

### Common analytical files
- Parquet source/sink
- Arrow IPC source/sink
- Arrow C Stream / FFI later
- CSV source/sink utility
- JSON / NDJSON source/sink utility
- Avro/ORC later
- compressed text/file wrappers where safe and explicit
- partitioned directory datasets

### Lakehouse/table
- Iceberg-compatible table metadata
- Delta-compatible table metadata
- Hive-style partition discovery
- table snapshot import/export
- schema evolution adapter
- delete/tombstone adapter
- CDC adapter

### Object stores
- local filesystem
- S3-compatible
- Google Cloud Storage
- Azure Blob / ADLS
- HTTP range-read when safe

### Catalogs
- local catalog
- Hive-compatible catalog
- Iceberg REST-compatible catalog
- Glue-like catalog adapter
- Nessie-like catalog adapter

### Relational and warehouse sources
- PostgreSQL-compatible source later
- MySQL/MariaDB-compatible source later
- SQLite/local embedded source utility later
- Snowflake-like warehouse import/export analyzer later
- BigQuery-like warehouse import/export analyzer later
- generic JDBC/ODBC source bridge only after no-fallback and pushdown-proof rules are satisfied

Relational/warehouse boundaries:
- Remote systems may provide tables, metadata, snapshots, and proof-backed source pushdown.
- Remote systems must not execute unsupported ShardLoom residual plans as fallback.
- SQL pushdown into remote systems must be represented as source pushdown with exactness, residual, and semantic-difference reporting.
- Federated reads must expose materialization, network, credential, consistency, and retry boundaries.

### Event, API, and SaaS sources
- local log/event file source utility
- webhook/event manifest import later
- Kafka-like stream source later
- Kinesis/Pub/Sub-like stream source later
- REST/HTTP JSON source later
- GraphQL source later
- SaaS/API connector manifests later
- explicit API enrichment adapter later

Event/API/SaaS boundaries:
- Event and API sources are adapters or explicit external effects; they must not become hidden execution engines.
- API calls, webhook pulls, SaaS reads, model calls, and external enrichment require credential, timeout, retry, rate-limit, idempotency, and cost diagnostics.
- Stream sources require bounded state, checkpoint, watermark, replay, backpressure, and cancellation semantics before certification.
- Remote filtering or projection is source pushdown only when exactness, residuals, semantic differences, and unsafe-rejected operations are reported.
- Capability discovery must not call remote APIs, open streams, validate credentials, or inspect private schemas by default.

### Unstructured and media inputs
- text files
- HTML and XML documents
- PDF/document references
- office document references
- image metadata and binary object references
- audio/video metadata and binary object references
- log/event records
- archive/container manifests
- extracted text/chunk manifests
- embedding/vector references where explicitly enabled

Unstructured/media boundaries:
- Native analytical execution operates on typed references, metadata, extracted fields, chunks, and manifests.
- OCR, speech-to-text, document parsing, LLM calls, embedding generation, and media decoding are explicit effectful extensions unless a later native path is certified.
- Raw binary/media payloads must not be silently decoded during capability discovery, explain, estimate, dry run, or planning.
- Extracted fields must record provenance, extractor version, confidence/quality when available, redaction status, and materialization cost.
- Unstructured and media adapters must emit native I/O certificate evidence for each source/sink path before they can count toward CG-20.

### Client/server
- CLI JSON runner
- Python API
- Rust API
- HTTP/gRPC query service later
- Flight/FlightSQL-like service later
- JDBC/ODBC bridge later

### Python and notebook
- thin Python wrapper over stable CLI JSON first
- source-tree Python live ETL smoke helpers for current CSV-to-Vortex and native Vortex local test paths
- Python advisory helpers for dynamic sizing/work-shaping and benchmark evidence discovery
- Python query builder/DataFrame-like API later
- notebook display helpers
- Python capability discovery helpers
- Python diagnostics/explain/estimate/profile helpers
- Python UDF boundary declarations
- Python package/release surface later

Python boundaries:
- The first Python wrapper should be a stable, thin, machine-readable client over CLI/API JSON, not a hidden execution engine.
- Current source-tree live ETL helpers are smoke-test conveniences only; they do
  not certify mature ETL, adapter, SQL, DataFrame, or production workload
  capability.
- CG-11 may establish the low-level API/protocol foundation, but CG-20 owns mature Python wrapper, DataFrame/query-builder, notebook, Python UDF, and Python packaging certification.
- Python APIs must not imply pandas/Polars/Spark/DataFusion execution fallback.
- DataFrame-like APIs must lower into ShardLoom-native capability checks and plans.
- Any conversion to pandas, Arrow, NumPy, or Python objects is a materialization boundary with explicit diagnostics.
- Python UDFs require explicit type, null, determinism, volatility, effect, sandbox/resource, and materialization metadata.
- Python must surface every unsupported construct, materialization boundary, external effect, credential requirement, and fallback-attempt field that the CLI/API JSON surface emits.

### Migration
- Spark SQL migration analyzer
- DataFusion compatibility analyzer
- Substrait-like import/export validator
- plan portability report
- adapter certification report

## Migration analyzers
Define:
- `SparkMigrationReport`
- `DataFusionMigrationReport`
- `DuckDBPolarsMigrationReport`
- `SqlCompatibilityReport`
- `PlanPortabilityReport`
- `MigrationCompatibilityReport`

`MigrationCompatibilityReport` compares a declared workload constitution against supported SQL/operators/functions/adapters and reports explicit deltas.

Migration reports must include:
- `supported constructs`
- `unsupported constructs`
- `semantic differences`
- `function differences`
- `adapter differences`
- `materialization requirements`
- `rewrite suggestions`
- `expected performance/cost delta estimate` (gain only when evidence-backed)
- `Vortex conversion payback`
- `fallback_attempted=false`

### MigrationCompatibilityReport fields
`MigrationCompatibilityReport` is report-only evidence for moving a declared workload toward ShardLoom-native execution.

Required fields:
- `report_id`
- `source_system`
- `source_version`
- `target_semantic_profile`
- `workload_ref`
- `workload_constitution_ref`
- `sql_constructs`
- `operator_families`
- `function_groups`
- `adapter_paths`
- `supported_constructs`
- `unsupported_constructs`
- `semantic_differences`
- `function_differences`
- `adapter_differences`
- `materialization_requirements`
- `rewrite_suggestions`
- `capability_report_refs`
- `semantic_profile_report_refs`
- `adapter_certification_report_refs`
- `native_io_certificate_refs`
- `expected_performance_cost_delta`
- `vortex_conversion_payback`
- `risk_level`
- `evidence_label`
- `diagnostics`
- `fallback_attempted=false`

`ConstructMigrationStatus` values:
- `supported_native`
- `supported_with_rewrite`
- `supported_with_materialization`
- `supported_with_semantic_difference`
- `requires_adapter`
- `requires_future_phase`
- `unsupported`
- `unsafe_rejected`

Supported construct entries must include:
- `construct`
- `construct_kind`
- `source_semantics`
- `target_semantics`
- `status`
- `semantic_profile`
- `required_operator_status`
- `required_function_status`
- `required_adapter_maturity`
- `materialization_requirement`
- `evidence_label`
- `diagnostics`

Unsupported construct entries must include:
- `construct`
- `construct_kind`
- `unsupported_reason`
- `blocking_capability`
- `semantic_risk`
- `rewrite_available`
- `rewrite_suggestion_refs`
- `requires_future_phase`
- `diagnostics`

Difference reports must distinguish:
- `semantic_difference`: nulls, casts, timestamps, decimals, NaN, collation, case sensitivity, overflow, aggregate empty input, and window frame defaults.
- `function_difference`: missing function, alias mismatch, type coercion difference, null behavior difference, determinism/volatility difference, effect boundary, or materialization requirement.
- `adapter_difference`: missing source/sink, weaker pushdown, metadata loss, fidelity loss, commit semantic loss, object-store range limitation, or streaming limitation.

`RewriteSuggestion` entries must include:
- `suggestion_id`
- `original_construct`
- `replacement_construct`
- `required_semantic_profile`
- `required_adapter_maturity`
- `materialization_requirement`
- `behavior_change_risk`
- `validation_required`
- `confidence`
- `diagnostics`

Performance/cost delta estimate fields:
- `estimate_status`
- `evidence_label`
- `baseline_refs`
- `benchmark_refs`
- `workload_assumptions`
- `uncertainty`
- `expected_runtime_delta`
- `expected_cost_delta`
- `memory_delta`
- `object_store_request_delta`
- `diagnostics`

`estimate_status` values:
- `not_estimated`
- `evidence_insufficient`
- `modeled`
- `benchmark_backed`
- `measured`

Vortex conversion payback fields:
- `candidate_source`
- `conversion_scope`
- `conversion_cost_estimate`
- `storage_delta`
- `metadata_pruning_benefit`
- `encoded_execution_benefit`
- `repeat_count_payback_threshold`
- `incremental_refresh_payback`
- `payback_uncertainty`
- `recommendation`
- `diagnostics`

Migration boundaries:
- Migration analyzers do not execute migrated workloads.
- Migration analyzers do not add compatibility execution modes.
- External engines must not be invoked as runtime fallbacks.
- External benchmark or differential evidence must be labeled as evidence, not execution availability.
- Expected gains must be expressed as deltas with uncertainty unless benchmark-backed evidence exists.
- Unsupported constructs must produce actionable diagnostics or rewrite suggestions when possible.
- `fallback_attempted=false` is mandatory in every migration report.

## Join/window/shuffle blockers
CG-20 cannot complete without native support plans for:
- inner/outer/semi/anti joins
- broadcast hash join
- partitioned hash join
- sort-merge join
- spillable joins
- skew handling
- runtime filters
- window functions
- external sort
- top-k
- repartition/exchange

## Workload constitution
`WorkloadConstitution` categories:
- metadata-only workloads
- selective scans
- wide sparse projections
- common analytical SQL
- TPC-H-like joins/aggregates
- TPC-DS-like windows/subqueries
- lakehouse insert/merge/delete
- object-store reads/writes
- incremental recompute
- common Parquet/Arrow workloads
- common CSV/JSON/log ingestion and cleaning
- relational source import/export
- document/text extraction pipelines
- unstructured media metadata pipelines
- Python notebook/data science workflows
- Python UDF enrichment workflows
- Vortex-native pipelines
- adapter migration workloads
- BI/dashboard query patterns
- notebook/dataframe patterns

### WorkloadConstitution fields
`WorkloadConstitution` scopes certification to a declared workload instead of allowing broad unqualified claims.

Required fields:
- `constitution_id`
- `workload_name`
- `workload_version`
- `workload_categories`
- `query_patterns`
- `data_source_profiles`
- `sink_target_profiles`
- `semantic_profiles`
- `required_sql_features`
- `required_operator_families`
- `required_function_groups`
- `required_adapter_paths`
- `required_data_etl_capabilities`
- `required_python_surfaces`
- `required_unstructured_media_capabilities`
- `required_api_surfaces`
- `required_observability_surfaces`
- `required_deployment_profiles`
- `required_security_governance_controls`
- `required_extension_surfaces`
- `user_journeys`
- `scale_shape`
- `latency_objective`
- `cost_objective`
- `memory_spill_objective`
- `object_store_profile`
- `freshness_incremental_profile`
- `correctness_fixture_refs`
- `benchmark_scenario_refs`
- `migration_source_refs`
- `certification_scope`
- `out_of_scope`
- `unsupported_budget`
- `materialization_budget`
- `evidence_refs`
- `diagnostics`
- `fallback_attempted=false`

`WorkloadCategoryEvidence` entries must include:
- `category`
- `required_sql_features`
- `required_operator_families`
- `required_function_groups`
- `required_adapter_paths`
- `required_data_etl_capabilities`
- `required_python_surfaces`
- `required_unstructured_media_capabilities`
- `required_semantic_profiles`
- `required_api_surfaces`
- `required_observability_surfaces`
- `required_deployment_profiles`
- `required_security_governance_controls`
- `required_memory_spill_properties`
- `required_source_sink_paths`
- `required_correctness_tests`
- `required_benchmark_scenarios`
- `required_native_io_certificates`
- `unsupported_budget`
- `materialization_budget`
- `evidence_status`
- `diagnostics`

Workload constitution boundaries:
- Certification is valid only for the declared workload constitution and version.
- Adding a category, data source, sink target, semantic profile, operator family, function group, API surface, observability surface, deployment profile, extension surface, or security/governance control requires refreshed evidence.
- Missing category evidence must produce `not_certified` or `evidence_insufficient`, not a partial best-default claim.
- Unsupported and materialization budgets must be explicit; hidden fallback cannot reduce unsupported rate.
- Workload constitutions may reference Spark, DataFusion, DuckDB, Polars, or other incumbent workloads as migration sources, but not as runtime execution paths.
- `fallback_attempted=false` is mandatory.

## Capability discovery UX
Capability discovery must remain deterministic and machine-readable, exposing exact coverage tiers, semantic profiles, and unsupported reasons.

Future commands:
- `shardloom capabilities sql`
- `shardloom capabilities functions`
- `shardloom capabilities operators`
- `shardloom capabilities adapters`
- `shardloom capabilities universal-adapters`
- `shardloom capabilities data-etl`
- `shardloom capabilities python`
- `shardloom capabilities unstructured-media`
- `shardloom capabilities semantic-profiles`
- `shardloom capabilities migration`
- `shardloom capabilities certification`
- `shardloom capabilities api-surfaces`
- `shardloom capabilities observability`
- `shardloom capabilities deployment`
- `shardloom capabilities extensions`
- `shardloom capabilities security-governance`

Capability discovery response fields:
- `scope`
- `schema_version`
- `engine_version`
- `workload_constitution_ref`
- `semantic_profile`
- `entries`
- `summary_counts`
- `unsupported_reasons`
- `next_step_hints`
- `materialization_requirements`
- `external_effect_requirements`
- `required_feature_flags`
- `required_config_refs`
- `side_effects`
- `filesystem_probe=false`
- `network_probe=false`
- `catalog_probe=false`
- `adapter_probe=false`
- `external_engine_invoked=false`
- `fallback_attempted=false`

Capability entry statuses:
- `supported`
- `partially_supported`
- `planned`
- `disabled`
- `requires_feature`
- `requires_config`
- `requires_materialization`
- `requires_external_effect_permission`
- `requires_dependency_review`
- `unsupported`
- `unsafe_rejected`

Capability discovery boundaries:
- Discovery must not parse arbitrary SQL, open files, inspect catalogs, call object stores, probe adapters, invoke external engines, or run benchmark workloads by default.
- Planned entries must remain visible as planned and must not be promoted by documentation wording.
- Unsupported entries must include a stable reason and an actionable next step when a safe rewrite or configuration path exists.
- Any entry requiring materialization, external effects, credentials, writes, or network access must say so before execution.
- Machine-readable fields are part of the user contract and require snapshot coverage once implemented.

## User API and BI/server access roadmap
Roadmap includes CLI/API/BI surfaces as explicit capability layers with no implicit execution delegation. A best-default engine must be easy to call, embed, inspect, automate, and connect to common user workflows without hiding ShardLoom-native execution boundaries.

API surface families:
- `cli_json_runner`
- `rust_api`
- `python_api`
- `dataframe_api`
- `query_builder_api`
- `sql_file_runner`
- `config_job_runner`
- `agent_plan_api`
- `notebook_surface`
- `http_query_service`
- `grpc_query_service`
- `flight_sql_like_service`
- `jdbc_odbc_bridge`
- `bi_dashboard_connector`

`PythonSurfaceReport` fields:
- `surface_id`
- `package_name`
- `wrapper_mode`
- `cli_json_protocol_version`
- `native_api_protocol_version`
- `dataframe_api_status`
- `query_builder_status`
- `notebook_status`
- `capability_discovery_status`
- `explain_estimate_profile_status`
- `materialization_boundary_status`
- `pandas_arrow_numpy_conversion_status`
- `udf_boundary_status`
- `async_cancellation_status`
- `idempotency_key_status`
- `error_mapping_status`
- `schema_version`
- `packaging_status`
- `diagnostics`
- `fallback_attempted=false`

`PythonWrapperMode` values:
- `declared_only`
- `cli_json_thin_wrapper`
- `native_api_client`
- `dataframe_query_builder`
- `notebook_integrated`
- `workload_certified`

Python wrapper acceptance boundaries:
- The wrapper belongs under CG-20 user capability because it is a primary adoption surface, not an execution shortcut.
- The first acceptable wrapper is thin over stable CLI/API JSON and preserves machine-readable diagnostics.
- Python must not call pandas, Polars, Spark, DuckDB, DataFusion, or another engine to execute unsupported ShardLoom plans.
- Python object conversion, pandas/Arrow/NumPy export, row iteration, and UDF calls are materialization/effect boundaries.
- Python package status cannot be `workload_certified` until the underlying CLI/API, SQL/operator/function/adapter, correctness, benchmark, observability, and no-fallback evidence is certified for the declared workload.

`ApiSurfaceMaturity` values:
- `U0_declared`: surface is documented only.
- `U1_discoverable`: surface appears in capability discovery with unsupported/planned diagnostics.
- `U2_dry_run_explain_estimate`: surface can expose plan, explain, estimate, and unsupported diagnostics without execution side effects.
- `U3_native_local_execution`: surface can execute supported local native plans without external delegation.
- `U4_write_commit_gated`: surface can plan and execute supported writes or commits only with explicit safety gates.
- `U5_streaming_profiled`: surface exposes streaming/profile/analyze behavior with bounded-memory reporting.
- `U6_client_server_ready`: surface has authentication, session, cancellation, resource, and protocol boundaries for multi-client use.
- `U7_workload_certified`: surface is certified for the declared workload constitution with correctness, benchmark, observability, and no-fallback evidence.

`ApiSurfaceReport` fields:
- `surface_id`
- `surface_kind`
- `maturity`
- `supported_workload_constitution_refs`
- `supported_input_modes`
- `supported_output_modes`
- `supports_dry_run`
- `supports_explain`
- `supports_estimate`
- `supports_profile`
- `supports_capability_discovery`
- `supports_cancellation`
- `supports_idempotency_keys`
- `supports_streaming`
- `supports_write_safety_gates`
- `native_vortex_output_selectable`
- `compatibility_output_diagnostics`
- `machine_readable_output`
- `schema_version`
- `stability_policy`
- `unsupported_surface_diagnostics`
- `security_governance_report_ref`
- `observability_report_ref`
- `deployment_readiness_report_ref`
- `fallback_attempted=false`

API usability acceptance boundaries:
- Simple read/filter/project/write and explain/estimate flows must not require advanced Vortex internals.
- Advanced controls for materialization, output fidelity, memory, object-store budgets, semantic profiles, and diagnostics must remain explicit and inspectable.
- Python and notebook surfaces must not hide materialization or external effects behind familiar APIs.
- BI/server/client bridges must translate protocol requests into ShardLoom-native capability checks before planning.
- A JDBC/ODBC or FlightSQL-like bridge must not imply DataFusion, Arrow Acero, Spark, or another external execution engine.
- Every public API surface must emit deterministic unsupported diagnostics and `fallback_attempted=false`.

## UDF/plugin strategy
UDF/plugin extensibility must remain typed, explicit about effects/determinism/materialization requirements, and constrained by no-fallback policy. Extension capability is part of CG-20 because real workloads need custom logic, but extensions must not weaken correctness, observability, or security.

Extension surface families:
- SQL-defined scalar UDFs.
- Rust-native scalar UDFs.
- Rust-native aggregate UDFs.
- Rust-native table functions.
- WASM scalar UDFs.
- WASM aggregate UDFs later.
- Python UDFs as explicit materialization/effect boundaries.
- External service UDFs as explicit `ExternalRead`, `ExternalWrite`, or `ModelCall` effects.
- Observability exporters.
- Adapter plugins.

`ExtensionCapabilityReport` fields:
- `extension_id`
- `extension_kind`
- `runtime_kind`
- `maturity`
- `function_metadata_refs`
- `type_signature`
- `null_behavior`
- `determinism`
- `volatility`
- `effect_level`
- `permission_requirements`
- `credential_ref_requirements`
- `sandbox_policy`
- `resource_limits`
- `batch_behavior`
- `streaming_support`
- `encoded_capability`
- `selection_vector_support`
- `materialization_requirement`
- `failure_behavior`
- `timeout_policy`
- `retry_policy`
- `idempotency_policy`
- `redaction_policy`
- `audit_event_policy`
- `license_provenance_status`
- `compatibility_notes`
- `diagnostics`
- `fallback_attempted=false`

Extension certification boundaries:
- Pure deterministic UDFs may become native only after type, null, semantic-profile, correctness, and diagnostics evidence exists.
- Python, external service, LLM, API, embedding, and vector operations require explicit materialization/effect boundaries unless a later native encoded path is certified.
- Effectful extensions must not run during capability discovery, explain, estimate, dry run, or snapshot tests.
- Extension inspection must not execute extension code.
- Missing sandbox, permission, credential, license, or effect metadata blocks certification.
- Extension behavior must be reflected in function/operator/API capability reports before a workload can be certified.

## Unstructured media capability
CG-20 includes unstructured and media data because common ETL work often starts from documents, logs, web content, images, audio, video, archives, and extracted text. ShardLoom should model these as typed source references, extracted metadata, chunks, embeddings, manifests, and explicit effectful extraction stages rather than silently decoding arbitrary media inside the analytical engine.

`UnstructuredMediaCapabilityReport` fields:
- `report_id`
- `workload_constitution_ref`
- `source_kinds`
- `binary_reference_model`
- `metadata_extraction_status`
- `text_extraction_status`
- `chunk_manifest_status`
- `ocr_status`
- `speech_to_text_status`
- `image_metadata_status`
- `audio_video_metadata_status`
- `embedding_reference_status`
- `vector_search_boundary_status`
- `extractor_provenance_status`
- `confidence_quality_fields`
- `redaction_status`
- `external_effect_policy`
- `materialization_boundaries`
- `adapter_certification_refs`
- `native_io_certificate_refs`
- `extension_capability_refs`
- `unsupported_media_types`
- `diagnostics`
- `fallback_attempted=false`

Unstructured media certification boundaries:
- Capability discovery may report supported/planned media categories but must not open, decode, OCR, transcribe, embed, or summarize media by default.
- Extracted text, chunks, and metadata must include provenance and materialization cost before downstream SQL/function/operator certification can use them.
- OCR, speech-to-text, LLM calls, API calls, embedding generation, and vector indexing require explicit effect permissions and extension capability reports.
- Raw media payload handling must preserve credential, privacy, redaction, and data-retention policy.
- Unsupported media types must produce deterministic diagnostics with safe rewrite/import suggestions where possible.

## Observability certification
Observability is a CG-20 certification dimension, not an optional debugging add-on. Users must be able to prove what ShardLoom did, what it avoided, what it could not do, and why no fallback happened.

`ObservabilityCertificationReport` fields:
- `report_id`
- `workload_constitution_ref`
- `explain_coverage`
- `estimate_coverage`
- `profile_analyze_coverage`
- `operator_profile_coverage`
- `kernel_profile_coverage`
- `native_io_certificate_visibility`
- `execution_certificate_visibility`
- `work_avoided_metrics`
- `decode_materialization_metrics`
- `memory_spill_metrics`
- `object_store_metrics`
- `adapter_fidelity_metrics`
- `semantic_difference_visibility`
- `unsupported_diagnostic_quality`
- `redaction_policy_status`
- `agent_readable_output_status`
- `schema_version`
- `diagnostics`
- `fallback_attempted=false`

Observability acceptance boundaries:
- Explain/estimate/profile surfaces must be side-effect-safe unless an explicit execution phase is requested.
- Reports must distinguish planned work from executed work.
- Work avoided, decode avoided, materialization avoided, segments pruned, bytes read, bytes decoded, rows scanned, rows materialized, object-store requests, memory, and spill must be visible where relevant.
- Missing observability for a mandatory workload category blocks best-default certification.
- Redaction must protect secrets, credentials, raw sensitive values, prompts, API payloads, and PII unless explicitly allowed by policy.

## Deployment and operational readiness
Deployment readiness is part of "best default" certification because production users need reproducible installs, safe configuration, resource controls, and operational diagnostics.

`DeploymentReadinessReport` fields:
- `report_id`
- `deployment_profile`
- `package_surface`
- `platform_targets`
- `configuration_surface`
- `resource_limit_surface`
- `object_store_posture`
- `local_filesystem_posture`
- `server_mode_posture`
- `baseline_harness_posture`
- `reproducibility_status`
- `upgrade_compatibility_status`
- `api_schema_compatibility_status`
- `diagnostic_schema_compatibility_status`
- `license_provenance_status`
- `security_scan_status`
- `operational_runbook_status`
- `known_limits`
- `diagnostics`
- `fallback_attempted=false`

Deployment boundaries:
- A deployment profile can be certified only for declared package, OS, hardware, object-store, authentication, and workload assumptions.
- Container, server, and client packages must not bundle fallback engines or imply external execution availability.
- Baseline harnesses remain external comparison tools and must not be included as runtime dependencies.
- Object-store and catalog credentials must be represented by references/handles, not raw values in plans, diagnostics, or reports.
- Upgrade and schema compatibility drift must be visible before release-gated claims.

## Security and governance certification
Security, governance, and agent safety must be explicit for adapters, effects, server/client surfaces, and production deployment.

`SecurityGovernanceReport` fields:
- `report_id`
- `workload_constitution_ref`
- `credential_handling_status`
- `permission_model_status`
- `effect_approval_policy`
- `external_write_policy`
- `destructive_operation_policy`
- `redaction_policy`
- `audit_event_policy`
- `data_classification_policy`
- `tenant_isolation_policy`
- `plugin_sandbox_status`
- `adapter_secret_boundary_status`
- `diagnostic_safety_status`
- `agent_safety_status`
- `known_limits`
- `diagnostics`
- `fallback_attempted=false`

Security and governance boundaries:
- Plans, diagnostics, certificates, traces, and scorecards must not contain raw secrets.
- External reads, external writes, model calls, API calls, and destructive operations require explicit permission and dry-run-safe validation.
- Agent-facing APIs must expose denied/unsupported decisions deterministically instead of silently reducing scope.
- Security/governance gaps block certification for workloads that require the affected controls.

## Correctness and semantic conformance
All capability claims require correctness and semantic conformance evidence before benchmarked superiority claims.

## Feature footprint / best-choice scorecard
`BestChoiceScorecard` summarizes capability coverage, semantic fit, migration friction, and evidence-backed suitability by workload constitution.

### BestChoiceScorecard fields
Required fields:
- `scorecard_id`
- `workload_constitution_ref`
- `scorecard_version`
- `claim_level`
- `dimension_entries`
- `dimension_weights`
- `mandatory_dimensions`
- `unsupported_rate`
- `performance_regression_budget`
- `correctness_report_refs`
- `semantic_profile_report_refs`
- `sql_coverage_report_refs`
- `operator_coverage_report_refs`
- `function_coverage_report_refs`
- `adapter_certification_report_refs`
- `migration_report_refs`
- `native_io_certificate_refs`
- `benchmark_refs`
- `open_blockers`
- `claim_publication_status`
- `diagnostics`
- `fallback_attempted=false`

Scorecard dimensions:
- correctness
- performance
- cost
- memory safety
- SQL coverage
- function coverage
- operator coverage
- adapter coverage
- data/ETL coverage
- Python usability
- unstructured/media coverage
- API usability
- observability
- migration ease
- deployment ease
- security and governance
- extension safety
- no-fallback integrity

`ScorecardDimensionEvidenceStatus` values:
- `not_certified`
- `evidence_insufficient`
- `partially_certified`
- `certified`
- `blocked`

Scorecard dimension entries must include:
- `dimension`
- `status`
- `evidence_label`
- `evidence_refs`
- `blocking_gaps`
- `unsupported_rate`
- `materialization_rate`
- `risk_level`
- `diagnostics`
- `fallback_attempted=false`

Dimension evidence requirements:
- `correctness`: requires CG-5 correctness evidence for the declared workload fixtures.
- `performance`: requires CG-6 reproducible benchmark evidence before any performance, superiority, or best-default claim.
- `cost`: requires an evidence-labeled cost or resource estimate tied to the workload constitution.
- `memory safety`: requires memory, spill, and OOM-safety evidence for required operators and workload scale.
- `SQL coverage`: requires SQL coverage entries for every required SQL feature.
- `function coverage`: requires function certification entries for every required function group and signature class.
- `operator coverage`: requires operator certification entries for every required operator family.
- `adapter coverage`: requires adapter certification and native I/O certificate evidence for required source/sink paths.
- `data/ETL coverage`: requires certified ingestion, transformation, cleaning, incremental, write/export, memory/spill, and observability evidence for the declared workload.
- `Python usability`: requires wrapper/API/report evidence, stable diagnostics, explicit materialization boundaries, and no Python-side fallback execution.
- `unstructured/media coverage`: requires adapter, extractor, provenance, materialization, redaction, effect-permission, and unsupported-diagnostic evidence for required non-tabular sources.
- `API usability`: requires declared CLI/API/client surfaces and diagnostics for unsupported surfaces.
- `observability`: requires diagnostic, explain, estimate, profile, and certificate report coverage for the workload.
- `migration ease`: requires migration compatibility reports and rewrite suggestions for declared migration sources.
- `deployment ease`: requires deployment/import/baseline evidence for the declared environment profile.
- `security and governance`: requires credential, permission, redaction, audit, external-effect, and agent-safety controls for the workload.
- `extension safety`: requires typed metadata, sandbox/effect policy, materialization boundaries, license/provenance status, and unsupported diagnostics for required UDF/plugin surfaces.
- `no-fallback integrity`: requires no-fallback dependency, diagnostic, and execution certificate invariants.

Dimension weights:
- `dimension_weights` are optional and deferred until scorecard implementation.
- Missing weights mean the scorecard is an unweighted evidence summary.
- If weights are provided, they must be explicit, workload-scoped, and must not hide a blocked mandatory dimension.
- Weights cannot turn missing correctness, benchmark, or no-fallback evidence into a publishable claim.

Claim publication requirements:
- A scorecard may always publish `not_certified` with blocking gaps.
- A best-default-engine claim requires the declared workload constitution, mandatory dimensions, correctness evidence, benchmark evidence, semantic conformance, adapter/native I/O certificate evidence, API/observability/deployment/security/extension evidence where required, and no-fallback integrity to be certified.
- Any mandatory dimension with `blocked`, `not_certified`, or `evidence_insufficient` blocks best-default publication.
- Performance or superiority wording is blocked unless benchmark evidence exists for the declared workload.
- Unsupported-rate and materialization-budget thresholds must pass for the declared workload.
- Open correctness, semantic, adapter-fidelity, or no-fallback blockers prevent publication.
- Claim publication must cite evidence refs and report `fallback_attempted=false`.

### BestDefaultCertificationDossier
`BestDefaultCertificationDossier` is the evidence bundle required before ShardLoom can be presented as the best default option for a declared workload.

Required fields:
- `dossier_id`
- `workload_constitution_ref`
- `claim_level`
- `scorecard_ref`
- `world_class_sufficiency_report_ref`
- `correctness_evidence`
- `semantic_conformance_evidence`
- `benchmark_evidence`
- `operator_certification_evidence`
- `function_certification_evidence`
- `adapter_certification_evidence`
- `data_etl_evidence`
- `python_surface_evidence`
- `unstructured_media_evidence`
- `native_io_certificate_evidence`
- `memory_spill_evidence`
- `observability_evidence`
- `migration_evidence`
- `api_ergonomics_evidence`
- `deployment_evidence`
- `security_governance_evidence`
- `extension_safety_evidence`
- `dependency_policy_evidence`
- `no_fallback_evidence`
- `known_limits`
- `blocking_gaps`
- `publication_decision`
- `diagnostics`
- `fallback_attempted=false`

Minimum evidence floor for a world-class claim:
- Correctness fixtures cover empty, single-row, all-null, mixed-null, low-cardinality, high-cardinality, duplicate, sorted, unsorted, invalid-schema, unsupported-encoding, temporal, decimal, string, nested, and ordering-sensitive cases where they apply to the workload.
- Benchmarks identify dataset shape, scale, schema, storage format, compression, engine versions, hardware, OS, runtime configuration, cache state, metrics, reproduction steps, and correctness validation.
- Metrics include wall time, CPU time where available, bytes read, bytes decoded, rows scanned, rows materialized, allocations, peak memory, output bytes, object-store requests where relevant, work avoided, decode avoided, and materialization avoided.
- Operator evidence covers native status, encoded capability, streaming, bounded memory, spill, shuffle, ordering, partitioning, and OOM behavior for every required operator family.
- Function evidence covers aliases, type signatures, null behavior, determinism, volatility, effect level, encoded capability, materialization requirements, semantic profile, tests, and benchmark status for required functions.
- Adapter evidence covers schema/metadata discovery, pushdown exactness, residuals, metadata loss, fidelity loss, encoded preservation, source/sink paths, commit semantics, streaming, object-store range behavior, and per-path native I/O certificates.
- Universal adapter catalog evidence covers required tabular, lakehouse/table, object-store, catalog, relational/warehouse, event/API/SaaS, client/server, Python/notebook, and unstructured/media paths for the declared workload.
- Data/ETL evidence covers ingestion, cleaning, transformation, joins/aggregates/windows, incremental recompute, CDC/merge/delete/update where applicable, write/export, partition/layout behavior, bounded streaming, memory/spill, and pipeline observability.
- Python evidence covers thin CLI/API JSON wrapper status, DataFrame/query-builder status, notebook ergonomics, diagnostics, materialization/export boundaries, UDF boundaries, packaging status, cancellation/idempotency where relevant, and no Python-side fallback execution.
- Unstructured/media evidence covers typed references, metadata extraction, chunk manifests, extractor provenance, redaction, effect permissions, materialization cost, and explicit unsupported diagnostics for required document/media sources.
- Migration evidence covers supported constructs, unsupported constructs, semantic deltas, function deltas, adapter deltas, rewrite suggestions, materialization requirements, uncertainty, and Vortex conversion payback.
- Observability evidence covers diagnostics, explain, estimate, profile/analyze, capabilities, certificates, work-avoided/decode/materialization metrics, redaction, and actionable unsupported-feature reporting.
- API evidence covers CLI, Rust API, Python/API roadmap status, DataFrame/query builder status, BI/server access status, machine-readable output, cancellation/idempotency where relevant, and user-visible unsupported diagnostics.
- Deployment evidence covers local execution, object-store posture, import/export posture, configuration, resource limits, packaging, reproducibility, upgrade/API-schema compatibility, and operational constraints for the declared environment.
- Security/governance evidence covers credential references, permission gates, external-effect approval, destructive-operation policy, redaction, audit, plugin sandboxing, tenant/isolation assumptions, and agent-safe denial diagnostics.
- Extension evidence covers UDF/plugin metadata, sandbox policy, effect level, materialization boundaries, resource limits, license/provenance, and no-execution inspection behavior.
- No-fallback evidence covers dependency invariants, plan/certificate fallback fields, unsupported diagnostics, and external baseline separation.

Disqualifiers for a best-default claim:
- Missing CG-5 correctness evidence for the declared workload.
- Missing CG-6 benchmark evidence for any performance, superiority, cost, or best-default statement.
- Any hidden fallback, delegated execution, or external engine runtime dependency.
- Planned-only SQL, operator, function, adapter, semantic-profile, migration, API, observability, deployment, extension, or security/governance entries presented as supported.
- A mandatory workload category without certified coverage evidence.
- A required source/sink path without native I/O certificate evidence.
- Required Python, ETL, or unstructured/media surfaces without certification evidence.
- CG-11 API/protocol foundation work presented as mature CG-20 Python, DataFrame, notebook, ETL, or adapter certification.
- Unreported materialization, metadata loss, fidelity loss, or semantic divergence.
- Unbounded memory or OOM behavior for required large-state operators.
- Unsupported constructs without deterministic diagnostics.
- API, BI, notebook, server, UDF, plugin, or effectful surfaces that hide materialization, credentials, external calls, destructive operations, or unsupported behavior.
- Missing redaction, permission, audit, or credential-boundary evidence for workloads that require governed execution.
- Scorecard publication without evidence refs and explicit known limits.

Publication decisions:
- `not_certified`: evidence is missing or blockers exist; publish gaps only.
- `partial_for_workload`: some workload categories are certified, but at least one mandatory category, dimension, or path is not.
- `certified_for_workload`: all mandatory workload categories and scorecard dimensions are certified for the declared constitution.
- `best_default_candidate`: certified for the workload and benchmark-backed against declared baselines, but public wording must include scope and limitations.
- `best_default_certified`: benchmark-backed, correctness-backed, adapter-certified, migration-documented, no-fallback-certified, and approved for the declared workload constitution only.

## CI and snapshot drift gates
Capability contracts must not drift silently as SQL, operator, function, adapter, migration, and scorecard surfaces grow.

`CapabilitySurfaceSnapshot` fields:
- `snapshot_id`
- `schema_version`
- `engine_version`
- `snapshot_kind`
- `scope`
- `field_keys`
- `entry_keys`
- `status_counts`
- `certified_counts`
- `planned_counts`
- `unsupported_counts`
- `fallback_attempted`
- `filesystem_probe`
- `network_probe`
- `catalog_probe`
- `adapter_probe`
- `parser_executed`
- `runtime_execution`
- `external_engine_invoked`
- `diagnostics`

Snapshot kinds:
- `diagnostic_schema`
- `capability_discovery_fields`
- `sql_coverage`
- `operator_coverage`
- `function_coverage`
- `adapter_certification`
- `semantic_profiles`
- `migration_compatibility`
- `workload_constitution`
- `best_choice_scorecard`
- `best_default_dossier`
- `world_class_sufficiency`
- `api_surface`
- `observability_certification`
- `deployment_readiness`
- `extension_capability`
- `security_governance`
- `feature_footprint`
- `no_fallback_invariants`

`CapabilityDriftPolicy` fields:
- `policy_id`
- `snapshot_kind`
- `allowed_changes`
- `blocked_changes`
- `requires_rfc_update`
- `requires_snapshot_update`
- `requires_correctness_evidence`
- `requires_benchmark_evidence`
- `requires_dependency_review`
- `requires_security_review`
- `requires_user_migration_note`
- `diagnostics`

Allowed snapshot changes:
- adding planned entries with status `planned` or `unsupported`
- adding diagnostics for newly documented unsupported behavior
- adding fields behind a schema-version bump
- adding certified entries only with matching correctness, semantic, adapter, native I/O, and benchmark evidence where applicable

Blocked snapshot changes:
- changing planned entries to supported without evidence refs
- removing no-fallback fields
- changing fallback flags away from false
- adding external engine execution availability
- dropping diagnostics fields
- dropping unsupported reasons
- changing schema versions without snapshot updates
- changing field names without user/API migration notes
- publishing best-default scorecard changes without workload constitution refs
- publishing world-class sufficiency changes without matching dossier, scorecard, certificate, correctness, benchmark, and no-fallback evidence

CI gate levels:
- `docs_only`: validates docs hygiene, hidden/bidi controls, duplicate headings, and no forbidden dependency/runtime changes.
- `report_only`: validates capability discovery and certification snapshots without parser/runtime/adapter probing.
- `correctness_gated`: requires correctness fixtures and no-fallback invariants before support status changes.
- `benchmark_gated`: requires reproducible benchmark evidence before performance, superiority, cost, or best-default claims.
- `release_gated`: requires API/diagnostic compatibility, migration notes, dependency review, and publication decision checks.

Snapshot execution boundaries:
- Snapshot tests must be deterministic and side-effect-free.
- Snapshot tests must not execute SQL, adapters, file readers, object-store IO, network calls, catalog discovery, external engines, or benchmark runners.
- Snapshot tests may instantiate report-only contract objects and run local CLI capability discovery with explicit no-probe fields.
- Benchmark gates are separate from snapshot gates.
- External baselines may appear as evidence labels only after explicit benchmark/correctness phases.
- Every snapshot surface must keep `fallback_attempted=false`.

## Dependency policy distinction
Spark/DataFusion and other engines remain external baselines for comparison, not runtime dependencies or fallback paths.

## Relationship to RFC 0030 and CG-11
RFC 0030 and CG-11 cover the earlier API/protocol foundation needed to expose ShardLoom safely. CG-20 owns the mature user-capability certification layer: Python wrapper usability, DataFrame/query-builder ergonomics, notebook behavior, Python UDF boundaries, API packaging readiness, common ETL workflows, universal adapters, unstructured/media handling, and best-default workload certification.

CG-11 completion can prove that an API surface exists and preserves native/no-fallback boundaries. It cannot by itself certify that the Python wrapper, ETL surface, adapter catalog, UDF system, or notebook experience is world-class. Those claims require CG-20 evidence, workload-scoped correctness and benchmark gates, native I/O certificates, execution certificates, and no-fallback diagnostics.

## Relationship to RFC 0025 and CG-20
RFC 0025 defines competitive gates; this RFC specifies CG-20 capability contracts and evidence expectations.

## Future implementation phases
Future phases may incrementally implement matrices, scorecards, analyzer reports, and certification workflows without adding execution fallback.
