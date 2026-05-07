# RFC 0032: World-Class SQL, Operator, Function, Adapter, and User Capability Surface

## Summary
This RFC defines CG-20 as the final user-capability certification gate for ShardLoom. It expands competitive scope beyond narrow Vortex acceleration into user-visible capability breadth and certified workload fitness.

## Motivation
Real users choose engines for end-to-end capability: SQL/function/operator breadth, adapters, semantics, APIs, migration ergonomics, diagnostics, and certification confidence.

## Goals
- Define capability-certification contracts for SQL/operators/functions/adapters/user surfaces.
- Define maturity ladders and conformance scorecards.
- Preserve no-fallback execution constraints.

## Non-goals
- no SQL parser implementation in this PR
- no DataFusion/Spark/Trino/DuckDB/Polars/Velox fallback
- no external engine execution
- no SQL execution delegation
- no adapter runtime implementation
- no broad dependency additions

## CG-20 definition
CG-20 is the final user-capability gate that defines evidence required before ShardLoom can be certified as the best default engine for declared workload constitutions. It is not only a fast subset-executor gate.

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

### Client/server
- CLI JSON runner
- Python API
- Rust API
- HTTP/gRPC query service later
- Flight/FlightSQL-like service later
- JDBC/ODBC bridge later

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
- `required_api_surfaces`
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
- `required_semantic_profiles`
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
- Adding a category, data source, sink target, semantic profile, operator family, function group, or API surface requires refreshed evidence.
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
- `shardloom capabilities semantic-profiles`
- `shardloom capabilities migration`

## User API and BI/server access roadmap
Roadmap includes CLI/API/BI surfaces as explicit capability layers with no implicit execution delegation.

## UDF/plugin strategy
UDF/plugin extensibility must remain typed, explicit about effects/determinism/materialization requirements, and constrained by no-fallback policy.

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
- API usability
- observability
- migration ease
- deployment ease
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
- `API usability`: requires declared CLI/API/client surfaces and diagnostics for unsupported surfaces.
- `observability`: requires diagnostic, explain, estimate, profile, and certificate report coverage for the workload.
- `migration ease`: requires migration compatibility reports and rewrite suggestions for declared migration sources.
- `deployment ease`: requires deployment/import/baseline evidence for the declared environment profile.
- `no-fallback integrity`: requires no-fallback dependency, diagnostic, and execution certificate invariants.

Dimension weights:
- `dimension_weights` are optional and deferred until scorecard implementation.
- Missing weights mean the scorecard is an unweighted evidence summary.
- If weights are provided, they must be explicit, workload-scoped, and must not hide a blocked mandatory dimension.
- Weights cannot turn missing correctness, benchmark, or no-fallback evidence into a publishable claim.

Claim publication requirements:
- A scorecard may always publish `not_certified` with blocking gaps.
- A best-default-engine claim requires the declared workload constitution, mandatory dimensions, correctness evidence, benchmark evidence, semantic conformance, adapter/native I/O certificate evidence, and no-fallback integrity to be certified.
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
- `correctness_evidence`
- `semantic_conformance_evidence`
- `benchmark_evidence`
- `operator_certification_evidence`
- `function_certification_evidence`
- `adapter_certification_evidence`
- `native_io_certificate_evidence`
- `memory_spill_evidence`
- `observability_evidence`
- `migration_evidence`
- `api_ergonomics_evidence`
- `deployment_evidence`
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
- Migration evidence covers supported constructs, unsupported constructs, semantic deltas, function deltas, adapter deltas, rewrite suggestions, materialization requirements, uncertainty, and Vortex conversion payback.
- Observability evidence covers diagnostics, explain, estimate, profile/analyze, capabilities, certificates, and actionable unsupported-feature reporting.
- API evidence covers CLI, Rust API, Python/API roadmap status, BI/server access status, machine-readable output, and user-visible unsupported diagnostics.
- Deployment evidence covers local execution, object-store posture, import/export posture, configuration, reproducibility, and operational constraints for the declared environment.
- No-fallback evidence covers dependency invariants, plan/certificate fallback fields, unsupported diagnostics, and external baseline separation.

Disqualifiers for a best-default claim:
- Missing CG-5 correctness evidence for the declared workload.
- Missing CG-6 benchmark evidence for any performance, superiority, cost, or best-default statement.
- Any hidden fallback, delegated execution, or external engine runtime dependency.
- Planned-only SQL, operator, function, adapter, semantic-profile, or migration entries presented as supported.
- A mandatory workload category without certified coverage evidence.
- A required source/sink path without native I/O certificate evidence.
- Unreported materialization, metadata loss, fidelity loss, or semantic divergence.
- Unbounded memory or OOM behavior for required large-state operators.
- Unsupported constructs without deterministic diagnostics.
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

## Relationship to RFC 0025 and CG-20
RFC 0025 defines competitive gates; this RFC specifies CG-20 capability contracts and evidence expectations.

## Future implementation phases
Future phases may incrementally implement matrices, scorecards, analyzer reports, and certification workflows without adding execution fallback.
