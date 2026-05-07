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

### Source pushdown statuses
- `exact`
- `exact_with_residual`
- `conservative_may_include_false_positives`
- `unsupported`
- `unsafe_rejected`

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

## Dependency policy distinction
Spark/DataFusion and other engines remain external baselines for comparison, not runtime dependencies or fallback paths.

## Relationship to RFC 0025 and CG-20
RFC 0025 defines competitive gates; this RFC specifies CG-20 capability contracts and evidence expectations.

## Future implementation phases
Future phases may incrementally implement matrices, scorecards, analyzer reports, and certification workflows without adding execution fallback.
