# RFC 0032: World-Class SQL, Operator, Function, Adapter, and User Capability Surface

## Summary
This RFC defines CG-20 as the final capability-supremacy gate for ShardLoom. It expands competitive scope beyond narrow Vortex acceleration into user-visible capability breadth and certified workload fitness.

## Motivation
Real users choose engines for end-to-end capability: SQL/function/operator breadth, adapters, semantics, APIs, migration ergonomics, diagnostics, and certification confidence.

## Goals
- Define capability-supremacy contracts for SQL/operators/functions/adapters/user surfaces.
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
CG-20 is the final capability-supremacy gate validating that ShardLoom is the best default engine for declared workload constitutions, not only a fast subset executor.

## Capability supremacy surface
Contract names:
- `CompetitiveClaimLevel`
- `SqlCoverageMatrix`
- `OperatorCoverageMatrix`
- `FunctionCoverageMatrix`
- `AdapterCertificationReport`
- `SourceCapabilityReport`
- `SourcePushdownReport`
- `SinkRequirementReport`
- `SemanticProfile`
- `MigrationCompatibilityReport`
- `BestChoiceScorecard`

## Competitive claim ladder
`CompetitiveClaimLevel`:
- L0 planning only
- L1 Vortex-native metadata/filter/project/count superiority
- L2 local analytical SQL superiority for supported operators
- L3 adapter-certified superiority across Vortex/Parquet/Arrow/local/object-store
- L4 lakehouse pipeline superiority over Spark-style jobs
- L5 broad user-capability parity with DataFusion local SQL
- L6 broad user-capability parity with Spark analytical SQL/pipeline workflows
- L7 best-default-engine certification for declared workload constitution

## SQL coverage tiers
`SqlCoverageMatrix` tiers:
- S0 unsupported
- S1 parsed only
- S2 bound/validated
- S3 native logical plan
- S4 native physical plan
- S5 executable decoded reference path
- S6 encoded-capable native path
- S7 benchmarked and certified

## Operator coverage matrix
`OperatorCoverageMatrix` tracks operator semantics, representation-state support, and certification status by workload profile and claim level.

## Function coverage matrix
`FunctionCoverageMatrix` tracks scalar/aggregate/window/table/UDF support with null/type determinism and certification evidence.

## Semantic compatibility profiles
`SemanticProfile` values:
- ShardLoomNative
- SparkCompatible
- DataFusionCompatible
- PostgresLike
- AnsiStrict

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

## Capability discovery UX
Capability discovery must remain deterministic and machine-readable, exposing exact coverage tiers, semantic profiles, and unsupported reasons.

## Migration analyzers
`MigrationCompatibilityReport` compares declared workload constitution against supported SQL/operators/functions/adapters and reports explicit deltas.

## User API and BI/server access roadmap
Roadmap includes CLI/API/BI surfaces as explicit capability layers with no implicit execution delegation.

## UDF/plugin strategy
UDF/plugin extensibility must remain typed, explicit about effects/determinism/materialization requirements, and constrained by no-fallback policy.

## Workload constitution
Declared workload constitutions define the basis for best-default certification and prevent overbroad unsupported claims.

## Correctness and semantic conformance
All capability claims require correctness and semantic conformance evidence before benchmarked superiority claims.

## Feature footprint / best-choice scorecard
`BestChoiceScorecard` summarizes capability coverage, semantic fit, migration friction, and evidence-backed suitability by workload constitution.

## Dependency policy distinction
Spark/DataFusion and other engines remain external baselines for comparison, not runtime dependencies or fallback paths.

## Relationship to RFC 0025 and CG-20
RFC 0025 defines competitive gates; this RFC specifies CG-20 capability contracts and evidence expectations.

## Future implementation phases
Future phases may incrementally implement matrices, scorecards, analyzer reports, and certification workflows without adding execution fallback.
