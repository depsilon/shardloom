# Engine Replacement Claim Inventory

Status: `report_only`

GAR slice: `GAR-0001B-A`

Schema marker: `engine_replacement_claim_inventory_schema_version=shardloom.engine_replacement_claim_inventory.v1`

## Purpose

This inventory maps engine-replacement, Spark-displacement, best-default, production
SQL/DataFrame, object-store/lakehouse, and managed-platform claim language to the evidence that
would be required before ShardLoom could make any public claim in that family.

The inventory is not runtime evidence. It does not rerun benchmarks, execute workloads, publish
packages, or authorize public displacement language.

## Release Gate Fields

The release-plan command emits these report-only fields:

- `engine_replacement_claim_inventory_claim_gate_status=not_claim_grade`
- `engine_replacement_claim_inventory_all_claims_blocked=true`
- `engine_replacement_claim_inventory_spark_displacement_claim_allowed=false`
- `engine_replacement_claim_inventory_public_engine_replacement_claim_allowed=false`
- `engine_replacement_claim_inventory_best_default_claim_allowed=false`
- `engine_replacement_claim_inventory_performance_superiority_claim_allowed=false`
- `engine_replacement_claim_inventory_production_platform_claim_allowed=false`
- `engine_replacement_claim_inventory_runtime_execution_performed=false`
- `engine_replacement_claim_inventory_benchmark_rerun_performed=false`
- `engine_replacement_claim_inventory_fallback_attempted=false`
- `engine_replacement_claim_inventory_external_engine_invoked=false`

These fields keep missing evidence visible as `claim_gate_status=not_claim_grade`.

## Claim Families

| Claim family | Required evidence before any public claim | Current status |
| --- | --- | --- |
| Spark displacement | Runtime, output, correctness, benchmark, execution-certificate, Native I/O, scale, and no-fallback evidence for Spark-comparable workloads. | `not_claim_grade` |
| General engine replacement | Broad local runtime, input/output adapter support, session/runtime evidence, reproducible benchmark artifacts, and per-claim evidence attachment. | `not_claim_grade` |
| Best default engine | Best-default dossier, workflow evidence, installation proof, benchmark manifest, correctness coverage, and release/security gates. | `not_claim_grade` |
| Production SQL/DataFrame replacement | SQL parser/binder/planner/runtime, DataFrame runtime, optimizer/operator runtime, semantic conformance, and benchmark evidence. | `not_claim_grade` |
| Object-store/lakehouse replacement | Object-store read/write, table metadata/runtime, commit protocol, rollback/recovery, table semantics, and scale evidence. | `not_claim_grade` |
| Managed-platform replacement | Real managed-platform invocation proof, output/evidence datasets, governance evidence, scale proof, and package/channel proof. | `not_claim_grade` |

## Boundaries

- No replacement claim.
- No public displacement language.
- Boundary marker: `no public displacement language`.
- No benchmark rerun.
- No runtime execution.
- No external engine fallback.
- `fallback_attempted=false`.
- `external_engine_invoked=false`.

External engines may appear only as baselines, oracles, or comparison rows. They never satisfy
ShardLoom execution evidence and never act as fallback execution.

## Follow-Up Gates

This inventory depends on later claim and runtime evidence, including:

- `GAR-0009-A` Spark-displacement benchmark evidence matrix.
- `GAR-0041-A` per-claim evidence attachment matrix.
- `GAR-SCALE-1` scale contract and claim gate.
- Runtime implementation slices for local runtime, SQL/DataFrame, output writers, object-store,
  lakehouse/table, and managed-platform proof.
