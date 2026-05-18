# ETL Workflow Capability Matrix

Schema: `shardloom.etl_workflow_capability_matrix.v1`

GAR slice: `GAR-0033-A`

Source references:

- `docs/rfcs/0033-user-data-workflow-etl-surface.md`
- `docs/architecture/phased-execution-plan.md`
- `docs/architecture/global-architecture-review.md`
- `docs/use-cases/README.md`
- `python/README.md`
- `benchmarks/traditional_analytics/README.md`

## Purpose

This matrix gives users, Python wrappers, and agents one compact report for the current
ShardLoom ETL workflow posture. It separates scoped local technical-preview paths from
report-only API posture and blocked production/runtime claims.

It is a capability and diagnostic surface. It does not add production ETL runtime, SQL/DataFrame
execution, object-store/lakehouse runtime, Foundry production support, package publication,
performance claims, Spark-displacement claims, external engine execution, or fallback execution.

## Rows

| Row | Status | Execution mode | Engine mode | Boundary |
| --- | --- | --- | --- | --- |
| `first_10_minutes_local_smoke` | `ready_local` | `no_dataset_smoke` | `batch_status` | Smoke/capability/status only; no dataset read and no output data claim. |
| `local_csv_parquet_certified_workload` | `smoke_supported` | `compatibility_import_certified` | `batch` | Scoped local CSV/Parquet workload evidence only. |
| `prepared_native_vortex_batch_smoke` | `smoke_supported` | `prepared_vortex/native_vortex` | `batch` | Prepared/native local batch smoke and source-backed scan evidence only. |
| `source_free_user_rows_jsonl` | `smoke_supported` | `source_free_generated_output` | `batch` | Scoped local JSONL output from caller-provided rows only. |
| `source_free_range_jsonl` | `smoke_supported` | `source_free_generated_output` | `batch` | Scoped local JSONL output from the admitted range generator only. |
| `dirty_csv_fixture` | `smoke_supported` | `compatibility_import_certified` | `batch` | Benchmark fixture evidence only, not broad data-quality runtime. |
| `nested_json_fixture` | `smoke_supported` | `compatibility_import_certified` | `batch` | Benchmark fixture evidence only, not broad JSON runtime. |
| `cdc_overlay_fixture` | `smoke_supported` | `compatibility_import_certified` | `batch` | Local fixture evidence only, not production incremental ETL. |
| `sql_dataframe_capability_posture` | `report_only` | `report_only` | `none` | Deterministic SQL/DataFrame diagnostics only. |
| `data_quality_api` | `report_only` | `report_only` | `none` | Deterministic data-quality diagnostics only. |
| `object_store_runtime` | `blocked` | `report_only_blocked` | `none` | S3/GCS/ADLS runtime remains blocked. |
| `table_lakehouse_runtime` | `blocked` | `report_only_blocked` | `none` | Iceberg/Delta/Hudi runtime and commit remain blocked. |
| `production_etl_certification` | `blocked` | `report_only_blocked` | `none` | Production ETL certification remains blocked. |

## Evidence Fields

Every row preserves:

- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status=not_claim_grade`

Supported local or smoke rows require workload-scoped evidence before any scoped claim can be made:

- correctness digest or correctness reference
- execution certificate
- Native I/O certificate where a real source or sink exists
- materialization/decode boundary
- result-sink evidence where output is written
- source-state evidence where applicable
- generated-source certificate where no input dataset exists
- output Native I/O certificate where output is written
- no-fallback evidence

Report-only and blocked rows require deterministic unsupported or blocked diagnostics. They cannot
be upgraded by documentation, capability vocabulary, or benchmark-adjacent rows alone.

## Python Surface

The Python wrapper exposes the matrix through:

```python
import shardloom as sl

ctx = sl.context()
matrix = ctx.etl_workflow_matrix()

print(matrix.schema_version)
print(matrix.supported_local_rows)
print(matrix.report_only_rows)
print(matrix.blocked_rows)
print(matrix.row("object_store_runtime").blocker_id)
print(matrix.all_no_fallback_no_external_engine)
```

`ctx.capabilities().etl_workflow_matrix` exposes the same typed view after collecting the standard
capability scopes.

## Claim Boundary

The matrix allows only local technical-preview statements that are already backed by scoped smoke or
certification evidence. It does not allow claims for:

- production ETL
- broad SQL/DataFrame runtime
- object-store/lakehouse runtime
- Foundry production support
- package publication
- performance or superiority
- Spark replacement or Spark displacement

If evidence is missing, the row remains `claim_gate_status=not_claim_grade`.

## Fallback Boundary

External engines may appear only as baselines, fixtures, or comparison oracles where a separate
benchmark/correctness policy admits them. They are never fallback execution for these workflows.

The matrix itself is side-effect-free: it must not read datasets, write outputs, resolve
credentials, probe networks, invoke Foundry, import external dataframe engines, or execute SQL.
