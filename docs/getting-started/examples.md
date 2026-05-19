<!-- SPDX-License-Identifier: Apache-2.0 -->

# Examples

ShardLoom release-readiness examples are local and no-fallback by default.

## Local Python Smoke

```powershell
python examples\local-python-smoke\run.py --repo-root .
```

Use this for import, CLI resolution, status, smoke, capabilities, and
`fallback_attempted=false` proof without reading or writing datasets.

## Local Vortex Benchmark Smoke

```powershell
python examples\local-vortex-benchmark\run.py --repo-root . --rows 64 --iterations 1
```

Use this for a small ShardLoom-only local Vortex benchmark smoke with result
sink evidence. The default example uses CSV only so optional Parquet and
external baseline dependencies are not required. It runs both the
`shardloom` compatibility-import lane and the `shardloom-prepared-vortex` lane
so users can inspect certification evidence and the current prepared/native
runtime-development path separately.

## Source-Free User Rows Local Output Smoke

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').from_rows([{'id': 1, 'label': 'alpha'}]).write('target/generated-reference.jsonl', allow_overwrite=True); print(r.claim_gate_status)"
```

Use this for the scoped GAR-GEN-1C path that writes caller-provided rows to a local JSONL output and
emits generated-source and output evidence. It is not SQL/VALUES execution, broad DataFrame
runtime, object-store output, Foundry output, production support, or a performance claim.

## Source-Free Literal Table And Calendar Local Output Smokes

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').literal_table([{'code':'A','weight':1.5},{'code':'B','weight':2.0}]).write('target/generated-literal.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
python -c "from shardloom import context; r=context(repo_root='.').calendar('2026-05-18','2026-05-21', column='dt').write('target/generated-calendar.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
```

Use these for scoped source-free Python helpers that generate local JSONL output and emit the same
generated-source/output/no-fallback evidence family as `ctx.from_rows(...).write(...)`. They are not
SQL `VALUES` execution; use the dedicated source-free SQL smoke below for that. They are not broad
DataFrame runtime, object-store output, Foundry output, production support, or performance claims.

## Source-Free Range Local Output Smoke

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').range(0, 5, column='id').write('target/generated-range.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
```

Use this for the scoped GAR-GEN-1D path that executes one ShardLoom-native range generator, writes
local JSONL output, and emits generated-source/output/no-fallback evidence. It is not SQL
`VALUES`/literal execution, broad DataFrame runtime, other generator-node support, object-store
output, Foundry output, production support, or a performance claim.

## Source-Free SQL Literal/VALUES Local Output Smoke

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').sql_values(\"VALUES (1, 'alpha'), (2, 'beta')\").write('target/generated-sql-values.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
python -c "from shardloom import context; r=context(repo_root='.').sql_literal_select(\"SELECT 1 AS id, 'alpha' AS label, true AS active\").write('target/generated-sql-select.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
```

Use this for the scoped GAR-RUNTIME-IMPL-1A path that parses ShardLoom's tiny source-free SQL smoke
subset, writes local JSONL output, and emits generated-source/output/no-fallback evidence. It is not
broad SQL runtime, SQL over input datasets, functions, joins, SQL/DataFrame production support,
object-store output, Foundry output, or a performance claim.

## SQL Local CSV Projection/Filter/Limit Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,label,amount
1,alpha,8
2,beta,15
3,gamma,
"@ | Set-Content -Encoding utf8 target\sql-local-source-smoke.csv
cargo run -q -p shardloom-cli -- sql-local-source-smoke "SELECT id,label FROM 'target/sql-local-source-smoke.csv' WHERE amount >= 10 LIMIT 1" --format json
```

Use this for the scoped GAR-RUNTIME-IMPL-1B path that parses, binds, plans, and executes one local
CSV SQL shape through ShardLoom-owned projection/filter/limit semantics. It prints bounded inline
JSONL and emits source-read, execution-certificate, materialization/decode, no-fallback, and
claim-gate evidence. It is not broad SQL runtime, a production SQL/DataFrame claim, Parquet/Vortex
SQL source support, joins, grouped aggregates, functions, subqueries, object-store/table support, or a
performance claim.

## SQL Local CSV Scalar Aggregate Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,label,amount
1,alpha,8
2,beta,15
3,gamma,
4,delta,21
"@ | Set-Content -Encoding utf8 target\sql-local-source-smoke.csv
cargo run -q -p shardloom-cli -- sql-local-source-smoke "SELECT count(*),sum(amount),avg(amount),min(amount),max(amount) FROM 'target/sql-local-source-smoke.csv' WHERE amount >= 10 LIMIT 1" --format json
```

Use this for the first GAR-RUNTIME-IMPL-1E operator-family promotion. It keeps the same local CSV
direct-transient boundary and emits `aggregate_runtime_execution=true`,
`aggregate_operator_family=scalar_aggregate`, scalar aggregate function labels, the aggregate
execution certificate ref, and no-fallback evidence. It is not grouped aggregation, joins, broad SQL
runtime, performance evidence, or production SQL/DataFrame support.

## SQL Local CSV Group-By Aggregate Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,region,amount
1,east,10
2,west,5
3,east,12
4,west,
5,north,3
"@ | Set-Content -Encoding utf8 target\sql-local-source-group-by.csv
cargo run -q -p shardloom-cli -- sql-local-source-smoke "SELECT region,count(*),sum(amount) FROM 'target/sql-local-source-group-by.csv' WHERE amount >= 0 GROUP BY region LIMIT 10" --format json
```

Use this for the next GAR-RUNTIME-IMPL-1E operator-family promotion. It emits
`sql_statement_kind=local_source_group_by_aggregate_filter_limit`,
`aggregate_operator_family=grouped_aggregate`, `group_by_runtime_execution=true`,
`group_by_columns`, group count, the grouped aggregate execution certificate ref, and no-fallback
evidence. It is not multi-key group-by generality, Python `group_by().agg(...)`, broad SQL,
prepared/native aggregate promotion, performance evidence, or production SQL/DataFrame support.

## Python Local CSV Query-Builder Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,label,amount
1,alpha,8
2,beta,15
3,gamma,
"@ | Set-Content -Encoding utf8 target\sql-local-source-smoke.csv
$env:PYTHONPATH = "python\src"
@'
import shardloom as sl

ctx = sl.context(repo_root=".", profile_order=("debug", "release"))
workflow = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .filter("amount >= 10")
    .limit(1)
)

collected = workflow.collect()
written = workflow.write("target/sql-local-source-result.jsonl", allow_overwrite=True)
aggregate = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .filter("amount >= 10")
    .aggregate("count(*)", "sum(amount)", "avg(amount)", "min(amount)", "max(amount)")
    .limit(1)
    .collect()
)

print(collected.result_jsonl)
print(written.output_path)
print(written.output_native_io_certificate_status)
print(written.fallback_attempted, written.external_engine_invoked)
print(aggregate.result_jsonl)
print(aggregate.aggregate_operator_family)
print(aggregate.aggregate_functions)
'@ | python -
```

Use this for the scoped GAR-RUNTIME-IMPL-1C path that exposes the same local CSV SQL smoke through a
Python DataFrame-like query builder. `collect()` returns bounded inline JSONL; `write()` writes a
local JSONL result and emits output Native I/O certificate fields. Scalar `aggregate(...)` lowers to
the same scoped SQL local-source smoke for `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`. It is not a
pandas/Polars backend, broad DataFrame runtime, grouped aggregate runtime, object-store/table path,
production SQL support, or performance claim.

## Foundry Lightweight Transform

```powershell
python examples\foundry-lightweight-transform\run.py --repo-root .
```

Use this to inspect the future Foundry transform shape without invoking Foundry,
Foundry Spark, virtual tables, Snowflake, Databricks, BigQuery, or external
compute. The example writes a local certificate-style JSON output and keeps
staged dataset execution deferred to P9.6.

Each example includes an environment file, fixture, expected output snapshot,
expected certificate field snapshot, and known limitations.
