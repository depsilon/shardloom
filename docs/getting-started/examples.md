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
