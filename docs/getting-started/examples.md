<!-- SPDX-License-Identifier: Apache-2.0 -->

# Examples

ShardLoom release-readiness examples are local and no-fallback by default.

Public status is owned by `docs/release/public-status-matrix.md`. Examples here are scoped local
proofs and blockers; they do not broaden package, production, performance, SQL/DataFrame,
object-store/lakehouse, Foundry, or Spark-displacement claims.

## Stable V1 Local Examples

These examples show the copy-paste shape of the current v1 local surface. Start with
`sl.context()` and `ctx.read(path)`; use explicit schemas or format-specific helpers only when a
test, benchmark, or reproducibility workflow needs them. The examples are intentionally bounded and
must emit ShardLoom evidence instead of delegating unsupported work to another engine.

`ctx.read(path)` infers local adapters for `.csv`, `.json`, `.jsonl`, `.ndjson`, `.parquet`,
`.arrow`, `.ipc`, `.feather`, `.avro`, `.orc`, and `.vortex`. CSV, flat JSON/JSONL/NDJSON,
generated rows, and scoped local Vortex inputs are the default public examples. Parquet, Arrow
IPC/Feather, Avro, and ORC are scoped local-format surfaces when the matching feature-gated build is
present; otherwise ShardLoom returns deterministic adapter blockers without fallback execution.

<!-- stable_v1_example_local_csv -->

```python
import shardloom as sl

ctx = sl.context()
orders = ctx.read("target/orders.csv")

result = (
    orders.filter(sl.col("amount") >= 10)
    .select("id", "amount", "status")
    .limit(100)
    .collect()
)
print(result.output_row_count, result.claim_summary.claim_gate_status)
print(result.fallback_attempted, result.external_engine_invoked)
```

<!-- stable_v1_example_local_jsonl -->

```python
events = ctx.read("target/events.jsonl")

result = (
    events.filter(sl.col("nested_payload").contains("target"))
    .select("id", "nested_payload")
    .limit(100)
    .collect()
)
print(result.output_row_count)
```

<!-- stable_v1_example_local_parquet -->

```python
facts = ctx.read("target/fact.parquet")

result = facts.filter(sl.col("metric") >= 0).select("id", "metric").limit(100).collect()
print(result.claim_summary.claim_gate_status)
```

Parquet and other structured compatibility inputs are scoped local-format routes and may require
the matching feature-gated build. They are not external-engine execution.

<!-- stable_v1_example_local_vortex -->

```python
native = ctx.read("target/orders.vortex")
result = native.filter("gte:amount:10").select("id", "amount").limit(100).collect()
print(result.fallback_attempted, result.external_engine_invoked)
```

<!-- stable_v1_example_prepare_vortex -->

```python
prepared = ctx.prepare_vortex(
    "target/orders.csv",
    "target/orders.vortex",
    allow_overwrite=True,
)
print(prepared.vortex_ingest_status, prepared.prepared_state_created)
```

<!-- stable_v1_example_warm_prepared_query -->

```python
prepared = ctx.prepare_vortex(
    "target/orders.csv",
    "target/orders.vortex",
    allow_overwrite=True,
)
result = prepared.query("selective filter").collect(check=True)
print(result.claim_gate_status, result.fallback_attempted, result.external_engine_invoked)
```

<!-- stable_v1_example_bounded_collect -->

```python
preview = ctx.read_csv("target/orders.csv").select("id", "amount").collect(limit=20)
print(preview.output_row_count)
```

<!-- stable_v1_example_local_output_write -->

```python
blocked_write = (
    ctx.read_csv("target/orders.csv")
    .filter(sl.col("amount") >= 10)
    .select("id", "amount")
    .write_jsonl("target/orders-filtered.jsonl", allow_overwrite=True, check=False)
)
print(blocked_write.blocker_id)
print(blocked_write.fallback_attempted, blocked_write.external_engine_invoked)
```

Compatibility exports such as JSONL/CSV require a native Vortex-derived export contract before
they are product routes. Use source-free/generated local writes or native Vortex sinks where the
route report says the shape is admitted.

<!-- stable_v1_example_evidence_inspection -->

```python
print(result.claim_summary.claim_gate_status)
print(result.evidence_summary.output_path)
print(result.fallback_attempted, result.external_engine_invoked)
print(result.diagnostics)
```

<!-- stable_v1_example_blocker_inspection -->

```python
blocked = ctx.read_csv("target/orders.csv").select("id").to_pandas()
print(blocked.blocker_id)
print(blocked.required_evidence)
print(blocked.fallback_attempted, blocked.external_engine_invoked)
```

## Unsupported Examples

Unsupported examples are part of the public contract: they fail closed and expose deterministic
blockers.

<!-- unsupported_example_broad_sql -->

```python
blocked_sql = ctx.sql("SELECT * FROM remote_table JOIN other_table USING (id)").collect()
print(blocked_sql.blocker_id)
```

<!-- unsupported_example_unbounded_collect -->

```python
blocked_collect = ctx.read_csv("target/orders.csv").select("id").to_pandas()
print(blocked_collect.blocker_id)
```

<!-- unsupported_example_object_store -->

```python
blocked_object_store = ctx.read_csv("s3://bucket/orders.csv").limit(10).collect()
print(blocked_object_store.blocker_id)
```

<!-- unsupported_example_foundry -->

```python
blocked_foundry = ctx.read("foundry://dataset/orders").limit(10).collect()
print(blocked_foundry.blocker_id)
```

<!-- unsupported_example_udf_effect -->

```python
blocked_effect = ctx.sql("SELECT CALL_API('https://example.invalid/score') AS score").collect()
print(blocked_effect.blocker_id)
```

Each unsupported example must preserve:

```text
fallback_attempted=false
external_engine_invoked=false
```

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

Use this for the scoped GAR-GEN-1C path that writes caller-provided rows to a local JSONL/CSV output and
emits generated-source and output evidence. It is not SQL/VALUES execution, broad DataFrame
runtime, object-store output, Foundry output, production support, or a performance claim.

The scoped user-row source also supports a small source-free transform before writing: projection
plus deterministic literal `with_column` values. The output still goes through the same
generated-source local-output command and no-fallback evidence path:

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').from_rows([{'id': 1, 'label': 'alpha'}, {'id': 2, 'label': 'beta'}]).with_column('batch_id', 1).select('id', 'batch_id').write('target/generated-reference-transformed.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status, r.fallback_attempted, r.external_engine_invoked)"
```

This is not broad DataFrame expression execution. Unsupported generated expressions fail
deterministically rather than falling back to another engine.

## Source-Free Literal Table And Calendar Local Output Smokes

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').literal_table([{'code':'A','weight':1.5},{'code':'B','weight':2.0}]).write('target/generated-literal.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
python -c "from shardloom import context; r=context(repo_root='.').calendar('2026-05-18','2026-05-21', column='dt').write('target/generated-calendar.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
```

Use these for scoped source-free Python helpers that generate local JSONL/CSV output and emit the same
generated-source/output/no-fallback evidence family as `ctx.from_rows(...).write(...)`. They are not
SQL `VALUES` execution; use the dedicated source-free SQL smoke below for that. They are not broad
DataFrame runtime, object-store output, Foundry output, production support, or performance claims.

## Source-Free Range Local Output Smoke

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').range(0, 50, column='id').limit(5).write('target/generated-range.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
```

Use this for the scoped GAR-GEN-1D path that executes one ShardLoom-native range generator, writes
local JSONL/CSV output, and emits generated-source/output/no-fallback evidence. The sequence helper uses
the same scoped integer-generator contract while reporting `generated_source_kind=sequence`:

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').sequence(0, 50, column='id').take(5).write('target/generated-sequence.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
```

Equivalent CLI command:

```powershell
shardloom generated-source-sequence-smoke target\generated-sequence.jsonl 0 5 --column id --allow-overwrite --format json
```

`limit(...)`, `head(...)`, and `take(...)` adjust the engine-native range/sequence bounds before
the same ShardLoom generator smoke runs; they do not materialize rows in Python. Range and sequence
smokes are not SQL `VALUES`/literal execution, SQL `generate_series`/`range`, broad DataFrame
runtime, other generator-node support, object-store output, Foundry output, production support, or
a performance claim.

## Source-Free SQL Literal/VALUES Local Output Smoke

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').sql_values(\"VALUES (1, 'alpha'), (2, 'beta')\").write('target/generated-sql-values.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
python -c "from shardloom import context; r=context(repo_root='.').sql_literal_select(\"SELECT 1 AS id, 'alpha' AS label, true AS active\").write('target/generated-sql-select.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
python -c "from shardloom import context; r=context(repo_root='.').sql(\"SELECT 2 AS id, 'beta' AS label\").write('target/generated-sql-from-context.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
```

Use this for the scoped GAR-RUNTIME-IMPL-1A path that parses ShardLoom's tiny source-free SQL smoke
subset, writes local JSONL/CSV output through either the explicit source-free helpers or scoped
`ctx.sql(...).write(...)`, and emits generated-source/output/no-fallback evidence. It is not broad
SQL runtime, SQL over input datasets, functions, joins, SQL/DataFrame production support,
object-store output, Foundry output, or a performance claim. Source-free `ctx.sql(...).collect()`
remains a deterministic unsupported diagnostic because this evidence contract requires an explicit
output sink.

## SQL Local CSV Projection/Filter/Limit Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,label,amount
1,alpha,8
2,beta,15
3,gamma,
"@ | Set-Content -Encoding utf8 target\local-source-runtime.csv
cargo run -q -p shardloom-cli -- local-source-runtime "SELECT id,label FROM 'target/local-source-runtime.csv' WHERE amount >= 10 LIMIT 1" --format json
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').sql(\"SELECT id,label FROM 'target/local-source-runtime.csv' WHERE amount >= 10 LIMIT 1\").collect(); print(r.result_rows, r.fallback_attempted, r.external_engine_invoked)"
```

Use this for the scoped GAR-RUNTIME-IMPL-1B path that parses, binds, plans, and executes one local
CSV SQL shape through ShardLoom-owned projection/filter/limit semantics. It prints bounded inline
JSONL and emits source-read, execution-certificate, materialization/decode, no-fallback, and
claim-gate evidence. It is not broad SQL runtime, a production SQL/DataFrame claim, Parquet/Vortex
SQL source support, joins, grouped aggregates, functions, subqueries, object-store/table support, or a
performance claim.

## Prepare Vortex Once With `vortex_ingest`

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,label,amount
1,alpha,8
2,beta,15
"@ | Set-Content -Encoding utf8 target\vortex-ingest-source.csv
cargo run -q -p shardloom-cli --features vortex-write -- vortex-prepare target\vortex-ingest-source.csv target\vortex-ingest-source.vortex --allow-overwrite --format json
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; ctx=context(repo_root='.', profile_order=('debug','release')); r=ctx.prepare_vortex('target/vortex-ingest-source.csv','target/vortex-ingest-source.vortex', allow_overwrite=True); print(r.vortex_ingest_status, r.prepared_state_created, r.input_row_count, r.fallback_attempted, r.external_engine_invoked)"
```

Use this for the scoped GAR-RUNTIME-IMPL-4H route that admits a local flat non-null
int/uint/float/UTF-8/date32/timestamp source, writes a local Vortex artifact, reopens/scans it for
row-count proof, and emits `VortexPreparedState` evidence. Default builds return a deterministic
feature-gate blocker unless `--features vortex-write` is enabled. It is not broad Vortex writer
support, object-store/table output, production SQL/DataFrame support, or a performance claim.

## SQL Local JSONL Cast Predicate Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
{"id":1,"amount":"8","label":"low"}
{"id":2,"amount":"15","label":"mid"}
{"id":3,"amount":"21","label":"high"}
"@ | Set-Content -Encoding utf8 target\sql-local-source-cast.jsonl
cargo run -q -p shardloom-cli -- local-source-runtime "SELECT id,amount,label FROM 'target/sql-local-source-cast.jsonl' WHERE CAST(amount AS int64) >= 10 LIMIT 10" --format json
```

Use this for the scoped GAR-RUNTIME-IMPL-4D cast-family path that parses, lowers, and executes a
local SQL `CAST(column AS dtype)` predicate for `int64`, `float64`, `utf8`, `boolean`, or `date32`
through ShardLoom-owned expression semantics. It emits `predicate_operator_family=cast`,
`cast_runtime_execution=true`, `cast_source_column`, `cast_target_dtype`, materialization/decode,
no-fallback, and claim-gate evidence. It is not broad SQL/DataFrame runtime, function support,
object-store/lakehouse support, or a performance claim.

## SQL Local CSV Date Arithmetic Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,event_date
1,2026-05-18
2,2026-05-19
3,2026-05-20
"@ | Set-Content -Encoding utf8 target\sql-local-source-date.csv
cargo run -q -p shardloom-cli -- local-source-runtime "SELECT id,event_date FROM 'target/sql-local-source-date.csv' WHERE DATE_ADD_DAYS(CAST(event_date AS date32), 1) >= DATE '2026-05-20' LIMIT 10" --format json
```

Use this for the scoped GAR-RUNTIME-IMPL-4D Date32 day-arithmetic slice. It parses, lowers, and
executes `DATE_ADD_DAYS(column, days)` / `DATE_SUB_DAYS(column, days)` comparisons through
ShardLoom-owned expression semantics, emits `predicate_operator_family=date_arithmetic`,
`date_arithmetic_runtime_execution=true`, `date_arithmetic_operator`, `date_arithmetic_days`, and
`date_arithmetic_source_column`, and blocks unsupported day counts or non-Date32 shapes before
fallback. Scoped `INTERVAL '<n>' DAY` literals are also admitted inside these helper functions. It
is not timestamp/timezone completeness, arbitrary interval arithmetic, broad SQL function support,
object-store/lakehouse support, or a performance claim.

## SQL Local CSV Date Extract Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,event_date
1,2026-04-18
2,2026-05-19
3,2026-05-20
"@ | Set-Content -Encoding utf8 target\sql-local-source-date.csv
cargo run -q -p shardloom-cli -- local-source-runtime "SELECT id,event_date FROM 'target/sql-local-source-date.csv' WHERE DATE_YEAR(CAST(event_date AS date32)) = 2026 AND DATE_MONTH(event_date) = 5 AND DATE_DAY(event_date) >= 19 LIMIT 10" --format json
```

Use this for the scoped GAR-RUNTIME-IMPL-4D Date32 extract slice. It parses, lowers, and executes
`DATE_YEAR(column)`, `DATE_MONTH(column)`, and `DATE_DAY(column)` comparisons through
ShardLoom-owned expression semantics, emits `predicate_operator_family=logical_predicate` when
combined with logical predicates plus `date_extract_runtime_execution=true`,
`date_extract_operator`, and `date_extract_source_column`, and blocks unsupported non-Date32 or
non-integer comparison shapes before fallback. It is not timestamp/timezone completeness,
generalized date function support, object-store/lakehouse support, or a performance claim.

## SQL Local CSV Scalar Aggregate Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,label,amount
1,alpha,8
2,beta,15
3,gamma,
4,delta,21
"@ | Set-Content -Encoding utf8 target\local-source-runtime.csv
cargo run -q -p shardloom-cli -- local-source-runtime "SELECT count(*),sum(amount),avg(amount),min(amount),max(amount) FROM 'target/local-source-runtime.csv' WHERE amount >= 10 LIMIT 1" --format json
```

Use this for the first GAR-RUNTIME-IMPL-1E operator-family promotion. It keeps the same local CSV
internal-source-smoke boundary and emits `aggregate_runtime_execution=true`,
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
cargo run -q -p shardloom-cli -- local-source-runtime "SELECT region,count(*),sum(amount) FROM 'target/sql-local-source-group-by.csv' WHERE amount >= 0 GROUP BY region LIMIT 10" --format json
```

Use this for the next GAR-RUNTIME-IMPL-1E operator-family promotion. It emits
`sql_statement_kind=local_source_group_by_aggregate_filter_limit`,
`aggregate_operator_family=grouped_aggregate`, `group_by_runtime_execution=true`,
`group_by_columns`, group count, the grouped aggregate execution certificate ref, and no-fallback
evidence. It is not multi-key group-by generality, Python `group_by().agg(...)`, broad SQL,
prepared/native aggregate promotion, performance evidence, or production SQL/DataFrame support.

## SQL Local CSV Order-By Top-N Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,label,amount
1,alpha,8
2,beta,15
3,gamma,21
4,delta,13
"@ | Set-Content -Encoding utf8 target\sql-local-source-topn.csv
cargo run -q -p shardloom-cli -- local-source-runtime "SELECT id,label FROM 'target/sql-local-source-topn.csv' WHERE amount >= 10 ORDER BY amount DESC LIMIT 2" --format json
```

Use this for the scoped GAR-RUNTIME-IMPL-4B top-N promotion. It emits
`sql_statement_kind=local_source_order_by_topn_filter_limit`,
`order_by_runtime_execution=true`, `top_n_runtime_execution=true`,
`sort_operator_family=single_key_numeric_topn`, sort key/direction fields, the top-N execution
certificate ref, and no-fallback evidence. It admits one numeric non-null sort key only. Multi-key
sorts, expression ordering, null ordering, collation parity, window ranking, broad SQL/DataFrame
runtime, object-store/table sources, performance evidence, and production claims remain blocked.

## SQL Local CSV Inner Equi-Join Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,customer_id,region,amount
1,10,east,8
2,20,west,15
3,20,east,21
4,30,east,22
5,30,west,23
"@ | Set-Content -Encoding utf8 target\sql-local-source-join-fact.csv
@"
customer_id,region,segment
20,west,enterprise
20,east,consumer
30,west,startup
99,east,orphan
"@ | Set-Content -Encoding utf8 target\sql-local-source-join-dim.csv
cargo run -q -p shardloom-cli -- local-source-runtime "SELECT f.id,d.segment FROM 'target/sql-local-source-join-fact.csv' AS f INNER JOIN 'target/sql-local-source-join-dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 LIMIT 10" --format json
```

Use this for the scoped GAR-RUNTIME-IMPL-4C join promotion. It emits
`sql_statement_kind=local_source_inner_equi_join_filter_limit`,
`join_runtime_execution=true`, `join_type=inner_equi`, left/right source refs, join keys,
`join_key_arity`, `join_multi_key_runtime_execution`, matched/candidate/unmatched/scanned/output
row counts, a scoped memory estimate, the join execution certificate ref, and no-fallback evidence.
It admits scoped single- or multi-key local-source inner equi-joins plus left/right/full outer,
left semi/anti, and cross joins with explicit aliases only. The same scoped shapes can run over other
admitted local sources such as
flat JSONL/NDJSON. Feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC joins use the same
deterministic adapter gates as the rest of `local-source-runtime`.
Expression, distributed, broadcast, shuffle, object-store/table,
performance, and production join claims remain blocked.

## Python Local CSV Query-Builder Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,label,amount
1,alpha,8
2,beta,15
3,gamma,
"@ | Set-Content -Encoding utf8 target\local-source-runtime.csv
$env:PYTHONPATH = "python\src"
@'
import shardloom as sl

ctx = sl.context(repo_root=".", profile_order=("debug", "release"))
workflow = (
    ctx.read_csv("target/local-source-runtime.csv")
    .select("id", "label")
    .filter(sl.col("amount") >= 10)
    .limit(1)
)
predicate_builder = (
    ctx.read_csv("target/local-source-runtime.csv")
    .select("id", "label")
    .where(sl.col("amount").between(10, 25) & sl.col("label").contains("ta"))
    .limit(10)
    .collect()
)
literal_column = (
    ctx.read_csv("target/local-source-runtime.csv")
    .select("id", "label")
    .with_column("segment", "lit('north')")
    .filter(sl.col("amount") >= 10)
    .limit(10)
    .collect()
)

head = ctx.read_csv("target/local-source-runtime.csv").head(limit=2)
take = ctx.read_csv("target/local-source-runtime.csv").take(2)
collected = workflow.collect()
written = workflow.write("target/sql-local-source-result.jsonl", allow_overwrite=True)
aggregate = (
    ctx.read_csv("target/local-source-runtime.csv")
    .filter("amount >= 10")
    .aggregate("count(*)", "sum(amount)", "avg(amount)", "min(amount)", "max(amount)")
    .limit(1)
    .collect()
)
row_count = (
    ctx.read_csv("target/local-source-runtime.csv")
    .filter(sl.col("amount") >= 10)
    .count()
)
grouped = (
    ctx.read_csv("target/local-source-runtime.csv")
    .filter("amount >= 10")
    .group_by("label")
    .agg("count(*)", "sum(amount)")
    .limit(10)
    .collect()
)
topn = (
    ctx.read_csv("target/local-source-runtime.csv")
    .select("id", "label")
    .filter("amount >= 0")
    .sort("amount", descending=True)
    .limit(2)
    .collect()
)
joined = (
    ctx.read_csv("target/sql-local-source-join-fact.csv")
    .join(ctx.read_csv("target/sql-local-source-join-dim.csv"), on=("customer_id", "region"))
    .select("f.id", "d.segment")
    .filter("f.amount >= 10")
    .limit(10)
    .collect()
)
joined_grouped = (
    ctx.read_csv("target/sql-local-source-join-fact.csv")
    .join(ctx.read_csv("target/sql-local-source-join-dim.csv"), on=("customer_id", "region"))
    .filter("f.amount >= 10")
    .group_by("d.segment")
    .agg(rows="count(*)", total_amount="sum(f.amount)")
    .limit(10)
    .collect()
)

print(collected.result_rows)
print(predicate_builder.result_rows)
print(literal_column.result_rows)
print(head.result_rows)
print(take.result_rows)
print(written.output_path)
print(written.output_native_io_certificate_status)
print(written.fallback_attempted, written.external_engine_invoked)
print(written.evidence_summary.output_native_io_certificate_status)
print(written.claim_summary.claim_gate_status)
print(aggregate.first_result_row)
print(aggregate.aggregate_operator_family)
print(aggregate.aggregate_functions)
print(row_count.first_result_row)
print(row_count.aggregate_functions)
print(grouped.result_rows)
print(grouped.aggregate_operator_family)
print(grouped.group_by_columns)
print(topn.result_rows)
print(topn.order_by_runtime_execution, topn.sort_keys, topn.sort_direction)
print(joined.result_rows)
print(joined.join_runtime_execution, joined.join_type)
print(joined_grouped.join_aggregate_runtime_execution, joined_grouped.join_aggregate_operator_family)
print(joined.evidence_summary.command)
print(joined.claim_summary.public_performance_claim_allowed)
'@ | python -
```

Use this for the scoped GAR-RUNTIME-IMPL-1C path that exposes the same local CSV SQL smoke through a
Python DataFrame-like query builder. `collect()` returns bounded inline JSONL; `write()` writes a
local JSONL result and emits output Native I/O certificate fields. `head(...)` and `take(...)`
are familiar aliases over the same bounded `preview(...)` select-star path. Scalar `aggregate(...)` lowers to
the same scoped SQL local-source smoke for `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`; `count()` is a
convenience wrapper over the same `COUNT(*)` smoke; one-column
`group_by(...).agg(...)` lowers to the scoped grouped aggregate smoke; single-key numeric
`sort(...).limit(...)` lowers to the scoped top-N smoke; local-source
`join(..., on="key")` or `join(..., on=("customer_id", "region"))` with qualified
projection/filter columns lowers to the scoped join smoke; scalar/grouped aggregates
over those scoped joined rows lower to the scoped join-aggregate smoke; and explicit-projection
literal `with_column(...)` lowers to scoped literal projection.
`where(...)` is a familiar alias for `filter(...)`. `sl.col(...)` is a Python predicate helper for
admitted comparison, inclusive `between(...)`, null, string `LIKE`, bounded `IN`, cast/date,
Date32 extract/day arithmetic, and logical predicates; it lowers into ShardLoom's existing local SQL
runtime rather than a Python engine. It is not a pandas/Polars backend, broad DataFrame runtime,
non-literal `with_column`, generalized grouped aggregate,
ordering, expression/non-equi join runtime, object-store/table path, production SQL support, or
performance claim.
Runtime reports also expose `result_rows` / `first_result_row` plus `evidence_summary` and
`claim_summary` helpers so users can inspect bounded rows, the output sink, no-fallback fields,
external-engine boundary, and claim gate without parsing raw JSONL or scraping raw JSON.

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
