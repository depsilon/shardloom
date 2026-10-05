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
result = (
    ctx.read_vortex("target/orders.vortex")
    .filter(sl.col("amount") >= 10)
    .select("id", "amount")
    .collect()
)
print(result.status, result.result_rows)
print(result.fallback_attempted, result.external_engine_invoked)
```

<!-- stable_v1_example_bounded_collect -->

```python
preview = ctx.read_csv("target/orders.csv").select("id", "amount").collect(limit=20)
print(preview.output_row_count)
```

<!-- stable_v1_example_local_output_write -->

```python
written = (
    ctx.read_csv("target/orders.csv")
    .filter(sl.col("amount") >= 10)
    .select("id", "amount")
    .write_jsonl("target/orders-filtered.jsonl", check=False)
)
print(written.status, written.output_path)
print(written.output_commit_status, written.native_io_certificate_status)
print(written.fallback_attempted, written.external_engine_invoked)
```

Memory-backed inputs and source-free SQL use the shared native workflow for both collection and
declared output sinks. Output formats and dtype combinations remain subject to enabled sink
adapters and admission rules; compatibility output does not change the execution engine.

<!-- stable_v1_example_evidence_inspection -->

```python
print(result.claim_summary.claim_gate_status)
print(result.evidence_summary.output_path)
print(result.fallback_attempted, result.external_engine_invoked)
print(result.diagnostics)
```

<!-- stable_v1_example_blocker_inspection -->

```python
blocked = ctx.read_csv("target/orders.csv").select("id").apply("row_udf", check=False)
print(blocked.blocker_id)
print(blocked.required_evidence)
print(blocked.fallback_attempted, blocked.external_engine_invoked)
```

## Unsupported Examples

Unsupported examples are part of the public contract: they fail closed and expose deterministic
blockers.

<!-- unsupported_example_broad_sql -->

```python
blocked_sql = ctx.sql("SELECT * FROM remote_table JOIN other_table USING (id)").collect(check=False)
print(blocked_sql.status, blocked_sql.diagnostics)
```

<!-- unsupported_example_unbounded_collect -->

```python
blocked_collect = ctx.range(0, 65_537).collect(check=False)
print(blocked_collect.status, blocked_collect.diagnostics)
```

<!-- unsupported_example_object_store -->

```python
blocked_object_store = ctx.read_csv("s3://bucket/orders.csv").limit(10).collect(check=False)
print(blocked_object_store.status, blocked_object_store.diagnostics)
```

<!-- unsupported_example_foundry -->

```python
blocked_foundry = ctx.read("foundry://dataset/orders").limit(10).collect(check=False)
print(blocked_foundry.status, blocked_foundry.diagnostics)
```

<!-- unsupported_example_udf_effect -->

```python
blocked_effect = ctx.sql("SELECT CALL_API('https://example.invalid/score') AS score").collect(check=False)
print(blocked_effect.status, blocked_effect.diagnostics)
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

This checks import, CLI resolution, status and capabilities, then creates a small CSV fixture,
collects its filtered rows and writes a generated JSONL result. It validates complete values,
output commit evidence and `fallback_attempted=false`.

## Guarded Native Benchmark Comparison

```powershell
python examples\local-vortex-benchmark\run.py `
  --shardloom-binary target\debug\shardloom `
  --workspace "$HOME\LocalData\shardloom\traditional-benchmarks" `
  --repo-root . --rows 64 --dim-rows 8 --repeats 1 --formats csv `
  --input-state raw --output-format collect --reference-engine pandas
```

Supply an already-built ShardLoom executable and a local-only workspace outside synced folders.
This thin example delegates fixture generation, resource checks, run isolation, and result handling
to `benchmarks/traditional_analytics/run.py`. It invokes one `shardloom` candidate and compares the
complete `selective filter` result with pandas as an independent correctness reference. The default
CSV format and `collect` output are small; `--formats` and `--output-format` accept the harness's
current supported values. `--input-state raw` may be changed to `prepared`. Unsupported ShardLoom
work is reported and is never executed by a comparison engine. The run is not a performance claim
or a complete benchmark acceptance review, and the wrapper does not build ShardLoom or install
dependencies.

## Source-Free User Rows Local Output Smoke

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').from_rows([{'id': 1, 'label': 'alpha'}]).write('target/generated-reference.jsonl', allow_overwrite=True); print(r.envelope.status, r.fallback_attempted, r.external_engine_invoked, r.claim_gate_status)"
```

`from_rows(...)` declares typed memory input. `collect()` returns complete native result rows, and
`write(...)` sends the result through the shared native workflow to the requested sink. The available
sink and dtype combinations depend on the release-user-surfaces build and the relevant adapter
admission rules. Unsupported combinations fail with a diagnostic instead of changing execution
engines.

The example also applies projection and a literal `with_column` before writing. The declaration,
transform, collection, and write request all use the shared native workflow:

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').from_rows([{'id': 1, 'label': 'alpha'}, {'id': 2, 'label': 'beta'}]).with_column('batch_id', 1).select('id', 'batch_id').write('target/generated-reference-transformed.jsonl', allow_overwrite=True); print(r.envelope.status, r.fallback_attempted, r.external_engine_invoked, r.claim_gate_status)"
```

The example demonstrates this transform shape only. Other expressions are admitted or rejected by
the native planner, with unsupported work reported deterministically.

## Source-Free Literal Table And Calendar Local Output Smokes

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').literal_table([{'code':'A','weight':1.5},{'code':'B','weight':2.0}]).write('target/generated-literal.jsonl', allow_overwrite=True); print(r.envelope.status, r.fallback_attempted, r.external_engine_invoked, r.claim_gate_status)"
python -c "from shardloom import context; r=context(repo_root='.').calendar('2026-05-18','2026-05-21', column='dt').write('target/generated-calendar.jsonl', allow_overwrite=True); print(r.envelope.status, r.fallback_attempted, r.external_engine_invoked, r.claim_gate_status)"
```

These caller-provided literals use the same native collection and declared-sink path as
`ctx.from_rows(...)`. The examples show literal table and calendar declarations; output formats and
dtype combinations follow the same enabled adapter and admission rules.

## Source-Free Range Local Output Smoke

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').range(0, 50, column='id').limit(5).write('target/generated-range.jsonl', allow_overwrite=True); print(r.envelope.status, r.fallback_attempted, r.external_engine_invoked, r.claim_gate_status)"
```

These Python helpers declare integer range/sequence input for the shared native workflow. They can
be collected as complete typed results or sent to a declared sink, subject to the same feature,
dtype, and sink-adapter rules as other memory-backed inputs. The examples use the local JSONL sink:

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').sequence(0, 50, column='id').take(5).write('target/generated-sequence.jsonl', allow_overwrite=True); print(r.envelope.status, r.fallback_attempted, r.external_engine_invoked, r.claim_gate_status)"
```

Equivalent SQL request through the public CLI facade:

```powershell
cargo run -q -p shardloom-cli --features release-user-surfaces -- run sql --sql "SELECT value AS id FROM range(1, 4)" --request write_jsonl --output target\generated-sequence.jsonl --bounded true --format json
```

The range form is also covered by public native-workflow tests. This example shows one bounded
range request; it does not claim support for every generator or sink/type combination.

## Source-Free SQL Literal/VALUES Local Output Smoke

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').sql_values(\"VALUES (1, 'alpha'), (2, 'beta')\").write('target/generated-sql-values.jsonl', allow_overwrite=True); print(r.envelope.status, r.fallback_attempted, r.external_engine_invoked, r.claim_gate_status)"
python -c "from shardloom import context; r=context(repo_root='.').sql_literal_select(\"SELECT 1 AS id, 'alpha' AS label, true AS active\").write('target/generated-sql-select.jsonl', allow_overwrite=True); print(r.envelope.status, r.fallback_attempted, r.external_engine_invoked, r.claim_gate_status)"
python -c "from shardloom import context; r=context(repo_root='.').sql(\"SELECT 2 AS id, 'beta' AS label\").write('target/generated-sql-from-context.jsonl', allow_overwrite=True); print(r.envelope.status, r.fallback_attempted, r.external_engine_invoked, r.claim_gate_status)"
```

Source-free `VALUES` and literal `SELECT` statements use the shared native workflow. They can be
collected as complete typed results or sent to a declared sink. This section demonstrates the shown
SQL forms; input bindings, expression support, and sink/dtype combinations remain subject to native
admission and feature gates.

## SQL Local CSV Projection/Filter/Limit Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,label,amount
1,alpha,8
2,beta,15
3,gamma,
"@ | Set-Content -Encoding utf8 target\local-source-runtime.csv
cargo run -q -p shardloom-cli --features release-user-surfaces -- run sql --input target/local-source-runtime.csv --input-format csv --sql "SELECT id,label FROM 'target/local-source-runtime.csv' WHERE amount >= 10 LIMIT 1" --request collect --bounded true --format json
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').sql(\"SELECT id,label FROM 'target/local-source-runtime.csv' WHERE amount >= 10 LIMIT 1\").collect(); print(r.result_rows, r.fallback_attempted, r.external_engine_invoked)"
```

This example submits the shown CSV projection, filter, and limit through the shared native workflow
and requests collection of the complete typed result. Input, expression, and sink admission is
checked by ShardLoom; unsupported requests return deterministic diagnostics.

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

This example prepares a local Vortex artifact from the shown CSV input. The command enables the
`vortex-write` feature; source type and writer admission still apply. It is a local example, not a
claim of support for every source format or sink.

## SQL Local JSONL Cast Predicate Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
{"id":1,"amount":"8","label":"low"}
{"id":2,"amount":"15","label":"mid"}
{"id":3,"amount":"21","label":"high"}
"@ | Set-Content -Encoding utf8 target\sql-local-source-cast.jsonl
cargo run -q -p shardloom-cli --features release-user-surfaces -- run sql --input target/sql-local-source-cast.jsonl --input-format jsonl --sql "SELECT id,amount,label FROM 'target/sql-local-source-cast.jsonl' WHERE CAST(amount AS int64) >= 10 LIMIT 10" --request collect --bounded true --format json
```

This example sends the shown JSONL cast predicate to the shared native workflow and requests the
complete typed result. Cast admission depends on the input and target dtypes; unsupported shapes
produce deterministic diagnostics.

## SQL Local CSV Date Arithmetic Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,event_date
1,2026-05-18
2,2026-05-19
3,2026-05-20
"@ | Set-Content -Encoding utf8 target\sql-local-source-date.csv
cargo run -q -p shardloom-cli --features release-user-surfaces -- run sql --input target/sql-local-source-date.csv --input-format csv --sql "SELECT id,event_date FROM 'target/sql-local-source-date.csv' WHERE DATE_ADD_DAYS(CAST(event_date AS date32), 1) >= DATE '2026-05-20' LIMIT 10" --request collect --bounded true --format json
```

This example submits a Date32 day-arithmetic predicate through the shared native workflow and
requests the complete typed result. The query's input and expression types are checked during native
admission; unsupported combinations produce deterministic diagnostics.

## SQL Local CSV Date Extract Smoke

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,event_date
1,2026-04-18
2,2026-05-19
3,2026-05-20
"@ | Set-Content -Encoding utf8 target\sql-local-source-date.csv
cargo run -q -p shardloom-cli --features release-user-surfaces -- run sql --input target/sql-local-source-date.csv --input-format csv --sql "SELECT id,event_date FROM 'target/sql-local-source-date.csv' WHERE DATE_YEAR(CAST(event_date AS date32)) = 2026 AND DATE_MONTH(event_date) = 5 AND DATE_DAY(event_date) >= 19 LIMIT 10" --request collect --bounded true --format json
```

This example submits the shown Date32 extraction predicate through the shared native workflow and
requests the complete typed result. Input and expression types are checked during native admission;
unsupported combinations produce deterministic diagnostics.

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
cargo run -q -p shardloom-cli --features release-user-surfaces -- run sql --input target/local-source-runtime.csv --input-format csv --sql "SELECT count(*),sum(amount),avg(amount),min(amount),max(amount) FROM 'target/local-source-runtime.csv' WHERE amount >= 10 LIMIT 1" --request collect --bounded true --format json
```

This example submits the shown scalar aggregate query through the shared native workflow and
requests the complete typed result. Aggregate argument and result types are subject to native
admission; unsupported combinations produce deterministic diagnostics.

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
cargo run -q -p shardloom-cli --features release-user-surfaces -- run sql --input target/sql-local-source-group-by.csv --input-format csv --sql "SELECT region,count(*),sum(amount) FROM 'target/sql-local-source-group-by.csv' WHERE amount >= 0 GROUP BY region LIMIT 10" --request collect --bounded true --format json
```

This example submits the shown grouped aggregate query through the shared native workflow and
requests the complete typed result. Grouping keys and aggregate input/output types are checked by
native admission; unsupported combinations produce deterministic diagnostics.

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
cargo run -q -p shardloom-cli --features release-user-surfaces -- run sql --input target/sql-local-source-topn.csv --input-format csv --sql "SELECT id,label FROM 'target/sql-local-source-topn.csv' WHERE amount >= 10 ORDER BY amount DESC LIMIT 2" --request collect --bounded true --format json
```

This example submits the shown filtered order-and-limit query through the shared native workflow
and requests the complete typed result. Sort keys, ordering expressions, and types are governed by
native admission; unsupported combinations produce deterministic diagnostics.

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
cargo run -q -p shardloom-cli --features release-user-surfaces -- run sql --source-bindings '{"target/sql-local-source-join-fact.csv":{"input_format":"csv"},"target/sql-local-source-join-dim.csv":{"input_format":"csv"}}' --sql "SELECT f.id,d.segment FROM 'target/sql-local-source-join-fact.csv' AS f INNER JOIN 'target/sql-local-source-join-dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 LIMIT 10" --request collect --bounded true --format json
```

This example submits the shown two-source inner equi-join through the shared native workflow and
requests the complete typed result. The bindings declare both local files as CSV inputs. Join shapes,
input dtypes, and output sinks are checked by native admission; unsupported combinations produce
deterministic diagnostics.

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
print(written.status, written.output_path, written.rows_written)
print(written.output_commit_status, written.native_io_certificate_status)
print(written.fallback_attempted, written.external_engine_invoked)
print(written.claim_summary.claim_gate_status)
print(aggregate.result_rows)
print(row_count.result_rows)
print(grouped.result_rows)
print(topn.result_rows)
print(joined.result_rows)
print(joined_grouped.result_rows)
print(joined.evidence_summary.command)
print(joined.claim_summary.public_performance_claim_allowed)
'@ | python -
```

These Python examples use the shared native workflow for collection and requested writes. They show
complete typed rows for the listed projection, filter, aggregates, ordering, and join shapes; the
write example reports sink commit and native-I/O status. Input, expression, dtype, and output
admission remain governed by the native planner and enabled adapters. `result_rows`,
`evidence_summary`, and `claim_summary` expose returned rows and execution posture without requiring
callers to parse raw JSON.

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
