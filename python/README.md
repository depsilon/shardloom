# ShardLoom Python CLI Client

This package is the first thin Python surface for ShardLoom. It invokes the
workspace `shardloom` CLI with `--format json`, parses the stable
`OutputEnvelope`, and preserves diagnostics, fields, and fallback status.

It is intentionally not a native binding, DataFrame API, SQL runtime, UDF
runtime, or fallback execution path. Importing the package has no ShardLoom
side effects. Work happens only when a caller explicitly invokes a CLI command
through `ShardLoomClient`.

## Local Use

From the repository root:

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import ShardLoomClient; print(ShardLoomClient.from_repo().status().status)"
```

Use `SHARDLOOM_BIN` to point at a specific CLI binary:

```powershell
$env:SHARDLOOM_BIN = "target\release\shardloom.exe"
```

Or pass an explicit binary:

```python
from shardloom import ShardLoomClient

client = ShardLoomClient(binary="target/release/shardloom")
print(client.status().status)
```

`ShardLoomClient.from_repo()` looks for `target/release/shardloom` and then
`target/debug/shardloom` when a command is invoked. It does not run commands or
probe the repository at import time.

## Live ETL Smoke

The current live ETL surface is intentionally narrow and explicit. CSV mode
runs `traditional-analytics-run`, which imports CSV inputs into temporary local
Vortex files before running the temporary benchmark operator. Native Vortex mode
runs `traditional-analytics-vortex-run` from existing `.vortex` inputs.

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_repo()
result = client.live_etl_smoke(
    "selective filter",
    "benchmarks/traditional_analytics/data/fact.csv",
    "benchmarks/traditional_analytics/data/dim.csv",
    input_format="csv",
    workspace="target/shardloom-python-live-etl",
)

print(result.status)
print(result.field("rows_scanned"))
print(result.field("materialization_boundary_reported"))
print(result.fallback.attempted)
```

For the current CSV universal-I/O path, use the replay helper when you want to
see both parts separately: boundary import into Vortex, then steady-state native
Vortex execution from the emitted artifacts.

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_repo()
result = client.live_etl_csv_to_vortex_replay(
    "selective filter",
    "benchmarks/traditional_analytics/data/fact.csv",
    "benchmarks/traditional_analytics/data/dim.csv",
    workspace="target/shardloom-python-live-etl",
)

print(result.csv_import.field("fact_vortex_path"))
print(result.native_vortex.field("source_format") if result.native_vortex else None)
print(result.fallback_attempted)
```

Universal I/O is broader than CSV. The current adapter registry is still
plan-only, but it makes the common format, object-store, catalog, effectful,
and unstructured queues visible from Python:

```python
adapters = client.input_adapters()
print(adapters.field("common_structured_adapter_order"))
print(adapters.field("critical_structured_adapter_order"))
print(adapters.field("object_store_adapter_order"))
print(adapters.field("catalog_adapter_order"))
print(adapters.field("parquet_status"))

plan = client.input_plan("file://tmp/example.parquet")
print(plan.field("source_kind"))
print(plan.field("capability_status"))
print(plan.field("plan_only"))
```

Common structured inputs are tracked as `native_vortex`, `parquet`,
`arrow_ipc`, `csv`, JSON/NDJSON through `jsonl`, `avro`, and `orc`.
Lakehouse/table, object-store, catalog, effectful, and unstructured/media
families are also represented in the registry. Planned means no reader/runtime
behavior is implied yet; the current implemented live path remains the local
CSV-to-Vortex benchmark smoke and native `.vortex` replay.

The client also exposes advisory optimization reports:

```python
dynamic = client.dynamic_work_shaping_plan("memory-pressure")
sizing = client.sizing_feedback_plan(8, ["task-too-large", "memory-pressure-high"])
```

These commands report planned/advisory state only; they do not mutate runtime
policy yet.

Planning and evidence commands may return `status="success"` while including
error-severity diagnostics that describe missing evidence or blocked future
work. The Python client preserves those diagnostics for inspection instead of
raising unless the CLI exits nonzero or the envelope status is `error` or
`unsupported`.

The example script wires the same calls together:

```powershell
$env:PYTHONPATH = "python\src"
python python\examples\live_etl_smoke.py `
  --mode csv `
  --scenario "selective filter" `
  --fact benchmarks\traditional_analytics\data\fact.csv `
  --dim benchmarks\traditional_analytics\data\dim.csv `
  --workspace target\shardloom-python-live-etl
```

## Test

```powershell
$env:PYTHONPATH = "python\src"
python -m unittest discover python\tests
```
