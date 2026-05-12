# ShardLoom Python CLI Client

This package is the first thin Python surface for ShardLoom. It invokes the
workspace `shardloom` CLI with `--format json`, parses the stable
`OutputEnvelope`, and preserves typed result/artifact/certificate payloads,
diagnostics, fallback status, and the temporary legacy field mirror.

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

Or install the source-tree package in editable mode for notebook, job, or
Foundry-style imports:

```powershell
python -m pip install -e python
```

The package exposes a non-placeholder development version through
`shardloom.__version__`. It is still pre-release and is not published from this
repository session.

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

`ShardLoomClient.from_env()` is the import-friendly constructor for managed
Python environments. It reads configuration only and does not run commands:

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_env()
smoke = client.smoke_check()
print(smoke.commands)
print(smoke.deployment_capabilities.field("surface_components"))
print(smoke.fallback_attempted)
```

Supported environment variables:

- `SHARDLOOM_BIN`: explicit `shardloom` CLI binary path.
- `SHARDLOOM_REPO_ROOT`: source checkout containing `target/<profile>/shardloom`.
- `SHARDLOOM_PROFILE_ORDER`: comma-separated target profile order, for example `release,debug`.
- `SHARDLOOM_TIMEOUT_SECONDS`: per-command subprocess timeout.

If no CLI binary is available, explicit client commands raise
`ShardLoomBinaryNotFoundError` with installation/configuration guidance instead
of leaking a raw subprocess error. The exception carries deterministic
no-fallback diagnostics plus a `shardloom.output.v2`-shaped error payload via
`to_error_payload(command)` for agents and wrappers that need protocol-shaped
missing-binary evidence. Importing the package and constructing
`ShardLoomClient.from_env()` remain side-effect-free.

## Package Build Smoke

The current package is pure Python and has no runtime dependencies. Release
readiness can be checked locally without publishing:

```powershell
python -m pip install build
python -m build python
python -m venv $env:TEMP\shardloom-wheel-smoke
$wheel = Get-ChildItem python\dist\shardloom-*.whl | Select-Object -First 1
& $env:TEMP\shardloom-wheel-smoke\Scripts\python -m pip install $wheel.FullName
& $env:TEMP\shardloom-wheel-smoke\Scripts\python -c "import shardloom; print(shardloom.__version__)"
```

Conda packaging should stay split so the pure Python wrapper can remain
`noarch: python` while the Rust CLI binary is built as a platform-specific
package. Local recipe scaffolds live under `packaging/conda/`:

- `shardloom-cli`: compiled Rust `shardloom` binary.
- `shardloom-python`: pure Python wrapper/import surface.
- Optional `shardloom` metapackage: depends on both the wrapper and CLI for a
  one-command install path.

The recipes are not published packages. A release pass must align versions,
replace local sources with tagged source archives and hashes, review license
metadata, build packages in clean Conda environments, and receive explicit
human approval before publication.

Spark, DataFusion, Polars, DuckDB, pandas, and Dask belong only in optional
benchmark environments; they are not ShardLoom runtime dependencies or fallback
engines.

## Live ETL Smoke

The current live ETL surface is intentionally narrow and explicit.
Compatibility-file mode runs `traditional-analytics-run`, which imports CSV,
JSONL/NDJSON, Parquet, Arrow IPC, Avro, or ORC inputs into temporary local
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

Resource sizing is automatic by default. ShardLoom derives applied parallelism,
batch rows, and target partition count from the local machine and source
footprint. Pass `memory_gb=` or `max_parallelism=` only when a job or benchmark
needs explicit caps.

For the current compatibility-file universal-I/O path, use the replay helper
when you want to see both parts separately: boundary import into Vortex, then
steady-state native Vortex execution from the emitted artifacts.

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

For lower-level local Vortex primitive testing, the wrapper exposes the same
explicit CLI JSON commands used by the current CG-2/CG-13/CG-16/CG-19 evidence
path:

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_repo()

count = client.vortex_count(
    "fixtures/local_primitive_struct_five.vortex",
    execute_local_encoded_count=True,
    memory_gb=1,
    max_parallelism=2,
)

filtered = client.vortex_filter_project(
    "fixtures/local_primitive_struct_five.vortex",
    "gte:value:3",
    ["metric"],
    execute_local_primitive=True,
    memory_gb=1,
    max_parallelism=2,
)

print(count.field("execution_certificate_reported"))
print(filtered.field("native_io_certificate_reported"))
print(filtered.fallback.attempted)
```

Execution remains explicit. Calls without `execute_local_encoded_count=True` or
`execute_local_primitive=True` use the existing metadata/plan evidence surfaces
where the CLI supports them; local primitive execution also requires explicit
`memory_gb` and `max_parallelism` caps.

Universal I/O is broader than local compatibility files. The current adapter
registry also makes object-store, catalog, effectful, and unstructured queues
visible from Python:

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
families are also represented in the registry. The current implemented live
path is the feature-gated local compatibility-file-to-Vortex benchmark smoke
and native `.vortex` replay; production adapter certification, object-store
runtime, catalogs, SQL, DataFrame runtime, and UDF runtime remain future work.

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
