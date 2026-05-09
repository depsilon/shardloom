# Traditional Analytics Benchmark Harness

This harness runs conventional dataframe/SQL workloads against ShardLoom plus
external comparison engines:

- ShardLoom
- pandas
- Polars
- DuckDB
- Spark/PySpark
- DataFusion Python
- Dask

The external engines are benchmark tooling only. They are never ShardLoom
runtime dependencies and never execute unsupported ShardLoom plans as fallback
engines.

## Workloads

The deterministic CSV generator creates a fact table and a dimension table, then
runs:

- `csv/file ingest`
- `selective filter`
- `group by aggregation`
- `sort and top-k`
- `hash join`
- `wide projection`
- `distinct count`

An opt-in stress lane is available with `--include-stress`:

- `scale stress skewed join aggregation`
- `scale stress multi-stage etl`

The stress lane is for volume/complexity bring-up. It combines larger CSV
inputs with shuffle-heavy joins, skewed grouped aggregation, derived metrics,
sort/top-N, and multi-stage ETL work that is usually where Spark-style engines
are most relevant. On a small local smoke dataset these rows are not expected to
prove Spark-only behavior; use larger-than-memory data, repeated runs, and the
same hardware/cache settings before drawing conclusions.

Each run writes a machine-readable JSON artifact and a human-readable Markdown
report. The report begins with fairness parameters, then includes an engine
overview, scenario timing matrix, fastest-row table, ASCII timing bars,
ShardLoom native microbenchmarks, universal-I/O blocker lanes, correctness
summary, and separate failure/unsupported rows.

Each result artifact records engine versions, Python/runtime details, dataset
shape, file sizes, wall/query time, sampled peak RSS when `psutil` is available,
rows scanned, rows materialized, bytes read, object-store request count, and a
correctness digest.

ShardLoom traditional analytics rows are expected to report `unsupported` until
native CSV/SQL/operator/adapter execution exists. That does not block the six
external baselines from running.

Dask is sensitive to partitioning, scheduler choice, file count, and dataset
size. The harness records `--dask-blocksize` and `--dask-scheduler`; small
single-file CSV runs can make scheduler overhead dominate.

This benchmark is intentionally explicit about fairness parameters. Before
interpreting results, check row count, storage format, cache mode, timing scope,
Dask partitioning, Spark Java status, and whether universal I/O or object-store
lanes were included.

## Setup

Use an isolated virtual environment. Do not add these packages to the Rust
workspace.

```powershell
python -m venv benchmarks\traditional_analytics\.venv
benchmarks\traditional_analytics\.venv\Scripts\python -m pip install -r benchmarks\traditional_analytics\requirements.txt
```

Spark/PySpark also requires a local JDK. Install JDK 17 or newer, set
`JAVA_HOME`, and ensure `java` is on `PATH` before expecting Spark rows to run.
Without Java, the harness records Spark as a missing dependency while still
running the other engines.

## Run

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --rows 100000 --iterations 3 --require-all-engines
```

`--require-all-engines` is strict for automation: it still writes JSON and
Markdown artifacts, but exits nonzero if an engine dependency is missing. Use the
default mode while bringing up local dependencies so partial results remain easy
to inspect.

For a fast smoke run:

```powershell
python benchmarks\traditional_analytics\run.py --rows 10000 --iterations 1
```

Run one engine or one scenario while troubleshooting:

```powershell
python benchmarks\traditional_analytics\run.py --engines polars --scenario "group by aggregation" --rows 10000 --iterations 1
```

Run the optional stress lane:

```powershell
python benchmarks\traditional_analytics\run.py --include-stress --rows 1000000 --iterations 3
```

Artifacts are written to `benchmarks/traditional_analytics/results/` by default.
Generated data and result artifacts are intentionally ignored by git unless a
specific report is promoted into `docs/benchmarks/`.
