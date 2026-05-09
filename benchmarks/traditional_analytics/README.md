# Traditional Analytics Benchmark Harness

This harness runs conventional dataframe/SQL workloads against ShardLoom plus
external comparison engines:

- ShardLoom
- ShardLoom native Vortex
- pandas
- Polars
- DuckDB
- Spark/PySpark default local profile
- Spark/PySpark tuned local profile
- DataFusion Python
- Dask

The external engines are benchmark tooling only. They are never ShardLoom
runtime dependencies and never execute unsupported ShardLoom plans as fallback engines.

## Workloads

The deterministic generator creates a fact table and a dimension table as CSV,
then writes requested compatibility-format copies. The default run covers CSV
and Parquet; `--formats` can also include JSONL/NDJSON, Arrow IPC, Avro, and
ORC. Each engine runs only the formats it declares support for, and unsupported
rows are captured without aborting the report. The `shardloom` lane imports
each selected compatibility format into local Vortex files before running the
temporary benchmark operator. The `shardloom-vortex` lane runs the same
scenario labels from native `.vortex` inputs prepared before scenario timing.
The scenarios are:

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
overview with startup/warmup timing, scenario timing matrix, resource metrics,
ShardLoom runtime-effect evidence, fastest-row table, ASCII timing bars,
ShardLoom native microbenchmarks, ShardLoom DecisionTrace/WhyReport evidence,
ShardLoom work-avoidance evidence, ShardLoom write/commit evidence,
universal-I/O evidence lanes, correctness summary, and separate
failure/unsupported rows.

Each result artifact records engine versions, Python/runtime details, dataset
shape, selected source file sizes, wall/query time, sampled peak RSS when
`psutil` is available, rows scanned, rows materialized, bytes read, object-store
request count, and a correctness digest. ShardLoom rows also retain the emitted
native I/O evidence fields for per-path certificate id/status, source
capability, pushdown, sink requirement, adapter fidelity, materialization
boundary, decode, materialization, row reads, Arrow conversion, writes, spill,
auto-derived resource sizing, and NativeIoCertificate status.

ShardLoom's reported benchmark version appends `-dirty` when the workspace has
uncommitted tracked changes, so local bring-up reports do not look like clean
revision evidence by accident.

ShardLoom native microbenchmark rows are separated from the traditional
compatibility-file engine rows. They include the approved local encoded
`CountAll` path plus local
`vortex-run` primitive evidence for count, projection, validity-filter
counting, and scan-pushdown comparison predicates. The report includes each
row's timing scope plus filter/projection pushdown fields so in-command
repeated timings are not mixed up with CLI-process wall-time smoke measurements.

The ShardLoom work-avoidance table is based on final `vortex-run` runtime effects,
not only plan analysis. It exposes decode avoided, materialization
avoided, rows not scanned, segment prune count, bytes not read, spill avoided,
and fallback blocked. Segment-prune and bytes-not-read values remain `unknown`
until the local primitive path can measure them safely.

The ShardLoom DecisionTrace/WhyReport evidence table explains why each native
runtime row is or is not claim-grade. It records decision-trace counts, the
primary reason, summarized claim blockers, and the next evidence needed before
runtime measurements can become correctness/benchmark/certification claims.

The ShardLoom write/commit evidence table runs the current local
committed-manifest step against a synthetic staged workspace and records commit
execution, manifest-commit status, bytes written, and average commit latency.
It is a local smoke benchmark only; object-store commit, table-format commit,
and recovery timing remain separate future work.

Numeric benchmark outputs are rounded to four decimal places before correctness
hashing. This keeps result comparison stable across engines with different
floating-point aggregation orders while preserving the two-decimal source metric
precision used by the deterministic dataset.

ShardLoom traditional analytics rows call the workspace-local native Rust
command `shardloom traditional-analytics-run`. Build time is excluded from
per-scenario timing and the selected `--shardloom-build-profile` is recorded in
the fairness parameters. The harness builds ShardLoom with the
`vortex-traditional-analytics-benchmark` feature and times compatibility source
adapter/import, local Vortex file write, Vortex file reopen, Vortex scan, and
temporary benchmark operators over Vortex-derived arrays. These rows prove a
feature-gated universal-I/O smoke path for CSV, JSONL/NDJSON, Parquet, Arrow
IPC, Avro, and ORC, not the future SQL parser/DataFrame API or mature
encoded-native operator surface.

ShardLoom native Vortex rows call `shardloom traditional-analytics-vortex-run`
against `.vortex` files produced before scenario timing. This separates native
Vortex input timing from compatibility-file import timing, while still reporting that the
current benchmark operators materialize Vortex-derived arrays after scan.

ShardLoom's compatibility-format rows report `row_read=true` and
`data_materialized=true` because the benchmark source adapters parse or convert
local compatibility files before Vortex import. That is intentionally
conservative: native Vortex microbenchmark rows remain separate and expose the
currently available zero-decode/no-row-read primitive evidence.

ShardLoom resource sizing is automatic by default. The CLI derives applied
parallelism from local CPU availability and derives batch/partition sizing from
the source footprint plus the resource budget. `--memory-gb` and
`--max-parallelism` are optional caps for reproducible troubleshooting, not
required user tuning knobs.

Dask is sensitive to partitioning, scheduler choice, file count, and dataset
size. The harness records `--dask-blocksize` and `--dask-scheduler`; small
single-file CSV runs can make scheduler overhead dominate.

This benchmark is intentionally explicit about fairness parameters. Before
interpreting results, check row count, storage format, cache mode, timing scope,
Dask partitioning, Spark Java status, Spark default/tuned-local profile split,
ShardLoom build profile/feature gate, whether CSV/Parquet/JSONL/Arrow IPC/Avro/ORC/native
Vortex rows were included, the applied ShardLoom resource policy, and whether
object-store lanes were included.

## Setup

Use an isolated virtual environment. Do not add these packages to the Rust
workspace.

```powershell
python -m venv benchmarks\traditional_analytics\.venv
benchmarks\traditional_analytics\.venv\Scripts\python -m pip install -r benchmarks\traditional_analytics\requirements.txt
```

Avro fixture generation uses `fastavro` from the benchmark virtual environment
only. Rust runtime Avro coverage is feature-gated in `shardloom-vortex` through
Apache Arrow's `arrow-avro` crate.

Spark/PySpark also requires a local JDK. Install JDK 17 or newer, set
`JAVA_HOME`, and ensure `java` is on `PATH` before expecting Spark rows to run.
Without Java, the harness records Spark profiles as missing dependencies while
still running the other engines.

Spark rows are split into `spark-default` and `spark-local-tuned`. The default
profile uses `local[*]` plus Spark defaults, while the tuned profile caps
shuffle/default parallelism to the local CPU count and enables AQE. The `spark`
engine alias expands to both profiles. Each Spark profile starts and warms its
own Spark session immediately before its scenario rows, and the harness records
that startup/warmup time separately from per-scenario timings.

On Windows the harness also checks common Temurin/Eclipse Adoptium install
paths and will set `JAVA_HOME` for the benchmark process when it finds a local
JDK there.

## Run

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --rows 100000 --iterations 3 --formats csv,parquet --require-all-engines
```

`--require-all-engines` is strict for automation: it still writes JSON and
Markdown artifacts, but exits nonzero if an engine dependency is missing. Use the
default mode while bringing up local dependencies so partial results remain easy
to inspect.

For a fast smoke run:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --rows 10000 --iterations 1
```

Run one engine or one scenario while troubleshooting:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines polars --scenario "group by aggregation" --rows 10000 --iterations 1
```

Run only ShardLoom's universal-I/O smoke row while troubleshooting its local
Vortex artifacts:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines shardloom --scenario "group by aggregation" --rows 10000 --iterations 1
```

Run ShardLoom across all currently supported local compatibility formats:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --engines shardloom --scenario "csv/file ingest" --formats csv,jsonl,parquet,arrow-ipc,avro,orc --rows 10000 --iterations 1
```

Run the optional stress lane:

```powershell
benchmarks\traditional_analytics\.venv\Scripts\python benchmarks\traditional_analytics\run.py --include-stress --rows 1000000 --iterations 3
```

Artifacts are written to `benchmarks/traditional_analytics/results/` by default.
Generated data and result artifacts are intentionally ignored by git unless a
specific report is promoted into `docs/benchmarks/`.
