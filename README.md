# ShardLoom

[![CI](https://github.com/depsilon/shardloom/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/depsilon/shardloom/actions/workflows/ci.yml?query=branch%3Amain)
[![PyPI](https://img.shields.io/pypi/v/shardloom?label=PyPI)](https://pypi.org/project/shardloom/)
[![Python client versions](https://img.shields.io/pypi/pyversions/shardloom?label=Python%20client)](https://shardloom.io/field-guide/start-local-proof/)
[![Homebrew](https://img.shields.io/badge/Homebrew-depsilon%2Ftap%2Fshardloom-2f4f4f)](https://github.com/depsilon/homebrew-tap)
[![Website](https://img.shields.io/badge/website-shardloom.io-0f766e)](https://shardloom.io/)
[![Field Guide](https://img.shields.io/badge/docs-field_guide-2563eb)](https://shardloom.io/field-guide/)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Patent Pending](https://img.shields.io/badge/patent--pending-designs-7c3aed)](#license)

**A unified data processing engine.**

Read, transform, query, and write data using SQL and DataFrame workflows, with multiple
input and output formats and one shared execution engine. Use SQL or DataFrames in
Python, run queries from the command line, or embed ShardLoom in an application.

ShardLoom reduces unnecessary reads, decoding, and intermediate data movement while
keeping memory and parallelism explicit. The published technical preview supports
specific local analytical workflows; broader capabilities remain under development.

**[Explore the website](https://shardloom.io/) · [Read the field guide](https://shardloom.io/field-guide/) · [Install and run](https://shardloom.io/field-guide/start-local-proof/)**

## First query

<!-- BEGIN SHARDLOOM RELEASE -->
Published **1.0.0**. [Install and platform requirements](https://shardloom.io/field-guide/start-local-proof).

```sh
python -m pip install shardloom==1.0.0
```

- macOS 26 or newer, Apple silicon; CPython 3.10 or newer; native or subprocess.

- Linux x86_64, glibc 2.39 or newer; CPython 3.10 or newer; native or subprocess.

- Windows x86_64; CPython 3.13; subprocess.

The example selects native execution. Use `transport="subprocess"` for the Windows CLI-backed package.
<!-- END SHARDLOOM RELEASE -->

<!-- BEGIN SHARDLOOM QUICKSTART -->
```python
from pathlib import Path
from tempfile import TemporaryDirectory
import shardloom as sl

workspace = TemporaryDirectory(prefix="shardloom-orders-")
orders = Path(workspace.name) / "orders.csv"
orders.write_text("order_id,status\n1,paid\n2,pending\n3,paid\n", encoding="utf-8")

# Illustrative caller choices: 256 MiB and two execution lanes.
ctx = sl.context(transport="native", memory_bytes=256 << 20, max_parallelism=2)

paid = ctx.read_csv(orders).filter(sl.col("status") == "paid").sort("order_id")
result = paid.collect(check=True)
print(result.to_rows())

ctx.close()
workspace.cleanup()
```

Expected values:

```python
({'order_id': 1, 'status': 'paid'}, {'order_id': 3, 'status': 'paid'})
```
<!-- END SHARDLOOM QUICKSTART -->

The DataFrame is lazy: reading, filtering, and sorting describe the work; `collect()`
executes it. The example creates its own input and cleans its temporary directory.
Memory and parallelism are illustrative caller choices. Continue with
[SQL and writing results](https://shardloom.io/field-guide/start-local-proof).

## What Makes ShardLoom Different

- **[Avoid unnecessary work](https://shardloom.io/field-guide/execution-model/#avoid-work-first).**
  Answer supported queries from metadata, skip irrelevant data, and decode only what
  the result requires.
- **[Compute on compressed data](https://shardloom.io/field-guide/runtime-and-io/#native-execution).**
  Use dictionary codes and repetition counts for supported aggregations, preserving
  exact DISTINCT and grouping results.
- **[One engine across interfaces and formats](https://shardloom.io/field-guide/execution-model/#one-native-pipeline).**
  SQL and DataFrame operations describe the work. Readers and writers handle formats.
  ShardLoom plans and executes the computation.
- **[Reuse valid preparation](https://shardloom.io/field-guide/execution-routes/#prepare-once).**
  Reuse loaded data or eligible query preparation while source, schema, and file
  identities still match. Each execution computes a fresh result.
- **[Keep results ready for their next use](https://shardloom.io/field-guide/runtime-and-io/#preparation-and-result-handoffs).**
  Supported results retain engine-owned columnar data for further execution or writing,
  without reconstructing it from rows or JSON.
- **[Make resource use explicit](https://shardloom.io/field-guide/runtime-and-io/#resources-and-recovery).**
  Set memory and parallelism, inspect reservations, and use supported spill and
  cancellation behavior. A memory reservation is not a limit on total process memory.
- **[Inspect execution details](https://shardloom.io/field-guide/execution-model/#execution-evidence).**
  See what was read, computed, converted, and written. Structured reports retain the
  details needed to validate correctness and benchmark methodology.
- **[Know which engine did the work](https://shardloom.io/field-guide/execution-model/#no-fallback).**
  ShardLoom executes supported requests and rejects unsupported ones with actionable
  diagnostics. It never delegates execution to Spark, DataFusion, DuckDB, Polars,
  or another query engine.

## Architecture

**Input formats → ShardLoom engine → Output formats**

ShardLoom uses Vortex's columnar representations, encodings, and file-format
capabilities within its execution and storage architecture. Its planning, execution,
resource management, and user interfaces form one ShardLoom system. Vortex is the
highest-fidelity persistence target; other formats have explicit type and metadata
limits. A Vortex file written by ShardLoom remains a Vortex file, with any additional
metadata or required extensions documented separately.

The architecture guide explains [work scheduling — PulseWeave — and bounded execution
tasks](https://shardloom.io/field-guide/compute-flow/#where-the-differentiators-apply),
including which controls are active and which are only reported as available.

## Explore the Field Guide

| Start with | What you will find |
| --- | --- |
| [Install and run](https://shardloom.io/field-guide/start-local-proof/) | Packages, source setup, and your first local query |
| [Python and DataFrames](https://shardloom.io/field-guide/python-surface/) | Queries, preparation reuse, batch input, and results |
| [SQL](https://shardloom.io/field-guide/sql/) | Supported queries and SQL through Python or the CLI |
| [Reading and writing data](https://shardloom.io/field-guide/runtime-and-io/) | Supported operators, formats, and writers |
| [How ShardLoom executes queries](https://shardloom.io/field-guide/execution-model/) | How the engine avoids work and carries data to its next use |
| [Query walkthroughs](https://shardloom.io/field-guide/compute-flow/) | The architecture from source to result |
| [Performance and benchmarks](https://shardloom.io/field-guide/benchmark-methodology/) | Workload evidence and measurement boundaries |
| [Support and limitations](https://shardloom.io/field-guide/limitations/) | Current coverage and remaining work |

For repository work, see [contributing](CONTRIBUTING.md), [agent and development instructions](AGENTS.md),
the [phase plan](docs/architecture/phased-execution-plan.md),
the [public support matrix](docs/release/public-status-matrix.md), and the
[API index](docs/reference/shardloom-user-surface-index.md) ([machine-readable](docs/reference/shardloom-user-surface-index.json)).

## License

ShardLoom is licensed under [Apache-2.0](LICENSE). PulseWeave, capillary work units, dynamic
work shaping, and related route/evidence/certificate machinery include patent-pending designs.
This notice does not expand the project's support claims.

ShardLoom is independent of, and not endorsed by, Vortex.
