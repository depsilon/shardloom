<!-- SPDX-License-Identifier: Apache-2.0 -->

# First 10 Minutes

This proof uses a source checkout and local commands only. It does not require
Spark, DataFusion, DuckDB, Polars, pandas, Foundry, object stores, or network
services.

## 1. Build The CLI

```powershell
cargo build -p shardloom-cli --bin shardloom
```

## 2. Run Status And Capabilities

```powershell
target\debug\shardloom status --format json
target\debug\shardloom capabilities --format json
```

## 3. Run The Python Smoke

```powershell
$env:PYTHONPATH = "python\src"
python examples\local-python-smoke\run.py --repo-root .
```

The script imports the Python wrapper, runs status, smoke, and capability
checks, and exits nonzero if fallback is attempted.

## 4. Inspect The Current Certified Slice

The current scoped workload certification is `local_vortex_analytics_v1`.
It is a local Vortex analytics workflow, not a broad SQL/DataFrame/live/hybrid
or Foundry production claim. See
`docs/getting-started/certified-local-workload.md` for the details.

## 5. Try A Local Benchmark Smoke

```powershell
python examples\local-vortex-benchmark\run.py --repo-root .
```

This wraps the local taxonomy benchmark harness with a small ShardLoom-only
smoke configuration.
