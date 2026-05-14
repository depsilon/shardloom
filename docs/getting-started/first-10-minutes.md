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

Additional example metadata, expected outputs, certificate fields, and known
limitations are listed in `docs/getting-started/examples.md`.

## Release Dry-Run Proof

For a single local proof that builds source artifacts, installs the local wheel
in a clean virtual environment, resolves the built CLI, runs the smoke checks,
and executes the benchmark smoke, use:

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
```

The transcript is written to `target/release-dry-run-proof/transcript.json`.
It is a local dry run only and does not publish packages, create tags, add
secrets, or install fallback engines.

When `mamba`, `conda`, or `micromamba` is available, the dry run also attempts a clean
Conda-style install proof from the locally built wheel. If no Conda-compatible tool is available,
the transcript records `clean_conda_env_install_status=skipped_tool_missing`; that remains blocked
for public release but does not weaken the local source smoke.
