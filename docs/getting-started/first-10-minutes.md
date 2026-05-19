<!-- SPDX-License-Identifier: Apache-2.0 -->

# First 10 Minutes

This proof uses a source checkout and local commands only. It does not require
Spark, DataFusion, DuckDB, Polars, pandas, Foundry, object stores, or network
services. The fastest complete path is the local release dry run below: it
builds source artifacts, installs the exact local wheel in a clean virtual
environment, runs smoke checks, writes scoped generated-source local outputs,
runs a tiny compatibility/prepared-Vortex benchmark smoke, and records the
evidence transcript.

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
```

The transcript is written to `target/release-dry-run-proof/transcript.json`.
It is local technical-preview evidence only. It does not publish packages,
create tags, add secrets, install fallback engines, make a performance claim, or
turn local package proof into a public package release.

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

## 5. Try Generated-Source Local Output

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').from_rows([{'id': 1, 'label': 'alpha'}]).write('target/generated-reference.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.generated_source_certificate_status, r.output_native_io_certificate_status, r.fallback_attempted, r.external_engine_invoked, r.claim_gate_status)"
```

This is source-free generated-output execution, not no-dataset smoke. The current runtime support
is scoped to local JSONL/CSV output from Python `ctx.from_rows(...).write(...)`,
`ctx.literal_table(...).write(...)`, `ctx.calendar(...).write(...)`, and
`ctx.range(...).write(...)`/`ctx.sequence(...).write(...)` smokes plus source-free SQL `VALUES` and
literal `SELECT` local JSONL/CSV smokes through `ctx.sql_values(...).write(...)`,
`ctx.sql_literal_select(...).write(...)`, and the scoped `ctx.sql(...).write(...)` bridge. Broad
SQL/DataFrame runtime, object-store/lakehouse output, and Foundry generated-output runtime remain
unclaimed.

## 6. Try A Local Compatibility/Prepared-Vortex Benchmark Smoke

```powershell
python examples\local-vortex-benchmark\run.py --repo-root . --rows 64 --iterations 1
```

This wraps the local taxonomy benchmark harness with a small ShardLoom-only
smoke configuration. By default it runs both `shardloom` and
`shardloom-prepared-vortex` so the compatibility-import certification lane and
current prepared-Vortex runtime-development lane are visible separately.

Additional example metadata, expected outputs, certificate fields, and known
limitations are listed in `docs/getting-started/examples.md`.

## Release Dry-Run Proof

For a single local proof that builds source artifacts, installs the local wheel
in a clean virtual environment, resolves the built CLI, runs the smoke checks,
writes generated-source local JSONL/CSV outputs, executes the prepared/native
benchmark smoke, and runs provenance dry-run evidence, use:

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
