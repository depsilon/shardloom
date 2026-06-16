<!-- SPDX-License-Identifier: Apache-2.0 -->

# First 10 Minutes

This proof uses a source checkout and local commands only. It does not require
Spark, DataFusion, DuckDB, Polars, pandas, Foundry, object stores, or network
services. The fastest complete path is the local release dry run below: it
builds source artifacts, installs the exact local wheel in a clean virtual
environment, runs smoke checks, writes scoped generated-source local outputs,
records that benchmark smoke is not required for package-channel proof, and
records the evidence transcript. Pass `--include-benchmark-smoke` when you
intentionally want the optional benchmark-only feature lane in the same local
transcript.

Public status is owned by `docs/release/public-status-matrix.md`. This walkthrough is local
technical-preview evidence only.

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
```

The transcript is written to `target/release-dry-run-proof/transcript.json`.
It is local technical-preview evidence only. It does not publish packages,
create tags, add secrets, install fallback engines, make a performance claim, or
turn local package proof into a public package release.

When the related local release/security/package/website reports have been generated, the
production-usability aggregate is:

```powershell
python scripts\check_production_usability_gate.py
```

That report is still local no-publication evidence. It is useful for checking that the install,
smoke, benchmark-artifact, website learning path, and unsupported-claim rows agree without reading
phase-plan internals.

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
checks, creates `target/local-python-smoke/orders.csv`, runs a scoped
`ctx.read(...).filter(...).select(...).write_jsonl(...)` workflow, runs a
scoped generated-source write, and prints result plus evidence markers such as
`quickstart_result_row_id`, `quickstart_output_row_count`,
`quickstart_claim_gate_status`, and `quickstart_unsupported_blocker_id`. It
exits nonzero if fallback or external-engine execution is attempted, if the
admitted workflow emits no row, or if the unsupported path lacks a blocker.

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
smoke configuration. By default it runs the internal `shardloom` and
`shardloom-prepared-vortex` engine IDs, presented publicly as ShardLoom Cold
Certified Route and ShardLoom Warm Prepared Query so their start states are
visible separately. Use `--engines shardloom-prepare-batch` with the underlying
`benchmarks\traditional_analytics\run.py` harness when you specifically want the
single-process compatibility prepare plus prepared/native batch route and its
`prepare_batch_*` adapter-timing evidence. Add `--include-benchmark-smoke` only when the local
benchmark smoke should run as optional benchmark evidence in the same transcript.

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

For the aggregate local usability gate and its no-publication claim boundary, see
`docs/release/production-usability-gate.md`.
