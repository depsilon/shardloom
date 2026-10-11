<!-- SPDX-License-Identifier: Apache-2.0 -->

# First 10 Minutes

This proof uses a source checkout and local commands only. The local release dry run does not require
Spark, DataFusion, DuckDB, Polars, pandas, Foundry, object stores, or network
services. The optional native benchmark comparison uses pandas only as an independent correctness
reference. The fastest complete path is the local release dry run below: it
builds source artifacts, installs the exact local wheel in a clean virtual
environment, runs smoke checks, writes scoped memory-backed local outputs,
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
cargo build -p shardloom-cli --bin shardloom --features release-user-surfaces
```

## 2. Run Status And Capabilities

```powershell
target\debug\shardloom status --format json
target\debug\shardloom capabilities --format json
```

## 3. Run The Python Smoke

```powershell
$env:PYTHONPATH = "python\src"
python examples\local-python-smoke\run.py --repo-root . --memory-gb 16 --max-parallelism 8
```

The script imports the Python wrapper, runs status, smoke, and capability
checks, creates a fresh `target/local-python-smoke/run-*/orders.csv`, runs a bounded CSV
workflow and a caller-declared memory-row write through the shared native
route, checks an unsupported UDF request, and prints evidence markers such as
`quickstart_local_file_blocker_id`, `quickstart_generated_output_row_count`,
`quickstart_generated_claim_gate_status`, and `quickstart_unsupported_blocker_id`.
It exits nonzero if fallback or external-engine execution is attempted, if the
memory-backed workflow emits no row, or if diagnostic-only paths lack stable
blockers.

## 4. Inspect The Current Certified Slice

The current scoped workload certification is `local_vortex_analytics_v1`.
It is a local Vortex analytics workflow, not a broad SQL/DataFrame/live/hybrid
or Foundry production claim. See
`docs/getting-started/certified-local-workload.md` for the details.

## 5. Try Memory-Backed Native Output

The 16 GiB / 8 lane values below are illustrative caller choices, not recommended defaults or measured usage.

After creating `ctx = context(repo_root='.', memory_gb=16, max_parallelism=8)`, use `ctx.read('orders.csv')`
for a file, `ctx.from_rows([{'id': 1}])` for declared memory rows, or
`ctx.range(0, 3)` for generated rows. These declarations enter the same native
planner when collected or written; Python does not evaluate the query.

```powershell
$env:PYTHONPATH = "python\src"
python -c "from pathlib import Path; import tempfile; from shardloom import context; output=Path(tempfile.mkdtemp(prefix='shardloom-first-steps-'))/'generated-reference.jsonl'; ctx=context(repo_root='.', memory_gb=16, max_parallelism=8); r=ctx.from_rows([{'id': 1, 'label': 'alpha'}]).write(output); print(output, r.envelope.status, r.fallback_attempted, r.external_engine_invoked, r.claim_gate_status)"
```

The example writes to a new temporary directory on each run and prints its path.

Source-free and typed-memory declarations use the shared native workflow. `collect()` can return
complete typed results, and callers can request the same declared sinks used by other workflows.
Release builds with `release-user-surfaces` enable complete execution and admitted sinks; available
format and dtype combinations remain subject to adapter and planner checks. This example shows one
memory-row write only; it does not imply unrestricted SQL or DataFrame execution.
Unsupported requests fail with deterministic diagnostics instead of switching engines.

## 6. Try The Guarded Native Benchmark Comparison

```powershell
python examples\local-vortex-benchmark\run.py `
  --shardloom-binary target\debug\shardloom `
  --workspace "$HOME\LocalData\shardloom\traditional-benchmarks" `
  --repo-root . --rows 64 --dim-rows 8 --repeats 1 --formats csv `
  --input-state raw --output-format collect --reference-engine pandas
```

Supply an already-built ShardLoom executable and a local-only workspace outside synced folders. The
example delegates fixture creation, resource checks, run isolation, and output handling to the
guarded harness. It runs one `shardloom` candidate against pandas as an independent correctness
reference for the small `selective filter` workload. Raw or prepared input and `collect` or an
admitted writer can be selected; unsupported work is never delegated to pandas. This is correctness
comparison evidence only, not a performance or full acceptance claim. The example does not build
ShardLoom or install dependencies.

Additional example request/result metadata and known limitations are listed in
`docs/getting-started/examples.md`.

## Release Dry-Run Proof

For a single local proof that builds source artifacts, installs the local wheel
in a clean virtual environment, resolves the built CLI, runs the smoke checks,
writes memory-backed local outputs through the shared workflow, runs the optional
benchmark comparison, and runs provenance dry-run evidence, use:

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
