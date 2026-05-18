<!-- SPDX-License-Identifier: Apache-2.0 -->

# Examples

ShardLoom release-readiness examples are local and no-fallback by default.

## Local Python Smoke

```powershell
python examples\local-python-smoke\run.py --repo-root .
```

Use this for import, CLI resolution, status, smoke, capabilities, and
`fallback_attempted=false` proof without reading or writing datasets.

## Local Vortex Benchmark Smoke

```powershell
python examples\local-vortex-benchmark\run.py --repo-root . --rows 64 --iterations 1
```

Use this for a small ShardLoom-only local Vortex benchmark smoke with result
sink evidence. The default example uses CSV only so optional Parquet and
external baseline dependencies are not required.

## Source-Free User Rows Local Output Smoke

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; r=context(repo_root='.').from_rows([{'id': 1, 'label': 'alpha'}]).write('target/generated-reference.jsonl', allow_overwrite=True); print(r.claim_gate_status)"
```

Use this for the scoped GAR-GEN-1C path that writes caller-provided rows to a local JSONL output and
emits generated-source and output evidence. It is not SQL/VALUES execution, broad DataFrame
runtime, object-store output, Foundry output, production support, or a performance claim.

## Foundry Lightweight Transform

```powershell
python examples\foundry-lightweight-transform\run.py --repo-root .
```

Use this to inspect the future Foundry transform shape without invoking Foundry,
Foundry Spark, virtual tables, Snowflake, Databricks, BigQuery, or external
compute. The example writes a local certificate-style JSON output and keeps
staged dataset execution deferred to P9.6.

Each example includes an environment file, fixture, expected output snapshot,
expected certificate field snapshot, and known limitations.
