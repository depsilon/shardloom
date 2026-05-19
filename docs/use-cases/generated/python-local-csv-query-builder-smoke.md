<!-- SPDX-License-Identifier: Apache-2.0 -->

# Python local CSV query-builder smoke

## Quick Answer

- **Audience:** Python user who wants a tiny DataFrame-like local CSV workflow with evidence
- **Status:** `smoke_supported`
- **Execution mode:** `direct_compatibility_transient`
- **Engine mode:** `batch`
- **Claim boundary:** One scoped Python read_csv/select/filter/limit collect/write workflow over local CSV only; no pandas/Polars backend, broad DataFrame runtime, production SQL support, object-store/table source, external fallback, or performance claim.

## Can ShardLoom Do This?

Python local CSV query-builder smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

One scoped Python read_csv/select/filter/limit collect/write workflow over local CSV only; no pandas/Polars backend, broad DataFrame runtime, production SQL support, object-store/table source, external fallback, or performance claim.

## How To Try It

```powershell
New-Item -ItemType Directory -Force target | Out-Null; "id,label,amount`n1,alpha,8`n2,beta,15`n3,gamma,`n" | Set-Content -Encoding utf8 target\sql-local-source-smoke.csv; $env:PYTHONPATH = "python\src"; python -c "import shardloom as sl; ctx=sl.context(repo_root='.', profile_order=('debug','release')); workflow=ctx.read_csv('target/sql-local-source-smoke.csv').select('id','label').filter('amount >= 10').limit(1); r=workflow.write('target/sql-local-source-result.jsonl', allow_overwrite=True); print(r.output_path, r.output_native_io_certificate_status, r.fallback_attempted, r.external_engine_invoked)"
```

## Blocker

The Python query-builder runtime admits only local CSV select/filter/limit collect/write through the SQL local-source smoke. Joins, aggregates, windows, schema/data-quality helpers, object stores, tables, pandas/Polars execution, and production DataFrame parity require later runtime slices.

## Internal Flow

`local_csv -> direct_compatibility_transient -> batch -> inline_jsonl_result, local_jsonl_output, typed_python_report, sql_local_source_evidence -> evidence -> claim gate`

## Evidence You Should See

- `schema_version=shardloom.sql_local_source_smoke.v1`
- `sql_parser_executed=true`
- `sql_binder_executed=true`
- `sql_planner_executed=true`
- `source_io_performed=true`
- `output_io_performed=true`
- `output_native_io_certificate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status=fixture_smoke_only`

## Expected Output Or Evidence

A typed Python report over the SQL local-source JSON envelope with local CSV source evidence, local JSONL output evidence, output Native I/O certificate status, fallback_attempted=false, external_engine_invoked=false, and claim_gate_status=fixture_smoke_only.

## Common Mistakes

- `expecting_dataframe_parity`
- `expecting_pandas_or_polars_execution`
- `treating_fixture_smoke_as_production_support`

## Reference Files

- `python/README.md` - What this proves: Python wrapper posture, local smoke usage, and Python API claim boundaries.
- `docs/getting-started/examples.md` - What this proves: Current example catalog and local workflow entrypoints.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `README.md` - What this proves: Public technical-preview posture, Vortex-first/no-fallback positioning, and primary repo entrypoints.

## Related Use Cases

- `python-wrapper-client-smoke`
- `sql-local-source-csv-smoke`
- `sql-dataframe-capability-posture`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- `website/field-guide/direct-compatibility-transient.html` - Direct Compatibility Transient (`Execution Modes` / `scoped-local-smoke`)
- `website/field-guide/python-wrapper-client.html` - Python Wrapper Client (`User Workflows` / `current-local-client`)
