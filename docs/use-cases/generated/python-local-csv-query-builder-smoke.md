<!-- SPDX-License-Identifier: Apache-2.0 -->

# Python local CSV/JSONL query-builder projection, aggregate, group-by, and top-N smoke

## Quick Answer

- **Audience:** Python user who wants a tiny DataFrame-like local CSV or flat JSONL workflow with evidence
- **Status:** `smoke_supported`
- **Execution mode:** `direct_compatibility_transient`
- **Engine mode:** `batch`
- **Claim boundary:** Scoped Python read_csv/select/filter/limit and read_json/select/filter/limit over local flat .jsonl/.ndjson sources with comparison, cast, date-literal, null, string, logical, and balanced parenthesized predicates; CSV and flat JSONL/NDJSON filter/scalar aggregate/limit, filter/one-column group_by/aggregate/limit, and select/filter/single-key numeric sort/limit collect/write workflows. No plain .json, nested JSON, JSONPath, pandas/Polars backend, broad DataFrame runtime, generalized grouped aggregate or ordering runtime, named grouped aggregate aliases, production SQL support, object-store/table source, external fallback, or performance claim.

## Can ShardLoom Do This?

Python local CSV/JSONL query-builder projection, aggregate, group-by, and top-N smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Scoped Python read_csv/select/filter/limit and read_json/select/filter/limit over local flat .jsonl/.ndjson sources with comparison, cast, date-literal, null, string, logical, and balanced parenthesized predicates; CSV and flat JSONL/NDJSON filter/scalar aggregate/limit, filter/one-column group_by/aggregate/limit, and select/filter/single-key numeric sort/limit collect/write workflows. No plain .json, nested JSON, JSONPath, pandas/Polars backend, broad DataFrame runtime, generalized grouped aggregate or ordering runtime, named grouped aggregate aliases, production SQL support, object-store/table source, external fallback, or performance claim.

## How To Try It

```powershell
New-Item -ItemType Directory -Force target | Out-Null; "id,label,amount`n1,alpha,8`n2,beta,15`n3,beta,21`n4,gamma,`n" | Set-Content -Encoding utf8 target\sql-local-source-smoke.csv; $env:PYTHONPATH = "python\src"; python -c "import shardloom as sl; ctx=sl.context(repo_root='.', profile_order=('debug','release')); workflow=ctx.read_csv('target/sql-local-source-smoke.csv').select('id','label').filter('amount >= 10').limit(1); r=workflow.write('target/sql-local-source-result.jsonl', allow_overwrite=True); a=ctx.read_csv('target/sql-local-source-smoke.csv').filter('amount >= 10').aggregate('count(*)','sum(amount)','avg(amount)').limit(1).collect(); g=ctx.read_csv('target/sql-local-source-smoke.csv').filter('amount >= 10').group_by('label').agg('count(*)','sum(amount)').limit(10).collect(); t=ctx.read_csv('target/sql-local-source-smoke.csv').select('id','label').filter('amount >= 0').sort('amount', descending=True).limit(2).collect(); print(r.output_path, r.output_native_io_certificate_status, a.aggregate_operator_family, g.aggregate_operator_family, g.group_by_columns, t.sort_keys, t.top_n_limit, r.fallback_attempted, r.external_engine_invoked)"
```

## Blocker

The Python query-builder runtime admits local CSV and local flat JSONL/NDJSON select/filter/limit with admitted predicate leaves and balanced grouping parentheses, scalar aggregate/filter/limit, one-column group_by/filter/aggregate/limit, and single-key numeric sort/filter/limit collect/write through the SQL local-source smoke. Joins, plain .json, nested JSON, JSONPath, arbitrary predicate-tree completeness beyond admitted leaves, multi-key/grouped aggregate generality, named grouped aggregate aliases, generalized ordering/null/collation support, windows, schema/data-quality helpers, object stores, tables, pandas/Polars execution, and production DataFrame parity require later runtime slices.

## Internal Flow

`local_csv, local_jsonl, local_ndjson -> direct_compatibility_transient -> batch -> inline_jsonl_result, local_jsonl_output, scalar_aggregate_result, grouped_aggregate_result, topn_result, typed_python_report, evidence_summary, claim_summary, sql_local_source_evidence -> evidence -> claim gate`

## Evidence You Should See

- `schema_version=shardloom.sql_local_source_smoke.v1`
- `sql_parser_executed=true`
- `sql_binder_executed=true`
- `sql_planner_executed=true`
- `source_format`
- `source_io_performed=true`
- `source_state_id`
- `source_state_digest`
- `aggregate_runtime_execution`
- `aggregate_operator_family`
- `group_by_runtime_execution`
- `group_by_columns`
- `group_by_group_count`
- `order_by_runtime_execution`
- `top_n_runtime_execution`
- `sort_keys`
- `sort_direction`
- `sort_null_ordering`
- `top_n_limit`
- `output_io_performed=true`
- `output_native_io_certificate_status`
- `evidence_summary`
- `claim_summary`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status=fixture_smoke_only`

## Expected Output Or Evidence

A typed Python report over the SQL local-source JSON envelope with local CSV or flat JSONL source evidence, source_format/source_state fields, source-format-aware source/execution certificate refs, materialization boundary and claim-gate reason fields, local JSONL output evidence when written, scalar/grouped/top-N fields when requested, group_by columns/count for grouped workflows, sort key/direction/top-N limit for sorted workflows, output Native I/O certificate status, compact evidence_summary/claim_summary helpers, fallback_attempted=false, external_engine_invoked=false, and claim_gate_status=fixture_smoke_only.

## Common Mistakes

- `expecting_dataframe_parity`
- `expecting_pandas_or_polars_execution`
- `expecting_plain_json_or_nested_json_runtime`
- `treating_fixture_smoke_as_production_support`
- `expecting_general_sort_or_null_ordering_support`

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
