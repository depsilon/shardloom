<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL local CSV projection/optional-filter/IN/limit, aggregate, group-by, top-N, and join smoke

## Quick Answer

- **Audience:** user who wants to try one tiny SQL query over a local CSV from CLI or ctx.sql without fallback
- **Status:** `smoke_supported`
- **Execution mode:** `direct_compatibility_transient`
- **Engine mode:** `batch`
- **Claim boundary:** Scoped local CSV SELECT projection/optional-filter/limit with comparison, cast, date-literal, Date32 extract/day arithmetic, bounded IN, null, string, logical, and balanced parenthesized predicates; scalar aggregate across all rows or after a scoped filter; one-column group-by aggregate; single-key numeric ORDER BY/LIMIT top-N; ctx.sql collect/write; optional local JSONL/CSV output sinks; and one Python query-builder local CSV inner equi-join bridge. Bounded IN admits up to 32 non-null scalar literals, including DATE literal lists, and blocks empty, NULL, mixed DATE/non-DATE, and oversized lists. Date extraction is scoped to DATE_YEAR/DATE_MONTH/DATE_DAY; date arithmetic is scoped to DATE_ADD_DAYS/DATE_SUB_DAYS. No broad SQL/DataFrame runtime, production SQL support, Parquet/Arrow/Avro/ORC/Vortex output sink, multi-output fanout, object-store/table source, multi-key group-by generality, generalized ordering/null/collation support, timestamp/timezone completeness, NULL/subquery-backed IN, arbitrary predicate-tree completeness beyond admitted parenthesized leaves, outer/semi/anti/cross/multi-key/expression joins, external fallback, or performance claim.

## Can ShardLoom Do This?

SQL local CSV projection/optional-filter/IN/limit, aggregate, group-by, top-N, and join smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Scoped local CSV SELECT projection/optional-filter/limit with comparison, cast, date-literal, Date32 extract/day arithmetic, bounded IN, null, string, logical, and balanced parenthesized predicates; scalar aggregate across all rows or after a scoped filter; one-column group-by aggregate; single-key numeric ORDER BY/LIMIT top-N; ctx.sql collect/write; optional local JSONL/CSV output sinks; and one Python query-builder local CSV inner equi-join bridge. Bounded IN admits up to 32 non-null scalar literals, including DATE literal lists, and blocks empty, NULL, mixed DATE/non-DATE, and oversized lists. Date extraction is scoped to DATE_YEAR/DATE_MONTH/DATE_DAY; date arithmetic is scoped to DATE_ADD_DAYS/DATE_SUB_DAYS. No broad SQL/DataFrame runtime, production SQL support, Parquet/Arrow/Avro/ORC/Vortex output sink, multi-output fanout, object-store/table source, multi-key group-by generality, generalized ordering/null/collation support, timestamp/timezone completeness, NULL/subquery-backed IN, arbitrary predicate-tree completeness beyond admitted parenthesized leaves, outer/semi/anti/cross/multi-key/expression joins, external fallback, or performance claim.

## How To Try It

```powershell
New-Item -ItemType Directory -Force target | Out-Null; "id,customer_id,amount`n1,10,8`n2,20,15`n3,30,21`n4,99,13`n" | Set-Content -Encoding utf8 target\sql-local-source-join-fact.csv; "customer_id,segment`n10,seed`n20,enterprise`n30,startup`n" | Set-Content -Encoding utf8 target\sql-local-source-join-dim.csv; cargo run -q -p shardloom-cli -- sql-local-source-smoke "SELECT f.id,d.segment FROM 'target/sql-local-source-join-fact.csv' AS f INNER JOIN 'target/sql-local-source-join-dim.csv' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 10" --format json; $env:PYTHONPATH = "python\src"; python -c "from shardloom import context; ctx=context(repo_root='.', profile_order=('debug','release')); r=ctx.read_csv('target/sql-local-source-join-fact.csv').join(ctx.read_csv('target/sql-local-source-join-dim.csv'), on='customer_id').select('f.id','d.segment').filter('f.amount >= 10').limit(10).collect(); print(r.output_row_count, r.join_runtime_execution, r.fallback_attempted, r.external_engine_invoked)"
```

## Blocker

Vortex SQL sources, broader Parquet type/nesting coverage, Python/DataFrame joins beyond the scoped local CSV inner equi-join bridge, outer/semi/anti/cross joins, multi-key and expression joins, timestamp/timezone completeness, NULL/subquery-backed IN, multi-key/grouped aggregate generality, named grouped aggregate aliases, generalized ordering/null/collation support, arbitrary predicate-tree completeness beyond admitted parenthesized leaves, functions beyond admitted scalar helpers, subqueries, catalogs, object stores, table/lakehouse sources, broader output sinks, and production SQL/DataFrame support require later runtime slices.

## Internal Flow

`local_csv -> direct_compatibility_transient -> batch -> inline_jsonl_result, result_rows, first_result_row, optional_local_jsonl_output, optional_local_csv_output, scalar_aggregate_result, grouped_aggregate_result, topn_result, join_result, sql_local_source_evidence, evidence_summary, claim_summary -> evidence -> claim gate`

## Evidence You Should See

- `schema_version=shardloom.sql_local_source_smoke.v1`
- `sql_parser_executed=true`
- `sql_binder_executed=true`
- `sql_planner_executed=true`
- `source_io_performed=true`
- `source_format=csv`
- `filter_runtime_execution`
- `predicate_operator_family`
- `date_extract_runtime_execution`
- `date_extract_operator`
- `date_extract_source_column`
- `date_arithmetic_runtime_execution`
- `date_arithmetic_operator`
- `date_arithmetic_days`
- `date_arithmetic_source_column`
- `in_predicate_runtime_execution`
- `in_list_value_count`
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
- `join_runtime_execution`
- `join_type`
- `join_left_key`
- `join_right_key`
- `join_matched_row_count`
- `join_left_rows_scanned`
- `join_right_rows_scanned`
- `join_rows_output`
- `join_memory_estimate_bytes`
- `output_format`
- `output_io_performed`
- `output_native_io_certificate_status`
- `output_certificate_ref`
- `materialization_boundary`
- `evidence_summary`
- `claim_summary`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status=fixture_smoke_only`

## Expected Output Or Evidence

A JSON envelope and typed Python report with inline JSONL result, result_rows/first_result_row helpers, optional local JSONL or CSV output path/format/digest/certificate fields, parser/binder/planner/runtime flags, local CSV source evidence, date_extract_runtime_execution/operator/source_column and date_arithmetic_runtime_execution/operator/days/source_column when requested, in_predicate_runtime_execution and in_list_value_count when requested, scalar/grouped/top-N/join fields when requested, left/right source refs for join rows, materialization/decode evidence, compact evidence_summary/claim_summary helpers, fallback_attempted=false, external_engine_invoked=false, and claim_gate_status=fixture_smoke_only.

## Common Mistakes

- `treating_smoke_as_sql_compatibility`
- `expecting_parquet_or_s3_sql_sources`
- `expecting_broad_python_dataframe_join_support`
- `expecting_general_join_or_grouped_aggregate_support`
- `expecting_general_order_by_or_null_ordering_support`
- `expecting_subquery_or_null_in_semantics`

## Reference Files

- `README.md` - What this proves: Public technical-preview posture, Vortex-first/no-fallback positioning, and primary repo entrypoints.
- `docs/getting-started/examples.md` - What this proves: Current example catalog and local workflow entrypoints.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/phased-execution-plan.md` - What this proves: Active planned work, claim boundaries, non-goals, and ledger move rules.

## Related Use Cases

- `python-local-csv-query-builder-smoke`
- `sql-dataframe-capability-posture`
- `source-free-generated-output-boundary`
- `local-file-etl-cleanup-smoke`

## Related Field Guide Terms

- `website/field-guide/direct-compatibility-transient.html` - Direct Compatibility Transient (`Execution Modes` / `scoped-local-smoke`)
