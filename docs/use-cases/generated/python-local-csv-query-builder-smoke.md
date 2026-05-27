<!-- SPDX-License-Identifier: Apache-2.0 -->

# Python local CSV/JSON/JSONL/Parquet query-builder projection, preview/head/take, literal-column, count, aggregate, group-by, join, join-computed-top-N, join-aggregate, and top-N smoke

## Quick Answer

- **Audience:** Python user who wants a tiny DataFrame-like local CSV, flat JSON, flat JSONL, or feature-gated flat scalar Parquet workflow with evidence
- **Status:** `smoke_supported`
- **Execution mode:** `direct_compatibility_transient`
- **Engine mode:** `batch`
- **Claim boundary:** Scoped Python read_csv/read_json/read_parquet/read_arrow_ipc/read_avro/read_orc local-source smokes cover projection, optional-filter, limit, preview/head/take, literal and admitted string-function with_column(...), count, scalar aggregate with aliases, multi-key group_by aggregate with aliases, single-key top-N, scoped local-source inner/outer/semi/anti equi-joins and cross joins, computed projections and single-key numeric top-N over joined rows, and scalar/grouped join aggregates over admitted local CSV, flat JSON/JSONL/NDJSON, and feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC. Filters support scoped comparison, between, cast, date literals, Date32 extract/day arithmetic, bounded literal/source-backed IN, null/string/logical predicates, UTF-8 lower/upper/trim transforms, CONCAT/SUBSTR/REPLACE, and balanced parentheses. Local JSONL, scoped CSV, and feature-gated flat scalar structured writes are admitted. No nested JSON, JSONPath, broader structured type/nesting/output coverage, pandas/Polars backend, broad DataFrame runtime, generalized joins/groups/orderings, timestamp/timezone completeness, locale/collation completeness, broad ANSI subquery parity, production SQL, object-store/table source, external fallback, or performance claim.

## Can ShardLoom Do This?

Python local CSV/JSON/JSONL/Parquet query-builder projection, preview/head/take, literal-column, count, aggregate, group-by, join, join-computed-top-N, join-aggregate, and top-N smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Scoped Python read_csv/read_json/read_parquet/read_arrow_ipc/read_avro/read_orc local-source smokes cover projection, optional-filter, limit, preview/head/take, literal and admitted string-function with_column(...), count, scalar aggregate with aliases, multi-key group_by aggregate with aliases, single-key top-N, scoped local-source inner/outer/semi/anti equi-joins and cross joins, computed projections and single-key numeric top-N over joined rows, and scalar/grouped join aggregates over admitted local CSV, flat JSON/JSONL/NDJSON, and feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC. Filters support scoped comparison, between, cast, date literals, Date32 extract/day arithmetic, bounded literal/source-backed IN, null/string/logical predicates, UTF-8 lower/upper/trim transforms, CONCAT/SUBSTR/REPLACE, and balanced parentheses. Local JSONL, scoped CSV, and feature-gated flat scalar structured writes are admitted. No nested JSON, JSONPath, broader structured type/nesting/output coverage, pandas/Polars backend, broad DataFrame runtime, generalized joins/groups/orderings, timestamp/timezone completeness, locale/collation completeness, broad ANSI subquery parity, production SQL, object-store/table source, external fallback, or performance claim.

## How To Try It

```powershell
New-Item -ItemType Directory -Force target | Out-Null; "id,label,amount`n1,alpha,8`n2,beta,15`n3,beta,21`n4,gamma,`n" | Set-Content -Encoding utf8 target\sql-local-source-smoke.csv; $env:PYTHONPATH = "python\src"; python -c "import shardloom as sl; ctx=sl.context(repo_root='.', profile_order=('debug','release')); preview=ctx.read_csv('target/sql-local-source-smoke.csv').preview(limit=2); head=ctx.read_csv('target/sql-local-source-smoke.csv').head(limit=2); take=ctx.read_csv('target/sql-local-source-smoke.csv').take(2); workflow=ctx.read_csv('target/sql-local-source-smoke.csv').select('id','label').where(sl.col('amount').between(10, 30)).limit(1); r=workflow.write('target/sql-local-source-result.jsonl', allow_overwrite=True); c=workflow.write_csv('target/sql-local-source-result.csv', allow_overwrite=True); w=ctx.read_csv('target/sql-local-source-smoke.csv').select('id','label').with_column('batch_id', 1).filter(sl.col('amount') >= 10).limit(10).collect(); n=ctx.read_csv('target/sql-local-source-smoke.csv').filter(sl.col('amount') >= 10).count(); a=ctx.read_csv('target/sql-local-source-smoke.csv').aggregate('count(*)','sum(amount)','avg(amount)').limit(1).collect(); g=ctx.read_csv('target/sql-local-source-smoke.csv').group_by('label').agg('count(*)','sum(amount)').limit(10).collect(); t=ctx.read_csv('target/sql-local-source-smoke.csv').select('id','label').sort('amount', descending=True).limit(2).collect(); print(preview.output_row_count, head.output_row_count, take.output_row_count, r.output_path, r.output_native_io_certificate_status, c.output_path, c.output_format, c.output_native_io_certificate_status, w.envelope.field('literal_projection_columns'), n.aggregate_functions, a.aggregate_operator_family, g.aggregate_operator_family, g.group_by_columns, t.sort_keys, t.top_n_limit, r.fallback_attempted, r.external_engine_invoked)"
```

## Blocker

The Python query-builder admits local CSV, flat JSON/JSONL/NDJSON, and feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC through the SQL local-source smoke for projection/filter/limit, preview/head/take, admitted computed-column helpers, aggregate aliases, group-by, top-N, scoped local-source joins, joined computed projection/top-N, join aggregates, and local output/fanout. Default binaries block Parquet, Arrow IPC, Avro, and ORC until built with --features universal-format-io. Nested JSON, JSONPath, broader structured type/nesting coverage, generalized with_column expressions, timestamp/timezone completeness, row-value/correlated/joined/nested subqueries and arbitrary predicate-tree completeness, broader grouped aggregate generality, generalized ordering/null/collation support, windows, schema/data-quality helpers, object stores, tables, pandas/Polars execution, and production DataFrame parity require later runtime slices.

## Internal Flow

`local_csv, local_json, local_jsonl, local_ndjson, local_parquet_feature_gated -> direct_compatibility_transient -> batch -> inline_jsonl_result, result_rows, first_result_row, local_jsonl_output, local_csv_output, feature_gated_local_parquet_output, literal_projection_result, string_function_projection_result, row_count_result, scalar_aggregate_result, grouped_aggregate_result, topn_result, join_result, join_aggregate_result, typed_python_report, evidence_summary, claim_summary, sql_local_source_evidence -> evidence -> claim gate`

## Evidence You Should See

- `schema_version=shardloom.sql_local_source_smoke.v1`
- `sql_parser_executed=true`
- `sql_binder_executed=true`
- `sql_planner_executed=true`
- `source_format`
- `source_io_performed=true`
- `source_state_id`
- `source_state_digest`
- `filter_runtime_execution`
- `predicate_operator_family`
- `null_predicate_runtime_execution`
- `null_predicate_operator`
- `null_predicate_source_column`
- `null_predicate_null_semantics`
- `string_transform_runtime_execution`
- `string_transform_operator`
- `string_transform_source_column`
- `string_function_runtime_execution`
- `string_function_operator`
- `string_function_source_column`
- `string_function_literal_count`
- `string_function_rhs_dtype`
- `string_function_projection_runtime_execution`
- `string_function_projection_operator`
- `string_function_projection_source_column`
- `string_function_projection_output_column`
- `string_function_projection_literal_count`
- `date_extract_runtime_execution`
- `date_extract_operator`
- `date_extract_source_column`
- `date_arithmetic_runtime_execution`
- `date_arithmetic_operator`
- `date_arithmetic_days`
- `date_arithmetic_source_column`
- `timestamp_arithmetic_runtime_execution`
- `timestamp_arithmetic_operator`
- `timestamp_arithmetic_seconds`
- `timestamp_arithmetic_source_column`
- `timestamp_arithmetic_projection_runtime_execution`
- `timestamp_arithmetic_projection_operator`
- `timestamp_arithmetic_projection_seconds`
- `timestamp_arithmetic_projection_source_column`
- `timestamp_arithmetic_projection_output_column`
- `literal_projection_runtime_execution`
- `literal_projection_columns`
- `literal_projection_count`
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
- `join_left_keys`
- `join_right_keys`
- `join_key_arity`
- `join_multi_key_runtime_execution`
- `join_computed_projection_runtime_execution`
- `join_order_by_top_n_runtime_execution`
- `join_projection_operator_family`
- `join_aggregate_runtime_execution`
- `join_aggregate_operator_family`
- `join_aggregate_group_count`
- `output_format`
- `output_io_performed=true`
- `output_native_io_certificate_status`
- `output_certificate_ref`
- `evidence_summary`
- `claim_summary`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status=fixture_smoke_only`

## Expected Output Or Evidence

A typed Python report over the SQL local-source JSON envelope with result_rows/first_result_row helpers, local CSV, flat JSON/JSONL, or feature-gated flat scalar Parquet source evidence, source_format/source_adapter/source_state/route fields, source/execution certificate refs, materialization boundary and claim-gate reason fields, string transform, timestamp arithmetic, and scoped string-function fields when requested, date extract/arithmetic and timestamp arithmetic fields when requested, literal-projection fields when requested, bounded IN evidence when requested, local JSONL/CSV or feature-gated flat scalar Parquet output evidence, count/scalar/grouped/top-N/join/join-computed-top-N/join-aggregate fields, fallback_attempted=false, external_engine_invoked=false, and claim_gate_status=fixture_smoke_only.

## Common Mistakes

- `expecting_dataframe_parity`
- `expecting_pandas_or_polars_execution`
- `expecting_nested_json_or_jsonpath_runtime`
- `expecting_parquet_default_build_support`
- `treating_fixture_smoke_as_production_support`
- `expecting_general_sort_or_null_ordering_support`
- `expecting_broad_subquery_parity`

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

- `website/field-guide/direct-compatibility-transient.html` - direct_compatibility_transient (`Execution Routes` / `smoke_supported`)
- `website/field-guide/source-adapter-status.html` - Source adapter status (`UniversalIngress` / `smoke_supported`)
