<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL local source projection/optional-filter/IN/EXISTS/limit, aggregate, group-by, top-N, join, join-computed-top-N, and join-aggregate smoke

## Quick Answer

- **Audience:** user who wants to try one tiny SQL query over an admitted local file from CLI or ctx.sql without fallback
- **Status:** `smoke_supported`
- **Execution mode:** `direct_compatibility_transient`
- **Engine mode:** `batch`
- **Claim boundary:** Scoped local CSV, flat JSON/JSONL/NDJSON, and feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC SELECT projection/filter/limit, aggregates, group-by, top-N, ctx.sql collect/write, local sinks/fanout, joins, and join aggregates. Computed filters/projections admit listed cast, null, conditional, numeric, Date32, UTC timestamp, temporal-difference, scalar/row-value IN-subqueries including projected joined/grouped variants, scoped EXISTS/NOT EXISTS, scoped quantified ANY/ALL subqueries including projected joined variants, and UTF-8 helper families including LIKE/NOT LIKE with single-character ESCAPE clauses. Lossy coercion, arithmetic/type/date/time errors, NULL fallback/sentinel or CASE mismatches, scalar-left multi-column, correlated, broad non-admitted subquery shapes, and broad projection trees block. No broad SQL/DataFrame or production runtime, broad format fidelity, claim-grade fanout/replay, object-store/table source, generalized joins/groups/orderings, timezone/collation completeness, fallback, or performance claim.

## Can ShardLoom Do This?

SQL local source projection/optional-filter/IN/EXISTS/limit, aggregate, group-by, top-N, join, join-computed-top-N, and join-aggregate smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Scoped local CSV, flat JSON/JSONL/NDJSON, and feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC SELECT projection/filter/limit, aggregates, group-by, top-N, ctx.sql collect/write, local sinks/fanout, joins, and join aggregates. Computed filters/projections admit listed cast, null, conditional, numeric, Date32, UTC timestamp, temporal-difference, scalar/row-value IN-subqueries including projected joined/grouped variants, scoped EXISTS/NOT EXISTS, scoped quantified ANY/ALL subqueries including projected joined variants, and UTF-8 helper families including LIKE/NOT LIKE with single-character ESCAPE clauses. Lossy coercion, arithmetic/type/date/time errors, NULL fallback/sentinel or CASE mismatches, scalar-left multi-column, correlated, broad non-admitted subquery shapes, and broad projection trees block. No broad SQL/DataFrame or production runtime, broad format fidelity, claim-grade fanout/replay, object-store/table source, generalized joins/groups/orderings, timezone/collation completeness, fallback, or performance claim.

## How To Try It

```powershell
New-Item -ItemType Directory -Force target | Out-Null; "id,customer_id,region,amount`n1,10,east,8`n2,20,west,15`n3,20,east,21`n4,30,east,22`n5,30,west,23`n" | Set-Content -Encoding utf8 target\sql-local-source-join-fact.csv; "customer_id,region,segment`n20,west,enterprise`n20,east,consumer`n30,west,startup`n99,east,orphan`n" | Set-Content -Encoding utf8 target\sql-local-source-join-dim.csv; cargo run -q -p shardloom-cli -- sql-local-source-smoke "SELECT f.id,d.segment FROM 'target/sql-local-source-join-fact.csv' AS f INNER JOIN 'target/sql-local-source-join-dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 LIMIT 10" --format json; $env:PYTHONPATH = "python\src"; python -c "from shardloom import context; ctx=context(repo_root='.', profile_order=('debug','release')); r=ctx.read_csv('target/sql-local-source-join-fact.csv').join(ctx.read_csv('target/sql-local-source-join-dim.csv'), on=('customer_id','region')).select('f.id','d.segment').filter('f.amount >= 10').limit(10).collect(); print(r.output_row_count, r.join_runtime_execution, r.join_key_arity, r.join_multi_key_runtime_execution, r.fallback_attempted, r.external_engine_invoked)"
```

## Blocker

Vortex SQL sources, broader Parquet/Arrow IPC/Avro/ORC/Vortex type/nesting coverage, default-build Vortex writes without --features vortex-write, arbitrary join predicate trees beyond the admitted expression ON families, timezone completeness, scalar-left multi-column and correlated subqueries, broader non-admitted ANSI subquery shapes, broader grouped aggregate generality, generalized ordering/null/collation support, generalized expression projections beyond admitted literal, null coalesce/nullif, single-branch CASE, numeric arithmetic, numeric ABS, numeric rounding, Date32 day arithmetic, UTC timestamp second arithmetic, temporal difference, UTF-8 transform/length/CONCAT/SUBSTR/REPLACE, and temporal extract computed columns, arbitrary predicate-tree completeness beyond admitted parenthesized leaves, functions beyond admitted scalar helpers, broad subqueries, catalogs, object stores, table/lakehouse sources, claim-grade or broad output/fanout replay, and production SQL/DataFrame support require later runtime slices.

## Internal Flow

`local_csv, local_json, local_jsonl, local_ndjson, local_parquet_feature_gated, local_arrow_ipc_feature_gated, local_avro_feature_gated, local_orc_feature_gated -> direct_compatibility_transient -> batch -> inline_jsonl_result, result_rows, first_result_row, optional_local_jsonl_output, optional_local_csv_output, optional_feature_gated_local_parquet_output, optional_feature_gated_local_arrow_ipc_output, optional_feature_gated_local_avro_output, optional_feature_gated_local_orc_output, optional_feature_gated_local_vortex_output, cast_projection_result, null_coalesce_projection_result, nullif_projection_result, conditional_projection_result, numeric_arithmetic_projection_result, numeric_abs_projection_result, numeric_rounding_projection_result, date_arithmetic_projection_result, timestamp_arithmetic_projection_result, temporal_difference_projection_result, string_transform_projection_result, string_length_projection_result, string_function_projection_result, date_extract_projection_result, timestamp_extract_projection_result, scalar_aggregate_result, grouped_aggregate_result, topn_result, join_result, join_aggregate_result, sql_local_source_evidence, evidence_summary, claim_summary -> evidence -> claim gate`

## Evidence You Should See

- `schema_version=shardloom.sql_local_source_smoke.v1`
- `sql_parser_executed=true`
- `sql_binder_executed=true`
- `sql_planner_executed=true`
- `source_io_performed=true`
- `source_format`
- `source_adapter_id`
- `filter_runtime_execution`
- `predicate_operator_family`
- `boolean_predicate_runtime_execution`
- `boolean_predicate_operator`
- `boolean_predicate_source_column`
- `boolean_predicate_null_semantics`
- `null_predicate_runtime_execution`
- `null_predicate_operator`
- `null_predicate_source_column`
- `null_predicate_null_semantics`
- `string_predicate_runtime_execution`
- `string_predicate_operator`
- `string_predicate_like_escape_runtime_execution`
- `string_predicate_like_escape_character`
- `string_transform_runtime_execution`
- `string_transform_operator`
- `string_transform_source_column`
- `string_length_runtime_execution`
- `string_length_source_column`
- `string_length_rhs_dtype`
- `string_function_runtime_execution`
- `string_function_operator`
- `string_function_source_column`
- `string_function_literal_count`
- `string_function_rhs_dtype`
- `cast_projection_runtime_execution`
- `cast_projection_source_column`
- `cast_projection_output_column`
- `cast_projection_target_dtype`
- `null_coalesce_projection_runtime_execution`
- `null_coalesce_projection_source_column`
- `null_coalesce_projection_output_column`
- `null_coalesce_projection_fallback_dtype`
- `nullif_projection_runtime_execution`
- `nullif_projection_source_column`
- `nullif_projection_output_column`
- `nullif_projection_sentinel_dtype`
- `conditional_projection_runtime_execution`
- `conditional_projection_predicate_family`
- `conditional_projection_source_column`
- `conditional_projection_output_column`
- `conditional_projection_then_dtype`
- `conditional_projection_else_dtype`
- `string_transform_projection_runtime_execution`
- `string_transform_projection_operator`
- `string_transform_projection_source_column`
- `string_transform_projection_output_column`
- `string_length_projection_runtime_execution`
- `string_length_projection_source_column`
- `string_length_projection_output_column`
- `string_function_projection_runtime_execution`
- `string_function_projection_operator`
- `string_function_projection_source_column`
- `string_function_projection_output_column`
- `string_function_projection_literal_count`
- `date_extract_projection_runtime_execution`
- `date_extract_projection_operator`
- `date_extract_projection_source_column`
- `date_extract_projection_output_column`
- `timestamp_extract_projection_runtime_execution`
- `timestamp_extract_projection_operator`
- `timestamp_extract_projection_source_column`
- `timestamp_extract_projection_output_column`
- `generic_expression_predicate_runtime_execution`
- `generic_expression_predicate_source_column`
- `generic_expression_predicate_operator_family`
- `generic_expression_predicate_binary_operator_count`
- `generic_expression_predicate_comparison_operator`
- `generic_expression_projection_runtime_execution`
- `generic_expression_projection_source_column`
- `generic_expression_projection_output_column`
- `generic_expression_projection_operator_family`
- `generic_expression_projection_binary_operator_count`
- `numeric_arithmetic_runtime_execution`
- `numeric_arithmetic_operator`
- `numeric_arithmetic_source_column`
- `numeric_arithmetic_rhs_dtype`
- `numeric_abs_runtime_execution`
- `numeric_abs_source_column`
- `numeric_abs_rhs_dtype`
- `numeric_rounding_runtime_execution`
- `numeric_rounding_operator`
- `numeric_rounding_source_column`
- `numeric_rounding_rhs_dtype`
- `numeric_arithmetic_projection_runtime_execution`
- `numeric_arithmetic_projection_operator`
- `numeric_arithmetic_projection_source_column`
- `numeric_arithmetic_projection_output_column`
- `numeric_arithmetic_projection_rhs_dtype`
- `numeric_abs_projection_runtime_execution`
- `numeric_abs_projection_source_column`
- `numeric_abs_projection_output_column`
- `numeric_rounding_projection_runtime_execution`
- `numeric_rounding_projection_operator`
- `numeric_rounding_projection_source_column`
- `numeric_rounding_projection_output_column`
- `date_arithmetic_projection_runtime_execution`
- `date_arithmetic_projection_operator`
- `date_arithmetic_projection_days`
- `date_arithmetic_projection_source_column`
- `date_arithmetic_projection_output_column`
- `timestamp_arithmetic_projection_runtime_execution`
- `timestamp_arithmetic_projection_operator`
- `timestamp_arithmetic_projection_seconds`
- `timestamp_arithmetic_projection_source_column`
- `timestamp_arithmetic_projection_output_column`
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
- `in_predicate_runtime_execution`
- `in_list_value_count`
- `in_list_null_value_count`
- `in_predicate_null_semantics`
- `in_subquery_runtime_execution`
- `in_subquery_filter_runtime_execution`
- `in_subquery_order_by_runtime_execution`
- `in_subquery_limit_runtime_execution`
- `in_subquery_input_row_count`
- `in_subquery_filtered_row_count`
- `in_subquery_materialization_bound`
- `in_subquery_materialized_value_count`
- `in_subquery_materialized_null_value_count`
- `having_in_subquery_runtime_execution`
- `exists_subquery_runtime_execution`
- `exists_subquery_projection_kind`
- `exists_subquery_source_column`
- `exists_subquery_source_format`
- `exists_subquery_filter_runtime_execution`
- `exists_subquery_order_by_runtime_execution`
- `exists_subquery_limit_runtime_execution`
- `exists_subquery_input_row_count`
- `exists_subquery_filtered_row_count`
- `exists_subquery_bounded_row_count`
- `exists_subquery_scan_bound`
- `exists_subquery_result`
- `exists_subquery_null_semantics`
- `having_exists_subquery_runtime_execution`
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
- `right_source_format`
- `join_source_formats`
- `join_matched_row_count`
- `join_candidate_row_count`
- `join_unmatched_left_row_count`
- `join_unmatched_right_row_count`
- `join_left_rows_scanned`
- `join_right_rows_scanned`
- `join_rows_output`
- `join_memory_estimate_bytes`
- `output_format`
- `output_io_performed`
- `output_native_io_certificate_status`
- `output_certificate_ref`
- `result_replay_verified`
- `output_replay_status`
- `output_replay_millis`
- `output_fidelity_report_status`
- `output_fidelity_loss`
- `vortex_output_runtime_execution`
- `vortex_output_reopen_verified`
- `vortex_artifact_digest`
- `upstream_vortex_write_called`
- `upstream_vortex_scan_called`
- `materialization_boundary`
- `evidence_summary`
- `claim_summary`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status=fixture_smoke_only`

## Expected Output Or Evidence

A JSON envelope and typed Python report with inline JSONL result helpers; optional local JSONL/CSV, feature-gated flat scalar structured, or feature-gated local .vortex output evidence; parser/binder/planner/runtime flags; local source/predicate/projection/aggregate/group/top-N/join/join-computed-top-N/join-aggregate evidence where requested, including generic-expression, temporal-difference, timestamp-arithmetic, string-predicate/LIKE ESCAPE, and string-function predicate/projection evidence; materialization/decode evidence; result_replay_verified, output_replay_status, output_fidelity_report_status, and output_fidelity_loss for written local outputs; Vortex output rows add vortex_output_runtime_execution=true, vortex_output_reopen_verified=true, vortex_artifact_digest, upstream_vortex_write_called=true, upstream_vortex_scan_called=true; fallback_attempted=false, external_engine_invoked=false, and claim_gate_status=fixture_smoke_only.

## Common Mistakes

- `treating_smoke_as_sql_compatibility`
- `expecting_parquet_or_s3_sql_sources`
- `expecting_broad_python_dataframe_join_support`
- `expecting_general_join_or_grouped_aggregate_support`
- `expecting_general_order_by_or_null_ordering_support`
- `expecting_broad_subquery_parity`

## Reference Files

- `README.md` - What this proves: Public technical-preview posture, Vortex-first positioning, and no-fallback boundaries.
- `docs/getting-started/examples.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/phased-execution-completed-ledger.md` - What this proves: Completed runtime provenance and historical phase evidence for this use case.

## Related Use Cases

- `python-local-csv-query-builder-smoke`
- `sql-dataframe-capability-posture`
- `source-free-generated-output-boundary`
- `local-file-etl-cleanup-smoke`

## Related Field Guide Terms

- `website/field-guide/direct-compatibility-transient.html` - direct_compatibility_transient (`Execution Routes` / `smoke_supported`)
