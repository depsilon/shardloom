# Admitted Semantics Matrix Validator

Run:

```powershell
python scripts\check_admitted_semantics_matrix.py
```

The validator writes:

```text
target/admitted-semantics-matrix-report.json
target/admitted-semantics-matrix
```

Schema:

```text
shardloom.admitted_semantics_matrix_report.v1
```

It consumes:

```text
docs/status/admitted-semantics-matrix.json
shardloom.admitted_semantics_fixture_matrix.v1
```

The 144-row matrix runs its 117 executable fixtures and 25 diagnostic cases
through the public `run sql` workflow, alongside two capability reports. SQL,
source fixtures, property seeds and row ordering retain their original independent
expectations. Complete native results must include their declared Vortex schema;
missing, duplicate, unsafe or truncated result evidence fails validation. Writer
cases check the full file and committed native sink evidence. No decoded runtime
or benchmark-specific execution command is used.

The expected transport follows the native schema: binary values use lowercase
hex, Date32 uses epoch days, timestamps use UTC epoch microseconds, and decimals
use `decimal128(precision,scale):coefficient`. Primitive SUM/AVG use the existing
F64 reduction contract. These representation changes preserve the original
reference values; the validator does not normalize observed rows to make them
match. Parser denials use `SL_UNSUPPORTED_SQL`; runtime/type/sink errors retain
`SL_INVALID_INPUT`. Failed writers must preserve preexisting files.

The matrix is a bounded regression suite, not a complete capability inventory.
Native static nested source keys, selected expressions and retained state have a
[contract](../architecture/native-nested-keys-state-2026-10-04.md) and
[local acceptance](../benchmarks/native-nested-keys-state-full43-2026-10-04.md).
The [current implementation contract](../architecture/native-typed-reductions-2026-10-04.md)
also covers constructor, numeric comparison and correlated HAVING behavior.
Full native replay is required before accepting this migration; these fixtures
do not establish broad SQL-standard parity.

Current required evidence:

```text
admitted_semantics_validator_status=passed
matrix_status=passed
matrix_row_count=144
executable_fixture_count=117
diagnostic_case_count=25
unsupported_diagnostic_count=23
runtime_error_diagnostic_count=1
invalid_shape_diagnostic_count=1
property_lane_count=10
property_seed_order=20260521,20260618,20260619,20260620,20260621,20260622,20260623,20260624,20260625,20260626
property_execution_performed=true
deterministic_fuzz_execution_performed=true
deterministic_fuzz_case_count=5
decoded_reference_differential_execution_performed=true
semantic_conformance_suite_status=passed
correctness_harness_boundary_status=passed
fallback_attempted=false
external_engine_invoked=false
production_claim_allowed=false
ansi_sql_claim_allowed=false
performance_claim_allowed=false
```

Covered fixture rows:

- `numeric_generic_property_seed_20260521`
- `filter_project_limit_property_seed_20260618`
- `join_property_seed_20260619`
- `aggregate_topn_property_seed_20260620`
- `in_subquery_property_seed_20260621`
- `string_function_property_seed_20260622`
- `temporal_property_seed_20260623`
- `decimal_property_seed_20260624`
- `binary_property_seed_20260625`
- `output_jsonl_property_seed_20260626`
- `try_cast_projection_null_on_invalid`
- `string_transform_length_utf8`
- `regex_predicate_utf8`
- `like_predicate_utf8`
- `like_escape_predicate_utf8`
- `temporal_extract_utc_date32_timestamp`
- `null_coalesce_nullif`
- `predicate_projection_three_valued`
- `null_safe_comparison_predicate_semantics`
- `order_by_explicit_null_ordering`
- `subquery_predicate_projection_semantics`
- `aggregate_having_output_rows`
- `string_function_composition_utf8`
- `temporal_arithmetic_difference_utc`
- `interval_literal_temporal_arithmetic`
- `conditional_projection_case_when`
- `binary_hex_literal_projection`
- `binary_text_literal_projection`
- `complex_array_literal_projection`
- `complex_struct_source_projection`
- `complex_csv_output_projection`
- `nested_arrow_ipc_source_projection`
- `typed_nested_compatibility_sink_preservation`
- `complex_distinct_projection_equality`
- `complex_order_by_projection`
- `sql_union_complex_distinct_equality`
- `sql_union_complex_ordering`
- `binary_cast_projection_predicate`
- `binary_cast_ordering_predicate`
- `decimal_cast_projection_predicate`
- `decimal_arithmetic_projection`
- `binary_helper_projection`
- `binary_helper_predicate`
- `in_predicate_literal_null_semantics`
- `row_value_in_predicate_semantics`
- `row_value_in_subquery_semantics`
- `not_in_subquery_semantics`
- `row_value_not_in_subquery_semantics`
- `exists_subquery_semantics`
- `quantified_subquery_semantics`
- `sql_union_composition_semantics`
- `sql_intersect_composition_semantics`
- `sql_except_composition_semantics`
- `in_subquery_scalar_semantics`
- `in_subquery_filtered_ordered_limited_semantics`
- `correlated_in_subquery_semantics`
- `correlated_row_value_in_subquery_semantics`
- `correlated_exists_subquery_semantics`
- `correlated_not_exists_subquery_semantics`
- `correlated_quantified_subquery_semantics`
- `joined_projected_in_subquery_semantics`
- `joined_projected_not_in_subquery_semantics`
- `joined_projected_row_value_in_subquery_semantics`
- `joined_projected_row_value_not_in_subquery_semantics`
- `grouped_having_projected_in_subquery_semantics`
- `grouped_having_projected_not_in_subquery_semantics`
- `grouped_having_projected_row_value_not_in_subquery_semantics`
- `joined_projected_exists_subquery_semantics`
- `joined_projected_not_exists_subquery_semantics`
- `grouped_having_projected_exists_subquery_semantics`
- `grouped_having_projected_not_exists_subquery_semantics`
- `joined_projected_quantified_subquery_semantics`
- `correlated_joined_projected_in_subquery_semantics`
- `correlated_joined_projected_not_in_subquery_semantics`
- `correlated_joined_projected_row_value_in_subquery_semantics`
- `correlated_joined_projected_row_value_not_in_subquery_semantics`
- `correlated_joined_projected_quantified_subquery_semantics`
- `correlated_joined_projected_exists_subquery_semantics`
- `correlated_joined_projected_not_exists_subquery_semantics`
- `correlated_grouped_having_projected_in_subquery_semantics`
- `correlated_grouped_having_projected_not_in_subquery_semantics`
- `correlated_grouped_having_projected_row_value_in_subquery_semantics`
- `correlated_grouped_having_projected_row_value_not_in_subquery_semantics`
- `correlated_grouped_having_projected_quantified_subquery_semantics`
- `correlated_grouped_having_projected_exists_subquery_semantics`
- `correlated_grouped_having_projected_not_exists_subquery_semantics`
- `nested_in_subquery_semantics`
- `having_in_subquery_semantics`
- `having_not_in_subquery_semantics`
- `having_row_value_in_subquery_semantics`
- `having_row_value_not_in_subquery_semantics`
- `having_exists_subquery_semantics`
- `having_not_exists_subquery_semantics`
- `having_quantified_subquery_semantics`
- `having_correlated_quantified_subquery_semantics`
- `distinct_count_grouped`
- `select_distinct_projection`
- `select_distinct_aggregate_having`
- `having_hidden_aggregate_expression`
- `window_rank_offset_distribution`
- `select_distinct_window`
- `join_multi_key_expression_condition`
- `join_scalar_expression_condition`
- `join_logical_or_condition`
- `select_distinct_join`
- `sql_parser_surface_fuzz_seed_20260613`
- `expression_parser_fuzz_seed_20260614`
- `route_selection_join_fuzz_seed_20260615`
- `route_selection_aggregate_topn_fuzz_seed_20260616`
- `output_writer_policy_fuzz_seed_20260617`
- `runtime_error_numeric_division_by_zero`
- `unsupported_output_no_overwrite_policy`
- `timestamp_offset_literal_normalization`
- `unsupported_nonbinary_source_binary_literal_predicate`
- `unsupported_nonbinary_source_binary_ordering_predicate`
- `unsupported_timezone_database_policy`
- `unsupported_timezone_database_function_policy`
- `unsupported_timestamptz_policy`
- `unsupported_locale_collation`
- `unsupported_locale_case_insensitive_predicate`
- `unsupported_list_array_access_cast`
- `unsupported_struct_access_cast`
- `unsupported_complex_subquery_membership`
- `unsupported_orc_nested_output_preservation`
- `unsupported_orc_typed_decimal_sink_preservation`
- `unsupported_variant_access`
- `unsupported_union_dtype_cast`
- `unsupported_arbitrary_interval_arithmetic`
- `unsupported_complex_join_key`
- `invalid_shape_scalar_multi_column_in_subquery`
- `unsupported_unbound_source_qualified_in_subquery_select`
- `unsupported_unbound_source_qualified_row_value_subquery_filter`
- `unsupported_unbound_source_qualified_exists_projection`
- `unsupported_unbound_source_qualified_quantified_order_by`
- `unsupported_outer_reference_non_column_comparison`
- `unsupported_outer_to_outer_subquery_comparison`
- `source_qualified_in_subquery_semantics`
- `source_qualified_not_in_subquery_semantics`
- `source_qualified_row_value_in_subquery_semantics`
- `source_qualified_row_value_not_in_subquery_semantics`
- `source_qualified_exists_subquery_semantics`
- `source_qualified_not_exists_subquery_semantics`
- `source_qualified_quantified_subquery_semantics`

These fixtures validate the shared native execution path against retained literal
values and seeded reference calculations. They cover bounded scalar and row-value
IN/NOT IN, EXISTS/NOT EXISTS, ANY/ALL, joined and grouped subqueries, correlated
`outer.<column>` parameters, predicate/CASE projections and HAVING. They also
exercise expression joins, logical OR, distinctness, ordering and windows. The
case list is the exact scope; broader SQL-standard parity, external-oracle result
artifacts and general fuzzing require separate evidence.

All source adapters normalize admitted data to Vortex before the shared binder
and operators execute. Result dtypes come from bound native schemas, including
empty and all-NULL results. No decoded row evaluator, result-type inference from
non-NULL rows or benchmark-scenario dispatch participates in this matrix.

ARRAY and STRUCT constructors produce native nested columns. Native structural
keys own admitted distinctness and ordering. Nested JSON/JSONL traverses those
columns directly; CSV translates each nested value to a quoted JSON cell, with
NULL parents distinct from empty lists. Vortex preserves the native logical
schema. Parquet, Arrow IPC and Avro use explicit compatibility writers with their
format-specific type/name checks. The pinned ORC writer rejects nested, decimal
and temporal output before publishing a file. The matrix's constructor syntax
denials do not describe the broader static nested source-key capability; that
capability has its own linked contract and acceptance suite.

Decimal casts, arithmetic and comparisons retain exact coefficients and declared
precision/scale. Division must be exact at
`decimal128(38,max(input_scales,6))`; inexact results fail explicitly. The same
native columns supply typed sinks and the tagged decimal JSON/CSV representation
described above. Binary fixtures cover literals, casts, `UNHEX`, `FROM_BASE64`,
NULLs, bytewise comparisons and binary source columns, with lowercase hex at the
JSON boundary. Calendar fixtures cover strict date/timestamp text casts, fixed
numeric offsets normalized to UTC, extraction, differences and admitted interval
helpers.

The diagnostic rows deliberately exercise named timezone databases, TIMESTAMPTZ,
locale collation/ILIKE, unimplemented accessor/cast/variant/union syntax, invalid
subquery shapes, disallowed outer references, mixed binary/text comparisons and
unsupported ORC types. They require the exact declared code and message fragment.
Numeric division by zero is a runtime data error; a scalar left operand paired
with a multi-column IN subquery is an invalid shape. Output-policy failures must
preserve existing files. Unsupported work never invokes another engine.

Claim boundary: admitted SQL local-source expression/operator correctness evidence only. This does
not authorize broad SQL-standard/ANSI-style compliance, production semantic parity, broad
SQL/DataFrame support, performance
claims, package publication, fallback execution, or external-engine runtime delegation.
