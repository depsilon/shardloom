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

Current required evidence:

```text
admitted_semantics_validator_status=passed
matrix_status=passed
matrix_row_count=44
executable_fixture_count=26
unsupported_diagnostic_count=18
property_lane_count=1
property_seed_order=20260521
property_execution_performed=true
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
- `try_cast_projection_null_on_invalid`
- `string_transform_length_utf8`
- `temporal_extract_utc_date32_timestamp`
- `null_coalesce_nullif`
- `predicate_projection_three_valued`
- `aggregate_having_output_rows`
- `string_function_composition_utf8`
- `temporal_arithmetic_difference_utc`
- `conditional_projection_case_when`
- `in_predicate_literal_null_semantics`
- `row_value_in_predicate_semantics`
- `row_value_in_subquery_semantics`
- `exists_subquery_semantics`
- `sql_union_composition_semantics`
- `in_subquery_scalar_semantics`
- `in_subquery_filtered_ordered_limited_semantics`
- `having_in_subquery_semantics`
- `distinct_count_grouped`
- `select_distinct_projection`
- `select_distinct_aggregate_having`
- `having_hidden_aggregate_expression`
- `window_rank_offset_distribution`
- `select_distinct_window`
- `join_multi_key_expression_condition`
- `select_distinct_join`
- `unsupported_numeric_division_by_zero`
- `unsupported_cast_decimal128`
- `unsupported_non_utc_timestamp_literal`
- `unsupported_timezone_database_policy`
- `unsupported_interval_literal`
- `unsupported_regex_predicate`
- `unsupported_locale_collation`
- `unsupported_list_literal`
- `unsupported_struct_literal`
- `unsupported_variant_access`
- `unsupported_union_dtype_cast`
- `unsupported_binary_literal_source`
- `unsupported_scalar_multi_column_in_subquery`
- `unsupported_nested_in_subquery`
- `unsupported_joined_in_subquery`
- `unsupported_grouped_having_in_subquery`
- `unsupported_correlated_in_subquery`
- `unsupported_any_all_subquery`

Current remaining gaps are broad ANSI subquery parity beyond bounded local scalar IN-subquery,
row-value IN-subquery, and scoped EXISTS fixtures, external-oracle result artifact population, and
fuzz execution beyond the deterministic seeded property lane. Decimal precision/scale, non-UTC
timestamp/timezone database semantics, interval arithmetic, regex, locale/collation, complex dtype
families, and remaining unsupported advanced subquery shapes now have deterministic unsupported
diagnostics with no fallback.

Claim boundary: admitted SQL local-source expression/operator correctness evidence only. This does
not authorize ANSI SQL parity, production semantic parity, broad SQL/DataFrame support, performance
claims, package publication, fallback execution, or external-engine runtime delegation.
