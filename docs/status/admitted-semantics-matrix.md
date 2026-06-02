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
matrix_row_count=67
executable_fixture_count=57
unsupported_diagnostic_count=10
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
- `regex_predicate_utf8`
- `like_predicate_utf8`
- `like_escape_predicate_utf8`
- `temporal_extract_utc_date32_timestamp`
- `null_coalesce_nullif`
- `predicate_projection_three_valued`
- `aggregate_having_output_rows`
- `string_function_composition_utf8`
- `temporal_arithmetic_difference_utc`
- `interval_literal_temporal_arithmetic`
- `conditional_projection_case_when`
- `binary_hex_literal_projection`
- `binary_text_literal_projection`
- `binary_cast_projection_predicate`
- `binary_helper_projection`
- `in_predicate_literal_null_semantics`
- `row_value_in_predicate_semantics`
- `row_value_in_subquery_semantics`
- `exists_subquery_semantics`
- `quantified_subquery_semantics`
- `sql_union_composition_semantics`
- `in_subquery_scalar_semantics`
- `in_subquery_filtered_ordered_limited_semantics`
- `correlated_in_subquery_semantics`
- `source_qualified_in_subquery_semantics`
- `correlated_row_value_in_subquery_semantics`
- `correlated_exists_subquery_semantics`
- `correlated_quantified_subquery_semantics`
- `joined_projected_in_subquery_semantics`
- `joined_projected_row_value_in_subquery_semantics`
- `grouped_having_projected_in_subquery_semantics`
- `joined_projected_exists_subquery_semantics`
- `grouped_having_projected_exists_subquery_semantics`
- `joined_projected_quantified_subquery_semantics`
- `correlated_joined_projected_in_subquery_semantics`
- `correlated_joined_projected_row_value_in_subquery_semantics`
- `correlated_joined_projected_quantified_subquery_semantics`
- `correlated_joined_projected_exists_subquery_semantics`
- `correlated_grouped_having_projected_in_subquery_semantics`
- `correlated_grouped_having_projected_row_value_in_subquery_semantics`
- `correlated_grouped_having_projected_quantified_subquery_semantics`
- `correlated_grouped_having_projected_exists_subquery_semantics`
- `nested_in_subquery_semantics`
- `having_in_subquery_semantics`
- `having_exists_subquery_semantics`
- `having_quantified_subquery_semantics`
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
- `unsupported_locale_collation`
- `unsupported_list_literal`
- `unsupported_struct_literal`
- `unsupported_variant_access`
- `unsupported_union_dtype_cast`
- `unsupported_scalar_multi_column_in_subquery`

Current remaining gaps are broad ANSI subquery parity beyond the admitted bounded local scalar
IN-subquery, nested scalar IN-subquery, row-value IN-subquery, source-qualified local subquery,
scoped correlated `outer.<column>` local subquery filter, joined/grouped projected IN/EXISTS
subqueries, projected row-value/quantified subquery, correlated joined and grouped/HAVING projected
scalar/row-value/quantified/EXISTS subqueries, scoped EXISTS, scoped quantified ANY/ALL, and
HAVING-level local subquery fixtures;
external-oracle result artifact population; and fuzz execution beyond the deterministic seeded
property lane. Decimal precision/scale, non-UTC timestamp/timezone database semantics,
locale/collation, complex list/struct/variant/union dtype families, broad binary source dtype
decoding, binary ordering, scalar-left multi-column subqueries, and remaining non-admitted broad
ANSI subquery shapes now have deterministic unsupported diagnostics with no fallback.
Scoped ANSI interval literals are
executable only inside `DATE_ADD_DAYS`/`DATE_SUB_DAYS` and
`TIMESTAMP_ADD_SECONDS`/`TIMESTAMP_SUB_SECONDS`; arbitrary ANSI interval arithmetic remains outside
the claim boundary. Scoped SQL `X'<hex>'` binary literal projections are executable with exact
hex evidence. Scoped `BINARY '<utf8>'` and `BLOB '<utf8>'` text byte literal projections are
executable with exact byte evidence. Scoped `CAST`/`TRY_CAST` to `binary`/`blob`/`varbinary`
projects admitted scalar values as UTF-8 bytes, and scoped binary cast equality/inequality
predicates admit `X'<hex>'`, `BINARY`/`BLOB` text literals, single-quoted UTF-8 byte literals, or
`NULL`. Scoped `UNHEX(<utf8-column>)` and `FROM_BASE64(<utf8-column>)` projections are executable
with strict UTF-8 text decoding, binary output evidence, null propagation, and deterministic
invalid-input blockers, while broad binary source dtype decoding, binary ordering, and nested binary
helper expressions remain outside the claim boundary. Scoped UTF-8 `LIKE` predicates with `%`, `_`,
and single-character
`ESCAPE` clauses are executable through
ShardLoom-owned string predicate lowering, and scoped UTF-8 regex predicates are executable through
`RLIKE`/`REGEXP`/`REGEXP_LIKE`; case-folding and locale-aware regex/collation semantics remain
outside the claim boundary.

Claim boundary: admitted SQL local-source expression/operator correctness evidence only. This does
not authorize ANSI SQL parity, production semantic parity, broad SQL/DataFrame support, performance
claims, package publication, fallback execution, or external-engine runtime delegation.
