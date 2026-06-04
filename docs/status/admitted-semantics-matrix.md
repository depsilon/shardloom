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
matrix_row_count=73
executable_fixture_count=64
diagnostic_case_count=9
unsupported_diagnostic_count=7
runtime_error_diagnostic_count=1
invalid_shape_diagnostic_count=1
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
- `binary_cast_projection_predicate`
- `binary_cast_ordering_predicate`
- `decimal_cast_projection_predicate`
- `decimal_arithmetic_projection`
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
- `runtime_error_numeric_division_by_zero`
- `timestamp_offset_literal_normalization`
- `unsupported_timezone_database_policy`
- `unsupported_timezone_database_function_policy`
- `unsupported_timestamptz_policy`
- `unsupported_locale_collation`
- `unsupported_locale_case_insensitive_predicate`
- `unsupported_variant_access`
- `unsupported_union_dtype_cast`
- `invalid_shape_scalar_multi_column_in_subquery`

Current remaining gaps are broad ANSI subquery parity beyond the admitted bounded local scalar
IN-subquery, nested scalar IN-subquery, row-value IN-subquery, source-qualified local subquery,
scoped correlated `outer.<column>` local subquery filter, scoped subquery-backed predicate/CASE
projections, joined/grouped projected IN/EXISTS subqueries, projected row-value/quantified
subquery, correlated joined and grouped/HAVING projected scalar/row-value/quantified/EXISTS
subqueries, scoped EXISTS, scoped quantified ANY/ALL, and HAVING-level local subquery fixtures;
external-oracle result artifact population; and fuzz execution beyond the deterministic seeded
property lane. Numeric division by zero now has a deterministic runtime-error diagnostic rather
than an unsupported feature label, and scalar-left multi-column IN-subqueries now have a
deterministic invalid-shape diagnostic because row-value left operands are required. Fixed numeric
timestamp offsets are now normalized into UTC timestamp_micros through the scoped local-source
runtime. Named timezone database conversion syntax, timezone conversion functions,
`TIMESTAMPTZ`/timestamp-with-local-time-zone type spellings, `COLLATE`, and `ILIKE`
locale/case-folding comparisons now have deterministic unsupported diagnostics. Variant/union
dtype families, list/struct accessors, complex equality, broad
binary source dtype decoding, SQL source-column binary ordering without explicit cast, and remaining non-admitted broad ANSI subquery
shapes now have deterministic unsupported diagnostics with no
fallback. Scoped `ARRAY[...]` literal projection and `STRUCT(<source column>, ...)` projection are
executable through the JSONL result boundary only; nested source decoding, complex equality,
subquery membership materialization, and flat/structured sink persistence remain outside the claim
boundary.
Scoped `decimal128` add/subtract/multiply projections over same-scale and mixed-scale decimal
operands plus integer operands are executable through the same generic-expression local-source
runtime and exact JSONL/CSV text result boundary. Mixed-scale decimal comparisons and exact
fixed-scale division are executable within the scoped decimal route. Non-exact decimal division,
broad ANSI decimal coercion, exponent notation, decimal/float comparison, and typed decimal sink
preservation outside feature-gated Parquet/Arrow IPC compatibility outputs remain outside the
claim boundary.
Scoped ANSI interval literals are
executable only inside `DATE_ADD_DAYS`/`DATE_SUB_DAYS` and
`TIMESTAMP_ADD_SECONDS`/`TIMESTAMP_SUB_SECONDS`; arbitrary ANSI interval arithmetic remains outside
the claim boundary. Scoped SQL `X'<hex>'` binary literal projections are executable with exact
hex evidence. Scoped `BINARY '<utf8>'` and `BLOB '<utf8>'` text byte literal projections are
executable with exact byte evidence. Scoped `CAST`/`TRY_CAST` to `binary`/`blob`/`varbinary`
projects admitted scalar values as UTF-8 bytes, and scoped binary cast equality/inequality
predicates admit `X'<hex>'`, `BINARY`/`BLOB` text literals, single-quoted UTF-8 byte literals, or
`NULL`. Scoped binary cast ordering predicates admit bytewise lexicographic comparisons against
explicit binary literals. Scoped `UNHEX(<utf8-column>)` and `FROM_BASE64(<utf8-column>)` projections are executable
with strict UTF-8 text decoding, binary output evidence, null propagation, and deterministic
invalid-input blockers, while broad binary source dtype decoding, SQL source-column binary ordering without explicit cast, and nested binary
helper expressions remain outside the claim boundary. Scoped `CAST`/`TRY_CAST` to
`decimal128(p,s)` / `decimal(p,s)` / `numeric(p,s)` is executable for projection and predicate
fixtures with exact fixed-scale JSONL string and CSV text output, and scoped `decimal128`
add/subtract/multiply projections are executable for same-scale and mixed-scale decimal operands
plus integer operands through generic expression projection evidence, mixed-scale decimal
comparisons are admitted, and exact fixed-scale decimal division emits
`decimal128(38,max(input_scales,6))` when the quotient is exact. Non-exact decimal division, broad
ANSI decimal coercion, exponent notation, decimal/float comparison, local Vortex typed decimal
output, and Avro/ORC typed decimal sink preservation remain outside the claim boundary. Feature-gated
Parquet/Arrow IPC compatibility sinks preserve scoped `decimal128(p,s)` output columns. Scoped UTF-8
`LIKE` predicates with `%`, `_`,
and single-character
`ESCAPE` clauses are executable through
ShardLoom-owned string predicate lowering, and scoped UTF-8 regex predicates are executable through
`RLIKE`/`REGEXP`/`REGEXP_LIKE`; case-folding and locale-aware regex/collation semantics remain
outside the claim boundary.

Claim boundary: admitted SQL local-source expression/operator correctness evidence only. This does
not authorize ANSI SQL parity, production semantic parity, broad SQL/DataFrame support, performance
claims, package publication, fallback execution, or external-engine runtime delegation.
