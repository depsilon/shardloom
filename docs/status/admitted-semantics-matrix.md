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

Current remaining gaps are broad ANSI subquery parity beyond the admitted bounded local
scalar/row-value IN/NOT IN, EXISTS/NOT EXISTS, quantified ANY/ALL, nested scalar IN,
projected joined/grouped scalar/row-value IN/NOT IN/EXISTS/NOT EXISTS, projected quantified,
source-qualified scalar/row-value IN/NOT IN/EXISTS/NOT EXISTS/quantified local subquery references,
correlated `outer.<column>` subquery filter, subquery-backed predicate/CASE projection,
HAVING-level scalar/row-value IN/NOT IN, EXISTS/NOT EXISTS, and correlated quantified variants,
and deterministic outer-reference diagnostics; external-oracle
result artifact population; and general fuzz execution beyond the deterministic v1 property/fuzz
lanes. Numeric division by zero now has a deterministic runtime-error diagnostic rather
than an unsupported feature label, and scalar-left multi-column IN-subqueries now have a
deterministic invalid-shape diagnostic because row-value left operands are required. Fixed numeric
timestamp offsets are now normalized into UTC timestamp_micros through the scoped local-source
runtime. Named timezone database conversion syntax, timezone conversion functions,
`TIMESTAMPTZ`/timestamp-with-local-time-zone type spellings, `COLLATE`, and `ILIKE`
locale/case-folding comparisons now have deterministic unsupported diagnostics. List/array
access-or-cast, struct access-or-cast, complex subquery membership materialization, variant, and
union dtype families, binary literal predicates against non-binary source columns,
non-binary source ordering predicates against binary literals, outer references outside admitted
column-to-column subquery comparisons, outer-to-outer subquery comparisons, and remaining
non-admitted broad ANSI subquery shapes now have deterministic unsupported diagnostics with no
fallback. Scoped
scalar-expression `JOIN ON` predicates over qualified local sources are executable through the
bounded expression-join route, including scoped logical `OR` over admitted qualified scalar leaves;
complex `ARRAY[...]`/`STRUCT(...)` join keys still block deterministically. Scoped `ARRAY[...]`
literal projection and `STRUCT(<source column>, ...)` projection are executable through the JSONL
result boundary and local CSV JSON-text output cells. Scoped `SELECT DISTINCT` and `UNION DISTINCT`
over those already-materialized ARRAY/STRUCT projection values are executable through structural
result-row equality, and scoped `ORDER BY` over those complex projection values is executable
through canonical structural result-boundary sort keys. Feature-gated local structured source
decoding now admits Arrow list/large-list/fixed-size-list and struct arrays into ShardLoom
`ScalarValue::List` / `ScalarValue::Struct` values through the JSONL result boundary and local CSV
JSON-text output cells, with Arrow IPC CLI smoke evidence and shared materializer coverage for the
admitted Arrow array families surfaced by local Parquet/Arrow IPC/Avro/ORC readers. Broad ANSI
nested ordering, nested accessors/casts, complex subquery membership materialization, complex-key
joins, broader non-scalar join predicates, and ORC nested output remain outside the claim
boundary. ORC nested output now has a validator-backed output-plan blocker,
`typed_complex_preservation_not_admitted`, before provider conversion, local write, or fallback.
All-null typed nested sink columns without child-schema evidence fail
closed with `typed_complex_child_schema_not_admitted` before structured writer conversion.
Feature-gated Parquet/Arrow IPC/Avro and scoped local Vortex typed nested compatibility sinks are
admitted when one stable Arrow nested dtype can be inferred from non-null `List` / `Struct` values
or carried from raw source-column child-schema evidence; local Vortex uses
`ArrayRef::from_arrow(RecordBatch)` before the existing Vortex writer/reopen proof. ORC nested
output remains blocked before provider conversion with no artifact write.
Scoped `decimal128` add/subtract/multiply projections over same-scale and mixed-scale decimal
operands plus integer operands are executable through the same generic-expression local-source
runtime and exact JSONL/CSV text result boundary. Mixed-scale decimal comparisons and exact
fixed-scale division are executable within the scoped decimal route. Exact exponent notation that
normalizes to the declared `decimal128(p,s)` scale is admitted through the scoped decimal cast route.
Non-exact decimal division, broad ANSI decimal coercion beyond that exact exponent normalization,
decimal/float comparison, typed decimal sink preservation outside feature-gated Parquet/Arrow
IPC/Avro compatibility outputs and scoped local Vortex known flat scalar output, and ORC typed
decimal sinks remain outside the claim boundary. ORC typed decimal sinks now have a
validator-backed output-plan blocker, `typed_decimal128_preservation_not_admitted`, before provider
conversion, local write, or fallback.
Scoped ANSI interval literals are
executable only inside `DATE_ADD_DAYS`/`DATE_SUB_DAYS` and
`TIMESTAMP_ADD_SECONDS`/`TIMESTAMP_SUB_SECONDS`; arbitrary ANSI interval arithmetic now blocks with
a deterministic unsupported diagnostic before fallback. Scoped SQL `X'<hex>'` binary literal
projections are executable with exact hex evidence. Scoped `BINARY '<utf8>'` and `BLOB '<utf8>'`
text byte literal projections are executable with exact byte evidence. Scoped `CAST`/`TRY_CAST` to
`binary`/`blob`/`varbinary`
projects source-backed UTF-8 column, string-transform, and string-function expression values as
bytes. Scoped binary cast equality/inequality predicates admit `X'<hex>'`, `BINARY`/`BLOB` text
literals, single-quoted UTF-8 byte literals, or `NULL` against those admitted source-backed UTF-8
expressions. Scoped binary cast ordering predicates admit bytewise lexicographic comparisons
against explicit binary literals for the same expression subset. Scoped direct binary source
predicates and source-column ordering over
feature-gated Arrow IPC binary byte-array source columns admit bytewise lexicographic comparisons
against explicit binary literals with SQL NULLs filtering out of WHERE results. Non-binary source
columns compared to binary literals fail with deterministic unsupported diagnostics. Scoped
`UNHEX(<utf8-column-or-admitted-utf8-expression>)` and
`FROM_BASE64(<utf8-column-or-admitted-utf8-expression>)` projections and predicates are executable
for source-backed UTF-8 column, string-transform, and string-function argument expressions, with
strict UTF-8 text decoding, binary output/equality evidence, null propagation, and deterministic
invalid-input blockers. The feature-gated local columnar materialization boundary also admits Arrow
binary byte-array source columns as `ScalarValue::Binary` for direct projection, with null
propagation and JSONL/CSV `binary[hex=...]` result evidence; the executable CLI proof covers Arrow
IPC, and the shared materializer covers Arrow `Binary`, `LargeBinary`, `FixedSizeBinary`, and
`BinaryView` arrays surfaced by admitted local structured readers. Feature-gated Parquet/Arrow
IPC/Avro/ORC flat scalar compatibility sinks preserve admitted binary byte payloads from SQL result
batches, including all-null Arrow IPC binary source columns with source-schema dtype evidence, with
focused SQL fanout and writer round-trip evidence. Feature-gated local Vortex flat scalar sinks now
preserve nullable/all-null boolean, int64, uint64, float64, utf8, binary, decimal128, date32, and
timestamp_micros result columns when dtype/family evidence is present, through the Vortex
writer/reopen path. Unknown or unsupported NULL-bearing Vortex output batches block before writer
conversion; binary sink preservation outside scoped Parquet/Arrow IPC/Avro/ORC and Vortex flat
scalar outputs, broader binary execution beyond scoped source projection/predicate/order plus
explicit casts/helpers over admitted source-backed UTF-8 expressions, and binary cast/helper
expressions outside the admitted source-backed UTF-8 expression subset remain outside the claim
boundary.
Scoped
`CAST`/`TRY_CAST` to
`decimal128(p,s)` / `decimal(p,s)` / `numeric(p,s)` is executable for projection and predicate
fixtures with exact fixed-scale JSONL string and CSV text output, and scoped `decimal128`
add/subtract/multiply projections are executable for same-scale and mixed-scale decimal operands
plus integer operands through generic expression projection evidence, mixed-scale decimal
comparisons are admitted, exact fixed-scale decimal division emits
`decimal128(38,max(input_scales,6))` when the quotient is exact, and exact exponent notation is
admitted when it normalizes to the declared target scale. Non-exact decimal division, broad ANSI
decimal coercion beyond exact exponent normalization, decimal/float comparison, and ORC typed
decimal sink preservation remain outside the claim boundary with a validator-backed
`typed_decimal128_preservation_not_admitted` output-plan blocker. Feature-gated Parquet/Arrow IPC/Avro
compatibility sinks plus scoped local Vortex known flat scalar output preserve scoped
`decimal128(p,s)` output columns, including nullable/all-null decimal columns with dtype evidence.
Scoped UTF-8
`LIKE` predicates with `%`, `_`,
and single-character
`ESCAPE` clauses are executable through
ShardLoom-owned string predicate lowering, and scoped UTF-8 regex predicates are executable through
`RLIKE`/`REGEXP`/`REGEXP_LIKE`; case-folding and locale-aware regex/collation semantics remain
outside the claim boundary.

Claim boundary: admitted SQL local-source expression/operator correctness evidence only. This does
not authorize ANSI SQL parity, production semantic parity, broad SQL/DataFrame support, performance
claims, package publication, fallback execution, or external-engine runtime delegation.
