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
matrix_row_count=9
executable_fixture_count=7
unsupported_diagnostic_count=2
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
- `unsupported_numeric_division_by_zero`
- `unsupported_cast_decimal128`

Current remaining gaps are decimal precision/scale semantics, timezone database and non-UTC
timestamp policy, binary and nested/list equality semantics, join/window semantic certification,
external-oracle result artifact population, and fuzz execution beyond the deterministic seeded
property lane.

Claim boundary: admitted SQL local-source expression/operator correctness evidence only. This does
not authorize ANSI SQL parity, production semantic parity, broad SQL/DataFrame support, performance
claims, package publication, fallback execution, or external-engine runtime delegation.
