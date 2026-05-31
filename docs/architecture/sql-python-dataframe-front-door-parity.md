# SQL/Python/DataFrame Front-Door Parity

Status: active `GAR-RUNTIME-IMPL-6C` evidence.

ShardLoom's target user experience is that users can express the same workload through SQL,
Python, or DataFrame-style code, have those front doors lower to the same ShardLoom-native plan, and
expect equivalent behavior and performance unless the capability surface says otherwise.

That target is not fully true yet. The current repo has scoped local parity for admitted local-file
and generated-output workflows, but broad "build anything" parity and performance equivalence remain
blocked.

## Current Admitted Parity

The Python package exposes `ShardLoomContext.front_door_parity_matrix()` with schema
`shardloom.front_door_parity_matrix.v1`.

Rows with `parity_status=equivalent_admitted_scope` are the current front-door parity contract:

- `local_file_filter_project_limit`: SQL, Python, and DataFrame-style local file
  filter/project/limit collect/write workflows lower to `sql-local-source-smoke`.
- `local_file_join_aggregate_sort_window`: admitted local join, aggregate, sort, computed-column,
  and window workflows lower to `sql-local-source-smoke`.
- `generated_source_output`: source-free SQL, Python, and DataFrame-style generated-output helpers
  lower through the generated-source smoke family.
- `schema_quality_preview`: `ctx.sql(...)`, Python `LazyFrame`, and DataFrame-style helpers expose
  bounded schema, validation, data-quality, preview, head, and take methods over
  `sql-local-source-smoke` inline results.

These rows allow scoped local parity, not broad production claims.

## Blocking Gap Families

Rows with `parity_status=front_door_gap` are real blockers for the user goal:

- General Vortex-native SQL/Python/DataFrame read-transform-write workflows.
- Decoded pandas, Arrow, NumPy, and notebook-display materialization.
- Object-store, lakehouse/table, catalog, commit, and remote sink workflows.
- Arbitrary SQL grammar, Python expressions, DataFrame API parity, UDFs, and effectful operations.
- Benchmark-backed performance equivalence across front doors.

The parity matrix intentionally keeps `flexible_anything_claim_allowed=false` and
`performance_equivalence_claim_allowed=false` until those rows are closed with correctness,
Native I/O, execution-certificate, no-fallback, and benchmark evidence.

## Validator

Run:

```bash
python3 scripts/check_sql_python_dataframe_parity.py --output target/sql-python-dataframe-parity-gate.json
```

The validator passes when:

- required parity rows are present;
- scoped admitted rows identify their shared ShardLoom runtime path;
- broad gap rows name blocker ids and required evidence;
- no row attempts fallback or invokes an external engine;
- broad flexibility and performance-equivalence claims remain blocked until evidence exists.

The validator failing is a release-readiness issue. The validator passing is not proof that broad
SQL/Python/DataFrame parity is complete; it is proof that the repo is honest and machine-readable
about current parity versus gaps.
