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
- `decoded_materialization_interop`: bounded local-source ShardLoom results can materialize to
  Python objects, optional pandas DataFrames, optional PyArrow tables/IPC bytes, optional NumPy
  arrays, and notebook preview HTML from the same inline result path; pandas/Arrow materialized
  input snapshots lower to generated-source user rows.

These rows allow scoped local parity, not broad production claims.

## Scoped Vortex Primitive Runtime

The Python/DataFrame-style Vortex front door now admits a narrow local primitive slice:

- `read_vortex(...).count()` lowers to `vortex-run ... count`.
- `read_vortex(...).filter(...).count()` lowers to `vortex-count-where`.
- `read_vortex(...).select(...).collect()` lowers to `vortex-project`.
- `read_vortex(...).filter(...).collect()` lowers to `vortex-filter`.
- `read_vortex(...).filter(...).select(...).limit(...).collect()` lowers to
  `vortex-filter-project --limit`.

All admitted Vortex primitive terminal paths use explicit local primitive execution flags and emit
no-fallback ShardLoom/Vortex evidence. This is intentionally not a new full front-door parity row:
general Vortex SQL, broad read-transform-write workflows, decoded row materialization, object-store
sources, and benchmark-backed performance equivalence remain blocked until the required evidence
lands.

## Blocking Gap Families

Rows with `parity_status=front_door_gap` are real blockers for the user goal:

- General Vortex-native SQL/Python/DataFrame read-transform-write workflows beyond the scoped
  local primitive runtime above.
- Broad unbounded decoded pandas, Arrow, NumPy, and notebook-display materialization outside the
  admitted local-source/materialized-input scope.
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
