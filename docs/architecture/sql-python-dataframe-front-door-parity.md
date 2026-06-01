# SQL/Python/DataFrame Front-Door Parity

Status: active `GAR-RUNTIME-IMPL-6C` evidence.

ShardLoom's target user experience is that users can express the same workload through SQL,
Python, or DataFrame-style code, have those front doors lower to the same Vortex-normalized
ShardLoom-native plan, and expect equivalent behavior and performance unless the capability surface
says otherwise.

User inputs are front doors, not alternate engines. Native `.vortex` inputs begin at the
Vortex-native boundary; compatibility files, generated rows, and materialized Python/Arrow inputs
must expose where they normalize or prepare into a Vortex-backed ShardLoom route before the workflow
is runtime-ready or claim-grade.

That target is not fully true yet. The current repo has scoped local parity for admitted local-file,
generated-output, bounded interop, and local Vortex primitive workflows. `GAR-RUNTIME-IMPL-6D`
tracks the remaining work to make Vortex normalization explicit for every non-Vortex input route and
to close broad "build anything" parity and performance-equivalence evidence.

## Current Admitted Parity

The Python package exposes `ShardLoomContext.front_door_parity_matrix()` with schema
`shardloom.front_door_parity_matrix.v1`.

For route selection, the Python package also exposes
`ShardLoomContext.user_route_capability_report()` with schema
`shardloom.user_route_capability_report.v1`. The route report is the agent-facing answer to
"given input X and desired output Y, which ShardLoom route should I use?" Each row carries the
start state, Vortex normalization point, execution mode, output route, evidence route,
materialization/decode boundary, runtime status, claim boundary, and no-fallback/no-external-engine
fields.

Rows with `parity_status=equivalent_admitted_scope` are the current front-door parity contract:

- `local_file_filter_project_limit`: SQL, Python, and DataFrame-style local file
  filter/project/limit collect/write workflows lower to `sql-local-source-smoke`; runtime-ready
  expansion must expose the adapter-to-Vortex normalization boundary.
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
  input snapshots lower to generated-source user rows and must re-enter through a Vortex-preparable
  route for runtime-ready claims.
- `local_vortex_primitive_runtime`: SQL, Python, and DataFrame-style local Vortex primitive report
  workflows lower to ShardLoom's explicit Vortex primitive command family for count, count-where,
  filter, project, and filter-project with optional source-order limit.

These rows allow scoped local parity, not broad production claims.

## Vortex Normalization Contract

Every user input route should answer four questions:

- What input did the user declare: native `.vortex`, local compatibility file, generated rows, or
  explicit materialized Python/Arrow data?
- Where does that input enter the Vortex-backed ShardLoom path: already-native Vortex,
  compatibility import, prepared Vortex artifact, generated row batch, or materialized snapshot
  re-entry?
- Which ShardLoom runtime command/family executes after that boundary?
- Which evidence fields prove no fallback engine was invoked and identify any decode,
  materialization, or output translation boundary?

Rows that cannot answer those questions are runtime-expansion checklist items, not vague
unsupported shapes.

## Scoped Vortex Primitive Runtime

The SQL/Python/DataFrame-style Vortex front doors now admit a scoped local primitive slice:

- `read_vortex(...).count()` lowers to `vortex-run ... count`.
- `read_vortex(...).filter(...).count()` lowers to `vortex-count-where`.
- `read_vortex(...).select(...).collect()` lowers to `vortex-project`.
- `read_vortex(...).filter(...).collect()` lowers to `vortex-filter`.
- `read_vortex(...).filter(...).limit(...).collect()` lowers to `vortex-filter --limit`.
- `read_vortex(...).select(...).limit(...).collect()` lowers to `vortex-project --limit`.
- `read_vortex(...).filter(...).select(...).limit(...).collect()` lowers to
  `vortex-filter-project --limit`.
- `ctx.sql("SELECT COUNT(*) FROM 'local.vortex'").collect()` lowers to `vortex-run ... count`.
- `ctx.sql("SELECT COUNT(*) FROM 'local.vortex' WHERE value >= 3").collect()` lowers to
  `vortex-count-where`.
- `ctx.sql("SELECT metric FROM 'local.vortex'").collect()` lowers to `vortex-project`.
- `ctx.sql("SELECT metric FROM 'local.vortex' LIMIT 5").collect()` lowers to
  `vortex-project --limit`.
- `ctx.sql("SELECT * FROM 'local.vortex' WHERE value >= 3").collect()` lowers to
  `vortex-filter-project` with `*` projection.
- `ctx.sql("SELECT * FROM 'local.vortex' LIMIT 5").collect()` lowers to
  `vortex-project --limit` with `*` projection.
- `ctx.sql("SELECT metric FROM 'local.vortex' WHERE value >= 3 LIMIT 5").collect()` lowers to
  `vortex-filter-project --limit`.

All admitted Vortex primitive terminal paths use explicit local primitive execution flags and emit
no-fallback ShardLoom/Vortex evidence. This is intentionally a scoped parity row, not a full
front-door parity claim: general Vortex SQL, broad read-transform-write workflows, decoded row
materialization, object-store sources, and benchmark-backed performance equivalence remain tracked
runtime-expansion work until the required evidence lands.

## Runtime Expansion Checklist Families

Rows with `parity_status=front_door_gap` are not generic engine-unsupported claims. They are
runtime/user-surface expansion items that must be worked through in `GAR-RUNTIME-IMPL-6D`:

- General Vortex-native SQL/Python/DataFrame read-transform-write workflows beyond the scoped local
  primitive runtime above.
- Explicit adapter-to-Vortex normalization/preparation evidence for every non-Vortex input route.
- Broad unbounded decoded pandas, Arrow, NumPy, and notebook-display materialization outside the
  admitted local-source/materialized-input scope.
- Object-store, lakehouse/table, catalog, commit, and remote sink workflows.
- Arbitrary SQL grammar, Python expressions, DataFrame API parity, UDFs, and effectful operations.
- Benchmark-backed performance equivalence across front doors.

The parity matrix intentionally keeps `flexible_anything_claim_allowed=false` and
`performance_equivalence_claim_allowed=false` until those checklist items are closed with
correctness, Native I/O, execution-certificate, no-fallback, and benchmark evidence.

## Validator

Run:

```bash
python3 scripts/check_sql_python_dataframe_parity.py --output target/sql-python-dataframe-parity-gate.json
python3 scripts/check_user_route_capability_report.py --output target/user-route-capability-report.json
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
