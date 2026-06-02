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
materialization/decode boundary, prepared-state reuse scope/manifest diagnostics, runtime status,
claim boundary, and no-fallback/no-external-engine fields.

For local compatibility-file benchmark families, the Python package exposes
`ShardLoomContext.local_file_benchmark_route_report()` with schema
`shardloom.local_file_benchmark_route_report.v1`. That report maps each named scenario from
`benchmarks/common/scenario_catalog.json` to the admitted direct or prepare-once ShardLoom route,
front-door examples, Vortex normalization point, output/evidence route, materialization boundary,
prepared-state reuse scope/manifest diagnostics, and claim boundary. Its purpose is to prevent
benchmark-range fixture coverage from being described as vague no-route work while also preventing
fixture-scoped nested JSON, CDC overlay, many-small-files, partition, dirty-data, sort/window, join,
and aggregate routes from being overclaimed as broad production or performance support.

Rows with `parity_status=equivalent_admitted_scope` are the current front-door parity contract:

- `local_file_filter_project_limit`: SQL, Python, and DataFrame-style local file
  filter/project/limit collect/write workflows lower to `sql-local-source-smoke`; runtime-ready
  expansion must expose the adapter-to-Vortex normalization boundary. SQL users can provide that
  boundedness in the statement, through `collect(limit=n)`, or through `.limit(n).collect()`.
  Python/DataFrame users can use `.limit(n).collect()` or `collect(limit=n)`. Unbounded
  local-source `collect()` returns a deterministic no-fallback diagnostic. Familiar aliases
  `project`, `with_columns`, `assign`, `groupby`, `order_by`, `sort_by`, and `sort_values` lower to
  the same admitted ShardLoom operations instead of creating separate runtime paths. Row-level
  duplicate removal is admitted for bounded local-source projection, aggregate/HAVING, join, and
  window output rows through SQL `SELECT DISTINCT` and Python/DataFrame `.distinct()`,
  `.drop_duplicates()`, and `.unique()`; the runtime deduplicates before applying `LIMIT` and emits
  `distinct_projection_*` evidence. Scalar literal `IN`/`NOT IN`, row-value literal `IN`/`NOT IN`,
  bounded scalar local-source `IN` subqueries, nested bounded scalar local-source `IN` subqueries,
  row-value local-source `IN` subqueries, scoped local `EXISTS`, quantified `ANY`/`ALL`, and scoped
  correlated `outer.<column>` source-subquery filters now share the same ShardLoom SQL runtime
  evidence boundary. Source-qualified local subquery references bind to an explicit source
  `AS <alias>` or SQL-identifier file stem; Python helpers expose the alias with `source_alias=`
  and render qualified refs with `sl.col("alias.column")`. Python/DataFrame users can express
  those routes with
  `isin_source(...)`, `not_in_source(...)`, `sl.row_in(...)`, `sl.row_not_in(...)`,
  `sl.row_in_source(...)`, `sl.row_not_in_source(...)`, `sl.exists_source(...)`,
  `sl.not_exists_source(...)`, `any_source(...)`, `all_source(...)`, and `sl.outer(...)` for the
  reserved correlated outer-row alias.
- `local_file_join_aggregate_sort_window`: admitted local join, aggregate, sort, computed-column,
  and window workflows lower to `sql-local-source-smoke`.
- `generated_source_output`: source-free SQL, Python, and DataFrame-style generated-output helpers
  lower through the generated-source smoke family; generated row/range aliases such as `project`,
  `with_columns`, `assign`, and `order_by` remain thin wrappers over the same generated-source
  commands. Scoped local-emulator object-store generated-output writes additionally stage generated
  rows through that same generated-source command before invoking `object-store-write-smoke`; live
  cloud providers and lakehouse/table commits remain platform/runtime expansion items. Scoped local
  Foundry-style generated-output writes use the same generated-source command before writing local
  result/evidence dataset-shaped artifacts; real Foundry runtime and output APIs remain platform
  integration gates.
- `schema_quality_preview`: `ctx.sql(...)`, Python `LazyFrame`, and DataFrame-style helpers expose
  bounded schema, `schema_contract`/validation, data-quality, runtime profile, scoped local-source
  quarantine, preview, head, and take methods over `sql-local-source-smoke` inline results.
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
unsupported shapes. The parity matrix therefore exposes two separate fields for gap rows:
`parity_status=front_door_gap` says the broad user story is not complete, while
`runtime_gap_status` says what kind of work remains. Current precise statuses are
`front_door_connection_pending`, `runtime_expansion_pending`, and
`benchmark_publication_pending`; generic `unsupported`, `blocked`, or `not complete` labels are
validator failures for engine-capable benchmark-range surfaces.

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

The Python package exposes the operation-level map as
`ShardLoomContext.local_vortex_primitive_route_report()` with schema
`shardloom.local_vortex_primitive_route_report.v1`. That report is the source of truth for the
local `.vortex` primitive route ids, SQL/Python/DataFrame/context/session forms, CLI command
mapping, source-order limit coverage, Vortex-native start state, output route, evidence route, and
no-fallback boundary.

All admitted Vortex primitive terminal paths use explicit local primitive execution flags and emit
no-fallback ShardLoom/Vortex evidence. This is intentionally a scoped parity row, not a full
front-door parity claim: general Vortex SQL, broad read-transform-write workflows, decoded row
materialization, object-store sources, and benchmark-backed performance equivalence remain tracked
runtime-expansion work until the required evidence lands.

The benchmark-family native route is separate from those primitive helpers:
`ctx.native_vortex_route('fact.vortex', 'dim.vortex', execution_mode='native_vortex',
memory_gb=4, max_parallelism=1)` and the matching session form run
`traditional-analytics-vortex-run` / `traditional-analytics-vortex-batch-run` with explicit source,
scenario, execution-mode, resource-policy, result-sink, and no-fallback fields. Use primitive
helpers for scoped count/filter/project reports; use the native route handle for route-comparable
benchmark-range workflows.

## Runtime Expansion Checklist Families

Rows with `parity_status=front_door_gap` are not generic engine-unsupported claims. They are
runtime/user-surface expansion items that must be worked through in `GAR-RUNTIME-IMPL-6D`:

- `native_vortex_general_runtime`
  (`runtime_gap_status=front_door_connection_pending`): general Vortex-native SQL/Python/DataFrame
  read-transform-write workflows beyond the scoped local primitive runtime above.
- Explicit adapter-to-Vortex normalization/preparation evidence for every non-Vortex input route.
- Compatibility-file prepare-once routes now have a concrete context/session handle:
  `ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared')`
  and the same session form return a route over
  `compatibility_import_certified -> prepared_vortex`. The Python route now writes a
  workspace-scoped prepared-state reuse manifest and can run subsequent compatible batches through
  the real prepared Vortex batch command without re-preparing when source and artifact fingerprints
  match. The user-route capability and local-file benchmark route reports now expose the same
  reuse contract with `prepared_state_reuse_scope`, `prepared_state_reuse_manifest_path`,
  `prepared_state_reuse_policy`, `prepared_state_reuse_hit`,
  `prepared_state_reuse_reason`, `prepared_state_reuse_manifest_digest`, and
  `prepared_state_invalidation_reason`. Rust/CLI reports now emit the same fields for cold first
  preparation, warm prepared Vortex input, native Vortex input, in-process prepare/batch reuse, and
  artifact-adjacent `vortex-ingest-smoke` prepared-state reuse hits/misses. The
  `traditional-analytics-prepare-batch-run` CLI now also validates the same workspace manifest and
  skips compatibility preparation on valid source/artifact/policy hits. Remaining work is
  broadening front-door parity and deepening evidence, not inventing a separate direct
  CSV/Parquet/JSONL prepared query path.
- Broad unbounded decoded pandas, Arrow, NumPy, and notebook-display materialization outside the
  admitted local-source/materialized-input scope.
- `object_store_lakehouse_catalog`
  (`runtime_gap_status=runtime_expansion_pending`): object-store, lakehouse/table, catalog, commit,
  and remote sink workflows.
- `arbitrary_sql_python_dataframe_breadth`
  (`runtime_gap_status=front_door_connection_pending`): arbitrary SQL grammar, Python expressions,
  DataFrame API parity, UDFs, and effectful operations. Scoped row-level `SELECT DISTINCT` over
  bounded local-source projection, aggregate/HAVING, join, and window output rows is now admitted,
  scoped row-value literal `IN`/`NOT IN` predicates are admitted through SQL and Python helpers,
  scoped nested scalar local-source `IN` subqueries execute through depth-first ShardLoom-owned
  materialization evidence, source-qualified selected/filter/order refs are reachable through
  `source_alias=` plus `sl.col("alias.column")`, and scoped correlated source-subquery filters are
  reachable through the `sl.outer(...)` helper over the admitted local-source subquery families;
  arbitrary expression/DataFrame breadth remains pending until its runtime evidence lands.
- `performance_equivalence`
  (`runtime_gap_status=benchmark_publication_pending`): benchmark-backed performance equivalence
  across front doors.

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
- broad gap rows expose precise `runtime_gap_status` labels instead of generic unsupported or
  blocked posture;
- local-file benchmark scenario rows cover the required catalog scenarios with direct or
  prepare-once ShardLoom route status and no vague no-route posture;
- broad gap rows name blocker ids and required evidence;
- no row attempts fallback or invokes an external engine;
- broad flexibility and performance-equivalence claims remain blocked until evidence exists.

The validator failing is a release-readiness issue. The validator passing is not proof that broad
SQL/Python/DataFrame parity is complete; it is proof that the repo is honest and machine-readable
about current parity versus gaps.
