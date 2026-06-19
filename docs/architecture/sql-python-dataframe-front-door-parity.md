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

The Python package also exposes `ShardLoomContext.front_door_semantic_surface_matrix()` with schema
`shardloom.front_door_semantic_surface_matrix.v1`. That matrix is the agent-facing source for
semantic claim language. ShardLoom does not claim broad pandas, Polars, DataFrame, or
SQL-standard/ANSI-style compatibility labels. The supported claims
are:

- ShardLoom exposes a familiar Python/DataFrame-style front door that lowers admitted operations
  into ShardLoom-native/Vortex-native routes.
- ShardLoom supports a documented subset of pandas/Polars-style DataFrame operations with
  equivalent semantics for admitted operations, deterministic blockers for non-admitted operations,
  and no fallback execution into pandas, Polars, DuckDB, Spark, DataFusion, or another engine.
- ShardLoom supports a documented SQL-standard-inspired SELECT-query subset for admitted local and
  Vortex-native routes, with documented deviations, deterministic blockers for non-admitted syntax
  or semantics, and no external query-engine fallback.

The semantic matrix covers Python/DataFrame-style construction/read APIs, selection/projection,
filtering, type system, casts/coercion, missing data, aggregation, joins, ordering/window-ish
behavior, reshaping, materialization, index semantics, expression/callable APIs, determinism,
errors/blockers, and fallback boundaries. It also covers SQL parser grammar, binder/name
resolution, type system, casts/coercion, NULL semantics, relational semantics, operator semantics,
aggregates, joins, subqueries, windows, ordering/collation, errors/edge cases, and fallback
boundaries.

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
  filter/project/limit collect workflows route through the public workflow facade and must use
  Vortex preparation plus an admitted native primitive/provider route, or fail closed with a
  deterministic no-fallback diagnostic. SQL users can provide boundedness in the statement, through
  `collect(limit=n)`, or through `.limit(n).collect()`.
  Python/DataFrame users can use `.limit(n).collect()` or `collect(limit=n)`. Unbounded
  local-source `collect()` returns a deterministic no-fallback diagnostic. Familiar aliases
  `project`, `with_columns`, `assign`, `groupby`, `order_by`, `sort_by`, and `sort_values` lower to
  the same admitted ShardLoom operations instead of creating separate runtime paths. Row-level
  duplicate removal is a native/prepared Vortex route family: SQL `SELECT DISTINCT`,
  Python/DataFrame `.distinct()` and `.unique()` lower to the distinct primitive, while scoped
  `.drop_duplicates(subset=..., keep="first"|"last"|False)` lowers to retained-row row-key state.
  Non-admitted nested/list/struct or hidden-index equality variants block instead of executing
  `sql-local-source-smoke` as the runtime middle. Scoped local-source set
  operations are admitted for already
  admitted branch `SELECT` plans through SQL `UNION`/`UNION ALL`/`INTERSECT`/`EXCEPT` and
  Python/DataFrame `.union(...)`, `.union_all(...)`, `.intersect(...)`, `.except_(...)`,
  `.except_rows(...)`, and `.subtract(...)`; branch dtypes must match and the runtime emits
  `sql_set_operation_*` no-fallback evidence. Scalar literal `IN`/`NOT IN`, row-value literal `IN`/`NOT IN`,
  bounded scalar local-source `IN`/`NOT IN` subqueries, nested bounded scalar local-source
  `IN` subqueries, row-value local-source `IN`/`NOT IN` subqueries, scoped local
  `EXISTS`/`NOT EXISTS`, quantified `ANY`/`ALL`, and scoped
  correlated `outer.<column>` source-subquery filters now share the same ShardLoom SQL runtime
  evidence boundary. Scoped subquery-backed predicate projections and CASE predicates use that same
  local-source runtime boundary when every source and outer reference is admitted.
  Source-qualified local subquery references for scalar IN/NOT IN, row-value IN/NOT IN, EXISTS,
  NOT EXISTS, and quantified predicates bind to an explicit source `AS <alias>` or SQL-identifier file stem; Python
  helpers expose the alias with `source_alias=` and render qualified refs with
  `sl.col("alias.column")`. Python/DataFrame users can express
  those routes with
  `isin_source(...)`, `not_in_source(...)`, `sl.row_in(...)`, `sl.row_not_in(...)`,
  `sl.row_in_source(...)`, `sl.row_not_in_source(...)`, `sl.exists_source(...)`,
  `sl.not_exists_source(...)`, `any_source(...)`, `all_source(...)`, and `sl.outer(...)` for the
  reserved correlated outer-row alias. `SqlLocalSourceSmokeReport` exposes
  `source_qualified_subquery_*` fields for the runtime-execution flag, bound qualifiers, operator
  families, and source columns. When those helpers render a non-admitted runtime shape, such as
  `outer.<column>` outside column-to-column subquery comparisons, Python exposes the CLI status,
  diagnostics, and deduplicated `unsupported_reasons` on `SqlLocalSourceSmokeReport` while keeping
  `fallback_attempted=false` and `external_engine_invoked=false`.
- `local_file_join_aggregate_sort_window`: admitted local join, aggregate, sort, computed-column,
  and benchmark-family workflows must route through prepared/native Vortex primitive or provider
  lanes when public product support is claimed; non-admitted residual shapes remain deterministic
  blockers rather than direct local-source smoke execution.
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
  only the bounded schema/quarantine/preview paths and metadata-first `profile()` routes that have
  admitted Vortex-backed route evidence. Base-source profile uses `vortex-metadata-summary` over a
  native or prepared Vortex source; transformed row-profile and residual materialization helpers
  return deterministic blockers instead of executing `sql-local-source-smoke` inline results as
  product runtime.
- `decoded_materialization_interop`: bounded local-source ShardLoom results can materialize to
  Python objects, optional pandas DataFrames, optional PyArrow tables/IPC bytes, optional NumPy
  arrays, and notebook preview HTML from the same inline result path; pandas/Arrow materialized
  input snapshots lower to generated-source user rows and must re-enter through a Vortex-preparable
  route for runtime-ready claims.
- `local_vortex_primitive_runtime`: SQL, Python, and DataFrame-style local Vortex primitive report
  workflows lower to ShardLoom's explicit Vortex primitive command family for count, count-where,
  filter, project, and filter-project with optional source-order limit.
- `native_vortex_general_runtime`: admitted native `.vortex` inputs and Vortex-prepared local
  compatibility sources share the `native_vortex_unified_plan` evidence contract for documented
  primitive, provider, profile, and declared sink operator capillaries.

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
`runtime_gap_status` says what kind of work remains. Current v1 gap statuses are
`runtime_expansion_pending` and `benchmark_publication_pending`; generic no-route or incomplete
labels are validator failures for engine-capable benchmark-range
surfaces.

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
no-fallback ShardLoom/Vortex evidence. The broader `native_vortex_general_runtime` parity row is
now admitted only for documented `native_vortex_unified_plan` families that share
primitive/provider/profile/sink capillaries across SQL, Python, and DataFrame-style front doors.
Arbitrary SQL/DataFrame breadth, object-store/table sources, non-admitted operator families, and
benchmark-backed performance equivalence remain tracked runtime-expansion work until the required
evidence lands.

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
- `arbitrary_sql_python_dataframe_breadth` is admitted for the documented v1 scoped surface, not as
  broad pandas/Polars compatibility or ANSI SQL compliance. Scoped row-level `SELECT DISTINCT` over
  bounded local-source projection, aggregate/HAVING, join, and window output rows is admitted.
  Scoped row-value literal `IN`/`NOT IN` predicates are admitted through SQL and Python helpers.
  Scoped nested scalar local-source `IN` subqueries execute through depth-first ShardLoom-owned
  materialization evidence. Source-qualified scalar/row-value `IN`/`NOT IN`, `EXISTS`/`NOT EXISTS`,
  and quantified subquery refs are reachable through `source_alias=` plus `sl.col("alias.column")`.
  Scoped correlated source-subquery filters are reachable through the `sl.outer(...)` helper over
  the admitted local-source subquery families; direct SQL predicate and CASE projections can reuse
  those admitted subquery predicates. Python `SqlLocalSourceSmokeReport` exposes runtime
  no-route diagnostics for non-admitted correlated subquery shapes directly as `status`,
  `diagnostics`, and reason metadata, so Python users can inspect the same deterministic
  no-fallback blocker emitted by the CLI. It also exposes source-qualified subquery evidence
  directly as `source_qualified_subquery_runtime_execution`,
  `source_qualified_subquery_source_qualifiers`,
  `source_qualified_subquery_operator_families`, and
  `source_qualified_subquery_source_columns`. Scoped local-source `INTERSECT` and `EXCEPT` reuse the
  same branch-bound set-operation runtime and Python/DataFrame aliases as scoped `UNION`, with
  `sql_set_operation_*` evidence over already-admitted branch `SELECT` plans. Scoped decimal casts
  plus mixed-scale add/subtract/multiply, comparison, and exact fixed-scale division lower through
  the same ShardLoom generic-expression route from SQL and Python/DataFrame helpers. Public
  local-source compatibility sinks must derive from a certified native Vortex result/export
  contract: scoped structured Vortex/Parquet/Arrow IPC/Avro expression-project exports are admitted
  through Vortex preparation plus native Vortex row export, while arbitrary compatibility exports
  and ORC nested output remain deterministic future-contract boundaries. Local Vortex remains the
  highest-fidelity sink where the provider or scoped structured route is admitted. Scoped
  `pivot_table` accepts one aggregate as a scalar string, one-element sequence, or one-column
  mapping when it maps to the admitted sum/count/mean/min/max native Vortex pivot aggregate family. Scoped
  `melt(id_vars=..., ignore_index=True)` can infer same-typed value columns from the current
  schema/projection and lowers to the same native/prepared Vortex melt primitive as explicit
  `value_vars`; scoped single-column `explode(..., ignore_index=True)` uses the existing native
  Vortex explode primitive with no hidden pandas index materialization.
  scalar-expression `JOIN ON` predicates over qualified local sources lower through the bounded
  expression-join route, including Python `LazyFrame.join(condition=...)` predicate objects and
  logical `OR` over admitted qualified scalar leaves; complex-key and broader non-scalar join
  predicates remain deterministic future-contract boundaries. Schema-declared local-source
  `rename`/`rename_columns` and `drop`/`drop_columns` lower to projection alias/rewrite runtime
  routes. Scoped local-source `value_counts` lowers to the grouped `count(*) AS rows` route with
  optional `IS NOT NULL` dropna filtering and rows-desc ordering. Scoped row-wise `concat` lowers to
  `UNION ALL` only for two local-source branches with explicit matching projected columns. Scoped
  explicit-key `merge(on=..., how=...)` lowers to the admitted join route. Scoped one-column
  `nunique(..., dropna=True)` lowers to `count(DISTINCT column)` with SQL null semantics. Scoped
  schema-declared `astype` lowers to `CAST` projection rewrites, scoped schema-declared
  `dropna(how="any"|"all")` and `dropna(thresh=<int>)` lower to `IS NOT NULL` filters joined with
  `AND`, `OR`, or threshold combinations, and `query(...)` aliases the admitted ShardLoom predicate
  path when no pandas expression-engine kwargs are requested. Scoped
  `nlargest` / `nsmallest` lower to `ORDER BY ... LIMIT` when `keep="first"` and the sort keys are
  admitted. Scoped local-source `sort(...)`/`order_by(...)`/`sort_values(...)` can pass
  `nulls="first"|"last"` to lower to explicit SQL `NULLS FIRST|LAST` top-N ordering. Scoped
  schema-declared `fillna`/`fill_null` lowers to `COALESCE` projection rewrites for scalar or
  per-column literals with `axis=0`/`index` and immutable `inplace=False`, and scoped
  schema-declared `isna`/`isnull`/`notna`/`notnull` lowers to `IS NULL` / `IS NOT NULL` boolean
  projection rewrites. Scoped schema-declared
  `mask(predicate, scalar, axis=0/index, inplace=False, level=None)` lowers to native/prepared
  Vortex expression-project conditional rewrites, and scoped schema-declared
  `replace(old, new, regex=False, inplace=False, method=None, limit=None)` lowers to native/prepared
  Vortex expression-project scalar replacement, including column-nested `{column: {old: new}}`
  scalar mapping forms. Scoped SQL/Python `IS DISTINCT FROM` and
  `IS NOT DISTINCT FROM` null-safe
  comparisons lower to the same ShardLoom-owned null/comparison/logical predicate runtime for
  admitted filters and predicate projections over column-literal, date/timestamp/binary literal,
  NULL literal, and column-column operands. Broad pandas result-shape variants, arbitrary Python
  callable/UDF execution, and external effects fail closed through deterministic workflow
  diagnostics instead of missing attributes or hidden pandas/Polars
  execution. Scoped `eval("amount = amount + 5")` and multi-assignment forms like
  `eval("amount = amount + 5; tax = tax * 2")` lower to the same native/prepared Vortex
  expression-project primitive for existing numeric columns; Python/numexpr engines, new-column
  assignment, non-assignment expressions, callables, and side effects remain deterministic
  future-contract boundaries.
- The DataFrame method matrix currently emits 27 future-contract variant IDs, all classified by
  `DATAFRAME_FUTURE_CONTRACT_CLASSIFICATION_ROWS`: 19 are repo-feasible broad-profile expansion
  items, 6 are unsafe callable/UDF boundaries that require a typed/sandboxed contract, and 2 are
  scoped product boundaries around hidden pandas-style index behavior. These IDs are not active
  base-method blockers; they identify where broad pandas/Polars-style parity would require
  additional contracts and evidence.
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
