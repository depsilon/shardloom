# SQL Parser And Binder Readiness

`GAR-0032-A` recorded the original SQL front-end posture. The broad SQL front-end remains
deterministically blocked, but the scoped local/source-free SQL ladder is now runtime-backed. The
important distinction is:

- admitted local/source-free fixture shapes parse, bind, plan, and execute through ShardLoom-owned
  `local-source-runtime` or `generated-source-sql-smoke` paths;
- broad SQL parse/bind/plan/execute requests still return deterministic unsupported diagnostics and
  never fall back to Spark, DataFusion, DuckDB, SQLite, Polars, pandas, or another engine.

## User Surfaces

- `capabilities sql --format json`
- `workflow-unsupported-plan sql-parse <workflow> <statement> --format json`
- `workflow-unsupported-plan sql-bind <workflow> <statement> --format json`
- `workflow-unsupported-plan sql-plan <workflow> <statement> --format json`
- `workflow-unsupported-plan sql-execute <workflow> <statement> --format json`
- Python helpers: `ctx.sql_parse(...)`, `ctx.sql_bind(...)`, `ctx.sql_plan(...)`, and
  `ctx.sql_execute(...)`
- Scoped runtime commands: `local-source-runtime`, `generated-source-sql-smoke`,
  `ctx.sql(...).collect()`, `ctx.sql(...).write(...)`, and the Python query-builder methods that
  lower into those commands.

## Runtime Ladder

`capabilities sql --format json` exposes
`shardloom.sql_frontend_runtime_ladder.v1`. It enumerates:

- local-source projection/filter/limit;
- local-source predicate and expression families;
- scalar/grouped aggregate and admitted HAVING;
- scalar and aggregate-output top-N;
- scoped local-source joins;
- scoped ranking/offset/distribution window projections;
- local output/fanout over admitted SQL rows;
- source-free SQL generated-output paths;
- blocked broad SQL, catalog/CTE/set-operation, broad subquery, object-store/table SQL, and fallback
  engine paths.

CTE syntax is an explicit parser-bound blocker. `WITH` and `WITH RECURSIVE` statements fail before
bind, plan, runtime, source I/O, or fallback with requirements for `cte_plan_nodes`, catalog scope,
recursive runtime policy, execution certificates, and no-fallback evidence.

## Contract Fields

The workflow unsupported report keeps these fields explicit for broad SQL parser/binder/planner
requests:

- `support_status=unsupported`
- `unsupported_status=unsupported`
- `claim_gate_status=not_claim_grade`
- `parser_executed=false`
- `binder_executed=false`
- `planner_executed=false`
- `query_execution=false`
- `runtime_execution=false`
- `external_engine_invoked=false`
- `fallback_attempted=false`
- no parser dependency

The SQL capability matrix also exposes staged planner-readiness fields such as
`planner_readiness_parser_executed`, `planner_readiness_binder_executed`,
`planner_readiness_planner_executed`, and `planner_readiness_row_ids`. In
`shardloom.sql_dataframe_planner_readiness.v2`, DataFrame rows that remain in this audit are broad
planner boundaries such as `dataframe_broad_join_planner`, not the admitted Python method rows.
Current admitted DataFrame method runtime support is owned by `dataframe_method_matrix`.

## Claim Boundary

Broad SQL remains a report-only readiness contract. It allows users and agents to classify
unsupported SQL parse, bind, plan, and execute requests into deterministic diagnostics, but it adds
no broad parser dependency, catalog, optimizer, production SQL runtime, DataFrame runtime, external
engine execution, or fallback execution.

SQL `VALUES`, literal `SELECT`, selected source-free projection/generate-series/range forms, and
scoped local-source SQL are admitted only where the runtime ladder reports `smoke-supported` and
the emitted runtime evidence carries parser/binder/planner/runtime flags, Native I/O/output
evidence where applicable, `fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status=fixture_smoke_only`.
