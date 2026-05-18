# SQL Parser And Binder Readiness

`GAR-0032-A` records the current SQL front-end posture. ShardLoom exposes deterministic SQL
diagnostics and planner-readiness rows, but it does not parse, bind, plan, or execute SQL text as a
runtime feature.

## User Surfaces

- `capabilities sql --format json`
- `workflow-unsupported-plan sql-parse <workflow> <statement> --format json`
- `workflow-unsupported-plan sql-bind <workflow> <statement> --format json`
- `workflow-unsupported-plan sql-plan <workflow> <statement> --format json`
- `workflow-unsupported-plan sql-execute <workflow> <statement> --format json`
- Python helpers: `ctx.sql_parse(...)`, `ctx.sql_bind(...)`, `ctx.sql_plan(...)`, and
  `ctx.sql_execute(...)`

## Contract Fields

The workflow unsupported report now keeps these fields explicit for SQL parser/binder/planner
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
`planner_readiness_planner_executed`, and `planner_readiness_row_ids`.

## Claim Boundary

This is a report-only readiness contract. It allows users and agents to classify SQL parse, bind,
plan, and execute requests into deterministic unsupported diagnostics, but it adds no parser
dependency, SQL AST contract, binder, logical planner, query runtime, DataFrame runtime, external
engine execution, or fallback execution.

SQL `VALUES`, literal `SELECT`, source-free projection, and generated-series forms remain
report-only unless a later generated-source or SQL runtime slice attaches parser, binder, planner,
execution certificate, Native I/O, output, correctness, benchmark, and no-fallback evidence.
