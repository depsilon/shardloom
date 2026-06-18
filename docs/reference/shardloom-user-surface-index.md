<!-- SPDX-License-Identifier: Apache-2.0 -->

# ShardLoom User Surface Index

Status: canonical agent-facing v0.1.x user-surface reference.

Schema marker: `shardloom.user_surface_index.v1`.

This file is the durable starting point for agents and humans that need to know which ShardLoom
commands, Python helpers, SQL forms, local inputs, local outputs, and diagnostics exist. It is an
index, not a runtime claim. Runtime support still depends on the command registry, capability
reports, execution certificates, feature gates, and no-fallback evidence.

All ShardLoom user surfaces must preserve:

```text
fallback_attempted=false
external_engine_invoked=false
```

## Source Of Truth Order

Use this order before executing or documenting a workflow:

1. Read this file for the stable user-surface map.
2. Read `docs/reference/shardloom-user-surface-index.json` for machine-readable surface groups.
3. Run `shardloom command-metadata --format json` for the exhaustive CLI command inventory.
4. Run `shardloom command-metadata <command> --format json` or
   `shardloom help <command> --format json` for one CLI command.
5. Run `shardloom agent-contract-pack --format json` for report-order and safety defaults.
6. Use `ctx.capabilities()` / `ShardLoomClient.capabilities(...)` for Python capability posture.
7. Use `ctx.user_surface_graduation_matrix()` and `ctx.front_door_parity_matrix()` before
   treating a Python, SQL, DataFrame-style, or CLI surface as promoted user workflow support.

The CLI command list is intentionally not hand-copied here. The exhaustive registry has 213 rows
and lives in `shardloom-cli/src/command_registry.rs`; the side-effect-free command
`shardloom command-metadata --format json` renders those rows for agents without scraping Rust
source or human text.

## Normal Python Entry Points

Normal application code starts with:

```python
import shardloom as sl

ctx = sl.context()
```

`sl.context(...)` and `sl.session(...)` construct clients only. They do not read data, probe object
stores, touch catalogs, execute SQL, invoke external engines, or create hidden global state.

Use `repo_root`, `profile_order`, `SHARDLOOM_BIN`, or `SHARDLOOM_REPO_ROOT` only when a source
checkout, CI job, managed environment, or benchmark reproduction needs explicit CLI resolution.

## Python Reads

Use `ctx.read(path)` for ordinary local input. It infers:

- `.csv` -> CSV compatibility source.
- `.json`, `.jsonl`, `.ndjson` -> flat JSON/JSONL/NDJSON compatibility source.
- `.parquet` -> Parquet compatibility source when the matching feature-gated build is present.
- `.arrow`, `.ipc`, `.feather` -> Arrow IPC compatibility source when feature-gated.
- `.avro` -> Avro compatibility source when feature-gated.
- `.orc` -> ORC compatibility source when feature-gated.
- `.vortex` -> native Vortex source.

Explicit helpers remain available when an agent needs a pinned adapter:

- `ctx.read_csv(...)`
- `ctx.read_json(...)`
- `ctx.read_parquet(...)`
- `ctx.read_arrow_ipc(...)`
- `ctx.read_avro(...)`
- `ctx.read_orc(...)`
- `ctx.read_vortex(...)`

Feature-gated readers return deterministic adapter blockers when unavailable. They must not invoke
another engine as a fallback.

Every normal Python execution or route result exposes `activation_summary`. Use it before scraping
raw envelopes when checking route ID/status, execution mode, native Vortex activation, required
feature gate, parallelism, scan/pushdown signals, source-state reuse, decode/materialization,
sink/write status, fallback/external-engine flags, claim gate, and unsupported diagnostics.

For direct `.vortex` inputs, use
`ctx.native_vortex_provider_route_certificate_report()` to inspect the exact release-feature-backed
Python/SQL provider routes for grouped aggregation, hash join, global top-N, cast/try-cast,
substring contains, and native `write_vortex` sink shapes. Scoped primitive routes also cover
count/filter/project/limit, no-argument row-level distinct, bounded source-order tail, and
deterministic bounded `sample(n=..., seed=...|random_state=<int>, replace=False)`. These
reports are route evidence, not broad arbitrary Vortex SQL/DataFrame parity or performance claims.

## Python Query Builder

Admitted scoped local-source and generated-source workflows use `LazyFrame` and related source
objects. Common supported or scoped methods include:

- Filters and predicates: `filter(...)`, `where(...)`, `query(...)` without unsupported keyword
  arguments, `having(...)` after an aggregate.
- Projection and schema rewrites: `select(...)`, `project(...)`, `rename(...)`,
  `rename_columns(...)`, `drop(...)`, `drop_columns(...)`, `astype(...)`.
- Row bounds: `limit(...)`, `head(...)`, `take(...)`, scoped `tail(...)`, and scoped deterministic
  `sample(n=..., seed=...|random_state=<int>, replace=False)`.
- Aggregation: `group_by(...).agg(...)`, `groupby(...).agg(...)`, scalar `agg(...)`,
  `aggregate(...)`, `nunique(...)`, `value_counts(...)`.
- Joins and set operations: scoped `join(...)`, `merge(...)` when it lowers to the same join,
  `concat(...)` when it lowers to `UNION ALL`, `union(...)`, `union_all(...)`,
  `intersect(...)`, `except_rows(...)`, `subtract(...)`.
- Ordering and top N: `sort(...)`, `order_by(...)`, `sort_by(...)`, `sort_values(...)`,
  scoped index metadata `set_index(..., drop=False)`, source-order-preserving
  `reset_index(drop=True)`/`sort_index(ascending=True)`, `nlargest(...)`, `nsmallest(...)`.
- Null and duplicate helpers: `dropna(...)`, `fillna(...)`, `fill_null(...)`, `isna(...)`,
  `isnull(...)`, `notna(...)`, `notnull(...)`, `distinct()`, `drop_duplicates()`, `unique()`.
- Computed columns: `with_column(...)`, `with_columns(...)`, `assign(...)` when the expression
  lowers to the scoped ShardLoom expression surface.
- Local execution and writes: bounded `collect(...)`, `run(...)`, `route(...)`, `prepare(...)`,
  `write(...)`, `write_jsonl(...)`, `write_csv(...)`, feature-gated `write_parquet(...)`,
  `write_arrow_ipc(...)`, `write_avro(...)`, `write_orc(...)`, `write_vortex(...)`, and
  `fanout(...)`.
- Bounded inspection: `schema(...)`, `describe_schema(...)`, `validate_schema(...)`,
  `schema_contract(...)`, `data_quality_check(...)`, `data_quality(...)`,
  `data_quality_summary(...)`, scoped `describe(...)`, `profile(...)`, `preview(...)`, `display(...)`,
  `to_python_objects(...)`, optional bounded `to_pandas(...)`, `to_arrow(...)`,
  `to_arrow_table(...)`, `to_arrow_ipc(...)`, and `to_numpy(...)`.

The optional pandas, PyArrow, and NumPy materialization helpers are decoded result containers, not
execution engines.

## Python Expressions

Expression helpers are scoped SQL/Python front-door builders:

- Column references: `sl.col(...)`, `sl.column(...)`, `sl.outer(...)`.
- Comparison and boolean composition through Python operators, `&`, `|`, and `~`.
- Null and membership: `is_null()`, `is_not_null()`, `is_distinct_from(...)`,
  `is_not_distinct_from(...)`, `isin(...)`, `not_in(...)`, `between(...)`.
- Source subqueries: `isin_source(...)`, `not_in_source(...)`, `any_source(...)`,
  `all_source(...)`, `exists_source(...)`, `not_exists_source(...)`, `row_in(...)`,
  `row_not_in(...)`, `row_in_source(...)`, `row_not_in_source(...)`.
- Strings and binary helpers: `contains(...)`, `startswith(...)`, `endswith(...)`,
  `like(...)`, `rlike(...)`, `lower()`, `upper()`, `trim()`, `length()`, `concat(...)`,
  `substr(...)`, `substring(...)`, `left(...)`, `right(...)`, `replace(...)`, `unhex(...)`,
  `from_base64(...)`, `byte_length(...)`.
- Numeric, temporal, and casts: `abs(...)`, `floor(...)`, `ceil(...)`, `round(...)`,
  `cast(...)`, `try_cast(...)`, `interval_days(...)`, `interval_hours(...)`,
  `interval_minutes(...)`, `interval_seconds(...)`, date and timestamp add/diff/extract helpers.
- Projection helpers: `case_when(...)`, `count_distinct(...)`, `null_if(...)`, `array(...)`,
  `struct(...)`.
- Scoped ranking windows: `row_number(...)`, `rank(...)`, `dense_rank(...)`.

Unsupported expression forms must return deterministic blockers or raise local validation errors
before execution.

## Generated And Source-Free Inputs

Source-free helpers are ShardLoom-generated inputs, not external-engine shortcuts:

- `ctx.from_rows(...)` / `sl.from_rows(...)`
- `ctx.literal_table(...)` / `sl.literal_table(...)`
- `ctx.range(...)` / `sl.range(...)`
- `ctx.sequence(...)` / `sl.sequence(...)`
- `ctx.calendar(...)` / `sl.calendar(...)`
- `ctx.sql_values(...)` / `sl.sql_values(...)`
- `ctx.sql_literal_select(...)` / `sl.sql_literal_select(...)`
- `sl.dataframe_source_free_projection(...)`
- `sl.dataframe_generated_with_column(...)`

They can write local JSONL/CSV by default and feature-gated structured outputs when the build
admits the sink. They do not read an input dataset.

## SQL Surface

SQL is a frontend into ShardLoom planning and execution. It is not DataFusion, DuckDB, Spark,
pandas, Polars, or another external execution engine.

Entry points:

- `ctx.sql("SELECT ...")`
- `sl.sql("SELECT ...")`
- `ShardLoomClient.sql_local_source_smoke(...)` for lower-level CLI-backed proof.
- `shardloom sql-local-source-smoke ... --format json` for CLI smoke execution.
- `shardloom generated-source-sql-smoke ... --format json` for source-free SQL writes.

Admitted forms include scoped local-source `SELECT` over local file references, scoped projection,
filter, group-by, having, order, limit, joins, set operations, bounded subquery predicates, source
free `VALUES`, source-free literal `SELECT`, and generated range forms such as `generate_series`
or `range` where the local runtime admits them.

Not claimed in v0.1.0: arbitrary ANSI SQL, recursive CTEs, arbitrary dialect functions, arbitrary
subqueries, broad optimizer parity, SQL UDFs, catalog-backed SQL, object-store/table SQL, JDBC/ODBC,
Flight SQL, or SQL execution delegated to another engine.

## CLI Surface

The CLI is scriptable and agent-readable. Use JSON whenever automation is involved.

Primary discovery commands:

```sh
shardloom --version
shardloom command-metadata --format json
shardloom command-metadata <command> --format json
shardloom help <command> --format json
shardloom agent-contract-pack --format json
shardloom capabilities api-surfaces --format json
```

High-level public workflow facade commands:

```sh
shardloom route <sql|python|dataframe|cli> --format json
shardloom run <sql|python|dataframe|cli> --format json
shardloom prepare <sql|python|dataframe|cli> --format json
```

Common report and safety commands:

```sh
shardloom --version --format json
shardloom status --format json
shardloom runs-today --format json
shardloom doctor --format json
shardloom support-bundle --format json
shardloom explain <operation> --format json
shardloom estimate <operation> --format json
shardloom workflow-unsupported-plan <operation> --format json
```

Common executable local proof commands:

```sh
shardloom generated-source-user-rows-smoke --format json
shardloom generated-source-range-smoke --format json
shardloom generated-source-sequence-smoke --format json
shardloom generated-source-sql-smoke --format json
shardloom sql-local-source-smoke --format json
shardloom vortex-ingest-smoke --format json
shardloom vortex-production-runtime-run <scenario> <fact.vortex> <dim.vortex> --format json
shardloom sqlite-local-import-export-smoke --format json
```

The exhaustive command inventory, support state, side-effect level, input/output contract, feature
gate status, owning phase item, and claim/fallback boundary are generated by
`shardloom command-metadata --format json`.

## Explicit Blockers And Non-Claims

These surfaces can be visible in Python or CLI metadata but remain blocked, report-only, or
future-gated unless the dynamic capability row says otherwise:

- Broad pandas/Polars/PySpark/DataFrame parity.
- Broad SQL grammar or arbitrary SQL execution.
- Hidden fallback execution in DuckDB, DataFusion, Spark, Polars, pandas, Velox, or another engine.
- Unbounded materialization as a convenience path.
- Object-store, lakehouse/table, catalog, remote API, Foundry, live/hybrid, distributed, and
  production workflows without the matching evidence gate.
- Effectful external writes, credentials, network calls, UDFs, plugins, LLM/API calls, embeddings,
  vector search, media/OCR extraction, or arbitrary database connectors without explicit effect
  admission and certificates.

## Related References

- `docs/architecture/agent-contract-pack.md`
- `docs/architecture/v1-front-door-runtime-scope.md`
- `docs/architecture/v1-source-prepared-state-scope.md`
- `docs/architecture/v1-local-output-sink-scope.md`
- `docs/architecture/v1-vortex-runtime-scope.md`
- `docs/status/cli-command-registry.md`
- `docs/getting-started/examples.md`
- `python/README.md`
- `README.md`
