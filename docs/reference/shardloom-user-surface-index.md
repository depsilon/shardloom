<!-- SPDX-License-Identifier: Apache-2.0 -->

# ShardLoom User Surface Index

Schema marker: `shardloom.user_surface_index.v1`.

This is the **development-source application reference**. The
[published guide](https://shardloom.io/field-guide/start-local-proof/) names the
artifact it describes. This index records current source; additions made after a
release do not change that release's immutable packages. The native publication
metadata in the JSON reference identifies the release and proof it describes.
For deliberate behavior changes, read the [Python migration guide](python-api-migration.md).
For the subsequent source implementation, read the
[connected SQL/DataFrame and local-client guide](connected-programming.md)
and [shared scalar definitions](native-scalar-functions.md).

## Source Of Truth Order

Start with the [task guides](https://shardloom.io/field-guide/). This index projects
Python names from `python/src/shardloom/_exports.py` and command facts from the
executable registry. The [JSON reference](shardloom-user-surface-index.json)
records their source identities, audiences and exact callable signatures.
Use `shardloom --version` to identify an installed engine and
`shardloom help COMMAND` for its operation contract.

## Normal Python Entry Points

Use `sl.context(...)` with explicit resources, then `ctx.read(path)` or
`ctx.sql("SELECT ...")`. Constructing a context or a fluent file plan does not
execute the query. Native input admission such as `from_rows` can validate and
allocate the declared source; execution starts at collect, batch demand or write.

The context chooses its backend once. Native Linux/macOS installations select
in-process execution; Windows and explicit CLI configuration select process
isolation. `transport="subprocess"` remains an explicit choice. Resource grants
are independent of that choice. Execution failure never changes the backend.

`collect()` returns `Result`: inspect `schema`, explicitly allocate Python
values with `to_rows()`, or inspect its typed `details`. Use
`ctx.from_result(result)` to chain retained native data without an Arrow or
Python-row conversion. `iter_batches()` supports close/cancel and reports final
completion only after full drain. Writes return the actual writer receipt.

## Generated application reference

<!-- BEGIN GENERATED APPLICATION REFERENCE -->
Source identity: `4029b12a60b59488f8cd9a2449d5d29cf523802e493de19290f80b397d185118` (SHA-256 of the listed source inputs).
This reference describes the listed source inputs; published package availability is tracked in the [selected-channel package readiness matrix](../release/package-channel-readiness-matrix.md).

### Python application exports

Names and signatures below come from the actual lazy export authority.
Method contracts are available on the linked classes; normal use starts with `context`.

| Export | Kind | Contract |
| --- | --- | --- |
| [`ColumnExpression`](../../python/src/shardloom/query.py) | class | A scoped column expression for Python query-builder predicates. |
| [`ComplexProjectionExpression`](../../python/src/shardloom/query.py) | class | A scoped ARRAY or STRUCT projection expression for local-source rows. |
| [`Diagnostic`](../../python/src/shardloom/models.py) | class | Diagnostic entry copied from a ShardLoom output envelope. |
| [`ExecutionResourceLimits`](../../python/src/shardloom/execution_resources.py) | class | Explicit deployment ceilings; this object does not authenticate their source. |
| [`ExecutionResources`](../../python/src/shardloom/execution_resources.py) | class | Immutable exact-byte allocation, independent of reservation or measured use. |
| [`GroupedLazyFrame`](../../python/src/shardloom/query.py) | class | Grouped lazy workflow handle for scoped aggregation and blockers. |
| [`IntervalLiteral`](../../python/src/shardloom/query.py) | class | A scoped ANSI interval literal for admitted temporal helper functions. |
| [`LazyFrame`](../../python/src/shardloom/query.py) | class | A lazy ShardLoom workflow plan. |
| [`PredicateExpression`](../../python/src/shardloom/query.py) | class | A scoped SQL predicate expression for ShardLoom local-source smokes. |
| [`Result`](../../python/src/shardloom/query.py) | class | Completed data and its existing native owner, or a completed write receipt. |
| [`ResultBatch`](../../python/src/shardloom/_batches.py) | class | One provisional batch; success is established by the iterator's final report. |
| [`ResultBatchIterator`](../../python/src/shardloom/_batches.py) | class | One native transaction with explicit backpressure and deterministic close. |
| [`ResultType`](../../python/src/shardloom/_result_schema.py) | class | ResultType(name: 'str', nullable: 'bool', parameters: 'tuple[Any, ...]' = ()) |
| [`ShardLoomBinaryNotFoundError`](../../python/src/shardloom/errors.py) | class | Raised when an explicit ShardLoom CLI command cannot resolve a binary. |
| [`ShardLoomClosedError`](../../python/src/shardloom/errors.py) | class | New work was requested after its caller-owned context was closed. |
| [`ShardLoomCommandError`](../../python/src/shardloom/errors.py) | class | Raised when an explicit ShardLoom CLI command returns an error envelope. |
| [`ShardLoomContext`](../../python/src/shardloom/context.py) | class | High-level Python context for side-effect-free discovery and explicit work. |
| [`ShardLoomError`](../../python/src/shardloom/errors.py) | class | Common base for ShardLoom configuration, lifecycle and execution errors. |
| [`ShardLoomInstallationError`](../../python/src/shardloom/errors.py) | class | The selected installation/backend cannot provide native execution. |
| [`ShardLoomProtocolError`](../../python/src/shardloom/errors.py) | class | Raised when CLI output does not match the expected JSON protocol. |
| [`ShardLoomResourceConfigurationError`](../../python/src/shardloom/execution_resources.py) | class | Invalid or incomplete allocation, rejected before any input is inspected. |
| [`ShardLoomTimeoutError`](../../python/src/shardloom/errors.py) | class | An execution deadline expired, preserving both historical timeout catches. |
| [`Window`](../../python/src/shardloom/_window.py) | class | Factories for WindowSpec, with explicit signed offsets and unbounded ends. |
| [`WindowExpression`](../../python/src/shardloom/query.py) | class | A SQL window declaration submitted to the shared native engine. |
| [`WindowSpec`](../../python/src/shardloom/_window.py) | class | Window partition/order/frame metadata, reusable across expressions. |
| [`__version__`](../../python/src/shardloom/_version.py) | constant | Package constant. |
| [`abs`](../../python/src/shardloom/query.py) | function | Return a scoped `ABS(column)` numeric absolute-value expression. |
| [`all_source`](../../python/src/shardloom/query.py) | function | Return a scoped bounded local-source `ALL (SELECT ...)` predicate. |
| [`any_source`](../../python/src/shardloom/query.py) | function | Return a scoped bounded local-source `ANY (SELECT ...)` predicate. |
| [`array`](../../python/src/shardloom/query.py) | function | Return a scoped `ARRAY[...]` projection over scalar SQL literals. |
| [`byte_length`](../../python/src/shardloom/query.py) | function | Return a scoped `BYTE_LENGTH(<binary-expression>)` byte-count expression. |
| [`calendar`](../../python/src/shardloom/query.py) | function | Create a scoped source-free calendar/date dimension for local output. |
| [`case_when`](../../python/src/shardloom/query.py) | function | Return a scoped single-branch `CASE WHEN` computed-column expression. |
| [`ceil`](../../python/src/shardloom/query.py) | function | Return a scoped `CEIL(column)` numeric rounding expression. |
| [`col`](../../python/src/shardloom/query.py) | function | Return a scoped column expression for local ShardLoom predicates. |
| [`column`](../../python/src/shardloom/query.py) | function | Alias for `col(...)`. |
| [`column_transform`](../../python/src/shardloom/query.py) | function | Declare scoped column rewrites for `map`, `applymap`, or `transform`. |
| [`concat`](../../python/src/shardloom/query.py) | function | Return a scoped `CONCAT(column-or-string-literal, ...)` expression. |
| [`context`](../../python/src/shardloom/context.py) | function | Return a side-effect-free ShardLoom context. |
| [`count_distinct`](../../python/src/shardloom/query.py) | function | Declare exact COUNT DISTINCT through the shared native aggregate binder. |
| [`dense_rank`](../../python/src/shardloom/query.py) | function | Return a scoped `DENSE_RANK() OVER (...) AS alias` expression. |
| [`exists_source`](../../python/src/shardloom/query.py) | function | Return a scoped local-source `EXISTS (SELECT ... FROM ...)` predicate. |
| [`floor`](../../python/src/shardloom/query.py) | function | Return a scoped `FLOOR(column)` numeric rounding expression. |
| [`from_arrow_ipc`](../../python/src/shardloom/query.py) | function | Create a scoped generated-row source from an Arrow IPC stream/file. |
| [`from_arrow_table`](../../python/src/shardloom/query.py) | function | Create a source from an Arrow-compatible input or retained native result. |
| [`from_base64`](../../python/src/shardloom/query.py) | function | Return a scoped `FROM_BASE64(column)` binary helper projection expression. |
| [`from_batches`](../../python/src/shardloom/query.py) | function | Declare native input in batches of up to 2,048 rows. |
| [`from_pandas`](../../python/src/shardloom/query.py) | function | Create a scoped generated-row source from a pandas DataFrame-like object. |
| [`from_result`](../../python/src/shardloom/query.py) | function | Admit completed ShardLoom data without an Arrow or row round trip in process. |
| [`from_rows`](../../python/src/shardloom/query.py) | function | Declare native rows; use an explicit schema for rich types or empty columns. |
| [`from_vortex_ipc`](../../python/src/shardloom/query.py) | function | Import one IPC record for the pinned Vortex provider using native transport. |
| [`interval_days`](../../python/src/shardloom/query.py) | function | Return a scoped `INTERVAL '<n>' DAY` literal. |
| [`interval_hours`](../../python/src/shardloom/query.py) | function | Return a scoped `INTERVAL '<n>' HOUR` literal. |
| [`interval_minutes`](../../python/src/shardloom/query.py) | function | Return a scoped `INTERVAL '<n>' MINUTE` literal. |
| [`interval_seconds`](../../python/src/shardloom/query.py) | function | Return a scoped `INTERVAL '<n>' SECOND` literal. |
| [`left`](../../python/src/shardloom/query.py) | function | Return a scoped `LEFT(column, count)` UTF-8 expression. |
| [`length`](../../python/src/shardloom/query.py) | function | Return a scoped `LENGTH(column)` UTF-8 length expression. |
| [`literal_table`](../../python/src/shardloom/query.py) | function | Declare a literal table for the shared native engine. |
| [`not_exists_source`](../../python/src/shardloom/query.py) | function | Return a scoped local-source `NOT EXISTS (SELECT ... FROM ...)` predicate. |
| [`null_if`](../../python/src/shardloom/query.py) | function | Return a scoped `NULLIF(column, literal)` computed-column expression. |
| [`outer`](../../python/src/shardloom/query.py) | function | Return the reserved outer-row column expression for correlated source predicates. |
| [`plan_transform`](../../python/src/shardloom/query.py) | function | Declare a workflow-level transform admitted by `LazyFrame.pipe(...)`. |
| [`range`](../../python/src/shardloom/query.py) | function | Declare a native integer input with an exclusive end for the shared engine. |
| [`rank`](../../python/src/shardloom/query.py) | function | Return a scoped `RANK() OVER (...) AS alias` expression. |
| [`read`](../../python/src/shardloom/query.py) | function | Declare a lazy local source by inferring the adapter from the path extension. |
| [`read_arrow_ipc`](../../python/src/shardloom/query.py) | function | Declare a lazy Arrow IPC file source without loading data. |
| [`read_avro`](../../python/src/shardloom/query.py) | function | Declare a lazy Avro source without loading data. |
| [`read_csv`](../../python/src/shardloom/query.py) | function | Declare a lazy CSV source; no source data is loaded by this call. |
| [`read_json`](../../python/src/shardloom/query.py) | function | Declare a lazy flat JSON, JSONL, or NDJSON source without loading data. |
| [`read_orc`](../../python/src/shardloom/query.py) | function | Declare a lazy ORC source without loading data. |
| [`read_parquet`](../../python/src/shardloom/query.py) | function | Declare a lazy Parquet source without loading data. |
| [`read_vortex`](../../python/src/shardloom/query.py) | function | Declare a lazy native Vortex source. |
| [`replace`](../../python/src/shardloom/query.py) | function | Return a scoped `REPLACE(column, needle, replacement)` expression. |
| [`right`](../../python/src/shardloom/query.py) | function | Return a scoped `RIGHT(column, count)` UTF-8 expression. |
| [`round`](../../python/src/shardloom/query.py) | function | Return a scoped `ROUND(column)` numeric rounding expression. |
| [`row_in`](../../python/src/shardloom/query.py) | function | Return a scoped bounded row-value `IN ((...),...)` predicate. |
| [`row_in_source`](../../python/src/shardloom/query.py) | function | Return a scoped bounded row-value local-source `IN (SELECT ...)` predicate. |
| [`row_not_in`](../../python/src/shardloom/query.py) | function | Return a scoped bounded row-value `NOT IN ((...),...)` predicate. |
| [`row_not_in_source`](../../python/src/shardloom/query.py) | function | Return a scoped bounded row-value local-source `NOT IN (SELECT ...)` predicate. |
| [`row_number`](../../python/src/shardloom/query.py) | function | Return a scoped `ROW_NUMBER() OVER (...) AS alias` expression. |
| [`row_transform`](../../python/src/shardloom/query.py) | function | Declare scoped row-shaped rewrites for `map_rows`. |
| [`scalar_subquery`](../../python/src/shardloom/query.py) | function | Declare a scalar query expression without executing its source. |
| [`sequence`](../../python/src/shardloom/query.py) | function | Alias for the native integer range, including its exclusive end. |
| [`sql`](../../python/src/shardloom/query.py) | function | Create a scoped SQL workflow over currently admitted ShardLoom SQL paths. |
| [`struct`](../../python/src/shardloom/query.py) | function | Return a scoped `STRUCT(...)` projection over source columns. |
| [`substr`](../../python/src/shardloom/query.py) | function | Return a scoped 1-based `SUBSTR(column, start, length)` expression. |
| [`substring`](../../python/src/shardloom/query.py) | function | Alias for `substr(...)`. |
| [`try_cast`](../../python/src/shardloom/query.py) | function | Return a scoped `TRY_CAST(column AS dtype)` dirty-value expression. |
| [`typed_scalar_udf`](../../python/src/shardloom/query.py) | function | Return an admitted typed deterministic scalar UDF expression. |
| [`unhex`](../../python/src/shardloom/query.py) | function | Return a scoped `UNHEX(column)` binary helper projection expression. |
| [`functions`](../../python/src/shardloom/functions.py) | module | Native expression declarations; Python never evaluates data operations. |
| [`shardloom.functions.grouping`](../../python/src/shardloom/functions.py) | function | Declare the UInt64 grouping omission mask, with the last key in the low bit. |
| [`shardloom.functions.grouping_id`](../../python/src/shardloom/functions.py) | function | Alias for the same grouping omission mask. |
| [`shardloom.functions.abs`](../../python/src/shardloom/functions.py) | function | Declare checked absolute value over a column or expression. |
| [`shardloom.functions.all_source`](../../python/src/shardloom/query.py) | function | Return a scoped bounded local-source `ALL (SELECT ...)` predicate. |
| [`shardloom.functions.any_source`](../../python/src/shardloom/query.py) | function | Return a scoped bounded local-source `ANY (SELECT ...)` predicate. |
| [`shardloom.functions.array`](../../python/src/shardloom/query.py) | function | Return a scoped `ARRAY[...]` projection over scalar SQL literals. |
| [`shardloom.functions.array_agg`](../../python/src/shardloom/functions.py) | function | Collect original typed values, including NULLs and duplicates; an empty domain yields NULL. |
| [`shardloom.functions.avg`](../../python/src/shardloom/functions.py) | function | Declare native AVG, ignoring nulls and optionally using exact distinct membership. |
| [`shardloom.functions.byte_length`](../../python/src/shardloom/functions.py) | function | Count bytes in a UTF8 or binary column or expression. |
| [`shardloom.functions.case_when`](../../python/src/shardloom/query.py) | function | Return a scoped single-branch `CASE WHEN` computed-column expression. |
| [`shardloom.functions.cast`](../../python/src/shardloom/functions.py) | function | Declare a checked native cast to a supported type. |
| [`shardloom.functions.catalog`](../../python/src/shardloom/functions.py) | function | Inspect native scalar names, aliases, argument counts and type/null rules. |
| [`shardloom.functions.ceil`](../../python/src/shardloom/functions.py) | function | Declare rounding toward positive infinity. |
| [`shardloom.functions.coalesce`](../../python/src/shardloom/functions.py) | function | Declare the first non-null expression; use lit() for scalar constants. |
| [`shardloom.functions.col`](../../python/src/shardloom/query.py) | function | Return a scoped column expression for local ShardLoom predicates. |
| [`shardloom.functions.column`](../../python/src/shardloom/query.py) | function | Alias for `col(...)`. |
| [`shardloom.functions.concat`](../../python/src/shardloom/functions.py) | function | Concatenate columns or expressions; use lit() for string constants. |
| [`shardloom.functions.contains`](../../python/src/shardloom/functions.py) | function | Match a literal UTF8 substring, including percent and underscore characters. |
| [`shardloom.functions.count`](../../python/src/shardloom/functions.py) | function | Count all rows with no argument; otherwise count non-null values. |
| [`shardloom.functions.count_distinct`](../../python/src/shardloom/query.py) | function | Declare exact COUNT DISTINCT through the shared native aggregate binder. |
| [`shardloom.functions.date_add_days`](../../python/src/shardloom/functions.py) | function | Return a scoped Date32 day-add expression. |
| [`shardloom.functions.date_day`](../../python/src/shardloom/functions.py) | function | Return a scoped Date32 day-of-month extract expression. |
| [`shardloom.functions.date_diff_days`](../../python/src/shardloom/functions.py) | function | Return a scoped Date32 day-difference expression. |
| [`shardloom.functions.date_month`](../../python/src/shardloom/functions.py) | function | Return a scoped Date32 month-extract expression. |
| [`shardloom.functions.date_sub_days`](../../python/src/shardloom/functions.py) | function | Return a scoped Date32 day-subtract expression. |
| [`shardloom.functions.date_trunc_minute`](../../python/src/shardloom/functions.py) | function | Truncate integer epoch seconds or timestamp text to the minute. |
| [`shardloom.functions.date_year`](../../python/src/shardloom/functions.py) | function | Return a scoped Date32 year-extract expression. |
| [`shardloom.functions.dense_rank`](../../python/src/shardloom/query.py) | function | Return a scoped `DENSE_RANK() OVER (...) AS alias` expression. |
| [`shardloom.functions.ends_with`](../../python/src/shardloom/functions.py) | function | Match a literal UTF8 suffix. |
| [`shardloom.functions.arg_max`](../../python/src/shardloom/functions.py) | function | Select the value at the maximum key, skipping NULL pairs; ties use declared then input order. |
| [`shardloom.functions.arg_min`](../../python/src/shardloom/functions.py) | function | Select the value at the minimum key, skipping NULL pairs; ties use declared then input order. |
| [`shardloom.functions.corr`](../../python/src/shardloom/functions.py) | function | Declare correlation for primitive numeric pairs, dropping rows where either input is null; zero variance yields null. |
| [`shardloom.functions.covar_pop`](../../python/src/shardloom/functions.py) | function | Declare population covariance for primitive numeric pairs, dropping rows where either input is null; a singleton yields zero. |
| [`shardloom.functions.covar_samp`](../../python/src/shardloom/functions.py) | function | Declare sample covariance for primitive numeric pairs, dropping rows where either input is null; fewer than two yields null. |
| [`shardloom.functions.exists_source`](../../python/src/shardloom/query.py) | function | Return a scoped local-source `EXISTS (SELECT ... FROM ...)` predicate. |
| [`shardloom.functions.extract_minute`](../../python/src/shardloom/functions.py) | function | Extract a minute from integer epoch seconds or timestamp text. |
| [`shardloom.functions.first`](../../python/src/shardloom/functions.py) | function | Select the first value, retaining NULL; equal order keys follow input order. |
| [`shardloom.functions.first_value`](../../python/src/shardloom/functions.py) | function | Select the first value of the frame supplied with over(). |
| [`shardloom.functions.floor`](../../python/src/shardloom/functions.py) | function | Declare rounding toward negative infinity. |
| [`shardloom.functions.from_base64`](../../python/src/shardloom/functions.py) | function | Declare Base64 text decoding to binary. |
| [`shardloom.functions.interval_days`](../../python/src/shardloom/query.py) | function | Return a scoped `INTERVAL '<n>' DAY` literal. |
| [`shardloom.functions.interval_hours`](../../python/src/shardloom/query.py) | function | Return a scoped `INTERVAL '<n>' HOUR` literal. |
| [`shardloom.functions.interval_minutes`](../../python/src/shardloom/query.py) | function | Return a scoped `INTERVAL '<n>' MINUTE` literal. |
| [`shardloom.functions.interval_seconds`](../../python/src/shardloom/query.py) | function | Return a scoped `INTERVAL '<n>' SECOND` literal. |
| [`shardloom.functions.is_null`](../../python/src/shardloom/functions.py) | function | Declare a boolean null check. |
| [`shardloom.functions.is_not_null`](../../python/src/shardloom/functions.py) | function | Declare a boolean non-null check. |
| [`shardloom.functions.json_extract`](../../python/src/shardloom/functions.py) | function | Extract JSON using a native literal path. |
| [`shardloom.functions.lag`](../../python/src/shardloom/functions.py) | function | Select an earlier ordered value; admitted offsets are 1 through 50000. |
| [`shardloom.functions.last_value`](../../python/src/shardloom/functions.py) | function | Select the last value of the frame supplied with over(). |
| [`shardloom.functions.lead`](../../python/src/shardloom/functions.py) | function | Select a later ordered value; admitted offsets are 1 through 50000. |
| [`shardloom.functions.left`](../../python/src/shardloom/functions.py) | function | Take a UTF8 prefix by code-point count. |
| [`shardloom.functions.length`](../../python/src/shardloom/functions.py) | function | Count Unicode code points in a column or expression. |
| [`shardloom.functions.list`](../../python/src/shardloom/functions.py) | function | Collect original typed values, including NULLs and duplicates; an empty domain yields NULL. |
| [`shardloom.functions.lit`](../../python/src/shardloom/functions.py) | function | Declare a scalar literal, including null, decimal and naive microsecond datetime values. |
| [`shardloom.functions.lower`](../../python/src/shardloom/functions.py) | function | Declare native lowercase conversion. |
| [`shardloom.functions.last`](../../python/src/shardloom/functions.py) | function | Select the last value, retaining NULL; equal order keys follow input order. |
| [`shardloom.functions.max`](../../python/src/shardloom/functions.py) | function | Declare native MAX over non-null values. |
| [`shardloom.functions.mean`](../../python/src/shardloom/functions.py) | function | Declare native AVG, ignoring nulls and optionally using exact distinct membership. |
| [`shardloom.functions.mean_distinct`](../../python/src/shardloom/functions.py) | function | Declare native AVG over distinct non-null values. |
| [`shardloom.functions.median`](../../python/src/shardloom/functions.py) | function | Declare continuous median (quantile 0.5) over non-null values. |
| [`shardloom.functions.min`](../../python/src/shardloom/functions.py) | function | Declare native MIN over non-null values. |
| [`shardloom.functions.not_exists_source`](../../python/src/shardloom/query.py) | function | Return a scoped local-source `NOT EXISTS (SELECT ... FROM ...)` predicate. |
| [`shardloom.functions.not_regex_match`](../../python/src/shardloom/functions.py) | function | Return a scoped UTF-8 regex negation. |
| [`shardloom.functions.nth_value`](../../python/src/shardloom/functions.py) | function | Select a value by its one-based position in the frame supplied with over(). |
| [`shardloom.functions.null_if`](../../python/src/shardloom/functions.py) | function | Return null when a column/expression equals another expression or literal. |
| [`shardloom.functions.outer`](../../python/src/shardloom/query.py) | function | Return the reserved outer-row column expression for correlated source predicates. |
| [`shardloom.functions.rank`](../../python/src/shardloom/query.py) | function | Return a scoped `RANK() OVER (...) AS alias` expression. |
| [`shardloom.functions.regex_domain`](../../python/src/shardloom/functions.py) | function | Apply the fixed HTTP(S) domain-replacement expression. |
| [`shardloom.functions.regex_match`](../../python/src/shardloom/functions.py) | function | Match UTF8 with a literal native regular expression. |
| [`shardloom.functions.regexp_replace`](../../python/src/shardloom/functions.py) | function | Apply the admitted fixed HTTP(S) domain pattern and capture replacement. |
| [`shardloom.functions.replace`](../../python/src/shardloom/functions.py) | function | Replace literal substrings in a column or expression. |
| [`shardloom.functions.right`](../../python/src/shardloom/functions.py) | function | Take a UTF8 suffix by code-point count. |
| [`shardloom.functions.round`](../../python/src/shardloom/functions.py) | function | Declare integer rounding with halfway values away from zero. |
| [`shardloom.functions.row_in`](../../python/src/shardloom/query.py) | function | Return a scoped bounded row-value `IN ((...),...)` predicate. |
| [`shardloom.functions.row_in_source`](../../python/src/shardloom/query.py) | function | Return a scoped bounded row-value local-source `IN (SELECT ...)` predicate. |
| [`shardloom.functions.row_not_in`](../../python/src/shardloom/query.py) | function | Return a scoped bounded row-value `NOT IN ((...),...)` predicate. |
| [`shardloom.functions.row_not_in_source`](../../python/src/shardloom/query.py) | function | Return a scoped bounded row-value local-source `NOT IN (SELECT ...)` predicate. |
| [`shardloom.functions.row_number`](../../python/src/shardloom/query.py) | function | Return a scoped `ROW_NUMBER() OVER (...) AS alias` expression. |
| [`shardloom.functions.scalar_subquery`](../../python/src/shardloom/query.py) | function | Declare a scalar query expression without executing its source. |
| [`shardloom.functions.starts_with`](../../python/src/shardloom/functions.py) | function | Match a literal UTF8 prefix. |
| [`shardloom.functions.std`](../../python/src/shardloom/functions.py) | function | Declare sample standard deviation over primitive numeric values, ignoring nulls; fewer than two yields null. |
| [`shardloom.functions.stddev`](../../python/src/shardloom/functions.py) | function | Declare sample standard deviation over primitive numeric values, ignoring nulls; fewer than two yields null. |
| [`shardloom.functions.stddev_pop`](../../python/src/shardloom/functions.py) | function | Declare population standard deviation over primitive numeric values, ignoring nulls; a singleton yields zero. |
| [`shardloom.functions.stddev_samp`](../../python/src/shardloom/functions.py) | function | Declare sample standard deviation over primitive numeric values, ignoring nulls; fewer than two yields null. |
| [`shardloom.functions.string_agg`](../../python/src/shardloom/functions.py) | function | Join non-NULL UTF-8 values with a literal separator; an empty domain yields NULL. |
| [`shardloom.functions.strptime`](../../python/src/shardloom/functions.py) | function | Parse timestamp text using a native literal format. |
| [`shardloom.functions.struct`](../../python/src/shardloom/query.py) | function | Return a scoped `STRUCT(...)` projection over source columns. |
| [`shardloom.functions.substr`](../../python/src/shardloom/functions.py) | function | Take a Unicode substring with SQL start/length semantics. |
| [`shardloom.functions.substring`](../../python/src/shardloom/functions.py) | function | Take a Unicode substring with SQL start/length semantics. |
| [`shardloom.functions.sum`](../../python/src/shardloom/functions.py) | function | Declare native SUM, ignoring nulls and optionally using exact distinct membership. |
| [`shardloom.functions.timestamp_add_seconds`](../../python/src/shardloom/functions.py) | function | Return a scoped timestamp second-add expression. |
| [`shardloom.functions.timestamp_day`](../../python/src/shardloom/functions.py) | function | Return a scoped timestamp day-of-month extract expression. |
| [`shardloom.functions.timestamp_diff_seconds`](../../python/src/shardloom/functions.py) | function | Return a scoped timestamp second-difference expression. |
| [`shardloom.functions.timestamp_hour`](../../python/src/shardloom/functions.py) | function | Return a scoped timestamp hour-extract expression. |
| [`shardloom.functions.timestamp_minute`](../../python/src/shardloom/functions.py) | function | Return a scoped timestamp minute-extract expression. |
| [`shardloom.functions.timestamp_month`](../../python/src/shardloom/functions.py) | function | Return a scoped timestamp month-extract expression. |
| [`shardloom.functions.quantile_cont`](../../python/src/shardloom/functions.py) | function | Declare continuous quantile with linear interpolation over ordered non-null values. |
| [`shardloom.functions.quantile_disc`](../../python/src/shardloom/functions.py) | function | Declare discrete quantile returning an original typed selected value. |
| [`shardloom.functions.timestamp_second`](../../python/src/shardloom/functions.py) | function | Return a scoped timestamp second-extract expression. |
| [`shardloom.functions.timestamp_sub_seconds`](../../python/src/shardloom/functions.py) | function | Return a scoped timestamp second-subtract expression. |
| [`shardloom.functions.timestamp_year`](../../python/src/shardloom/functions.py) | function | Return a scoped timestamp year-extract expression. |
| [`shardloom.functions.try_cast`](../../python/src/shardloom/functions.py) | function | Declare native conversion that maps malformed values to null. |
| [`shardloom.functions.try_strptime`](../../python/src/shardloom/functions.py) | function | Parse timestamp text with a native literal format, yielding null on parse failure. |
| [`shardloom.functions.sum_distinct`](../../python/src/shardloom/functions.py) | function | Declare native SUM over distinct non-null values. |
| [`shardloom.functions.avg_distinct`](../../python/src/shardloom/functions.py) | function | Declare native AVG over distinct non-null values. |
| [`shardloom.functions.trim`](../../python/src/shardloom/functions.py) | function | Declare native whitespace trimming. |
| [`shardloom.functions.typed_scalar_udf`](../../python/src/shardloom/query.py) | function | Return an admitted typed deterministic scalar UDF expression. |
| [`shardloom.functions.unhex`](../../python/src/shardloom/functions.py) | function | Declare hexadecimal text decoding to binary. |
| [`shardloom.functions.upper`](../../python/src/shardloom/functions.py) | function | Declare native uppercase conversion. |
| [`shardloom.functions.url_domain`](../../python/src/shardloom/functions.py) | function | Return the native URL-domain extraction expression. |
| [`shardloom.functions.var`](../../python/src/shardloom/functions.py) | function | Declare sample variance over primitive numeric values, ignoring nulls; fewer than two yields null. |
| [`shardloom.functions.var_pop`](../../python/src/shardloom/functions.py) | function | Declare population variance over primitive numeric values, ignoring nulls; a singleton yields zero. |
| [`shardloom.functions.var_samp`](../../python/src/shardloom/functions.py) | function | Declare sample variance over primitive numeric values, ignoring nulls; fewer than two yields null. |
| [`shardloom.functions.variance`](../../python/src/shardloom/functions.py) | function | Declare sample variance over primitive numeric values, ignoring nulls; fewer than two yields null. |
| [`types`](../../python/src/shardloom/types.py) | module | Metadata-only constructors for declared ShardLoom schema types. |
| [`shardloom.types.ResultType`](../../python/src/shardloom/_result_schema.py) | class | ResultType(name: 'str', nullable: 'bool', parameters: 'tuple[Any, ...]' = ()) |
| [`shardloom.types.int8`](../../python/src/shardloom/types.py) | function | Declare int8 metadata for the native binder. |
| [`shardloom.types.int16`](../../python/src/shardloom/types.py) | function | Declare int16 metadata for the native binder. |
| [`shardloom.types.int32`](../../python/src/shardloom/types.py) | function | Declare int32 metadata for the native binder. |
| [`shardloom.types.int64`](../../python/src/shardloom/types.py) | function | Declare int64 metadata for the native binder. |
| [`shardloom.types.uint8`](../../python/src/shardloom/types.py) | function | Declare uint8 metadata for the native binder. |
| [`shardloom.types.uint16`](../../python/src/shardloom/types.py) | function | Declare uint16 metadata for the native binder. |
| [`shardloom.types.uint32`](../../python/src/shardloom/types.py) | function | Declare uint32 metadata for the native binder. |
| [`shardloom.types.uint64`](../../python/src/shardloom/types.py) | function | Declare uint64 metadata for the native binder. |
| [`shardloom.types.float32`](../../python/src/shardloom/types.py) | function | Declare float32 metadata for the native binder. |
| [`shardloom.types.float64`](../../python/src/shardloom/types.py) | function | Declare float64 metadata for the native binder. |
| [`shardloom.types.boolean`](../../python/src/shardloom/types.py) | function | Declare boolean metadata for the native binder. |
| [`shardloom.types.string`](../../python/src/shardloom/types.py) | function | Declare string metadata for the native binder. |
| [`shardloom.types.binary`](../../python/src/shardloom/types.py) | function | Declare binary metadata for the native binder. |
| [`shardloom.types.date32`](../../python/src/shardloom/types.py) | function | Declare date32 metadata for the native binder. |
| [`shardloom.types.timestamp_micros`](../../python/src/shardloom/types.py) | function | Declare timestamp_micros metadata for the native binder. |
| [`shardloom.types.decimal128`](../../python/src/shardloom/types.py) | function | Declare decimal128 metadata for the native binder. |
| [`shardloom.types.list_`](../../python/src/shardloom/types.py) | function | Declare list_ metadata for the native binder. |
| [`shardloom.types.fixed_size_list`](../../python/src/shardloom/types.py) | function | Declare fixed_size_list metadata for the native binder. |
| [`shardloom.types.struct`](../../python/src/shardloom/types.py) | function | Declare struct metadata for the native binder. |
| [`shardloom.types.scalar`](../../python/src/shardloom/types.py) | function | Bind a scalar with an explicit native type, including typed nulls. |
| [`adbc`](../../python/src/shardloom/adbc.py) | module | Local ADBC 1.1 access to an explicitly selected ShardLoom context. |
| [`shardloom.adbc.database`](../../python/src/shardloom/adbc.py) | function | Open the independent ADBC manager's database handle on this context. |
| [`shardloom.adbc.driver_options`](../../python/src/shardloom/adbc.py) | function | Yield one-use options for an independent ADBC manager's database open. |
| [`dbapi`](../../python/src/shardloom/dbapi.py) | module | PEP 249 connection backed by the independent Apache ADBC driver manager. |
| [`shardloom.dbapi.connect`](../../python/src/shardloom/dbapi.py) | function | Borrow an existing context; close releases this connection's operations. |
| [`shardloom.dbapi.apilevel`](../../python/src/shardloom/dbapi.py) | constant | Package constant. |
| [`shardloom.dbapi.threadsafety`](../../python/src/shardloom/dbapi.py) | constant | Package constant. |
| [`shardloom.dbapi.paramstyle`](../../python/src/shardloom/dbapi.py) | constant | Package constant. |
| [`shardloom.dbapi.Warning`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.Error`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.InterfaceError`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.DatabaseError`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.DataError`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.OperationalError`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.IntegrityError`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.InternalError`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.ProgrammingError`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.NotSupportedError`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.Date`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.Time`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.Timestamp`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.DateFromTicks`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.TimeFromTicks`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.TimestampFromTicks`](../../python/src/shardloom/dbapi.py) | alias | PEP 249 helper or exception loaded from the optional ADBC manager. |
| [`shardloom.dbapi.Binary`](../../python/src/shardloom/dbapi.py) | class | bytes(iterable_of_ints) -> bytes |
| [`notebook`](../../python/src/shardloom/notebook.py) | module | Explicit-context notebook SQL that returns the same lazy DataFrame. |
| [`shardloom.notebook.install`](../../python/src/shardloom/notebook.py) | function | Register SQL magic on an existing context and return a removable handle. |

Exact callable signatures are generated in the [JSON reference](shardloom-user-surface-index.json)
and package type stubs. `shardloom.diagnostics` contains runtime inspection and compatibility
types; `shardloom.testing` contains maintainer fixtures and rehearsal reports.

### CLI application commands

The executable registry contains 203 commands:
14 application commands and 189 maintainer commands.
These counts describe discovery, not the number of implemented SQL operations.

| Command | Input contract | Output contract |
| --- | --- | --- |
| `help` | registry_or_capability_scope_args | typed_envelope_metadata_report_only |
| `command-metadata` | registry_or_capability_scope_args | typed_envelope_metadata_report_only |
| `status` | registry_or_capability_scope_args | typed_envelope_metadata_report_only |
| `run` | declared_public_workflow_request_with_route_admission | typed_runtime_envelope_with_attached_public_workflow_route |
| `prepare` | declared_public_workflow_request_with_route_admission | typed_runtime_envelope_with_attached_public_workflow_route |
| `route` | declared_public_workflow_route_request | typed_public_workflow_route_envelope_no_execution |
| `vortex-prepare` | local_source_or_vortex_artifact_args | typed_envelope_plus_local_runtime_or_artifact_evidence |
| `input-adapters` | declared_input_source_or_adapter_shape | typed_envelope_metadata_report_only |
| `input-plan` | declared_input_source_or_adapter_shape | typed_envelope_metadata_report_only |
| `capabilities` | registry_or_capability_scope_args | typed_envelope_metadata_report_only |
| `doctor` | diagnostic_or_estimate_args | typed_envelope_metadata_report_only |
| `support-bundle` | diagnostic_or_estimate_args | typed_envelope_metadata_report_only |
| `explain` | diagnostic_or_estimate_args | typed_envelope_metadata_report_only |
| `estimate` | diagnostic_or_estimate_args | typed_envelope_metadata_report_only |

```bash
shardloom help
shardloom command-metadata --audience application --format json
shardloom command-metadata --audience maintainer --format json
shardloom command-metadata --format json
shardloom command-metadata run --format json
shardloom command-metadata --completion bash
```

Completion also supports `zsh` and `fish`; its default vocabulary is application commands.
Existing JSON envelopes and the unfiltered metadata inventory remain available.
<!-- END GENERATED APPLICATION REFERENCE -->

<a id="python-reads"></a>
<a id="python-query-builder"></a>
<a id="python-expressions"></a>
<a id="generated-and-source-free-inputs"></a>

## DataFrame and SQL contracts

[Python/DataFrames](https://shardloom.io/field-guide/python-surface/) explains
readers, expressions, plan transformations and result consumption.
[SQL](https://shardloom.io/field-guide/sql/) explains the implemented query
language. The [types and formats guide](https://shardloom.io/field-guide/runtime-and-io/)
keeps exact input/output restrictions near their operations. These guides state
their applicable version.

<a id="optional-native-python-surface"></a>
<a id="resident-native-results"></a>
<a id="sql-surface"></a>
<a id="cli-surface"></a>

## Advanced integration and exact semantics

Direct `shardloom.native` and Rust embedding remain available through the
[integration guide](https://shardloom.io/field-guide/embedding/).
Existing focused references retain detailed contracts:
[SQL binding](../architecture/sql-parser-binder-readiness.md), [resident owners](resident-native-results.md),
[query spill](native-query-spill.md),
[distinct-set spill](../architecture/native-set-membership-spill-2026-10-18.md)
with [finite local acceptance](../benchmarks/native-set-membership-spill-2026-10-18.md),
[unary membership spill scope](../architecture/native-unary-membership-spill-2026-10-18.md)
with [finite local acceptance](../benchmarks/native-unary-membership-spill-2026-10-18.md),
[sampling population spill scope](../architecture/native-sampling-population-spill-2026-10-19.md)
with [finite local acceptance](../benchmarks/native-sampling-population-spill-2026-10-19.md),
[subquery predicate spill scope](../architecture/native-subquery-membership-spill-2026-10-19.md)
with [finite local acceptance](../benchmarks/native-subquery-membership-spill-2026-10-19.md)
(hosted, installed-package and other-platform proof remain separate), and
[stream destinations](../architecture/native-stream-destinations-2026-10-15.md).
They identify the behavior and source/release scope they cover.

<a id="semantic-claim-surface"></a>
<a id="explicit-blockers-and-non-claims"></a>

## Availability and diagnostics

Unsupported operations fail with structured diagnostics; no external engine
executes the residual work. Execution evidence preserves
`fallback_attempted=false` and `external_engine_invoked=false`.
Use [known issues](https://shardloom.io/field-guide/limitations/) for concrete
version restrictions and [troubleshooting](https://shardloom.io/field-guide/troubleshooting/)
for recovery. Operation availability, correctness coverage, operational support
and comparative performance are separate facts.

## Related References

Contributor fixtures and release controls use `shardloom.testing` and the
maintainer CLI audience. Runtime inspection and compatibility types use
`shardloom.diagnostics`. The JSON inventory preserves older semantic/evidence
sections for existing tools; its `application` object is the generated current
discovery surface. Historical acceptance fields retain their original scope and
do not set the published guide's availability.

- [CLI registry status](../status/cli-command-registry.md)
- [Maintainer agent contract](../architecture/agent-contract-pack.md)
- [Release and source rehearsal](../release/local-source-rehearsal.md)
- [Current phase plan](../architecture/phased-execution-plan.md)

Regenerate reference facts with
`python scripts/write_public_api_reference.py --binary /absolute/path/to/shardloom`
using this source's freshly built CLI. Check freshness with the same command plus
`--check`; without `--binary`, the check verifies recorded registry identity
and all Python sources/signatures.
