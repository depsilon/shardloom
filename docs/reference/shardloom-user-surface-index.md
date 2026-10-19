<!-- SPDX-License-Identifier: Apache-2.0 -->

# ShardLoom User Surface Index

Schema marker: `shardloom.user_surface_index.v1`.

This is the **development-source application reference**. The
[published guide](https://shardloom.io/field-guide/start-local-proof/) names the
artifact it describes. Source additions below are not changes to published 0.6.0.
For deliberate behavior changes, read the [Python migration guide](python-api-migration.md).

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
Development source identity: `fdd3ee32bcaca2ed8ed1e7bbd7979180fa8cefd39b83f4b94542f2d5bc5436de` (SHA-256 of the listed source inputs).
This API is unpublished; the package version is held independently of source changes.

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
| [`SqlWorkflow`](../../python/src/shardloom/query.py) | class | A scoped SQL workflow entry point over currently admitted ShardLoom SQL paths. |
| [`WindowExpression`](../../python/src/shardloom/query.py) | class | A SQL window declaration submitted to the shared native engine. |
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
| [`count_distinct`](../../python/src/shardloom/query.py) | function | Declare COUNT DISTINCT, retaining owned query sources when present. |
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
| [`from_vortex_ipc`](../../python/src/shardloom/query.py) | function | Import one Vortex 0.85 IPC record using the native transport. |
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
| [`read_arrow_ipc`](../../python/src/shardloom/query.py) | function | Declare a lazy Arrow IPC compatibility source. |
| [`read_avro`](../../python/src/shardloom/query.py) | function | Declare a lazy Avro compatibility source. |
| [`read_csv`](../../python/src/shardloom/query.py) | function | Declare a lazy CSV compatibility source. |
| [`read_json`](../../python/src/shardloom/query.py) | function | Declare a lazy flat JSON, JSONL, or NDJSON compatibility source. |
| [`read_orc`](../../python/src/shardloom/query.py) | function | Declare a lazy ORC compatibility source. |
| [`read_parquet`](../../python/src/shardloom/query.py) | function | Declare a lazy Parquet compatibility source. |
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
(local acceptance in progress), and
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
