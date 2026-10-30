# Application batch functions

This describes the development source after 1.0.0, not the published 1.0.0
packages. A trusted Rust callback or Python callable can transform rows at an
explicit position in a native plan. Ordinary filters, joins, reductions,
materialized relations and writers compose around that position.

Use native expressions when they already express the operation. Application
functions are a separate, observable code and materialization boundary; they
do not inherit built-in kernel performance or an untrusted-code sandbox.

## Python registration and execution

```python
import pyarrow as pa
import shardloom as sl

def add_tax(batch, invocation):
    invocation.check_cancelled()
    values = [None if amount is None else amount * 1.1
              for amount in batch.column("amount").to_pylist()]
    return pa.RecordBatch.from_arrays(
        [batch.column("id"), pa.array(values, type=pa.float64())],
        names=["id", "gross"],
    )

with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2) as ctx:
    function = ctx.batch_functions.register(
        "add_tax", add_tax, version="1",
        input_schema={"id": "int64", "amount": "float64"},
        output_schema={"id": "int64", "gross": "float64"},
        max_batch_rows=2048, max_output_bytes=1 << 20,
        workspace_bytes=256 << 10, trusted=True, rowwise=True,
    )
    source = ctx.from_rows(
        [{"id": 1, "amount": 10.0}, {"id": 2, "amount": None}],
        schema={"id": "int64", "amount": "float64"},
    )
    transformed = source.map_batches(function)
    ctx.register("taxed", transformed)
    result = ctx.sql("SELECT id, gross FROM taxed ORDER BY id").collect()
    assert result.to_rows() == ({"id": 1, "gross": 11.0}, {"id": 2, "gross": None})
```

The registry is available on contexts and sessions. Registration, `list()`,
`get(name)`, plan composition, schema inspection and explain do not invoke the
function. Names are case-insensitive ordinary identifiers. `register(...,
replace=True)` and `drop(name)` change subsequent lookups. Already constructed
plans keep their original immutable function and version.

Every callable receives one nonempty `pyarrow.RecordBatch` and a scoped
invocation object. Return one RecordBatch with exactly the declared ordered
schema, nullability and row count. Preserve row order. An empty relation invokes
no callback and keeps its declared output schema. The engine may split input to
`max_batch_rows`; `rowwise=True` promises that these batch boundaries cannot
change any row's result. Only deterministic, null-aware functions without
external effects are admitted. These are caller declarations, not proofs of
arbitrary application code.

`invocation.check_cancelled()` cooperatively checks the current action's token.
`invocation.float_policy` exposes its floating policy. An invocation expires when
the callable returns. Synchronous code is not forcibly interrupted. Do not start
another native action from inside a callback: same-thread reentry fails before
locking, and starting and awaiting a second action on the active context from
another thread violates the contract.

## Ownership, limits and failure

Inputs pass through independent, credited native ownership before application
code receives them. Python uses the existing Arrow export boundary. Returned
application buffers are validated and copied into independent native ownership
before delivery. Retained input and output aliases preserve their buffer credits
and values after the query or context closes. This boundary does not promise
zero-copy or zero-decode execution.

`workspace_bytes` is reserved before invocation and held through output intake.
It is a declared allowance for private application allocations, not a measured
allocator total or an enforced process-memory cap. Native conversion/output
allocations separately use the current action's shared grant. `max_output_bytes`
conservatively bounds the returned Arrow array memory estimate and the resulting
native payload, including expansion during intake. Unavailable credit, excess
rows/bytes, wrong fields/types/nullability and invalid values fail explicitly.

Python exceptions retain their identity and traceback. Unwinding Rust panics
fail the current action; a process configured to abort on panic cannot recover
that panic. There is no automatic callback retry. A caller may start a new
action after correcting the failure. An unsuccessful write is not committed;
earlier streamed batches remain provisional until complete action success.

Filters, projections, limits and metadata counts do not move across this stage.
A limit before the function changes its input; a limit after it does not erase
a later callback failure. Repeated actions execute again. Repeated references
also execute normally unless the caller explicitly uses a materialized relation.

Completed native I/O reports identify function name/version, declared semantics,
completed rows/batches, input/output copying and Arrow conversions. Private
application allocation is not reported as measured native allocation. The
surrounding engine does not invoke a fallback executor.

## Typed embedding and interface scope

Rust callers use `ApplicationBatchDeclaration`, `ApplicationBatchLimits` and
`ApplicationBatchFunction::new_trusted` from
`shardloom_vortex::application_batch`. The callback receives an `ArrayRef` and
borrowed `ApplicationBatchContext`, and returns `ApplicationBatchOutput`.
`context.native_session()` borrows the active Vortex provider; it does not start
another query. With the native Arrow features enabled, `context.to_arrow()` and
an Arrow RecordBatch result use the same accounted conversion boundaries.

Attach the handle with `VortexRelationalPlan::map_batches`, or with
`NativeRelationDeclaration::map_batches` for mixed typed/SQL embedding. Exact
schemas use native Vortex DTypes; `ApplicationBatchDeclaration::parse_fields`
accepts the existing bounded native DType JSON field declarations.

The workspace Rust crates remain unpublished internal source interfaces. Source
embedders with exhaustive matches must handle the new application-error and
pipeline-stage variants when updating to this revision.

The optional `shardloom.native` facade exposes `BatchFunction` and
`BatchFunctionContext`. Bind a typed `Relation` to a `Session` first, then call
`Plan.map_batches(function)`. Its low-level constructor accepts the same ordered
native DType JSON fields and explicit trust/rowwise/resource declarations. The
function must belong to that session. Ordinary application code should use the
context registry above.

| Interface | Admitted boundary |
| --- | --- |
| Native Python DataFrame | Registered immutable handle passed to `map_batches`. |
| Native Python SQL | SQL consumes a registered relation containing the stage. |
| Typed native Python | A session-bound `Plan.map_batches` with a native function handle. |
| Embedded Rust | Native plan/declaration retains a trusted Rust callback handle. |
| Stock CLI and subprocess transport | Callback registration/transport is unsupported; SQL text cannot serialize a live handle. |

There is no module import, dynamic-library loader, serialized-code execution,
general scalar/aggregate UDF registration, or effectful/table-generating callback
in this API. Existing plugin/UDF sandbox blockers remain in force. The
[implementation contract](../architecture/native-application-batch-functions-2026-10-30.md)
records the Vortex provider decision and acceptance obligations.
