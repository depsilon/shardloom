<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL, DataFrames and local clients

This page describes the source implementation, including temporal fidelity work
whose source-build acceptance is still in progress. Published packages retain
the support shipped with each release; source changes do not change published
artifacts. Use a native build from this checkout for source-only APIs.

Readers, `sql()`, `table()` and transformations return one lazy DataFrame class,
`LazyFrame`. Naming a plan or changing syntax does not execute it,
collect rows or create an intermediate file. `collect()` returns a native-backed
`Result`; `to_rows()` is an explicit conversion. `iter_batches()` supplies bounded
output batches, and a writer returns a completed action receipt.

## Compose a complete computation

```python
import shardloom as sl
from shardloom import functions as f, types as t

with sl.context(transport="native", memory_bytes=128 << 20,
                max_parallelism=2) as ctx:
    facts = ctx.from_rows(
        [{"id": 1, "amount": 10}, {"id": 2, "amount": 20},
         {"id": 3, "amount": None}],
        schema={"id": t.int64(nullable=False), "amount": t.int64()},
    ).filter(f.col("id") >= 1)
    ctx.register("facts", facts)
    selected = ctx.sql(
        "WITH a AS (SELECT id,amount FROM facts), "
        "b(key,total) AS (SELECT id,COALESCE(amount,0)+:extra FROM a) "
        "SELECT key AS id,total FROM b",
        parameters={"extra": 5},
    )
    labels = ctx.from_rows([
        {"id": 1, "label": "one"}, {"id": 2, "label": "two"},
        {"id": 3, "label": "three"},
    ])
    joined = selected.join(labels, on="id").select(
        "f.id AS id", "f.total AS total", "d.label AS label",
    )
    final = joined.select(
        "id", "total", "label",
        f.row_number().over(sl.Window.order_by("id")).alias("position"),
    ).sort("id").observe("quality", rows=f.count(), total=f.sum("total"))
    result = final.collect()
    assert result.observations == {"quality": {"rows": 3, "total": 45}}
    assert result.to_rows() == (
        {"id": 1, "total": 15, "label": "one", "position": 1},
        {"id": 2, "total": 25, "label": "two", "position": 2},
        {"id": 3, "total": 5, "label": "three", "position": 3},
    )
```

The same composition accepts file readers such as `read_csv()`, `read_parquet()`
and `read_vortex()`. An action uses the admitted adapter and shared native plan.
To persist this plan, call `final.write_vortex(destination)` while the context
is open. Require `receipt.output_commit_status == "committed"` before treating
the output as published; reopen it with `ctx.read_vortex(destination)`.
Vortex is the native persistence target. Compatibility exports report their
own fidelity boundaries.

`ctx.read_vortex(path).limit(1).run()` uses the native Vortex policy by default
and executes the same connected native declaration. Selecting `native_vortex`
explicitly on a Vortex reader is also supported.

## Names, parameters and schemas

`ctx.register(name, frame)` snapshots an immutable plan. Use `replace=True` to
replace it, `ctx.list_tables()` to list names, `ctx.table(name)` to bind one,
`ctx.catalog.schema(name)` to inspect its schema, and `ctx.drop_view(name)` to
remove it. Ordinary names normalize to lowercase. Duplicate names, unknown
names and cross-context bindings fail explicitly.

A query keeps its construction-time bindings after catalog replacement/drop.
Repeated references share declarations and source ownership, not cached answers.
`ctx.sql(..., relations={"input": frame})` gives one query local bindings that
shadow catalog names. Nested nonrecursive WITH scopes, shadowing and CTE
column-name lists use the same binder. `NOT MATERIALIZED` retains ordinary plan
composition. `MATERIALIZED` is rejected until its CTE ownership contract exists;
use an explicit `collect()` result and `ctx.from_result(result)` when a completed
data boundary is required. Recursive execution is separate work.

A query inherits resource ceilings only from the named plans its SQL actually
references. Unused catalog entries and names shadowed by a local CTE do not
constrain the query. Referenced plans keep their ceilings, including plans with
no file source; an allocation above a ceiling fails before execution.

Named parameters bind scalar values, never SQL fragments, identifiers or table
names. Missing, extra and malformed parameters fail explicitly. For exact
declared types use, for example,
`parameters={"amount": t.scalar(Decimal("12.30"), t.decimal128(12, 2)),
"missing": t.scalar(None, t.int64()), "small": t.scalar(2, t.uint64())}`
with `from decimal import Decimal`. Fresh bindings recompute parameter-sensitive
planning decisions. Positional `?` parameters are unsupported.

Schema inspection binds types without calling batch producers or executing a
query. It supports declared schemas, native Vortex metadata, Parquet footers
and Arrow IPC footer schemas. Schemaless text and the currently unadmitted
Avro/ORC metadata readers require an explicit action or complete declared schema.
Type hints alone do not declare every file column.

## Expressions, windows and observations

`shardloom.functions` declares native expressions. In column positions, strings
name columns; use `f.lit()` for scalar constants. `f.catalog()` exposes scalar
names, aliases, arities and type/null rules without opening data. The generated
[scalar reference](native-scalar-functions.md) records those shared definitions.
JSON extraction returns JSON text, including quotes around strings and `null`
for JSON null; an absent path produces SQL null.

`shardloom.types` declares temporal metadata with `date32()` and
`timestamp_micros()` retained as compatibility names, plus `date64()`,
`time(unit)`, `timestamp(unit, timezone=None)` and `duration(unit)` for the
`s`, `ms`, `us` and `ns` units. Explicit integer ticks preserve nanoseconds and
the full signed-64-bit timestamp/duration range; Python calendar objects have
their own range and precision limits. Exact unit casts reject overflow or
discarded precision.

Temporal expression declarations lower to the shared native registry:
`temporal_extract`, `temporal_trunc`, `calendar_add_days`,
`calendar_add_months`, `temporal_add_duration`, `temporal_sub_duration`,
`timestamp_difference`, `timestamp_assign_timezone`,
`timestamp_convert_timezone` and `timestamp_remove_timezone`. A naive timestamp
is a civil value. Assigning a timezone resolves it into an instant; converting
the timezone preserves the instant; removing it preserves local civil fields.
Calendar operations use pinned IANA 2026a rules, with `reject` defaults and
explicit `earlier`/`later` ambiguity or `clamp` month-end choices. Fixed-duration
arithmetic remains distinct from calendar-day and calendar-month arithmetic.
The declarations do not perform calendar evaluation in Python. `f.lit()` continues
to accept a naive Python datetime for its existing microsecond literal contract.
The complete time-series acceptance checklist remains open.

Source builds also expose `left.join_asof(right, on="time", by="entity")` and
typed `Relation.join_asof`. These preserve left input order, accept unsorted
inputs and select at most one right row per left row. `strategy` accepts
`backward` (default), `forward` or `nearest`; `allow_exact_matches=False`
excludes equal ordering keys. Optional `tolerance` is an inclusive nonnegative
integer in the ordering type's ticks: days for Date32, milliseconds for Date64,
and the declared unit otherwise. Temporal kind, unit and timezone must agree.
NULL ordering or partition keys do not match. Backward chooses the last source
occurrence among equal right keys; forward chooses the first. Nearest prefers
backward on equal distance and uses that direction's duplicate rule.
Separate names use `left_on`/`right_on` and `left_by`/`right_by`; DataFrame output
names use `f.` and `d.` as for ordinary joins.

SQL uses `ASOF LEFT JOIN ... MATCH_CONDITION (f.time >= d.time)` with optional
equality-only `ON f.entity = d.entity` and `TOLERANCE 1000`. Use `>` for strict
backward, `<=` or `<` for forward, and `NEAREST(f.time,d.time,TRUE)` or `FALSE`
for nearest with or without exact matches. This is one shared native join owner
across SQL, DataFrame, typed relation, workflow and CLI declarations. See the
[time-series contract](../architecture/native-time-series-completion-2026-10-29.md)
for memory, spill and verification scope. Resampling, gap filling and
interpolation remain separate work.

Window specifications are immutable reusable values. For example,
`sl.Window.partition_by("group").order_by("time").rows_between(-2, 0)`
selects the current row and two preceding rows per partition.
`range_between(0, 0)` selects the current peer range for admitted ordering types.

Source builds also admit temporal RANGE offsets. `Window.duration(value, unit="us")`
uses elapsed time with `s`, `ms`, `us` or `ns` precision. Negative values precede
the current ordering value; positive values follow it. Duration boundaries keep
their exact precision even when the ordering column has coarser ticks. Dates
require a whole number of elapsed days.

`Window.calendar_days(value, ambiguity="reject")` and
`Window.calendar_months(value, month_end="reject", ambiguity="reject")` shift
local date/time fields for date and timestamp ordering columns. For example:

```python
daily = sl.Window.partition_by("entity").order_by("time").range_between(
    sl.Window.calendar_days(-1, ambiguity="earlier"),
    sl.Window.calendar_days(1, ambiguity="earlier"),
)
monthly = sl.Window.order_by("time").range_between(
    sl.Window.calendar_months(-1, month_end="clamp"),
    sl.Window.current_row,
)
```

Calendar offsets reject ambiguous or nonexistent local times by default; use
`earlier` or `later` to select the corresponding instant. Month-end shifts reject
an unavailable destination day unless `month_end="clamp"` is explicit. A zero
calendar offset preserves the current instant, including its fold occurrence.
Descending order reverses the shift direction. The existing peer, NULL and
exclusion rules apply, and policy/range errors stop the query.

Typed relations use `FrameOffset.duration`, `FrameOffset.calendar_days` and
`FrameOffset.calendar_months` from `shardloom.native`, with the same keyword
policies and a **nonnegative** magnitude. Put the direction in `WindowFrame`:

```python
from shardloom.native import FrameOffset, WindowFrame

daily_frame = WindowFrame(
    "range",
    ("preceding", FrameOffset.calendar_days(1, ambiguity="earlier")),
    ("following", FrameOffset.calendar_days(1, ambiguity="earlier")),
)
```

SQL accepts `RANGE CALENDAR_DAYS(1, 'earlier') PRECEDING` and
`RANGE BETWEEN CALENDAR_MONTHS(1, 'clamp', 'reject') PRECEDING AND CURRENT ROW`.
`INTERVAL '1' DAY` remains an elapsed 24 hours; `INTERVAL '1' MONTH` and
`INTERVAL '1' YEAR` use calendar months with rejecting policies. SQL, DataFrame,
typed relation, workflow and CLI declarations share the same native window owner
and explicit native spill policy. See the [calendar RANGE contract](../architecture/native-time-series-completion-2026-10-29.md#calendar-range-contract)
for the current acceptance scope.

`frame.transform(sl.plan_transform(function, name="name"), ...)` invokes Python once
to build a same-context DataFrame plan; native expressions perform its data operations.
`observe(name, ...)` attaches COUNT, SUM, MIN and MAX metrics at that exact plan
position and shares the action's input pass. Moving it across a filter/range
changes its domain. Names must be unique across the complete action. DISTINCT
metrics and observations inside correlated repeated execution are unsupported.

Completed results and write receipts expose `.observations`. Streams expose
their completed report after EOF. Early close, cancellation, producer failure
or failed publication does not certify provisional metrics. Data completion
and output publication remain separate receipt fields. Delivered batches retain
their buffers after stream closure.

## DB-API, ADBC and notebook SQL

Install the source package with its `adbc` extra for the independent Apache ADBC
manager and Arrow client boundary. Every connection borrows an existing native
context; no implicit global session is created. With an open context and the
`facts` catalog entry from above:

```python
from shardloom import dbapi

with dbapi.connect(ctx) as connection, connection.cursor() as cursor:
    cursor.execute("SELECT id FROM facts WHERE id>=:minimum ORDER BY id",
                   {"minimum": 2})
    assert cursor.fetchone() == (2,)
    assert cursor.fetchall() == [(3,)]
```

The adapter supports named parameters and autocommit queries. Transactions,
SQL writes, bulk parameter streams, remote partitions and Substrait execution
are unsupported. Connection closure cancels that connection's active streams
and leaves the context available. Context closure invalidates further execution;
already delivered batches remain valid.

For independent ADBC clients, enter `shardloom.adbc.driver_options(ctx)` and
open the client's database handle inside that block. Its one-use context
capability expires on block exit. `shardloom.adbc.database(ctx)` directly opens
an independent manager handle. Bind one named Arrow parameter row using the
supported scalar types; unsupported widths/units need an explicit client cast.
Prepared statements retain their catalog snapshot, consume bindings on execution
and do not reuse stale parameters after failed binds. Metadata calls do not
execute producers. After EOF, the `shardloom.execution_report` statement option
returns the completed native report.

With the `notebook` extra installed, `handle = shardloom.notebook.install(ctx)`
registers explicit-context IPython SQL. `%shardloom SELECT * FROM facts` returns
a lazy DataFrame. Cell form supports `%%shardloom --params bindings --into frame`,
using a named mapping from the notebook namespace. An explicit action runs the
frame. `handle.close()` removes the registration; existing magic names are not
overwritten.

Executable workflow and failure controls are in
[`test_connected_programming.py`](../../python/tests/test_connected_programming.py),
[`test_native_observations.py`](../../python/tests/test_native_observations.py) and
[`test_native_standard_adapters.py`](../../python/tests/test_native_standard_adapters.py).
