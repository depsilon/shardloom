<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL, DataFrames and local clients

This page describes the source implementation, including the temporal and
time-series capabilities developed under item 20. Published packages retain
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
composition. Source builds also admit explicit materialization and recursive
queries under the contracts below.

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

### Retain a CTE within an action

`WITH kept AS MATERIALIZED (<query>)` completes and validates the CTE once per
action when referenced. Every reference in that action uses the same completed
native rows. A new action recomputes them; an unused declaration does not run.
Consumer filters, projections and limits do not narrow the retained definition.
Use ordinary CTEs when you only need to name a plan, and an explicit `collect()`
result with `ctx.from_result(result)` when you need to reuse completed data across
actions.

### Evaluate recursive frontiers

```python
import shardloom as sl

with sl.context(transport="native", memory_bytes=64 << 20,
                max_parallelism=1, recursion_limit=50) as ctx:
    numbers = ctx.sql(
        "WITH RECURSIVE numbers(n) AS ("
        "SELECT 0 AS n UNION ALL SELECT n+1 FROM numbers WHERE n<3) "
        "SELECT n FROM numbers"
    )
    assert numbers.sort("n").collect().to_rows() == (
        {"n": 0}, {"n": 1}, {"n": 2}, {"n": 3},
    )
```

The anchor runs once. Each recursive-term evaluation reads the preceding
frontier, and an empty frontier ends iteration. `UNION ALL` preserves duplicates;
`UNION` removes duplicate complete rows using native equality, including NULL
and the selected floating policy. It can terminate a finite reachability query
with cycles, but cannot detect every cycle when other output columns keep
changing. Use a final `ORDER BY` or `.sort()` when order matters.

The positive `recursion_limit` defaults to 1,000 recursive-term evaluations per
relation; the anchor does not consume an evaluation. A final evaluation that
produces an empty frontier does consume one. A required next evaluation beyond
the limit fails with `SL_RECURSION_LIMIT_EXCEEDED`; an outer LIMIT, including zero,
does not authorize partial completion. The policy is immutable on the context,
client and native Session. The CLI uses `--recursion-limit` on `run` or `route`.

Each recursive definition requires a nonrecursive anchor, `UNION` or `UNION ALL`,
and one self-reference in the recursive term. Projection, filtering, ordering,
limits and inner/cross joins may surround that reference. Nonlinear or mutual
recursion, recursive outer joins, correlated retained definitions and
data-dependent pivot schemas fail explicitly. The anchor fixes field names and
value domains; recursive fields become nullable and the step cannot silently
widen numbers, rescale decimals or change timezones. Ordinary scalar parameters
bind before execution and remain supported.

Materialization and recursion share the action's memory grant and explicit
[native spill policy](native-query-spill.md). They never invoke another engine.
Cancellation, failed input validation, early close or failed publication does
not yield a successful action certificate. Already delivered output batches
retain their own owners. See the [composition contract](../architecture/native-recursive-composition-2026-10-29.md)
and [executable public tests](../../python/tests/test_native_recursive_composition_binding.py)
for the source-build acceptance scope.

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
`temporal_extract`, `temporal_trunc`, `temporal_bucket`, `calendar_add_days`,
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
Installed-package and combined item-20 acceptance remain separate requirements.

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
for memory, spill and verification scope.

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

### Resample, complete a time grid and fill values

Source builds expose `frame.resample(time, every, unit, by=...)`, which returns
a grouped declaration for `.agg(...)`. It composes `temporal_bucket` with the
ordinary native grouping owner. Buckets are half-open intervals labeled by
their lower boundary. Positive fixed spacing uses `s`, `ms`, `us` or `ns`;
`calendar_days` and `calendar_months` apply local calendar arithmetic to dates
and timestamps. The source time dtype and timezone are preserved.

For an Arrow IPC source with a temporal `instant`, string `fund` and numeric
`value` column, this is the tested composition:

```python
import shardloom as sl
from shardloom import functions as f

with sl.context(transport="native", memory_bytes=128 << 20,
                max_parallelism=2) as ctx:
    samples = ctx.read_arrow_ipc("measurements.arrow")
    daily = (
        samples.filter(sl.col("instant").is_not_null())
        .resample("instant", 1, "calendar_days", by="fund")
        .agg(total=f.sum("value"))
        .gap_fill("instant", 1, "calendar_days", by="fund")
    )
    ordered = sl.Window.partition_by("fund").order_by("instant")
    completed = daily.select(
        "fund", "instant", "total",
        sl.col("total").forward_fill().over(ordered).alias("forward"),
        sl.col("total").backward_fill().over(ordered).alias("backward"),
        sl.col("total").interpolate_linear().over(ordered).alias("linear"),
    )
    result = completed.collect()
```

`gap_fill` requires aligned, unique, non-NULL times per partition. Resample and
aggregate duplicate times first; filter NULL times explicitly. Existing payload
values, including NULLs, remain unchanged. Inserted rows carry the partition
keys and time, with NULLs in other fields; those payload fields become nullable.
NULL partition keys form a group. Input may be unsorted; output is ordered by
partition keys with NULLs first, then ascending time. Time and partition names
must identify top-level columns; project nested keys first.

Optional `start`, `end` and `origin` are signed integer ticks in the source
domain: days for Date32, milliseconds for Date64, and the declared unit otherwise.
`start` and `end` are inclusive aligned grid points that clip or extend each
observed partition. Omit a bound to use that partition's observed endpoint.
An empty unpartitioned source generates rows only when both bounds are given;
an empty partitioned source cannot invent group identities. Fixed grids default
to epoch origin zero; calendar grids default to local midnight on 1970-01-01.
Use identical grid controls for resampling and gap filling.

Calendar operations default to `month_end="reject", ambiguity="reject"`.
Explicit `month_end="clamp"` keeps the original anchor across every step, so a
January 31 monthly grid returns to March 31 after February. `ambiguity="earlier"`
or `"later"` resolves gaps and folds. A grid whose adjacent points do not advance
is rejected. Fixed spacing must be representable in the source unit, and dates
require whole elapsed days; cast explicitly to a finer unit when needed.

The three fill expressions require one temporal ORDER BY key and no explicit
window frame. Times must be unique and non-NULL within each partition.
Forward/backward follow the declared order, including descending order, and
preserve the selected logical type, including nested payloads. These partitioned
windows are separate from the source-order `fill_null(strategy="forward")` API.
Linear interpolation uses elapsed temporal distance, returns nullable Float64
and leaves unbracketed edge gaps NULL. Integers and decimal coefficients must be
within `[-2^53, 2^53]`; decimal scaling and interpolation then use Float64
rounding. Cast to Float64 explicitly to authorize rounding outside that exact
conversion range. Nonfinite inputs or results are rejected.

Typed relations use `Relation.gap_fill(time, every, unit, by=[...], start=...,
end=..., origin=..., month_end=..., ambiguity=...)`. Resampling composes
`Expression.call("temporal_bucket", ...)`, projection and `.aggregate(...)`.
`WindowExpression` accepts `forward_fill`, `backward_fill` and
`interpolate_linear`, with `source_column`, `partition_by` and `order_by`.
The SQL relation syntax is:

```sql
GAP_FILL((<input query>), 'instant', 1, 'calendar_days', 'fund',
         NULL, NULL, NULL, 'reject', 'reject') AS filled
```

The final six controls are optional positional arguments; the partition argument
is a quoted comma-separated list, or `''` for no keys. An alias is required.
SQL uses `TEMPORAL_BUCKET(value, every, unit[, origin[, month_end[, ambiguity]]])`
and `FORWARD_FILL(value)`, `BACKWARD_FILL(value)` or `INTERPOLATE_LINEAR(value)`
with the same OVER declaration. All five public interfaces share the native
temporal, grouping, ordering and window owners. See the
[grid contract](../architecture/native-time-series-completion-2026-10-29.md#resampling-and-grid-contract)
for resource and acceptance details.

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
