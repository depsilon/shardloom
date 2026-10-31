<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native analytical aggregates

Current source builds expose these reducers through shared native execution in
CLI SQL, embedded workflows, Python SQL, DataFrames and typed native relations.
They compose with ordinary grouping and admitted analytic window frames. Vortex
is the native representation and persistence target; compatibility readers use
their declared source adapters. Unsupported functions, types and resource
configurations fail explicitly without external-engine fallback.

## Values, NULLs and result types

| Family | Declaration | Contract |
| --- | --- | --- |
| Distinct addition | `SUM(DISTINCT x)`, `AVG(DISTINCT x)` | Exact membership on original non-NULL values, then the existing additive reduction. Empty/all-NULL input yields NULL. Primitive results are Float64; admitted decimals retain their exact scale and overflow rules. |
| Variance | `VAR_POP(x)`, `VAR_SAMP(x)` | Ignore NULLs. Population variance is zero for one value; sample variance is NULL below two values. Both are NULL for no values and return nullable Float64. `VARIANCE`/`VAR` mean sample variance. |
| Standard deviation | `STDDEV_POP(x)`, `STDDEV_SAMP(x)` | The same sample and NULL rules as variance. `STDDEV`/`STD` mean sample standard deviation. A representable deviation may succeed even when its squared variance would overflow. |
| Covariance | `COVAR_POP(x,y)`, `COVAR_SAMP(x,y)` | Drop a pair if either input is NULL. Population covariance is zero for one pair; sample covariance is NULL below two pairs. Results are nullable Float64. |
| Correlation | `CORR(x,y)` | Pairwise NULL exclusion; NULL below two pairs or when either selected input has zero variance. Returns nullable Float64. |
| Continuous quantile | `QUANTILE_CONT(x,q)`, `MEDIAN(x)` | Ignore NULLs; order original numeric values; interpolate at `q*(N-1)`. Median uses `q=0.5`. Primitive results are nullable Float64. |
| Discrete quantile | `QUANTILE_DISC(x,q)` | Ignore NULLs and select zero-based rank `max(ceil(q*N)-1,0)`. Preserve the nullable input dtype. |
| Ordered selection | `FIRST(x ORDER BY k)`, `LAST(x ORDER BY k)` | Retain NULLs and the nullable input dtype. With no local order, use established input order. Empty input yields NULL. |
| Keyed selection | `ARG_MIN(x,k)`, `ARG_MAX(x,k)` | Skip rows with a NULL value or key; compare the original key dtype. Return the original nullable value dtype. Equal extrema select the first row under local ordering, then input order. |
| List collection | `LIST(x ORDER BY k)`, `ARRAY_AGG(x ORDER BY k)` | Retain duplicates, NULL elements and the original child dtype. Empty input yields NULL, distinct from a valid list containing NULL. |
| String collection | `STRING_AGG(x,separator ORDER BY k)` | Accept UTF-8 values and a literal UTF-8 separator. Skip NULL values; retain empty strings. No non-NULL values yields NULL. |

Statistical moments admit primitive numeric inputs, not arbitrary decimal
statistics. They retain original-width integer differences, use scaled centered
updates and, under the default strict float policy, reject observed non-finite
values. Unrepresentable finite results still fail. A value excluded by a NULL
companion is not observed. Ordinary COUNT,
MIN/MAX and SUM/AVG retain their existing semantics.

Quantile levels are finite scalar literals in `[0,1]`. Empty/all-NULL input
yields NULL. Continuous Decimal128 quantiles use precision 38 and
`max(input_scale,6)`; inexact interpolation at that scale fails rather than
silently rounding unless an explicit decimal output is selected as described
below. Exact rank and membership do not promise exact real arithmetic in
Float64.

## Floating policy

`ShardLoomClient(float_policy="extended")` or
`shardloom.context(float_policy="extended")` admits NaN and signed infinities.
The CLI option is `--float-policy extended`; typed native `Session` accepts the
same keyword. The immutable policy is recorded in execution evidence and stays
with the session when resource limits change. `strict` remains the default.

Under extended policy, all NaNs compare equal and sort after positive infinity;
signed zeros compare equal. NULL remains separate. Grouping, DISTINCT, joins,
sets and window peers share those identities, while selected outputs preserve
their original bits. `IS_NAN`, `IS_INFINITE`, `IS_FINITE` and `SIGNBIT` classify
values and propagate NULL; classification itself is available under either policy.

SUM/AVG yield NaN for any NaN or opposing infinity signs, and the corresponding
infinity for one infinity sign. Statistical moments yield NaN when a nonfinite
value participates, after the usual minimum sample-size check. Moving windows
recover finite results after those values leave. MIN/MAX and discrete quantiles
use the analytical order and preserve selected bits. Newly computed NaNs use
canonical quiet-NaN bits. Quantile probabilities remain finite.

Native Vortex, Arrow IPC and Parquet preserve admitted F32/F64 bits, including
signed zeros and NaN payloads. Raw nonfinite CSV/JSON numbers fail explicitly;
an explicit UTF8 cast chooses `NaN`, `inf` or `-inf` text. Exact integer/decimal
conversion rejects nonfinite inputs; `TRY_CAST` returns NULL on failure.
See the [full floating contract](../architecture/native-float-policy-2026-10-29.md)
for strict intake, continuous quantiles, arithmetic and ownership boundaries.

## Explicit decimal output

`SUM`, `AVG`, `SUM(DISTINCT ...)`, `AVG(DISTINCT ...)`, `MEDIAN` and
`QUANTILE_CONT` may take an opt-in output precision, scale and rounding policy.
Precision is total coefficient digits (1 through 38); scale is fractional
places (0 through precision). The rounding name is one of `exact`,
`half_even`, `half_away`, `toward_zero`, `floor` or `ceil`. `exact` rejects a
nonzero discarded fraction; the half modes round to nearest, with ties to an
even coefficient or away from zero. `toward_zero` truncates, while `floor` and
`ceil` round toward negative and positive infinity. Overflow is checked after
rounding.

In SQL, append the three literal options to the aggregate arguments. For
`QUANTILE_CONT`, the quantile level comes first; `MEDIAN` keeps its implicit
level of 0.5:

```sql
SELECT SUM(amount, 18, 2, 'half_even'),
       AVG(DISTINCT amount, 18, 2, 'exact'),
       MEDIAN(amount, 18, 2, 'half_away'),
       QUANTILE_CONT(amount, 0.1, 18, 2, 'floor')
FROM 'sales.vortex';
```

Python helpers expose the same controls as keyword arguments. Supply all three
or omit all three:

```python
from shardloom import functions as f

total = f.sum("amount", precision=18, scale=2, rounding="half_even")
average = f.avg("amount", distinct=True, precision=18, scale=2, rounding="exact")
median = f.median("amount", precision=18, scale=2, rounding="half_away")
p10 = f.quantile_cont("amount", 0.1, precision=18, scale=2, rounding="floor")
```

Typed native relations accept the declaration as a
`decimal_output=(precision, scale, rounding)` tuple on `AggregateMeasure`;
the corresponding window measure also accepts `decimal_output`:

```python
from shardloom.native import AggregateMeasure, Relation

relation = Relation.scan("sales.vortex").aggregate(
    [],
    [AggregateMeasure("sum", "amount", "total",
                      decimal_output=(18, 2, "half_even"))],
)
```

An explicit output policy is admitted only for Decimal128 aggregate input.
Ordinary integer `SUM` keeps its existing result behavior; to request an exact
widened integer total, cast the input to Decimal128 first, for example
`SUM(CAST(quantity AS DECIMAL(38,0)), 38, 0, 'exact')`. The selected scale and
rounding apply once when the aggregate result is finalized, after aggregation
and merging; window aggregates apply it to each finalized frame. Stored
execution retains exact accumulation state and applies the policy at result
finalization. Omitting the option leaves existing result types, precision,
scale and rounding behavior unchanged. This option does not apply to discrete
quantiles or other aggregate families.

Local ordering accepts direction and NULL placement. Input ordinals break ties.
Window order determines frame membership; aggregate-local order independently
orders the selected values. Positional window functions such as `FIRST_VALUE`
retain their separate contracts.

## Filters and grouping combinations

`aggregate(...) FILTER (WHERE predicate)` admits true rows only; false and NULL
are excluded before argument observation. A group remains present when its
filter excludes every row. SQL and DataFrame computed arguments are lazily
guarded by that filter. A typed `filter_column` selects values already produced
by the caller's explicit preceding projections.

`GROUPING SETS`, `ROLLUP` and `CUBE` consume the input through the same aggregate
owner. Omitted group keys become typed NULLs; genuine NULL keys remain distinct
from subtotal rows. Duplicate sets retain duplicate results unless `GROUP BY
DISTINCT` is requested. An empty set emits its empty-domain aggregate on empty
input. `GROUPING`/`GROUPING_ID` returns a UInt64 omission mask, with the rightmost
argument in the low bit; arguments must identify grouping keys. Cast explicitly
when combining that UInt64 mask with signed arithmetic. HAVING, final ordering
and limits operate on the combined grouped result.

```sql
SELECT fund, currency, GROUPING_ID(fund,currency) AS subtotal,
       VAR_POP(value) FILTER (WHERE keep) AS variance,
       LIST(value ORDER BY id DESC) FILTER (WHERE keep) AS items
FROM 'positions.vortex'
GROUP BY GROUPING SETS ((fund,currency), (fund), ())
ORDER BY subtotal, fund, currency;
```

Equivalent DataFrame declarations remain lazy until an action:

```python
import shardloom as sl
from shardloom import functions as f

frame = sl.read_vortex("positions.vortex")
summary = frame.grouping_sets(["fund", "currency"], ["fund"], []).agg(
    subtotal=f.grouping_id("fund", "currency"),
    variance=f.var_pop("value").filter(f.col("keep")),
    items=f.list("value", order_by=[("id", True, "last")]).filter(f.col("keep")),
)
```

Typed relations use the same reducer descriptors:

```python
from shardloom.native import AggregateMeasure, Relation

relation = Relation.scan("positions.vortex").aggregate(
    ["fund", "currency"],
    [AggregateMeasure("var_pop", "value", "variance", filter_column="keep")],
    grouping_sets=[["fund", "currency"], ["fund"], []],
    grouping_indicators=[("subtotal", ["fund", "currency"])],
)
```

Existing three-element Python aggregate tuples and existing Rust constructors
remain usable. The unpublished Rust runtime descriptors gain optional fields;
source callers using struct literals must initialize those fields, while
constructors supply the ordinary no-filter/no-local-order defaults.

## Memory, spill and execution boundaries

Fixed-size compatible moments share charged state. Exact distinct membership,
quantiles, ordered collections and window indexes have separate reservations.
Admitted aggregate spill uses the existing native ordering/run store under an
explicit spill policy. Cancellation, denied allocation and failed delivery
release owned state and files; retry starts a fresh execution.

A quantile window's active rank index still requires live memory even when its
input spills. A complete list or string result must fit its output reservation.
Neither input spill nor a streaming source removes those limits. Grouping-set
expansion and indicator width are validated before execution.

See [native query spill](native-query-spill.md) and the
[implementation contract](../architecture/native-analytical-completion-2026-10-27.md)
for ownership and acceptance details. General temporal/recursive/numeric
completion, combined [application batch-function](application-batch-functions.md)
workflow acceptance, remaining performance gaps and broad
competitive certification remain open.
