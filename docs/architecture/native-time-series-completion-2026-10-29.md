<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native time-series completion

Status: the as-of join, calendar-window and temporal-grid units pass
[as-of acceptance](../benchmarks/native-asof-joins-2026-10-29.md) and
[calendar acceptance](../benchmarks/native-calendar-windows-2026-10-29.md), with
[grid acceptance](../benchmarks/native-time-grid-2026-10-29.md) covering resampling,
gap filling and interpolation within active item 20. This extends the accepted temporal
types and ordinary join/window owners; installed-package and combined item-20
acceptance remain separate.

## As-of join contract

An as-of join preserves every left row in input order and selects at most one
right row from the same optional equality-key partition. The ordering key is
separate from the equality keys. Inputs need not be sorted. Backward selects
the greatest eligible right key below the left key; forward selects the least
above; nearest selects the smallest absolute distance, preferring backward on
an equal-distance tie. Exact matches are enabled by default and can be excluded.
Backward chooses the last source occurrence among equal right ordering keys;
forward chooses the first. Nearest follows the selected direction's tie rule,
including backward for an exact match. NULL ordering/equality keys do not match;
an unmatched left row receives NULL right fields.

Admit exact integer ordering domains and compatible temporal domains. Temporal
units, kind and timezone metadata must agree; callers use an explicit cast for
conversion. A nonnegative inclusive tolerance is expressed in ordering-key
ticks (days for date32, milliseconds for date64, declared units otherwise).
Distance arithmetic uses an exact wider intermediate, including signed/unsigned
extremes. Float/string/decimal ordering keys require separate semantic admission
and fail explicitly in this work unit. Equality keys reuse ordinary native key
semantics. No arbitrary residual ON predicate is silently ignored.

Use a leased native build owner and stable ordered row locations for lookup.
Every retained payload, ordering vector, lookup block and output selection uses
the session's existing grant. Probe batches preserve input order; output is
bounded and independently owned. Cancellation and allocation failure release
owners. Explicit spill uses the existing native ordering/run store and its
validation/cleanup policy; no external engine participates.

## Provider decision and verification

Vortex-first classification: `implement_shardloom_kernel`. The pinned Vortex
array/compute inventory has native arrays, validity, temporal extensions, take
and ordering inputs, but no admitted relational as-of provider. Reuse those
existing boundaries through ShardLoom's native batch, key, ordering, gather and
lease owners. Do not add an Arrow execution substrate or an engine integration.
The existing relational execution and source certificates continue to report
`fallback_attempted=false` and `external_engine_invoked=false`.

Acceptance includes unsorted and multiple-batch inputs, empty sides, exact and
excluded matches, inclusive tolerance boundaries, partition NULLs, duplicate
ties across batches, negative epochs, integer extremes, all temporal units and
timezone mismatch rejection. Add independently calculated fresh cases and
five-interface parity, composition, pressure/cancellation, producer-close output
ownership and native spill lifecycle checks. Existing join behavior must remain
unchanged. This capability does not claim a speedup on the 347-query cohort.

Calendar-aware windows and temporal grids follow the accepted contracts below.
These local capability units do not complete the parent item's installed-package,
combined-workflow, numeric, composition, batch-function or diagnostic acceptance.

## Calendar RANGE contract

Extend the existing window owner, preserving peer, exclusion, partition, stable
output and NULL ordering rules. Fixed durations compare exact elapsed quantities
in the ordering key's declared unit. They must work for all admitted timestamp,
time and duration units; dates require whole elapsed days. Keep the existing
`DurationMicros` declaration and fixed SQL `INTERVAL ... DAY` meaning, and admit
explicit nanosecond through second duration declarations without rounding the
offset to the source's tick precision. Compare in widened integer nanoseconds.

Calendar day and month offsets apply the shared temporal calendar kernel to the
current row's local date/time fields. Admit dates and timestamps only. Reuse the
existing `reject`/`earlier`/`later` gap/fold policy and `reject`/`clamp` month-end
policy. Defaults reject ambiguity or an invalid destination day. Zero offsets
retain the original instant, including its fold occurrence, and have the same
endpoint position as `CURRENT ROW` without bypassing type admission. Descending order
reverses the direction of the shift. Unsupported domains, calendar range
overflow and rejected policies fail explicitly. Frontends declare these policies;
they do not evaluate calendar arithmetic.

Calendar boundaries can move backward across a daylight-saving fold. Resolve
calendar endpoints by binary search over the existing sorted native keys instead
of advancing a one-way cursor. Resident state applies the exact entering/leaving
membership difference; extrema rebuild their existing charged candidate queue
when an endpoint retreats. Stable statistical summaries may reuse complete
overlapping blocks in either direction. No uncharged second partition is created.

Explicit spill keeps the existing stored bounds, native value-interval unions
and extrema summaries. Split the stored output bounds into maximal monotone
ranges and run those existing algorithms on each range, in output order, over
the same full input partition. This preserves bounded source lookup and the
existing spill admission/cleanup contract even when a calendar boundary retreats.
The extra work per range is explicit and carries no new performance claim.

Provider classification remains `implement_shardloom_kernel`: the pinned Vortex
inventory supplies native temporal arrays, keys and take, but no calendar window
operator. Reuse ShardLoom's already admitted temporal kernel and native window
owners, with metadata/timezone admission before binding. Required acceptance
includes independent unit/domain matrices, spring gaps and fall folds (including
nonmonotone endpoints), month ends/leap years, both directions and all exclusions,
resident/spilled agreement, fault/ownership/resource checks, and all five public
interfaces. The [local acceptance record](../benchmarks/native-calendar-windows-2026-10-29.md)
passes these checks, including 460 public executions and 37,500 compared rows.
The temporal-grid acceptance below covers resampling, gap filling and interpolation
through the existing grouping, ordering and window owners.

## Resampling and grid contract

Resampling composes a typed `temporal_bucket` expression with the existing
native grouping and aggregate owners. A bucket is the half-open interval
`[point(k), point(k+1))`, labeled by its lower boundary. Positive fixed durations
use exact widened Euclidean division, including before the epoch. A materialized
boundary must be representable in the input domain: no fractional-tick rounding,
and dates require whole days. Fixed grids default to epoch origin zero.

Calendar-day/month grids admit dates and timestamps. Their default origin is
local midnight on 1970-01-01, resolved by the declared gap/fold policy. An
explicit origin is exact source-domain ticks and retains its original fold
occurrence. Every point is calculated from that original anchor, so clamping
January 31 to February's last day does not move the March anchor to the 28th or
29th. Reuse the existing month-end and gap/fold policies. Refuse non-advancing
adjacent boundaries, including a skipped whole civil day, rather than looping
or silently dropping a point. Calendar-provider range errors remain explicit;
an adjacent boundary outside the output tick domain alone must not invalidate
an otherwise representable bucket.

`temporal_bucket(value, every, unit[, origin[, month_end[, ambiguity]]])` takes
literal controls. Units are `s`, `ms`, `us`, `ns`, `calendar_days` and
`calendar_months`; `origin=NULL` selects the documented default. The Python
builder declares the same controls. Frontends do not calculate buckets or rows.

Gap filling adds missing points to an already aligned time grid, normally after
resampling. Reuse native stable ordering, key equality, payload access, spill
and output owners. Inputs may arrive unsorted and in multiple batches. Partition
NULLs are ordinary equal keys. NULL times, unaligned times and duplicate times
within a partition fail explicitly; callers can filter, bucket or aggregate
first. Preserve original payloads, including existing NULLs, and make generated
non-key fields NULL and nullable. Sort output by partition keys and time.

Optional start/end are inclusive aligned grid points, clipping or extending
each observed partition; otherwise use its observed minimum/maximum. Reject
reversed bounds. Empty unpartitioned input can generate a grid only with both
bounds; empty partitioned input cannot invent partition identities. Generate
bounded output batches with cancellation and existing allocation admission;
never accumulate the expanded result or fall back to an external engine.

Alternating original rows and generated gaps can create many tiny fragments.
Pass these through the same bounded native record-compaction buffer used by
stored aggregate/set results before the next operator retains them. Its row,
chunk and byte estimates decide when to flush; actual schema, index and payload
owners still reserve against the existing query grant. This prevents fragment
metadata from defeating an otherwise adequate window grant without raising
resource limits or retaining the expanded output.

Explicit forward/backward filling and linear interpolation operate within the
same ordered partitions. Keep edge gaps NULL for linear interpolation, use
temporal distances rather than row positions, and require distinct non-NULL
ordering keys. Numeric interpolation returns nullable Float64 and retains
non-NULL values numerically; exact integers/decimals require checked conversion
and a documented precision policy. Reuse window input access for resident and
spilled partitions; do not introduce a second full partition or hidden Python
evaluation. The [local grid acceptance](../benchmarks/native-time-grid-2026-10-29.md)
checks these policies with independent complete results and actual resource/fault
fixtures.

The window declarations are `FORWARD_FILL(value)`, `BACKWARD_FILL(value)` and
`INTERPOLATE_LINEAR(value) OVER (PARTITION BY ... ORDER BY time [DESC])`. Each
requires exactly one temporal ordering key and refuses explicit window frames.
Forward/backward follow that order and preserve the selected value's logical
dtype, including nested payloads. This is separate from the existing source-order
`fill_null(strategy="forward")` rewrite, which has no temporal partition contract.
Linear interpolation preserves input row order, fills only between non-NULL
endpoints and returns nullable Float64. Compute elapsed tick differences in
`i128` before conversion, avoiding cancellation between large epoch timestamps.
Integer values and decimal coefficients must lie within `[-2^53, 2^53]` before
Float64 conversion; decimal scaling and interpolation then use ordinary Float64
rounding. Larger exact inputs require an explicit Float64 cast to authorize
rounding. Nonfinite inputs/results fail. No second complete partition or endpoint
index is retained: walk the existing resident/stored input with two endpoint
records and emit into the existing window result owner.

The mathematical reference is ordinary piecewise linear interpolation over
ordered coordinates, as described in the [NumPy interpolation reference](https://numpy.org/doc/stable/reference/generated/numpy.interp.html).
ShardLoom additionally enforces unique non-NULL temporal coordinates and leaves
unbracketed gaps NULL. No NumPy dependency, code or runtime evaluation is used.

Vortex-first decision: `implement_shardloom_kernel`. The pinned Vortex 0.90.0
array, compute and scan inventory provides temporal extensions, validity and
take, but no admitted resampling, per-partition gap-fill or interpolation
operator. Extend ShardLoom's metadata-bound temporal kernel and existing native
grouping/window/sort owners. All five interfaces must share these semantics,
admission and certificates, with `fallback_attempted=false` and
`external_engine_invoked=false`. No dependency, external execution provider,
benchmark claim or installed-package acceptance is added by this design.
