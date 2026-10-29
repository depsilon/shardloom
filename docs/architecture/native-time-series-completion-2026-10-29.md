<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native time-series completion

Status: the as-of join and calendar-window units pass
[as-of acceptance](../benchmarks/native-asof-joins-2026-10-29.md) and
[calendar acceptance](../benchmarks/native-calendar-windows-2026-10-29.md)
within active item 20. Resampling, gap filling and interpolation remain in
progress. This extends the accepted temporal
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

Calendar-aware windows follow the accepted contract below. Resampling, gap
filling and interpolation remain open; do not mark the parent item complete
from the as-of and calendar units alone.

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
Resampling, gap filling and interpolation remain separate unfinished owners.
