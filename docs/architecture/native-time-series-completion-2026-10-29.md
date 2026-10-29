<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native time-series completion

Status: the as-of join unit passes [local acceptance](../benchmarks/native-asof-joins-2026-10-29.md)
within active item 20. The remaining calendar-window, resampling, gap-filling and
interpolation owners are still in progress. This extends the accepted temporal
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

Calendar-aware windows, resampling, gap filling and interpolation remain the
next time-series contracts; do not mark the parent item complete from as-of
support alone.
