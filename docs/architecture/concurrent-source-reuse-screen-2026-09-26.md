# Concurrent source reuse — R8

Status: bounded attribution in progress; no runtime candidate retained.
PERF-INTAKE / RFC 0044, following merged R9.b in PR #1465.

## Existing behavior and measured question

Owned-array sources already share immutable native buffers and their original
owners. Prepared aggregates reuse lowering and source identity while constructing
fresh mutable state. Reimplementing either is duplicate work. File-backed serving
shares a held descriptor and parsed footer, but builds a reader tree over each
call's I/O scope. That scope owns cancellation, active I/O and drain before the
call returns its CPU grant. Current segment reuse closes at the operation boundary.

R8 must first locate material repeated work across compatible concurrent calls.
Repeated segment demand alone is not exclusive time saved, and a shared file
handle does not establish shared decoding. The screen records successful
positional-read ranges and bytes, complete native-call and cohort times, output
ownership and memory release. It does not measure physical device traffic or
attribute every provider decode allocation. The accepted concurrent-host timing
explanation is not reopened by this experiment.

## Bounded source screen

Generate one 131,072-row native file in local temporary storage: eight batches of
16,384 rows, a renamed exact integer beyond floating-point precision, and renamed
nullable Unicode text. Use the upstream default writer and report the resulting
file identity and size, capped at 64 MiB. No retained ClickBench artifact changes.

Use the existing serving session, P4 and 256 MiB, with one caller CPU lane per
general operation and no reserved metadata lane. Compare one and four identical
projections, two disjoint key/text projections, and four callers split into two
repeated projection groups. Start each cohort from a bounded readiness
gate. Preserve all warmup and measured observations (one warmup plus three
measured cohorts per role/scenario); alternate observed and ordinary order.

The test-only reader wraps the real `ResidentFileReadAt` using the same descriptor,
allocator, provider handle, concurrency and per-call I/O scope. Record at most
4,096 completed read events per call, with start/end timestamps relative to the
cohort release. Errors invalidate this favorable-workload screen;
successful futures correspond to completed positional reads. Cancellation-time
physical reads are not inferred from this observer. Installing it creates an
additional test-only file view, so measure the ordinary path separately and
report instrumentation cost without subtracting it from another clock.

Each call produces bounded owned native output, then drains its I/O and checks
the source generation. Owned projections may retain encoded children; this is
read attribution, not a full-decode timer. Independent scalar verification of every typed output
and null is outside timing and must trigger no additional source reads. Bound
output rows, bytes and arrays; drop all results/source/session and require zero
remaining reservations. Report individual calls and complete cohort elapsed
time, never add overlapping intervals as saved wall time. Few cohort samples
are not production p95/p99 evidence. No result cache or external engine is used.

## Vortex-first provider check

Decision: `use_vortex_native_provider` for the existing observation path.
Pinned Vortex 0.85.0 provides `VortexFile`, cached `LayoutReader` trees,
`FileSegmentSource`, `SegmentCache` and the shared-future mechanisms already used
by ShardLoom's query-local segment reuse. Replacing a file's segment source clears
its cached reader tree. Its Moka byte cache does not supply ShardLoom's operation
cancellation, shared reservation, bounded consumer lifetime or source-generation
contracts. No new cache is justified merely because that provider exists.

Any admitted follow-up must preserve independent consumer errors/cancellation,
source-generation fencing, credits through the last owner, bounded slow-consumer
retention and drain of actual I/O. Sharing a future owned by the first caller's
cancellation scope is insufficient. Measure duplicate decode separately if reads
are inexpensive. Retention uses useful complete-operation gains with correctness,
resource and regression acceptance; the numerical serving target is a priority
target, not an automatic rejection cutoff. Full serving acceptance requires a
frozen arrival trace and adequate tail samples after a candidate exists.

No runtime throughput, latency, decode-reuse or production fairness claim follows
from this design or source audit. Broader serving/operator obligations remain open.
