# Exact numeric and UTF8 COUNT workers

Status: retained under RFC 0044, with scoped public and independent correctness
evidence in the [continuation checkpoint](../benchmarks/perf-native-continuation-2026-09-06.md).
This is one PERF-04/05 family, not completion of either phase. The September 8
worker-admission repair below has separate correctness checks; existing measured
timings remain pinned to their original source commit.

The native aggregate path admits two identity grouping columns, one non-null
integer of any supported physical width and one non-null UTF8 column, COUNT(*),
descending count order and an explicit limit/offset. Roles come from schema and
expressions, independently of names and column ordering. Existing native paths
continue to handle other shapes. No new public option, external engine or spill
path is added.

Nullable root structs are rejected before worker admission even when their
declared child fields are nonnullable, because logical projection carries the
parent validity. Successful numeric provider executions feed the existing decode
and materialization report; already-primitive inputs add no decode call.

## Provider and ownership contract

Pinned Vortex 0.85 supplies retained ArrayRef/PrimitiveArray/VarBinViewArray,
Dict codes and values, execution contexts and the existing native scanner.
Native Dict codes remain bound to the same retained values owner. Canonical
numeric decoding preserves original-width native buffers; canonical string
decoding is an explicit provider boundary. No Arrow or StatValue row vector is
the execution substrate. ShardLoom supplies exact compound counting and resource
admission; Vortex provides no equivalent certified grouped-count reducer.

Each worker produces every distinct compound key from its source chunk. A
partial retains its native string domain and a byte-reserved hash table. Equality
checks numeric bits, signedness and complete string bytes, never hashes or local
dictionary codes alone. No partial top-K is permitted. Existing bounded compute
jobs retain the source, partial, metadata credits and queue permit through merge.

Complete compound keys enter deterministic logical partitions on those workers.
Each partition owns a charged string domain and a separate compound-key table;
many numeric keys sharing a string do not copy that string per compound entry.
Strings may occur in several partitions and those bytes are counted. Exact
entry credits and byte reservations apply before insertion or vector growth,
including simultaneous old/new capacity. Credits remain attached until release.

When state cannot grow, counting joins outstanding jobs before handing complete
committed counts and only unconsumed partial suffixes to the existing weighted
numeric/UTF8 native candidate and exact-recount route. Every source row contributes
once. No corruption or cancellation is reclassified as resource pressure. The
existing single full source replay, gated by typed owned-resource denial, retains
the same VortexFile and source generation. Final partition selection runs only
after EOF and uses the existing complete tie comparator with offset + limit.

The initial task reservation has its own typed denial outcome before any ordinal,
queue permit or job is published. If another owner consumes capacity after the
availability snapshot, complete prior work drains and the untouched current chunk
returns to the existing exact native route. A provider error after admission is
never classified from an unrelated reservation-counter change.

CPU ownership follows actual worker admission as well as shape/schema planning.
When a caller-only session's real worker allocation declines, joined provider
drivers start on that same runtime before the scan. No input has yet contributed,
so this path neither reopens nor replays the source. Successful worker admission
does not create a second provider pool. If a later typed source-allocation denial
requires the existing replay, aggregate jobs are cancelled and joined before
temporary provider drivers start for the same-file replay.

The driver count is carried as typed execution evidence into an enclosing segment
reuse report. Its scope is the completed scan and wrapper-owned drivers; inner
drivers from a failed outer cache attempt are excluded, and it is not a total
thread or CPU-time count. Actual reservation-denial and cache-wrapper tests at
1/2/4 lanes verify complete ordered results, one source open, advancing execution
counters, matching driver evidence and released owned credit.

The claim covers explicitly reserved adapter/operator capacity. Upstream native
allocations that bypass HostAllocator, allocator metadata and total RSS remain
outside that enforcement claim. Existing output-state accounting is reported
separately. Unsupported paths fail explicitly; fallback_attempted=false and
external_engine_invoked=false remain required.

## Acceptance

Tests cover all integer widths/extrema, renamed/reordered schemas, conflicting
dictionary codes and duplicate dictionary values, collisions, empty/nullable
admission, skew/all-unique keys, a global winner absent from each partial top-K,
ties/offsets, byte and entry pressure, suffix handoff, failure/cancellation and
source invalidation. Full results must match an independent Rust reference at
multiple worker counts. Evidence separates native decode, partial count,
reconciliation, final selection, caller handoff/recount, observed capacity and
copied string bytes. Retention requires paired public Q17 and independent
compound-key measurements; timing or memory regressions must remain explicit.

Root owns all compilation, tests and benchmarks in the shared target. The linked
checkpoint records the retained workload's measurements and limitations; this
contract does not extend those claims to the later admission repair or all inputs.
