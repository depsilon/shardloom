# Scan-local compressed segment reuse

This candidate continues RFC 0044 and PERF-03/PERF-09/PERF-12 from the
retained PR 1435 implementation. It is not accepted performance evidence until
the paired completed-read and lifecycle measurements pass. Numeric execution,
text layout policy, predicates, and external-engine boundaries do not change.

Automatic admission is limited to aggregate scans whose exact lowered `And`
predicate repeats a scalar field that is absent from the actual projection,
including residual-predicate projection requirements. The source must have a
native Struct root, whose readers separate fields. Flat, Chunked, and other root
wrappers are rejected without visiting children. The gate reads at most 256
root schema fields, 64 projection fields, 128 predicate nodes, and 64 distinct
leaf references. It does not open a second source, execute a scan, or walk a
footer's field/row-group descendants. This is a bounded layout heuristic, not
a proof of disjoint segment IDs or read savings for every admitted file.
The retention limit is the smaller of one sixteenth
of the shared session budget and 64 MiB, with admission disabled below 1 MiB.
Individual segments are limited to the smaller of that limit and 16 MiB, and
the table holds at most 128 entries. This is a duplicate-consumer opportunity,
not a prediction that every admitted source benefits. No new public knob or
null guard is introduced; ordinary single-consumer scans keep their path.

## Provider grounding and contract

Pinned Vortex 0.85 provides `SegmentSource`, `SharedSegmentSource`,
`SegmentCacheSourceAdapter`, and `VortexFile::with_segment_source`. The last
method clears cached layout readers, so a replacement source can serve the
unchanged native layout/scan implementation. The stock cache adapter registers
its downstream request before checking the cache. Its Moka implementation
weighs a returned slice's logical length, which need not describe the full
coalesced allocation retained by that slice. A cache-get error is also treated
as a miss. Those semantics do not establish this candidate's admission contract.

The implementation therefore wraps the native source at one admitted execution
boundary. It checks retained data before downstream registration and shares
concurrent misses using the provider's futures sharing primitive. A bounded
sorted entry table limits retained entries and registered shared misses. Completed entries
are evicted under entry or byte pressure; all active consumers retain their
own allocation credits. No query result, decoded array, or cross-query answer
is cached.

Source identity belongs to the prepared source's retained file handle. The
wrapper validates that generation before delivery, and the execution boundary
validates before and after the complete operation. A detected mutation latches
invalidation and clears retained entries. Engine-owned immutable sources use
their existing immutable-source contract. This is detected-generation safety,
not an OS snapshot against arbitrary concurrent file mutation.

Unknown source-buffer ownership is never inferred from logical slice length.
A retained compressed segment is copied once into a known host allocation.
Before that allocation, its complete pinned-provider capacity (logical length
plus preferred alignment) is admitted under both the session budget and a
separate cache-retention limit. The source allocation and copied allocation may
overlap and remain independently owned. Clones and slices retain both credits
until the final owner drops, including after eviction or scan close. Oversized
segments, a full in-flight table, or unavailable optional cache credit bypass
retention through the same native source; source read errors are propagated.
No pressure wait holds the entry lock or waits for a consumer to release data.

Optional table reservation/allocation refusal executes the callback uncached.
If a terminal native scan error is specifically the typed owned-allocation
denial, the callback can request one uncached replay. It returns before the
wrapper closes and drops its cache/file owners, validates the generation, and
executes fresh query state on the original prepared file. Error strings and
denial counter changes cannot classify this signal, so simultaneous corruption
is still an error. The replay cannot request a second cache replay. Typed
worker-materialization pressure has its own compound-worker handoff; decoded
errors outside a typed signal boundary are not inferred to be retryable.

The wrapper closes after the admitted operation. Closing rejects new work,
clears retained entries, and prevents in-flight work from publishing into the
cache. Dropping the last request cancels its shared future. Native blocking
reads already underway may finish after cancellation; their allocator owners
and the read observer's drain boundary remain responsible for that lifetime.
The active-request counter is independent of table membership and remains
nonzero across close until the corresponding futures release their owners.
Requests bypassing a full table remain subject to the native caller's existing
request/concurrency bounds. Their future metadata is not charged as payload.
Cache close and callback-state release are not described as a drain of native
blocking reads. Outstanding native I/O retains its original credits during a
replay. Discarded-attempt and uncached-replay wall spans are separate evidence.

Compound worker admission is checked against the prepared schema and exact
request before selecting CPU ownership. Unsupported two-key shapes temporarily
restore the missing provider drivers inside the existing execution gate; that
callback receives no dedicated aggregate worker pool. Driver creation is
bounded by the same caller-plus-workers ceiling and teardown joins the group.
Both cached and uncached variants retain one prepared source open. Reported
temporary driver counts are scoped to the operation, not resident idle state.

## Evidence and acceptance

The report separates hits/shared misses, downstream logical segment bytes,
copied compressed bytes, eviction/bypass counts, retained entries, and live /
peak allocation credit. Logical segment completion is not filesystem I/O.
The test-only retained-file observer separately measures completed positional
read calls, ranges, and bytes including repeats and coalescing gaps. Those are
OS reads, not cold-cache device bytes. CPU time and copy work are reported
separately; cache hits alone cannot establish a performance benefit.

Required focused acceptance includes sequential and concurrent same-segment
consumers, exact returned bytes and complete native scan values, a duplicate
consumer fixture with fewer completed positional-read bytes, pressure eviction
with retained slices, shared read errors and retry, cancellation, close,
generation replacement/mutation, oversized segments, zero-length segments,
and zero owned credit after every owner and pending read has drained. A paired
lifecycle comparison must include the copy cost. Provider metadata, arbitrary
upstream scratch, OS page cache, and process RSS remain outside the payload
reservation claim. No new dependency, unsafe code, decoded cache, or external
execution fallback is introduced.

The corrected focused run passed all 19 cases. Explicit sequential native segment
consumers reduced completed positional reads from 2,594,056 to 1,297,028 bytes,
with 1,297,028 compressed/serialized bytes copied. Those diagnostic wall spans
also included reference-byte verification setup and are not accepted latency
evidence. The whole-struct Flat predicate fixture read 1,297,028 bytes with
either setting: its projected identifier pins the same native shared segment
needed by the text predicates, so extra retention provides no read saving.
That negative result remains a hard no-gain control. A Table layout with
independent field segments reduced completed positional-read bytes from
2,462,688 to 1,296,976 while returning every expected nullable-filter result
and exact large integer. This supports the narrowed filter-only-field/Struct-root
admission; predicate-name duplication alone is insufficient. The two-range
typed-pressure test uses the existing bounded sequential native Flat writer
and proves one complete uncached replay on the same source and budget. The
subsequent admission tests check the measured positive/negative layouts,
projected filter fields, unsupported roots/nested fields, bounded traversal,
generation validation, and no additional open or execution; their final gate
results remain pending the root agent's serialized validation.
