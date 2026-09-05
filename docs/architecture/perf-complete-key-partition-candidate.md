# Complete-key string count candidate

Historical C6 design: scoped Q34/Q35 acceptance and remaining family obligations
are recorded in the [retain/drop packet](../benchmarks/perf-drop-ship-2026-09-05.md).

This RFC 0044 PERF-04/05 experiment followed C4's measured string count
improvement. Q34's first C4 sample spent 10.635 seconds in caller reconciliation
for 28,311,069 chunk keys, versus 9.277 seconds of summed worker work. Those C4
measurements motivated moving complete-key identity and reconciliation into the
existing workers; C6's outcomes are recorded separately in the linked packet.

Admission remains one nonnullable identity string key, COUNT(*), reconstructable
constant group columns, descending count order, no HAVING, and bounded OFFSET +
LIMIT. Other C4 routes remain unchanged. Native dictionary codes are counted only
inside their own values domain; referenced values acquire a content hash before
partitioning. Constants remain weighted, without row expansion.

Every worker first counts all chunk keys, then groups those counts into 64 fixed
hash partitions. The same worker updates one leased partition at a time. Each
partition owns an open-addressed cached-hash/count/string-offset table and UTF-8
bytes. Full byte equality resolves collisions. A complete key has exactly one
owner across the whole query; no per-worker duplicate global dictionaries exist.
Partition locks serialize only matching partition updates, and lock wait is
reported separately from reconciliation work. The existing P−1 compute workers
and caller retain the total CPU ceiling.

All table and UTF-8 vector capacity is reserved before allocation, including old
and new storage during growth. The source window continues to bound queued,
active, and completed chunk work. Denied partition growth requests a drained
handoff: workers return unconsumed weighted suffixes with payload metadata credits;
the caller first joins every
job, then replays every committed partition count once into the existing native
count or heavy-hitter state and merges each suffix once. A canonicalization failure
while partition storage is retained may be retried once after this handoff only
when an owned reservation denial was observed, using the same retained native
array. A typed scanner allocation error and an observed denial delta allow one
complete native replay through the same retained VortexFile after all partition
jobs, state, and the failed iterator are dropped. That replay has no partition
workers and cannot retry recursively. Corruption and cancellation do not authorize
replay, and source identity validation still encloses the complete execution.
Discarded work and replay elapsed time are reported. No new query rejection is
justified by row-count estimates or by the new
representation's memory requirements. Legacy state after handoff retains its
existing memory scope and is reported separately from leased partition capacity.

Only after all source work completes does each partition select its final
OFFSET + LIMIT candidates, using count descending and UTF-8 ascending. The caller
selects from the bounded union. This is exact because every omitted key already
has that many better complete keys in its own partition. No chunk-local top-K is
used. Source identity remains validated after all jobs and any native refinement.

Validation must include exact public outputs at workers 1/2/4/8/12; candidates
outside every chunk's local top-K; collisions, dictionary domain changes and
duplicate dictionary values, constants, non-URL/Unicode strings, null rejection,
OFFSET/ties, checked overflow, pressure after partial progress, cancellation,
lease lifetime and release. These are the design's acceptance obligations.
Completed scoped validation, retention and later combined-source measurements
are maintained in the retain/drop packet linked above, rather than a separate
pending status in this historical note.
