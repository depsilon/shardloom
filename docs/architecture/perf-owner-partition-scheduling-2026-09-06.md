# Private owner-partition scheduling experiment

This RFC0044/PERF-04/05 experiment compares scheduling after complete native string
partials exist. It is test-only and initially unregistered. Production admission,
query plans, tables, certificates and defaults do not change. The compact string
representation remains a separate, unmeasured decision.

Vortex-first provider check: pinned Vortex 0.85 `VarBinViewArray` owners and the
existing `StringCountPartial` remain the native data representation. ShardLoom's
existing `ComputePool`, complete-key partition reducer and entry credits already
provide the required runtime semantics. This experiment implements only private
range scheduling; no new provider, engine, dependency or filesystem effect is
needed. There is no external fallback or local top-K.

Both cases arrange the same bounded native partials once before measurement and
invoke the actual `StringCountPartitions::reduce_partition` method on identical
complete partition ranges. Dynamic scheduling uses one bounded queue shared by
the existing workers. Owner scheduling maps each of 64 partitions to one of P
logical lanes and uses a bounded queue for each of P−1 background lanes. The
caller is the final lane and also submits work. P is an explicit positive bound;
the experiment admits 1, 2, 4 or 8 lanes. There is no thread per partition and no
second runtime. Partition locks remain present in both cases; uncontended lock
cost is included, so lock-wait observations are not pure contention time.

Queue storage, queue/control objects, task handles, active range envelopes and
completed task envelopes are reserved before allocation. Each range retains an
Arc to its immutable partial and the partial's count/owner credits. Native test
fixture payload construction remains outside this reservation scope, with
separate hard row, string-length and logical-byte caps; the report does not claim
whole-process or complete provider-allocation accounting. Table and arena
capacities use the actual production reservations and global entry credits.

The producer never occupies an additional CPU lane. Owner queues can backpressure
the caller; every background consumer is submitted once to the existing persistent
pool before production starts. Waits check cancellation at bounded intervals.
Failure cancels admission, closes and discards queued ranges, and joins every
started task before returning. A partial range is never reported as a complete
result on pressure. This experiment fails explicitly on pressure; it does not
claim the production exact pressure handoff is implemented by this test adapter.

The paired release fixture covers repeated, skewed and high-cardinality strings,
positive weights, collisions, empty/Unicode/NUL values, and 1/2/4/8 lanes. At least
seven alternating measured pairs follow warmup. Both cases retain the same native
inputs and equivalent complete output maps across both timed cases. Construction,
range submission/update/drain and teardown are separate clocks. Native source
construction, hashing, partition arrangement, independent BTreeMap oracle,
complete output comparison and output hashing are outside these clocks.

Reports include exact rows/groups and complete output digests, actual queue peak
and active ranges, queue waits, actual reducer lock/reconcile timings, entry-credit
refunds, copied UTF8 bytes including arena relocation, table/state reservations,
and final zero refunds. This is an internal scheduling experiment, not public
query latency, a lock-free table, a compact-state retention decision or a general
performance guarantee. The five ordinary native tests pass in the root's serial
gate. The ignored paired release measurement and retain/drop decision remain
pending.
