# R4: bounded triple-key partition sort/reduce

Status: admitted for a bounded prototype, not yet retained. The implementation is
isolated from PR #1486; its final frozen runtime is the comparison control.
No new performance claim is established by this design note.

Current PERF-INTAKE candidate R4 / PERF-03/04/05/06. Preserve CG-1 through CG-23
and V1 candidate scope. No release or format pulse reopening.

Reuse the existing triple-role admission proof, producer-owned string domain,
typed numeric/minute/string-ID key construction, independent routing mixer,
64 complete partitions and exact output comparator. Reuse the numeric-pair
partition vector-growth reservation pattern and AggregateChunkJobs for EOF
sort/reduction. Do not add a scheduler, engine dependency or source representation.

Admit the sorted strategy only when the complete existing triple-key contract
holds and OFFSET+LIMIT is at most128. Large windows keep the existing hash
partitions from admission; no switch/replay after committing input. A small
private typed-key helper can serve both consumers; avoid duplicating key or
prepared-minute semantics. The nullable derived-minute/raw-input proof stays.

The producer appends every complete raw key to leased vectors. No sampled/local
winner elimination or pair deduplication can discard weight. Vectors charge old
plus new capacity before growth, then exact observed capacity. Existing interner
and upstream accessor scope stays explicit. This may use MORE memory:100m keys
alone occupy2,399,939,928bytes before capacity slack, versus roughly2.3GB peak
modeled pool credit in the recent hash control. Measure actual RSS.

At EOF move the completed producer interner into one Arc, with no string or
directory clone. Parallel partition reducers read that immutable interner for
the existing complete-key comparator. Sort by all physical key fields only to
cluster equality; output ties still use typed numeric order, minute and UTF8
value. Count complete adjacent runs with checked arithmetic. Retain only the
local bounded Top-K of each COMPLETE partition, then merge the bounded union
with the same comparator. Move the original interner back after all jobs finish.
On failure there is no result or replay; all captured vectors/interner/leases
must drain and drop. Cancellation is checked before/after each in-place sort
and periodically during append/reduce; do not claim interruption inside std sort.
Propagate the caller operation token to new workers using the existing child
token mechanism. Do not cancel the caller on healthy cleanup or retirement.

Record source chunks separately from EOF selection jobs. Preserve observable
rows, complete groups, duplicates, capacities, old/new growth overlap, buffer
peak, source/interner scope, route time, worker sort/reduce spans, caller merge,
and complete elapsed/RSS. Worker spans overlap and are not exclusive CPU time.

The latest completed pre-review Q19 cohort at c557 had complete calls4.255–4.297s,
caller accessor2.175–2.193s, binding0.996–1.011s, routing0.609–0.617s,
join waits0.102–0.104s, selection0.102–0.104s. Worker update sums3.169–3.214s
and lock wait1.388–1.453s overlap these phases. Do not assume all update work is
on the critical path. The final PR1486 cohort will be the actual control context.

Preserve existing fourteen triple tests. Add unsigned key extrema, lexical ties
whose string IDs disagree with lexical order, actual prepared native admission,
source owned-denial/corruption no-replay with refund/fresh recovery, and bounded
selection-job/cancellation/lease checks. Assert actual route activation. Wide
windows must demonstrate original hash admission rather than huge sort output.

Vortex-first: implement_shardloom_kernel. Existing Vortex0.85 scan, array and
numeric/UTF8 accessors remain the provider boundary. The already certified
ShardLoom COUNT/key-domain/order/memory contract is what changes; no external
integration or decoded Arrow execution is introduced. The pinned 0.85 sources were checked: `vortex-array/src/aggregate_fn/fns/count/grouped.rs`
requires already grouped List/FixedSizeList values through `GroupedAccumulator`;
`vortex-scan/src/strict_sorted_buffer.rs` validates already sorted unique row
indices. Neither inspected surface constructs flat complete triple-key COUNT
partitions, owns this query's leases, or applies the certified output comparator.
This is a bounded provider inventory, not a claim that upstream has no grouping APIs.
The native execution and Native I/O reports remain the certificate boundary:
`fallback_attempted=false`; no residual executor or decoded Arrow substrate is added.
V1 candidate and CG-1 through CG-23 gates retain their existing scope.

Screen complete Q19 against the final merged R3.b frozen executable: three calls
per role, guarded sequential native CLI including output/exit and all exact
values. Preserve all samples; retain useful smaller complete-call gains while
reporting memory tradeoffs. Drop the runtime if there is no useful gain. Retained
work gets required workspace/native gates and Full43, a cohesive PR and exact-head
CI. On drop retain the reproducible patch/evidence and restore only owned candidate
runtime changes, then advance R6.c, R10, R2.b and profiling refresh.
