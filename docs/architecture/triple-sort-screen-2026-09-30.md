# R4: bounded triple-key partition sort/reduce

Status: **drop runtime**. All three complete Q19 calls were slower. The lower
observed RSS is retained as a tradeoff, not a speed gain or a reason to change
the default path. The original hash implementation is restored, including its
original key builder; no prototype runtime or test helper remains.

## Recorded decision

The frozen prototype is `910b059f79cec1f6dd98f91171861daf36cf2bea`, binary SHA-256
`07214525436502080b79360310ac673a64dd980e422b727bb3be3396e935b663`.
Its control is the final R3.b executable `dff85c33763ac773c51ca1dd5e61a675cef6e20f`,
SHA-256 `f2b0fb728093cdee69aa0ad127d8d4d75be564c4c442e64ad8d849ab5c94009c`.
PR #1486 merged that runtime at `4ae717a7d67d1d8ffe7aa384c5c96bc48413be44`;
the Rust/Cargo diff from the frozen control to that merge is empty.

| Paired run | Control | Sorted candidate | Control peak RSS | Candidate peak RSS |
| --- | ---: | ---: | ---: | ---: |
| 1 | 4.239155 s | 4.985931 s | 4,318,953,472 B | 3,590,111,232 B |
| 2 | 4.122425 s | 4.403890 s | 4,274,143,232 B | 3,588,456,448 B |
| 3 | 4.123709 s | 4.431523 s | 4,268,048,384 B | 3,659,874,304 B |

Best time regresses **6.83%**; median time rises from 4.123709 to 4.431523 seconds.
Each RSS observation improves about 14–17%, but the memory acceptance contract
also requires no complete-call time regression. Modeled peak reservations
increase from 2.31–2.33 GB to 2.68–2.69 GB. This is not a cutoff-only rejection
of a positive speed result. The paired harness's coarse regression flag requires
more than 10%; its empty flag list does not override these slower observations.

Both routes account for 99,997,497 rows, 56,384,822 complete groups and 43,612,675
duplicate updates. The candidate records 1,550 source chunks and 64 joined
partition jobs. Its EOF finish takes 0.514–0.523 seconds; the control's join waits
are 0.095–0.110 seconds plus 0.088–0.093 seconds of final selection. Candidate
routing takes 0.696–0.705 seconds versus 0.585–0.593 seconds for the control.
These boundaries support retaining the existing overlapping hash route for
this workload. Worker sort/update spans overlap; they are not exclusive CPU
time or additive savings. No host-load cause is assigned to any sample.

The guarded alternating cohort is `paired43_20260930T043424247863Z`; all six
complete results match the retained native references, including exact unsigned
values. It uses the same 99,997,497-row/112-column optimized Vortex artifact and
Apple M5 Mac17,3 with 10 logical CPUs/16 GiB RAM. Calls include process startup,
full CLI output and exit. Requested 24 GiB is not a physical-memory/RSS cap.
Caches are uncontrolled and unrelated host work is accepted. No new ingest,
format-pulse, cold-storage or production-serving claim is made.

The prototype passes formatting, native all-target Clippy and 18 focused tests,
including prepared/owned/export, source-denial/corruption recovery, active worker
cancellation, refund, exact ties, nullable decline and large-window hash retention.
Two initial test-style Clippy errors were corrected. Full43 and broad workspace
runtime checks were not run for this rejected candidate; the final diff retains
only documentation/evidence. The
[portable evidence](../benchmarks/evidence/triple-sort-drop-2026-09-30.json.xz)
preserves all six outputs, samples, source manifest, reproducible prototype patch,
validation logs and bounded source review. SHA-256:
`9fc801f4ad6e70d48438f45ade540c65bcae0a9fd15274281a04870c7656a015`.
Independent audit verifies all six exact results, 24 raw archive members,
seven pinned source entries, eight portable text assets and the recomputed
time/RSS comparisons without a mismatch.
After that audit and the committed runtime restoration, the unused frozen
prototype executable was retired, recovering 64,036,864 allocated bytes. Its
receipt, source patch and complete evidence remain; reproducing the executable
requires rebuilding the pinned source. Inputs and current accepted controls
were verified and preserved.

Reuse learned: the existing typed key builder/comparator, owned job completion,
leased vectors and moved interner are sufficient building blocks. A new generic
sort/reduce framework is not justified by this result. Reopen only for a workload
that can demonstrate a favorable complete-operation time/memory tradeoff.

## Original admission and implementation contract

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
