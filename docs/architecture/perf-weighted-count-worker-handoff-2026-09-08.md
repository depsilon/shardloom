# Weighted COUNT worker-to-spill handoff

## Status and decision

Design for the next cohesive PERF-03/04/05/06 implementation under
[RFC 0044](../rfcs/0044-resident-runtime-resource-ownership.md). This note is
source-grounded planning, not an implemented or measured public worker-to-spill
path. The [continuation ledger](performance-continuation-2026-09-06.md) remains
the completion record. The active serial weighted spill integration still needs
its integrated validation gate; this note does not change that status or close
the broader aggregate/distinct/join spill item.

Add one optional partition-worker epoch to the explicitly admitted weighted COUNT
route. At pressure, stop admitting chunks, join healthy work, transfer every
complete committed key and deferred contribution into the already reserved
weighted accumulator, then consume the remaining source serially. At EOF, drain
the same complete state before final global selection. Do not restart workers
after the transition in this first implementation.

Preserve the existing public family: COUNT(*) over a nonnullable identity UTF8
key, optionally with one nonnullable integer key in either group order; native
pushdown predicates; positive checked limit/offset; count descending with the
currently admitted optional ascending group-key prefix. Preserve exact integer
bits/signedness and complete UTF8 values across dictionary domains. The caller's
existing aggregate spill policy authorizes temporary native runs. No new public
policy knob, external engine, general join spill, approximate state, cached
answer, or alternate runtime is required.

## Existing implementation seams

| Source | Current contract and required extension |
|---|---|
| `local_primitives/weighted_count_spill_query.rs` | Holds one native scan and serially calls `Accumulator::push_source`; its finalized owner survives the enclosing prepared source validation. Add the optional worker epoch inside this same operation |
| `local_primitives/weighted_count_spill_accumulator.rs` | Reserves the entire operator envelope from query memory before child allocations; holds native buffers, work, selection and lazy runs. The complete-key `transfer_drained_epoch` visitor is test-only. Promote it behind an owned, drained production boundary |
| `local_primitives/aggregate_count_workers.rs` | Single UTF8 workers have committed partitions, deferred `StringCountPartial` owners and uncounted `RetryString` arrays. Their current pressure destination is `GroupedAggregateStates` |
| `local_primitives/compound_count_workers.rs` | Compound workers retain the analogous committed/deferred state and numeric/text retry pairs. `try_submit` distinguishes an untouched chunk from an admitted job. Current pressure replay targets legacy in-memory state |
| `local_primitives/{string_count,compound_count}_partitions.rs` | Pressure stops partition growth. `replay_and_release` visits all committed complete keys and releases each partition after its visit. Partial suffix visitors preserve precisely the uncommitted weight |
| `local_primitives/aggregate_chunk_jobs.rs` | Source-order completions retain input/result reservations and window permits through their consume callback. Drop cancels pending jobs; it is not the healthy pressure-drain operation |
| `shardloom-exec/src/compute_pool.rs` | `ComputePool::drop` closes the queue, requests shutdown and joins background threads. The consuming drain must reach this boundary before provider drivers start |
| `local_primitives/aggregate_scan_runtime.rs`, `resident_session.rs` | Caller-only execution and temporary provider drivers already share the same `CurrentThreadRuntime`. The existing weighted entry currently uses the ordinary provider-driven resident session |

Existing single and compound worker admission is coupled to legacy grouped
state and count-only ordering. Extract an internal, typed constructor for an
already admitted weighted contract; retain existing public/no-policy admission
unchanged. In the new mode, group-order ties affect the final weighted consumer,
not partial counting. Do not fabricate a legacy state with a different order
request merely to bypass its gate. A rejected optional worker admission selects
the already admitted serial weighted route before any job contributes.

## Ownership and state machine

Use a private worker mode or narrowly factored shared counting core, not a second
copy of the worker algorithm. The weighted mode exposes admission, ordered
receipt collection and a consuming full-state drain. It cannot call the current
legacy `handoff`, `finish`, heavy-hitter initialization, unpartitioned replay into
legacy maps, or partition top-K selection.

The operator states are `Serial`, `Counting`, `Draining`, `Transferring`,
`SerialTail`, `Finalizing` and `Failed`. Only `Counting` admits new jobs. A normal
transition is `Counting -> Draining -> Transferring -> SerialTail`; at EOF it goes
from transfer directly to finalization. Any failed admission after a job ran,
worker/provider failure, transfer failure or conservation mismatch is terminal
for that attempt. A drained owner cannot be cloned or consumed twice.

A proposed private `DrainedWeightedEpoch` enum owns either the string or compound
partition Arc, every retained deferred partial, the bounded retry-array owners,
their handoff metadata lease, immutable worker evidence and exact row counters.
It owns no live `AggregateChunkJobs` or compute pool. Its fields remain private;
the only production consumer transfers it into `Accumulator`. This replaces the
test seam's caller assertion that jobs have already been joined with an ownership
boundary enforced by construction.

The consuming drain does the following:

1. Stop admission and request partition pressure. Keep the current source chunk
   outside the worker epoch if its submission did not succeed.
2. Join every submitted job in source ordinal order and consume every healthy
   receipt. Retain deferred and retry owners within the pre-reserved window.
   Validate joined/submitted counts and outstanding window permits.
3. Check the caller cancellation flag, capture final worker/partition evidence,
   and destroy the empty jobs owner and its compute pool. All background threads
   must be joined before constructing the drained owner or starting run I/O.
4. Transfer the partition/deferred/retry owners and their leases into the drained
   epoch. Drop obsolete worker contexts and session clones. No reference capable
   of mutating partitions remains outside the owner.

Do not call `jobs.cancel()` to implement healthy pressure draining: admitted jobs
can already have committed a prefix, and cancellation can destroy the receipt
needed to account for its suffix. Error/abort/replay instead requests pressure,
cancels internally, joins and discards the entire attempt without publishing it.

## Memory and progress admission

Keep one full `policy.memory_bytes` parent reservation in the query pool and one
operator child pool. Construct the existing accumulator first, reserving its
records, UTF8 arena, worst-case 64 KiB keys/heads, merge/conversion work, checksum
scratch, bounded selection and owner metadata. Workers and their native execution
allocator must charge this same child pool. Source-reader owners remain charged
to the configured source/query pool, as in the current adapter. Do not place
worker state in unreported query-pool slack outside the operator envelope.

Constructing spill state only after partition pressure is unsafe: it can require
a second full envelope while the worker prefix still owns the first budget.
Constructing the accumulator first is necessary but insufficient. Run footers,
paths and allocator-backed native read buffers are also admitted dynamically;
workers must not exhaust the credits needed to release their state into runs.

Before admitting workers, hold a separate **transition headroom lease in that
same child pool**. Derive a conservative finite bound from the maximum retained
partition entries plus all in-flight/deferred partial capacities and native run
overlap. Use explicit internal entry/capacity limits; do not infer this bound
from input row count, observed averages or current unique-key estimates. A
constant/dictionary partial can represent many source rows with few records.

The current run metadata formula is `4096 + 1024 * ceil(rows / block_rows)`.
For an admitted maximum of `R` complete records awaiting transfer, a conservative
metadata calculation must cover input-plus-output overlap of up to `2 * R`
record leaves at `block_rows = 1`, the live-run/header bound, workspace/descriptor
paths and reader-path overlap. This is a required term, not the whole headroom
formula: include actual allocator-backed footer/block overlap separately, using
the existing one-row-range-at-a-time native reader contract. Keep per-run actual
key/geometry optimization; the admission calculation remains worst-case.

Enforce the corresponding partition-entry and queued-partial limits before job
publication. A chunk whose conservative partial capacity does not fit is an
untouched serial-tail chunk, not permission to grow the epoch beyond its bound.
If no useful worker window plus transition headroom fits, decline optional
workers and execute serially. A 4 MiB policy minimum is not a promise of worker
admission, arbitrary retained output, or arbitrary run-footer capacity.

Hold transition headroom through healthy drain and compute-pool destruction.
Release that lease immediately before transfer, when worker allocations are
frozen; native run allocations may then consume its credits. Existing partition
and partial leases remain charged until their corresponding owned storage drops.
The child pool's lifetime peak includes simultaneous worker, emergency spill and
native-run storage. The full parent envelope survives final source validation
and the final owned result exactly as it does now.

The precise headroom helper and its capacity proof are implementation
prerequisites. Do not substitute a guessed percentage or silently claim an RSS
bound. Exhaustion from a source chunk, provider allocation outside the declared
coverage, admitted run quota or later serial-tail metadata still produces a
deterministic failure with cleanup under the existing contract.

## Exact source accounting and transfer order

Track these roles separately with checked arithmetic:

- `submitted_rows`: rows in source chunks whose worker submission succeeded.
- `completed_rows`: full input weight of successful count receipts.
- `committed_weight`: all complete-key counts retained in partitions.
- `deferred_weight`: only the unconsumed suffixes in retained partials.
- `retry_rows`: rows in admitted jobs that returned an uncounted immutable array.
- `direct_rows`: an unsubmitted current chunk and all subsequently read tail
  chunks accepted by the serial accumulator.

Before allowing tail input, require `submitted_rows = completed_rows + retry_rows`
and `completed_rows = committed_weight + deferred_weight`. A retry array has no
committed contribution; it is not the original whole array for a partially
reduced receipt. Current partition reducers already advance their cursor only
after successful complete-key updates and retain only the remaining weight.

Visit committed partitions first through `replay_and_release`; visit every
deferred owner once and drop it immediately after successful transfer. Each
visitor supplies complete text and optional exact integer bits/signedness with
a positive checked weight. Preserve the existing independent prefix/suffix sum
check in the accumulator. A visitor error may follow already released partitions
and written runs, so it makes the entire accumulator terminal.

After all counted state is released, feed each uncounted retry array once into
the serial intake. Add a shared admitted-column intake for the single text array
or `[numeric, text]` pair so retry does not reopen the source or reconstruct
materialized rows. Keep declared output key order distinct from this physical
input role order. Retrying needs no new worker jobs. Release each source owner
after it has contributed successfully; count these rows separately from the
completed epoch weight.

Then consume any current unsubmitted chunk once and continue the existing scan
iterator. `InitialCapacityDenied` must continue to mean no job ran and no ordinal
advanced; upgrade single UTF8 submission to the same typed distinction used by
compound workers. Aggregate input totals must equal transferred completed weight
plus retry and direct rows. Source scan evidence counts each successfully read
source chunk once, regardless of which consumer processed it.

At source EOF, drain and transfer all remaining complete state before calling
the weighted accumulator's one global finish. Do not feed partition-selected
candidates, local top-K, a heavy-hitter sketch or a partially refined registry
to spill. Global group reconciliation, count order, ties, offset and limit happen
only after every source contribution is present. Small/empty or highly repeated
inputs retain lazy workspace behavior whenever the resulting complete weighted
records fit the existing in-memory spill buffer.

## CPU, cancellation and source generation

The weighted request-only gate chooses a caller-only resident runtime when this
worker attempt is enabled. Schema and actual memory admission occur on the same
held file. If admission declines, restore temporary provider drivers on that
runtime before serial scanning. If workers run, they use at most `P - 1`
background compute threads plus the caller; start no provider driver pool beside
them. After their pool is fully dropped/joined, provider drivers may serve the
transfer, serial tail and final native merge. Keep that guard alive through those
operations and join it on every exit. Report actual worker/driver counts and the
phase change, including the one-thread case.

Observe both the internal jobs cancellation token and the caller spill-policy
flag inside long worker loops, queue waits and transition boundaries. Keep the
two owners separate. `AggregateChunkJobs::drop` cancels its internal token even
after a healthy drain; sharing that token's AtomicBool with the caller policy
would incorrectly cancel the subsequent spill phase. Add an optional external
flag check to the narrow worker context/admission plumbing, without allowing
internal shutdown to set the caller flag. Native blocking I/O retains its current
documented cancellation limits.

Before enabling retry in the new single UTF8 path, preserve typed provider errors
as `Counted` versus `OwnedAllocationDenied`, following `compound_count_partial`.
Its present string-match retry check is insufficient for this boundary. An
unrelated reservation denial concurrent with corruption must never turn the
corruption into a retry or serial continuation.

A failed source `scan.next()` is not an untouched retry array: the provider may
have advanced internally. Match the existing owned-source-pressure behavior with
at most one full serial weighted retry on the **same held file/session/runtime**,
only for a typed owned reservation denial during the worker attempt. First drop
the scan, cancel/join workers, discard all epoch/spill/evidence owners and clean
their runs; then create a fresh accumulator/registry with workers disabled. Keep
the caller cancellation flag intact. Never append a replayed prefix to surviving
weights or reuse the failed iterator. All other source errors remain terminal;
the retry attempt has no second retry. Record discarded-attempt work separately.

The enclosing prepared source validates its generation before and after the
complete successful operation, including any retry and merge. The finalized
result retains the parent reservation until that final check. Source mutation,
native-run replacement/corruption, quota/write failure and cancellation return
no partial result or passing certificate and clean only owned paths.

## Evidence and certificate changes

Keep weighted spill evidence separate from exact DISTINCT. Extend the typed
weighted report with worker admission/decline reason, submitted/joined jobs,
worker source rows, completed/committed/deferred/retry/direct rows, handoff reason
(`pressure` or `eof`), actual CPU ownership by phase, transition reservation and
worker/whole-operator peak bytes. Report discarded-attempt rows/time separately
if source replay occurred. Define counters at the typed producer, not by parsing
the JSON summary.

`source_rows` remains final accepted source weight. `source_records` remains
weighted records supplied to the spill accumulator; it can be much smaller than
source rows after partition aggregation. `initial_run_records` counts persisted
records after bounded-buffer coalescing. Preserve the minimum/maximum actual run
block rows and maximum actual key bytes, including mixed-key merges. The existing
full-envelope peak, quota/cleanup and input/output certificate checks still apply.
Add conservation and worker-pool closure checks; forged rows, phantom workers,
overlapping CPU pools or a handoff without a complete drain cannot certify.

## Smallest coherent implementation and verification

One implementation batch should include the typed complete-state drain for both
existing text worker forms; single-worker typed denial handling; transition
headroom admission; shared retry-column intake; the held-source weighted dispatch;
CPU/cancellation ownership; and typed evidence/certificate/public regression
coverage. These share one conservation and resource contract. Leave no-policy
workers, exact DISTINCT, compact-state experiments and general join spill outside
this batch. No new dependency or Vortex provider abstraction is needed.

Proposed test selectors below are acceptance work, not existing passing tests:

| Selector prefix | Required proof |
|---|---|
| `weighted_count_worker_drain_` | Real string and compound workers, forced pressure after a committed prefix, multiple deferred suffixes, bounded receipts, out-of-order completion joined in source order, no cancellation of healthy contributions, no pool surviving the drained owner |
| `weighted_count_worker_admission_` | Exact headroom/capacity arithmetic and overflow; near-budget worker decline before jobs; unsubmitted oversized chunk and initial-reservation race; all credits refunded on every declined/failed constructor |
| `weighted_count_worker_retry_` | Typed owned denial yields an uncounted retry owner; corruption plus concurrent denial remains error; retry-column shape/dtype and integer-width checks; failure after partial transfer is terminal |
| `public_weighted_count_worker_spill_values_` | Actual public native scan, renamed/reordered fields, both compound orders, all integer widths/extrema, changing/reversed dictionary domains, UTF8/long/mixed keys, late global winner, ties and offsets, full independent values after many runs/compactions |
| `public_weighted_count_worker_spill_lifecycle_` | Empty/all-filtered/small lazy route; no-policy unchanged; one-thread and multiworker operation; cancellation while queued/counting/draining/transferring; quota and run corruption; no thread leaks and exact parent/child refunds |
| `public_weighted_count_worker_spill_source_` | Same held file/runtime on full serial replay; no duplicated prefix; corruption cannot retry; mutation/replacement/truncation at final boundary; final result owner retains parent credit until source validation |
| `public_weighted_count_worker_spill_certificate_` | Forged source/completed/committed/deferred/retry/direct rows, input versus output/limit counts, CPU ownership, run geometry, quota and cleanup evidence fail certification |
| `public_weighted_count_worker_spill_sql_dataframe_` | Real SQL/DataFrame request with explicit workspace, full returned values and actual worker/handoff evidence, unsupported shapes rejected before effectful source/workspace work |

Run the existing private/public weighted, exact DISTINCT and aggregate-worker
regressions, feature/minimal/no-write matrix, workspace fmt/clippy/tests and public
certificates through the root-owned serial validation gate. Then measure frozen
serial weighted versus worker-weighted complete public executions under matched
source, codec, query/operator budgets and actual CPU grants. Include repeated,
skewed and high-cardinality keys, pressure/no-pressure cases, 1/2/4/8 grants,
short/long mixed keys and filtered/offset output. Record complete values, time,
owned/query peaks, RSS separately, source rows/bytes, weighted/native records,
disk overlap and cleanup. Keep the worker path conditional or unpromoted until
its retain/drop evidence supports a production default.

## Feasibility and remaining proof

The native ownership, complete-key visitors, bounded jobs, query reservations and
same-runtime driver primitives already exist; there is no identified dependency
or upstream-API blocker. The code cannot safely implement this by merely removing
`cfg(test)` from the accumulator transfer seam. The concrete prerequisites are a
consuming healthy drain, typed single-key denial classification, bounded emergency
run headroom, separate cancellation ownership and caller-only CPU admission.

The principal feasibility risk is useful worker capacity after reserving
worst-case transfer metadata/native read overlap within small operator budgets.
The admission helper and adversarial bounded-state tests must establish that
capacity before worker execution is promoted. Serial admitted execution remains
available when the worker subplan cannot fit. No latency or memory improvement is
claimed by this design; only source inspection and documentation checks have run.
