# Concurrent native serving

Implementation in progress under RFC 0044 and the maintainer's explicit native
runtime completion priority. Native tests and bounded fixed-arrival load receipts
pass. This does not close production serving or the broader operator/public-availability matrix.

The default resident constructor keeps exclusive batch admission and its current
CPU grant. An explicit typed serving policy bounds queued calls/ticket metadata, general
call CPU grants, and native positional I/O requests/bytes across prepared sources.
General calls retain their fixed CPU grant including the caller. At P >= 2 a
separate one-CPU metadata lane can remain available while general work is active
when `reserve_metadata_lane` is enabled. The queue-byte counter covers admission
tickets, not request payloads retained by waiting callers.
FIFO applies within each class. P1 has one general lane and cooperative progress;
it does not promise concurrent CPU execution or preempt arbitrary native callbacks.
Multiple general reads can overlap when at least two general grants fit. Small
result limits do not classify full filtered scans as metadata or bounded work.

Serving sessions create no persistent background CPU drivers. Existing bounded
per-operation aggregate/provider groups may use only their granted lanes and
must join before admission is returned. The implementation does not describe
these groups as persistent, or claim a process-wide limit across independently
constructed sessions. Composed native operators borrow the same execution context
and shared memory pool; nested admission on the same caller is rejected explicitly.

Vortex-first provider check: use pinned Vortex 0.85 CurrentThreadRuntime's documented
shared executor, VortexFile, FileSegmentSource and VortexReadAt. An operation-scoped
FileSegmentSource uses the existing held descriptor and parsed footer. It rebuilds
the serving call's reader tree without reopening the source or caching an answer;
ordinary batch reader caching remains unchanged. Native allocator ownership and
exact generation checks remain in force. No external engine participates.

Native file read guards move into the actual blocking closure and completion
envelope. Scope closure refuses late I/O and drives cleanup until admitted blocking
jobs release their guards. A discarded buffer drops before its job is declared
drained. Running OS reads cannot be interrupted; cancellation at that boundary is
cooperative and does not promise a fixed completion deadline. I/O caps are explicit
admission limits, not OS RSS bounds or device-byte measurements. Provider metadata,
allocations bypassing the existing allocator and independent provider open_path
calls are not certified by these positional-reader counters.
An I/O envelope denial returns an explicit error; the I/O layer does not wait for
another operation's request or byte credits. Call admission waits in its bounded
queue and observes cancellation separately.

Acceptance must exercise exact public prepared projections and counts concurrently,
real native writing, queued cancellation before provider work, per-class ordering,
closed/full queues, P1/P4 CPU bounds, generation changes, foreign/nested contexts,
retained outputs, I/O cancellation/drain and reuse. A separate fixed-arrival harness
must retain scheduled/actual arrival, admission, completion and rejection records;
complete values, p50/p95/p99, throughput, resource counters and host context remain
required before a serving latency claim. Existing channel-held and closed-loop
fixtures do not constitute this evidence.

## Bounded executable load harness

`resident_serving_load_harness.rs` adds an ignored, explicitly invoked test with a
fixed arrival schedule. Each mode submits 96 scheduled requests to eight client
threads and a 16-slot dispatch queue. Submission uses `try_send`; a full client
queue records a rejected arrival rather than postponing the schedule. The mix is
footer counts, full 4,096-row projection plus JSON delivery, and six native writers
of 32,768 rows each. Every completed operation has an independent complete-value
check. Temporary source/output files are owned by the fixture and removed on drop.

Run under the repository's normal external build/log storage supervision:

```sh
SHARDLOOM_SERVING_INTERVAL_US=1000 cargo test -p shardloom-vortex --features release-user-surfaces serving_fixed_arrival_load_receipt -- --ignored --nocapture --test-threads=1
```

The interval accepts 100–100,000 microseconds; workload size and dispatch capacity
remain fixed and bounded. The harness prints its configuration and one JSON record
for each of exclusive batch and serving. Each mode retains all scheduled/submitted
arrivals, client dispatch, first native queue/service time, delivery, client release,
errors and rejections. Per-family p50/p95/p99 use scheduled-to-delivery time. JSON
delivery includes the result sink's separate admission and work; its queue is not
misreported as first-stage native queue time. Independent oracle checks happen
after delivery and keep the client occupied until their separately recorded end.
Reported throughput includes those checks and final drain.

Final leases, active CPU credits, queued calls and active positional I/O must reach
zero. Peak admission and I/O counters are retained alongside every request receipt.
Any engine error fails the harness after printing its receipt. Client overload is
reported explicitly, and latency must always be read with its rejection rate.

This small local workload establishes reproducible harness behavior and bounded
native progress. It does not establish production-scale latency, throughput,
fairness, arbitrary callback preemption, or whole-process memory bounds. Exclusive
mode runs first; comparisons need externally repeated/interleaved runs, source and
binary identity, host contention, and OS RSS evidence. P1 semantics are covered by
deterministic ownership/cancellation tests; this overlap harness requires P >= 2.

## Local validation receipt

The seven concurrent-serving tests pass, together with a prepared-spill queue
cancellation test covering text COUNT, compound COUNT and integer DISTINCT.
The explicitly invoked fixed-arrival test passes at both 1,000 and 100 microsecond
arrival intervals. In the 1,000-microsecond receipt, serving completed all 96
requests with complete-value validation and no client rejection; exclusive batch
completed 39 and recorded 57 client-queue rejections. At 100 microseconds, serving
completed 45/rejected 51 and exclusive batch completed 30/rejected 66. Both runs
reported zero engine errors and zero final owned reservations. Serving observed
at most four active CPU lanes and two positional reads; all queued/active I/O
and CPU counters drained. These are individual debug-build harness receipts,
not production performance comparisons or a throughput retention gate.

Raw request records, quantiles and command receipts remain under
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/` as
`admission-runtime-completion-serving-load-2.log` and
`admission-runtime-completion-serving-load-burst-1.log`, with adjacent JSON command
receipts. The first command rebuilt the test binary, so its external `time -l`
peak includes compilation and must not be reported as query RSS. The host is the
local Apple M5/10-logical-CPU Mac; requested serving parallelism is four and
source data/arrival geometry are printed by the harness. OS page cache and other
host activity remain uncontrolled. The failed first harness attempt is retained:
it used a single-leaf writer with eight chunks; the corrected harness reuses the
existing bounded sequential native Flat writer and validates all 32,768 output rows.
