# Writer subtree occupancy — R9.b

Status: bounded next-input preparation prototype admitted; no gain retained.
PERF-INTAKE / RFC 0044, after R9.a's fragment-reuse audit.

The retained writer awaits one source-batch subtree at a time; column/zone/codec
work already overlaps within that subtree. Existing conversion wait and summed
codec spans do not locate recoverable batch-tail capacity. The current full
ingest observation is 132.527161 seconds with 287.221417 user CPU seconds,
13.185876 system CPU seconds and 3,146,301,440 bytes peak RSS. That process-wide
ratio does not identify writer idle time or justify changing CPU allocation.
Its receipt is `ingest_cli_uat_gated_20260926T172832Z` in the local UAT workspace.

Freeze a bounded screen before changing scheduling. Read row groups 0, 113 and
225 from the resident official Parquet source, using the production dictionary
schema and existing lean derived-column preparation. Retain at most three
131,072-row batches per region. These are capped prefixes; tails inside the cap
are included, while row group 0's fourth batch is excluded. Convert into
reservation-owned native input before timing. Keep all 112 retained fields and
the existing source-text writer, row block 262,144, byte target 8 MiB,
compression/statistics concurrency four, and caller plus one provider driver.
Use a 2 GiB native pool, a 256 MiB output cap and sequential cases. No source or
conversion drivers run during this writer-only observation.

Use the pinned Vortex public `Executor`, `Handle`, `BlockingRuntime` and native
layout interfaces as a test-only observer over the existing current-thread
runtime. Preserve task cancellation/detachment semantics and the strong runtime
owner until drain. Measure CPU tasks queued/running, synchronous future poll
occupancy, each child wall interval and its terminal drain. Count a thread once
when scopes nest; keep blocking-I/O pool work separate from provider drivers.
Record all underoccupied intervals and the CPU-queue-empty subset separately.
Child records are in input order. The analysis labels their index and whether
a prepared successor exists; final-child occupancy is kept separate from the
ready-successor opportunity estimate.
The async runnable queue is not observable through this wrapper. Poll scopes can
include synchronous I/O, blocking work and preemption; they are not CPU time.
The complete writer and each input poll are also observed. The fresh-input
child-only screen omitted file-level statistics outside child intervals and
cannot support a drop decision. Its successor observes the full interval.

All prepared batches being ready is a favorable opportunity screen, not a
production prefetch promise. Record per-batch retained bytes and peak credits;
identify whether a ready successor can fit the intended memory budget. Compare
ordinary and instrumented retained-writer runs to expose instrumentation cost.
Prepare fresh native arrays for each sample so a preceding write or verification
cannot warm their statistics. Preserve the initial reused-input screen as
diagnostic evidence, but use the fresh-input screen for the admission decision.
Verify complete reopened values and schema outside the measurement. Retain all
samples, source/binary identities and guard receipts. Payloads stay in bounded
memory or guarded local scratch and are released after each case.

Repeated material unused capacity can admit one bounded two-subtree prototype.
The estimate is neither an achievable speedup nor a strict full-ingest upper
bound. A screen without credible material headroom parks overlap without claiming
it can never help. Retention still requires at least 10% lower complete guarded
ingest under matched resources, no larger artifact, exact full values/schema/
statistics/reopen checks and query regression acceptance. No new CPU allocation,
codec, file topology, external engine or publication semantics is admitted here.

## Admitted narrow prototype

Complete attribution identifies serial input polling (including existing file
statistics) while only one provider driver is occupied. Most samples show
14–16% total unoccupied capacity, so test overlapping one `input.next()` with the
current child. Poll the child first; retain at most one next array and its native
credits. Keep one serial statistics accumulator and one live child strategy.
No extra worker or concurrently buffered second encoded subtree is introduced.
Child errors drop pending input; input errors preserve their position after the
current child. Cancellation and pressure must release all retained owners.
The initial prototype is test-only and defaults off.
The existing conversion producer replenishes its prefetch window when a native
array is yielded, so this adds one live input beyond the current writer and
existing producer queue. Native reservations remain attached to that input;
unchanged worker count does not establish unchanged memory use. Synchronous
statistics and source joins are not interruptible mid-poll. Pending futures and
their retained owners must still drop on errors or outer cancellation.

The complete observer at `1872e55f` passed all 18 full-value checks and the
watchdog/source-identity gates. Across its three profiled samples per region:

| Source row group | Complete writer unused capacity | Input-poll time per write |
| --- | --- | --- |
| 0 | 14.92–15.97% | 59.55–61.10 ms |
| 113 | 15.82–16.23% | 38.50–39.18 ms |
| 225 | 9.97–14.11% | 44.86–50.27 ms |

These capacity estimates include terminal children and first input preparation;
those cannot all overlap a preceding/following batch. The raw records retain
input-poll order and child order so those boundaries remain visible. The loaded
367.93 ms row-group-225 sample remains included. The reproducible
[summary](../benchmarks/writer-subtree-occupancy-2026-09-26.json) links all three
raw screens, frozen binaries/source hashes, OS process counters and guard receipts.

Compare fresh-input control/candidate samples in alternating order with the same
retained writer, source regions, field set and memory/CPU/output limits. Charge
the complete writer, flush and driver teardown. Independently reopen every value
and compare complete file statistics and artifact bytes before considering full
guarded ingest. All attribution observations remain evidence, including warmed
and incomplete earlier screens; none is an achieved speedup.
