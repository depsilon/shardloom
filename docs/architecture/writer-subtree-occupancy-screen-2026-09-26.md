# Writer subtree occupancy — R9.b

Status: attribution admitted; no overlap implementation or gain retained.
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
131,072-row batches per region. These are capped prefixes; only a tail inside
that cap is included (row group 225), while row group 0's fourth batch is excluded. Convert into
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
The async runnable queue is not observable through this wrapper. Poll scopes can
include synchronous I/O, blocking work and preemption; they are not CPU time.

All prepared batches being ready is a favorable opportunity screen, not a
production prefetch promise. Record per-batch retained bytes and peak credits;
identify whether a ready successor can fit the intended memory budget. Compare
ordinary and instrumented retained-writer runs to expose instrumentation cost.
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
