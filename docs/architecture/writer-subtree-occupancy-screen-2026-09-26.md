# Writer subtree occupancy — R9.b

Status: retain shared-slot writer lookahead after complete ingest and paired Full43 acceptance.
PERF-INTAKE / RFC 0044, after R9.a's fragment-reuse audit.

The maintainer rejected the cutoff-only drop. The revised implementation retains
**6.52% lower complete ingest**, saving **9.406 seconds** in the matched
best-to-best comparison, while sharing the existing producer input-slot envelope.
The earlier 6.58% observation remains separately scoped historical evidence.
The producer refills at the next pull, after the prior child completes, rather
than immediately after yielding lookahead. Workers, configured window and
per-task credits stay unchanged. Generic streams remain sequential. The
[revised evidence](../benchmarks/writer-input-slot-retention-2026-09-26.json)
records full acceptance for PR #1465; R8 concurrent source reuse is next.

## Revised complete ingest

Frozen candidate `5409a7ba` and control `e662f674` ran C/A/A/C through the same
guarded CLI, resident 99,997,497-row source, P4 and 24 GiB allocation. These
measurements belong to the revised slot-sharing implementation:

| Order | Role | Complete CLI time | OS peak RSS bytes |
| --- | --- | --- | --- |
| 1 | Control | 144.348138 s | 3,108,405,248 |
| 2 | Candidate | 134.942151 s | 2,968,305,664 |
| 3 | Candidate | 135.190966 s | 3,023,683,584 |
| 4 | Control | 148.315027 s | 3,028,516,864 |

Both candidate samples are faster than both controls. The same fastest-valid
rule applies to each role, with every sample retained. Cache and host activity
remain uncontrolled; do not combine this comparison with earlier timings from
different code. Every complete output matches the retained artifact's SHA-256
and 15,682,956,116-byte size. Each duplicate was removed only after its whole-file
hash and source/reference/binary generations passed. Hashing is outside the
native ingest clock. Publication and durability semantics are unchanged.
Maximum reported native reservations are 6,614,972,633 bytes for control and
6,797,471,986 bytes for candidate; all four runs have zero denials and zero final
reservations. The input-slot envelope is preserved, but scratch overlap and
reported reservation peaks need not be identical. This is not a general memory
reduction claim.

The same frozen control executable previously completed in 98.982644 seconds,
with a second sample of 219.553181 seconds. Its revised-session best is
144.348138 seconds despite unchanged binary, source and resource settings.
The earlier prototype's 92.465509-second result is valid historical evidence;
the newer absolute times do not isolate a code regression or a particular host
process. Native CPU counters also vary. Concurrent activity, scheduling, cache
and other host conditions were not independently controlled or attributed.

## Paired query acceptance

Both frozen executables ran every ClickBench query three times against the same
retained artifact, P12 and 24 GiB, alternating role order. All **258 complete
results pass**, with final binary/source identity checks. The best-of-three
query sums are **83.274614 s control** and **81.519259 s candidate**; geometric
means are 0.769216 and 0.763637 seconds. No query is slower by both 10% and
150 ms, so the predefined screen requires no focused follow-up. These are query
regression observations, not an attributed query speedup from an ingest change.
Returned values are compared with retained ShardLoom outputs, including finite
float tolerance of 1e-12; this is a regression oracle, not an independent engine.
All samples, host observations, identities and validation receipts are archived
with the revised evidence. OS cache and host activity remain uncontrolled.

## Shared-slot implementation

The revised candidate uses a private one-use producer grant. Native owned input,
a nonzero conversion window, the retained-row layout and a background provider
driver are required. Without that grant, the writer remains sequential. The
producer keeps its original workers, window W and per-task credit, but refills a
handed-off slot on the next pull. The input envelope is therefore
`current child + next input + (W−1) producer slots`, replacing `current child + W`.
Only one child writer and one statistics accumulator remain active. Empty input
owners drop before another pull; failure and cancellation drain the retained
source, conversion work, input owners and unpublished staging.

This uses the pinned Vortex 0.85 native layout/sequence/runtime APIs and the
existing ShardLoom bounded writer. It adds no codec, file format, worker, external
execution provider or public tuning option. Native persistence and no-fallback
contracts are preserved. The route evidence records the configured lookahead
capacity and existing-producer-window scope, not observed queue occupancy.

Validation at `5409a7ba` includes the default workspace gates, native-feature
Clippy and all-target tests (1,937 native library tests passed, 15 explicitly
ignored measurement/regeneration helpers). Six producer tests cover W1/W3 boundaries, admission,
ordering, empty input, pressure, cancellation and owner release. Eight layout
tests cover child/input ordering and failures. Actual serial and shared-slot
writes produce identical files at 5/6/8 MiB, and the codec-held cancellation test
checks cleanup with the second native input live. These checks establish the
slot contract and availability at those budgets, not equal scratch peaks for
every workload. Full revised ingest and query measurements are separate.

## Original experiment — historical evidence

The narrow one-input lookahead passes all eight lifecycle tests and 18 bounded
complete-value/whole-file checks. Its best full candidate ingest is 92.465509 s
against 98.982644 s for control: **6.58%**, below the frozen 10% gate.
Every output exactly matches the retained 15,682,956,116-byte artifact, including
encodings, statistics and footer. All four samples remain in the evidence:

| Order | Role | Complete CLI time | OS peak RSS |
| --- | --- | --- | --- |
| 1 | Control | 98.982644 s | 2.766 GiB |
| 2 | Candidate | 113.855263 s | 2.960 GiB |
| 3 | Candidate | 92.465509 s | 2.941 GiB |
| 4 | Control | 219.553181 s | 2.687 GiB |

The same fastest-valid rule applies to both roles; no slower sample is removed.
Cache state and other host activity are uncontrolled. Whole-output hashing is
outside the ingest clock. Native CPU counters, generations and all raw logs are
in the linked evidence. Four exact duplicate outputs were removed after hashing,
62,731,824,464 bytes cumulatively, keeping at most one new bulk output at a time.

The initial cutoff-only decision removed the prototype, its route marker and
experimental source fixtures. That disposition is superseded. The evidence
archive preserves the exact source patches from
`0d8dd21201aafb31955fb51e9a105087c3f656de`, frozen build identities, commands,
bounded observations and full-ingest receipts for reproduction. The revised
candidate retains the runtime and lifecycle tests; test-only attribution code
stays archived. Revised ingest and Full43 acceptance are recorded above.

Review of the original prototype also identified a low-budget availability gap:
the producer replenished its existing queue before yielding input, and lookahead
added another live owner. Reservations stayed safe, but the lifecycle fixture
demonstrated a budget where serial succeeded and lookahead rejected. The revised
shared-slot implementation and production-path availability tests above close
that gap without relying on a free-bytes snapshot or retry after input consumption.

The retained writer awaits one source-batch subtree at a time; column/zone/codec
work already overlaps within that subtree. Existing conversion wait and summed
codec spans do not locate recoverable batch-tail capacity. The preceding full
ingest observation was 132.527161 seconds with 287.221417 user CPU seconds,
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

Repeated material unused capacity admitted one bounded next-input overlap
prototype, with one live child strategy. Two concurrent subtrees were not tested.
The estimate is neither an achievable speedup nor a strict full-ingest upper
bound. A screen without credible material headroom parks overlap without claiming
it can never help. The original experiment targeted at least 10% lower complete
guarded ingest. The maintainer's revised retention policy accepts useful positive
gains below that target, with matched resources, no larger artifact, exact full
values/schema/statistics/reopen checks and query regression acceptance. No new CPU allocation,
codec, file topology, external engine or publication semantics is admitted here.

## Evaluated narrow prototype

Complete attribution identifies serial input polling (including existing file
statistics) while only one provider driver is occupied. Most samples show
14–16% total unoccupied capacity, so test overlapping one `input.next()` with the
current child. Poll the child first; retain at most one next array and its native
credits. Keep one serial statistics accumulator and one live child strategy.
No extra worker or concurrently buffered second encoded subtree is introduced.
Child errors drop pending input; input errors preserve their position after the
current child. Cancellation and pressure must release all retained owners.
The initial prototype was test-only and defaulted off.
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

The `e662f674` paired prototype passes eight deterministic lifecycle tests and
18 complete-value/whole-file-hash checks. Its best complete-writer savings are
8.11%, 8.66% and 5.25% for row groups 0, 113 and 225. These bounded writes retain
all preconverted inputs and have first/last-batch costs, so they do not establish
the complete ingest gate. A full-size comparison followed before disposition.

Matched portable CLI builds ran control/candidate/candidate/control
through `run_clickbench_ingest_uat.sh`: the resident 99,997,497-row source,
P4, 24 GiB, 600-second timeout and 17 GB artifact ceiling. Preserve the existing
watchdog, source-residency and storage admission controls. The full candidate
enables lookahead only in the existing memory-owned stream writer with at least
one provider background driver; array-only and P1 writers stay sequential.
No extra worker or codec/row-block setting changes. Retain all samples and use
the symmetric fastest valid complete process time. Hash each completed output;
remove it only if identical to the retained native artifact. Any different
output stays for full value/schema/statistics investigation. Retention still
required 10% complete savings, correctness/resource acceptance and query UAT.

Compare fresh-input control/candidate samples in alternating order with the same
retained writer, source regions, field set and memory/CPU/output limits. Charge
the complete writer, flush and driver teardown. Independently reopen every value
and compare complete file statistics and artifact bytes before considering full
guarded ingest. All attribution observations remain evidence, including warmed
and incomplete earlier screens; none is an achieved speedup.

## Initial drop validation — superseded disposition

At the initial drop, the Rust tree was byte-identical to `0d8dd212` for this change. Formatting,
workspace Clippy with warnings denied, workspace all-target tests, public-claim
and public-status validators passed. The architecture tracker passed with its
documented `--allow-blocked` option; existing unfinished gates remain open.
Independent review checked all archived source patches, hashes, four raw full
receipts, gate arithmetic and the next-item update. Native prototype tests and
bounded complete-output validation remain historical experimental evidence.
