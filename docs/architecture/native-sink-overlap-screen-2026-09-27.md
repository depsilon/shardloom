# Native workflow overlap — R5.c

Status: drop a second generic overlap queue at bounded admission under
PERF-INTAKE / RFC 0044. R5.b's spill ownership candidate merged in PR #1468.
Keep the existing provider overlap and the test-only attribution screen. No
runtime candidate was benchmarked or rejected for a small percentage gain.

Native aggregate calls already submit work while the caller advances the source.
`AggregateChunkJobs` bounds outstanding jobs and retained input bytes, reserves
before submission, and retains the slot through ordered merge. Vortex 0.85's
`ScanBuilder` spawns scan tasks and buffers them in source order; its configured
concurrency is multiplied by available workers. A second generic read-ahead
queue would duplicate existing work, and its row count would not bound variable
payload bytes.

The saved C2 Full43 envelopes record caller `scan_next` spans of approximately
55–173 ms across Q10/Q13/Q19/Q23/Q29/Q33/Q34/Q35/Q36. These include provider
progress and are not exclusive I/O stalls. They neither prove recoverable idle
time nor establish that another queue accelerates those queries. Original
envelopes remain in the C2 evidence packet.

Native filter/project persistence has a distinct complete workflow to screen.
`NativeSinkPlan::write` pulls ordered native arrays, completes only nonserializable
native expressions, pushes each array to the existing writer, then finishes,
synchronizes, reopens, hashes and publishes the output. It uses 8,192-row source
splits and an explicit metadata reservation. Aggregate result sinks intentionally
start from complete exact result owners; removing that semantic boundary is not
authorized by a generic overlap analogy.

Vortex-first decision: inspect and reuse the pinned native scan and writer.
The apparently blocking writer drives its async implementation. It already has
one-slot array and segment channels, spawns the layout task, and advances output
while a push waits for room. ShardLoom's `SequentialNativeFlatLayout` awaits each
Flat leaf to avoid unbounded child tasks; this is a resource contract, not an
accidental omission of a parallel loop. Do not replace it with an unbounded
writer or add an independent CPU pool.

Before admitting a runtime candidate, measure three complete public Vortex-output
calls for each of two bounded cases over a 524,288-row renamed nullable fixture
with a 96-byte suffix on each non-null text value:
project all source columns, then native filter/project with an ordered limit of
100,003 rows. Run each at one and four requested CPU lanes, keeping a 64 MiB
session budget. Verify every output value and native schema independently,
source identity, published output checksum and owned staging cleanup outside
the clock. Fixture/oracle creation and verifier readback are excluded; native
preparation through synchronized validated publication and report release are
included. The source and one output are the only live data artifacts.
This is a warm-source-cache screen: fixture creation and a complete source hash
precede each measured series. It does not establish cold-storage behavior.

Test-only caller timers separate next-array progress, serialization completion,
writer push, writer finish, synchronization/reopen and checksum/publication.
Provider jobs can overlap these spans; neither their sum nor `scan_next` is an
exclusive CPU/idle measurement. Sample the writer's existing buffered-byte
counter separately, without describing it as all queued memory. Record complete
public-call time and process RSS, retaining their different scopes. The external
supervisor's CPU/RSS and storage-guard receipts accompany the samples. Its
whole-process RSS includes fixture creation, verification and all twelve calls;
it cannot establish per-case or per-parallelism memory peaks. Session
reservations and the sampled provider counter are not process RSS.

Use a guarded local-only TMPDIR, exclusive workspace, supervised timeout and
disk/log ceilings. Remove each verified output before the next repetition.
The frozen baseline is `b8d4f7732375eb5a83108b45b1099d4848cf62e0`, after the
R5.b merge. All twelve complete outputs pass the independent schema/value
oracle, output checksum, source identity, reservation and cleanup checks.
The source is 9,977,028 bytes, SHA-256
`6eb5dc17357ab351936133e5582008fbc69deb0036cb9984f9adc6e6c957e9dc`.

| Case | Requested lanes | Complete call samples, ms | Median, ms |
| --- | ---: | --- | ---: |
| Project all | 1 | 23.315, 25.729, 13.021 | 23.315 |
| Project all | 4 | 14.665, 12.595, 16.217 | 14.665 |
| Filter/project/limit | 1 | 15.514, 13.480, 13.200 | 13.480 |
| Filter/project/limit | 4 | 11.879, 13.085, 14.812 | 13.085 |

Median caller next-array spans are 0.55–0.60 ms per complete call. Project
serialization completion is 0.04–0.06 ms; filtered completion is 1.39–1.52 ms.
Median writer push totals are 3.74–3.79 ms for projection and 1.61–1.79 ms for
filtering. Synchronization/reopen and checksum/publication are substantial
terminal spans. They occur after completed output and cannot be hidden by adding
a read-ahead queue. These separate medians are attribution summaries, not an
additive decomposition of the median call. Requested-lane cases ran in fixed
order and are not a parallel-scaling experiment.

The process peaked at 124,796,928 bytes RSS, including fixture and verification
work; user/system CPU were 1.988342/0.096948 seconds. Maximum session reservations
were 10,622,956 bytes for projection and 9,102,988 bytes for filtering. All sampled
layout buffered-byte counters were zero, which does not mean no provider input
was queued. Each output was removed after verification; the owned temporary
directory was empty on success. Storage admission/watchdog snapshots are retained.

Decision: the scoped source audit and warm baseline do not identify an unmet
independent stage with recoverable idle time. Source progress is already
prefetched, layout/output already cooperate through bounded provider channels,
and moving serialization between their tasks is not itself evidence of reduced
complete work. Do not add another queue or CPU pool on this evidence. This is an
admission decision, not a measured claim that another implementation is slower
or that smaller gains are worthless. A cold/high-latency source, different output
shape or explicit stage-idle trace can reopen it. Changes to terminal checksum
or publication semantics require their own contract and are not an overlap fix.
Broader streaming, serving and memory obligations remain open.

The [summary](../benchmarks/native-sink-overlap-2026-09-27.json) and its linked raw
packet retain all samples, build identity, stage counters, process metrics,
guard receipts, oracle code and failed/final validation logs. Reproduction uses
the archived build/guarded runner and the exact ignored test named in the receipt.
Hardware: Apple M5, ten logical CPUs, 16 GiB RAM, macOS 27.0 (26A428), Rust 1.98.0,
Vortex 0.85.0. No cold-cache, production-scale, ingest, Full43 speedup or
per-case RSS claim is made. No Full43 rerun is needed for this observer-only
change; the production runtime remains the fully validated R5.b runtime.
