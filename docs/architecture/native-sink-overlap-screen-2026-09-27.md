# Native workflow overlap — R5.c

Status: bounded admission screen under PERF-INTAKE / RFC 0044, after R5.b's
accepted spill ownership candidate (PR #1468). No new overlap or speedup is
retained by this screen.

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
If complementary stages and recoverable intervals are supported, freeze a
candidate using the existing native provider and ownership limits, and compare
all complete calls. Keep useful positive gains without a percentage or one-second
cutoff. Otherwise record a scoped admission drop with the evidence; broad
streaming, serving and memory obligations remain open. No fallback engine,
Arrow execution, format change or package publication is admitted.
