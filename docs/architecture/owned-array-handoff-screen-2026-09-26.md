# Direct owned-array handoff — R5.a

Status: baseline attribution in progress under PERF-INTAKE / RFC 0044.
No runtime change or new performance result is accepted by this note.

The existing native result composition contract preserves owned Vortex arrays,
typed empty schemas, validity, session admission, cancellation and reservations.
It then serializes those arrays into immutable memory-file segments before the
ordinary optimized aggregate kernels consume them. This is an explicit copy
boundary. The public Rust composition API is available; the current CLI query
workload does not invoke it, so removing that boundary is not an automatic
ClickBench Full43 speedup.

First attribute a complete bounded workflow over the retained native artifact.
The initial screen freezes three 131,072-row ranges beginning at rows 0,
50,000,000 and 99,800,000.
Project AdvEngineID, UserID and URL into an owned native result, construct the
current memory generation with its default 64 MiB serialized limit, and execute
the existing exact grouped COUNT by AdvEngineID ordered by that key. The wide
producer followed by the narrower consumer measures the cost of retaining and
serializing intermediate fields; it does not claim that these unused fields
must be evaluated by every future optimizer.

Use a 512 MiB credited session and one CPU lane. Before the timed repetitions,
compute an independent exact count for every key from every projected row.
Measure three fresh-session repetitions per range, including preparation,
source read, composition, complete aggregate report output and owner release.
Keep producer, composition, consumer and teardown spans distinct. Retain every
sample and verify complete ordered values outside the clock. Native provider
buffers outside the pool and uncontrolled OS cache remain explicit; reserved
bytes are not total process RSS. Do not widen the composition cap if admission
fails, or describe failed admission as a completed timing observation.

Only advance to a runtime experiment if attribution establishes a material
opportunity. The frozen workflow retention gate is at least 20% and 100 ms saved
in a complete operation, or at least 30% lower OS peak RSS with no elapsed-time
regression. A retained implementation must reuse downstream lowering, weighted
reducers, exact partitions and finalizers; preserve schema/selection/validity,
source lifetime and parent cancellation; and retain every buffer's reservation
until its final consumer drops. Small escaping outputs must not silently pin
unbounded backing domains. Native persistence and spill keep their file contract.

Vortex-first check: pinned Vortex 0.85 exposes owned ArrayRef values, native
expression evaluation, the LayoutReader interface and ScanBuilder. VortexFile
creates its reader from a footer and segment source; it cannot accept an arbitrary
owned-array reader. Check existing array-backed providers before selecting a
wrapper around those native interfaces. No new file format, Arrow execution,
external engine, package release or parallel aggregate implementation is admitted.
The provider decision and exact touched boundary will be recorded before a
production implementation. Native input/output and no-fallback contracts remain.

ShardLoom technique review: avoid intermediate work before overlapping stages;
use the existing session admission and capillary scan/aggregate ownership.
Separate construction and execution evidence. R5.b page transfer and R5.c stage
overlap remain independent decisions. Retained changes require complete semantic,
resource, cancellation and independent-workload acceptance plus Full43 regression
coverage wherever the shared file aggregate boundary changes.

The initial complete baseline takes 18–28 ms, below the absolute latency gate.
Before deciding its disposition, compare process RSS against an attribution run
that retains the same producer, ranges, repetitions and untimed scalar oracles,
but omits the entire composition and consumer. Run both through the same frozen
binary, in three alternating fresh-process pairs. This is deliberately less work
than a real candidate and cannot establish a speedup or exact RSS bound: allocator
reuse, code residency and provider allocations can interact. It tests whether
even removing both stages reveals a credible 30% memory opportunity in the
frozen workload. Preserve all OS RSS measurements separately from pool credits.

The three alternating pairs record complete-process RSS of 147,783,680,
147,800,064 and 147,783,680 bytes, versus producer-only RSS of 107,364,352,
107,315,200 and 107,380,736 bytes. Even omitting both stages exposes only about
27.4% less observed memory in these samples. This is below the retention gate,
but does not decide whether a larger intermediate can qualify. Before that
decision, extend both attribution arms with one explicitly frozen 524,288-row
range beginning at zero. Preserve the original three cases, projection, P1,
512 MiB session and default 64 MiB serialization limit. A rejected larger case
must remain an admission failure; it cannot authorize raising the cap or claiming
a successful workflow. This extension tests half the default maximum row count
without changing the admitted composition policy.
