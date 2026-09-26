# Direct owned-array handoff — R5.a

Status: retain under the complete-workflow memory gate, PERF-INTAKE / RFC 0044.
Full43, workspace, native-feature and independent review acceptance are complete.

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

The four-case extension completed in all three process pairs. Complete-process
RSS was 210,173,952 / 211,107,840 / 210,272,256 bytes; producer-only RSS was
126,795,776 bytes in each process. The larger intermediate serialized 56,776,688
bytes within the unchanged 64 MiB cap. Removing both composition and consumption
exposes approximately 40% lower observed RSS, sufficient to admit a real candidate.
This attribution is not an equivalent-query comparison or a retained speedup.
The frozen control is commit `f352ec71258d85db316078d61157b47fcc9aedb7`, executable
SHA-256 `12374bda4d37f5abf70c6c8926e3f545891ffdd077e65cbde5feaa0f7cf60eed`.
All six guard receipts and samples are retained in
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926/r5a-large-paired-receipts.json`.

## Prototype boundary

Pinned Vortex has no existing ArrayRef-backed scan source. Use its public
`LayoutReader` and `ScanBuilder` interfaces around immutable owned arrays. Reuse
native slicing, masks and bound expressions; filter before evaluating projection
expressions. Register bounded natural row splits. Preserve the producer's owned
buffers and metadata reservations for the entire prepared source lifetime.
Normalize nullable Struct validity with the same helper as memory-file composition.

The aggregate source binding distinguishes file and owned-array variants. Both
use the existing lowering, physical policy, workers, exact recounts and finalizers;
file scans keep their existing footer/pruning behavior. Owned arrays have no file
footer or persistent segment statistics and must not claim them. The initial
owned-array binding rejects explicit spill; callers retain the existing memory-file
composition path when they require the admitted file-backed spill contract.
Construction has explicit row, field, batch, logical-byte and metadata bounds,
separate from the session's credited memory limit and observed OS RSS.
No query answer or mutable aggregate state is retained. Cancellation and nested
composition borrow the existing native operation grant; completion is counted
once per outer call. The actual candidate must pass the complete four-case
paired experiment, ownership/cancellation/semantic tests and applicable regression
coverage before retention.

## Complete candidate comparison

The prototype is `4ed042b742cb50f0896961431cd1130ea8f4046f`. Three
counterbalanced fresh-process pairs run all four frozen cases three times per
process. All **72 complete workflows** match their complete ordered scalar
oracles, retain the source generation, and release all pool reservations.
Each workflow records one file open and two completed native executions.
The [machine-readable evidence](../benchmarks/owned-array-handoff-2026-09-26.json)
links a compressed receipt preserving every sample and native guard record.

| Projected range | Control best of nine | Owned-array best of nine |
| --- | ---: | ---: |
| 0–131,072 | 17.920750 ms | 15.098333 ms |
| 50,000,000–50,131,072 | 20.196292 ms | 14.608458 ms |
| 99,800,000–99,931,072 | 27.028000 ms | 15.397334 ms |
| 0–524,288 | 34.804417 ms | 24.125208 ms |

Process peak RSS is **210,419,712 / 210,157,568 / 230,424,576 bytes**
for the control and **132,349,952 / 130,498,560 / 130,482,176 bytes** for
the candidate. Paired reductions are 37.10%, 37.90% and 43.37%; even the
largest candidate measurement versus the smallest control is 37.02% lower.
Each case's best and median complete elapsed time improves. This passes the
30% RSS gate with no elapsed regression. The absolute latency improvement does
not pass the separate 100 ms workflow gate, and no ClickBench speedup is claimed.
RSS includes the process's four untimed oracles and all cases; it cannot be
attributed to an individual range. OS caching is uncontrolled.

The candidate holds existing native arrays through `OwnedArraySource` and
prepares the ordinary `PreparedVortexAggregate`. Its 64 MiB logical-input limit
and the control's 64 MiB serialized-output limit measure different things;
both admit every frozen case without changing their defaults. The source has
explicit row/field/batch/metadata bounds and retains producer credits. It neither
serializes a file nor asserts persisted footer statistics. Exact scalar footer
completion remains file-only. Empty owned-source scans have a distinct proof.
Explicit spill still uses memory-file composition; this adapter rejects it.

Native release validation passes: 1,939 tests across all targets (1,922 library
tests), 15 explicitly ignored performance fixtures, no failures; native all-target
Clippy with warnings denied; complete
owned-source, nullable root/field, mixed-width, dictionary, multiple-batch,
cross-split DISTINCT, typed-empty, cancellation and owner-release checks.
The paired Full43 regression run completed all 258 exact comparisons over the
retained 99,997,497-row artifact. Control/candidate best-of-three query sums are
64.806309 / 64.120800 seconds and geometric means are 0.627881 / 0.622257 seconds.
No query crosses the frozen regression flag (both 10% and 150 ms slower).
All samples and identities are retained in the linked compressed Full43 evidence.
These CLI queries exercise the shared file-backed boundary, not the new direct
owned-array route; this is regression acceptance, not a claimed query speedup.
Workspace format, Clippy and all 3,425 tests pass. The lean
`--no-default-features --features vortex-local-primitives` release check passes
with the existing unused `complete_for_serialization` warning in the unchanged
Flat-layout module. The owned-source adapter adds no lean-build warning.
Independent source review found no actionable issues. No replacement ingest is
required: this change adds no writer or persisted-format behavior.

Retain the bounded Rust adapter under its measured memory gate. Ordinary
prepared aggregate semantics and the shared file path remain covered. Broad
relational composition, direct-array spill and automatic Python/CLI pipeline
selection remain outside this implementation. Continue with R9.a and R9.b.
