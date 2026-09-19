# Exact numeric-pair partition reduction

Status: retained after paired speed and memory gates, Full43 UAT and broad
regression checks.

Q19 merged in PR [#1447](https://github.com/depsilon/shardloom/pull/1447) as
`2be959bf` after all 40 checks passed. This is the next existing candidate under
PERF-04/05/06, following the [approved packet](performance-domain-transfer-2026-09-19.md#f-exact-partitioned-duplicate-reduction-for-near-unique-pairs--conditional).
G/H/I follow; broader capability and parked experiment gates remain unchanged.

## Evidence and bounded change

The verified saved Q33 calls process 99,997,497 rows into 99,997,493 complete
groups, with four duplicate keys. First-pass update spans are 5.557–7.446 seconds;
these are caller elapsed spans, not exclusive CPU. The exact raw archive,
complete-result checks and current-control identity are recorded in
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/q33-preflight-attribution.json`.
Use the frozen `e96f8896` executable, SHA-256
`d2506a5135337c00ae670917edc75eca384fed642d3ab57674e43b270c94a8f5`, as the control.
The merge's subsequent changes are documentation only; the binary keeps its
actual source identity.

Replace only first-pass count ownership for the existing nonnullable integer-pair
late-measure route, ordered by COUNT(*), with a positive bounded OFFSET+LIMIT of
at most 128 and no explicit spill. Preserve all complete typed keys in persistent
partitions. Sampling chooses the strategy before commitment; it never proves
uniqueness, drops input, or supplies final counts. A declined first sample retires
workers before the original native route resumes on that same input chunk/source.

Sort each complete partition in place and count adjacent exact keys. Only after
partition completion may its bounded candidates be selected. Merge these small
candidate sets with the existing count/signed-key comparator; do not create a
full merged count map. Feed exact groups and selected keys into the unchanged
second-pass measure updates, preserving input order and floating accumulation.

Persistent vector capacity, growth overlap, staging keys and retained candidates
carry leases. A bounded job window alone is insufficient for all retained input.
Committed denial or cancellation fails and drains owners; no serial replay or
new spill store is admitted. Sort jobs retire before provider CPU drivers resume
for late measures on the same held source. No concurrent second pool is allowed.
Expose actual partition sizes/capacities, sort/reduction spans, modeled peak
reservations and OS peak RSS separately. Provider/accessor and existing
second-pass state allocations are not claimed as fully covered by these leases.

Vortex-first decision: `implement_shardloom_kernel`. Reuse pinned Vortex 0.85
scan, integer accessor, validity and session providers. Complete typed pair
reconciliation and COUNT-ranked late measures are ShardLoom aggregate policy,
not an upstream array-sort substitution or external-engine query. Use the
existing AggregateChunkJobs/CPU grant, allocation-free native Rust unstable sort,
and exact native comparator. No dependency, Arrow execution substrate, persisted
answer, new encoding, or query-number dispatch is introduced.

## Registered acceptance

Renamed fixtures must cover cross-chunk duplicates, global winners absent from
local Top-K, signed/unsigned extremes, ties/OFFSET, empty input, a misleading
near-unique sample followed by repeated keys, nullable measures, rejected key/
COUNT(column)/spill shapes, and cancellation/denial after retained input exists.
Full public-call evidence must prove route activation, complete values and the
same source generation.

Screen three alternating calls per control/candidate at P12 and 24 GiB on the
unchanged full-size Vortex artifact. Compare each role's fastest valid complete
CLI call symmetrically; retain all samples. Retain for at least one second saved,
or separately at least 30% lower peak RSS with nonregressing complete time. Include
scan, redistribution, sort, reduction, late measures, full output and exit. The
24-byte existing key alone requires 2.24 GiB for 100 million rows; memory savings
must be measured after capacity slack and all other process allocations.

If retained, complete broad gates, independent review and Full43 UAT before PR.
Otherwise remove the experiment, preserve the evidence and advance to G.

## Activated full-size screen

Frozen candidate `06da983b5928876c7a07e75a4c77f06a2d931d61` was built with
`cargo build --release -p shardloom-cli --features release-user-surfaces`.
Its executable SHA-256 is
`98e6fea73ea15a2d99c2cea97bb6f43f01530692c88e139aaaf90b83ecea9b0a`.
All six complete outputs match the retained native reference. All three candidate
calls report the actual `complete_numeric_pair_partition_sort_reduce` family,
99,997,497 rows, 99,997,493 groups, four duplicate keys and 64 completed partition
tasks. Partition totals reconcile exactly. Workers retire before the same held
source resumes provider execution for late measures.

| Pair | Control seconds | Candidate seconds | Control peak GiB | Candidate peak GiB |
| --- | ---: | ---: | ---: | ---: |
| 1 | 6.171186 | 2.621692 | 3.406 | 2.589 |
| 2 | 5.536625 | 1.851821 | 3.783 | 2.581 |
| 3 | 5.650200 | 1.800403 | 4.361 | 2.567 |

The symmetric fastest-valid comparison saves **3.736222 seconds (67.5%)** and
reduces those calls' OS peak RSS by **32.1%**, passing both registered gates.
This measures achievable complete-call latency on the shared host, not a
production percentile or a causal explanation for every slower sample.

In the fastest candidate, producer accessor and routing spans are 0.232 and
0.413 seconds; complete partition finishing takes 0.531 seconds. Worker sort
spans sum to 4.035 seconds and overlap across workers. Do not add them to caller
wall time. These counters exclude some provider and second-pass allocations;
the reservation model is not an RSS limit. Sorting checks cancellation before
and after each complete partition, so it does not promise preemption inside a
large skewed sort.

The [machine-readable packet](../benchmarks/q33-exact-partition-reduction-2026-09-19.json)
preserves all six calls, exact binary/source/query identities, archive hashes,
the comparison rule and validation receipts. Raw evidence is under
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/paired43_20260919T211838565256Z`.
Complete-reference comparison is regression evidence, not a new independent
oracle; renamed fixtures separately compare exact native serial semantics.

Twelve focused tests and workspace/native Clippy pass. Independent review found
one misleading task-counter label, corrected before the frozen screen; no
runtime correctness or ownership findings remained. Three additional native-file
tests pass with 1,048,576 renamed rows at P1/P4. They verify repeated execution
on one held source, full COUNT/SUM/AVG outputs, provider restoration after both
completion and sample decline, and fatal allocation/corruption faults after
commitment followed by fresh execution and complete lease cleanup.

## Full43 correctness acceptance

The same frozen executable passes **129/129 complete-result comparisons**, all
43 queries three times, on the unchanged 18,591,586,804-byte native artifact.
The observed best-of-three query sum is **82.196766 seconds**; the hot sum is
83.459779 seconds and all calls total 259.158202 seconds. This is an unpaired
suite observation. Only the alternating Q33 screen above establishes this
candidate's measured speedup; other queries' times do not establish attribution.
No fresh ingest or storage-size gain is claimed for this query-only change.

The full receipt is
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/full43_20260919T212225238049Z/summary.json`.
Before this run, one closed historical transcript archive was losslessly
recompressed from gzip to xz, preserving its decoded tar stream byte-for-byte
and recording both archive identities. That released 8,523,776 accounted log
bytes; the 256 MiB log guard and all other storage/source/process guards remained
unchanged. The receipt is `q33-uat-log-recompression.json` in the local ship/drop
evidence directory. The fixture and documentation additions after the frozen
build do not change release runtime behavior.

Final acceptance passes formatter checks; workspace, native-feature and minimal
native-feature all-target Clippy; all 3,424 default workspace tests; and 3,362
native-feature CLI/Vortex tests with nine existing manual tests ignored.
Five documentation/governance validators pass. Minimal-feature validation also
exposed an existing native-test helper compiled without its only consumers;
its feature guard now matches the write/Unix test module. That cleanup and the
test literal-format correction affect tests only. The evidence packet retains
the initial failures and successful reruns.

Open F's cohesive PR before advancing to the existing G/H/I attribution gates.
The speed and memory retention here does not close whole PERF/CG obligations or
reopen parked codec, topology, compact-state, binding or PGO experiments.
