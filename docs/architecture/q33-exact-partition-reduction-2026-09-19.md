# Exact numeric-pair partition reduction

Status: candidate F prototype; no retained performance claim.

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
