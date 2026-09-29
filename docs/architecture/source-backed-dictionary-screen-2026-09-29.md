# Source-backed UTF8 dictionaries — R2.a

Status: positive bounded implementation screen under PERF-INTAKE; full regression
and final validation are pending. The maintainer resumed the remaining
September 26 candidates after the completed 0.3.2 release train.

## Admission and reusable boundary

Three fresh Q29 executions of the protected 0.3.2 binary pass complete-value
comparison. Native wall times are 8.058376 / 7.969747 / 8.406454 seconds. Each
execution builds 25,771,910 dictionary entries across 1,550 chunks, covering
81,032,736 post-scan rows. Dictionary setup takes 3.347680 / 3.353632 / 3.551349
seconds and copies 3,120,823,803 UTF8 payload bytes. Provider execution takes
2.274015 / 2.243616 / 2.335952 seconds. These are disjoint accessor caller spans,
not exclusive CPU samples. RSS peaks are 1,360,150,528 / 1,459,437,568 /
1,464,139,776 bytes. This confirms that the historical copy opportunity remains
after the representation and handoff changes.

The existing `Utf8ChunkDictionary` already owns exact byte lookup, cached hashes,
growth and first-seen IDs. The existing aggregate interner already tests
membership before allocating persistent keys. Reuse both. Pinned Vortex 0.85.0
provides `VarBinViewArray::bytes_at` and validated `BufferString` owners; no new
buffer allocator, execution provider, Arrow conversion or unsafe view is needed.

Screen retaining validated native string slices in the chunk dictionary. Borrow
bytes for lookup before obtaining a reference-counted slice on a miss. Keep the
same accessor contract for grouping, transforms, exact DISTINCT and string
predicates across SQL, Python and CLI. Adapt ownership at the shared boundary,
not in a Q29-specific route. Values escaping to persistent group keys, DISTINCT,
cached transforms or MIN/MAX must become independent owned strings. Source
buffers and their existing allocator leases must live through chunk consumers
and be releasable when the accessor drops. Native dictionary IDs, row-null and
value-null roles, collision equality, UTF8 validation and ordering stay exact.

ShardLoom technique review: retain current metadata/pruning, selection and
weighted partials; this removes work within their shared accessor. Do not add
PulseWeave scheduling, a second worker queue or a capillary work abstraction.
Keep caller timing, source ownership and copied-byte evidence separate; zero
dictionary payload copies would not establish zero decode or a process RSS bound.

## Decision and validation

Run focused collision, UTF8, null, ownership/escape and budget-release fixtures,
then a sequential counterbalanced complete Q29 comparison against the frozen
control. Preserve all samples and use the fastest valid observation for both
roles, with distributions and RSS alongside. One second remains the original
prioritization target, not an automatic rejection of a useful smaller gain.
Remove the prototype if complete-query or resource evidence does not support it.
For a retained change, run full 43-query regression UAT, investigate material
regressions, complete workspace/native gates and review the ownership diff before
preparing its PR. Do not repeat full-size ingest for an accessor-only change.

Control source: `b06a77d9a994684ee483d43d65a8bc254dd998a6` (0.3.2), SHA-256
`75fea24c4998347d9e8a3733d096c1f041bf3d345f4bedb597383674576b8313`.
Input: `derived-dictionary-20260926.vortex`, 15,682,956,116 bytes. The existing
guarded runner uses 24 GiB declared memory, 12 lanes and 120 seconds per call.
OS caches are uncontrolled. Q29 complete result SHA-256:
`fc6242e120770d4cd9ffc73c5fbb0d55b12dc1b98b055bcad96945290e151308`.

Local evidence: `/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/full43_20260929T210530813444Z/summary.json`.
All three outputs and process receipts are retained there in compressed form.

## Initial paired screen

Candidate source `d7e898260a866293ebcec7e7384223cee6ecd1f8`, executable SHA-256
`b52a54ecb882b19b4c38d99779da8b1e43b10417c1c367257d899d282e0a9b77`, passes
all three complete outputs against the same frozen control and input. The
counterbalanced observations are:

| Q29 native process, complete output and exit | Control | Candidate |
| --- | ---: | ---: |
| Pair 1, control first | 9.225822 s | 7.554119 s |
| Pair 2, candidate first | 8.080750 s | 6.965657 s |
| Pair 3, control first | 8.036210 s | 6.857854 s |
| Best | 8.036210 s | 6.857854 s |
| Median | 8.080750 s | 6.965657 s |

Best complete-query reduction is 1.178357 seconds (14.66%); median reduction is
13.80%. Every pair improves. Dictionary payload copies fall from 3,120,823,803
bytes to zero; the same logical bytes instead retain provider buffer slices.
Best dictionary setup span falls from 3.382277 to 2.357503 seconds. Provider
execution, weighted counts, IDs, rows and complete results stay represented in
the receipts. Escaping aggregate keys still copy; this is not zero-copy execution.
RSS ranges overlap (control 1.394–1.463 GB, candidate 1.409–1.468 GB), so the
screen does not establish a process-memory improvement.

Reuse is contained in the existing chunk dictionary and aggregate interner:
one typed value owner admits borrowed reads or explicit independent promotion,
one nullable entry lookup replaces duplicate Arc/string lookup implementations,
and one insertion helper handles new persistent keys. Native DictArray values
retain their existing independent Arc ownership; this screen does not change
native dictionary selection/gather policy. No new query route is introduced.

The 82 focused dictionary tests pass. Added checks prove exact byte equality
under forced collisions, no retain callback for a duplicate, UTF8 rejection,
source pointer reuse and readable values after input release. A reservation test
proves inline and external buffers stay charged through the accessor, deny an
over-budget allocation while pinned, and refund after accessor drop while
independently owned interner/DISTINCT keys remain readable. Existing MIN-after-drop,
nullable, all-null, empty, cross-chunk dictionary and pressure tests remain.

The initial reservation fixture incorrectly assumed a smaller allocator alignment;
it was corrected to use the pinned provider's actual `DEFAULT_ALIGNMENT` before
the passing run. No runtime budget behavior was changed to make the test pass.

Evidence: `/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/paired43_20260929T212923089628Z/summary.json`,
verified compressed complete outputs beside it, and
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926/r2a-paired-screen.json`.

## Cleanup

Retired only the completed `release-lto` and `release-native-benchmark` Cargo
caches after recording binary hashes and checking for active consumers/open
handles. This removed 2,770,919,424 allocated bytes. Frozen experiment binaries,
current inputs, complete result references, release provenance and active build
caches remain. Receipt:
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926/completed-profile-cleanup-20260929.json`.
