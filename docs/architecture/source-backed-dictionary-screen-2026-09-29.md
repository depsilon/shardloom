# Source-backed UTF8 dictionaries — R2.a

Status: **retain** the shared source-backed dictionary and borrowed string-count
reads. Final Full43, workspace/native/public-call validation and independent
ownership review pass. The maintainer resumed the remaining September 26
candidates after the completed 0.3.2 release train.

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
and one nullable entry lookup replaces duplicate Arc/string lookup implementations.
The initial shared interner insertion helper was removed during regression
isolation; restoring the original insertion body did not remove the slowdown.
Native DictArray values
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

## Regression isolation and shared borrowed reads

The `0d102cd5` revision passes all 258 complete results in paired Full43. Best-of-three
sums are 63.850814 seconds for control and 63.447120 seconds for the candidate;
Q29 improves from 8.147487 to 7.032149 seconds (13.69%). This is not a 14% gain
across the whole suite. The apparent Q17 best-time regression does not repeat in
a focused check (2.384584 versus 2.357409 seconds). Q34/Q35 losses do repeat:
3.490826 versus 3.857358 seconds and 3.521933 versus 3.956253 seconds respectively.
Those workers bypass the changed chunk dictionary. Do not dismiss these losses
as concurrent host activity or claim a cause from timing alone.

Restoring the interner insertion body in `5942b941` retains the Q29 gain
(7.896337 versus 6.701291 seconds) but leaves the URL losses. Rebuilding unchanged
`b06a77d9` with the current toolchain/features reproduces the original unstripped
binary exactly: SHA-256 `55a6039d8673678ed2fec57926bbefc74272959ef3bea1bfdd1c2c1ee59b5979`.
The protected control is its stripped release artifact; absence of local symbols
there is **not** evidence of different inlining. Applying the same strip step to
the candidate still leaves about 6% higher best Q34/Q35 time. This rejects build
recipe drift and the insertion helper as established explanations; compiler layout
or scheduling effects remain unproven.

The next screen shares native UTF8 ownership rules with `StringCountPartial`.
Its canonical-row lookup, duplicate comparison, partition hashing, borrowed entry
delivery and merge callbacks currently obtain temporary `ByteBuffer` owners.
They need only byte slices while the partial's `VarBinViewArray` remains alive.
Reuse a `native_utf8` helper for these reads and the chunk dictionary; acquire a
`BufferString` only when a chunk-dictionary miss needs retention, and promote
escaping persistent keys independently. Partition storage still copies admitted
keys and owns its existing leases. No count, selection, cancellation or spill
policy changes.

Vortex-first check: pinned Vortex 0.85.0 already exposes borrowed
`BinaryView::bytes`, `VarBinViewArray::views` and `buffer`. The first requires a
pre-resolved buffer-slice directory; the shared helper uses the latter two safe
APIs directly so these existing array owners need no extra directory allocation.
It preserves the provider's inline/external view semantics and caller validity
checks. Test both against upstream borrowed/owned access, including sliced views,
empty and 12-byte inline values, multiple external buffers, nonzero offsets,
duplicate strings, worker counts and release of partial reservations.

Evidence directories under the local ClickBench `logs` directory:
`paired43_20260929T214205054974Z` (Full43),
`paired43_20260929T215108994162Z` (Q17),
`paired43_20260929T215311083508Z` (Q34/Q35),
`paired43_20260929T220827143260Z` (restored interner), and
`paired43_20260929T221808900795Z` (matched packaging).
Complete outputs and process receipts remain archived beside each summary.

## Combined borrowed-read screen

Revision `bb47125c13adc1d65b7db13133bf9107f92aa605` shares the native UTF8 helper
across dictionary lookup and string-count workers. The frozen executable uses the
same release build and strip steps as control; SHA-256 is
`2d729f527c5812b8900ff94587e0c94bdf7b7ee1ccb7884cc62740ec03e1b76b`.
All 24 complete results pass in the sequential four-query screen:

| Query | Control best | Candidate best | Best reduction | Control median | Candidate median |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q17 | 4.771813 s | 4.838261 s | -1.39% | 4.840296 s | 4.972724 s |
| Q29 | 11.683908 s | 10.314917 s | 11.72% | 12.133923 s | 14.507366 s |
| Q34 | 7.685674 s | 6.681898 s | 13.06% | 7.885916 s | 7.606495 s |
| Q35 | 7.513949 s | 7.108249 s | 5.40% | 7.968639 s | 7.191927 s |

Both URL queries improve in every pair. Q29's median is slower in this changing
host-load cohort; preserve it alongside the best-time improvement rather than
discarding samples. Absolute times are higher for both roles than earlier runs.
This screen supports the combined candidate, not an attribution of the earlier
regression to a specific compiler or scheduling cause. Peak RSS ranges overlap;
no process-memory reduction is claimed.

The new borrowed-value oracle and 52 string-count tests pass; one existing manual
benchmark remains ignored. Release-feature all-target Clippy passes. Independent
read-only review of `09b8f259..bb47125c` found no actionable correctness,
lifetime, resource or test-coverage issue; it did not execute validation. The
final acceptance below supersedes this preliminary screen.

Evidence: `paired43_20260929T223300874428Z/summary.json` under the local ClickBench
logs directory, and `r2a-borrowed-screen-analysis.json`,
`shardloom-r2a-bb47125c.json`, `r2a-borrowed-focused-final.log`,
`r2a-borrowed-clippy-final.log` and `r2a-independent-review.json` under
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926`.

## Final acceptance

The same frozen `bb47125c` executable passes all **258 complete-result
comparisons**: three counterbalanced observations per role for all 43 queries.
Every parsed result also reports `fallback_attempted=false` and
`external_engine_invoked=false`. Timing includes native process startup, complete
CLI output and exit; it excludes the comparison harness. The sum of each query's
best observation falls from **92.389200 to 88.973689 seconds (3.70%)**.

| Query | Control best | Candidate best | Best reduction | Control median | Candidate median |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q6 | 4.232010 s | 3.788743 s | 10.47% | 4.447039 s | 3.834961 s |
| Q13 | 1.085695 s | 0.893367 s | 17.71% | 1.088653 s | 0.954493 s |
| Q17 | 4.197320 s | 4.235176 s | -0.90% | 4.233673 s | 4.660645 s |
| Q29 | 11.389133 s | 9.525375 s | 16.36% | 12.552298 s | 10.827237 s |
| Q34 | 7.362601 s | 6.468848 s | 12.14% | 7.637906 s | 6.736073 s |
| Q35 | 7.343670 s | 6.980206 s | 4.95% | 7.853014 s | 7.032284 s |

Q29, Q34 and Q35 improve in every final pair. No query crosses both the 10% and
150 ms best-time regression screen. Smaller losses remain in the evidence,
including Q10 (+4.2%, 190 ms) and Q23 (+4.6%, 333 ms); the screen does not prove
every query is faster. Preserve all samples and medians, including Q17's slower
median. Other host processes and uncontrolled caches explain why absolute times
cannot be compared directly across cohorts; they do not establish a cause for
any individual timing difference. No isolated-host or subsecond-suite claim is
made.

All six Q29 executions cover the same 1,550 chunks, 81,032,736 post-scan rows and
25,771,910 dictionary entries. Chunk-dictionary UTF8 payload copies fall from
3,120,823,803 bytes to zero. Candidate slices retain that cumulative logical
string length; this is neither unique source bytes nor retained buffer capacity.
In the fastest pair, dictionary construction falls from 4.859505 to 3.380463
seconds. Escaping aggregate keys still copy. Peak RSS ranges overlap, so this
does not establish lower process memory or zero-decode execution.

Final validation against the accepted runtime revision:

- `cargo fmt --all -- --check` and
  `cargo clippy --workspace --all-targets -- -D warnings` pass.
- `cargo test --workspace --all-targets`: 3,436 passed, no failures or ignores.
- `cargo clippy -p shardloom-cli -p shardloom-vortex --all-targets --features release-user-surfaces -- -D warnings` passes.
- `cargo test -p shardloom-vortex --lib --features release-user-surfaces`:
  1,991 passed, 22 existing ignores (21 manual benchmarks and one fixture
  regeneration helper); no changed correctness case is ignored.
- `cargo test -p shardloom-cli --bin shardloom --test sql_local_source_runtime_smoke --test public_workflow_route --test resident_worker --features release-user-surfaces`:
  1,170 passed, no failures or ignores. Counts overlap across configurations.
- Public-status documentation, contribution governance and architecture-tracker
  validators pass. Architecture validation uses `--allow-blocked` to audit
  tracking; it does not close the broader release or competitive gates.

No input, writer or sink behavior changes, and no fresh ingest run is claimed.
The shared helper removes temporary ownership work in two existing consumers;
it adds no query-specific route, public API, dependency or unsafe code.

Final Full43 evidence:
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/paired43_20260929T223724665757Z/summary.json`.
Complete outputs and process receipts are retained in the verified archive beside
the summary. Derived per-query observations, work counters and validation logs
are recorded in `r2a-borrowed-full43-analysis.json`, `r2a-final-q29-work.json` and
`r2a-final-validation.json` under the local performance-candidates directory.

## Cleanup

Retired the completed `release-lto` and `release-native-benchmark` Cargo caches
(2,770,919,424 allocated bytes), then three completed test-screen executables and
five superseded R2 executables (627,159,040 allocated bytes). Total allocated
bytes removed: **3,398,078,464 (3.40 GB decimal)**. Exact-path identity, SHA-256,
completed evidence and absence of process/file consumers were checked before
removal. Protected control and accepted candidate hashes were verified afterward.

Current inputs, complete result references and archives, source revisions,
release provenance and active build caches remain. Replaying a retired executable
requires rebuilding its recorded revision. Receipts under
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926`:
`completed-profile-cleanup-20260929.json` and
`completed-binary-cleanup-20260929.json`.
