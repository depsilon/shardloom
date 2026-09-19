# Q19 complete-key partition screen

Status: retained after the complete-query speed gate, broad regression checks and Full43 UAT.

This is candidate E in the [existing ship/drop packet](performance-domain-transfer-2026-09-19.md#e-complete-key-worker-partitions-for-the-existing-tri-key-state--conditional),
under PERF-03/04/05/06. PR [#1446](https://github.com/depsilon/shardloom/pull/1446)
merged as `400874587547e21dc2d9de80074af9009f433b1a` after all 40 CI checks passed.
Q13/Q36 remain retained; Q29 and the Q23 dictionary rewrite remain dropped.
Q33, duplicate ingest traversal, serving queues and result delivery still follow
E. Broader phase and competitive gates remain open.

## Admission evidence

The six saved same-day Q19 calls process 99,997,497 rows into 56,384,822 groups.
Their group-update spans are 6.764–7.401 seconds; these spans include producer
dictionary binding and must not be treated as entirely parallelizable work.
The verified extraction is
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/q19-preflight-attribution.json`.

Three subsequent guarded calls of the unchanged `69ce65ac` executable all match
the complete retained native reference. macOS CPU sampling observes the typed
update-loop branch 1,449 / 1,355 / 1,262 times, with separate growth branches of
245 / 234 / 203. These are inclusive sibling stack observations at distinct
instruction offsets, not exclusive CPU seconds or predicted worker savings.
Run 1's named dictionary-binding branches total 283 observations; the adjacent
prepared-minute validation branch is not dictionary work. The captures justify
one direct-partition prototype without a permanent diagnostics-only change.

The reproducible sampling wrapper and receipt are
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/profile-q19.py` and
`q19-cpu-sampling-receipt.json`. Raw samples, identities and complete-result
checks are under
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/full43_20260919T194059908959Z`.
Those elapsed times are profiler-affected diagnostics and are excluded from the
ship/drop comparison.

The comparison control is explicitly the frozen `69ce65ac` executable, SHA-256
`3fa5b72e098b02d3f5c8803df259d6c65d54ce98e636cfdd6ada30a398a24bed`, built with
`cargo build --release -p shardloom-cli --features release-user-surfaces`.
Source comparison through merged `40087458` finds only the core publisher and
its tests changed among runtime crates/manifests. This collected SQL path does
not write through that publisher. The control is not relabeled as a binary of
`40087458`; the source check is preserved in `q19-control-provenance.json` beside
the sampling receipt.

## Prototype contract

Reuse the existing Vortex-native accessors, producer string interner and typed
integer/minute/string-ID key. The producer routes bounded complete-key buffers
into 64 disjoint persistent `FxHashMap` partitions using `AggregateChunkJobs` and
the existing CPU grant/window. Each chunk contributes every row exactly once.
There is no local Top-K elimination, duplicate string pool, local preaggregation,
new scheduler or full merged map. Final selection visits all completed groups
with the existing comparator before OFFSET/LIMIT.

Reserve captured key buffers and old/new map-growth capacity before allocation.
The table capacity model is conservative and is not an allocator/RSS guarantee.
The producer interner and existing accessor allocation boundaries remain separate.
The earlier 12,799,679,616-byte CLI descriptor is a post-execution estimate, not
a live lease transferable into this state. Committed capacity denial cancels and
drains workers and fails without a result; triple-key serial/spill replay is not
admitted. Nullable or explicit-spill shapes remain on their existing admission
path before worker commitment.

Worker admission changes provider CPU ownership, so previous accessor times are
not assumed unchanged. The comparison must include binding, routing, scheduling,
map updates/growth, completion, result delivery and process exit. Insert/existing
key counts are not bucket-probe counts or chunk-local duplicate counts. Worker
timings overlap and must not be summed into wall time.

Vortex-first provider decision: `implement_shardloom_kernel`, using the pinned
native array/accessor/session providers in `shardloom-vortex`. Complete-key COUNT,
ordering, resource ownership and failure semantics remain ShardLoom concerns.
No Arrow execution substrate, new dependency, external query engine, fallback,
new storage encoding or result cache is introduced.

## Decision registered before timing

Run the existing guarded, unsampled paired Q19 harness, three calls per role,
against the same full-size native artifact and complete retained reference with
P12 and the 24 GiB policy budget. Use each role's fastest valid complete call,
preserve every sample, and retain only for at least one second saved. A separate
resource qualification requires at least 30% lower OS peak RSS without slower
complete time. Profiling samples and unrelated slower concurrent-load calls do
not replace either role's comparable fastest valid run.

If retained, finish broad regression gates and full UAT, then open the cohesive
PR before moving to Q33. Otherwise remove the prototype and record its evidence
and drop reason before advancing.

Pre-screen validation: 14 focused tests pass, including cross-chunk dictionary
domains, global winners, signed extrema, negative timestamps, U8 prepared minutes,
invalid losing groups, empty input, ties/OFFSET, large result windows,
nullable/spill rejection, cancellation and committed capacity denial. Formatting
and native-feature all-target Clippy pass. Independent source review found no
remaining actionable issues for the bounded screen. Full-suite UAT and broad
regression gates remain conditional on retention.

## Admission correction before the decisive screen

The first six unsampled paired calls all matched complete reference values but
did not activate the candidate. They are admission diagnostics, not a prototype
performance comparison. Their saved summary is
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/paired43_20260919T200214061560Z/summary.json`.

File metadata confirms that the original integer timestamp is nonnullable while
its embedded prepared minute field is nullable. The existing derived-column
rewrite therefore caused actual worker admission to decline correctly. For the
otherwise eligible, unfiltered triple COUNT shape only, lowering now preserves
the original nonnullable integer minute expression when its prepared replacement
is nullable. Explicit nullable keys remain declined, and predicates retain the
existing rewrite. The source precheck uses this same lowering. A regression test
uses renamed columns, negative timestamps and an all-null prepared field to
verify raw semantics and admission without assuming prepared-field validity.
The actual raw timestamp scan, decode and minute extraction costs belong in the
repeated complete-query screen; no prepared-column timings are carried forward.

## Activated paired screen

The corrected frozen runtime is `e96f8896536949352880c1fdc7b5bdc3d77759b8`, binary
SHA-256 `d2506a5135337c00ae670917edc75eca384fed642d3ab57674e43b270c94a8f5`.
All six complete results match the retained native reference. Each of the three
candidate runs reports `complete_numeric_minute_string_partitions`, 99,997,497
input rows, 56,384,822 exact groups and 43,612,675 existing-key updates.

| Pair | Control seconds | Candidate seconds | Saved seconds | Control peak GiB | Candidate peak GiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 9.501540 | 6.102660 | 3.398880 | 3.332 | 3.775 |
| 2 | 9.541134 | 6.513406 | 3.027727 | 3.299 | 3.484 |
| 3 | 10.568104 | 9.810903 | 0.757201 | 3.344 | 3.214 |

The symmetric fastest-valid comparison saves **3.398880 seconds (35.8%)**, clearing
the one-second speed gate. Fastest-run RSS increases 13.3%; the separate memory
gate does not pass. All samples remain part of the record, including the slower
third pair. This is a scoped achievable-time result on the shared host, not a
latency guarantee or an attribution of the third pair's variation.

The [machine-readable evidence](../benchmarks/q19-complete-key-partitions-2026-09-19.json)
preserves per-run counters, identities, host/build configuration, archive hashes
and the registered decision. The raw summary is
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/paired43_20260919T201636584675Z/summary.json`.
The complete native-reference comparison is regression evidence, not an
independent correctness oracle. Renamed focused fixtures compare with native
serial semantics. Full43 and broad gates must pass before the PR is retained.

Broad checks pass on the frozen runtime: formatting, workspace Clippy, native
all-target Clippy, 3,424 workspace tests and 3,347 native-feature tests. The suites
overlap; nine existing manual native cases remain ignored. Fourteen focused
triple-key tests are included in the native total. Independent runtime review
found no actionable issues. Benchmark constitution, optimization-target and
public-claim validators pass; the architecture tracker retains its expected
116 open phase items. Commands, exit codes and log hashes are in
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/q19-retention-validation.json`
and `q19-docs-validation.json`; the native Clippy log is `q19-native-clippy-v3.log`.

The first Full43 attempt stopped at the existing 256 MiB accumulated-log guard
after 31 passing calls. It is incomplete evidence, not an engine mismatch or a
full-suite score. Fifty-four closed stdout logs were losslessly gzip archived,
verified byte-for-byte and hashed before their raw copies were removed, freeing
6,836,224 accounted bytes. The archive receipt is
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/log-compaction-20260919/receipt.json`.
No storage guard, source artifact or frozen executable changed. The complete
suite is rerun from the beginning rather than combining partial score records.

The complete rerun passes **129/129 calls across all 43 queries**, using the same
frozen `e96f8896` binary and native artifact. Every compressed stdout archive's
raw/compressed hashes and complete results were checked. Q19 is the only Full43
query using the new family, and all three calls report the full expected rows and
groups. The best-of-three sum is **117.174841 seconds**; this later, unpaired suite
is correctness acceptance and a separate timing observation, not a matched
performance delta. Its summary is
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/full43_20260919T202919608170Z/summary.json`.
No ingest rerun is needed for this query-only change on unchanged bytes. Candidate
E is ready for PR; Q33 (F) is next in the existing queue.
