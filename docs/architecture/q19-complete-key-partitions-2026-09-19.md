# Q19 complete-key partition screen

Status: bounded prototype under validation; no retained speedup claimed.

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

Pre-screen validation: 13 focused tests pass, including cross-chunk dictionary
domains, global winners, signed extrema, negative timestamps, U8 prepared minutes,
invalid losing groups, empty input, ties/OFFSET, large result windows,
nullable/spill rejection, cancellation and committed capacity denial. Formatting
and native-feature all-target Clippy pass. Independent source review found no
remaining actionable issues for the bounded screen. Full-suite UAT and broad
regression gates remain conditional on retention.
