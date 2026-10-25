<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Vortex provider refresh acceptance — October 24, 2026

The controlled Vortex 0.90.0 refresh passes local runtime and UAT acceptance on
the final matched build. This records provider compatibility and bounded caller
evidence; it does not establish a ShardLoom performance improvement. The
portable evidence replay passes, including eight deliberate corruption controls.
The coherent provider category PR, CI, review and merge remain pending.

## Source and build identity

The runtime refresh is committed locally as
`4117846158edc7b1ab692ef90dd99ba686c5360c`. The frozen release-matched build
recorded `a22760ecb8eaa6b34204a63dfa434be30a1258f1` as its base. Its `artifacts-r2/build-summary.json` records 2,126 source-file hashes
before and after the build, the `release-user-surfaces` profile, and the exact
build report identity. The source binding verifies all 2,139 tracked native and
Python source files against runtime commit `41178461`; eight generated
standalone-provider test lockfiles retain their own identities and are not
required in a clean checkout. Together these are the build's 2,126 native plus
21 runtime Python sources. Acceptance statements below refer to those exact
matched artifacts and sources.

The refresh retains eight focused Vortex path patches, retires the complete
DateTimeParts patch, and uses upstream Arrow/Python 59.3.1 instead of its former
local patch. Targeted evidence covers the empty-buffer owner bridge, referenced
byte costing, bounded immediate Parquet view export, sparse FSST selection and
frozen writer-edition checks. See the [controlled refresh](../architecture/native-vortex-provider-refresh-2026-10-24.md)
for the exact provider obligations and caller limits.

## Final matched-build acceptance

| Surface | Accepted result |
| --- | --- |
| Native workspace | 6,072 passed; 35 existing opt-in ignored |
| Default workspace | 3,246 passed |
| Formatting, strict lint, all-feature/minimal and Rust 1.96.0 checks | Passed |
| Installed Python wheel | 140 native integration tests passed; no skips |
| Public suite | 5,930 cases across 39 families passed |
| Saved public semantic replay | 5,639 actual/expected complete-value files; 56 nested and 81 case JSONL archives validated |
| Recovered modular matrix | 1,408 accepted records across 18 cohorts: 704 native and 704 independent-oracle |
| Fresh ingest | 99,997,497 rows; native stream digest matches the output file |
| Isolated Full43 | 43 queries once each; 381 complete result rows and exact schema continuity |

The final source checks `check-msrv-native-090-r2`,
`check-msrv-no-default-090-r2` and
`check-workspace-all-features-090-r2` all pass. Public inventory annotations
from the earlier pass are superseded by
`public-reference-primary-verification-r4.json`, which verifies the erratum and
all 137 public archive records. Separately,
`provider-validation-primary-r5.json` preserves 138 validation receipts and
identifies two historical zero-execution filtered tests
(`test-fsst-activation-090-r1.json` and `test-writer-edition-090-r1.json`).
The installed wheel result is tied to the final matched build.

## Recovery history and limits

The first modular r1 attempt wrote output successfully but failed strict report
validation because the nested primitive writer emitted duplicate report fields.
The second r2 attempt completed 17 cohorts, then the unchanged 192 MiB
retained-log ceiling interrupted the 18th before its third native query. After
verified lossless log compaction, the final matrix reused those 17 completed
cohorts and reran only the final 44-record cohort with the same command, build,
input workspace and limits. The accepted matrix preserves both interruptions
and all prior partial records; neither failure is reclassified as a pass.

The first fresh-ingest attempt failed before engine execution because it used
system Python. The accepted retry changed `PATH` only and produced the recorded
99,997,497-row Vortex artifact. Ingest acceptance uses the minimal digest path;
it does not claim an independent full 100-million-row readback. Subsequent
Full43 and public writer/reopen checks supply the accepted query and persistence
scope.

The isolated Full43 engine total is **49.625514041 seconds**, compared with the
prior CLI observation of **48.701809680 seconds**. Each pass runs the 43 queries
once and reports its own engine time. These are single observations on
different generated physical inputs and do not establish a causal speedup,
regression or equivalence. No further benchmark repetition is implied.

The accepted scope does not admit package publication, remote I/O, rich-data
execution or V2 list-layout execution. V1 reader-cache reuse remains admitted;
V2 retained-state lifecycle questions remain with the warm-source package.
Rich-type and remote-provider work stays in its queued packages, and optional
encoding, Bloom-filter and index experiments remain conditional.

## Evidence packet

The [replay result](evidence/native-vortex-provider-refresh-2026-10-24/replay.json)
records the accepted verification. The
[manifest](evidence/native-vortex-provider-refresh-2026-10-24/manifest.json)
binds 52,268 file identities to 32,927 distinct objects across four verified
archives. The [verifier](evidence/native-vortex-provider-refresh-2026-10-24/verify_packet.py)
replays both preserved public suites, all 1,408 accepted modular records and
1,780 native/oracle command payloads, fresh-ingest linkage, 43 complete query
results and schemas, the installed-wheel proof, and all 138 validation receipts.
Ten validation gates match the final source set. It executes no engine or
archived helper code and does not claim to rehash retired bulk payloads.

The original selection inventory is retained as compressed JSON. Primary review
restored 81 case-level archive paths omitted by the historical public-r1
inventory; their exact bytes already existed as shared public-r2 objects.
The verifier checks all 137 case/nested archives in each public run. Original
failed records and obsolete inventory annotations remain distinguishable from
accepted evidence. An initial replay incorrectly required SQL-route fields from
the separate `vortex-prepare` report namespace; the corrected replay preserves
native no-fallback and unique-field checks for both contracts.

Eight corruption controls reject source mismatch, skipped installed tests,
missing Full43 results, substituted engine timing, expanded ingest readback
claims, changed modular values or SQL, and missing case-level rows. Six focused
regressions also cover source-file absence, generated test locks, strict JSON,
complete values and preparation-report evidence.

```bash
python -B docs/benchmarks/evidence/native-vortex-provider-refresh-2026-10-24/verify_packet.py --self-test
```

Use `--check-current-source` when checking this provider runtime revision.

A [clean checkout](evidence/native-vortex-provider-refresh-2026-10-24/clean-checkout.json)
of evidence commit `f7b1e566` passes the complete replay with current-source
verification, all eight corruption controls and the six focused regressions.
All eight generated standalone-provider lockfiles are absent in that checkout.
The proof requires neither those ignored files nor the original local output
directories.

The [cleanup receipt](evidence/native-vortex-provider-refresh-2026-10-24/cleanup.json)
records retirement of 49,949 generated regular files and six environment
symlinks after exact inventory, packet matching and repeated closed-handle
checks. Completed public/matrix outputs, superseded binaries and wheel,
temporary package environments, packet staging copies and the clean-checkout
clone are removed. The original Parquet, accepted Vortex input, current runtime,
tested wheel and all reports remain; 262 unarchived helpers, fixtures and
diagnostics are explicitly preserved. Post-cleanup checks verify 291 retained
file identities, all four packet archives and both input generations. No
payload backup, storage-limit change or benchmark rerun occurs.

The authoritative local acceptance records are `provider-validation-primary-r5.json`,
`public-reference-primary-verification-r4.json`,
`artifacts-r2/build-summary.json`, `public-transcript-verification-r3.json`,
`modular-r1/provider-native-matrix-2-recovered-report.json`,
`fresh-ingest-acceptance-r2.json` and `full43-acceptance-r1.json` in
`/Users/dylan/LocalData/shardloom/vortex-provider-refresh-20261024/`.
