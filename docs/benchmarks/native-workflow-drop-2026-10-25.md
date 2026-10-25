<!-- SPDX-License-Identifier: Apache-2.0 -->

# October 25 complete-workflow item 15 drop

Status: item 15 is closed as dropped at the maintainer's request. The whole
unmerged runtime batch was removed, including the unstarted item 15 branches
and packages for shared execution, decorrelation, keep-data-moving and setup.
The drop removed 37 tracked runtime files and 11 new Rust files.
The runtime source is restored to latest merged main,
`167ac248ba662ae7967a7801995e9b160b17bae5` (#1605). No item 15 runtime change
is retained.

## Full43 observations

Each observation used one isolated pass: each of the 43 queries ran once. The
records verify 381 complete result rows and exact schemas under the 24 GiB,
12-worker configuration. The observations are separate runs; they do not
identify a cause for the timing differences.

| Observation | Runtime identity | Full43 engine time | Result |
| --- | --- | ---: | --- |
| Combined item 15 candidate | Candidate runtime before removal | 52.873938411 s | All 43 results, 381 rows and schemas validated; failed the frozen regression gate at +12.5617926% versus the prior accepted 46.973255495 s. |
| Restored accepted executable | Exact previously accepted executable | 47.176619540 s | Correctness and identity checks passed. |
| Fresh latest-main executable | Source restored to #1605 main | 47.889862794 s | All 43 results, 381 rows and schemas validated; +1.95133867% versus the prior accepted observation. |

The candidate exceeded the 10% regression threshold, so the maintainer dropped
the complete item 15 batch. The restored executable and fresh latest-main
executable provide separate observations; neither isolates the contribution of
an individual change. The earlier counted-join and fused-measure mechanism
passes remain historical evidence only and do not represent retained runtime
claims.

## Item 16 ingestion baseline

The active [ingestion representation and batch/window handoff](../architecture/native-ingest-batch-window-scheduling-2026-10-25.md)
starts from the latest-main ingest record `current-main-r1-acceptance.json`.
That isolated ingest workflow took 49.303 seconds: 42.115 seconds for
preparation and 7.188 seconds for sequential publication. It produced
99,997,497 rows, 112 columns and a 15,715,228,660-byte Vortex output with
SHA-256
`95a393d153a8c2e1ffeb5084b2656ac61465c6ef9dad435d032365844f1ac057`, equal to
the accepted output. This is the baseline for item 16's finite before/after
decision, not a speedup claim. Item 16 requires independent readback, subsequent
queries and a final isolated Full43 before its retain/drop decision is closed.
The 1.0.0 major-version bump follows item 16 whether its candidate is retained
or dropped; package publication remains separately authorized.

## Evidence and history

The local ingest acceptance record is at
`/Users/dylan/LocalData/shardloom/ingest-representation-20261025/current-main-r1-acceptance.json`.
The [portable workflow-drop packet](evidence/native-workflow-drop-2026-10-25/README.md)
will bind the original receipts, failed observations, restored runs, query
comparisons, drop manifest and candidate patch by content hash. The three
acceptance records, `full43-query-differences-latest-main.txt`, and
`dropped-batch-r1/{manifest.json,candidate-runtime.patch}` are among its inputs.
The larger `dropped-batch-r1/candidate-changed-files.tar.xz` remains preserved
locally and is intentionally excluded from the portable packet.

The earlier [work-elimination handoff](../architecture/native-workflow-work-elimination-2026-10-25.md),
[counted-join mechanism record](../architecture/native-join-count-work-elimination-2026-10-25.md),
[fused-measure evidence](native-fused-measure-2026-10-25.md),
[multi-output investigation](../architecture/native-multi-output-work-elimination-2026-10-25.md),
and [item 16 ingestion handoff](../architecture/native-ingest-batch-window-scheduling-2026-10-25.md)
remain available as historical source and decision context. The finite
mechanism decisions in those records do not override this whole-batch drop.
