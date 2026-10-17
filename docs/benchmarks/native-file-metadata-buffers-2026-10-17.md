<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native file metadata ownership and fresh UAT

The native reader now admits copied footer, schema/layout, cached and in-memory
segment realignment and explicitly requested metadata buffers through the existing
session allocator. Retained schemas, layouts and buffer clones keep their
credits until their final owner drops. The [contract](../architecture/native-file-metadata-buffers-2026-10-16.md)
defines the finite ownership boundary and preserved raw compatibility API.
The reader also skips unrequested optional metadata and avoids constructing a
discarded cache for in-memory opens. Corrected local and hosted acceptance are
complete. [PR #1565](https://github.com/depsilon/shardloom/pull/1565) merged at
`6b76ff8d29b785668e2d3c73852547e17b51032a` after all 44 checks passed.

## Accepted source and correctness

The frozen release CLI and binding use source
`566643cd83a7b528bf225496ebd5b3b9a9fa4e5a`, based on merged worker correction
`88c836bd64e1ff93ebc4978ae35e187ed10cb026`. All 2,410 selected source identities
are retained and verified. CLI SHA-256 is
`4dff4bfea01887fe960e081e38a1e556a51cabe04f6af64aea53762ed77f51a6`.

Three original-provider regressions reproduce missing allocation admission or
retained ownership. Four further regressions reproduce the review's in-memory
alignment and unused-cache defects before correction. All twenty focused
ownership tests and 139 standalone provider tests pass. The published
Apache-2.0 `vortex-file` 0.85.0 archive,
all 33 original members and the exact modified-source provenance are retained.
No dependency version, native format or external-engine execution path changes.

| Acceptance surface | Result |
| --- | --- |
| Workspace formatting, strict Clippy and tests | Pass; 3,225 tests, zero ignored |
| Native library | Clean complete run: 2,924 pass, zero fail; 27 unchanged declared manual exclusions |
| Native public routes | 1,079 tests pass |
| Preparation features | 1,149 tests pass |
| Release-script tests | 164 pass |
| Frozen direct native binding | 100 methods pass, zero skips |
| Public workflow matrix | 4,827 cases across 36 families pass |
| Fresh ingestion | All 99,997,497 rows validated |
| Fresh format/output workflows | All 18 cohorts pass: 704 native records and 704 independent pandas oracle records |
| Full43 | Corrected-source fresh pass validates all 43 queries and 381 complete result rows; both earlier observations are retained |

The original complete native run passed 2,918 tests and failed two old cleanup
assertions that still retained cloned schemas while expecting zero credits.
The corrections check live credit, drop those schemas, then require zero.
Both focused reruns pass with unchanged production sources. The original run,
exact test-only edits and reruns remain separate evidence; this is not relabeled
as one clean full-suite run. After the four additional review regressions and
their production corrections, a new complete native run passes all 2,924 tests.
A strict-Clippy failure in one new test's assertion style is also retained;
the corrected assertion and renewed lint pass. Provider formatting/strict Clippy,
CI-matrix and license-bundle checks pass.

The modular suite exercises eight inputs with collection and eight outputs
from CSV input, each in raw and prepared modes across 22 workflows. This is not
the full Cartesian product of inputs and outputs. Pandas supplies an independent
testing oracle only; all ShardLoom execution remains native with no fallback.
Full43 compares complete values with the retained native reference packet and
checks exact schemas, native certificates and resource declarations.

## Hosted integration

The [135-member hosted packet](evidence/native-file-metadata-hosted-2026-10-17.json)
binds final PR head `0c4a78e3`, its CI merge and the merged commit to all 2,410
source identities of the accepted frozen CLI and binding. Complete raw logs
confirm 6,744 native/provider tests, including all twenty file-metadata ownership
regressions, and 1,079 public-route tests. The 27 declared native manual exclusions
remain explicit. All three installed-wheel lanes pass 100 methods each without
failures or skips. All eleven review threads are resolved; the final-head review
reports no findings and no PR code-scanning alerts remain open.

Preview and production each preserve reviewed source across eight HTTP responses;
the production check admits only its recorded Cloudflare script insertion.
Desktop/mobile rendering, search for the current 68.767-second evidence,
section navigation, the mobile menu and browser-console checks pass. Every
hosted packet member is reopened and hashed. The local packets retain their
historical premerge status; this separate packet completes their hosted proof.
No benchmark was rerun for integration and no package was published.

## Engine timing observations

| Complete single pass | Sum of engine execution for 43 queries |
| --- | ---: |
| Initial source `277699af`, retained-input acceptance | 69.556562366 s |
| Initial source `277699af`, requested fresh-ingest UAT | 64.908912375 s |
| Corrected source `566643cd`, fresh-ingest UAT | 68.767127459 s |

Each pass executes every query once using the unchanged standard query runner,
24 GiB declared memory and maximum parallelism 12. The machine is an Apple M5
with 16 GiB physical RAM and ten logical CPUs on macOS arm64. The memory request
is an admission policy, not a whole-process memory limit or a statement of
physical capacity. OS page-cache state is uncontrolled; preparation and hashing
can affect it. These are separate observations, not a paired causal comparison or a
speedup claim. The earlier timing variation remains evidence for the following
optimization work, without assuming that it will resolve automatically.

Only `engine_execution_seconds` contributes to these totals. Fresh ingestion,
builds, preparation, hashing, result serialization, external validation,
archival and cleanup add nothing to them. The packet retains each query's raw
clock fields, complete timing CSV and other diagnostic measurements separately.

Fresh ingestion uses the resident 14,779,976,446-byte ClickBench Parquet source,
SHA-256 `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.
The new 15,713,610,513-byte Vortex output has SHA-256
`06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`,
matching the earlier accepted native artifact byte for byte. Final source,
binary, dataset-generation and complete-file identity checks pass.

## Reproduction and evidence boundary

The [original packet index](evidence/native-file-metadata-buffers-2026-10-17.json) binds
28,207 verified members in three archives to the frozen source. It includes
all selected runtime/test/provider sources, original failures, commands,
receipts, public values, both Full43 runs and all fresh modular cohorts.
Archived public results and both Full43 observations also pass semantic readback.
The [review-correction packet](evidence/native-file-metadata-review-2026-10-17.json)
adds 25,641 verified members in two archives: all four review reproductions,
the failed lint, renewed gates, release artifacts' identities, all 4,827 public
cases, fresh ingestion, Full43 and all 18 modular cohorts. Its six changed
selected sources overlay the hash-bound parent packet's 2,404 unchanged sources.
Every archive member is reopened; archived public results and the new Full43
values, schemas and engine clocks also pass semantic readback. The nine CodeQL
logging alerts are preserved with their source-specific false-positive
dispositions: they concern generated nonsecret fixtures in private provider
tests. No runtime source, scanning rule or coverage is excluded for that triage.
Large source/output datasets and executable binaries have explicit identity
records rather than duplicate archived payloads.

Reproduce with the recorded release-user-surfaces build, the existing
`scripts/run_clickbench_ingest_uat.sh` and `scripts/run_clickbench_query_uat.py`,
one execution per query and the retained complete reference packet. Use the
recorded modular runner and cohort arguments for format/output acceptance.
Existing storage, residency, concurrency and cleanup guards remain enabled.
No new timing-audit script or benchmark method is introduced.

Structural metadata collections, writer scratch, fixed allocator overhead and
process RSS remain outside this finite buffer contract. The separately retained
raw nullable-root projection finding remains under its existing semantics owner.
The immutable 0.6.0 packages are unchanged; these local results do not establish
public package availability, broad production support or competitive superiority.
