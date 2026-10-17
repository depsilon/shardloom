<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native file metadata ownership and fresh UAT

The native reader now admits copied footer, schema/layout, cached-segment
realignment and explicitly requested metadata buffers through the existing
session allocator. Retained schemas, layouts and buffer clones keep their
credits until their final owner drops. The [contract](../architecture/native-file-metadata-buffers-2026-10-16.md)
defines the finite ownership boundary and preserved raw compatibility API.
Local acceptance is complete; installed-wheel and hosted integration remain pending.

## Accepted source and correctness

The frozen release CLI and binding use source
`277699af4204d71c1c8377dd11dadfb15d122d26`, based on merged worker correction
`88c836bd64e1ff93ebc4978ae35e187ed10cb026`. All 2,410 selected source identities
are retained and verified. CLI SHA-256 is
`373f05a3f1f35488189ccf5a780c85f58da97daec5392c95a8014b99caaa6be0`.

Three original-provider regressions reproduce missing allocation admission or
retained ownership. All sixteen focused ownership tests and 139 standalone
provider tests pass. The published Apache-2.0 `vortex-file` 0.85.0 archive,
all 33 original members and the exact modified-source provenance are retained.
No dependency version, native format or external-engine execution path changes.

| Acceptance surface | Result |
| --- | --- |
| Workspace formatting, strict Clippy and tests | Pass; 3,225 tests, zero ignored |
| Native library | 2,920 distinct tests pass after the two test-lifetime corrections below; 27 declared manual exclusions |
| Native public routes | 1,079 tests pass |
| Preparation features | 1,149 tests pass |
| Release-script tests | 164 pass |
| Frozen direct native binding | 100 methods pass, zero skips |
| Public workflow matrix | 4,827 cases across 36 families pass |
| Fresh ingestion | All 99,997,497 rows validated |
| Fresh format/output workflows | All 18 cohorts pass: 704 native records and 704 independent pandas oracle records |
| Full43 | Both observations pass all 43 queries and 381 complete result rows |

The complete native run initially passed 2,918 tests and failed two old cleanup
assertions that still retained cloned schemas while expecting zero credits.
The corrections check live credit, drop those schemas, then require zero.
Both focused reruns pass with unchanged production sources. The original run,
exact test-only edits and reruns remain separate evidence; this is not relabeled
as one clean full-suite run. Provider formatting/strict Clippy, CI-matrix and
license-bundle checks also pass.

The modular suite exercises eight inputs with collection and eight outputs
from CSV input, each in raw and prepared modes across 22 workflows. This is not
the full Cartesian product of inputs and outputs. Pandas supplies an independent
testing oracle only; all ShardLoom execution remains native with no fallback.
Full43 compares complete values with the retained native reference packet and
checks exact schemas, native certificates and resource declarations.

## Engine timing observations

| Complete single pass | Sum of engine execution for 43 queries |
| --- | ---: |
| Retained-input acceptance | 69.556562366 s |
| Requested fresh-ingest UAT | 64.908912375 s |

Each pass executes every query once using the unchanged standard query runner,
24 GiB declared memory and maximum parallelism 12. The machine is an Apple M5
with 16 GiB physical RAM and ten logical CPUs on macOS arm64. The memory request
is an admission policy, not a whole-process memory limit or a statement of
physical capacity. OS page-cache state is uncontrolled; preparation and hashing
can affect it. These are two observations, not a paired causal comparison or a
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

The [packet index](evidence/native-file-metadata-buffers-2026-10-17.json) binds
28,207 verified members in three archives to the frozen source. It includes
all selected runtime/test/provider sources, original failures, commands,
receipts, public values, both Full43 runs and all fresh modular cohorts.
Archived public results and both Full43 observations also pass semantic readback.
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
