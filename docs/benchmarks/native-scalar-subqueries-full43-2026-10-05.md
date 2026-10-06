<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native scalar-value subqueries: local core acceptance

The frozen implementation passes 23,786 public workflow checks and 15,351,524
complete row comparisons. The scalar-value family contributes 1,128 checks:
1,018 complete value/resource proofs and 110 explicit denials, covering 2,174
rows. A separate retained-workflow matrix passes 202 checks and 131,734 rows.
All 129 Full43 executions pass through the shared native engine. These are
correctness and availability results; no comparative performance claim follows.

SQL scalar expressions and Python `sl.scalar_subquery(...)` use the existing
native subquery, expression, source, resource and writer owners. The
[implementation contract](../architecture/native-scalar-subqueries-2026-10-05.md)
defines the admitted scope. Current source builds include this addition;
published v0.4.0 packages predate it.

Hosted integration completed in [PR #1524](https://github.com/depsilon/shardloom/pull/1524)
after all 39 checks passed on `c8cc6ab58ad827f9362d2a9b614dc14c53614a8c`.
Merge `024b872dc89fec05e4bc7ff7a499725d2d9a831e` has the same tree as that accepted
head. Primary adversarial review passed; no independent submitted review is
claimed, and the automated review bot reported the account review limit.
The [hosted integration receipt](evidence/native-scalar-subqueries-hosted-2026-10-06.json)
preserves pre/post-merge snapshots, final review state and successful production
deployment. Ordinary browser checks passed for preview and production guidance,
the support label and its link. Anonymous HTTP checks returned 403; complete
deployed HTML byte identity remains unverified.

## Frozen identities

| Artifact | Identity |
| --- | --- |
| Runtime source commit | `8440b04f8f786cfd0fc8444e0e7b2caa4fcb65d9` |
| Runtime source tree | `a2039a25b9b4108e20d6c8bf7e6700f2e5d5f3e8` |
| Native executable SHA-256 | `92968440064b61e662d0c7027198fde5404d8712f674f1ddeef478c7addbd29a` |
| Source-check manifest SHA-256 | `c18378c50441027ddd9d9f1c2b4df67ede83870c062de8f5acc9fb1643541101` |
| Acceptance packet | [native scalar-subquery packet](evidence/native-scalar-subqueries-2026-10-05.json.xz) |
| Packet compressed size / SHA-256 | 41,218,972 bytes / `2eac32d1a2bb27735d310846cbd98706b7b44c999acd9e3628b15ec9d303575c` |
| Packet uncompressed size / SHA-256 | 3,155,636,023 bytes / `d1c4e813b592e36290f7b0db0f88375949834df30c01cfdae953f764ce577092` |

The release build uses Rust 1.99.0 and `shardloom-cli/release-user-surfaces`.
All 901 frozen Rust, Python and Cargo source assets are retained in the packet,
along with the exact commands, guards, fixtures and executable identities.
An [independent streaming inspection](evidence/native-scalar-subqueries-inspection-2026-10-05.json)
verifies the completed packet's counts, source identities, resource proofs,
native-family reports, no-fallback fields and portable paths. Its policy-field
reader separately passes 96 policy cases, four ordinary-field cases and 12
path cases distinguishing generic search literals from concrete private paths.

The first independent inspection reached its 600-second deadline while parsing
the complete archive. Its supervisor drained the process group and retained
the timeout receipt. A second inspection completed but counted its own archived
generic user-root search literal as a private path. A full bounded scan located
both literal occurrences in that checker source. After correcting only the
independent checker and testing concrete-path rejection, the third inspection
passed against the unchanged packet. Every other counter remained identical.
The inspection receipt retains all three attempts, the diagnosis, exact checker
changes and their fixture results.

The [documentation integration record](evidence/native-scalar-subqueries-integration-2026-10-05.json)
records twelve passing documentation and website gates on `0776f888`, an
additional passing scope check, and desktop/mobile Field Guide inspection.
All 901 runtime source hashes remain unchanged. The architecture tracker still
reports 130 unchecked phase items; its CI-compatible `--allow-blocked` command
does not certify completion of that queue.

The source manifest records 27 passing gates: formatter, strict default and
native Clippy, workspace tests, native Vortex and CLI tests, Python tests,
native-without-write and lean builds, minimum-supported Rust 1.96 builds,
four harness/storage/consumer suites, contribution governance, CI matrix,
API/schema stability, website dependency audit/build/check, user-surface
reference, public status, documentation productization, front-door scope,
static assets and website readiness. A further run with the retained optional
dependencies passes all 599 Python tests with no skips. Test configurations
overlap and do not represent distinct-test totals.

## Complete public workflows

| Family | Checks |
| --- | ---: |
| Base relational workflows | 797 |
| Unary composition | 1,166 |
| Nested composition and retained state | 5,214 |
| Dynamic scalar pivot | 846 |
| Typed payloads, keys, expressions, unary state and reductions | 11,513 |
| Typed memory and source-free workflows | 909 |
| Analytic frames | 2,213 |
| Scalar-value subqueries | 1,128 |
| Total | 23,786 |

The scalar oracle freezes 42 successful declarations and nine negative
declarations before candidate execution. It covers both SQL and Python,
repeated collection, all eight representable local writers and declared input
formats, renamed inputs, explicit outer parameters, expression composition,
empty/all-null input and selected conditional demand. Complete JSON/CSV
readbacks and native Vortex schema readbacks verify values and persistence.
Existing format-specific denials remain part of the matrix. Five
result-container forms are exercised across the full public suite; NumPy, pandas and
PyArrow serve input/result boundaries, without executing query residuals.

The packet retains 49,298 public raw envelopes and a separate 2,414-envelope
focused scalar replay. These cohorts overlap. It also records 145
admitted-semantics stages, nine golden workflow stages and the two documented
SQL/Python examples, whose six rows match their literal expectations.

## Semantics, resources and failure evidence

An inner query must bind one static output column. Zero rows yield a typed
NULL; one row yields its value; a second row fails even when its value equals
the first. Explicit `outer.<column>` references retain the nearest admitted
scope. An uncorrelated query runs on first demand per execution; a correlated
query has fresh state for each selected outer row. CASE/COALESCE preserve
selected-branch evaluation while all branches still pass static admission.
Dynamic-pivot-dependent scalar schemas and lateral relations are rejected.

Native tests also cover results split across batches, exact logical dtypes,
nullable nested children, selected rows after an unused outer batch,
cancellation, source replacement, narrow grants, retained output ownership,
failed consumers and cardinality failure after provisional output. Existing
inner operator budgets and spill/denial policies remain in force. These
reservations do not establish complete reader/codec accounting or a process-RSS
ceiling.

Development checks caught and corrected parenthesized COUNT DISTINCT parsing
and arithmetic with a leading CAST. Failed logs remain in the packet. The first
Full43 storage preflight stopped before queries. Verified compaction of 516
completed call artifacts recovered 3,362,816 accounted bytes under unchanged
limits. The continuation retained passed focused checks only after verifying
their receipts, executable and all frozen source hashes; it then ran the entire
public matrix, direct matrix and Full43 cohort. Every archived member was
reopened during finalization. Failed and incomplete observations remain intact.

## Full43 regression boundary

Each of 43 queries runs three times through the public native SQL workflow.
Every complete result matches the retained native reference, which is
regression evidence rather than a fresh independent correctness oracle.
Independent small fixtures establish the new scalar semantics.

The retained input contains 99,997,497 rows and 112 columns in a
15,682,956,489-byte Vortex file. Its SHA-256 is
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`;
the query file SHA-256 is
`4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b`.
The reference packet SHA-256 is
`cb83c770674073f31ad0c4224f02faeb1ba557c512595a5c2d36f92bb2344b39`.
Input residency and identity were checked before hashing. No fresh ingest was
performed for this cohort.

The host is macOS 27.0 arm64 with 10 logical CPUs and 16 GiB physical RAM.
Full43 retains a 24-GiB memory admission policy and maximum parallelism of 12;
those settings are distinct from physical RAM and measured process memory.
Native work remains serial across builds, tests and acceptance under the
existing storage/process guards. The source was prehashed; OS cache and ordinary
desktop activity were uncontrolled. There is no paired timing comparison,
answer cache or external-engine execution.

Core-local status is `passed_core_local`; hosted review and integration remain
pending. Broader nested-pivot, adapter and resource/recovery work stays open.
This report does not certify a new package release, production support,
competitive superiority or the prior modular workload harness with this binary.
CG-1 through CG-23 retain their independent obligations; real native Vortex
payload evidence remains distinct from placeholder artifact status.
