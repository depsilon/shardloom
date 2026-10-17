<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared text aggregation optimization

Five changes in the shared native aggregation path pass their separate focused
comparisons and complete local acceptance. The fresh Full43 pass records
**67.405259296 seconds of engine execution**, with one execution of each query.
The
[design and decision record](../architecture/native-text-aggregation-screen-2026-10-17.md)
preserves each predeclared mechanism, original variant, result and control.
These changes serve the existing SQL, Python and CLI paths without new settings.
[PR #1566](https://github.com/depsilon/shardloom/pull/1566) is integrated at
`4ea5851e` after local and hosted acceptance. The matched whole-workload follow-up
below finds only a small overall improvement.

## Retained mechanisms

| Shared change | Focused target | Control → candidate engine time | Improvement |
| --- | --- | ---: | ---: |
| Borrow chunk extrema; acquire independent ownership only for adopted winners | Q29 | 6.765927376 → 6.564265666 s | 2.98% |
| Use the pinned native byte-search dependency for domain delimiters | Q29 | 6.810786624 → 6.401919502 s | 6.00% |
| Reuse borrowed-entry lookup when binding native dictionary values | Q19 | 5.719205624 → 5.558743042 s | 2.81% |
| Use exact 16-byte partial count records when row and value domains fit | Q34 / Q35 | 2.631022374 → 2.624237210 s / 2.637280208 → 2.455160875 s | 0.26% / 6.91% |
| Route immutable typed keys in two sequential passes with one complete-key buffer | Q19 | 5.646606042 → 5.536037668 s | 1.96% |

Each row compares the candidate with the preceding retained implementation,
using five counterbalanced P12 pairs for its primary target. These are the
fastest valid complete-call engine observations under the frozen rule. Gains
are not additive and do not describe a combined Full43 improvement. Single-worker
and unaffected-query controls remain in each packet, including slower samples.

The broader borrowed-entry variant is dropped because its Q29 target fails the
predeclared improvement rule. The in-place key-routing variant is also dropped:
all five Q19 pairs are slower. The later dictionary-only and sequential-routing
variants have different mechanisms and independent frozen comparisons.
Both routing comparisons trigger a Q35 median investigation; each uses exactly
one reversed-order follow-up, which does not reproduce the material flag.
Neither follow-up rescues the failed in-place primary target.
The complete comparison record contains 364 validated native calls across the
five retained mechanisms, two dropped variants and their bounded follow-ups.

## Correctness and resource boundaries

Temporary extrema borrow existing native owners; escaping winners retain
independent ownership. URL parsing preserves the complete scalar semantics.
Dictionary identities remain first-seen and exact across chunks. Compact counts
retain 64-bit hashes and checked arithmetic; larger row/value domains use the
wide representation. This does not impose a new input limit.

The one-buffer routing path requires the existing immutable typed numeric,
minute and dictionary shape. Other admitted native shapes retain their existing
routing implementation. Both passes preserve exact keys and stable order within
partitions, with reservation before allocation and cancellation before submission.
No query-name branch, floating-point reassociation, new encoding or external
execution provider is introduced. Vortex input/output and no-fallback
certificates remain required.

Focused tests include control-failing ownership and buffer regressions, 1,214
domain-parser cases on both implementations, interner growth and reinterning,
Unicode, exact large counts around `u32::MAX`, raw/prepared renamed inputs,
skew, complete key permutations, invalid dictionary codes, cancellation,
reservation denial and spill.

| Acceptance surface | Result |
| --- | --- |
| Workspace formatting, strict Clippy, default tests and reduced-feature writer tests | Pass; 4,374 test executions, zero ignored |
| Native feature Clippy and library tests | Pass; 2,943 tests, with 27 unchanged declared manual exclusions |
| Native public routes | 1,079 tests pass |
| Direct native binding | 100 methods pass, zero skips |
| Public workflow matrix | 4,827 cases across 36 families pass |
| README documentation contracts | Three checks and seven regression tests pass |
| Fresh ingestion | All 99,997,497 rows validated |
| Fresh format/output workflows | All 18 cohorts pass: 704 native records and 704 independent pandas oracle records |
| Full43 | All 43 queries and 381 complete result rows pass |

These counts describe executed tests, not unique test methods across feature
configurations. Independent public readback checks 39,979 explicit expected-value
rows and 1,578,741 pressure rows. The modular suite exercises eight inputs with
collection and eight outputs from CSV input, in raw and prepared modes across
22 workflows; it is not the complete Cartesian product of formats. Pandas is an
independent correctness oracle only. Full43 uses the retained native complete
reference results, with exact schema, resource and native-certificate checks.

The maintainer-requested README simplification removes the release badge and
support-posture section while retaining the public support-matrix and limitations
links. Two documentation validators and their existing fixture change after the
broader Rust gates; every compiled engine and native test source remains identical.
The source bridge records those three files, the initial missing-report and stale
fixture failures, and the corrected passing documentation checks. Hosted CI and
installed-wheel proof remain integration gates.

## Measurement and reproduction

The frozen final candidate is based on `6b76ff8d` plus ten implementation, test
and dependency files and the three documentation-check files described above.
Its complete 2,410-file source snapshot and source patch are retained. Release
CLI SHA-256 is
`7dd858fb4dd9bcd06353e77e8b9d6c3bcfe72aeb5b60877fa15827f2da3bf146`.
The focused runs use the same accepted 99,997,497-row native file, complete
reference values, exact result schemas and ordinary public CLI. The declared
memory policy is 24 GiB and maximum parallelism is 12 on an Apple M5 with
16 GiB physical RAM and ten logical CPUs. The declaration is not an RSS limit.
OS cache state and desktop activity are uncontrolled; input prehashing is recorded.

Only `engine_execution_seconds` contributes to reported query times. Every
sample, native certificate, complete result, source/build identity and diagnostic
clock is retained. Existing query, storage, concurrency and process-cleanup
guards remain in force. No new benchmark or timing-audit framework is added.

Fresh ingestion reads the resident 14,779,976,446-byte ClickBench Parquet file,
SHA-256 `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.
The 15,713,610,513-byte Vortex result has SHA-256
`06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`,
identical to the preceding accepted output. Both files pass full final hashing,
stable-generation and residency checks. The prior duplicate native payload is
retired only after matching both complete hashes and confirming closed handles;
the newly accepted output and all comparison evidence remain retained.

## Fresh Full43 engine times

This is one complete regression observation with uncontrolled OS cache state.
It does not establish a causal combined speedup over the earlier 68.767127459 s
pass or replace any focused ship/drop comparison. The total is calculated from
the original integer engine nanoseconds; the table retains nine decimal places.

| Query | Engine seconds |
| --- | ---: |
| Q1 | 0.000070542 |
| Q2 | 0.198990542 |
| Q3 | 0.949291792 |
| Q4 | 0.407146584 |
| Q5 | 1.365840040 |
| Q6 | 1.973947000 |
| Q7 | 0.000511001 |
| Q8 | 0.154314041 |
| Q9 | 1.276001335 |
| Q10 | 2.538370000 |
| Q11 | 1.242376291 |
| Q12 | 1.934612250 |
| Q13 | 0.779134667 |
| Q14 | 1.697583292 |
| Q15 | 1.711766002 |
| Q16 | 2.686749040 |
| Q17 | 2.620736708 |
| Q18 | 0.347065583 |
| Q19 | 6.029895084 |
| Q20 | 0.220679500 |
| Q21 | 1.332997625 |
| Q22 | 1.599637584 |
| Q23 | 4.050615833 |
| Q24 | 1.492658208 |
| Q25 | 0.368088417 |
| Q26 | 2.716753457 |
| Q27 | 2.798074083 |
| Q28 | 2.313336542 |
| Q29 | 7.182734541 |
| Q30 | 0.275420959 |
| Q31 | 1.389787500 |
| Q32 | 1.560130750 |
| Q33 | 2.189008166 |
| Q34 | 3.524519250 |
| Q35 | 2.998699209 |
| Q36 | 0.437085668 |
| Q37 | 0.431548959 |
| Q38 | 0.461568625 |
| Q39 | 0.249948959 |
| Q40 | 0.927242499 |
| Q41 | 0.565357501 |
| Q42 | 0.197917708 |
| Q43 | 0.207045959 |
| **Total** | **67.405259296** |

## Portable evidence

The [28,286-member evidence index](evidence/native-text-aggregation-2026-10-17.json)
links four verified archives totaling 29,998,624 compressed bytes and a complete
compressed member manifest. Every member is reopened and hashed. The packet
includes the entire selected source snapshot, original/final source patches,
predeclared designs, five diagnostic stack samples, all 364 focused calls,
both rejected variants, original failures and bounded follow-ups, complete public
and format/workflow results, fresh-ingest records, all 43 outputs and timing/PID
companions, the complete timing CSV and transitive maintained helper sources.
The public and Full43 results are independently checked again from archived bytes.
The original archive field/path lookup failures and corrected readback are retained;
no native execution was repeated to correct packaging.

`receipts/fresh-uat588-plan.json` retains the exact commands and frozen build,
source, reference and input identities. Each focused protocol records its query
order, repetition count, decision rule and comparison binaries. Large datasets
and executables are excluded from the portable archives with their complete
identities preserved. Reproduction requires the named resident inputs and a build
matching the archived source and feature set. Hosted integration is recorded
separately after this immutable local packet.

## Hosted integration and matched comparison

All 44 checks and completed final-head review passed. The tested PR head,
synthetic merge and merged tree agree, including all 2,410 selected source files.
Hosted logs record 6,763 native test executions with 27 unchanged manual
exclusions, 1,079 public cases, and all 100 direct methods in each of three
installed-wheel lanes. Sixteen preview/production responses preserve expected
source artifacts; desktop/mobile navigation and search checks pass.

The [183-member hosted packet](evidence/native-text-aggregation-hosted-2026-10-17.json)
and [archive](evidence/native-text-aggregation-hosted-2026-10-17.tar.xz) retain the
complete logs, source comparison, served/browser evidence and the following
matched cohort. Every member is reopened and hash-checked. Archive SHA-256:
`4190ff494d7127cb01303176fe849a15132a50337b38efe451f8e9e4fb0a694c`.

The follow-up compares both builds on the same native input and resource policy,
with two calls per query per build and counterbalanced ordering. All 172 complete
results match, and no query crosses the predeclared material-regression rule.
These are sums of per-query two-sample means, separate from the single-pass
Full43 result above:

| Scope | Control engine seconds | Candidate engine seconds | Change |
| --- | ---: | ---: | ---: |
| All 43 queries | 65.6668180395 | 65.3663956280 | −0.46% |
| Five priority families | 23.3461796045 | 22.8896376670 | −1.96% |
| Other 38 queries | 42.3206384350 | 42.4767579610 | +0.37% |

This bounded matched cohort does not establish a material overall speedup or
eliminate environmental variation. All samples, including slower calls, remain
in the packet. It motivates the subsequent shared text reconstruction and
validation work rather than a stronger combined performance claim.

This work does not close whole
PERF or CG gates, change the immutable 0.6.0 packages, or establish competitive
superiority. Nullable-root structural-layout fidelity, external-memory support
for remaining resident state, and safe output replacement/publication remain
explicit breadth priorities.
