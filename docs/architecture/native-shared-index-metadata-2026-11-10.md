<!-- SPDX-License-Identifier: Apache-2.0 -->

# Compact metadata for the shared native row index

Status: recommendation 8 retained and integrated; verified retirement complete.
This is a bounded item 20 / RFC 0044 experiment under the
[shared-pipeline program](native-shared-pipeline-investigations-2026-11-10.md).
Recommendations 9 and 10 remain independent. Item 19, item 20, RFC 0044 and
CG-1 through CG-23 remain open; no broad competitive claim follows.

## Decision and complete-cohort result

Retain the one frozen compact-metadata candidate. All 2,452 candidate cases
passed complete result, engine-clock, resource and no-fallback checks. Their
engine sum is **319.977114074 seconds**, against **320.508980218 seconds** for
the declared baseline: **0.531866144 seconds lower (0.165944225%)**. The
maintainer's binary rule accepts any lower complete-cohort engine sum after the
other gates pass; there is no minimum percentage gain.

This is one observation per candidate case, with no warmup, repeat, cache
manipulation, concurrent task work or performance tuning after the result.
The baseline explicitly reuses 433 exact saved cases from the accepted
dictionary-repair runtime and measures the remaining 2,019 cases in a separate
isolated pass. It is not one new full10k pass, a statistical confidence claim or
proof that the removed loads caused every timing difference.

The [portable evidence packet](../benchmarks/evidence/native-shared-index-metadata-2026-11-10/README.md)
binds the fixed selection, requests, reference answers, complete raw results,
engine clocks, source, runtime, diagnostics and original failures. It rechecks
4,904 saved baseline/candidate results without executing queries. The original
current-runtime 10,000-case observation remains invalid: 9,904 passed, 76
crashed and 20 had harness errors. The separate dictionary repair's 517 passing
cases do not turn that original observation into a valid complete score.

| Family | Cases | Baseline engine seconds | Candidate engine seconds | Candidate minus baseline |
|---|---:|---:|---:|---:|
| Composite | 1 | 0.196451791 | 0.206145667 | +0.009693876 |
| CTE composition | 77 | 6.574839740 | 6.481490618 | -0.093349122 |
| Distinct | 52 | 3.800366995 | 3.809380041 | +0.009013046 |
| Global aggregation | 39 | 0.754539622 | 0.743973121 | -0.010566501 |
| Grouped aggregation | 568 | 31.666012791 | 31.432966664 | -0.233046127 |
| Joins | 525 | 82.513678364 | 82.738867255 | +0.225188891 |
| Multiway joins | 110 | 22.149421166 | 22.321152454 | +0.171731288 |
| Nested | 30 | 2.691527295 | 2.598333494 | -0.093193801 |
| Nullable | 31 | 3.932252750 | 3.878984629 | -0.053268121 |
| Numeric | 34 | 1.111719916 | 1.139965249 | +0.028245333 |
| Ordering | 32 | 15.387749505 | 15.672170962 | +0.284421457 |
| Predicates | 72 | 2.970187626 | 2.738715002 | -0.231472624 |
| Projection | 7 | 0.258170125 | 0.204212875 | -0.053957250 |
| Sets | 296 | 40.113036128 | 40.161269036 | +0.048232908 |
| Strings | 67 | 5.702321290 | 5.772477828 | +0.070156538 |
| Subqueries | 229 | 53.175659079 | 53.049873425 | -0.125785654 |
| Temporal | 94 | 5.216377418 | 5.166514376 | -0.049863042 |
| Windows | 188 | 42.294668617 | 41.860621378 | -0.434047239 |
| **Complete cohort** | **2,452** | **320.508980218** | **319.977114074** | **-0.531866144** |

There are 1,382 lower and 1,070 higher individual engine observations. Ordering,
ordinary joins and multiway joins are the largest family regressions and remain
part of the complete decision. None is removed from the denominator.

## Native owner and admission contract

The existing
[RowIndex](../../shardloom-vortex/src/local_primitives/native_relational_index.rs)
retains its buckets, stable entry identities, inline first row, ordered duplicate
links, full hashes and caller-defined exact equality. An optional charged byte
vector screens lookup candidates before loading their full entries. A seven-bit
fingerprint never establishes equality.

Controls use `0x80` for empty and the high seven hash bits for occupied slots.
Seven mirrored tail bytes permit safe eight-byte little-endian loads at every
bucket position. Exact matching-byte masks stop at the first empty lane and visit
candidates in the original linear order. The full hash and exact native equality
still decide each match. Cancellation checks remain at most 1,024 logical slots
apart, including the final check after equality.

The fixed admission rule requires bucket capacity at least 128 and
`capacity + 7` control bytes no greater than one sixteenth of free operation
credit. Free credit is observed after reserving replacement buckets, while old
buckets and controls are still charged. New controls use a fresh exact
reservation. Old and new storage overlap until successful publication. An
admitted allocation or cancellation error propagates; pre-admission decline uses
the existing ordinary native layout. No constants were tuned after scoring.

Insertion and rehash keep their original vacancy algorithm and fallible
publication order. Controls publish only after all fallible work and the final
cancellation check. No new equality owner, unsafe code, dependency, public API,
Arrow execution boundary or external-engine delegation is introduced.

The Vortex-first decision is `implement_shardloom_kernel` in the existing
logical row-index owner: the pinned Vortex structural-array hash/equality
surface does not replace caller-defined row equality. Native Vortex input/output,
encoded key ownership and certificates remain authoritative.

## Attribution and resource cost

Separate debug-only diagnostics execute 22 complete requests and eight cyclic
consumer calls. In the 11 large-layout requests, full-entry reads fall from
86,369 to 30,671. The 55,698 avoided reads equal the baseline's screenable
fingerprint mismatches. Logical probe counts, exact equality calls, vacancy and
rehash work, complete output identities and schemas remain unchanged. These
counters are absent from the ordinary release runtime used for scoring and are
not CPU-time attribution.

Tiny cases allocate no metadata and retain the same probe counts. Two tiny
grouping cases nevertheless increase peak charged memory from 277,589 to
277,909 bytes: the embedded optional owner makes reserved row-set containers
larger. Larger cases add up to 16,398 peak charged bytes. All diagnostics stay
within their 64-MiB grant and refund all credits. This is not a zero-cost small
table claim or a total-memory reduction. The existing exact eight-entry,
320-byte grant continues to pass.

## Correctness, provenance and remaining acceptance

The baseline is accepted merge
`037b6801320aa0bfe939ba10aed9304198b77a5a`. The final scored candidate source
fingerprint is
`114c325ac558cedcf63697a7eb5439934d8c65afaf4537d5b7913282e2411fda`.
The 2,452-case selection was frozen by mechanism, route, shape, build, stress
and single-factor coverage before candidate execution, including 695 holdout,
1,565 primary, 63 scale and 129 smoke cases across 18 families. The original
65-file client and three separately admitted protocol imports remain exact.

Local validation passes formatting, strict default/native workspace lint,
3,323 default tests and 7,075 native tests (38 explicitly ignored). The focused
mechanism run passes 16 tests, including all 14 index owner/metadata tests, the
cyclic consumer control and the explicitly included diagnostic request test.
The native suite also contains all 41 frozen named consumer controls. Direct
tests cover exhaustive byte masks, occupancy and wraparound, full-hash
collisions, ordered duplicates, exact grants, admitted/declined metadata,
populated growth denial/retry, cancellation and refunds.

The ordinary release CLI and native library are freshly built from that same
validated source. Documentation receipts may differ only in the four named
status documents; runtime, tests, dependencies and harness bytes remain exact.
The first hosted native build compiled successfully but its inventory preparation
rejected the new opt-in attribution test, which was missing from CI's explicit
ignored-test registry. The correction adds only that exact test name: the 33
existing entries and all scheduling estimates stay unchanged. The CI feature
selection therefore registers 34 ignored tests; the broader local workspace
selection above records 38. A separate receipt reproduces rejection by the old
registry, verifies complete partition coverage with the corrected registry, and
passes all 17 native-runner unit checks. Exact hosted executable discovery remains
required. The portable verifier admits only this byte-exact registry insertion
in addition to status prose and rejects added skips, removed entries and changed
scheduling estimates. The original hosted failures and earlier packet controls
remain preserved. No runtime source, test body, dependency or scored request changes.
The packet requires source-bound and relocated saved-result replay plus negative
controls for altered source, results, clocks, missing tests/cases, resource
cleanup, no-fallback claims and archive bytes. Original diagnostic, lint and
packaging failures are preserved with their corrected disposition; no failed
proof attempt prompted a scored query rerun.

## Hosted integration and retirement

PR #1678 merged as `a640475ae66bc36cefcae42088dc23da6776e8ed`, with the same
source tree as the reviewed head. All 58 PR checks and 55 exact-main checks
passed with review clear. The original hosted inventory failure and corrected
registry remain separate observations; the successful main workflow was not
restarted. Source-bound and portable evidence replay passed after retirement.

Verified retirement removed 40 task-owned roots containing 5,826 entries,
including 5,822 regular files and no symlinks, totaling 1,508,581,322 logical
bytes. Repeated identities and closed-handle checks passed; all 220 input
generations and 633 retained files stayed unchanged. The superseded dictionary
runtime, redundant raw ledgers and reconstructible packet/source copies were
retired with exact retained replacements. The accepted index runtime remains
retained for the independent window comparison. No payload backup, storage-limit
change, package publication or installed-index-wheel claim is made.

The [window evidence packet](../benchmarks/evidence/native-shared-window-state-2026-11-11/README.md)
preserves the index closeout receipt. Recommendation 9's
[separate outcome](native-shared-window-state-2026-11-11.md) includes versioned
multi-output requests because the original one-`OVER` window cases cannot prove
shared traversal. Recommendation 10 retains its separate allocated-storage
objective.
