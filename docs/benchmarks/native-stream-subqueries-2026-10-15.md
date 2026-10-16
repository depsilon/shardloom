<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite streaming subquery acceptance

Status: integrated through [PR #1560](https://github.com/depsilon/shardloom/pull/1560)
at `c30846afb0ab5abfff6fdb5d95ed20f1c10a9477`. Local acceptance, independent
inspection, all 44 hosted checks and preview/production verification pass.
This source change does not modify the immutable 0.6.0 packages.

Finite streaming producers now compose with existing IN/NOT IN, row membership,
EXISTS/NOT EXISTS, quantified ANY/ALL and scalar subqueries. Inner sources and
schemas must be statically declared; indexed correlation and parameterized
`Outer` plans are admitted within that boundary. Selected branches preserve
existing evaluation rules while every producer still requires valid completion.
The [implementation contract](../architecture/native-stream-subqueries-2026-10-15.md)
defines ownership, resources and the remaining opaque dynamic-inner boundary.

## Accepted evidence

- [Evidence index](evidence/native-stream-subqueries-2026-10-15.json).
- [Complete compact archive](evidence/native-stream-subqueries-2026-10-15.tar.xz).
- [Per-member manifest](evidence/native-stream-subqueries-2026-10-15.manifest.json.xz).
- [All 43 engine timings](evidence/native-stream-subqueries-2026-10-15.engine-times.csv).
- [Final source, documentation and browser review](evidence/native-stream-subqueries-final-review-2026-10-15.json.xz).
- [Hosted and production verification](evidence/native-stream-subqueries-hosted-2026-10-16.json).

The archive contains **14,066 members**, representing 86,933,111 original bytes.
The packager and a separate verification parser each reopen every member and
check its identity. The audit also checks complete public values and schemas,
the fixed expectations declared before candidate execution, all Full43 results
and clock records, the original failures and the exact source transitions.
Large datasets and executable binaries are excluded with their identities and
build evidence retained. The archive is not rewritten by later documentation
or hosted closeout.

| Gate | Result |
| --- | --- |
| Before-change controls | 15 streaming refusals before producer opening; 15 resident controls pass |
| New native tests | 14 streaming subquery tests and one projected-EXISTS parser regression pass |
| Workspace tests | 3,220 pass, zero ignored |
| Native-feature tests | 4,264 pass; the same 29 existing exclusions remain |
| Rust validation | Formatter, workspace Clippy, workspace tests, read-feature Clippy, release-feature Clippy and native tests pass |
| Direct native binding | 96 methods pass, zero skips; five new methods cover 127 subcases |
| Public SQL/DataFrame acceptance | 3,724 cases across 31 families pass |
| Focused subquery acceptance | 1,123 cases pass: 722 values/writers, 230 composition, 164 faults and seven pressure cases |
| Evidence compression checks | Five tests pass, zero skips |
| Full43 | One execution of each query; all 43 complete results and 381 rows pass |

The native exclusions are the unchanged 26 manual cases and three platform
cases; they are not new subquery exclusions. Direct binding tests use the
source Python package with the frozen extension. Installed-wheel and hosted
proof are separate gates.

Final review verifies the unchanged runtime source and the documentation-validator
transition. Python core discovery finds 620 methods across 56 modules; 524 tests
execute with 11 skip records, while all 96 native methods pass separately with
the extension installed. All 19 documentation/site checks pass. The first docs
attempt exposed a missing release-scope row; its failure remains preserved, and
the continuation reuses nine unchanged passing checks before completing the
remaining ten. Browser inspection verifies all four updated guides at desktop
and narrow-screen widths, search results and navigation. Screenshots were
inspected inline; no screenshot archive or cross-browser matrix is claimed.

The public inspection reopens 9,892 compressed JSON records, including 3,500
exact-value comparison files with 36,841 expected rows and 5,156 envelopes.
It also verifies all 45 inherited pressure files containing 1,578,741 rows.
Every case identity matches the complete predeclared matrix. The four new
families preserve 27 subquery kinds across mixed, empty-outer, empty-inner and
all-null inputs, with separate SQL and DataFrame declarations. Existing unary
reference values remain unchanged.

## Hosted integration and production

All 44 checks pass on `b6b585a45aadd144f6b90cc2053744544518897a` before
PR #1560 merges. The reviewed, synthetic-test and merged trees match, and all
2,291 accepted source identities are verified against each revision. Final-head
automated review reports no findings.

The separate [150-member archive](evidence/native-stream-subqueries-hosted-2026-10-16.tar.xz)
retains raw checks, six job logs, review state, source verification and deployment
observations. Native/provider tests pass 2,895 cases with 26 declared manual
exclusions; native public tests pass 1,077. Each of three installed-wheel lanes
passes all 96 methods without skips. Hosted core discovers 620 methods across
56 modules, executes 524 and records 13 skips. These hosted counts remain
separate from the local feature and platform selections above.

Preview and production verification checks all sixteen served responses.
Production version `9c198deb-b695-4f9a-8cf0-ec614ead833f` completes on the merged
revision. Six production HTML responses preserve the complete generated source
around bounded Cloudflare script insertions; the JavaScript and search entry
are byte-identical. Actual browser checks verify all four guides, search and
navigation. They prove the deployed pre-closeout wording, including its earlier
pending label; this later status correction needs its own deployment. Screenshots
were inspected inline, and no mobile or cross-browser matrix is claimed.

Primary and independent inspection reopen every hosted archive member and verify
its bytes. The original local archive and Full43 observation remain unchanged;
no additional queries were run solely to verify merge or deployment.

## Behavior and failure boundaries

The binder collects source roles before demand. Ordinary independent eager
inners consume their producer once without unnecessary source replay. Nested
scalar, guarded and parameterized reads release the original outer owner before
requesting inner input. Shared outer/inner sources complete raw native replay
before their first query read, allowing nested reads of completed storage.
Parameterized inners reuse that execution's declared replay.

Skipped branches and empty outer inputs validate unused producers through the
normal session, schema, row/byte and end-marker checks. That validation does not
evaluate skipped arithmetic, scalar cardinality or unused subquery state. Late
failure, foreign sessions, invalid schemas, cancellation and missing completion
prevent final success and publication. Retained output remains valid after the
input and query owners are released.

Native and public tests cover shared resident/spilled replay, multiple nesting
levels, execution-time dynamic outer lowering, all eight admitted local writers,
fanout, consumer failure, copy denial, late failure, cleanup and a fresh execution.
The pressure controls distinguish three behaviors: a bounded inner aggregate can
consume cumulative input beyond its grant without source replay; shared input
can use explicitly permitted native replay spill; subquery membership/index
state remains resident and refuses excess growth even when replay spill is
enabled. The query grant is not a whole-process RSS ceiling.

Execution remains native and certificate-backed, with
`fallback_attempted=false` and `external_engine_invoked=false`. No dependency,
query-engine fallback or new subquery algorithm is added. The existing Vortex
0.85 provider, compact native payloads and credited replay store supply the
representation and ownership boundaries. Actual copies are recorded; this is
not a zero-copy or zero-decode claim.

## Preserved failures and corrections

The first focused public attempt passed 104 cases and then exposed a real
frontend gap: a projected EXISTS expression was sent to generic function
parsing. The correction selects the existing predicate lowering for EXISTS,
with positive bare/parenthesized/negated/correlated coverage and negative
identifier, string-literal and scalar-inner controls. Complete candidate
executables were rebuilt before the accepted direct and public runs.

The initial five direct methods recorded five failures and 11 errors. Their
records retain the projected-EXISTS defect and fixture corrections for required
SQL aliases, existing fanout method names and the existing rejection of non-finite
Arrow intake. The corrected suite tests both valid skipped scalar evaluation and
invalid intake refusal; it does not weaken source completion or selected-branch
semantics. Earlier compilation, lint, pivot-type fixture and declaration-count
failures remain in the packet.

A later workspace lint check rejected the syntax of one test assertion. Its
exact replacement is confined to the parser's `#[cfg(test)]` module, with
byte-identical production code. All six Rust gates subsequently pass. The
accepted source map preserves this test-only transition separately from the
original build source; direct/public and Full43 evidence use the unchanged
frozen release executables.

The original baseline design used an incorrect feature label. The corrected
contract names `vortex-local-primitives` and `vortex-write`. Both byte-exact
versions are retained, with the original verified against its baseline hash.
The first packaging preflight stopped on that mismatch before creating an
archive. The corrected packaging records the explicit label-only transition;
baseline expectations, engine behavior and results are unchanged.

## Frozen execution and Full43

The source basis is `ff9aa27338ca9f1ff5581f5a57603400f5aec926` plus the archived
overlay. The build and accepted test source manifests each identify 2,291 files.
The release CLI SHA-256 is
`d0c2201b81f399568f08e7201ea77b96c08edac0c0369f67160645e165366700`;
the native extension SHA-256 is
`f96fb76b77aea73c24ce27a88b3fed3285e5f3f1921a59949d1de7c7944591dd`.
The later surface-reference validator update changes documentation checks only;
it does not rebuild or alter these artifacts.

Full43 uses the retained 100-million-row Vortex input, 24 GiB declared query
memory, maximum parallelism 12 and one sequential execution per query. No new
ingest or alternate engine participates. The 15,713,610,513-byte input retains
SHA-256 `06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`.
Its identity is checked before and after the run. The retained native reference,
all query statements, schemas, executable identities, raw envelopes and cleanup
receipts are bound to the packet. The input was prehashed; OS cache state is
uncontrolled.

Total engine time is **61.819463793 seconds**. Every measurement below uses
`engine_execution_seconds` for one execution. This is a complete regression
observation; a single pass does not establish acceleration or attribute a timing
change. Other clocks and resource observations remain in the diagnostic archive.

| Query | Engine seconds |
| --- | ---: |
| Q01 | 0.000085042 |
| Q02 | 0.196295667 |
| Q03 | 0.862347667 |
| Q04 | 0.390917292 |
| Q05 | 1.360075625 |
| Q06 | 1.330757999 |
| Q07 | 0.000576416 |
| Q08 | 0.145579375 |
| Q09 | 1.468450001 |
| Q10 | 2.452220499 |
| Q11 | 1.114393041 |
| Q12 | 1.750937751 |
| Q13 | 0.704453500 |
| Q14 | 1.634266709 |
| Q15 | 1.616662082 |
| Q16 | 2.417541918 |
| Q17 | 2.524358457 |
| Q18 | 0.345989209 |
| Q19 | 5.122964250 |
| Q20 | 0.270313333 |
| Q21 | 1.113064041 |
| Q22 | 1.312832875 |
| Q23 | 3.617658084 |
| Q24 | 1.233724041 |
| Q25 | 0.347590000 |
| Q26 | 2.140393208 |
| Q27 | 2.121569874 |
| Q28 | 2.316400126 |
| Q29 | 5.895547417 |
| Q30 | 0.265588001 |
| Q31 | 1.309461667 |
| Q32 | 1.698347250 |
| Q33 | 2.131437333 |
| Q34 | 3.668753583 |
| Q35 | 2.941734584 |
| Q36 | 0.417887250 |
| Q37 | 0.560446542 |
| Q38 | 0.642109958 |
| Q39 | 0.252103417 |
| Q40 | 1.023237458 |
| Q41 | 0.660308460 |
| Q42 | 0.231547583 |
| Q43 | 0.208535208 |

## Remaining scope

Opaque `DeferredSubquery` closures cannot yet declare all parameter-dependent
inner sources and schemas before first demand. They remain explicitly refused
for streaming input until the following source-declaration/lifetime contract is
implemented. This unit does not add subquery state spill, lazy OR semantics,
lateral relations, broad SQL parity, remote transport or a package release.
The six-area/eight-investigation completion contract and all competitive gates
remain under their existing owners. Native transport follows that broader work.
