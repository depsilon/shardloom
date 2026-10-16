<!-- SPDX-License-Identifier: Apache-2.0 -->

# Declared dynamic streaming subquery acceptance

Status: local acceptance and hosted integration complete in
[PR #1561](https://github.com/depsilon/shardloom/pull/1561), with all 44 checks
passing and preview/production verification complete. This source change does
not modify the immutable 0.6.0 packages.

Finite streaming inputs now compose with existing parameter-dependent IN/NOT IN,
row membership, EXISTS/NOT EXISTS and quantified ANY/ALL subqueries. Each inner
pivot may discover different columns for its outer parameter. The complete set
of possible input identities must be declared before schema discovery can demand
input. SQL and DataFrame lowering derive those declarations automatically; native
callers use `defer_subquery_with_sources`. The
[implementation contract](../architecture/native-stream-dynamic-subqueries-2026-10-15.md)
defines the declaration, ownership and resource boundaries.

## Accepted evidence

- [Evidence index](evidence/native-stream-dynamic-subqueries-2026-10-16.json).
- [Complete compact archive](evidence/native-stream-dynamic-subqueries-2026-10-16.tar.xz).
- [Per-member manifest](evidence/native-stream-dynamic-subqueries-2026-10-16.manifest.json.xz).
- [Independent audit and primary acceptance](evidence/native-stream-dynamic-subqueries-2026-10-16.audit.json.xz).
- [All 43 engine timings](evidence/native-stream-dynamic-subqueries-2026-10-16.engine-times.csv).
- [Final source, documentation and browser review](evidence/native-stream-dynamic-subqueries-final-review-2026-10-16.json.xz).

The archive contains **25,530 members**, representing 135,710,138 original bytes.
The packager and independent verification parser each reopen every member and
check its identity. Complete values, schemas, source transitions, original
failures, clocks and cleanup records remain available. Large datasets and
executable binaries are excluded with complete identities and build evidence
retained. Later documentation and hosted verification do not rewrite this archive.

| Gate | Result |
| --- | --- |
| Before-change controls | 16 streaming refusals before producer opening and 16 exact-value resident controls retained |
| Changed behavior | All 32 controls now pass exact values, including the 16 previously refused streaming cases |
| New native tests | 19 dynamic declaration/lifetime tests and two scalar-predicate parser tests pass |
| Workspace tests | 3,221 pass, zero ignored |
| Native-feature tests | 4,285 pass; the same 29 existing exclusions remain |
| Rust validation | Formatter, workspace Clippy, workspace tests, read-feature Clippy, release-feature Clippy and native tests pass |
| Direct native binding | 100 methods pass, zero skips; four new methods include a 288-query predicate matrix |
| Python declaration tests | 74 related methods pass, including two new predicate-value declaration tests |
| Public SQL/DataFrame acceptance | 4,827 cases across 36 families pass |
| Focused dynamic acceptance | 1,103 cases pass: 716 predicate/writer, 32 baseline controls, 153 composition, 195 fault and seven pressure cases |
| Evidence compression checks | Five tests pass, zero skips |
| Full43 | One execution of each query; all 43 complete results and 381 rows pass |

The 29 native exclusions remain the previous 26 manual and three platform cases;
there are no new dynamic-subquery exclusions. Direct binding tests use the source
Python package with the frozen extension. The installed-wheel and hosted proof
below records its separate build and integration identities.

All nineteen documentation/site checks pass. Python core discovery finds
626 methods across 57 modules; 526 execute with 12 skip records in the local
environment without the native extension, while all 100 direct native methods
pass separately with the extension installed. Browser inspection checks the
four changed guides at desktop and narrow-screen widths, search results,
mobile navigation and light/dark readability. Screenshots were inspected inline;
no screenshot archive or cross-browser matrix is claimed. A documentation-only
validator extension checks the declared dynamic fields without changing the
accepted runtime or rerunning native workloads.

Public inspection reopens 13,004 compressed JSON records, including 4,495
exact-value comparison files with 39,979 expected rows and 6,330 envelopes.
All 45 inherited pressure files, containing 1,578,741 rows, remain verified.
The complete ordered case list matches the predeclared matrix. Exact expectations
retain 18 dynamic predicate kinds across mixed, empty-outer, empty-inner and
all-null shapes, all 108 inherited static-subquery kind/shape combinations and
39 inherited unary value entries. Original baseline values are compared directly
with the new streaming and resident controls.

## Hosted integration

PR #1561 merged at `4217c2cd7d322e054d483e3e389f6f22df8d0970` after all
44 checks passed on `18b61c23d1f68bedc8bf70e06a54fb9d7f00f1c6`.
Final-head review reports no findings. Reviewed, synthetic-test and merged trees
are identical. All 2,303 frozen source identities match the merged source.

The [147-member hosted index](evidence/native-stream-dynamic-subqueries-hosted-2026-10-16.json)
and [compact archive](evidence/native-stream-dynamic-subqueries-hosted-2026-10-16.tar.xz)
retain complete logs and cleanup tails for the six scoped jobs, source checks,
merge receipts, deployment responses and browser observations. The archive is
558,640 bytes with SHA-256
`96b8d94e0afc7087efaa37f4e2af9698e7409a256866b1b9de435be7f98e81d3`.
The [independent archive audit](evidence/native-stream-dynamic-subqueries-hosted-2026-10-16.audit.json)
checks every member and the recorded job/deployment totals. This is evidence
integrity and consistency verification, not a new runtime execution.

| Hosted gate | Result |
| --- | --- |
| Native/provider tests | 2,914 pass; 26 existing manual exclusions |
| Native public-surface tests | 1,079 pass, zero ignored |
| Installed native wheels | Linux Python 3.10/3.13 and macOS arm64 Python 3.13 each pass 100 methods, zero skips |
| Wheel lifecycle | Isolated install, payload identity, native load and uninstall pass on all three lanes |
| Python core | 626 discovered across 57 modules; 526 run, 14 skip records |
| Focused hosted coverage | All 33 required native regressions, 19 new native declarations and four CLI regressions appear in complete passing logs |
| Preview and production | All sixteen checked responses preserve reviewed source; search, desktop layout and mobile navigation pass |

Production deployment check `116904627523` completes successfully for the merge
commit, with version `fa42ee3b-6011-40b6-b843-0152334292d4`. The HTML responses
preserve source around Cloudflare's injected script; the JavaScript and search
entry responses are byte-identical. Browser coverage is limited to the Codex
in-app browser at desktop and phone widths. The observed pages still contain
the reviewed pre-closeout wording; this documentation update has its own later
deployment. No query rerun supplies merge or deployment evidence.

The packet also retains early partial captures and three evidence-helper
corrections. Source-checker lookup/attribute corrections and a malformed helper
string replacement did not change engine code. Their original helpers and
failure receipts remain separate from the successful complete verification.
The [documentation closeout](evidence/native-stream-dynamic-subqueries-closeout-2026-10-16.json.xz)
retains ten passing generated-site/documentation checks, 584 verified local
links and desktop/phone inspection of the completion paragraph. All 2,303
runtime source identities remain unchanged; this later wording is verified
locally and awaits its own hosted deployment.

## Behavior and failure boundaries

A declared deferred relation records its possible native sources without running
the callback during source-role classification. Metadata reservations account for
the bounded declaration and live through retained reference clones. References
remain execution-scoped, single-use and valid only in their admitted correlated
position. Unknown, foreign, reused, misplaced and unconsumed references fail
deterministically.

The binder installs source restrictions before invoking the per-parameter
lowerer. It checks complete native subtrees before schema discovery can request
input, and nested source declarations must be subsets of the parent declaration.
An undeclared source hidden beneath a pivot or another deferred plan is refused
before that producer opens. Opaque nested callbacks cannot escape the restriction.

Independent inner producers record replay on demand. A source shared by outer
and possible inner relations completes native replay before query reads when
recursive producer opening would otherwise be possible. Nested reads detach
the current outer batch under credit and release its original owner before
requesting another producer. Per-parameter schema and inner state have separate
lifetimes within the same execution and grant.

Empty outer inputs and false/null selected guards do not invoke unused inner
lowerers or evaluate skipped expressions. Every unused producer still undergoes
raw session, schema, row/byte and completion validation. A late source failure,
absent end marker, cancellation or consumer failure prevents final success and
publication. All eight admitted local writers and fanout retain their existing
completion and partial-publication contracts. Recovery permits a fresh execution;
it does not resume a failed execution.

Declaration metadata, outer/inner copies, replay, parameter values, pivot state,
membership state and outputs share the query grant. Tests distinguish explicit
native source replay spill from pivot-state spill and from resident membership
denial. Replay spill does not spill membership/index state. Credited bytes are not
a whole-process RSS measurement or ceiling.

Execution remains native and certificate-backed with `fallback_attempted=false`
and `external_engine_invoked=false`. The pinned Vortex 0.85 provider, existing
native binder, correlated executor and credited replay store supply the required
representation and execution. No dependency, query-engine fallback or new
subquery algorithm is added. Recorded copies remain explicit; no zero-copy or
zero-decode claim is made.

## Preserved failures and corrections

The first direct candidate exposed a real parser gap: relational predicate values
inside CASE were treated as generic function calls. The correction uses the
existing explicit-predicate recognizer and binding, preserving demand guards and
the separate scalar-value output-schema restriction. Both native artifacts were
rebuilt before accepted direct and public runs. The packet retains the original
four-test attempt with one failure and 21 errors.

That attempt also retained 285 live Arrow results before the explicit 32-MiB
grant refused another result. Releasing those owners refunded credits and allowed
a fresh query. The corrected matrix releases retention groups without raising
the grant. Row-membership fixtures use distinct source columns under the existing
parser contract.

Successive focused public attempts retain 727, 824 and 842 passing cases before
their respective failures. Corrections keep SQL and DataFrame construction
separate, use a valid non-null pivot measure while retaining the null key, and
preserve the existing declaration-credit policy. Twelve predeclared diagnostic
calls demonstrate six nested source/predicate refusals at 32 MiB and six exact
successes at 64 MiB. Those 32-MiB refusals remain explicit acceptance cases;
other composition cases retain their original grant.

A complete diagnostic of the remaining 229 cases records 135 successes and
94 failures: eight pivot-fixture cases and 86 Python declarations that rejected
predicate values before native execution. Python scalar branch rendering now
retains typed predicates in CASE, COALESCE and NULLIF, including source bindings
and resource ceilings. String arguments remain literal strings. The two new
declaration tests and complete public runs verify that correction separately
from the unchanged frozen native executables.

Default-feature Clippy subsequently found an unread source-identity field when
the Unix native provider was disabled. Its conditional field declaration now
matches the existing constructor and consumers. Another lint correction changes
one empty-vector assertion inside a test module. Exact before/after source
verification proves that neither change alters production code enabled in the
frozen Unix native artifacts. Fresh complete workspace and native checks pass;
the public/direct runs retain their original source identity and an explicit
link to the final verification source.

Earlier failed source-snapshot admission, fixture/build observations and all
failed checks remain retained. No incomplete attempt supplies the accepted
public or Full43 totals.

## Frozen execution and Full43

The source basis is `c30846afb0ab5abfff6fdb5d95ed20f1c10a9477` plus the archived
overlay. Build, public/direct execution and final verification manifests each
identify 2,303 source files. The packet verifies the exact four Python fixture/SDK
changes after the native build, then the conditional field and test-only
corrections before final source validation. Native code enabled by the frozen
artifacts remains equivalent across these documented transitions.

The release CLI SHA-256 is
`67076fdf957a3b3950ab8c20dbdd0fb77b2b9336b808e06545fe2aa7a5a1dc8c`;
the native extension SHA-256 is
`fba4eaafdd7693a9e808afde63569a978eb48a48837a9827ccb52ddc13fb065b`.

Full43 uses the retained 100-million-row Vortex input, 24 GiB declared query
memory, maximum parallelism 12 and one sequential execution per query. No new
ingest or alternate engine participates. The 15,713,610,513-byte input retains
SHA-256 `06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`,
checked before and after the run. The retained native reference, all statements,
schemas, executable identities, raw envelopes and cleanup receipts are bound to
the packet. The input was prehashed; OS cache state is uncontrolled.

Total engine time is **61.146182539 seconds**. Each measurement below uses
`engine_execution_seconds` for one execution. This complete regression observation
does not establish acceleration or attribute a timing change. Other clocks and
resource observations remain in the diagnostic archive.

| Query | Engine seconds |
| --- | ---: |
| Q01 | 0.000068376 |
| Q02 | 0.197089625 |
| Q03 | 0.875142708 |
| Q04 | 0.381607875 |
| Q05 | 1.388285209 |
| Q06 | 1.339984376 |
| Q07 | 0.000466249 |
| Q08 | 0.147323625 |
| Q09 | 1.523312791 |
| Q10 | 2.536595250 |
| Q11 | 1.107828582 |
| Q12 | 1.743033582 |
| Q13 | 0.713176500 |
| Q14 | 1.750045083 |
| Q15 | 1.587280417 |
| Q16 | 2.423441709 |
| Q17 | 2.519380542 |
| Q18 | 0.354725042 |
| Q19 | 5.205028709 |
| Q20 | 0.284811124 |
| Q21 | 1.107366541 |
| Q22 | 1.354146209 |
| Q23 | 3.472471542 |
| Q24 | 1.389938293 |
| Q25 | 0.356961750 |
| Q26 | 2.105809459 |
| Q27 | 2.189606168 |
| Q28 | 2.270315042 |
| Q29 | 5.815670000 |
| Q30 | 0.269286249 |
| Q31 | 1.370766792 |
| Q32 | 1.599561583 |
| Q33 | 2.087429624 |
| Q34 | 2.979352208 |
| Q35 | 3.175734791 |
| Q36 | 0.426439084 |
| Q37 | 0.439710875 |
| Q38 | 0.483214207 |
| Q39 | 0.232331665 |
| Q40 | 0.902118917 |
| Q41 | 0.613699958 |
| Q42 | 0.218336583 |
| Q43 | 0.207287625 |

## Remaining scope

Opaque undeclared inner callbacks remain inadmissible with streaming inputs.
Dynamic scalar-value subqueries still require a separate output-schema contract.
Membership/index state remains resident, and replay spill does not change that
boundary. Broader allocation, state and recovery coverage, the six-area/eight-
investigation completion contract and all CG-1 through CG-23 retain their owners.
This finite unit does not establish transport completion, broad SQL parity,
production readiness, a general speedup or a package release.
