<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite streaming set-operation acceptance

Finite streaming producers now compose through the four existing native set
operations: `UNION ALL`, `UNION`, `INTERSECT` and `EXCEPT`. The admission walk
inspects both branches and records repeated source use. The existing binder,
typed row comparison, credited membership, native replay and completed-result
consumers perform the work. No new set algorithm or execution provider is added.

Status: complete local and hosted acceptance passes; PR #1558 is merged.
The [portable index](evidence/native-stream-sets-2026-10-15.json) records **5,460
verified members**. This source change follows the immutable 0.6.0 packages;
no new package is published. The [implementation contract](../architecture/native-stream-set-operations-2026-10-15.md)
keeps high-cardinality membership spill, other stateful families and native
transport outside this finite unit.

## Shared behavior and scope

The frozen prior runtime passes twelve controls: eight explicit streaming
refusals before producer demand and four complete resident results. The same
native set executor was already available for resident/file-backed input.
Traversing both branches during streaming admission now supplies the complete
source-use information required by that executor and its replay owner.

`UNION ALL` preserves multiplicity. The other three operations use full-row
distinct membership, including null-equal comparison and admitted typed/nested
values. Positional schema alignment, common-type admission and left-side field
names retain their existing semantics. Tests check the existing first-occurrence
order; general SQL ordering still requires `ORDER BY`. `INTERSECT ALL` and
`EXCEPT ALL` remain unsupported.

Independent producers retain separate demand, ordinal and completion state.
Repeated references consume the original producer once and use declared native
replay. Limits, including zero, drain complete input; a late producer or schema
failure cannot become a successful partial result. Unsupported branches and
excessive recursive depth still reject before demand. Dynamic stages retain
their separate discovery-time admission and failure boundaries.

Collection, incremental output, all eight existing local writers and mixed
fanout use their established consumers. Writer format/type/fidelity restrictions
remain. Native Vortex output is the highest-fidelity path; compatibility output
is an explicit translation. Complete producer observation, adapter completion
and owned cleanup precede publication. Multi-file publication remains non-atomic.

## Correctness, ownership and pressure

| Check | Result |
| --- | --- |
| Workspace formatting | Passed |
| Workspace, native-read and release-user-surfaces Clippy | Passed with warnings denied |
| Default workspace tests | 3,219 passed; none ignored |
| Native CLI/Vortex tests | 4,241 passed; 29 declared manual/platform exclusions |
| New native set tests | 9 passed |
| Direct native Python binding tests | 87 passed; no failures, errors or skips |
| Corrected focused public run | 588 passed across four families |
| Complete public workflows | 1,121 passed across 22 families |
| New set families | 324 semantic/composition cases and 19 pressure cases passed |
| Full43 regression | 43/43 complete queries; 381 result rows; one call per query |

Complete semantic cases cover independent, repeated, mixed resident/file and
empty producers; duplicates, nulls, exact scalar/recursive types and positional
schema alignment; and composition with filters, projections, ordering, limits,
aggregation, joins, windows, nested sets and dynamic pivots. Python object checks
verify exact recursive types, values and field order. Native output owners remain
valid after producer and prepared-plan teardown, keep their credits while
retained, and refund them when their last owner is released.

The pressure fixtures use 8,193 rows per producer, 2,048-byte text values,
251-row producer batches and a 16-MiB native grant. All four operations complete
low-cardinality input whose cumulative logical bytes exceed that grant. Complete
output values, source release before further demand, peak credited allocation
and final refunds are checked. Distinct membership remains resident: growth
beyond its available grant fails deterministically with and without replay-spill
permission. That permission does not turn membership into a spillable operator.

Repeated-source `UNION ALL` separately proves actual native replay spill. A
16-MiB resident control refuses; a 128-MiB ample control completes without spill.
With the original 16-MiB grant and an explicit 128-MiB disk quota, two executions
each complete every output row using multiple native runs and merge passes,
preserving once-per-execution producer consumption and cleanup. Producer errors,
corrupt runs, exhausted quota, cancellation, consumer closure and late schema
failure retain their failure/cleanup contracts. Existing and foreign files are
preserved. These checks bound credited owners, not whole-process RSS.

Independent public-evidence inspection verifies 1,199 complete-value files with
23,567 expected rows, 1,830 native envelopes and 26 pressure files with 726,729
rows. The exact ordered 1,121-case list matches the frozen protocol. Complete
values, source identities and false fallback/external-execution fields are
checked. Local direct tests use the source package with an explicit frozen native
library; they do not establish installed-wheel or all-platform acceptance.

## Preserved failures and source identity

The packet retains the initial Clippy finding and its test assertion correction.
The first full native gate also found an older dynamic-admission test that still
expected every set to be refused. The corrected fixture places an unsupported
correlated source on either set branch, preserving early-refusal coverage. The
complete rerun passes, including all nine new tests and that corrected fixture.

The first focused public run passes 236 cases, then fails its rich-value Python
object assertion. The engine's complete values and declared schema already match;
the expected dictionaries insert their final fields in a different order. A
separate reproduction on the unchanged executable confirms the original failure
and then passes every recursive type, value and nested-order assertion when
expected top-level fields follow the declared schema. The fixture now uses that
order, as the existing typed-input suite already does. The strict assertion is
preserved. The diagnostic helper's first outer tuple/list comparison mistake is
also retained and corrected separately.

Both 2,272-file source inventories are retained. Only
`scripts/native_streaming_set_composition_cases.py` changes between the original
build/native/direct acceptance and corrected public acceptance. The immutable
release binaries and 87 direct tests are reused; the manifest explicitly links
their original build source to the corrected test fixture. All Rust and public
Python runtime sources remain unchanged across that correction. The later
documentation-validator update is recorded separately from runtime acceptance.

The first hosted Python core shard then finds a test-module import error:
package loading cannot resolve the new test's absolute sibling import. The test
now selects the existing repository pattern for package and directory loading.
The corrected core check passes with 606 discovered tests; its console records
519 tests run and nine skip records. Separate runs against the same frozen
native binding pass all 87 tests in each loading mode, with no skips. The
[import-correction supplement](evidence/native-stream-sets-test-import-correction-2026-10-15.json.xz)
retains the original hosted log, both test sources, complete native logs and the
2,272-file source transition. Runtime code and native artifact hashes remain
unchanged; the Rust and public workflow gates above are retained without reruns.
The corrected hosted core shard subsequently passes with 606 discovered tests,
519 tests run and 11 skip records. Those hosted skips remain distinct from the
nine local skip records. Each installed native-wheel lane runs all 87 tests with
zero skips.

## Hosted integration

[PR #1558](https://github.com/depsilon/shardloom/pull/1558) merges at
`e859b3a8b9d323587cee9641f5ebacd21e7e799c` after all **44 checks** pass on
`42440f5fb6585accb0553add4dad065820171cab`. The reviewed head, synthetic CI merge
`f3c5c327db36c57de8ddf61f6675cdae53ae372c` and merged revision share tree
`e3b72f03d2e2363180d600876cc9a1a6cafcbf77`. Automated code review completes on
the final head without recorded findings; no review threads or requested
changes remain.

The [hosted index](evidence/native-stream-sets-hosted-2026-10-15.json) and
[archive](evidence/native-stream-sets-hosted-2026-10-15.tar.xz) preserve and verify
**107 members**, including original job logs, review/check responses, served
bytes and complete verifier dependencies. All **2,272** accepted source hashes
match immutable merged Git blobs. The frozen CLI and binding hashes remain
unchanged. The three changes after the original build are the public field-order
fixture, documentation validator and package/discovery test import; the original
acceptance and correction evidence remain separately identified.

The native Vortex job passes 2,822 tests with 26 declared manual exclusions;
five provider groups pass 10, 7, 7, 23 and 4 tests, for 2,873 total passes.
The raw log records all nine new set-test outcomes. Native public runtime tests
pass 1,076 cases. Three isolated installed-wheel lanes—Linux Python 3.10/3.13 and
macOS arm64 Python 3.13—each pass 87 integration tests, installation, independent
load/hash checks and uninstall verification. The package-integrity inspection's
`load_verified=false` describes that inspection's scope; a separate isolated
load step succeeds. No package is published.

All eight preview and eight production responses preserve their respective
immutable source. Six production HTML responses include a bounded Cloudflare
script insertion; every original byte around each insertion is unchanged.
Production check `116698574880` completes successfully at
`2026-10-15T22:11:22Z`, identifying version
`785c4ca6-95bc-4b1b-94f7-8408af584d2e`. Actual browser inspection verifies four
guides, four search results for `set membership` and navigation to the benchmark
evidence section in both environments. Screenshots were inspected inline and
are not archived; this is not a cross-browser or responsive test matrix.

The initial preview timeout and partial responses, original failed core job,
intermediate check states and corrected source-name parser remain in the packet.
The deployed pages at this observation still say hosted acceptance is pending;
this closure update supersedes that earlier status without rewriting its bytes.

## Single-pass query observation

One sequential pass executes all 43 queries against the resident
99,997,497-row, 15,713,610,513-byte native Vortex input. Complete values and schemas
match the retained native regression reference, with its declared `1e-12`
finite-float tolerance. This is not a fresh independent correctness oracle.
No new ingest is needed for the set-admission change. Input hashes/generations,
binary and source identities are verified before and after the run; native
children and the workload lock are released.

The host is macOS 27.0 on arm64 with ten logical CPUs and 16 GiB physical RAM.
The request uses parallelism 12 and a 24-GiB query allocation; those policy
settings differ from physical capacity and measured RSS. Input prehashing affects
the OS cache, whose state and ordinary desktop activity remain uncontrolled.
There is no artificial pacing, battery/thermal admission, automatic retry,
replacement sample or matched speedup claim.

Engine clocks sum to **93.121722 seconds**, result serialization to **0.008430
seconds**, other process work to **3.826548 seconds**, and native process wall
time to **96.956701 seconds**. Summed supervised calls take **99.341469 seconds**,
including **2.384768 seconds** of supervisor overhead. Process user/system CPU
totals 241.352806/57.483998 seconds; complete-value validation totals 0.047912
seconds. The guarded query harness takes 130.312596 seconds including surrounding
identity/evidence work. These are different clocks, not ingest-to-result elapsed.
This single observation does not replace previous paired optimization decisions
or attribute a timing difference to the admission change.

Engine time includes query I/O, decoding, operators, validation and native
certification. Other process work includes startup, parsing/preparation, control,
report rendering and exit. CPU overlaps wall time and includes parallel workers.
The [complete timing CSV](evidence/native-stream-sets-2026-10-15.query-timings.csv)
also retains supervision and validation. Every query is shown below.

| Query | Engine (s) | Serialize (s) | Other process (s) | Process (s) | CPU (s) | Peak RSS (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 0.000089 | 0.000126 | 0.072281 | 0.072496 | 0.060518 | 147.250 |
| Q2 | 0.236479 | 0.000014 | 0.066801 | 0.303294 | 0.823564 | 562.672 |
| Q3 | 1.434786 | 0.000124 | 0.072911 | 1.507822 | 1.921482 | 450.406 |
| Q4 | 0.611993 | 0.000034 | 0.072473 | 0.684501 | 0.946987 | 376.406 |
| Q5 | 1.983392 | 0.000032 | 0.069123 | 2.052547 | 2.295236 | 1928.969 |
| Q6 | 2.274762 | 0.000034 | 0.073216 | 2.348012 | 6.459387 | 1293.859 |
| Q7 | 0.000612 | 0.000035 | 0.063009 | 0.063656 | 0.061228 | 150.672 |
| Q8 | 0.201094 | 0.000031 | 0.072847 | 0.273972 | 0.713324 | 573.344 |
| Q9 | 3.033488 | 0.000072 | 0.075915 | 3.109475 | 11.354951 | 3137.203 |
| Q10 | 4.101557 | 0.000200 | 0.136368 | 4.238125 | 7.591216 | 2390.719 |
| Q11 | 1.660656 | 0.000209 | 0.077710 | 1.738574 | 3.796869 | 495.500 |
| Q12 | 2.665435 | 0.000040 | 0.075462 | 2.740937 | 4.511928 | 561.469 |
| Q13 | 0.918801 | 0.000065 | 0.070220 | 0.989086 | 6.018340 | 1515.719 |
| Q14 | 2.734400 | 0.000067 | 0.175540 | 2.910007 | 16.110984 | 2264.500 |
| Q15 | 2.184244 | 0.000062 | 0.085622 | 2.269928 | 14.027803 | 1954.844 |
| Q16 | 3.447915 | 0.000041 | 0.073341 | 3.521296 | 5.665847 | 2378.312 |
| Q17 | 4.028217 | 0.000063 | 0.134201 | 4.162481 | 26.295127 | 2751.875 |
| Q18 | 0.495973 | 0.000043 | 0.071307 | 0.567323 | 1.020727 | 490.953 |
| Q19 | 8.689913 | 0.001242 | 0.084888 | 8.776043 | 15.646160 | 3108.703 |
| Q20 | 0.346585 | 0.000315 | 0.098519 | 0.445420 | 0.736400 | 1926.469 |
| Q21 | 1.986747 | 0.000019 | 0.086661 | 2.073427 | 12.190888 | 2112.453 |
| Q22 | 2.336155 | 0.000071 | 0.088809 | 2.425035 | 12.683981 | 1824.438 |
| Q23 | 5.771536 | 0.003832 | 0.109105 | 5.884473 | 19.128965 | 2235.641 |
| Q24 | 1.918461 | 0.000295 | 0.102874 | 2.021629 | 11.528026 | 2379.328 |
| Q25 | 0.496679 | 0.000077 | 0.087174 | 0.583929 | 1.614187 | 1960.781 |
| Q26 | 2.727492 | 0.000039 | 0.070684 | 2.798215 | 3.570042 | 615.562 |
| Q27 | 3.176069 | 0.000049 | 0.086472 | 3.262590 | 4.393475 | 2358.672 |
| Q28 | 3.317297 | 0.000247 | 0.090377 | 3.407922 | 4.253170 | 2649.844 |
| Q29 | 8.675213 | 0.000169 | 0.076932 | 8.752314 | 17.461597 | 1475.781 |
| Q30 | 0.372400 | 0.000079 | 0.069719 | 0.442198 | 0.690801 | 348.141 |
| Q31 | 2.104748 | 0.000063 | 0.144715 | 2.249526 | 5.376304 | 2134.359 |
| Q32 | 2.427154 | 0.000043 | 0.167596 | 2.594793 | 6.032125 | 3058.438 |
| Q33 | 3.481651 | 0.000072 | 0.073315 | 3.555039 | 10.705825 | 2856.000 |
| Q34 | 4.566262 | 0.000059 | 0.073398 | 4.639719 | 26.142329 | 4417.031 |
| Q35 | 4.363814 | 0.000060 | 0.079813 | 4.443688 | 24.923921 | 5114.266 |
| Q36 | 0.527860 | 0.000051 | 0.070440 | 0.598352 | 3.376101 | 868.016 |
| Q37 | 0.485939 | 0.000060 | 0.079577 | 0.565576 | 1.102445 | 1925.562 |
| Q38 | 0.555850 | 0.000061 | 0.096254 | 0.652166 | 1.281843 | 2958.344 |
| Q39 | 0.321609 | 0.000058 | 0.091192 | 0.412859 | 0.978159 | 1881.266 |
| Q40 | 1.168456 | 0.000064 | 0.097038 | 1.265558 | 1.981549 | 2899.906 |
| Q41 | 0.737322 | 0.000031 | 0.102888 | 0.840242 | 1.784510 | 2785.328 |
| Q42 | 0.282897 | 0.000031 | 0.076450 | 0.359378 | 0.747160 | 635.859 |
| Q43 | 0.269716 | 0.000051 | 0.083309 | 0.353075 | 0.831323 | 1433.094 |

## Portable provenance and reproduction

The [archive](evidence/native-stream-sets-2026-10-15.tar.xz) and
[compressed manifest](evidence/native-stream-sets-2026-10-15.manifest.json.xz)
retain all 5,460 verified members, 14 changed runtime/test/harness source files,
both source inventories, release build receipts, native/direct/focused/public
checks, failure history, complete query outputs, timing/PID records and retained
schema/reference observations. Every archive member is reopened and hash-checked.
Bulk executables, native input and output payloads have explicit hashed exclusions.

The [final review supplement](evidence/native-stream-sets-final-review-2026-10-15.json.xz)
retains the independent archive audit, documentation-validator source transition,
all 19 successful documentation checks and the separate executed helper identity.
Local browser inspection verifies the four updated guides and search results for
`set membership`; this local observation does not substitute for hosted checks.

The source base is `54f107d4add9d1d218f0f78e47c8d6f76785120a`. The CLI SHA-256 is
`eb3f5efefc735655fd0d97485a138ea5f812545020498d414107fb41120ca0cb`;
the binding SHA-256 is
`df872270b30bd4c72a1b47589203ddce2c6ebe4105aefbed17cf149df0f13b17`.
The packet preserves complete build commands and frozen helpers. Reproduction
uses the feature-gated release build, explicit native library, all 22 public
families and the retained complete query reference with `--repeats 1`, under
the existing local storage, workload, deadline and cleanup guards. Historical
absolute paths identify the original observations; another host must supply or
regenerate identical bulk fixtures.

This finite unit does not close the six-area/eight-investigation completion
contract or a complete competitive gate. Remaining allocation, stateful breadth,
recovery and platform obligations retain their owners. Native transport follows
the optimization and breadth work.
