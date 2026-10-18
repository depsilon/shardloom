<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared native text reconstruction and validation

The combined shared-provider change reduces the sum of per-query two-sample
means from **67.4536999765 to 58.1389694790 seconds of engine execution** in the
matched 43-query comparison: **9.3147304975 seconds, or 13.81%**. Both paired
totals improve by more than nine seconds. All 172 complete results and schemas
match, with no predefined material per-query regression.

This is a matched comparison with two calls per query per build, not one
standard single-pass Full43 result. The change is fully implemented and merged in
[PR #1567](https://github.com/depsilon/shardloom/pull/1567) at `5bcee25a`, with
complete local and hosted acceptance. The
[design and provider contract](../architecture/native-text-validity-2026-10-17.md)
records the checked construction, ownership and malformed-input boundaries.

## Shared behavior

Native Zstd text materialization now reconstructs selected views while checking
every stored record's length and extent. It consumes length headers in the newly
allocated decoder output and replaces those headers with zeros; compressed
source bytes and text values remain unchanged. This permits one SIMD UTF-8 pass
over the selected immutable heap. Zeroed headers separate adjacent character
sequences. Bounds, buffer indexes, prefixes and character boundaries still pass
validation, and inline values receive independent checks after freezing.

A private provider receipt carries established text validity through immutable
native values. Shared dictionary, aggregation, DISTINCT, materialization and
sort/key consumers reuse that receipt. A UTF-8 dtype alone is insufficient:
generic unchecked builder output still receives ordinary value validation.
Host/device boundaries remain explicit, and invalid selected text is rejected.

The output retains the original admitted payload owner and full allocation
charge. Views and buffer tables use the same allocator. This adds no payload
copy or lengths vector. Nulls, Unicode, selected ranges, independently owned
escaping keys and allocation refusal preserve their existing contracts.
Primitive/binary decoding, sparse selection, raw append paths and oversized
text retain their existing checked native handling. No query IDs, column names,
SQL shapes, codec format, compression policy, new dependency or external
execution provider is introduced.

## Frozen decision and complete-workload outcome

Before any calls, the decision required at least a 5% reduction in the sum of
all 43 per-query means, lower candidate totals in both paired repetitions, and
no material per-query regression. A query flags when its mean increase exceeds
the larger of 10% and 0.15 engine seconds and both paired deltas are positive.
No flags occur, and no follow-up or replacement samples are used.

| Scope | Control engine seconds | Candidate engine seconds | Change |
| --- | ---: | ---: | ---: |
| All 43 queries | 67.4536999765 | 58.1389694790 | −13.81% |
| Five priority families: Q19, Q23, Q29, Q34, Q35 | 24.1317589360 | 20.0758572290 | −16.81% |
| Other 38 queries | 43.3219410405 | 38.0631122500 | −12.14% |

| Paired repetition | Control engine seconds | Candidate engine seconds | Candidate minus control |
| --- | ---: | ---: | ---: |
| 1 | 67.606362326 | 58.430980916 | −9.175381410 |
| 2 | 67.301037627 | 57.846958042 | −9.454079585 |

All calls use the ordinary native CLI, the same immutable input and resource
policy, fresh processes and counterbalanced ABBA/BAAB order within each query.
There is no cache purge, artificial pacing, automatic retry or best-sample
selection. The comparison includes every query, including slower observations.
The largest mean increases are Q28 (+0.1497732095 s), Q32 (+0.1336063965 s)
and Q38 (+0.0816983550 s); each stays below its frozen material threshold.
This bounded cohort does not eliminate environmental variation or establish
performance on unrelated workloads.

The preceding getter-only implementation also passed all 172 results and
schemas, but its matched sum decreased only 2.23%, from 65.5297315465 to
64.0689343415 engine seconds. It failed the same standalone 5% gate. Its full
cohort remains diagnostic. New stack samples motivated the changed
reconstruction/validation mechanism; profiling receives no benchmark credit.
The combined result above compares with the accepted runtime, not with the
unsuccessful getter-only candidate, and those two improvements are not added.

## Correctness and acceptance

Independent readback reopens all 43 archives and verifies all 688 members,
172 complete outputs, 1,524 result rows, schemas, native certificates, raw
integer engine clocks and companion records. Every CSV field agrees with its
corresponding call and query statement. Both executables, all selected source
files, harness/reference files and the complete input hash retain their frozen
identities. Resource policy agrees across both roles and repetitions within
each query. No external execution or fallback is reported.
The query values use the retained native regression reference, and schemas match
the paired accepted control. This readback is separate from an independent
correctness oracle; format/workflow oracle checks are recorded below.

Focused tests cover checked and generic construction, release-build rejection
of malformed builder text, host/device access, selection and replacement,
inline/outlined Unicode, nulls, empty values, full allocation credit and release.
The new decoder cases also cover every contiguous range of mixed values,
invalid selected versus unused text, partial UTF-8 sequences split across
records, corrupt framing/counts/ranges, immutable frozen views and forced
allocation failure. The original compressed input remains reusable.

The optimized focused run passes five new provider tests and 159 native
memory/decoder tests, with three unchanged manual exclusions. Broader acceptance
passes on the same 2,411-file selected source snapshot and measured executable:

- Required formatting, strict workspace clippy, workspace tests and the reduced
  writer configuration: 4,374 passing test executions, without exclusions.
- Strict native-feature clippy, 2,946 engine tests and 1,079 public-route tests:
  4,025 passing test executions, with 27 unchanged manual exclusions.
- All 100 direct Python binding checks, without skips, using the frozen release
  binding and executable.
- All 4,827 public cases across 36 families. Independent readback verifies
  13,004 compressed JSON files, 39,979 expected value rows, 6,330 envelopes
  and 1,578,741 pressure rows.
- All 18 format/workflow cohorts: 704 native workflow records and 704 independent
  pandas reference records. Coverage is eight inputs with collection and eight
  outputs with CSV input, raw and prepared, across 22 workflows. It does not
  claim every input/output Cartesian combination.

Counts are test executions across configurations, not unique tests. Pandas is
only an independent reference in the format/workflow suite. An initial modular
launch omitted the existing Arrow test dependency path and stopped during
fixture generation with zero workload records. Its failure and the corrected
launch are retained; no engine or harness code changed.

This read-side change does not require another full-size ingestion; the
accepted native payload is unchanged and has been fully hashed before and
after the comparison.

## Hosted closeout

All 44 PR checks passed at final head `2adcfc95`. The eight primary jobs were
verified from complete logs: 6,772 native test executions across the engine and
provider suites, 1,079 native public checks, and three installed-wheel lanes
(Ubuntu Python 3.10/3.13 and macOS Python 3.13), each passing all 100 direct tests.
The 27 established native manual exclusions are unchanged. Final-head review
reported no findings; no review threads or open PR code-scanning alerts remained.

The final PR head, CI merge tree and actual merge have identical Git trees.
All 2,411 selected measured source files match the merged runtime; the measured
build's complete tree differs in later documentation and evidence. Preview and
production served-source verification covers eight responses each. Production
HTML contains the explicitly verified Cloudflare script insertion; it is not
claimed byte-identical. Both browser checks pass desktop/mobile navigation and
search, without claiming other browsers. The production deployment completed
for `5bcee25a`. Published 0.6.0 packages remain unchanged.

The [hosted evidence index](evidence/native-text-reconstruction-hosted-2026-10-17.json)
and [138-member archive](evidence/native-text-reconstruction-hosted-2026-10-17.tar.xz)
retain the exact logs, source maps, review/deployment metadata and browser checks.
The 656,120-byte archive has SHA-256
`801dbe1c72870d7e48a38f93470ac14d52113a6894e46b3299457df7a64e5dd5`.
This subsequent closeout leaves the original local measurements unchanged.

## Source and reproduction

The control is the accepted PR #1566 implementation at `7e7346f8`, merged at
`4ea5851e`. Its complete 2,410-file selected source map agrees with the frozen
control binary. The combined candidate is `9ffac6b7`, with 2,411 selected source
files. The source tree used to build and measure the candidate is retained.

| Artifact | SHA-256 |
| --- | --- |
| Accepted control CLI | `7dd858fb4dd9bcd06353e77e8b9d6c3bcfe72aeb5b60877fa15827f2da3bf146` |
| Combined candidate CLI | `b02196fde4f2fff291cb6562a18d6d31b8aab405c92418c16f65a81c1fa544f5` |
| Native input | `06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22` |
| Frozen comparison decision | `61a235d6f980cabcdae21a0f98298d6aec8dba124e904fd8b2831677cf9c23ec` |

The input contains 99,997,497 rows and occupies 15,713,610,513 bytes in its
accepted Vortex representation. Both release builds use `release-user-surfaces`.
The observation uses macOS 27.0 on an Apple M5 with ten logical CPUs and 16 GiB
physical RAM. The declared memory policy is 24 GiB with maximum parallelism 12;
this is not an RSS limit. OS cache state and ordinary desktop activity are
uncontrolled. Input prehashing and host observations remain in the evidence.

Only `engine_execution_seconds` contributes to the tables. Preparation,
serialization, process/supervisor clocks, validation and archive work remain
separate in diagnostic records. The existing paired-query, storage, workload
and cleanup guards are unchanged. No new benchmark or timing-audit framework
is introduced.

The [portable evidence index](evidence/native-text-reconstruction-2026-10-17.json)
retains the predeclared decisions, both complete paired cohorts, every
sample and failure, diagnostic profiles, complete source/build identities,
validation receipts and maintained helper sources. Large datasets and
executables are excluded with their full identities recorded. Replay requires
the named resident input, a new output location and builds matching the
recorded sources/features.

## Per-query engine means

Each cell is the mean of two complete executions; original integer nanoseconds
and every individual sample remain in the timing CSVs and comparison packet.

| Query | Control engine seconds | Candidate engine seconds |
| --- | ---: | ---: |
| Q1 | 0.000041562 | 0.0000501255 |
| Q2 | 0.137950729 | 0.1692443335 |
| Q3 | 0.9406529385 | 0.9035423325 |
| Q4 | 0.4195372295 | 0.401523583 |
| Q5 | 1.307490438 | 1.357508772 |
| Q6 | 1.991186063 | 1.209424354 |
| Q7 | 0.0004769995 | 0.000558812 |
| Q8 | 0.152238271 | 0.155690458 |
| Q9 | 1.263490562 | 1.256497021 |
| Q10 | 2.469111187 | 2.5126554165 |
| Q11 | 1.2795111245 | 0.8731652085 |
| Q12 | 1.9113285205 | 1.5228688755 |
| Q13 | 0.78295525 | 0.6051560835 |
| Q14 | 1.722304166 | 1.634783229 |
| Q15 | 1.679968104 | 1.6497461455 |
| Q16 | 2.5653703335 | 2.561874833 |
| Q17 | 2.6324872085 | 2.5148875205 |
| Q18 | 0.3483910625 | 0.3530901875 |
| Q19 | 5.9272563125 | 4.938866104 |
| Q20 | 0.2037309795 | 0.2294433955 |
| Q21 | 1.3739146255 | 1.0865539165 |
| Q22 | 1.790672646 | 1.5000017085 |
| Q23 | 4.6997912915 | 3.9310914375 |
| Q24 | 1.6977256875 | 1.398466917 |
| Q25 | 0.316971625 | 0.311228917 |
| Q26 | 2.7778718955 | 1.4202317285 |
| Q27 | 2.7798254595 | 1.5674492085 |
| Q28 | 2.258419354 | 2.4081925635 |
| Q29 | 7.478375541 | 5.7491072075 |
| Q30 | 0.261355083 | 0.2643296035 |
| Q31 | 1.325875063 | 1.2812825405 |
| Q32 | 1.477382583 | 1.6109889795 |
| Q33 | 2.4153222085 | 2.3866326665 |
| Q34 | 2.999824479 | 2.893685855 |
| Q35 | 3.026511312 | 2.563106625 |
| Q36 | 0.4433152295 | 0.436872146 |
| Q37 | 0.3879980615 | 0.3246225625 |
| Q38 | 0.276644333 | 0.358342688 |
| Q39 | 0.2472737295 | 0.2228868955 |
| Q40 | 0.8699621455 | 0.8670903125 |
| Q41 | 0.425208083 | 0.3224036465 |
| Q42 | 0.183422396 | 0.1877053545 |
| Q43 | 0.2045581035 | 0.1961192085 |
| **Sum of means** | **67.4536999765** | **58.1389694790** |

This work does not change the immutable 0.6.0 packages, close whole PERF or CG
gates, or establish competitive superiority. Nullable-root structural-layout
fidelity, external-memory support for remaining resident state and safe output
replacement/publication remain explicit breadth priorities.
