<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native optimization coverage: retained observations

Status: the two original requested observations are complete and correct at source
`9f4580c723f876f087d3e75fb56153b4b099ef20`, tree
`30f702666a8d28c901e0e6806743fd1f23d0f7df`. They precede the subsequent Sort
demand connections. The later, separately measured cohorts below are retained
with their own source identities. None establishes complete optimization coverage. The
[active contract](../architecture/native-optimization-coverage-2026-11-13.md)
retains the existing and planned implementation obligations.

## Original 10,000 declarations

Every original declaration passes in one isolated execution per case, in its
original order, against its independent complete expected result. There are
zero rejected, incorrect, incomplete, engine, resource or harness outcomes.
No warmup, repeat, cache manipulation or external execution was added.

| Observation | Complete cases | Engine seconds |
| --- | ---: | ---: |
| Previous shared native pipeline | 10,000 | 1,514.235435577 |
| Equivalent-pipeline integration fixes | 10,000 | 1,560.142540948 |

This is a same-ID, same-clock comparison of one observation per runtime. It
establishes neither repeatability nor attribution to an individual change.
Full input and source checks surround execution; the active supervisor retains
storage, concurrency, deadline, result-validation and process-cleanup guards.

The saved certificate audit records the common native pipeline for all 10,000
queries, no catch-all execution and no external fallback. It records 66,280
operator invocations and 2,974 file-kernel invocations in 2,379 cases. The 6,122
cases reporting direct-source demand write zero prepared-input bytes.

Recorded mechanism counts are unchanged from the previous observation:

| Recorded work | Cases |
| --- | ---: |
| Positive column demand narrower than available columns | 6,022 |
| Source buffer transfer | 4,238 |
| Materialized relation reuse | 196 |
| Rank before selected-payload source reads | 24 |
| Metadata-pruned scan | 3 |
| Adaptive scan policy exercised | 8,632 |
| Scan window shrunk / grown | 0 / 0 |

These overlapping counts cover only recorded mechanisms. They are not a
denominator for optimization coverage, and absent counters do not classify an
opportunity. Column counts also do not imply proportional byte savings for
every input format.

The [compact packet](evidence/native-optimization-coverage-2026-11-13/README.md)
preserves identities, complete case/clock comparisons, selected raw certificate
fields and validation history. Its verifier passes after relocation and rejects
five identity, clock, fallback and activation corruptions. Complete answers were
independently replayed against the original reference ledgers locally; this
compact verifier does not rerun that full answer comparison. The original
ledgers remain retained.

## Requested original 347 declarations

The separate requested pass uses the exact retained 347 declarations and five
Parquet files, with one million fact rows, the Python SQL surface, two threads
and a 2 GiB native budget. All 347 complete results and 12,565 output rows pass
independent replay.

| Observation | Complete cases | Engine seconds |
| --- | ---: | ---: |
| Retained October 29 observation | 347 | 11.978234339 |
| Current requested pass | 347 | 12.065545164 |

This is an observation of the current runtime. Historical retention thresholds
remain diagnostic and do not decide acceptance of a new optimization. The
comparison does not attribute an individual mechanism's benefit.

The same 347 IDs are also the first 347 declarations in the new 10,000-case
pass, where their engine sum is 12.472699075 seconds. The remaining 9,653 take
1,547.669841873 seconds. All 347 use direct-source demand and none invokes a
file kernel. Thus a file-kernel count cannot explain their performance or
establish whether later, more varied declarations receive every useful
optimization.

## Original observed-source validation

The measured source passed formatting, strict default and native workspace
Clippy, 3,325 default tests, 172 focused native integration tests, 1,567 native
CLI/Python tests, and 442 installed-wheel tests with no installed-test skips.
Three platform-specific CLI/Python tests remain intentionally ignored. An
earlier broad native-library attempt was interrupted; a complete native-library
pass is not claimed for this measured source. All failed and interrupted check
records remain in the validation history.

The first implementation unit addresses equivalent flat/nested declarations,
source demand, alias pushdown, rolling demand, the single-projection credit
window and empty filtered COUNT certification. Remaining operator connections
and planned mechanisms are still work to complete. The subsequent connections
below are not part of either original observation above.

## Shared source-demand connections

Both cohorts are frozen from declaration shapes before candidate execution.
They reuse the exact original case IDs, input generations, independent expected
results and public worker. The [original expanded-corpus design](native-expanded-query-2026-11-03.md)
defines the profile schemas and populations. Execution uses two threads, a
2 GiB native grant, one attempt per case and no warmups or cache manipulation;
the cache state is uncontrolled. Full source/input inventories run before and
after queries, with lightweight storage, workload, deadline and cleanup guards
during execution. All other task work is idle during each candidate pass.

| Connection and measured source | Cases | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Sort demand, `cac86e8b0a5fa5fa8bf4c900dbef7a703746bfa8` | 606 | 85.218734162 | 45.164316757 |
| Cardinality/unary demand, `c79f27a8222e7a1fe5009b07a2a19c1c9dd387bb` | 213 | 19.627180451 | 18.502198420 |

Every complete result and schema passes independent replay, with the common
native pipeline, no catch-all execution and no external fallback. The cohorts
have no overlapping IDs. Their baseline observations retain the original `9f4580c72`
source identity; they are not relabeled as current executions. For cardinality,
the baseline-reuse proof checks that the intervening production change only
connects downstream demand through Sort and that all affected declarations are
in the disjoint 606-case cohort. The two engine totals must not be combined into
a synthetic current full-corpus result. One observation per runtime does not
establish repeatability.

### Sort demand

The selection contains all 606 Parquet/IPC declarations with downstream Project
demand reaching Sort through Limit, including controls. It spans 429 Parquet and
177 Arrow IPC cases across all five interfaces: 126 embedded workflows, 110
Python SQL, 131 typed relations, 132 CLI SQL and 107 DataFrame requests.

| Prospective group | Cases | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Newly narrowed | 98 | 52.121035209 | 12.557384620 |
| Already narrowed | 19 | 3.087769543 | 3.024694083 |
| All fields required | 489 | 30.009929410 | 29.582238054 |

All 606 source-column counts match their required dependencies. Among the 98
newly narrowed cases, physical reads decrease in 69, increase in two and remain
unchanged in 27 IPC cases. All 508 controls report unchanged physical bytes.
The independent metadata-only decision review finds nineteen prior ranked-read
admissions: eleven remain admitted and eight decline under the unchanged strict
worst-case compressed-byte cost rule after projection narrows. This explains
the selected policy; it does not prove that policy is optimal or remove the two
observed increases.

The 489 all-fields-required controls add 32–64 KiB of conservative binding
reservations, with peak increases no larger than 128 KiB. Across the entire
cohort, the largest peak/retained native-credit increases are 327,169/175,369
bytes. No denied reservation or spill occurs. Credits measure owned native
reservations, not process RSS. These costs remain part of the accepted result.

### Cardinality and unary demand

The selection contains all 196 expansion declarations and all seventeen scalar
argument-free counts. It spans 97 Parquet, 38 Arrow IPC and 78 Vortex cases, with
53 Python SQL, 45 embedded workflow, 41 typed relation, 38 CLI SQL and 36
DataFrame requests. It includes all 96 remaining extra-column observations from
the dependency audit, plus 117 controls.

| Prospective group | Cases | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Newly narrowed | 96 | 11.806045532 | 10.802229006 |
| Already narrowed | 38 | 0.736060747 | 0.667655748 |
| Metadata COUNT control | 1 | 0.000110083 | 0.000121459 |
| Native Vortex controls | 78 | 7.084964089 | 7.032192207 |

All 135 comparable Parquet/IPC column counts match the frozen dependency demand.
All 96 targeted cases narrow; none expands. Physical reads decrease in 67 and
remain unchanged in 29. The other 39 comparable controls have unchanged read
bytes. Native Vortex controls lack this generic per-source read counter; their
unavailable counts must not be reported as zero reads or completed coverage.

One newly narrowed case, four already-narrowed cases, the metadata-count control
and fifteen Vortex controls have higher individual engine times. The complete
cohort and newly narrowed group both improve. The largest peak/retained native
credit increases are 105,151/47,807 bytes; no denied reservation or spill occurs.
Required-list retention, hidden predicates and new binding metadata remain
charged. Pressure, cancellation, late failures and final refunds are separately
covered by the focused and complete native tests.

### Connection validation and portable proof

Sort passes complete default/native formatting, strict Clippy and tests, with
3,325 default and 7,128 native workspace tests. Cardinality/unary passes the
same gates with 3,325 default and 7,139 native workspace tests, including all
eleven new regressions. The latter full native library has 4,166 passes and
36 intentional ignores; the full native workspace has 39 intentional ignores.
Each release artifact passes all 442 installed-package tests with zero skips.
Checks, builds and installed proofs bind the exact source files for their own
measured commit. Later documentation changes do not relabel those runtimes.

The [portable connection packet](evidence/native-optimization-connections-2026-11-13/README.md)
contains 168 archived members. Its standard-library verifier checks exact
declaration/order/identity joins, both saved engine sums, dependency counts,
full selected certificates, physical work/resource changes and source-bound
default/native/build/install proof. Relocated replay and eighteen corruption
controls pass without executing queries. Complete answer/reference ledgers and
large runtime binaries remain local; this compact verifier does not repeat full
answer comparison. The first packet's verifier failed because the older
baseline certificate export intentionally contained only five fields. That
failure and original packet remain preserved. The revised packet adds complete
saved baseline certificates for the exact 819-case union, without new queries,
and rejects optimized Python rather than silently disabling assertion checks.

The [mechanism inventory](../architecture/native-optimization-inventory-2026-11-13.md)
records the full audit and outstanding integration/decision evidence. These
connections close 194 specific excessive-column observations. They do not close
item 19, item 20, RFC 0044 or CG-1 through CG-23, or establish competitive superiority.

## Sort payload validation correction

The bounded file-Sort shortcut omitted mandatory validation for hidden or
non-winning payload values. The correction retains shared Sort for those cases.
All 21 prior file-Sort shortcuts are removed; all 36 other file-kernel cases
remain admitted. A source-bound 388-case cohort at
`4d5fa4a46268380a634e78f057abba087f4042f5` completes one execution per case,
with independently replayed answers, schemas, certificates and engine clocks.
Every case reports the native pipeline, with no catch-all, external fallback,
denied reservation or spill. All ten owned-source controls retain the same
column counts.

The measured candidate total is 337.804658081 engine seconds. Its historical
comparisons remain four separate groups because their baselines come from two
source commits; these totals are not combined into a synthetic baseline:

| Group | Cases | Baseline source | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | --- | ---: | ---: |
| Owned source controls | 10 | `cac86e8b0a5fa5fa8bf4c900dbef7a703746bfa8` | 0.521766793 | 0.637267499 |
| Vortex, no file kernel | 321 | `9f4580c723f876f087d3e75fb56153b4b099ef20` | 253.571914075 | 250.922701375 |
| Vortex, other file kernel | 36 | `9f4580c723f876f087d3e75fb56153b4b099ef20` | 18.020221127 | 17.696910084 |
| Vortex, prior file Sort | 21 | `9f4580c723f876f087d3e75fb56153b4b099ef20` | 0.191429042 | 68.547779123 |

The higher Sort-group cost corrects previously omitted mandatory validation;
this is not a speedup or an optimized Vortex source-demand claim. The complete
answers were replayed locally against independent references. The
[portable packet](evidence/native-sort-payload-validation-2026-11-13/README.md)
contains 120 archived members in a 28,559,515-byte archive and a standard-library
verifier. Relocation replay, 19 negative controls and refusal to run under
optimized Python pass. The portable verifier does not repeat full-oracle answer
validation.

At the measured source, default validation passes 3,325 tests and native
validation passes 7,145 tests with 39 ignored, including the full native library
suite (4,172 passed, 36 ignored). The installed package passes 442 tests with
zero skips. All 5,691 compared source identities match fingerprint
`1f1882408d996c1b5a9ccb42c89adceb84036b8592ac8d523b10b0e4637baaff`.
These source-bound checks and the original failed/stopped observations are
preserved in the packet. Existing narrower-column claims remain scoped to their
own cohorts. This correction does not close item 19, item 20, RFC 0044 or CG-1
through CG-23.

## Shared Sort prefix connection

The correctness-preserving prefix connection at
`788a8925e587316ed6f6c3d9ca114fef29937bd5` is locally accepted. The fixed 388-case
cohort uses exactly the same current `4d5fa4a46` baseline, declarations, inputs,
references and resource grant. Each candidate declaration executes once in
isolation; no baseline query is repeated. The complete engine total decreases
from **337.804658081 to 327.589944482 seconds**, a 10.214713599-second reduction.
All complete values, schemas, certificates and clocks pass independent replay.

The change connects full typed payload validation to bounded output construction
through the existing shared Sort owner. It preserves validation outside the
retained prefix, hidden-field and copy-policy barriers, error order, cancellation
and last-owner memory credit. Actual pipeline/file-kernel counts and all ten
owned-source column controls are unchanged. No denied reservation or spill
occurs; retained reservations do not increase, and the largest peak increase is
25,264 bytes. A logical projection does not establish physical Vortex bytes saved.

At this source, formatting and strict default/native Clippy pass. The default
suite passes 3,325 tests; native validation passes 7,157 with 39 ignored, including
the full native-library suite (4,184 passed, 36 ignored). All 442 installed-wheel
tests pass with zero skips. Source-bound receipts share 5,694 file identities
and fingerprint `e0192f0cdd42382038ef7aec913b3bd3c3d126159252ddd68d390481dd6387b6`.

The [portable packet](evidence/native-sort-prefix-validation-2026-11-13/README.md)
contains 107 archived members, exact source/runtime bindings, saved case and
certificate observations, work/resource deltas and the initial packaging-verifier
failure. Corrected portable replay, relocation and all 26 corruption/refusal
controls pass without rerunning queries. Full-answer oracle replay occurred
locally; portable verification checks the saved evidence. The single observation
does not establish repeatability or whole-engine superiority. Remaining Vortex
scan decisions, COUNT/cyclic connections, composition breadth, runtime filters,
skew handling and cross-query reuse remained active work at that source.

## Subsequent activation unit

The [November 14 report](native-optimization-activation-2026-11-14.md) records a
separate accepted unit at `f414d9dd3`: 7,498 complete declarations per runtime
and five two-call lifecycles per runtime. Fresh-session engine time decreases
from 1267.114826002 to 1263.682737384 seconds, and the complete lifecycle total
also decreases. All 5,189 individual regressions and the slower complete COUNT
subgroup remain explicit. Executed scan reporting, bounded shared connections,
runtime filters, skew ranges, segment reuse and public breadth have concrete
evidence. These totals do not replace or combine with the historical 10,000,
347, 606, 213 or 388-case observations above. Broader coverage stays open.
