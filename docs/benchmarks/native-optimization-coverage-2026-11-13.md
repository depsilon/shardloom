<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native optimization coverage: retained observations

Status: both requested observations are complete and correct at source
`9f4580c723f876f087d3e75fb56153b4b099ef20`, tree
`30f702666a8d28c901e0e6806743fd1f23d0f7df`. They precede the subsequent Sort
demand connection and do not establish complete optimization coverage. The
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

## Validation and remaining scope

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
and planned mechanisms are still work to complete. The subsequent Sort demand
change is not part of either runtime measured here. Item 19, item 20, RFC 0044
and CG-1 through CG-23 remain open; no competitive superiority claim follows
from these observations.
