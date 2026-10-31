# Typed numeric ordering — rejected complete-cohort experiment

## Decision

**DROP under the unchanged complete-cohort rule.** The 347-query cohort increased from 11.979219492 to 12.086481459 engine seconds (0.107261967 seconds; 208 slower cases, 139 faster, no ties). The 48-workflow cohort improved from 0.920503372 to 0.808468919 engine seconds (-0.112034453 seconds; 9 slower workflows, 39 faster, no ties). The supplemental gain does not offset the required 347-query loss.

Each result is one complete execution of every declared case. These are single observations; no statistical confidence or isolated family causality is claimed. The accepted baseline remains 11.979219492 seconds for 347 queries and 0.920503372 seconds for 48 workflows. It was not advanced. No repeats, Full43 run, Polars ratio, or speedup claim is included.

## Correctness and acceptance checks

All recorded correctness checks passed: native workspace 6,593 passed / 0 failed / 36 existing opt-in ignores; default workspace 3,290 passed / 0 failed / 0 ignored; source Python 918 tests run (40 expected skips); public acceptance 480 comparisons / 2338650 rows; installed package 285 tests / 0 failures / 0 errors; 24 mechanism controls and 12 recovery controls. Correctness passing does not change the performance disposition.

## 347-query family totals

Totals use `math.fsum` over paired per-query engine execution clocks. Positive delta means the candidate was slower. Family results are descriptive and do not establish isolated causality.

| Family | Cases | Baseline (s) | Candidate (s) | Delta (s) |
| :-- | :-- | :-- | :-- | :-- |
| projection | 26 | 1.024053996 | 1.064794540 | 0.040740544 |
| predicates | 16 | 0.403710000 | 0.414690499 | 0.010980499 |
| nullable | 8 | 0.126778918 | 0.127941543 | 0.001162625 |
| numeric | 4 | 0.134438208 | 0.145011166 | 0.010572958 |
| strings | 5 | 0.509474750 | 0.521245791 | 0.011771041 |
| temporal | 6 | 0.384818583 | 0.399716168 | 0.014897585 |
| global_aggregation | 67 | 1.322176042 | 1.323496249 | 0.001320207 |
| distinct | 4 | 0.102166251 | 0.103076584 | 0.000910333 |
| ordering | 10 | 0.347847373 | 0.365042584 | 0.017195211 |
| joins | 14 | 1.152746624 | 1.152480043 | -0.000266581 |
| sets | 4 | 0.004319583 | 0.004271458 | -0.000048125 |
| grouped_aggregation | 180 | 5.354105415 | 5.360292167 | 0.006186752 |
| multiway_joins | 2 | 0.914848333 | 0.901451375 | -0.013396958 |
| composite | 1 | 0.197735416 | 0.202971292 | 0.005235876 |

All 208 slower 347-query cases are retained in the complete packet observations; no case or family was selected to change the decision.

## 48-workflow template totals

Totals use `math.fsum` over each template's four input-size observations.

| Template | Cases | Baseline (s) | Candidate (s) | Delta (s) |
| :-- | :-- | :-- | :-- | :-- |
| asof_ordered | 4 | 0.089046834 | 0.089249208 | 0.000202374 |
| asof_shuffled | 4 | 0.176084540 | 0.150688000 | -0.025396540 |
| collections_ordered | 4 | 0.031693708 | 0.029728749 | -0.001964959 |
| collections_shuffled | 4 | 0.142430125 | 0.114781209 | -0.027648916 |
| selectors_ordered | 4 | 0.014186292 | 0.014270625 | 0.000084333 |
| selectors_shuffled | 4 | 0.017593333 | 0.017555376 | -0.000037957 |
| sort_ascending | 4 | 0.037362916 | 0.036833667 | -0.000529249 |
| sort_nullable_mixed | 4 | 0.038269625 | 0.037229458 | -0.001040167 |
| sort_reversed | 4 | 0.097614792 | 0.084369375 | -0.013245417 |
| sort_shuffled | 4 | 0.116787375 | 0.100358709 | -0.016428666 |
| window_ordered | 4 | 0.030088416 | 0.029213710 | -0.000874706 |
| window_shuffled | 4 | 0.129345416 | 0.104190833 | -0.025154583 |

All 9 slower supplemental workflows are retained in the complete packet observations. The 48-workflow gain remains supplemental; the required complete 347-query cohort increased.

## Recalculation checks

The summary validates passed, complete, posthash-verified observations; one engine-clock observation per case; matching case order and identities; complete row counts; and agreement with recorded cohort dispositions. Engine execution seconds are the only timing values reported.


## Source, failure and ownership evidence

The [prospective contract](../architecture/native-typed-ordering-2026-10-31.md)
was declared at `94e88391` before runtime edits or candidate execution. The
candidate shared exact typed primitive comparison with Top-K after complete
input validation and matching-domain proof. It added no retained view table,
row-location table, dependency or external execution. The general unproven
comparison paths and changing-block spill merge stayed unchanged.

Both original cohort gates were required. The prospective declaration allowed
local measurement before the prior PR's hosted closeout; a later wrapper had
unnecessarily required that merge first. Before any candidate timing, its
prerequisite was corrected with original/new source and pending-check evidence
retained. The observer, worker, engine clock, case order, limits, independent
references and comparison rule did not change. This did not waive hosted merge
checks. No observation was discarded or repeated to improve the result.

The [complete packet](evidence/native-typed-ordering-2026-10-31/acceptance.json)
binds 7,373 records and 8,287 original paths, including source/build identities,
all query results, activation and optimized-code checks, resource/ownership
controls, and the two original failed validation attempts. Those failures were
a documentation lint and a saved-result helper's scratch fixture path, corrected
with source history preserved. Native query results were not changed.

The [verification addendum](evidence/native-typed-ordering-verification-2026-10-31/verification.json)
records complete offline replay and 14 corruption controls in a source-only
copy: 353 copied files, 8,467 original-path rejection probes, no native engine
imports, subprocesses, network or new queries. It also retains the first two
source-only failures caused by a missing test-helper import and the later
verification-builder provenance-path failure. All were fixed without repeating
benchmark execution. Missing proof was corrected rather than suppressing tests.

The [restoration record](evidence/native-typed-ordering-verification-2026-10-31/rejection.json)
proves that the exact 14 experimental Rust paths match the frozen candidate
source/patch before restoration, then restores the accepted source. All 2,609
Rust/Cargo inputs match the retained ordered-input runtime. Candidate source
and the original 34,204-byte patch remain in the portable packet. No unrelated
work was changed. The unchanged accepted runtime remains the execution baseline.

The [retirement record](evidence/native-typed-ordering-retirement-2026-10-31/cleanup.json)
removes 13,470 generated regular files and three environment links after repeated
identity and closed-handle checks. All 2,850 raw output/comparison files have
byte-exact portable replacements; post-retirement replay passes. The accepted
ordered-input runtime/wheel and original inputs/references remain.

The [maintainer queue screen](../architecture/native-shared-owner-optimizations-2026-10-30.md#october-31-maintainer-pruning-decision)
is a separate judgment about future priorities. It does not alter this
experiment's original gate or turn the supplemental gain into acceptance.
