# Shared mechanisms and the complete native observation, November 4, 2026

All **10,000 original requests passed** their independent schema, value,
ordering, resource and native-attribution checks. Total engine time fell from
**1,825.631237664 to 1,513.095098453 seconds**, a **17.1193% reduction**.
The combined candidate saves 312.536139211 engine seconds in this observation.
The comparison uses the immediately preceding complete native cohort.
[Replayed results](evidence/native-expanded-query-2026-11-03/shared-mechanisms/results.json).

The binary complete-cohort decision retains the combined implementation after
its correctness, resource, ownership and no-fallback gates passed. The new
1,513.095098453-second result becomes the comparison baseline. Of the 10,000
requests, 7,954 became faster and 2,046 became slower. Every original result
and regression remains in the packet. Both cohorts are single observations
with uncontrolled cache context. Each implementation's incremental timing
effect remains unmeasured within this combined comparison.
[Capture and source bindings](evidence/native-expanded-query-2026-11-03/shared-mechanisms/index.json).

## Six investigation decisions

| Investigation | Disposition | Implemented or observed result |
|---|---|---|
| A. Window partitions | Retain in the combined candidate | Stable typed partition membership, original partition visitation order and partition-local delivered-order checks avoid unnecessary global sorting. Globally ordered input uses the existing check before allocating membership. |
| B. Gather and final ownership | Retain in the combined candidate | Eligible resident primitive columns gather directly into the final credited allocation. Encoded inputs and casts use their admitted provider. Complete index checks precede selected-value checks. |
| C. Prepared validation and payload | Retain the validated join scope | Prepared joins carry required selected-value validation separately from retained payload. Hidden selected invalid values still fail in the original column order. Wide prepared ranking remains an open target. |
| D. Public construction | Drop the tested prototype | The lazy report-model split breaks fresh `get_type_hints` and `inspect.getsource`. The measured runtime uses the original public Python client. |
| E. Prepared read ownership | Retain the corrected policy in the combined candidate | Actual read and release traces justify the four-times added-extent bound. The initial factory path missed the policy and produced a distinct no-effect result. |
| F. General aggregation and sets | Retain shared final ordering | Existing typed reducer state already supplies compact extrema. Fully validated aggregate and join output can enter the existing bounded final-ordering owner. |

Focused controls cover low-memory decline, cancellation, ordering ties,
NULL partitions, shuffled input, extended floating policy, typed gathers,
producer teardown, nested parent validity, selected versus unmatched values,
first errors and grouped set composition. The provider controls include actual
asynchronous segment reads through the owned-map factory.
[Implementation contracts](../architecture/native-shared-work-reduction-2026-11-03.md).

## Complete engine comparison

| Family | Requests | Previous engine s | Current engine s | Change s |
|---|---:|---:|---:|---:|
| Joins | 2,612 | 429.425057 | 427.421712 | -2.003344 |
| Ordering | 206 | 172.498488 | 171.173030 | -1.325458 |
| Windows | 776 | 399.608238 | 162.317284 | -237.290954 |
| Multiway joins | 580 | 160.135547 | 143.973396 | -16.162151 |
| Sets | 789 | 133.724670 | 128.122668 | -5.602002 |
| Membership subqueries | 392 | 121.905516 | 115.633912 | -6.271604 |
| Grouped aggregation | 1,756 | 123.297266 | 94.474149 | -28.823117 |
| Nullable expressions | 205 | 40.296721 | 40.305314 | +0.008593 |

The result file contains all 18 families. All five public-interface totals
decreased. The 64 scale-profile requests fell from 111.906948374 to
51.426450205 engine seconds. The 880 holdout requests fell from 16.271745144
to 14.853563428 seconds. The 132 smoke requests increased from 0.095333885
to 0.095808586 seconds. Profile rows and source routes retain their original
definitions.
[Complete family results](evidence/native-expanded-query-2026-11-03/shared-mechanisms/results.json) ·
[Request-shape analysis](evidence/native-expanded-query-2026-11-03/shared-mechanisms/analysis.json).

## Next targets selected from the saved results

1. **Prepared join output and outer-join regressions.** Ordinary joins still use
   427.421712469 engine seconds. Right outer grouped requests increased from
   45.460418173 to 49.027552128 seconds. Full outer grouped requests increased
   from 53.941490249 to 56.088546806 seconds. Their DataFrame strata account
   for increases of 4.502394292 and 2.902933585 seconds, while each of their
   other interface totals decreased. Prepared Vortex strata also increased.
   Those overlapping classifications identify a concrete attribution cohort.
   Causal attribution requires separate operator evidence. The 5M product-left
   grouped request remains expensive at 13.096459875 seconds.
2. **Prepared ranking and selected payload delivery.** The 196 generated
   ranking declarations use 170.824012641 seconds. The 79 prepared Vortex
   declarations contribute 120.614137921 seconds. The largest individual
   requests use 15.246097041 and 14.587332874 seconds. The original wide
   prepared 5M alternate-route deadline remains open. The new primary pass
   preserves that separate observation.
3. **Remaining shared finalization and window work.** Windows still use
   162.317283748 seconds, including 60.043576590 seconds for rolling SUM
   declarations. Multiway joins use 143.973395709 seconds, sets use
   128.122667833 seconds and grouped aggregation uses 94.474149479 seconds.
   Existing gather, validation and final-ordering owners remain the next
   attribution boundaries for these complete consumers.
4. **Compatible public construction.** The tested model split has a concrete
   incompatibility disposition. A future design must preserve fresh type-hint
   resolution and class source inspection alongside the existing import,
   signature, identity and pickle contracts.

These are follow-up targets from existing observations. Additional query
captures retain their own justification and authorization gates.
[Per-shape totals, complete cells and top regressions](evidence/native-expanded-query-2026-11-03/shared-mechanisms/analysis.json).

## Prepared read and ownership result

The original 5M multi-key reduction returned all 31 expected rows on both
source routes in each diagnostic. The baseline and first policy trial issued
125 completed filesystem reads with 506,943,228 returned data bytes and a
516,436,046-byte native reservation peak. The corrected factory path issued
1,835 reads with 12,473,340 returned data bytes and a 12,646,044-byte peak.
All three traces consumed the same 2,444 segments and 12,462,016 useful bytes.
Every observed allocation reached its final release. The direct Parquet
control retained its 19,762,336-byte peak.

The policy exchanged 1,710 additional read requests for 494,469,888 fewer
returned data bytes in that declaration. Portable verification reconstructs
each ownership chain from raw trace events and independently compares all six
complete outputs. Diagnostic clocks remain excluded from timing acceptance.
[Replayed ownership observations](evidence/native-expanded-query-2026-11-03/shared-mechanisms/results.json).

## Saved Polars comparison

The saved Polars observation exposes the complete-request clock. On that
matching boundary, current ShardLoom requests sum to 1,936.373348428 seconds
and Polars requests sum to 409.166984346 seconds, a 4.7325-times ratio.
ShardLoom is faster on 3,001 requests and slower on 6,999. The 423.278249975
seconds between ShardLoom's complete-request and engine totals contain
multiple kinds of construction and delivery work. Polars engine time remains
unavailable.
[Comparable-clock analysis](evidence/native-expanded-query-2026-11-03/shared-mechanisms/analysis.json).

## Source, isolation and verification

The release-mode build and admitted source are both `ecef3a10`. The pass used
the original 10,000-case manifest, order, five interfaces, source routes,
input generations, independent expectations, two-thread setting, 2 GiB native
budget, 2.5 GiB process-group RSS ceiling and 120-second per-request deadline.
Each request ran exactly once. Full input hashing and recursive inventories
ran outside query execution. Other task-owned work stayed idle during queries.
Post-run identities and process cleanup passed.
[Bound protocol and supervision](evidence/native-expanded-query-2026-11-03/shared-mechanisms/index.json).

Local gates passed 6,901 native workspace tests with 36 existing ignored tests,
3,319 default workspace tests, 167 file-provider tests and 303 installed-wheel
integration tests. Formatting and strict native, default and provider Clippy
passed. A storage guard stopped the first native workspace build. Verified
compiler-scratch retirement allowed the complete retry. The source-gate
receipts preserve all earlier compiler, lint and test failures.
[Verification packet](evidence/native-expanded-query-2026-11-03/shared-mechanisms/index.json).

During capture-tool preparation, the historical driver was overwritten and
then restored byte-for-byte from its committed archive. Its local filesystem
generation changed. The original archived driver, protocols and query
observations retain their bytes, and complete archived replay passed after
recovery. The new pass used a separately named driver. Portable packaging also
preserves its initial metadata-binding correction and an intake attempted
before its output index existed.
[Recovery and attempt records](evidence/native-expanded-query-2026-11-03/shared-mechanisms/index.json).

The new portable verifier checks the exact build, source gates, installed
package, isolation, prior complete baseline, all 10,000 current results,
all 10,000 previous native results and all 10,000 saved Polars results against
the original independent expectations. It checks native clocks against their
nanosecond counters. Twelve tests cover capture contracts, resource ownership,
public compatibility, complete denominators and rehashed result, identity,
clock, fallback and order corruption. Request-shape arithmetic is recomputed
from the same independently replayed cases. Nine additional trace controls exercise
consumption, overlap, missing endpoints, pointer reuse and malformed events.
The broader standard-library query-surface sweep ran 113 tests with nine
optional-dependency skips. All twelve new evidence tests passed with zero
skips. Final formatting, license verification and sealed packet replay passed.
[Final validation receipts](evidence/native-expanded-query-2026-11-03/shared-mechanisms/validation.json).

Run the saved-result verification with Python 3.10 or later.

```sh
PACKET=docs/benchmarks/evidence/native-expanded-query-2026-11-03
python -B -S benchmarks/traditional_analytics/query_surface/expanded_shared_replay.py \
  --baseline "$PACKET/packet" --previous "$PACKET/updated-native" \
  --archive "$PACKET/shared-mechanisms"
```
