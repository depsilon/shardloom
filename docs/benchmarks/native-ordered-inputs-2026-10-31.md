# Preserve already ordered validated native inputs

The ordered-input candidate is **retained** under the binary complete-cohort
gain rule. Both required cohorts completed with lower candidate engine time.
The unchanged 347-query cohort moved from **11.997152207 s** to
**11.979219492 s** (0.1495% lower). The separately declared 48-workflow cohort
moved from **1.307041040 s** to **0.920503372 s** (29.58% lower). Each role ran
one isolated execution per case; these single observations make no statistical
confidence claim. No Full43 or competitor run occurred.

The accepted comparison baseline advances to the ordered-input candidate for
this analytical work. The 48-case gain is supplemental evidence and does not
rescue or substitute for the 347-case result. The 347 total is lower, so both
gates pass. No individual case or family crossed the recorded material
regression flags, though several families were slower; their contributions are
shown below. The under-ten-second objective and CG-1 through CG-23 remain
separate claims and gates.

## Experiment and boundary

The [prospective contract](../architecture/native-ordered-inputs-2026-10-31.md)
was frozen before implementation and timed execution. The candidate extends the
existing native ordinal sorter with an adjacent-order check for full sorting,
window ordering, as-of build ordering, ordered collection aggregates, and
grouped selectors. It admits the check only when the existing complete-value
validation boundary and a compatible prepared-key domain are established.
The check uses the existing comparator and source-ordinal tie-break; an
inversion or declined proof enters the unchanged heap path. Ordered input
returns before heap operations. Top-K selection is unchanged.

This is ShardLoom-native execution over the existing Vortex-backed source and
prepared key owners. It adds no source read, decode, materialization, cached
answer, dependency, or external query-engine integration. Vortex remains the
native input/output boundary. The acceptance record reports
`external_engine_invoked=false`; Full43 was not run. These observations do not
establish performance for other workloads or sortedness distributions.

The optimized-code review covered seven symbols: the shared check, full sort,
window, as-of, collections, metadata proof, and grouped selectors. It verified
the ordered return before heap calls, inversion/decline into the original heap,
and absence of debug observation symbols in the shipping binary. Thirteen
targeted native controls passed, covering cancellation, comparison/error-trace
preservation, linear stable scanning, flat-domain compatibility, strict float
validation, complete results across all five owners, and prior error
precedence. The controls are correctness/mechanism evidence, not a timing
attribution or a claim that every case activated the check.

## Complete 347-query cohort

One isolated execution of each of the same 347 cases ran against the same five
Parquet source files and complete references. Both roles returned all **12,565
expected rows**. The engine-owned `engine_execution_seconds` sums are:

| Role | Cases | Complete rows | Engine seconds |
| --- | ---: | ---: | ---: |
| Accepted grouped-state baseline | 347 | 12,565 | 11.997152207 |
| Ordered-input candidate | 347 | 12,565 | 11.979219492 |
| **Change** | — | — | **-0.017932715 (-0.1495%)** |

Family totals preserve slower results as well as gains. Percent change is
`(candidate / baseline - 1) × 100`; positive means slower.

| Family | Cases | Baseline engine s | Candidate engine s | Change |
| --- | ---: | ---: | ---: | ---: |
| Projection | 26 | 1.017473002 | 1.024053996 | +0.65% |
| Predicates | 16 | 0.403261831 | 0.403710000 | +0.11% |
| Nullable | 8 | 0.128082915 | 0.126778918 | -1.02% |
| Numeric | 4 | 0.135670584 | 0.134438208 | -0.91% |
| Strings | 5 | 0.510266417 | 0.509474750 | -0.16% |
| Temporal | 6 | 0.385259374 | 0.384818583 | -0.11% |
| Global aggregation | 67 | 1.322157172 | 1.322176042 | +0.00% |
| DISTINCT | 4 | 0.103582626 | 0.102166251 | -1.37% |
| Ordering | 10 | 0.346410833 | 0.347847373 | +0.41% |
| Joins | 14 | 1.144240711 | 1.152746624 | +0.74% |
| Sets | 4 | 0.004275125 | 0.004319583 | +1.04% |
| Grouped aggregation | 180 | 5.371484657 | 5.354105415 | -0.32% |
| Multiway joins | 2 | 0.916937626 | 0.914848333 | -0.23% |
| Composite | 1 | 0.208049334 | 0.197735416 | -4.96% |
| **Complete cohort** | **347** | **11.997152207** | **11.979219492** | **-0.15%** |

No case or family crossed the experiment's material-regression flags. Projection,
predicates, global aggregation, ordering, joins, and sets were slower in aggregate;
their observed changes remain part of the complete-cohort total. Family deltas
are descriptive for this single pass, not isolated causal estimates.

## Supplemental 48-workflow cohort

The separately frozen cohort contains 12 templates at 0, 33, 4,097, and 65,537
rows: four full-sort shapes, ordered and shuffled partitioned windows, as-of
builds, ordered collection aggregation, and grouped selectors. Inputs are
Zstandard Parquet fixtures with complete independent references; each operation
used a fresh native session and complete result validation. Both roles completed
all **48 comparisons and 557,420 rows**. Reference values were independently
regenerated for all 48 cases. The table groups the four sizes per template;
seconds are summed engine clocks. Positive change means slower.

| Template | Cases | Baseline engine s | Candidate engine s | Change |
| --- | ---: | ---: | ---: | ---: |
| Ascending full sort | 4 | 0.099973459 | 0.037362916 | -62.63% |
| Shuffled full sort | 4 | 0.116316583 | 0.116787375 | +0.40% |
| Reversed full sort | 4 | 0.097744624 | 0.097614792 | -0.13% |
| Nullable mixed-direction full sort | 4 | 0.105663584 | 0.038269625 | -63.78% |
| Ordered partitioned window | 4 | 0.112967918 | 0.030088416 | -73.37% |
| Shuffled partitioned window | 4 | 0.128653543 | 0.129345416 | +0.54% |
| Ordered as-of build | 4 | 0.166770708 | 0.089046834 | -46.61% |
| Shuffled as-of build | 4 | 0.172833999 | 0.176084540 | +1.88% |
| Ordered collection aggregation | 4 | 0.129669707 | 0.031693708 | -75.56% |
| Shuffled collection aggregation | 4 | 0.142758957 | 0.142430125 | -0.23% |
| Ordered grouped selectors | 4 | 0.016126417 | 0.014186292 | -12.03% |
| Shuffled grouped selectors | 4 | 0.017561541 | 0.017593333 | +0.18% |
| **Complete supplemental cohort** | **48** | **1.307041040** | **0.920503372** | **-29.58%** |

The paired baseline and candidate records bind the same case IDs and generated
fixtures. Ordered and shuffled results are shown together so the large gains on
ordered templates do not obscure small regressions on some shuffled templates.
The total is the sum of engine clocks, not workflow wall time.

## Correctness and validation record

The acceptance record reports:

- **6,590 native workspace tests passed** across 119 binaries; 36 existing
  ignored tests; no failures.
- **3,290 default workspace tests passed** across 107 binaries; no ignored or
  failed tests.
- The source Python suite ran **918 tests**, with 40 expected optional-native
  and diagnostic skips and no failures. Native/default strict Clippy and
  formatting passed.
- **480 public-interface comparisons**, **2,338,650 complete rows**, and 96
  source fixtures passed. Thirty-two independent references were regenerated;
  source sessions were released before result export.
- **285 installed-package tests passed**, with zero failures, errors, or skips.
- **13 targeted mechanism controls passed** and optimized-control-flow review
  covered seven symbols across all five owners.

Earlier failed attempts remain preserved with these specific outcomes:

- `o005`: one focused native test panicked because its fixture reached the
  cancellation path and unwrapped the resulting `execution cancelled` error.
- `o008`: strict Clippy found two test-code lints: `stable_sort_primitive` and
  `manual_let_else`; the later accepted validation passed.
- `o014`: the broad workspace run exposed an existing unary-membership recovery
  lifecycle race: a required file was absent (`NotFound`) during recovery. The
  correction changed **nine test-only files** and records 12 corrected lifecycle
  controls; it did not change runtime source. The original failure remains in
  evidence.
- `o026`: six public CLI cases exceeded the 65,536-row in-memory collection
  limit. Their fixtures were corrected to use the declared export path. The 474
  completed comparisons were retained, all 480 replayed complete, and
  independent references and release products remained identical.
- `o032`: the disassembly capture assertion expected a symbol in command output
  and no stderr; the assertion failed on that capture. The documented helper
  was corrected and `o033` captured the same seven intended symbols for manual
  review without executing queries. The later review passed; no assembly result
  is represented as timing evidence.

## Environment, evidence, and remaining gates

The [portable packet](evidence/native-ordered-inputs-2026-10-31/manifest.json)
and [replayed acceptance](evidence/native-ordered-inputs-2026-10-31/acceptance.json)
retain complete source, reference, result and failed-attempt records. Collection
and complete portable replay pass without new queries. The frozen source is
`4ac818b42260d5aa9af239dabb266815e3c1bc98` plus its recorded source overlay,
built in release mode with `release-user-surfaces`. The candidate CLI SHA-256 is
`6635a2940e5ba55541f96e2c107e8bc919cd81d1056036f7be086e994296e233`;
the native library and optimized-code review bind
`c2fa8d14b46d883f4ef1fc1f33a2d5dea26d8a526456bfeac4b91718f284839e`.

The 347 input record declares Python 3.13.13 on macOS 27.0 ARM64, a 2 GiB
execution-memory grant and parallelism two. Both cohorts use that same grant
and parallelism. The original five Parquet files are prehashed, with no prepared
artifacts; the operating-system cache is uncontrolled. Neither cold-cache nor
warm-cache behavior is claimed. Every timing above sums the declared complete
cohort's engine-owned clock. No Full43, competitor execution, publication or
release occurs.

The [acceptance contract](../architecture/native-ordered-inputs-2026-10-31.md)
continues to define the full admission, failure, ownership, and no-fallback
requirements. All 14 corruption controls and isolated source-only replay pass
from 348 copied source/evidence files. The strengthened guard rejects all 9,185
original packet paths, including 296 external payload identities, and prohibits
native imports, subprocess execution and network access. It explicitly checks
rejection before complete replay. The first passing proof guarded the active
work folder; review found five older Parquet inputs outside that folder, so the
guard was extended and offline replay repeated. Both proof versions remain
preserved in the [verification record](evidence/native-ordered-inputs-verification-2026-10-31/verification.json);
no query observation was repeated or changed. The local CI gate matrix and
public-status checks pass. Hosted acceptance and generated-artifact cleanup
remain pending closeout work.

Replay the complete results and rejection controls from a source checkout:

```bash
python -B scripts/verify_ordered_input_packet.py docs/benchmarks/evidence/native-ordered-inputs-2026-10-31
python -B -m unittest discover -s scripts -p test_ordered_input_packet.py -v
```

Both commands consume saved evidence without native query execution. The
workflow runs these same checks in CI.
