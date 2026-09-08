# Native topology grouping: September 8 evidence

**Decision: REVISION REQUIRED; this grouping path is not promoted.** Complete returned values passed for all three 129-run packets, but Auto and Target1 regressed the aggregate timing and did not change independently schedulable native split geometry. This is an internal same-engine regression packet, not an independent-oracle or superiority claim.

The [saved analyzer report](/Users/dylan/LocalData/shardloom/perf-all-20260906/topology-a3c62434-grouping-analysis.json) records complete raw-output revalidation against the same-binary Existing control, including exact integer values, row order, finite binary64 values and signed zero. This report checked the three Full43 and two heldout summary hashes and derives the measurements below from those saved files; it ran no queries, tests or benchmarks. [Derived JSON](/Users/dylan/LocalData/shardloom/perf-all-20260906/native-topology-staging/phase1-a3c62434-evidence-derived.json) preserves all 43 per-query comparisons, identities, hashes, route decisions and counter availability.

## Frozen packet and timing

All modes use commit `a3c62434d2d9839d5893e0871da388ca753ff68e`, binary SHA-256 `2f78f7185b881ae369ed4a88797811035c34d9e4a222848ff4c5fbca0f559108`, the same pinned SQL and harness hashes, and the same `perf-current-c71a558e.vortex` generation: 18,643,482,956 bytes; device 16777232; inode 87315296; mtime_ns 1788657393091822305; ctime_ns 1788657393091977014. Selected reports cover the actual 99,997,497 source rows.

The host is ARM64 macOS 26.6.2 with 10 reported CPUs. The release build uses Rust 1.98.0, LLVM 22.1.8 and `release-user-surfaces`; the native provider is Vortex 0.85. The request is P12 and 24 GiB memory; the selected region route's actual CPU grant is P10 with nine background workers. Each run uses a new native process, with uncontrolled OS page cache and no answer cache. Wall time includes native process creation, public CLI output completion and exit. “Best sum” adds the minimum of three runs for each query; “hot sum” uses the minimum of runs 2–3. These are measured sample statistics, not confidence intervals.

| Mode / saved summary | Exact runs | Best sum, seconds | Change | Hot sum, seconds | Geomean, seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Existing](/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/full43_20260908T101651141111Z/summary.json) | 129/129 | 91.866821957 | control | 91.934308581 | 0.773034019 |
| [Auto](/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/full43_20260908T102227282398Z/summary.json) | 129/129 | 92.932750837 | +1.1603% | 93.130342420 | 0.787809066 |
| [Target1](/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/full43_20260908T102912949084Z/summary.json) | 129/129 | 93.055419416 | +1.2938% | 93.152873582 | 0.788117489 |

The sums of all 129 measured wall times are 278.741062541 seconds Existing,
282.922340422 Auto, and 282.190864083 Target1. The full suite preserves every
query and sample, including retained routes and first-process observations.

The 13 queries that actually selected regions total 34.222765457 seconds under Existing, 34.972642210 under Auto, and 34.958690668 under Target1. Twelve of those 13 are slower in both candidate packets; Q6 is the exception. The rest of the full-suite change includes ordinary-route variation and must not all be attributed to the new scheduler.

## Operator shapes and actual route selection

The classification below follows the pinned SQL and observed admission. It does not infer that every ORDER BY runs a local Top-K. Q5/Q6 are scalar COUNT DISTINCT results even though the analyzer's descriptive shape helper calls them “rows.”

| Queries | Operator shape | Region outcome for both candidates |
| --- | --- | --- |
| Q1 | Metadata COUNT(*) | Outside this region route; report absent |
| Q2, Q21 | Filtered count: numeric / text predicate | Existing CountWhere route; report absent |
| Q3–Q7, Q30 | Scalar SUM/COUNT/AVG, integer and UTF8 DISTINCT, MIN/MAX, wide repeated arithmetic SUM | All six selected; final reduction remains central |
| Q8–Q19 | Single/multiple-key grouped count, distinct and mixed aggregates; mostly ordered limited results | Q12/Q19 selected; the other ten explicitly retained |
| Q20 | Filtered single-column point lookup | Outside this region route; report absent |
| Q22–Q23 | Text-filtered grouped extrema/count/distinct and ordered limit | Both explicitly retained |
| Q24–Q27 | Filtered raw-row ORDER BY/LIMIT, including wide payload and multiple keys | Existing row-sort route; report absent |
| Q28–Q29 | Expression-derived grouping, AVG/COUNT/MIN, HAVING and ordered limit | Both selected |
| Q31–Q38 | Grouped measures/count, numeric pairs, expressions or filtered string keys | Q36 selected; other seven explicitly retained |
| Q39–Q43 | Filtered grouped counts with ordered LIMIT/OFFSET, including CASE/date truncation | Q40/Q43 selected; other three explicitly retained |

That is **13 selected, 22 explicitly retained, and 8 outside the reported region preflight**. The 22 retained decisions comprise 16 `aggregate_count_worker_candidate_retains_existing_scan`, three `string_count_topk_requires_existing_scan`, and three `numeric_pair_late_measure_requires_existing_scan`. Absent region fields are not zero-valued region work and do not prove metadata-only execution by themselves.

## What changed, what was pruned, and where computation ran

Every selected run reports 1,170 original natural splits, full source coverage, completed/drained ownership and validated generation. Auto creates 382 groups; Target1 creates one. Both retain P10, nine background workers, a 20-split outstanding window and a 20,480-byte task-queue allowance. Observed peak active native steps is nine and peak admitted native steps is 20. Their reported region caps differ (20 versus one), but the [source-level grouping review](/Users/dylan/LocalData/shardloom/perf-all-20260906/native-topology-staging/grouping-refinement-review-20260908.md) shows that the public group cap is redundant with the existing split window. One group still contains independently submitted original splits.

The measured grouping modes consequently change bookkeeping and group-level counters, not the 1,170 native jobs or original array boundaries. Requested 4/8/16/etc. are not distinct job geometries here and are not being run merely to produce more labels. Equal reported split counts alone are not boundary fingerprints; the scheduling conclusion also depends on the pinned source review.

There is **no observed metadata pre-pruning benefit** in these selected Full43 runs: zero natural splits and zero whole groups were proven pruned; no selected run was empty or whole-file metadata-pruned. Q12/Q28/Q29/Q40/Q43 each performed 1,170 metadata-pruning evaluations, with no skip. Q40/Q43 each yielded 11 arrays and 1,159 native tasks without output. Those 1,159 jobs still ran; their empty results are not evidence of pruning before payload scheduling. Auxiliary statistics work is allowed during pruning evaluation, so such evaluation is not a zero-I/O claim.

All selected reports explicitly say `region_local_reduction_applied=false` and `region_local_topk_applied=false`, with zero region-local encoded/materialized batches. Existing encoded consumers still run in the central aggregate path. For example, Q3 records 2,042 native numeric decode calls and Q7 records three in all modes; both report zero typed-value bytes copied and nonzero encoded-reduction timings. Q30 records 1,170 native decode calls and zero typed-value bytes copied, while its encoded-reduction timing field is absent. Zero copies in that accessor scope do not mean zero decoding or zero allocation. These observations do not establish the central consumer as the sole bottleneck.

## Per-query regressions

Best-of-three wall times for every selected query are shown below; positive deltas are regressions. The derived JSON also includes all retained/unselected queries and exact unrounded values.

| Query | Existing, s | Auto delta, s | Auto change | Target1 delta, s | Target1 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q3 | 0.947629 | +0.030645 | +3.23% | +0.042604 | +4.50% |
| Q4 | 0.415917 | +0.005523 | +1.33% | +0.004671 | +1.12% |
| Q5 | 1.276894 | +0.108489 | +8.50% | +0.085122 | +6.67% |
| Q6 | 3.816213 | −0.011880 | −0.31% | −0.022099 | −0.58% |
| Q7 | 0.039358 | +0.002683 | +6.82% | +0.004817 | +12.24% |
| Q12 | 1.476575 | +0.062112 | +4.21% | +0.037199 | +2.52% |
| Q19 | 7.779713 | +0.279398 | +3.59% | +0.220839 | +2.84% |
| Q28 | 2.455692 | +0.073370 | +2.99% | +0.069202 | +2.82% |
| Q29 | 9.214378 | +0.150042 | +1.63% | +0.169955 | +1.84% |
| Q30 | 0.190862 | +0.005362 | +2.81% | +0.003827 | +2.00% |
| Q36 | 5.921825 | +0.019809 | +0.33% | +0.094501 | +1.60% |
| Q40 | 0.633367 | +0.008400 | +1.33% | +0.009756 | +1.54% |
| Q43 | 0.054343 | +0.015924 | +29.30% | +0.015533 | +28.58% |

Q19/Q29 dominate selected-route absolute losses; Q5 and Q43 are material regression targets in this packet. Ordinary-route Q17 also rises by 0.240933 seconds (+8.62%) under Auto and 0.197566 seconds (+7.07%) under Target1. Conversely, ordinary-route Q34/Q35 improve by 0.089114/0.120216 seconds under Auto. These changes demonstrate run-to-run/environmental variation outside the selected route; they are not evidence of region acceleration.

## Resource scope and next gate

CPU and peak process RSS are present for all 129 runs in every packet. Summed user+system CPU over those runs is 593.346915 seconds Existing, 606.834996 Auto, and 603.460338 Target1. Maximum single-process RSS is 6.5850, 6.5632 and 6.3004 GiB respectively, each on retained Q35. Do not sum RSS samples or treat these suite maxima as selected-route memory improvements. CPU work overlaps wall time and other workers.

Phase/accessor fields exist on 105 of 129 runs per mode; encoded-reduction elapsed fields exist on 12. Provider-worker fields exist on 30 Existing runs and 48 per candidate; aggregate-worker timing fields exist on 18 runs per mode. Other entries are unavailable, not measured zero. The 20,480-byte queue allowance is task ownership, not an all-provider or process RSS bound. Peak owned native batches is 20 for most selected queries and nine for Q40/Q43; it does not bound all central aggregate state or materialized payload bytes.

Both independent held-out matrices pass. Each contains 19 cases × five requested
worker ceilings (1/2/4/8/12) × Existing/Auto × one warmup plus three measured runs:

| Fixture | Passed acceptance records | Complete paired comparisons | Summary SHA-256 |
| --- | ---: | ---: | --- |
| [4,096 rows](/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/heldout_operators_20260908T103742651028Z/summary.json) | 760/760 | 95/95 | `6610200351eff6d47c21d4b70aa8610a1b875f01585970dbf4babce119f95e3f` |
| [131,072 rows](/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/heldout_operators_20260908T105450948053Z/summary.json) | 760/760 | 95/95 | `faa166355dd29e62fddb40e264119f85274a00d27b8beb6d7c64148bab6f4140` |

The same a3c62434 binary is used for both modes, checked against independent
Python integer, set, grouping and ordering operations. Each fixture includes 40
expected checked-overflow diagnostic records; their nonzero native exit is an
accepted semantic result, not an unexpected query failure. The remaining 720
records per fixture return complete values. All 1,520 acceptance records pass.
This adds bounded correctness evidence, not production-throughput or
worker-scaling evidence, and does not reverse the Full43 regression decision.

Two earlier 131,072-row attempts stopped on storage guards, not query failures:

- The [initial guard log](/Users/dylan/LocalData/shardloom/perf-all-20260906/topology-a3c62434-heldout131072.log)
  records 233,512,960 allocated log bytes against a 228,982,784-byte allowance
  after summary reservation. It stopped before native preparation or acceptance
  queries.
- [Retry 1](/Users/dylan/LocalData/shardloom/perf-all-20260906/topology-a3c62434-heldout131072-retry1.log)
  completed native preparation, producing the 2,170,380-byte Vortex fixture and a
  successful [prepare envelope](/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/heldout_operators_20260908T105141368540Z/prepare.stdout.json).
  The post-command guard then stopped the run before acceptance queries at
  238,063,616 allocated log bytes against the same allowance.
  The 24,773,226-byte source fixture was preserved in verified lossless gzip;
  its [archive receipt](/Users/dylan/LocalData/shardloom/perf-all-20260906/heldout131072-failed-fixture-archive-20260908.json)
  records raw SHA-256 `3df4840fec30e842d6a81d106790b4f0c3c793c89eb4b10261eb2fa6b8e829bb`.

After headroom recovery, retry 2 produced the successful larger matrix above.
Its native fixture is 2,170,380 bytes with SHA-256
`0f1331fc70ec19829bff7edf6e38247fda1e618875f013cd3951b81eed1dc2d4`.
The [completed source-fixture archive receipt](/Users/dylan/LocalData/shardloom/perf-all-20260906/heldout131072-completed-fixture-archive-20260908.json)
preserves the same complete JSONL source hash. Neither stopped attempt contributes
query samples to the completed matrix.

Keep Existing as the retained control. A later candidate coalesces original split
work into fewer actual jobs while preserving each original array and ordering:
at P10/W20, its intended job count is 585 rather than 1,170 where adjacent ranges
fit its row quantum, while retaining the original 20-output ownership bound.
That candidate is integrated with validation pending at this report's freeze;
it is not the a3c62434 binary measured here. It requires its own source/binary
packet, complete-value tests, regressions and resource measurements. No speed
claim for coalescing or later scalar work follows from this grouping packet.

## Reproduction and recoverable receipts

The [frozen binary receipt](/Users/dylan/LocalData/shardloom/perf-all-20260906/topology-a3c62434-binary.json)
records build command, source tree, compiler, complete binary hash and source
generation. The prior full source SHA-256 was re-admitted by unchanged generation;
no new payload hash was included in query timing. The pinned harness checkout is
`/Users/dylan/LocalData/shardloom/perf-topology-phase1-evidence`; use its recorded
script hashes rather than a later candidate's modified harness.

Run its `scripts/run_clickbench_query_uat.py` with the frozen binary and source,
`--build-commit a3c62434d2d9839d5893e0871da388ca753ff68e`,
`--memory-gb 24 --max-parallelism 12 --exact-floats --archive-stdout`, all 43
queries, and the guarded UAT root. Omit the region option for Existing; use
`--native-execution-regions auto` or `--native-execution-regions 1` for the two
candidates. Existing used `full43_20260905T201730795323Z` as its retained reference;
Auto and Target1 used the fresh same-binary Existing directory. All native
commands are preserved in their summaries. Heldout reproduction uses
`scripts/run_heldout_operator_uat.py` with that same binary for both binary
arguments, `--candidate-native-execution-regions auto`, `--rows 4096` or
`--rows 131072`, `--samples 3`, and `--workers 1,2,4,8,12`. Storage, residency and
process guards must remain enabled; fixture preparation and oracle comparison
are outside native query clocks.

Every Full43 and heldout record points to its complete `*.stdout.json.gz`
envelope and stores the original stdout SHA-256 and byte length. Lossless
compression preserves complete result values and fields, not just extracted
counters. Re-reading an envelope must decompress it and match both the raw hash
and length before interpreting the result. The [per-query derived artifact](/Users/dylan/LocalData/shardloom/perf-all-20260906/native-topology-staging/phase1-a3c62434-evidence-derived.json)
retains all 43 comparisons, full-suite metrics, selected/retained reasons,
counter presence and heldout receipts; its extraction scope is explicit.

The three Full43 directories and the 4,096-row heldout directory also preserve
timing JSON, PID and stderr auxiliaries in `completed-process-logs.tar.gz`.
Each tar includes `MANIFEST.json` with original names, lengths and SHA-256;
the [archive journal](/Users/dylan/LocalData/shardloom/perf-all-20260906/completed-process-log-archive-20260908.jsonl)
records archive hashes and verification of every byte before loose files were
retired. The Full43 archives contain 387 auxiliary files each; the small heldout
archive contains 2,281. The larger heldout auxiliaries remain loose at this
report's freeze. These are reversible storage changes, not missing observations.
This report rechecked small summary and receipt identities; it did not rerun
native work or repeat the analyzer's complete raw-output validation.
