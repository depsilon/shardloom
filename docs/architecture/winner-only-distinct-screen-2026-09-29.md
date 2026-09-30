# Winner-only exact DISTINCT — R3.a

Status: **retained and merged**. [PR #1485](https://github.com/depsilon/shardloom/pull/1485)
merged as `3fd7584dedff339883f0316184f4e83aedaa6057` after all 40 checks passed.

This packet describes candidate `22f7acd22e8c06c651f835bb817e25edcad5d5f5`
against the accepted R2 runtime,
`d1a53815846abe1cbcc5574d6cfc616f7e99e694`. R3.a closes its PERF-INTAKE
ship/drop screen. The earlier `34c30199` focused/adverse and Full43 results below
are screens; final acceptance includes the subsequent floating-admission repair.

## Final design

[The shared implementation](../../shardloom-vortex/src/local_primitives/winner_distinct.rs)
admits an unfiltered source of at least 1,000,000 rows, one nonnullable identity
integer group key and DISTINCT argument, one DISTINCT measure, and identity
COUNT(*)/SUM/AVG measures. SUM/AVG inputs must be nonnullable integer columns.
Selection must be COUNT(*) descending, optionally followed by the same group key
ascending. OFFSET plus LIMIT must fit 1–128 keys. HAVING, spill, group expressions
and argument offsets are excluded.

1. Project only the grouping key and use the existing native single-integer
   COUNT consumer. Read four spatial ranges of 65,536 rows. These 262,144 rows
   estimate cost only: decline when the retained-key row share is at least 70%.
2. Discard the sample count map and reset its row total. Run a complete key-only
   COUNT pass; require its total to equal the held source row count. Select
   OFFSET plus LIMIT keys with the existing capillary selector and complete
   integer-key tie comparator. Samples never establish winner membership.
3. Convert the selected keys directly to signed/unsigned integer IN values.
   Apply the normal Vortex bound IN filter and original projection, then run
   all original mixed measures for those keys. Verify the selected row weight
   against the complete counts. Apply OFFSET only during original finalization.

The original mixed-measure kernels are unchanged from merged base
`fe2da1df41328a4e8aa370188e9b45865169f08a`: source review found the entire
`local_primitives.rs` suffix from `SimpleAggregateStates` through EOF
byte-identical. SUM/AVG contributions still include every selected row,
independently of DISTINCT pair deduplication.

All scans retain the same source, session, runtime and cancellation scope.
Prepared generation validation encloses the complete operation. A cost decline
leaves the original complete aggregate plan in place; execution errors propagate.
Vortex 0.85.0 supplies the native projection, row-range and filter operations.
Input, execution and output remain Vortex-native, with no external-engine
fallback or new scan abstraction.

The final review found that the earlier `34c30199` admission of floating measures
could hide an overflow in a losing group. A native regression reproduces that
failure before repair. Floating SUM/AVG now keep the original complete native
aggregate, preserving its non-finite-input and overflow errors. Integer SUM/AVG
remain admitted: even the largest 64-bit integer magnitude accumulated for the
largest representable source row count is finite in the existing f64 state.
The repair does not change the mixed-measure update kernels. All eight focused
tests pass after the repair; the final broad gates below include this repair.

## Bounds and diagnostics

The auxiliary map may retain at most 65,536 keys before another chunk. A chunk
over 262,144 rows declines before updating; after each accepted chunk, exceeding
65,536 groups declines before another update. Since one row adds at most one
key, the temporary map contains at most **327,680 keys**
(`MAX_COUNT_GROUPS + MAX_CHUNK_ROWS`). The map is dropped before measure state
is populated.

This is a key-count bound, not a hash-table capacity, allocation-byte or process
RSS bound. General aggregate accounting remains observational; no new
whole-query memory bound is claimed. Spatial sampling is a cost heuristic with
no distribution-independent performance guarantee.

`aggregate_winner_distinct` distinguishes decline from complete-count proof and
records sampled/count/retained/measure row weights, key limits, projection and
separate auxiliary timings. Reader evidence includes auxiliary scans. The
existing first-pass timing fields describe the subsequent mixed-measure scan;
neither those fields nor the auxiliary caller timers represent complete wall
time or CPU time.

## Focused evidence

Local receipts named below are under
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926`.

`r3a-count-spatial-q10-analysis.json` records six exact complete-result/archive
comparisons across three counterbalanced pairs. Q10 best time changes from
**3.522130375 to 2.406216417 seconds (31.6829% lower)**; median changes from
**3.554403875 to 2.415877791 seconds**. Every pair improves. Peak child RSS is
approximately **1.07–1.08 GB to 0.67–0.68 GB** (decimal GB). The paired runner
measures complete native child processes through output and exit; OS cache state
is uncontrolled. These are focused Q10 observations.

The frozen candidate identity is in
`shardloom-r3a-count-spatial-34c30199.json`; its stripped executable SHA-256 is
`1e4788044be25c69cd739cf666270966491807c1cc179ecdc959f8fcea0707bf`.
The analysis references
`paired43_20260930T011313739555Z/summary.json` under the local UAT logs.

Historical admission cohorts explain the selected design:

| Cohort | Best-time reduction | Observation / local receipt |
| --- | ---: | --- |
| Public SUM-first calls | −5.36% | Slower complete composition; `r3a-existing-two-pass-screen.json`. |
| Original shared measure-pass prototype | +12.69% | One pair was slower; `r3a-prototype-q10-analysis.json`. |
| Public COUNT-first calls | +28.11% | Every pair improved; `r3a-count-first-screen.json`. |
| Native prefix cost screen | Declined Q10 | Estimated 79.87% winner coverage versus complete 42.45%; `r3a-count-prefix-q10-analysis.json` and `r3a-global-winner-share.json`. |

These are separate admission cohorts with their own controls and execution
boundaries. Their percentages do not establish incremental gains between
revisions. The historical measure-pass prototype is superseded by the COUNT-only
design above.

## Adverse and pre-repair Full43 evidence

The `34c30199` adverse screen compares three SQL variants to each variant's first
complete control result. All **18 outputs match strictly**, including ordinary
measures and exact DISTINCT. The modified group columns remain integer columns.

| Variant | Control best | Candidate best | Candidate decision |
| --- | ---: | ---: | --- |
| Group by MobilePhone | 3.120423 s | 3.119908 s | Decline high sample winner share |
| Group by AdvEngineID | 2.955211 s | 2.968221 s | Decline high sample winner share |
| COUNT descending, RegionID ascending, LIMIT 3 OFFSET 2 | 3.670297 s | 2.180501 s | Complete COUNT followed by selected measures |

Both declined cases stay within 0.5% of control at best; medians are recorded
alongside every sample. The OFFSET variant improves 40.59% at best and retains
approximately 0.55 GB peak RSS versus 1.07–1.08 GB. These comparisons are not a
distribution-independent cost guarantee. Receipts: `r3a-adverse-screen.json`,
`r3a-adverse-screen-resumed.json` and `r3a-adverse-analysis.json`.

The first adverse attempt stopped at the 256 MiB log guard after ten recorded
results. Lossless archival of completed historical logs restored headroom;
the remaining eight calls then ran with the same source and binary identities.
The interrupted attempt and its extra uncounted call logs remain archived.

The first Full43 cohort, also before the floating-admission repair, passes all
258 complete result comparisons and archive checks. Q10 improves from 3.663904
to 2.403372 seconds (34.40%), but the sum of query bests is flat:
67.750055 versus 67.811620 seconds. Q35 is flagged for follow-up, from 6.554376
to 7.569942 seconds (15.49% slower); no cause is established from that timing.
This cohort is retained separately from final repaired-runtime acceptance:
`paired43_20260930T013601231767Z/summary.json` and
`r3a-final-full43-analysis.json`.

## Existing proof and limits

[Unit fixtures](../../shardloom-vortex/src/local_primitives/winner_distinct_tests.rs)
cover reordered columns, duplicate DISTINCT pairs with differing ordinary
measures, signed extremes, ties/OFFSET, admission rejection and row-weight checks.
[Native fixtures](../../shardloom-vortex/src/local_primitives/winner_distinct_native_tests.rs)
use 1,048,576 rows across sixteen chunks and an independent complete-value oracle.
They exercise ordinary/prepared execution, fresh reexecution, owned export after
source deletion, cost decline, cancellation and source replacement between passes,
reservation release, and owned-output refusal followed by recovery.
The losing-floating-group fixture additionally proves preservation of the
original non-finite SUM error after integer-only admission. The final source
review confirms ordered provider splits, stable surviving-row order and
unchanged chunk-partial accumulation for admitted integer measures; its scope
is value arithmetic, not universal equality of resource or work-counter errors.

`r3a-source-invariant-review.json` records source hashes and verification of the
seven design invariants at `34c30199`; that review ran no tests or benchmarks.
No native fixture explicitly crosses the auxiliary chunk/group thresholds.
The pressure fixture checks owned-output refusal/recovery, not injected pressure
inside the COUNT prepass. Coverage descriptions here identify assertions, not
final validation results.

## Reproduction

Use the [R2 input, reference and build recipe](source-backed-dictionary-screen-2026-09-29.md)
with separate clean checkouts at R2 `d1a53815846abe1cbcc5574d6cfc616f7e99e694`
and candidate `22f7acd22e8c06c651f835bb817e25edcad5d5f5`.
Build `release-user-surfaces` with the same toolchain/profile, resolve the Cargo
target directory, and strip distinct frozen copies. Record their new identities.
Set `R3_CONTROL` and `R3_CANDIDATE` to those absolute executable paths; retain
the R2 recipe's `R2_INPUT`, `R2_UAT` and `R2_REFERENCES`. The unchanged input
SHA-256 is `31cc61cfc347cf19a0328c196d59cd1eb431679311294cdc92263fef31062b35`.

Run the existing guarded paired runner sequentially, without concurrent builds
or tests:

```sh
python3 -B scripts/run_clickbench_paired_query_uat.py \
  --control-binary "$R3_CONTROL" --control-commit d1a53815846abe1cbcc5574d6cfc616f7e99e694 \
  --candidate-binary "$R3_CANDIDATE" --candidate-commit 22f7acd22e8c06c651f835bb817e25edcad5d5f5 \
  --input "$R2_INPUT" --uat-root "$R2_UAT" --reference-dir "$R2_REFERENCES" \
  --queries benchmarks/clickbench/queries.sql --query-ids 10 \
  --memory-gb 24 --max-parallelism 12 --timeout 120 \
  --max-workspace-gib 100 --reverse-order
```

This is the focused reproduction recipe for the repaired runtime. The earlier
focused observations used `34c30199`. Omit `--query-ids 10` for Full43. Preserve
all samples, complete comparisons, identities and resource observations.

## Final acceptance

The final integer-only runtime passes all **258 complete Full43 comparisons**.
Q10 best changes from **4.381091 to 3.108368 seconds (29.05% lower)** and median
from **4.481784 to 3.218940 seconds**. Every Q10 pair improves. Peak child RSS
ranges from 1.063–1.078 GB for control and 0.665–0.680 GB for candidate. The
complete COUNT selects 10 of 9,040 groups; mixed measures visit 42,451,524 of
99,997,497 source rows. No group membership is inferred from sampling.

The sum of all 43 query bests changes from **66.507349 to 65.177969 seconds
(2.00% lower)** in `paired43_20260930T015704489072Z`. This is a separate cohort
from the flat pre-repair Full43, not an incremental percentage to combine with
it. Q35's earlier slowdown does not recur: final best is 4.198459 versus
3.935819 seconds. Q15 flags 1.810864 versus 2.062160 seconds in final Full43;
six targeted calls with the same executables then pass, with best 1.446355
versus 1.420025 seconds and median 1.446592 versus 1.421493 seconds. Its filtered
compound grouping is outside R3.a admission. Neither slowdown is established
as a repeatable candidate regression; their cause is not attributed. Every
sample, including the adverse observations, remains recorded.

Final validation passes formatting, workspace and release-surface Clippy,
3,436 workspace tests, 2,020 native all-target tests and 1,520 CLI all-target
tests. Counts overlap across configurations; 22 pre-existing native manual or
regeneration tests remain ignored. The native overflow regression fails before
the integer gate and passes afterward. Source review verifies unchanged mixed
kernels, ordered surviving rows, complete-count selection, source ownership and
error propagation, within the proof limits above.

The [portable evidence bundle](../benchmarks/evidence/winner-only-distinct-2026-09-29.json.xz)
contains complete envelopes, all cohort samples, archive/member identities,
test logs, admission screens, source review and reproduction harnesses. It links
the retained full-size reference envelopes in the R2 bundle. Its final binary
SHA-256 is `f4bcaa6c7320553859932dc04e038e9313a68eead62f3055416e326cc629a50d`.
Local receipts are `r3a-integer-final-validation.json`,
`r3a-integer-final-full43-analysis.json`, `r3a-q15-followup-analysis.json` and
`r3a-ship-decision.json`.

Retain the integer-only shared-consumer design. This is scoped native-query
evidence, not an ingest, cold-storage, production fairness or subsecond-suite
claim. R3.b must separately prove any benefit from mixed-measure workers against
this new baseline; no duplicate worker family is authorized by this result.

## Local artifact cleanup

After final acceptance and portable evidence verification, the three superseded
R3.a executables (`7556758d`, `3b779619`, `34c30199`) were retired. Exact SHA-256,
generation, unique-file and active-consumer checks preceded removal; receipts,
test logs and complete result archives remain. This removed **191,782,912
allocated bytes**. The final R3.a candidate, R2 baseline, released control and
source/reference data remain protected. Receipt:
`r3a-superseded-binary-cleanup-20260929.json` in the local receipt directory.

Completed and interrupted historical logs were separately archived losslessly,
with per-member length/hash checks before original removal. Their two receipts,
`completed-log-compaction-r3a-20260929.json` and
`interrupted-log-compaction-r3a-20260929.json`, record **8,830,976 net allocated
log bytes** removed after archive/manifests, excluding receipt overhead. Failed
attempts and their summaries remain available. These are file-allocation counts,
not measured APFS free-space changes. No active input, build cache, source file
or worktree was deleted by this cleanup.
