<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native optimization drop-history review — October 18, 2026

Status: finite historical review. The reviewed records show no additional unused,
proven whole-workload Full43 engine-time gain of at least 2%. This is not evidence
that further native optimizations are impossible, and it does not change any
historical decision or gate.

## Scope and result

The review starts with the recent shared-execution and speed records, their
focused experiment packets, and the earlier performance inventories linked from
them: the [September 6 continuation](performance-continuation-2026-09-06.md),
[September 19 ship/drop record](performance-ship-drop-2026-09-19.md),
[September 26 candidate intake](performance-candidate-intake-2026-09-26.md),
[October 6 hardware campaign](../benchmarks/native-engine-acceptance-2026-10-06.md),
[October 7 state and structure campaign](native-state-structure-campaign-2026-10-07.md),
and [October 12 query revisit](../benchmarks/native-query-revisit-2026-10-12.md).
The October 12 report states that it reviewed 78 records across several
inventories and is not an exhaustive inventory of every possible optimization.

Two previously measured whole-workload results are already accounted for. The
2.23% getter-only text-reconstruction comparison is a predecessor to the
combined reconstruction in [PR #1567](../benchmarks/native-text-reconstruction-2026-10-17.md),
which records a 13.81% matched Full43 reduction; the two percentages are not
additive. The three-mechanism 4.206790% result is recorded as merged in
[PR #1571](../benchmarks/native-shared-execution-2026-10-17.md); it first failed
its frozen 5% threshold and was later accepted under the maintainer's explicit
greater-than-3% decision. Neither is an unused opportunity.

The remaining larger figures below are focused engine-time results, not Full43
gains. Each candidate remains dropped under its own frozen gate. Failed
repeatability, target, control, or resource checks are not waived by the size of
another focused gain.

| Run | Focused engine-time result | Independent rejection reason |
| --- | --- | --- |
| 601 — bound group preparation | Q28 improves 29.84% in five pairs. | The unchanged Q10 control increases from 2.0945095835 to 2.3091059380 seconds, both pairs slower and beyond its frozen control limit; all results and schemas pass. [Record](native-shared-execution-2026-10-17.md#bind-group-identities-in-the-existing-typed-aggregate-block) |
| 603 — dense occupied string counts | Q34/Q35 combined mean improves 4.21%. | Below the 5% target gate; Q35 improves in only three of five pairs, and Q10 regresses 17.27% against its control limit. Complete results and schemas pass. [Record](native-shared-execution-2026-10-17.md#store-occupied-string-counts-densely) |
| 605 — separate integer maps and bound groups | Q10/Q16/Q28 combined target time improves 12.44%; Q16 and Q28 improve 5.22% and 27.82%. | Q10 improves in only three of five pairs; Q34 and Q35 controls regress 17.22% and 11.94%, both pairs slower. Complete results and schemas pass; the contingent Full43 comparison was not run. [Record](native-shared-execution-2026-10-17.md#keep-integer-group-identity-outside-count-map-entries) |
| 607 — one-write integer hashing and static group preparation | Q16/Q28 combined target time improves 15.57%; each has five positive pairs. | Q34 control regresses 11.95%, with four of five pairs slower. Other result, CPU, RSS, and control gates pass; the candidate was dropped and Full43 was not run. [Record](native-shared-execution-2026-10-17.md#bind-group-preparation-without-changing-aggregate-state-layout) |
| 609 — occupied string-count table | Q34/Q35 combined mean improves 2.86%; Q34 improves 4.81%, Q35 0.83%. | Below the frozen 5% combined target; Q35 improves in only three of five pairs. Other result, control, CPU, and RSS gates pass; contingent Full43 was not run. [Record](native-shared-execution-2026-10-17.md#count-canonical-strings-in-occupied-table-entries) |
| 614 — expanded native preparation workers | Q29 mean improves 39.89% in five pairs. | Target process CPU rises 26.54%, exceeding the frozen 10% limit. Results, schemas, controls, and RSS gates pass; Full43 was not run. [Record](native-shared-execution-2026-10-17.md#prepare-native-text-inside-the-bounded-worker-grant) |
| 621 — co-located native preparation and reduction | Q29 mean improves 30.28% in five pairs. | The frozen resource-efficiency check fails. The report does not specify a more precise failing resource dimension here; results pass and the contingent Full43 comparison was not run. [Record](native-shared-execution-2026-10-17.md#keep-native-preparation-and-reduction-in-the-same-bounded-jobs) |
| 638 — mixed-key compact state | Q14/Q17 family mean improves 6.28%; Q17 improves 9.40% in all five pairs. | Q14 improves in only three of five pairs; separate Q19 target improves 1.34%, below 5%; unchanged Q10 control regresses 10.83% in both pairs. Component gates fail, so Full43 was not run. [Record](native-shared-speed-2026-10-18.md#mixed-key-screen-outcome) |

These are distinct experiments and cohorts; their gains must not be added or
reinterpreted as Full43 evidence. The merged retained implementation carries
the narrow original-width integer-access binding for the single-integer grouped
COUNT loop, but does not retain the rejected AVG/COUNT generic key-preparation
closure or separate signed/unsigned map design. See the retained mechanism
description in the [PR #1571 report](../benchmarks/native-shared-execution-2026-10-17.md#retained-shared-mechanisms)
and the explicit drops above.

## Prospective target and evidence limits

For the next prospective review, the whole-workload target is at least a 2%
reduction in Full43 engine execution time, using the sum of `engine_execution_seconds`
for one execution of each of the 43 queries. This target does not revise prior
frozen thresholds or waive correctness, repeatability, control-regression,
resource, and complete-result gates.

Several linked historical records report process wall time, complete public-call
time, or other clocks rather than the current engine clock. Those measurements
remain useful in their original scope but are incomparable to an
`engine_execution_seconds` Full43 result and are not converted or added here.
The reviewed historical inventories are finite, and not every linked packet or
archive was audited row by row. This review makes no claim of exhaustive history
coverage and does not treat missing engine-clock evidence as a measured gain.
