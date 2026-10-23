<!-- SPDX-License-Identifier: Apache-2.0 -->

# PGO prerequisite decision — October 20, 2026

Historical October 20 status: **missing current cost attribution; new training is not admitted**.
This is row 20's evidenced prerequisite decision under `PERF-13.pgo`, as allowed
by its [verification contract](native-roadmap-verification-contracts-2026-10-18.md#perf-13).
The substep and broader owner remain open. Portable ThinLTO remains the release
default; this review changes no compiler profile or runtime behavior.

The [October 22 current-baseline diagnostic](../benchmarks/native-current-attribution-2026-10-22.md)
now supplies source-bound numeric and text preparation observations, complete
raw records and semantic replay controls. The subsequent
[scalar-region rejection](../benchmarks/native-scalar-regions-2026-10-22.md)
settles that finite decision and restores every Rust/Cargo source identity to
the attributed baseline. The attribution therefore remains source-valid for
PGO intake. A separate frozen training, held-out, package and break-even protocol
is still required before training; no new PGO result is claimed here.

The latest October 21 maintainer decision schedules current-source profiling
after required competitive gaps and before the four pending investigations.
PGO is fourth, after ordered candidates, preparation design and topology
comparison, and still requires a material cost signal before training. Optional
breadth is last and may never be implemented. See the
[canonical sequence](phased-execution-plan.md#competitive-prerequisites-and-four-investigations).
Use the resulting source identity for attribution and refresh it when an
accepted change affects the selected paths. This adds no roadmap ID and does
not change historical evidence or promote a compiler profile.

## What the existing records establish

The [September compiler screen](compiler-profile-screen-2026-09-27.md) did run a
matched PGO experiment, including training, complete queries, ingestion, held-out
operators and pressure controls. It did not promote that tested configuration
because its complete workload tradeoff was unfavorable. Training selected fewer
native paths than the evaluation workload. Its missing-profile diagnostics and
coverage audit are retained; neither isolates the cause of the timing changes.
That historical decision remains binding for its tested corpus/profile. It does
not rule out all future PGO corpora.

The newer [October 14 diagnostic](../benchmarks/native-performance-gap-2026-10-14.md)
contains absolute hardware instruction and cycle counts. Review of its raw
packet verifies all 36 Q3/Q23/Q29 records: three repetitions, both orders and
both roles, plus all twelve reported median cells. The encompassing diagnostic
has 48 calls across four queries; Q34 is outside this instruction-statement
extraction. The raw values are not merely relative percentages.

Those counters are one post-exit Darwin `proc_pid_rusage` snapshot per native
process. They cover startup, execution, complete output and exit. They are not
operator/function/dispatch-site counts, sampled stacks or a counter time series.
The query route labels identify selected paths but do not attribute the total
instruction count to any of them. The two executables also predate the current
source, and the original binaries/input have been retired with their identities
preserved. These observations do not identify material remaining dispatch cost
in source `730f793d4509ef09a1791e2d0a5aa45de68260e4`.

The initial summary-only inventory understated this evidence. This review
corrects that statement: **absolute counts exist; current dispatch attribution
is missing**. Whole-process CPU accounting and historical process clocks retain
their original definitions and are not relabeled as current engine time.

## Tooling and validation

Read-only local inspection reports Rust 1.99.0 with LLVM 23.1.1 and its bundled
`llvm-profdata` reporting `23.1.1-rust-1.99.0-stable`. Both exact executable
identities and complete version output are recorded. Version agreement does not
replace a fresh instrument/run/merge/profile-use compatibility check or prove
full-workspace compatibility. The older tiny smoke used a different toolchain.

Both required orchestration regressions pass:

- `PgoTests.test_failed_training_never_merges_or_builds_profile_use`
- `PgoTests.test_three_frozen_binaries_flags_and_fresh_merge_inputs`

These tests use mocked compilation/profile tools. They prove failed-training
refusal and frozen artifact/input handling; they do not run or certify PGO.
No new instrumented build, training, benchmark, profile-use build or package
installation occurs in this review. There is no new PGO performance result.

## Reopening condition

First provide current-production cost attribution tied to selected native
callers, operators and types, with valid counter scope, negative controls and
complete raw records. The inference that PGO might improve an expensive query
is insufficient to select training targets or justify a new comparison.

Then freeze representative selected-path/type training and genuinely held-out
evaluation, without training on evaluation results. Bind fresh matched control,
instrumented and profile-use builds to the actual toolchain, source, flags and
profiles. Require complete query/ingest and pressure controls, portable-package
correctness and measured compilation/training/transfer break-even. Cached build
observations from the previous screen do not establish that break-even.

Accepted changes can alter those paths; attribute the actual source identity
before reopening. This is a specific missing prerequisite, not a compiler/tool
availability failure or a newly rejected PGO candidate. Optional breadth need
not be implemented before this scheduled investigation.

## Evidence

The [acceptance record](../benchmarks/evidence/conditional-prerequisites-2026-10-20/perf-13-acceptance.json)
preserves the source hashes, full extracted counter inventory, original archive
identities, tooling output and both test receipts/logs. Its
[verifier](../benchmarks/evidence/conditional-prerequisites-2026-10-20/verify_conditional_prerequisites_832.py)
reopens all 36 raw counter members in the existing portable diagnostic archive,
checks their complete key coverage and equality with the encompassing result,
and recomputes all twelve medians. It also verifies the historical screen bundle
identities, current source references and mocked test results. It does not claim
new physical-input validation, current dispatch attribution or PGO execution.
