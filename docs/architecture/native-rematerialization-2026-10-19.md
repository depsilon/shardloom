<!-- SPDX-License-Identifier: Apache-2.0 -->

# Private projection regeneration — October 19, 2026

Status: **finite candidate rejected**. The four memory targets pass in both
frozen cohorts, but wide-dependency and all-null controls exceed the allowed
engine-time regression in both. The one-derived-column control also fails in
cohort A. The candidate, observer and test harness are archived and removed;
production is restored to the previously accepted source contents.

This is the eighth finite decision in the
[conditional investigation sequence](native-conditional-investigations-2026-10-19.md):
six rejected mechanisms, one retained mechanism and one no-signal attribution,
with 13 first decisions remaining. `PERF-06.rematerialization`, the broader
owners and CG-1 through CG-23 remain open. This result covers the frozen
projection/sort mechanism; it does not reject all regeneration policies.

## Native contract and frozen experiment

The [pre-experiment design](native-rematerialization-design-2026-10-19.md) and
[protocol](../benchmarks/evidence/rematerialization-protocol-778.json) precede
candidate implementation and measurement. The admitted root is a bound
`Sort(Project(ScanSource::Memory))`, with 32,768–65,536 immutable rows, direct
same-name Int64 keys and eight–64 checked Int64 arithmetic outputs. Dynamic,
parameterized, filtered, file, one-shot batch, nested-parent and explicit-spill
routes keep their ordinary execution. The original prepared source remains
pinned in every role.

The candidate evaluates the entire projection in original input/expression
order, preserving eager checked errors before any sorted delivery. It then
drops private derived arrays and retains the complete dependency union in the
existing native sort. Ordered gathering reconstructs each output once per
bounded batch. Recipe metadata, planning headroom, real reconstruction and
escaped output owners use the original shared native memory pool. No published
alias or slice is revoked, no source is replayed, and no extra grant is created.

Vortex-first classification is `implement_shardloom_kernel` using pinned Vortex
0.85.0 arrays and the existing native expression, sort and spill boundaries.
The provider review distinguishes Vortex's one-way `SharedArray` cache and
deferred `ScalarFnArray` from this caller's eager-error and ownership contract.
There is no new dependency, external-engine execution or Arrow execution substrate.

Four targets combine 32,768/65,536 rows with eight/64 derived columns. Nine
controls cover small inputs, one derived output, aliases, constants, derived
keys, Boolean output, wide unused source columns, wide dependencies and all-null
arithmetic. Nullable keys sort stably with NULLS LAST; an original ordinal
witnesses ties without becoming a sort key. Every source/result value, field
order, dtype and nullability is independently reconstructed from the formulas.

The three roles are ordinary retention, existing authorized native spill and
regeneration with spill disabled. All share one source construction, one
preparation, one completed public `for_each_batch` call, a 64-MiB native grant,
one CPU lane, zero provider background workers and 2,048-row output batches.
Spill retains its 1-MiB buffer, 128-MiB temporary quota and two-reader limit;
each target must create actual native runs. Caller-owned fixture/witness bytes
are explicit and do not establish a whole-process memory bound.

The same freshly compiled release executable runs both cohorts. A uses
retain/spill/regenerate; B reverses roles and cell order. Three samples per
cell/role/cohort plus six retained, excluded warmups make 240 comparison
workflows. All 117 release preflight workflows, including empty and 17-row
overrides, pass before measurement. Both cohorts finish before comparison.
No sample, target, threshold or role is changed after inspecting results.

Each target must lower median complete native-pool peak by at least 20% and
131,072 bytes in each cohort. Every cell must avoid an engine or complete-workflow
regression exceeding both 5% and 1 ms against retention; peak may not increase
by more than 65,536 bytes. Targets must also pass the throughput gate against
actual spill. Source preparation, full consumption and owner destruction remain
in the separate complete-workflow control. Engine time excludes measured
synchronous consumer work under the existing prepared-execution clock contract.

## Decision and limits

All four target memory gates pass in both cohorts. Their median peaks are
identical across cohorts:

| Rows / derived columns | Retention peak bytes | Regeneration peak bytes | Reduction |
| --- | ---: | ---: | ---: |
| 32,768 / 8 | 4,282,464 | 3,151,264 | 26.41% |
| 32,768 / 64 | 22,332,384 | 7,897,376 | 64.64% |
| 65,536 / 8 | 7,916,784 | 4,310,592 | 45.55% |
| 65,536 / 64 | 41,334,896 | 9,056,704 | 78.09% |

The binding engine-time failures are below; each value is the median of the
three frozen samples. Complete-workflow controls also fail for the same cells
and cohorts, with their distinct clocks retained in the
[decision record](../benchmarks/evidence/rematerialization-release-779-a1-decision.json).

| Control / cohort | Retention engine ms | Regeneration engine ms | Increase |
| --- | ---: | ---: | ---: |
| Wide dependencies / A | 436.257 | 599.317 | 37.38% |
| Wide dependencies / B | 443.592 | 599.587 | 35.17% |
| All-null / A | 70.286 | 78.641 | 11.89% |
| All-null / B | 71.093 | 78.923 | 11.01% |
| One derived output / A | 53.013 | 56.135 | 5.89% |

The one-output control does not activate regeneration. Its cohort A observation
remains a failed frozen control without attributing the difference to candidate
activation or a presumed host cause; cohort B passes. Wide-dependency and
all-null failures independently reject the mechanism in both cohorts. All
peak-growth controls and all target comparisons against actual spill pass.
Target gains do not override control failures. There is no accepted speedup,
Full43 improvement or general memory-policy claim.

## Verification and preserved attempts

The final debug source passes 12 focused lifecycle/admission tests and all 81
smoke workflows. The freshly compiled release source passes the same 12 tests,
117 preflight and 240 comparison workflows. Independent checks validate complete
source/output/schema bytes, activation, full dependencies, actual private-credit
release, bounded reconstruction, source counters, certificates and final zero
reservations/owned spill cleanup. Debug workflows receive no comparison credit.
Fifteen oracle/checker tests include 19 mutations of actual native records.

Focused tests cover optimization and public source bounds, forbidden source/plan
shapes, nulls/ties, late overflow before any output, metadata/headroom/allocation
denial, cancellation, failing consumers, fresh retry, and escaped clones/slices.
The last-owner test keeps validity storage credited until its final owner drops.

The [pre-measurement repair record](../benchmarks/evidence/rematerialization-validation-repairs-779.json)
preserves three failures: SHA-256 digest formatting at compile time; a test
expecting successful execution above the public 65,536-row memory-source bound;
and a test expecting zero credits while still holding a validity owner. A
separate compile-only attempt is retained after review corrected its one-shot
producer batch shape before execution. Only test/harness code changes across
these repairs; candidate runtime and observation code remain byte-identical.
None contributes a comparative measurement. The repair record's pending-release
status is historical and is superseded by the completed release evidence.

## Portable evidence and exact restoration

The [portable packet](../benchmarks/evidence/rematerialization-portable-779.json)
preserves 2,177 regular members: accepted source base, all six frozen source
versions, patches/additional files, helpers, build/executable identities,
failed attempts and complete debug/release streams and witnesses. Compiled
executables stay outside Git; dependencies are pinned rather than vendored by
this packet. Replay requires fresh local paths and the original guards.

[Independent reopening](../benchmarks/evidence/rematerialization-packet-verification-779.json)
checks every archived member against its original bytes, reconstructs each
1,904/1,905-file source version from the 1,899-file accepted base, validates
every helper identity, and recomputes all 438 successful workflows from 69
complete content witnesses. It checks 30 guarded receipts and independently
reproduces the rejection before any candidate removal.

The [restoration receipt](../benchmarks/evidence/rematerialization-restoration-780.json)
proves removal of three tracked-file changes and six added Rust files. All
1,899 Rust/Cargo inputs match accepted commit `0b52edf4`; all 2,217 previously
accepted runtime/Python/test/harness source contents remain identical. Formatting
passes. Prior workspace/native/CLI, installed Python, Public39, Full43 and
modular acceptance applies to those unchanged production bytes; those suites
are not represented as fresh runs or acceptance of the rejected candidate.

The next investigation is `PERF-07.output_windows`: establish a measured
complete-workflow queue bottleneck before a separately designed byte-credited
overlap candidate. Slow/failing consumers, retained output lifetime, demand,
cancellation and provisional completion remain mandatory.
