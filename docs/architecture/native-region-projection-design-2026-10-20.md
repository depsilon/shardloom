<!-- SPDX-License-Identifier: Apache-2.0 -->

# Deferred native projection payload requests

Status: prospective finite candidate protocol, frozen before its harness,
provider patch, prerequisite observations or timing comparisons. The accepted
source baseline is `603e8ec1`. This is the next stage of conditional row 16,
`PERF-03.regions`, under RFCs 0031/0044. Broader region scheduling, local reduction,
ordered candidates, preparation design and CG-1 through CG-23 remain open.

## Intake and exact candidate

The [count intake](native-region-projection-intake-2026-10-20.md) passes all
32 calls, 1,048,680 complete independent source rows, exact schemas, generation,
natural-boundary and ownership checks. All eight sparse-target calls pass the
frozen count threshold. Their completed operation reads overlap 1,183,392 to
1,742,216 potentially avoidable projected-payload bytes, or 35.61–36.91% of
completed operation bytes. Unique coverage is 788,928 bytes per target call.
Repeated reads are included only in the multiplicity count. These are possible
savings, not actual removed reads or performance evidence.

Do not move `LayoutReader::projection_evaluation` into the post-filter future.
Its current synchronous expression, child-layout and validity errors must remain
visible even if the eventual filter is all false. Refine the intake hypothesis
to defer only the flat projected-payload segment request until its array future
is polled. The existing `split_exec` already awaits the filter before polling
that future. Keep projection tree construction, row-range conversion, metadata
validation, filter evaluation and decode/expression behavior in their existing
locations. A projection directly polled by another consumer retains its normal
result and error behavior.

Vortex-first classification: `use_vortex_native_provider`. Use pinned Vortex
0.85's `FlatReader`, `SegmentSource`, `SegmentFuture`, native split scan and
current source/execution owners. The only runtime candidate is inside the
existing excluded `vortex-layout` provider patch: separate eager filter requests
from lazy projection requests while sharing the existing array decoding logic.
No second region model, split coalescing policy, scheduler, worker pool, cache,
engine, dependency or production environment toggle is admitted. Native inputs,
typed native aggregation and certificates remain authoritative; no Arrow or
external-engine execution fallback is added.

## Fixed fixtures and prerequisite proof

Reuse the eight exact immutable files and complete witnesses from the successful
intake. Check every byte identity before and after each phase. Do not regenerate,
change geometry, or discard a profile based on its observations. Retain the
131,085-row tail, nullable I64 schema, f/p formula, native field layouts and
filter `f >= 100`. The complete public prepared query remains COUNT(*), COUNT(p),
SUM(p), MIN(p), MAX(p), with the same complete independent result and dtype.
Use fresh native sessions at CPU grants 1 and 4 and 64 MiB of native memory.

Before candidate timing, require:

- Complete ordered filtered values and the original per-split batch boundaries
  on all eight files at both grants. Inspect natural boundaries independently of
  grant, including misaligned fields, skewed chunks, NULL zones and the tail.
- Projection-construction errors remain observable under a zero-match filter;
  filter/projection decode and provider errors remain explicit. Direct flat
  projections preserve nullable, signed/unsigned boundary and floating values,
  including signed zero, infinities and NaN payloads where the provider supports
  them. Empty and all-false masks retain their native semantics.
- A delayed first split allows later admitted work to progress while yielding
  original order; cancellation/drop and injected failure drain every admitted
  future and credit. Controlled provider tests prove scheduling/error behavior;
  they are not physical-read or process-memory measurements.
- Public prepared-source checks reject a changed generation and pre-cancellation,
  preserve one prepared source and the requested execution count, forbid fallback
  and Arrow conversion, and return all managed credits after completion. Keep the
  runtime alive through observer drain. Observation failures invalidate the run.

Run the existing relevant native source, cancellation, allocator, aggregate and
file-pruning regression selectors as well as the new exact prerequisite tests.
Freeze the selector list against actual source before the candidate run; zero
matches fail. Run formatting and strict feature-specific Clippy before measurement.

## Actual-read admission

Run two observed complete calls per profile/grant/role: 64 calls, using the same
completed positional-read observer, unchanged limits and physical-range verifier
as the intake. Use separately qualified baseline and candidate executables with
identical measurement source. Retain complete values, field/segment ranges,
preparation versus operation reads, metadata, payload, gaps and multiplicity.
These observer clocks are not performance evidence.

For each of the four sparse profile/grant targets, require the candidate's larger
operation-read total across its two calls to be at most 90% of the baseline's
smaller total and at least 65,536 bytes lower. Also require its larger rejected-p
payload count to be at most 75% of the baseline's smaller rejected-p count. This
gate deliberately handles variable coalescing conservatively. All 64 calls and
the prerequisite checks must be valid. Other profiles remain controls with all
counts reported; actual throughput and memory controls follow below.

If this gate fails, reject the finite payload-deferral mechanism before timing.
Do not select a winning grant, weaken a bound or rerun unchanged. Positive intake
counts alone cannot override absent actual read reduction.

## Frozen performance comparison

Only after actual-read admission, execute two complete independent cohorts A/B.
Each has three paired baseline/candidate repeats of 20 cells. Sixteen cells are
the eight profiles at both grants, each retaining one prepared source for 16
consecutive complete query executions. Four further one-call controls use
`all_true_zoned` and `mixed_zoned` at both grants. Fresh session/source/aggregate
preparation per workflow; no state survives into the next workflow. Each call
returns its complete result. There are 240 measured workflows and 3,120 complete
queries. Ordinary unobserved file reads execute these comparisons.

Alternate role order by cell and repetition; cohort B reverses A's order. Freeze
the exact schedule before either cohort and complete both before reading timing
scores. Each workflow is a fresh process using the ordinary prepared aggregate.
Source hashing and independent validation are outside the measured workflow;
record the complete native workflow from session/source preparation through the
last result and owned-work drain. Record native engine time and serialization for
every call separately; the workflow's engine score is the sum of its 16 or one
engine executions. Keep first/later calls and preparation diagnostic data, all
samples, complete-process/CPU/RSS context and host observations.

In **each** cohort, all four repeated sparse targets must reduce median engine
time by at least 5%, and their geometric-mean complete-workflow ratio must be at
most 0.95. Every one of the 20 cells must keep its median engine and complete-
workflow ratios at most 1.10. Candidate peak managed native bytes in each paired
cell must stay within the larger of 10% or 65,536 bytes above baseline, with no
denied reservations, leaked credits, invalid generation, failed output, or
incomplete drain. Native allocation is not process RSS; retain both separately.
Both cohorts must pass every applicable gate; a single-cohort pass is insufficient.

Use the existing serial workload/storage/process guards, local-only paths,
unique attempts, two build jobs and current optimized portable release features.
Bind the prebuild source closure, actual compiled dependency inputs, toolchain,
executable SHA-256, fixed inputs, schedule, oracle and protocol. Retain failures.
OS cache state is uncontrolled and source hashes may warm the cache. No artificial
pacing, host-state admission, sample substitution or post-result threshold change.
Routine timing reports use engine time only.

## Decision and restoration

Retention needs the complete read and two-cohort gates, then full workspace and
native checks plus affected public/installed-Python and complete query regression
acceptance. A finite rejection removes the candidate and temporary harness,
verifies exact runtime/source restoration and runs applicable fresh checks. Keep
the compact complete results, failed attempts, source patch and portable replay;
retire temporary payloads and obsolete comparison executables after verification.

Even a retained local payload-request optimization does not establish independent
region worker ownership or admit the parked coalesced-job topology. Rows 17–19
retain their specific region/local-reduction/new-topology prerequisites. No broad
Full43, package, production, remote-service or competitive claim follows here.
