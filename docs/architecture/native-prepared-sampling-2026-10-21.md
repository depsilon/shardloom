<!-- SPDX-License-Identifier: Apache-2.0 -->

# Prepared weighted sampling: finite reuse protocol

Status: prospective design and comparison protocol, before candidate
implementation or timing. This is the remaining authorized
[sampling follow-up](native-investigation-followups-2026-10-20.md#reusable-weighted-sampling-preparation)
under `PERF-10.learned_indexes`, `PERF-02.physical_cache` and PERF-03/06,
governed by RFC 0044. It adds no roadmap ID. The 14 owners, 58 substeps,
37 required substeps, conditional-first order and CG-1 through CG-23 remain.
Required breadth resumes after this finite decision.

## Evidence and different mechanism

The [prior intake](native-learned-index-intake-2026-10-20.md) establishes real
ordered-search work in the resident sampler. The
[prior candidate](native-learned-index-decision-2026-10-20.md) calibrated a
bounded predictor on every execution. Its target gains did not satisfy the
complete retention contract: few-draw and tiny controls failed cohort A while
B passed. Their cause was not isolated. That implementation remains unretained;
this protocol neither replaces its cohorts nor treats narrower admission as
a demonstrated repair.

This candidate amortizes calibration across calls on one immutable prepared
plan. Only scalar prediction metadata persists. Every call reconstructs and
validates the ordinary credited population, performs the actual seeded draws,
and delivers complete native output through the existing consumer. There is
no retained population, CDF copy, borrowed population pointer or cached answer.

## Native provider and identity

The pinned provider remains Vortex 0.85.0. Reviewed sources are
`vendor/vortex-array/src/search_sorted/{mod,primitive}.rs`,
`resident_memory_source.rs`, `resident_memory_owned_intake.rs`,
`resident_memory_arrow.rs`, `local_primitive_prepared_relational.rs`,
`local_primitive_relational_bind.rs`, `local_primitive_relational_dynamic.rs`,
`local_primitive_prepared_unary.rs`, `local_primitive_unary_sample.rs` and
`shardloom-exec/src/live_memory.rs`.

Vortex supplies native immutable arrays and exact sorted search. The current
sampler's CDF is embedded in its already credited candidate records, rather
than a separate Vortex array. Vortex's searched provider surfaces do not supply
this plan-bound calibration owner. Classification: `implement_shardloom_kernel`
for the small original prediction/reuse helper; reuse current native source,
population, payload, consumer, allocation and certificate owners. No new
dependency, unsafe code, public index option or execution provider is admitted.
Residual evaluation remains ShardLoom-native, with `fallback_attempted=false`
and `external_engine_invoked=false`.

Admit only a statically bound root sampling operation directly over one native
immutable memory scan, weighted replacement with a Float64 weight column,
no filter/residual predicate, no source limit and no intervening operator.
The normal projected source fields and their bound order remain unchanged.
Require at least 4,096 source rows, 4,096 draws and one draw per four source
rows. File inputs, streaming/batch inputs, dynamic lowering, parameterized
relations, child sampling nodes and explicit spill plans do not retain models.
They keep their existing native behavior.

The bound object exclusively owns the cache and the exact immutable source
projection, request, schema, weight index, source order and numerical semantics
for its entire lifetime. `ResidentMemorySource` copies borrowed slice and Arrow
buffers into admitted storage, consumes owned native columns, or shares already
owned immutable native results. No arbitrary external mutable array is admitted
through those constructors. This retained ownership and inaccessible bound-plan
mutation establish identity; a source name, address reused after drop, count,
total weight or content hash alone does not. Independently prepared plans have
independent owners even when counts/totals match. Native resource changes in the
Python prepared-plan owner already discard the prepared operation.

## Scalar owner, pressure and failure contract

One optional owner belongs to the admitted bound operation. Reserve actual
owner, model and added pointer metadata before allocation, with at most
**128 additional credited bytes** in total versus ordinary execution. Account
for the owner shell separately from the evictable model. Preserve credits while
their storage remains alive; release the shell at prepared teardown and the
model on eviction, failure or teardown. Allocator bookkeeping and process RSS
are not represented by these logical metadata credits.

The existing pool has reservations, not a general cache-reclaim callback.
This experiment therefore uses an explicit local policy: at execution entry
and before calibration/reuse, discard the model and disable model use for that
call when free shared credit is below 1 MiB. Do not create an optional owner
without that headroom. The immutable owner shell, if already admitted, remains
charged until prepared teardown. A later ample-memory call can recalibrate.
Mid-operation denial remains a deterministic error and clears the model; no
hidden retry, query replay, new spill or pool-wide automatic eviction is added.
A racing reservation denial preserves the existing error.

Only a completely inspected population can produce an accepted model. Its
scalar record contains population length, exact total-weight bits and either
the calibrated radius or a calibration-refused sentinel. A refusal may persist
for the same immutable plan, avoiding repeated partial calibration. Length and
total are defensive consistency checks on reuse, not the identity proof. A
mismatch clears the record and fails deterministically.

No reference into an execution-local population may escape. A new record is
provisional during native execution and reusable only after that execution
successfully finishes. Cancellation, consumer error, failed collection/writer
completion, reservation denial and unwind clear provisional and prior records.
Outer completion boundaries must also clear records if final native ownership,
serialization or writer completion fails after the inner kernel succeeds.
Tests must exercise these boundaries, not merely the calibration helper.

Prepared native execution is not made `Sync`. Existing public Python calls
serialize the prepared object with its mutex; Rust concurrent callers must use
the same exclusive-owner discipline. Reentrant consumers must not observe a
provisional record: disable reuse/calibration for the overlapping calls and
clear the record. No lock may be held across consumer callbacks, and no global
or cross-session cache is introduced.

## Exact prediction and correction

Retain the prior mathematical mechanism and its exact arithmetic order:

```text
p(t) = min(n-1, floor((t / total) * float(n-1)))
e = max over all i of abs(p(c[i]) - i)
radius = e + 1
```

Calibrate over every current CDF entry, poll cancellation at least every 256
entries and refuse immediately when an error exceeds eight. Division precedes
multiplication; no reciprocal substitution is allowed. Correct with the
original strict `< threshold` lower-bound search inside the clipped inclusive
radius, then preserve the original final clamp. Nonfinite or out-of-domain
thresholds use ordinary exact search. Refused/ineligible/pressure-bypassed
models also use that ordinary native algorithm.

For lower bound k, monotonic prediction lies between the predictions at
`c[k-1]` and `c[k]`, so the calibrated endpoint errors bound the distance by
`e+1`. `p(0)=0` covers the first position. Every rounded duplicate participates
in calibration, preserving the first equal entry. Reuse does not weaken this
argument because the same immutable source, operation and ordered cumulative
arithmetic reconstruct the same CDF.

## Correctness and activation before timing

Check every equality, adjacent representable value and gap against ordinary
exact search, including empty/singleton cases, an admitted large distribution
with rounded duplicates, tiny and large finite totals, threshold underflow,
refusal and final clamp. Check every seeded result and its logical schema
against the independent Python reference used by the prior intake.

Add public native lifecycle tests for fresh/repeated calls; changed values with
equal counts/totals; changed weight, seed, projection, filter and plan shape;
cross-plan/source isolation; cancellation before/during calibration and output;
consumer failure, failed final collection, unwind, pressure eviction and
successful recalibration; prepared teardown and serialized concurrent calls.
Verify file/stream/dynamic/child/spill exclusions. Require complete output,
unchanged certificates, actual calibration/reuse counts, exact retained credits
and zero credits after final owners drop. Invalid source weights continue to
fail through the existing semantics before a model can be published.

Before measured cohorts, run one observed preflight workflow per role and
comparison cell. Uniform/periodic candidate workflows calibrate once on their
first call and reuse the accepted model on every later call, with radii one
and five respectively. Shifted/hot-tail workflows calibrate and refuse once,
then reuse only that refusal. Few-draw, tiny, unweighted and rounded-tie
workflows never create an owner or calibrate. The baseline has no model.
Each call still builds one complete population. Record actual calibration
visits, searches, correction comparisons, reuse/refusal and credited bytes.
Do not infer activation from a role label or faster timing.

## Frozen workloads, cohorts and gates

Use all eight exact input definitions in the prior intake, in its table order:
uniform, periodic, shifted, hot_tail, few_draws, tiny, unweighted, rounded_ties.
Preserve population/draw counts, exact Float64 bits, nullable weight schema,
source order, seed seven, replacement, one projected Int64 ID, one CPU and
64-MiB grant. There is no filter. Keep its independent complete input and
output reference logic unchanged; bind its bytes before execution.

Cross every shape with **1, 4 and 16 calls** on one freshly constructed prepared
object, yielding 24 cells. Every workflow creates and destroys that owner.
Use ordinary public `prepare_relational_with_schema` and `collect_jsonl` in
both roles, byte-identical harness/reference code and fresh separately bound
ordinary/candidate builds. Role labels must never choose runtime behavior.
Observation is disabled symmetrically for timing.

Run cohorts A and B, each with one unscored warmup and seven scored samples
per cell and role. A visits shapes in table order, then call counts 1/4/16.
B reverses that complete 24-cell order. For each cell, sample indices are
0 through 7; A uses baseline/candidate on even indices and candidate/baseline
on odd ones, while B uses the opposite order. Run all 768 workflows and
5,376 calls sequentially; 672 workflows and 4,704 calls are scored. The
48 observed preflight workflows are additional and unscored.

In each cohort/cell, score the median of seven complete workflow engine totals
per role, each total the sum of every call's engine clock. The four repeated
targets (uniform/periodic at 4/16 calls) must each have candidate/baseline
engine ratio **at most 0.98 in both cohorts**. Every one of the 24 cells,
including the single-call and formerly failing few-draw/tiny controls, must
also have engine and complete-workflow ratios **at most 1.05** in both cohorts.
First-call and later-call clocks remain separately inspectable; they cannot
replace the complete-workflow score. Report the measured 1/4/16-call behavior
without extrapolating an unmeasured break-even count.

Complete workflow includes native source construction, preparation, all query
calls, serialization, result release and prepared/source teardown. Fixture and
reference generation, hashing, validation and evidence writes remain outside
those clocks. Routine messages report engine time only; other clocks and
host/process observations stay in diagnostic evidence.

Every complete result, schema, source witness, ordering and certificate must
pass. No denied reservation is allowed in this comparison; each paired peak
must stay within the baseline plus 128 bytes, and complete teardown must leave
zero credits. All gates are conjunctive. Preserve both cohorts and all failed
attempts. Do not replace a cohort, relax a bound, discard a control or rerun an
unchanged failure. A one-cohort failure means not retained, without claiming
a repeated regression.

Bind complete local compiler inputs, locked provider, harness, protocol,
reference, features, compiler and executable identities before invocation.
Use the existing unsynced-storage ceilings, process cleanup, competing-native-
work checks and exclusive UAT lock. Qualify copied executables under names the
unchanged guard recognizes. Preserve and independently replay full evidence
before retiring temporary payloads under the current retention policy.

Retention additionally requires ordinary public selection, relevant lifecycle
and sampling coverage through native/CLI/installed Python, strict formatting
and Clippy, full workspace/native tests and applicable PERF-12 public, Full43
and format acceptance. A runtime integration change must meet this same frozen
comparison. If not retained, remove experiment runtime/observation code, retain
compact decision evidence and verify restoration. A finite result closes this
follow-up only, not broader sampling, pressure, platform or competitive gates.
