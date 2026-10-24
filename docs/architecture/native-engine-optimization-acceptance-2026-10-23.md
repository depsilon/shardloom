<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native engine optimization acceptance

Status: October 23, 2026 protocol with subsequent candidate decisions below.
Recorded benefit gates do not replace the final coherent validation. The
baseline is PR #1593's repaired runtime, `407c1e80`. The maintainer
requested all six candidates from the [source review](native-engine-optimization-review-2026-10-23.md),
with PR #1593 integrated first. The repaired PR passed all 43 hosted checks and
merged as `06653efb`. Candidate work remains on a separate local branch.

This is engine work under the maintainer's explicit review direction. It does not
resume the paused optional breadth queue, close a competitive gate, or change a
released package. The existing implementation phase and CG-1 through CG-23 remain
as recorded in the [phase plan](phased-execution-plan.md). In particular, CG-5 and
CG-6 still govern broader correctness and performance claims.

## Common contract

The unit is the existing native relational executor, shared by SQL, DataFrame,
typed plans, embedded workflows and CLI. Vortex remains the native input/output
substrate. Existing arrays, dictionaries, validity masks, credited buffers and
native comparison/hash owners are reused. There is no external engine execution,
new array format, query-name matching, cross-query cache or implicit spill.

The Vortex-first check finds the needed domain/array and mask providers already
inside the approved adapter: `VarBinViewArray`, dictionary domains, original-width
numeric owners, `Mask`, `null_as_false`, take and native scalar execution. The
optimization boundary is how their existing ShardLoom consumers prepare, retain
and consume state. No Vortex query-engine integration is needed. For finite sort
demand, the current payload gather combines validation and copying; there is no
existing validation-only path with the same ordering contract. That candidate
must pass its semantic feasibility gate before a different gather is retained.

Metadata-first execution and dynamic resource admission remain authoritative.
These candidates remove repeated work inside admitted operators; they do not
introduce a new PulseWeave or capillary scheduler. Existing batch boundaries,
state grants, cancellation, source-generation checks, certificates and output
ownership remain in force. Timing uses engine-owned execution; preparation,
serialization and cleanup are recorded separately in diagnostic evidence.

## Finite candidate decisions

| Candidate | Change to evaluate | Required mechanism evidence | Primary target | Unchanged-path control |
| --- | --- | --- | --- | --- |
| Value preparation | Request validated values without unused string/binary hash state; hash users keep their current owner | No hash slots or bytes for value-only domains; hash users still reserve exact domain slots and compare complete values | String expression and string extrema, plain and dictionary correctness | Genuine group/distinct keys and numeric expressions |
| Aggregate reuse | Reuse adjacent compatible column owners; represent scalar group ordinals implicitly; add ungrouped COUNT batches with checked arithmetic | Fewer owner preparations, no new overlapping column-owner lifetime, no row-sized scalar ordinal buffer | Repeated MIN/MAX/COUNT and SUM/AVG over the same source; composed COUNT(*) | Interleaved unique columns, independent distinct measures, nullable/nested/decimal values |
| Sort locations | Resolve the two source row locations once per comparison | Reduced location searches without a second row index | Multikey sort over many uneven batches, including tied leading keys | One-batch sorting, NULL placement, stable ties and nested/decimal keys |
| Finite sort output | Carry immediate Limit demand only if all existing validation can be preserved | Fewer output copies with complete validation of omitted keys and payloads | Wide sort output with a small offset/count | Full output, zero limit, computed projections, invalid omitted payloads and spill |
| Predicate masks | Compile the existing finite pure recipe once per filter execution and consume a native mask | One recipe compilation per execution, no Boolean-value expansion for the admitted filter | Repeated batches with nullable compound I64 comparisons | Projected three-valued Booleans and unadmitted/lazy expressions |
| Existence joins | Retain unique right keys for keyed left semi/anti joins without a residual condition | Less retained right state; no duplicate links or unused payload; exact collision equality | Duplicate-heavy, wide right inputs | Unique right keys, NULLs, residual conditions, other joins and explicit spill |

Value preparation and aggregate reuse share one ownership boundary but receive
separate implementation checkpoints. The sort locator follows without changing
output demand. Predicate and existence strategies remain separately admitted.
Each candidate has a recorded retain, revise or drop decision; “work through all
six” does not authorize keeping a correctness or cost regression.

The maintainer's October 23 follow-up requests one isolated Full43 after each
optimization. A subsequent instruction requested the current combined state
instead of reconstructing earlier checkpoints. That pass at `cafa1b0f` stopped
at Q28 because the SQL spelling selected Unicode character length while the
unchanged independent benchmark reference requires UTF-8 byte length. Q1–Q27
passed; Q29–Q43 did not execute, and the failed pass has no Full43 total.

The latest instruction requests a Full43 after merging PR #1593. Run the merged
runtime at `06653efb` first, with the explicitly recorded benchmark dialect
correction described in the [benchmarking skill](../skills/benchmarking.md).
Preserve the combined candidate and its failed observation. Future implemented
checkpoints receive an isolated pass after focused validation; candidates dropped
before runtime implementation do not. Do not add duplicate end-of-batch passes.
These single observations complement the frozen cost screen below and do not
independently isolate repeatable speedups.

## First finite sort-output feasibility decision

Drop the immediate-Limit output-copy candidate. Existing native sort gathering
validates payloads in sorted output order while copying each output batch. The
regression in `native_relational_sort_tests.rs` emits the first valid row and
then rejects a nonfinite non-key payload in the next row, even when a consumer
already has enough rows. Skipping that second gather hides a required failure.
Keys are independently validated before sorting, including later keys that an
earlier distinct key would prevent the comparator from observing. Existing
streamed-ordering tests also retain late input failure and zero-limit behavior.
There is no admitted validation-only gather with identical failure order and
resource behavior, so a second validator is outside this bounded candidate.

## Workloads and scoring

The cost harness uses the real prepared relational execution API with complete
results and schemas. The main fixtures have 65,536 rows, deterministic values,
explicit NULL patterns, a 128-MiB execution grant and 64-MiB source/result bounds.
String values are 128 bytes for value/reuse cases, sort keys are 64 bytes, and
wide sort/join payloads are 256 bytes. The public 8-MiB JSON collection ceiling
is unchanged. Smaller 257-row controls expose fixed preparation costs. Batch
cases use deterministic uneven partitions with empty batches interspersed; the
partition list and every input/schema/expected-result identity are retained.
Dictionary-domain correctness is checked directly against plain-value results;
a claim about dictionary performance requires an actual dictionary cost cell.

The exact plans, generators and independent expected schemas are in the
[cost harness](../../shardloom-vortex/src/local_primitive_relational_engine_cost_tests.rs)
and its [data/reference definitions](../../shardloom-vortex/src/local_primitive_relational_engine_cost_data.rs).
Protocol `native-engine-cost-v1` passed all seventeen small preflights and the
seven large value/aggregate cases, then correctly failed full-sort collection
at the existing 8-MiB ceiling. It produced no scored release comparison. Protocol
`native-engine-cost-v2` narrows sort keys from 128 to 64 bytes to fit that public
boundary; it preserves the failed preflight and all other fixture definitions.

| Family | 65,536-row target cases | 65,536-row controls | 257-row controls |
| --- | --- | --- | --- |
| Value preparation and aggregate reuse | `value_text`, `aggregate_text`, `aggregate_numeric`, `aggregate_count` | `value_numeric`, `aggregate_interleaved`, `aggregate_distinct` | All seven value/aggregate cases |
| Sort locations | `sort_many` | `sort_limit` | `sort_many`, `sort_single`, `sort_limit` |
| Finite sort demand, if semantically admitted | `sort_limit` | `sort_many` | `sort_limit`, `sort_single` |
| Predicate masks | `predicate_filter` | `predicate_projected`, `predicate_single` | All three predicate cases |
| Existence joins | `join_semi_duplicate`, `join_anti_duplicate` | `join_semi_unique`, `join_anti_unique` | All four join cases |

These are shared complete operations. Improvements from the value owner may
also affect aggregate or sort targets; the report must identify that overlap
instead of attributing an entire combined speedup to each individual change.

Workload definitions, their source, and scoring are frozen before the first
baseline execution. A harness defect invalidates the affected comparison and
requires a new explicitly versioned protocol; original results remain preserved.
The baseline and candidate use the same harness, build features, optimization
profile, input identities, grants and expected results. Debug-only observation
hooks provide preparation/hash/location counts. They are compiled out of release
measurements and production artifacts.

For each implemented candidate family, run exactly three paired release
observations of each declared target and control, with pair orders baseline /
candidate, candidate / baseline, baseline / candidate. These repetitions decide
whether to retain that candidate; they are not routine Full43 repeats. There is
no cache manipulation, artificial pacing, extra warm-up or selective retry. All
observations, failures and omitted cells remain in the result record.

Retain a time-oriented candidate only if its target geometric-mean engine-time
ratio is at most 0.95 and complete semantics/resource checks pass. A target or
control must not increase its median engine time by more than the larger of
10% or 100 microseconds; the absolute allowance prevents sub-millisecond control
noise from deciding the entire change. Value-state and existence-state candidates may
instead pass by reducing the relevant peak credited state by at least 20%, with
the same timing ceiling. A lower reservation or call count alone supports
only that mechanism claim. No full-workload speedup follows from it. A result
that misses the declared threshold is revised once for a specific diagnosed
cause or dropped; it does not acquire a weaker threshold after measurement.

## Correctness and completion

Focused tests cover empty/all-null inputs, nested parent NULLs, full-domain UTF-8
validation, invalid dictionary codes, numeric and decimal boundaries, checked
overflow, cancellation, late source errors, denied grants, output ownership and
cleanup. Sort tests include invalid non-key values beyond a limit and preserve
ordinary versus streaming zero-limit behavior. Join tests force hash collisions
and preserve the distinction between a NULL key and a non-NULL nested key with
NULL children. Existing spill paths remain separately tested.

After the coherent implementation, run formatter, default/native workspace lint
and tests, all-features compilation and affected public-surface conformance.
Run the baseline and per-optimization isolated standard Full43 passes described
above: exactly one execution per query in each pass, all other task-owned work
idle, unchanged storage/deadline/concurrency/result checks, full hashes and
inventories outside query execution. These are regression observations, not
comprehensive measurement of the composed workloads. Review the final diff
adversarially and preserve every candidate decision and remaining proof gap.

## Subsequent shared-operator feedback intake

The maintainer requested a current Full43 when useful, followed by the attached
review of main at `06653efb`. The scan-preparation candidate received that pass;
its complete observation and subsequent drop are in the
[hotspot decision](native-query-hotspots-2026-10-23.md). That work did not replace
the six candidates above. Source work resumes from the preserved combined
candidate at `ef57dfda`; no earlier implementation is silently treated as accepted.

| Feedback | Current disposition and remaining proof |
| --- | --- |
| Purpose-specific value/hash preparation and compatible measure views | Already implemented in the preserved candidate, with full-domain validation, hash-consumer controls and adjacent compatible reuse. Apply the frozen composed cost screen before accepting it. |
| Resolve retained rows once; replace discarded sort hashes with validation | Already implemented with the value owner and comparator. Preserve stable global ordinals and all-key validation; apply the existing many-batch sort target and controls. |
| Finite-demand composed sorting | Revisit the dropped first attempt only with a different, explicit validation-preserving design. The prior failure remains. Existing streaming ordering input detachment already validates every payload; do not repeat that work or infer equivalent validation for ordinary resident input. Reuse existing ordering, gather and value validation machinery. |
| Prepare expressions once and share pure batch-local work | The current filter recipe is compiled once per execution, while ordinary expression evaluation still recompiles. Extend preparation to retained positive and negative admission decisions; confine reuse to exact expression/input/type/selection identity and preserve lazy branches. |
| Reusable optimized aggregate consumer | The composed consumer already owns binding, grouping, state and finalization. Reuse a narrow native numeric reduction algorithm inside that boundary; do not move the entire scan-specific grouped-state object or change floating addition order. |
| Cross-surface timing spread | Inspect the historical reports' actual provider, worker, input and work evidence. Missing typed strategy fields are a proof gap; the observations do not establish Python overhead or current-runtime cost. |
| Repeated rejected mechanisms and completed routing fixes | Already addressed or rejected. Do not restart format dispatch, provider selection, physical caches, compression, region scheduling or PGO under this feedback. |

The expert comparator remains an encoded-columnar engine maintainer reviewing
ordered errors, cancellation, native ownership, exact ties, source validation
and complete-operation cost. New extensions need their own prospective baseline,
targets/controls and retain/drop decision. Do not change the original cost
fixtures or gates after observing their results. Changes to active supervision
are separately recorded: the first composed comparison uses only current-call
file sizes and bounded witness-file sizes while executing, with full recursive
inventories before or after queries. Both roles use the same supervisor.

### Preserved combined candidate: finite benefit decision

At `5c702829`, all 198 calls pass the original v2 screen: 33 workload/scale cells,
three alternating baseline/candidate pairs per cell. Complete values, schemas,
input identities, partitions and producer consumption agree. Every call releases
its credited state, stays within the unchanged grant and has no denied
reservation. Every cell meets its original engine-time ceiling.

| Large workload | Baseline median engine seconds | Candidate median engine seconds |
| --- | ---: | ---: |
| Text expression | 0.003177414 | 0.003000045 |
| Repeated text aggregates | 0.015104459 | 0.010340999 |
| Repeated numeric aggregates | 0.007793417 | 0.007895374 |
| Scalar COUNT(*) | 0.000582167 | 0.000372958 |
| Many-batch multikey sort | 0.236651626 | 0.195323916 |
| Compound predicate filter | 0.005273674 | 0.004644287 |

The declared target-family geometric means satisfy the 0.95 retention bound
for value preparation, aggregate reuse, sort locations and predicate masks.
Retain the combined implementation subject to final broad validation and its
isolated Full43 checkpoint. Numeric aggregate execution itself does not improve
in this comparison; its median remains inside the control ceiling. Shared value
preparation also affects aggregate and sort costs, so this combined checkpoint
does not isolate each edit's unique contribution. No 20% whole-workload peak-state
claim is made. Finite sort demand and existence-join state are not implemented
by this checkpoint.

The local `native-engine-optimization-20261023/composed-comparison-1015` packet
preserves the fixed schedule, each measured call, complete result/schema
witnesses, source/build identities and `decision.json`. The immutable baseline
and candidate use the same v2 harness, toolchain, features and release profile.
The following feedback extensions must compare against this retained operator
checkpoint rather than crediting its existing improvements again.

The isolated Full43 at `f896294f` passes all 43 queries and 381 expected rows,
with 49.682630379 seconds of engine execution. Full input/source identities and
cleanup pass. This is regression evidence for the retained combined checkpoint;
the composed screen above supplies its workload-specific benefit evidence.

### Revised finite-demand sort contract

The later feedback explicitly asks for a validation-preserving extension of
finite demand. The earlier immediate-copy omission remains dropped. This new
candidate uses the existing result builder's validation and the existing
ordering input's validation contract rather than deleting late checks.

An immediate bound Limit may provide `offset + count` to its child Sort.
Do not push that demand through a projection, filter or other operator that
may observe discarded rows. Preserve the existing ordinary zero-limit behavior
and complete finite streaming-source consumption. An explicit spill policy
continues through the existing full ordering path.

For ordinary resident input, retain full ordering. Extend the shared scalar
result builder to construct only a requested prefix while visiting every value
in the original sorted batch/column order, using the same type, null, finite,
range and variable-payload checks. Admit this reduced copying only for the
flat payload family it can validate; other payloads keep their full gather.
No independent validator or decoded row representation is introduced.

Streaming ordering already detaches and validates every input payload before
retention. Only with that established validation and the normal ValidateValues
policy may the sort omit subsequent copies and select a bounded ordinal prefix.
Reuse the existing fallible/cancellable heap ordering kernel and its comparator,
including stable source ordinals; do not add another value-comparison system.
The primitive sort block's owned StatValue candidates and infallible selection
cannot replace the composed nested/decimal comparator. Extend its existing
ordinal-ordering owner instead. Retain all input; this is not bounded total
query memory. Use bounded selection only for K <= 16,384 and K <= N/4;
larger requests use full ordinal sorting and reduced output copying where
admitted. Grouped results are ordered only after groups finish.

Before release measurement, prove exact multikey/null/tie/offset results,
ordinary versus streaming zero-limit behavior, omitted invalid payloads,
late source/consumer errors, cancellation, denied grants and credit cleanup.
Keep nested/decimal and explicit spill controls. Debug evidence must distinguish
validated rows, constructed output rows and retained ordinals.

Compare against the immutable `candidate-runtime-1015.json` binary using the
unchanged v2 fixtures and three alternating pairs. `sort_limit` at 65,536 rows
is the benefit target; full many-batch sorting and all small sort cells are
controls. The target engine ratio must be <= 0.95; every target/control retains
the existing 10% or 100-microsecond median ceiling and complete correctness,
grant and cleanup checks. Preserve every observation; no cache manipulation or
concurrent task work. A failed candidate is revised once only for a diagnosed
cause, or dropped. Run one isolated Full43 at the retained checkpoint.

The revised implementation at `3d1411ca` passes all 30 declared calls, with
complete values, schemas, producer consumption, source identities and credited
cleanup checked. The [decision](../benchmarks/evidence/native-engine-optimization-2026-10-23/sort-prefix-decision.json)
retains it under the unchanged gate. Large limited-sort median engine time is
0.196951667 seconds for the previous checkpoint and 0.043669208 seconds for the
candidate, a ratio of 0.221726. Full-sort medians are 0.193829292 and 0.194905796;
all three small-input controls also meet their original ceilings. This is a
composed-workload result, not a Full43 speedup claim, and input retention remains
unbounded by the requested prefix.

Focused evidence covers independent stable ordering, nullable multikey ties,
complete omitted-payload validation, every admitted flat scalar family,
resident nested/decimal controls, prepared immediate-demand activation,
computed projection/filter barriers, explicit spill, cancellation, denied
grants, consumer refusal, result ownership and cleanup. The existing binder
may safely remove an identity projection before demand reaches its sort;
a computed projection remains a barrier. An initial test incorrectly expected
the identity projection to remain and was corrected to cover both cases.
The original failed fixture/build observations remain in the local record.
Focused tests and native-feature lint pass; final workspace gates remain due
after the coherent feedback batch.

The isolated Full43 at the same runtime (`a19471ee`, documentation checkpoint)
passes exactly one execution of every query and all 381 complete result rows.
Summed engine execution is **49.674458790 seconds**. Full source, executable,
input and reference identities remain unchanged, and owned processes and locks
are released. This is the sort-demand regression checkpoint.

### Prepared-expression and batch-reuse contract

The next candidate retains the existing finite predicate program, including a
negative admission decision, when each native expression is bound. Replace its
borrowed column references with bounded owned names and nullability. Keep the
existing eight-column, 32-instruction and 64-visit limits, and cap retained name
bytes at 256 per column. Prove the program and its owning expression fit the
existing 4,096-byte per-expression preparation allowance before allocating it.
The same program serves projected three-valued Booleans and filter truth masks.

Within a projection, bind exact reuse of an earlier completed projection
output. Admit only ordered, type-identical pure column/literal/unary/binary/
comparison trees, with floating literals compared by bits. Search at most 16
earlier outputs and 64 nodes per comparison/annotation traversal. Store the
resolved output ordinal during preparation; do not repeat structural searches
for every batch. Reuse the output arrays the projection already retains, with
ordinary dtype/cardinality checks. Do not retain another array cache.

Reuse is scoped to the same input batch and projection invocation. Lazy branches
and selected sub-inputs retain their existing evaluation scope; CASE, COALESCE,
NULLIF, casts, functions and constructors are not annotated through this first
reuse family. There is no cross-query state, algebraic reassociation, new
provider or new array representation. Original error order, cancellation,
output ownership and metadata admission remain part of the contract.

Extend the existing cost fixture as `native-engine-cost-v3` before implementing
the runtime change. Preserve the v2 definitions and every previous observation.
Two additional cases use the same nullable I64 inputs, uneven batches, grants
and complete-result checks. `expression_shared` projects `id`, `n * id`,
`n * id - n` and `n * id + 3`. `expression_unique` changes the latter products
to `(n + 1) * id` and `(n + 2) * id`, respectively. Independent arithmetic fixes
all expected values and schemas before either executable is built.

Freeze a baseline with this harness and the retained sort runtime. Compare
three alternating pairs per cell against the same-harness candidate. The
65,536-row shared-expression case is the benefit target (ratio <= 0.95).
Controls are the large unique-expression, projected-predicate, filter-predicate,
single-predicate and numeric-value cases, plus all six cases at 257 rows.
All 12 cells keep the existing median ceiling of 10% or 100 microseconds,
complete-result, source, grant and cleanup checks. Debug checks must prove zero
recipe compilation during repeated executions and actual reuse of completed
outputs. Run one isolated Full43 after the retained checkpoint; broad validation
remains at the end of the cohesive feedback batch.

The implementation checkpoint retains owned predicate programs at expression
construction and binds completed-output ordinals once per projection. Compile-time
size checks tie retained recipe/name storage to the existing metadata allowance.
All 55 selected expression, predicate, typed-reference, lazy-branch, ownership
and resource checks pass, including zero compilation during repeated execution,
actual array-owner reuse, name/search limits, signed-zero distinctions, late
overflow, consumer failure, cancellation and denied preparation/execution credit.
Focused Clippy passes with warnings denied. The first cancellation test mistakenly
expected the enclosing cancelled session to succeed; correcting that expectation
required no runtime change. The immutable same-harness baseline is `4e6db3a9`.

**Retain** the prepared-expression candidate at `22a42070`. All 72 alternating
release calls pass complete values, schemas, input/partition identity, producer
completion, grants and cleanup. The
[recorded decision](../benchmarks/evidence/native-engine-optimization-2026-10-23/expression-decision.json)
preserves every sample and the declared gates. The 65,536-row shared-expression
median falls from **0.021299086 to 0.016807333 engine seconds** (ratio 0.789111,
21.1% lower). The unique-expression control is 0.025621416 versus 0.025508494
seconds; all 12 cells pass their frozen regression ceiling. Predicate controls
remain essentially unchanged at the larger scale. This is a composed-expression
benefit, not evidence of a general Full43 speedup. The isolated Full43 checkpoint
follows this retained implementation.

The isolated Full43 at `f6e2f9ed` passes all 43 queries and 381 expected rows,
with **50.472347960 engine seconds**. Each query executes once; all other task
work remains idle. Full input/source/reference identity checks, process cleanup
and lock release pass. The same frozen runner, input, reference and explicit
Q28/Q29 byte-length spelling remain in use. This single regression observation
does not isolate the composed-expression improvement.

### Composed numeric-consumer contract

Extend the existing `NativeNumericOwner::bind_typed_row_update` consumption
boundary to the composed aggregate's scalar SUM and AVG states. The primitive
and compact aggregate callers already use that typed, validity-aware row kernel.
Reuse its original-width slices and one-time type/validity dispatch; preserve
the existing composed state, adjacent input-view sharing and finalization.
There is no new numeric representation, accumulation algorithm or provider.

Admit only ungrouped SUM/AVG over an existing numeric owner. Keep grouped,
decimal, extrema, count, distinct and other inputs in their existing consumers.
Each measure still processes its entire batch in declaration order. NULLs do
not update state. For a valid value, preserve input-finiteness validation before
checked count increment, then the original `NumericAggregateSum::add` and its
nonfinite-total check, with the same errors and partial state. Do not merge
fresh partial sums or change floating addition order across batches.

Reserve the bounded borrowed-kernel metadata before allocating its closure,
and release it before the input view. Keep cancellation checks at the original
row cadence, including NULL rows. Verify all ten admitted physical numeric
types, nullable/empty/encoded/sliced inputs, ordered floating results, exact
failure precedence and partial state, credits, producer release and retained
output ownership. Exercise the consumer after a computed projection/filter,
and retain primitive-kernel, grouped, decimal and explicit-spill controls.

Use the unchanged v3 cost harness and the accepted `22a42070` expression runtime
as the immutable baseline. The large `aggregate_numeric` case is the target
(engine ratio <= 0.95). Controls are `aggregate_text`, `aggregate_count`,
`aggregate_interleaved`, `aggregate_distinct` and `value_numeric`, plus all six
cases at 257 rows. Run exactly three alternating pairs for all 12 cells; keep
the existing 10% or 100-microsecond median ceiling, complete values/schema,
source, grant, producer and cleanup checks. Debug observations must prove actual
typed-consumer use and no new retained input lifetime. A retained implementation
receives one isolated Full43; final broad validation remains at the end of the
cohesive feedback batch.

### Composed numeric-consumer decision

Retain `90251f28`. The existing typed row kernel now feeds scalar SUM/AVG in
composed execution, with one physical-type/validity dispatch per measure and
batch. It borrows the existing numeric owner and updates the existing state in
input order. A 256-byte reservation covers the borrowed closure until it drops;
the actual closure size is checked for every admitted physical type and validity
shape. Finalization and adjacent column-owner reuse remain shared.

The [complete numeric-consumer decision](../benchmarks/evidence/native-engine-optimization-2026-10-23/aggregate-numeric-decision.json)
preserves all 72 release observations, their record hashes and the frozen v3
scoring against `22a42070`. Large numeric aggregation falls from **0.007787084**
to **0.005647500 engine seconds**, a ratio of **0.725239384** (27.5% lower).
The small numeric case falls from 0.000136083 to 0.000129457 seconds. All 12
cells meet their timing ceilings and validate complete values, schemas, input
identities, producer completion, grants and cleanup. This is evidence for the
declared composed workload, not an isolated Full43 speedup.

The focused regression groups cover 104 distinct passing tests, including the
existing primitive/compact numeric kernels, exact keys, grouped and decimal
aggregates, spill recovery, native writers and the larger-than-grant cases.
Eight new tests verify all ten numeric widths, nullable/empty/dictionary/constant/
chunked/sliced inputs, exact accumulator bits after every row, failure precedence,
partial state, ordered floating addition, denied credits and cancellation.
Streaming tests run after a filter and computed projection, execute the prepared
plan twice with different data, witness producer release and retain output beyond
the prepared owner's lifetime. The initial diagnostic-prefix regression was
fixed by sharing the existing scalar validator. An overlap assertion in the test
selection wrapper stopped after 54 passing tests; a separate preserved receipt
completes the remaining control groups. Test-only lint repairs are followed by
a passing eight-test rerun and feature-enabled Vortex Clippy with warnings denied.
Final whole-batch validation is still pending.

The isolated Full43 at `e955081d` passes all 43 queries and 381 expected rows
in **50.456658502 engine seconds**, with one execution per query and other task
work idle. Input/source/reference identity, process cleanup and lock-release
checks pass. An earlier driver startup selected Python 3.9 and failed during
imports before preparation or any query; that failure is retained separately.
The unchanged driver then runs with the existing supported Python 3.13 runtime.
This is a regression observation, not evidence that Full43 activates the new
composed numeric consumer.

### Existence-join implementation contract

Reuse `RowSet`'s first-occurrence native key retention and `RowIndex`'s exact
collision equality for keyed left semi/anti joins without a residual condition.
Add an explicit non-null-equal membership mode; existing set and aggregate
callers retain their current null-equal behavior. Bind the unique right key
field list under preparation credits, retain only those fields, and gather in
the existing admitted output-batch size. Repeated right keys must not add
duplicate links or retain unused payload. Existing per-input validation and
producer detachment remain in place before this retention boundary.

Keep complete right-input consumption, source ordering, left-row multiplicity,
output schemas, cancellation, credits and error behavior. In particular,
preserve the first-NULL short circuit for compound keys, including a later
nonfinite key that the current hash never observes. A non-null nested key with
null children must still participate in exact equality. Residual conditions,
other join kinds, explicit spill and the existing cyclic-spill strategy retain
their current consumers. Test these boundaries with forced-collision index
controls, invalid inputs, multiple batches/executions, producer release,
retained output, denied state and failure cleanup.

Use the unchanged v3 harness and the accepted `90251f28` numeric-consumer binary
as baseline. Run exactly three alternating pairs for `join_semi_duplicate`,
`join_anti_duplicate`, `join_semi_unique` and `join_anti_unique` at 65,536 and
257 rows: eight cells and 48 calls. The two large duplicate cases are targets;
the other cells are controls. Retain if the target geometric-mean engine ratio
is <= 0.95, or each large target reduces peak credited bytes by at least 20%,
while every cell meets the existing 10% or 100-microsecond timing ceiling and
all semantic/resource checks pass. Record the timing and state decisions
separately. Debug observations must establish unique retained-row counts;
they are absent from release measurements. One isolated Full43 follows a
retained implementation, then final coherent validation.

The implementation extends the existing membership owner with an explicit
NULL policy. Only eligible keyed semi/anti joins select the new mode; their
ordinary duplicate-preserving table remains empty. The binder credits and
deduplicates retained field metadata while preserving repeated comparison-key
positions. All input detachment and validation still precede retention.

Nine new tests cover exact values against the original consumer, multiple and
empty batches, dictionary domains/codes/slices, float zero, nested child NULLs,
compound-key short circuits, malformed input, unused invalid build payload,
fresh execution state, producer release, retained output, cancellation and
denied credits. The focused suite passes 138 tests, including forced-collision
index controls, residual/outer joins, grouped/decimal aggregates, set membership,
native writers and spill pressure/recovery. Feature-enabled Vortex Clippy also
passes. An initial test expected the wrong text for the existing finite-value
diagnostic; the corrected assertion preserves its exact original message.
The initial function-length lint is resolved by sharing the existing semi/anti
output branch.

The first 48-call comparison at `f7cf7654` passes complete values, schemas,
input/producer checks and credit cleanup. The two duplicate-heavy target
medians have an engine-time geometric-mean ratio of 0.945843, and their peak
credited-state ratios are 0.063848 and 0.081544. Nevertheless, the candidate
**fails retention**: the large unique-key semi/anti controls increase by 17.7%
and 24.1%, above the unchanged 10% ceiling. Preserve this failed decision in
`join-first-decision.json` and all 48 observations.

The single permitted revision addresses two source-confirmed extra costs on
new unique keys. `RowSet` first searches the index, then `RowIndex::insert`
repeats that search. Its bounded key copy also calls scalar storage preparation
per retained numeric row, whereas the existing native payload copier binds a
`NativeNumericOwner` once. Reuse the same index publication and native numeric
owner boundaries to remove those repeated operations. This is a source-based
explanation to test, not an isolated measurement of either cost's contribution.
Preserve exact collision equality, duplicate order for ordinary index users,
reservation-before-publication, cancellation, selection, finite-value checking,
and owned output credits. Add collision/denial and numeric-copy regression
coverage, then run the same 48-call schedule against the same accepted baseline
in a new result directory. Thresholds, fixture sizes and supervision remain
unchanged. If this revision fails, drop the existence-join candidate.

The revision adds a unique-insertion mode to the existing index publication
routine; ordinary joins still append their full duplicate chains. `RowSet`
uses the returned insertion decision instead of searching twice. The shared
selected-column copier binds the existing numeric owner after take/cast and
keeps the same final result builder, finite-value checks and owned buffers.
Decimal, nested and preserve-unobserved float copies retain their prior routes.

The revised focused run passes 156 tests, with one new test incorrectly
expecting a cancelled session to complete successfully. Its corrected assertion
checks both the copy's cancellation and the existing session cancellation
diagnostic; all four new tests then pass. Together the 157 distinct tests cover
the revision and the existing join/set/aggregate/payload controls. The first lint
attempt finds only a missing test semicolon; no lint exemption is added.
Feature-enabled Vortex Clippy, workspace formatting and diff checks pass.

### Historical cross-surface investigation

The [25-observation extraction](../benchmarks/evidence/native-engine-optimization-2026-10-23/surface-comparison.json)
binds Q19/Q23/Q29/Q34/Q35 to the original compressed output and summary members
of the [five-surface packet](../benchmarks/native-shared-surfaces-2026-10-23.md).
Its source is `7c65f339`, before the final PR #1593 repairs. These are historical
engine seconds, not new runs or measurements of the current candidate:

| Surface | Q19 | Q23 | Q29 | Q34 | Q35 |
| --- | ---: | ---: | ---: | ---: | ---: |
| CLI SQL | 4.722240 | 3.293214 | 3.118415 | 2.568379 | 2.682682 |
| Embedded workflow | 4.872714 | 3.947033 | 4.255317 | 3.614409 | 3.546948 |
| Python SQL | 5.331907 | 6.764748 | 4.975018 | 4.242673 | 3.492865 |
| DataFrame | 5.567369 | 4.577225 | 4.563809 | 3.818391 | 3.847976 |
| Typed expressions | 5.512588 | 4.517834 | 4.637102 | 3.864888 | 3.767842 |

Every selected observation uses the same recorded Vortex input identity and
reports native scan pushdown. For each query, scanned/selected rows and arrays
read agree across all five surfaces. CLI, embedded workflow and Python SQL
report identical strategy labels and available non-timing work counters for
each query. The DataFrame Q34 report also agrees. The other four DataFrame
reports and all five typed reports omit these strategy details, so their
algorithm parity cannot be independently established from this evidence.

All 25 reports admit at most 10 workers; this is a policy ceiling, not an actual
worker count. Only seven reports expose peak active aggregate workers: Q34/Q35
for CLI, embedded workflow and Python SQL, plus DataFrame Q34. Those report
nine active workers and 64 partitions. Actual worker counts are unavailable
for the other selected observations. Physical scan/read column lists are
unreported in all 25; the projected-column field describes output columns and
must not be substituted for them.

Each summary engine duration equals its output's native execution nanoseconds.
That confirms a consistent recorded clock, but does not isolate a frontend
cost or exclude host/cache/order effects. The evidence therefore establishes
neither Python overhead nor a specific runtime defect. The actionable proof
gap is consistent strategy, actual-worker and physical-read reporting across
the existing surfaces. Preserve this finding without attributing the spread
or rerunning historical cohorts merely to fill missing fields.
