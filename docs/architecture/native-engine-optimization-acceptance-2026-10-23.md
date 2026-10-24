<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native engine optimization acceptance

Status: prospective protocol, October 23, 2026. No candidate is accepted by this
document. The baseline is PR #1593's repaired runtime, `407c1e80`. The maintainer
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

## Finite sort-output feasibility decision

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
