<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native engine optimization acceptance

Status: prospective protocol, October 23, 2026. No candidate is accepted by this
document. The baseline is PR #1593's repaired runtime, `407c1e80`. The maintainer
requested all six candidates from the [source review](native-engine-optimization-review-2026-10-23.md),
with PR #1593 integrated first. Candidate work uses a separate local branch while
that PR completes hosted validation; it will not be integrated ahead of the repair.

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

Value preparation and aggregate reuse share one ownership boundary and are
evaluated together first. The sort locator can follow that boundary without
changing output demand. Predicate and existence strategies remain separately
admitted. Each candidate has a recorded retain, revise or drop decision; “work
through all six” does not authorize keeping a correctness or cost regression.

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
Run one isolated standard Full43: exactly one execution per query, all other
task-owned work idle, unchanged storage/deadline/concurrency/result checks, full
hashes and inventories outside query execution. It is a regression observation,
not comprehensive measurement of these composed workloads. Review the final diff
adversarially and preserve every candidate decision and remaining proof gap.
