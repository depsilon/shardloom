<!-- SPDX-License-Identifier: Apache-2.0 -->

# Exact join counts without expanded row pairs

Status: dropped at the maintainer's request after the combined Full43 checkpoint.
The whole unmerged item-15 runtime batch is removed. The finite comparison below
remains historical evidence; it does not describe the current runtime. See the
[drop and restoration record](../benchmarks/native-workflow-drop-2026-10-25.md).
This was the first mechanism in item 15's
[workflow work-elimination package](native-workflow-work-elimination-2026-10-25.md).
The baseline is merged #1605, `167ac248ba662ae7967a7801995e9b160b17bae5`.
RFC 0044 and RFCs 0013–0017/0021–0022 govern the native plan, resource, semantic,
streaming and error boundaries; no new phase or competitive-gate closure follows.

## Existing component and extension

The current prepared dispatcher is
`local_primitive_prepared_relational.rs::PreparedVortexRelational::run`.
An aggregate executes its input, and an ordinary inner join builds the right
side, probes left rows, constructs bounded matching-pair arrays and gathers
their output columns. Scalar `COUNT(*)` then adds those arrays' lengths.
The handoff's two standalone join/aggregate dispatcher filenames do not exist;
the current bound nodes and aggregate specification live in
`local_primitive_relational_bind.rs` and
`local_primitive_relational_aggregate_bind.rs`.

Extend the existing exact `RowSet`/`RowIndex` and native `Batch` key owners with
credited key multiplicities. Retain one native representative per non-null
right key and a checked count, then probe every left key and add its matching
multiplicity. This computes the same sum of left/right multiplicity products
without retaining a second left table or enumerating matching row pairs.
Use the existing scalar aggregate state and finalization for all requested
COUNT aliases. Execution state stays local to each operation; repeated prepared
calls recompute it.

The existing RowSet equality, collision handling, first-occurrence order,
gathering and backing ownership are the reuse boundary. Do not add a second
key hash/equality implementation, factorized plan IR, cache, thread pool or
external executor. Keep original children and right-before-left execution order.
Complete both producers even when one side has no matching keys.

When an entire input batch contributes new non-null keys, keep those native key
arrays and their already prepared key owners directly. Remove unselected payload
columns and retain the complete backing credits. Mixed/duplicate batches use the
existing compact native gathering. Ordinary set/grouping consumers keep their
existing retention path; this specialization belongs only to counted membership.

Vortex-first check: pinned Vortex 0.90.0 supplies native arrays, builtins,
validity, execution contexts and allocation owners used by the existing key
and gather providers. The inspected `vortex-compute` crate exposes lane-level
map kernels, not this relational join/aggregate consumer contract. This change
extends ShardLoom's existing native relational algorithm around admitted Vortex
owners (`wrap_existing_provider`); it adds no dependency or query-engine
integration. Dictionary identity, native typed equality and complete backing
charges stay with the existing providers.

## Admission and semantic contract

Admit only a scalar aggregate containing one or more `COUNT(*)` measures directly
over an inner equality join with at least one key and no residual condition.
All existing admitted key types must use their current exact key semantics.
Composite keys with any SQL-null component do not match. Preserve empty-input
zero, duplicate multiplicity, output U64/schema/aliases, checked count overflow,
full producer/source-generation validation, cancellation and release on failure.
No result is emitted before both complete inputs are accepted.

The initial strategy does not replace the explicitly selected spill executor.
Explicit-spill requests, outer/cross/semi/anti joins, residual conditions,
grouped aggregates, COUNT(column), DISTINCT and other reductions keep their
existing native strategies. A resident reservation failure remains a deterministic
resource error; it must not replay a consumed source or silently switch strategy.
Pressure validation must prove both that denial releases the counted state and
that explicitly requested ordinary native spill still completes correctly.

Keep public SQL, typed/DataFrame and native declarations on the same bound
consumer path. Record actual activation, input rows, distinct retained right
keys and represented matching pairs. Separate those facts from materialized
join rows. Preserve source/error work even when output payload is unused.

The completed report exposes `counted_join_stages`, `counted_join_build_rows`,
`counted_join_probe_rows`, `counted_join_distinct_build_keys` and
`counted_join_represented_pairs`; public route fields add the existing
`relational_` prefix. Counters sum actual completed stages. Represented pairs
are cardinality contributions, not materialized output rows or a speed claim.

## Frozen comparison

Use one identical test harness through the real prepared relational API in
baseline and candidate release builds, with the same Rust toolchain, features,
ThinLTO profile and source data. Freeze harness, executables, source and expected
result hashes before each comparison. Setup includes source registration and
binding; engine time comes from the returned execution clock. Record both,
plus serialization, memory and cleanup. Ordinary host/cache state is uncontrolled;
do not label the comparison storage-cold or attribute unrelated host activity.

| Case | Left/right rows | Key domain and purpose |
| --- | ---: | --- |
| Duplicate numeric target | 8,192 / 16,384 | 64 numeric keys; 2,097,152 matching pairs |
| Composite text target | 4,096 / 8,192 | 32 UTF8-plus-integer keys with non-ASCII text; 1,048,576 pairs |
| Skew target | 1,024 / 2,048 | Nine of each ten positions share one key; other positions have unique keys |
| Unique-match control | 32,768 / 32,768 | One matching right row per left key |
| Unique-no-match control | 32,768 / 32,768 | Disjoint exact key ranges |
| Tiny control | 17 / 23 | Seven numeric keys; setup-sensitive complete operation |
| Ineligible aggregate control | 8,192 / 8,192 | Unique keys with COUNT(column), keeping the ordinary path |

All cases use a single existing CPU allocation and a 128-MiB native grant.
Generate immutable input values outside measured preparation. Independently
derive the complete scalar answer and authoritative schema from those values;
do not use another execution strategy's result as the sole oracle. Include
streamed/schema-only source variants in correctness proof, and keep prospective
comparison inputs fixed across both roles.

Run two complete cohorts with three paired samples per case. Alternate
baseline/candidate order within the first cohort and reverse it in the second.
No sample is silently retried, replaced or removed. Each target must improve
median complete-workload engine time by at least 2% in both cohorts. Every
case's median engine time and preparation-plus-engine time must remain within
5% of its control; the tiny case also allows an absolute 50-microsecond increase
to avoid deciding from timer-scale noise. This absolute allowance applies only
to the predeclared tiny control. Resource ownership and complete result gates
must pass regardless of timing.

Run separate debug-observation executions to establish zero matching-pair
construction in admitted candidates, exact represented cardinality, complete
input consumption and unique-key retention. Do not mix instrumented times into
the release score. Preserve every failure and partial cohort. A failed gate is
a revise/drop decision, not permission to add repetitions until a favorable
result appears. A materially different revision requires a new explicit protocol;
the original failed decision remains visible.

## Correctness and completion

Test mixed/all-null and empty keys, composite strings and integers, dictionary
identity and slicing, duplicates across batches, forced hash collisions, skew,
checked count overflow without constructing impossible-sized inputs, and every
non-admission boundary. Exercise source changes, late producer errors, schema
drift, cancellation, denied reservations and complete resource release. Verify
write/readback and equivalent SQL/DataFrame declarations at package acceptance.

Run focused native unit/integration checks before the required workspace
format/lint/tests, native suite and public/installed-package acceptance for the
cohesive package. Retained runtime changes receive one isolated standard Full43
pass at that boundary, exactly one call per independent query. Do not use Full43
to replace the composed-workflow proof, reduce its correctness checks, or claim
that a join-count benefit is a whole-engine speedup. The following fused pipeline
and remaining item-15 packages keep their own acceptance decisions.

## October 25 mechanism decision

The initial finite decision was to retain the candidate; the subsequent whole-item
drop above supersedes it. All 84 release operations completed with exact full
results, authoritative schemas and released native reservations. Both cohorts
pass every predeclared target and control gate. The engine medians below are
milliseconds; preparation and serialization remain separately recorded in the
complete evidence.

| Case | Cohort 1 baseline | Cohort 1 candidate | Cohort 2 baseline | Cohort 2 candidate |
| --- | ---: | ---: | ---: | ---: |
| Duplicate numeric | 91.584543 | 1.479416 | 90.817042 | 1.170750 |
| Composite text | 43.243376 | 1.183208 | 42.983417 | 1.188833 |
| Skew | 53.092291 | 0.289958 | 52.727540 | 0.285333 |
| Unique match | 3.677209 | 2.647168 | 3.697208 | 2.687334 |
| Unique no match | 1.671250 | 1.688540 | 1.671292 | 1.692125 |
| Tiny | 0.141458 | 0.130958 | 0.146958 | 0.130167 |
| Ineligible COUNT(column) | 1.104541 | 1.111417 | 1.113999 | 1.106625 |

The three duplicate-heavy targets reduce engine time by 97.2–99.5% across the
two cohorts. This is a result for these bounded composed workloads on this
host. It does not establish a Full43 or general engine speedup. In particular,
the no-match control increases by 1.0–1.3%, within its frozen limit; the complete
observations are retained rather than reporting only favorable cells.

Fourteen separate debug executions verify the mechanism. Eligible candidates
construct zero matching-pair rows while consuming every input row. The three
targets retain respectively 64, 32 and 205 distinct build keys and represent
2,097,152, 1,048,576 and 1,700,270 matching pairs. The ineligible COUNT(column)
control retains ordinary execution and constructs all 8,192 required pairs in
both roles. These observations are excluded from the release score.

The final native suite passes 3,312 tests with 33 intentionally ignored cost
and environment-specific tests. The focused selection passes 19 tests, and
native all-target lint passes. Preserved earlier failures include a new test
fixture's collection-type error, missing bound inner-join key metadata, and an
existing writer-test temporary-directory collision. The latter is repaired by
adding a process-local atomic sequence to timestamp-based names; the complete
native suite passes after that repair.

The [compact evidence manifest](../benchmarks/evidence/native-join-count-2026-10-25/manifest.json)
and packet retain every release/observation call, full input/result/schema
witness, prospective protocol, build/source identity, correctness log and
failed attempt. The [portable verifier](../benchmarks/evidence/native-join-count-2026-10-25/verify.py)
recomputes complete answers, source-delivery and mechanism counters, and both
cohorts' timing gates without running archived code or reading the original
inputs. An isolated replay with original-source reads denied and 18 corruption
checks pass. The [validation receipt](../benchmarks/evidence/native-join-count-2026-10-25/portable-validation.json)
identifies the verifier and each check.

Workspace, public SQL/DataFrame, writer/readback, installed-package and the
single isolated Full43 acceptance remain at the cohesive package boundary.
The next implementation mechanism is the composed encoded scan-to-aggregate
path; this decision closes only the first mechanism's finite comparison.
