<!-- SPDX-License-Identifier: Apache-2.0 -->

# Broad analytical surface and performance program

## Decision and ownership

The October 26 maintainer request expands existing queue item 20, everyday
analytical completion, into a shared query-family program. Its objective is:
across supported operators, types, distributions, layouts and public interfaces,
queries execute correctly, reach appropriate native implementations and avoid
unexplained material performance cliffs. A finite corpus supplies evidence for
its declared coverage; it cannot establish that arbitrary queries are fastest.

The [phase plan](phased-execution-plan.md#everyday-analytical-completion) owns
the active checklist. This document owns the implementation and evidence
contract under RFCs 0014/0015/0016/0021/0033/0044. It creates no new phase ID or
competitive gate. CG-1 through CG-23 remain visible and independent; local
capability completion does not close CG-5 correctness or CG-6 competitive claims.

Item 17 and its two review corrections are merged in
[PR #1610](https://github.com/depsilon/shardloom/pull/1610) as `c287ba45`, after
all 54 hosted checks pass. Its prerequisite documentation correction is merged
in [PR #1609](https://github.com/depsilon/shardloom/pull/1609) as `931bf814`.
Completed generated payload cleanup precedes this program. Unfinished item 18
remains preserved at `87b5a174` on `codex/universal-io`; it is neither accepted
nor a prerequisite. Items 18/19/21/22 keep their existing ownership.

**The first stages expand and validate ShardLoom itself.** A later October 26
maintainer instruction authorizes **one Polars execution per selected case** as
an initial comparison for prioritization. This narrowly replaces the earlier
blanket deferral; DuckDB, DataFusion and other-engine execution remain deferred.
Use actual Polars lazy expressions and complete result checks against the same
independent expectations. Keep its API clock separate from ShardLoom's engine
clock; compare only matching declaration-through-complete-delivery boundaries.
Identify input-format differences and unexposed schema metadata explicitly.
One observation supplies neither a noise estimate nor a superiority claim.
Current correctness also uses independent explicit expectations, bounded
standard-library reference calculations, existing admitted conformance tests and
metamorphic checks with stated preconditions. Reference and comparison engines
never supply ShardLoom candidate output.

ClickBench is an established regression guard. Targeted family tests are the
development loop; a focused shared-runtime concern can justify using the existing
isolated Full43 procedure. This program does not authorize routine repeats,
cache manipulation, artificial pacing or reruns for presentation alone.

## October 30 coverage expansion after item 19

The maintainer schedules the expanded analytical testing/benchmark campaign after
rich-data lifecycle item 19 and before Foundry item 21. Current item 20 continues
its seven performance candidates plus explicit ordering-kernel, predicate and
multiway/composed-movement follow-through; ordinary targeted acceptance is not
deferred. The original 347-query corpus, denominator, inputs and historical
observations remain frozen. Version the expanded corpus separately and extend
the existing typed declarations, independent references, adapters and constrained
interaction selector; do not create a second benchmarking system.

Promote implemented capabilities missing from the executable catalog, including
windows, as-of joins, richer aggregates, subqueries, reshaping, materialization
and recursion. Cover expression placement in projection/filter/group/join/order,
hidden/renamed columns, safe wrappers, repeated subplans and multiple consumers.
Declare equivalence conditions; fallible expression reordering, floating
reassociation and materialization-boundary crossing are not automatic rewrites.

Case metadata must identify actual operator/composition, physical and logical
types, floating policy, decimal precision/scale, temporal units/zones, nulls,
cardinality/selectivity, skew/correlation/fanout, build/probe size ratio, source
encoding/layout/chunks, result demand, resources and lifecycle/delivery. Exercise
empty/singleton through high cardinality, long/wide/nested output, full sorts and
large K/deep offsets, nullable/text/temporal ordering, heavy ties, mixed directions,
partial ordering and actual spill. Numeric coverage includes column-column and
literal operations, widths/signs, casts and expression chains; text includes
substring/concat/pattern/regex/JSON and key/predicate consumers. Temporal coverage
includes negative epochs, DST, arithmetic, windows, grids and interpolation.
Include nullable CASE/COALESCE/NULLIF and guarded errors, large set branches and
multiple distinct states. Generator options alone are not executed coverage.

Keep 1M rows as the primary development scale. Use inexpensive tiny semantic
fixtures, approximately 100–200 balanced smoke representatives, constrained
pairwise plus targeted three-to-five-factor interactions, genuinely held-out
plan/profile/cardinality/layout combinations, and 50–200 selected scale/resource
sentinels. Grow toward the existing 80–120 archetype and 5,000–10,000 meaningful
variant targets as budgets allow; counts do not establish completion. Record
input bytes, key/state cardinality, actual layouts and large-output delivery,
not row count alone. Once a held-out combination guides tuning, retain it as a
regression and introduce a fresh acceptance combination. Bound generated storage
and keep broad runs at cohesive integration boundaries.

Report fixed-cohort engine total separately from per-family and family-balanced
results, absolute gaps, severe outliers and actual interaction coverage. Include
observed reads/decodes/copies/gathers/retained state/spill and selected routes.
Incorrect, unsupported, adapter-missing, timed-out and unexecuted cases remain
visible. Comparator cases require compatible semantics, complete results and
matched clock/resource/input boundaries; unavailable comparison stays explicit.
The retained Polars API clock is not an engine-only clock. Freeze the comparator
protocol and execution budget at this later stage, preserving earlier observations
without automatic repeats or a claim of broad superiority.

## Historical first source-demand work unit

The October 26 maintainer proposal follows the completed 347-case baseline and
foundation, merged in PR #1611 as `4d25a06e`. The
[source-demand work unit](native-source-demand-2026-10-26.md) records the pinned
provider and existing-owner review, the frozen pre-change comparison and the
locally accepted first Parquet adapter. The
[acceptance report](../benchmarks/native-source-demand-2026-10-26.md) records
all 347 corrected Parquet cases passing in 50.877676708 seconds of engine time,
344 cases with reduced physical column demand, and the retained first checkpoint.
The source change is merged in #1612 (`81ab0a4d`), and its Parquet completion is
merged in #1613 (`04e7d92a`); neither is a package release. The saved item-18
connector branch remains separate. The October 27
[completion continuation](native-analytical-completion-2026-10-27.md) carries
the remaining analytical and shared-performance work, then format/parity work.
This work unit does not close the broader analytical capability or operator
performance program.

The subsequent [aggregate/grouping acceptance](../benchmarks/native-analytical-aggregates-2026-10-27.md)
implements the richer shared reducers, windows, aggregate filters and grouping
combinations. All nine new feature suites pass through five public interfaces
on Vortex and Parquet; the installed wheel passes 194 integration tests without
skips. The unchanged 347-case corpus passes at 31.962394957 engine seconds versus
34.960427343 in the immediately preceding checkpoint. Original Polars observations
remain unchanged, and the performance gap campaign is still open.

The maintainer's subsequent clarification makes demand propagation a universal
planning contract. Start with Parquet as the first adapter, but derive demand in
the shared planner and carry it through the common source boundary. Each format
declares the projection, predicate, range and limit work it can safely avoid;
unsupported pushdown leaves the residual in existing native execution. Source
capabilities must preserve hidden columns, ordering, global operator semantics,
validation scope and cancellation. Do not create a Parquet-only execution stack
or promise equal physical pruning for sequential and random-access formats.

The first milestone is eligible fresh Parquet execution through demanded,
bounded native batches without requiring a complete durable Vortex artifact.
Separate metadata/schema/source-generation admission from physical preparation,
then carry required output and hidden predicate/join/sort columns, admitted
row-group pruning, ordering and cancellation/resource requirements into the
reader. Reuse existing native arrays, operators, ownership and backpressure.
Durable full preparation remains explicit and available for reuse.

Preserve full-ingestion validation guarantees. Query-scoped access must state
what it validates, including invalid values inside/outside the requested region.
Current metadata-based identity is not repeated full-file hashing; do not invent
that as the cause. External Arrow intake's allocation/immutability copy guard
remains until a separately proven ownership contract permits avoiding copies.
No zero-copy or physical-read reduction claim follows from a lazy wrapper alone.

Freeze a prospective comparison of fresh full preparation plus query, demanded
native batches, already-prepared Vortex and equivalent Polars over the same
original Parquet. Cover selective and broad first queries, repeated/shared/new
columns, changed generations, low memory, large output, cancellation and invalid
data. Retain complete results and observable read/decode/copy/encode/write work;
unknown counters stay unknown. Report complete-workflow/first-output/sequence
costs where explicitly requested, keeping engine clocks separate. Check whether
the new source reaches efficient existing native operators before retention.

Incremental region/column persistence and background preparation follow only
after that direct path proves its value. They require source-generation,
coverage/alignment, ownership and lifecycle evidence and are not authorized
shortcuts around the current phase or a reason to create a second connector stack.

## Intake and existing foundations

| Request | Disposition and existing owner |
| --- | --- |
| Broad generated coverage, distributions, layouts, strict correctness and diagnosis | Accepted into item 20; extend the traditional analytics harness and public native conformance helpers. |
| Existing five-interface parity and maximal shared implementation | Required preservation and extension; no SQL-only capability or optimization acceptance. |
| Statistics, temporal fidelity, numeric semantics, recursive composition and declared batch functions | Existing item-20 requirements retained; verify actual support before adding behavior. |
| Ordinary nested CTEs, column lists, typed parameters and named lazy plans | Already implemented by item 17; expand composition and regression coverage. Explicit materialized CTE ownership and recursive execution remain distinct unfinished contracts. |
| Complete remote/table input, output and refresh | Existing item-18/19 contracts; expose their current gaps in the corpus without importing unfinished work or inventing a parallel connector stack. |
| New performance mechanisms | Investigation categories only; implement a shared correction after a demonstrated cause and a prospective retain/drop decision. |
| Other-engine comparisons | Later October 26 instruction admits one Polars run per selected case for prioritization; other engines and broader repeated comparison remain deferred. |
| Foundry, AI workflows, durable live state, transactional tables and distributed execution | Keep existing items and RFC owners; local analytical breadth does not imply their completion. |

The [traditional analytics harness](../../benchmarks/traditional_analytics/README.md)
already separates workload declarations, fixture generation, native public
requests, independent comparisons and resource supervision. Its historical
[704 comparisons](../benchmarks/native-workload-comparisons-2026-10-22.md) cover
22 tasks on 1,000 fact rows and twenty dimension rows. Its four-decimal rounding
and absent/NaN-sum-to-zero normalization are explicitly limited historical
semantics. Preserve those records; do not reuse that comparator as a universal
correctness contract or relabel the old observations as 1M-row evidence.

The [shared provider coverage](shared-native-provider-coverage-2026-10-23.md),
[five-surface acceptance](../benchmarks/native-surface-parity-2026-10-24.md) and
[connected programming acceptance](../benchmarks/native-connected-programming-2026-10-26.md)
are foundations to extend. They establish concrete complete-result, schema,
ownership and no-fallback contracts, not the entire operator/type/interface
Cartesian product. The earlier Q34 paired timing limitation remains recorded;
correctness parity does not imply statistical timing equivalence.

## Shared implementation and reuse map

Each retained change must name its common owner, existing callers, semantic gap,
extension and cross-interface evidence. Keep justified physical strategies inside
that owner. Sharing only a crate, allocator or method name is insufficient.

| Concern | Existing component to extend | Required boundary |
| --- | --- | --- |
| SQL scopes, parameters, aliases and plan references | `shardloom-cli/src/sql_native_relation_declaration.rs`, `sql_native_relational.rs`, `native_runtime_plan*` | One bound native plan; preserve output names, types, scope, errors and source identity. |
| Native ownership, preparation and provider choice | `shardloom-vortex/src/local_primitive_prepared_relational.rs`, resident session and existing provider selection | Select from normalized typed semantics; record admission and rejection reasons before execution. |
| Scalar expressions across consumers | `local_primitives/native_relational_expression*`, shared scalar/numeric/text/temporal providers | Reuse in projection, predicates, keys, aggregate arguments, joins and ordering; decode only required inputs. |
| Aggregation, ordering, joins, windows and sets | Existing `local_primitives/native_relational_*` families and their spill owners | Shared scan/group/key preparation where semantics permit; correct state, merge and cleanup accounting. |
| Public interfaces | `python/src/shardloom/_connected.py`, `_native_backend.py`, `shardloom-python/src/session.rs` and existing CLI/workflow bindings | Thin declarations/adapters into the shared runtime, preserving laziness and ownership. |
| Inputs and results | Existing source/prepared state, native writer and result-owner adapters | Vortex-native execution and persistence; explicit compatibility conversion and complete consumption/readback. |
| Corpus infrastructure | `benchmarks/traditional_analytics`, `scripts/native_workflow_protocol.py`, existing storage/process guards and five-surface tests | Shared case identity, typed semantics, datasets, assertions and reporting; interface-specific code only constructs and consumes public requests. |

A workload is declared once as typed intent with factors and an expected semantic
contract. SQL rendering, DataFrame construction and typed relation construction
are adapters over that declaration. CLI SQL, embedded workflow and Python SQL
submit the complete rendered request; DataFrame and typed-expression cases use
their actual public constructors. Sending SQL through a differently labelled
wrapper does not count as DataFrame or typed-expression coverage.

The five existing entry points remain the acceptance set: CLI SQL, embedded
workflow, Python SQL, DataFrame and typed native expressions. Applicable supported
cases compare complete values, observable schemas, errors, ownership/resource
behavior, source/input treatment and selected native implementation. Record a
specific interface gap when an equivalent declaration cannot yet be expressed;
do not silently drop that interface. Intentional semantic or delivery differences
must be documented and excluded only from the particular equivalence claim.

Vortex-first check: the pinned dependency is Vortex `0.90.0` with the repository's
reviewed provider patches. Arrays, DTypes, validity, encodings, statistics,
deferred expressions, file layouts and scan/write APIs remain provider concepts.
The corpus wraps existing ShardLoom source, plan, result and certificate surfaces;
it does not invent a runtime layout, allocator, scheduler or execution provider.
For a subsequent kernel change, inspect the pinned source and record
`use_vortex_native_provider`, `implement_shardloom_kernel` or an explicit blocked
decision before implementation. Query-engine integrations remain prohibited.

PulseWeave and capillary work units apply through existing scheduling and resource
owners. Dynamic admission/work shaping, metadata-first execution and preserved
encodings are measured choices, not mandatory new mechanisms. Separate engine
timing from preparation and delivery, retain evidence tiers, and leave unavailable
counters unknown. No new worker pool or answer cache is admitted by the harness.

## Dataset family and observed identities

The main scale is **1,000,000 fact rows**, with independently sized supporting
tables. Initial sizes are proposed defaults, not evidence of a completed run.

| Component | Initial size | Purpose |
| --- | ---: | --- |
| Event/transaction fact | 1,000,000 | Scans, expressions, filtering, grouping, ordering and windows |
| Entity dimension | 100,000 | Many-to-one joins and selective dimension predicates |
| Product/category dimension | 10,000 | Multiway joins and alternative join orders |
| Calendar/small dimensions | Hundreds to thousands | Temporal and tiny-build-side behavior |
| Relationship table | About 200,000 | Duplicate/null/absent keys and controlled fanout |
| Edge fixtures | Zero to a few thousand | Exact semantics, all-null groups, overflow and errors |

Use about 32–48 meaningful fact columns covering IDs, categorical keys, signed
and unsigned integers, decimals, floats, booleans, dates/timestamps, short/long
strings and explicitly selected nested profiles. Unsupported types remain visible
capability targets; do not coerce them into strings merely to make a case pass.

Column streams have independent deterministic seeds, with intentional correlation
declared separately. Freeze the generator revision, seed derivation, schema and
logical record identity. A row-index modulo pattern alone is not an adequate
general distribution. Include controlled cardinality (constant through nearly
unique), uniform/moderate/dominant-key skew, nullable-with-no-nulls through all-null,
selectivity (none, one, 0.01%, 1%, 10%, 50%, all), independent/correlated/misleading
predicates, Unicode/empty/prefix/long strings, unique/duplicate/missing/null join
keys, constants/runs/monotone/high-entropy values and scalar through large/wide
outputs. Tiny fixtures separately cover nonfinite numeric and overflow boundaries.

Observe actual null and distinct counts, predicate selectivities and relationship
fanout from generated data; label approximate measurements explicitly. Record
physical file sizes, row/chunk boundaries and observed encodings where available.
The generator's requested distribution or a format's name is not that evidence.

Produce shuffled, sorted, clustered, partitioned and fragmented arrangements of
the **same logical records**. Verify identity and duplicate multiplicity across
layouts. Sorting and chunking may change physical order and compression, not
the logical dataset. Native preparation occurs through ordinary ShardLoom input
and writer APIs, with its own receipt, schema/fidelity checks and readback.

Generate and retire a bounded set of payloads at a time. Use the existing local
storage admission, deadlines, process cleanup and workload exclusion. Keep current
inputs, the latest accepted output/runtime, active comparison inputs and compact
results. Preserve seeds, exact requests, identities, complete-result evidence,
diagnostics, failures and regeneration instructions before retiring temporary
payloads. Do not create payload backup archives or raise storage ceilings.

## Typed corpus and coverage

Start with approximately 80–120 reviewed archetypes. Grow toward 5,000–10,000
meaningful variants as supported coverage and execution budgets allow. Counts are
targets, not completion criteria; repetitions and extra interfaces do not count
as new semantic variants. A case identifies its family, archetype, typed request,
factors, dataset/layout identity, expected semantics/error, applicable interfaces,
resource/delivery policy, neighboring-case relationship and exploration/held-out
partition. Exact requests and all outcomes remain reproducible.

| Family | Required variations |
| --- | --- |
| Scans and projection | Narrow/wide, aliases/reordering, constants/computed fields and hidden predicate/order columns |
| Predicates | Equality/ranges/BETWEEN, AND/OR/NOT, IN, null checks, expression predicates and selectivity sweeps |
| Nullable expressions | CASE/COALESCE/NULLIF, three-valued comparisons, guarded errors and all-null inputs |
| Numeric expressions | Integer/float/decimal arithmetic, widths, signs, casts, precision/scale and overflow |
| Strings | Character versus byte length, substring/trim/concatenation/patterns, Unicode and long values |
| Temporal operations | Units, negative epochs, extraction/truncation/arithmetic, timezone contracts and calendar boundaries |
| Global aggregation | COUNT/SUM/AVG/MIN/MAX, mixed measures, empty and all-null cases |
| Grouped aggregation | Multiple/computed keys, mixed/conditional measures, HAVING, skew and state cardinality |
| Distinct | Exact distinct measures, multiple distinct states, row deduplication and multiplicity |
| Basic joins | Inner/outer/semi/anti, composite/null keys, residuals and unique/duplicate keys |
| Multiway joins | Star/chain/self joins, selective inputs, table ordering and controlled fanout |
| Ordering and ranges | Sort/Top-K, K/offset thresholds, ties, null order and computed keys |
| Windows | Ranking, lag/lead, running/rolling frames, multiple windows and uneven partitions |
| Subqueries | Scalar, IN/EXISTS/quantified comparisons, correlation and cardinality errors |
| CTEs and derived tables | Nested scopes, aliases, shared references and filters/aggregates around composition |
| Sets | UNION ALL/distinct union/intersection/difference, empty branches and type coercion |
| Nested data and reshaping | Struct/list/JSON access, explode, pivot/unpivot, missing fields and dynamic schemas |
| Composite analytics | Useful filter/expression/join/aggregate/window/order workflows |
| Inputs and outputs | Raw/prepared/native sources, batches, collection, large exports and independent readback |
| Interfaces and lifecycle | Five front doors, parameters, first/repeated/prepared execution and result ownership |
| Resources and failures | Low memory, skew, admitted spill, cancellation/deadlines, large output and slow/failing consumers |

Combine reviewed archetypes, type/null/cardinality/implementation boundaries,
constrained pairwise factors, targeted three-to-five-factor combinations,
mutations guided by actual plans/rejections and held-out seeds/combinations.
Measure feasible interaction coverage against declared constraints. Pairwise
coverage is not a claim of complete higher-order coverage. Keep the intended
coverage cell, such as a nullable decimal argument with mixed aggregates, two
grouping keys, HAVING and high cardinality, visible in results.

Unsupported desired capabilities stay in the catalog with their exact diagnostic
or an explicit unimplemented-adapter/unexecuted status. Distinguish harness errors,
engine rejection, incorrect results, crashes, timeouts and deliberate out-of-scope
cases. An execution failure cannot become an expected error after observation
without a reviewed semantic contract and preserved original record.

## Neighboring queries and shared causes

For each useful optimized implementation, exercise nearby requests: add AVG/MIN/MAX
to SUM; move a predicate behind an alias/derived table; change nonnullable to
nullable; replace a direct argument with arithmetic or COALESCE; add a grouping
key; change integer to text/dictionary keys; vary LIMIT 10/11/100; split one chunk
into several; and express the same work through another interface. Legitimate
additional work and output are accounted for. A material, unexplained change in
strategy or repeated work is an investigation target, not an assumed defect.

Classify a demonstrated gap before changing code:

1. Missing capability: define and implement its shared semantic contract.
2. Inefficient general execution: correct the common operator, evaluator,
   algorithm, memory or data-movement behavior.
3. Lost optimization under composition: improve normalization, eligibility,
   lineage or strategy composition with explicit semantic conditions.

Group cases only when plans, reasons and observed work support a shared cause.
The workstreams are semantic reachability; safe predicate/projection reasoning;
expressions shared across consumers; aggregate composition; encoding and
materialization; algorithm selection; and setup/output/resource costs. Outer
joins, LIMIT, null predicates, effectful calls and error-producing expressions
can be real optimization barriers. Preserve those semantics.

Mixed aggregates should share scanning, filtering, hashing and group lookup where
legal. AVG state reuse must preserve accumulator types, decimal/overflow behavior
and empty/all-null semantics. Track repeated decode, dictionary/constant expansion,
intermediate materialization, buffer copies and row-object conversion where
observable. A decoded expression must not force unrelated columns to decode.

Evaluate build side/order, dense versus hash grouping, ordered reductions, Top-K
versus sort, batch size/parallelism and skew/pressure policies only after evidence
shows an opportunity. Preserve all earlier retain/drop decisions, especially the
[dropped item-15 batch](../benchmarks/native-workflow-drop-2026-10-25.md) and
region/preparation prerequisites. New evidence must justify a materially changed
mechanism; this program does not restart rejected candidates unchanged.

## Strict correctness contract

The comparator validates exact column names/order, logical types, decimal
precision/scale, temporal unit/timezone and applicable nullability/metadata before
values. Integer and decimal values are exact. NULL, zero, NaN, infinity, signed
zero and empty strings are distinct according to the declared operation policy.
Floating tolerances are explicit per operation/field, with absolute and relative
bounds and a stated special-value policy; there is no global rounding rule.

Unordered results use multiset semantics, preserving duplicates. Ordered results
validate the full declared ordering. For incomplete ordering with LIMIT, validate
the permitted tied outcomes; retain an original nondeterministic reproducer
unchanged. Generated performance representatives normally add deterministic tie
breakers. Approximate floating comparisons must not use a greedy row pairing
that can incorrectly reject or accept a valid multiset; require deterministic
keys or a complete bounded matching rule.

Check expected errors and their public diagnostic/category before crediting a
case: overflow, invalid casts, scalar-subquery cardinality, incompatible schemas,
unsupported domains, resource refusal and cancellation. Independent reference
calculations consume original fixture values, not candidate results. Metamorphic
checks state conditions for aliases, transparent wrappers, alternative layouts,
equivalent interfaces and partition/recombine. Such checks complement explicit
expectations; agreement among interfaces alone does not establish correctness.

Keep the original six end-to-end acceptance workloads: per-fund statistics;
entity/currency as-of valuation; timezone-aware daily reporting across daylight
saving transitions; nested parameterized multi-CTE composition; an explicit custom
batch transformation; and safe dataset refresh under its existing connector
owner. Preserve complete values/types, write/readback and applicable pressure,
spill, cancellation, changed-source and publication-failure cases. This broad
corpus extends those workflows rather than replacing them with a status matrix.

## Measurement and reports

Record logical/physical plans, selected provider/kernel/strategy, rejected
alternatives, input/output rows, physical read columns/bytes and observable
decoded/copied/materialized/output/spill bytes. Keep unknown counters explicitly
unavailable with their observation scope; do not infer zero from a missing field
or infer physical work from a logical row count. Retain peak memory, resource
requests, source/build identities, raw timing observations and complete results.

Distinguish fresh process, first query in an existing session, repeated session,
public prepared execution, native result production and complete delivery/export.
Record setup, planning, execution and delivery spans only where actually measured;
an unexplained process residual is not measured setup. Raw/prepared source state,
storage format and output consumption remain separate dimensions. Lazy plan
construction is not execution, and a fresh process is not a cold filesystem cache.

Routine timing communication uses engine time only. Other clocks and resource
details remain in diagnostic evidence unless requested. Profile separately from
low-overhead timing when necessary, with the same frozen requests and records.
Run timed candidates sequentially, keeping unrelated task workloads idle. Preserve
all unfavorable observations and record interference instead of silently replacing
samples. Set material relative and absolute thresholds prospectively after a
baseline/noise screen; do not select a favorable threshold after seeing a candidate.

| Tier | Initial scope | Purpose |
| --- | --- | --- |
| Tiny semantics | Thousands of inexpensive selected cases | Values, types, errors, nulls and composition |
| Developer smoke | About 100–200 representatives plus selected 1M cases | Fast, balanced family feedback |
| Broad 1M | Hundreds initially, growing toward 5,000–10,000 selected variants | Coverage gaps and unexplained cliffs |
| Focused candidate evaluation | Affected families, neighbors and stable controls | Decide a shared change against the current ShardLoom baseline |
| Larger/resource sentinels | About 50–200 selected cases as justified | Scaling, skew, spill, scheduling and large output |
| Existing ClickBench | Its frozen workload and acceptance procedure | Protect earlier gains where the change warrants it |

Do not multiply every case by every profile/layout/interface/lifecycle/repetition
on every change. Select representative crossings and report actual coverage.
Family-balanced reports keep capability, correctness, chosen-path coverage,
latency, memory/data movement, scaling, interface consistency and unexplained
cliffs separate. Unsupported, incorrect, crashed, timed-out and unexecuted rows
remain in denominators and visible outcome groups; they cannot disappear because
no successful timing exists. Do not pool thousands of easy variants into a score
that hides a smaller failing family.

## Completion and future comparison boundary

The coherent implementation stages are measurement/semantic foundation, generated
breadth and baseline, evidence-backed shared corrections and capability completion,
then continuous regression/exploration. Freeze a baseline before changing runtime
behavior. The concrete dependency between baseline discovery and candidate
selection is a valid PR boundary; individual methods or interfaces are not.

Every retained shared correction must demonstrate the unchanged reproducer,
predicted implementation/work change, neighboring types/nulls/cardinalities/layouts,
held-out variations, applicable public interfaces, unrelated representative
families and relevant larger/resource checks. Maintain ClickBench protection with
its existing proof or a specifically justified observation. New capabilities need
semantic, composition, performance and resource coverage plus discovery/reference
updates. A correct capability need not be faster than an unsupported predecessor.

The October 28 maintainer decision treats performance retention as an overall
workload tradeoff. Substantial complete-cohort gains, or substantial declared
family gains with a positive overall balance, can justify retaining a change
despite a small isolated timing regression. Record the original slowdown, its
absolute contribution, affected workload and remaining uncertainty. Investigate
in proportion to that impact and track a bounded follow-up when warranted;
an individual threshold flag is not an automatic implementation blocker.
Evaluate broad or consequential workload harm separately. Correctness, resource,
ownership and no-fallback requirements remain acceptance conditions. Preserve
frozen protocols and original observations; record this decision separately
instead of rewriting an earlier measurement or replacing a slower sample.

The specifically authorized single-observation Polars cohort may run during
baseline discovery. Only after ShardLoom's expanded surface passes its declared
acceptance should a broader comparison stage execute DuckDB/DataFusion references
and repeated Polars comparisons where semantics match. Freeze versions, resources, complete output,
source preparation and measurement mode. Keep common-source and prepared/native
comparisons distinct, including preparation/storage costs. Run engines sequentially
with a balanced order and preserve every sample. Disagreement requires diagnosis;
majority agreement is not proof. Relative latency may compare ShardLoom engine
time with the fastest valid comparable engine time, accompanied by individual
results and absolute differences. Missing or incompatible clocks are unavailable.

No runtime fallback, hidden external residual evaluation, package publication or
superiority claim is authorized. Foundry integration, AI effects, persistent
transactions, durable live state and distribution retain their existing contracts.

## Source basis and verification

Use existing focused Rust/Python targets for affected owners, then required
formatting, workspace Clippy/tests and applicable native/package/public-surface
checks at coherent boundaries. Corpus infrastructure needs corruption controls
for altered identities, missing/duplicate cases, weakened types/NULL semantics,
changed requests, erased failures and false parity/timing claims. Listing cases
or constructing plans is never credited as execution evidence.

The generation/coverage design uses NIST's
[Practical Combinatorial Testing](https://csrc.nist.gov/pubs/sp/800/142/final),
[coverage measurement](https://www.nist.gov/publications/combinatorial-coverage-measurement)
and [testing guidance](https://csrc.nist.gov/Projects/automated-combinatorial-testing-for-software/software-testing-methodology/dos-and-don-ts-of-testing):
declare constraints, measure actual interaction coverage and do not assume
pairwise coverage is sufficient. Vortex's public
[array](https://docs.vortex.dev/concepts/arrays) and
[expression](https://docs.vortex.dev/concepts/expressions) descriptions motivate
encoding-aware and deferred-work observations; the pinned repository source,
not current online documentation alone, governs the actual provider API.
