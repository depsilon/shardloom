<!-- SPDX-License-Identifier: Apache-2.0 -->

# Optimization coverage across the native engine

Status: active audit and implementation under the November 13 maintainer request
for complete optimization coverage. This extends item 20 and the existing
RFC 0044 owners. It is not a completed coverage or performance claim.

## Contract

Supported requests must reach applicable, admitted native optimizations through
the shared compiler and runtime, independently of their public interface or
equivalent declaration shape. Missing integration is work to complete. A
planning-only recommendation, an unimplemented mechanism, an unexplained
decline, or missing execution evidence cannot count as covered.

Coverage is evaluated for a mechanism at an operator/source/sink opportunity.
The earlier 193 whole-query file-provider cases and the later 2,379 cases with
file kernels are not denominators for all optimization. The 10,000 correct
native results establish the frozen corpus's execution and correctness result;
they do not establish that every applicable optimization ran.

The maintainer's follow-up makes this a query-by-query integration obligation.
A current admission restriction is not a completed optimization opportunity:
establish what work remains, connect the existing mechanism where possible,
and keep missing implementations open. Required keys, observed computations,
complete input populations and effects still define the answer the engine must
preserve. An operator that needs every input field has no unused-column work
to remove; that fact must be established from its dependencies, not inferred
from a missing counter. Competing algorithms need an admitted selection policy.
An applied mechanism's work counters do not prove its individual speed benefit.

## Scope and source baseline

The audit starts from the consolidated runtime at `4f2d1c842`, whose production
code is unchanged from `6e5881f38`. The intervening commit only removes a full
certificate dump from a synthetic test's failure message. PR #1683 has a separate
hosted acceptance boundary.

The maintainer explicitly includes existing and planned optimizations. Review
supported native sources, operators, data types, representations, output modes,
resource policies and public interfaces, and implement the planned mechanisms
through their existing design and evidence owners. General runtime filters,
skew handling and automatic cross-query reuse are included. The first bounded
implementations now pass the November 14 activation contract below; broader
opportunities remain open. Preserve rejected experiments and
the direct COUNT bridge's invalid baseline; this request does not turn those
observations into acceptance.

The maintainer's sequencing instructions keep the Sort validation connection
first, followed by Vortex scan decision/reporting gaps and demonstrated missed
execution, remaining COUNT/file-backed cyclic semi-join connections, and native
runtime filters, skew handling and automatic cross-query reuse through existing
RFC owners. The subsequent efficiency review treats these connected changes as
one coherent validation unit: prepare the uncovered operator/type/interface
declarations alongside implementation, then prove breadth and lifecycles against
the finished owners before the isolated affected-cohort acceptance decisions.
The phase plan orders the metadata/encoded-consumer follow-ons by dependency.
These are active deliverables. Inventory entries and future-work labels alone
do not satisfy them. Missing Vortex reporting must be distinguished from absent
projection/pruning execution, and existing partial implementations must be
reused and identified before adding mechanisms.

## Accepted activation unit — November 14

The [bounded activation unit](native-optimization-activation-2026-11-13.md) at
`f414d9dd3` connects executed scan evidence and whole-file projection pruning,
eligible grouped COUNT and file-backed cyclic semi-joins, post-key-validation
join membership filters, stable heavy-duplicate ranges and automatic held-source
segment reuse. Complete values, lifetimes, resource checks, public breadth and
both prospective timing gates pass in the
[7,498-case and five-lifecycle comparison](../benchmarks/native-optimization-activation-2026-11-14.md).
The coherent unit is retained; 5,189 cases and the complete COUNT subgroup
regress. No individual mechanism speedup is established.

This closes the four scoped activation deliverables in the phase plan, not
complete engine optimization coverage. Source runtime-filter pushdown,
join-build/result reuse, physical per-column/decode evidence and per-opportunity
decisions remain open. The metadata follow-on now starts from this valid accepted
baseline. Historical corpus results below retain their original source and scope.

## Source inventory at the audit baseline

The public relation declaration at the audit baseline has 22 variants. Its lowering uses
20 internal node variants, including the deliberately unexecutable `Unadmitted`
sentinel. These are different inventories: correlated/deferred/result
declarations lower through existing native owners rather than three additional
runtime enum arms. File, memory and batch are the three internal scan classes;
they do not enumerate every input format or its capabilities.

## Initial verified gaps

| Boundary | Existing behavior | Required disposition |
| --- | --- | --- |
| Flat pipeline versus equivalent nested transforms | File-kernel recognition traverses nested project/filter/limit nodes, but does not inspect the same stages inside a flat pipeline. | Share semantic recognition without rebuilding deep trees; preserve effect, error, range and resource barriers. |
| Direct-source prefix demand | Nested limits and direct column projections propagate safe prefix demand; every flat pipeline currently clears it. | Propagate the same safe demand through flat stages in reverse order, retaining completion barriers. |
| Direct-source column demand and predicate pushdown | The binder traverses nested projections, filters and ranges, but flat stages stop payload pruning and alias-only filter pushdown. | Reverse column demand through equivalent flat stages; retain unused computations and effect barriers; preserve physical predicate rewrites. |
| Rolling output demand | Nested projections/ranges can cap the admitted rolling output; flat stages are not traversed. | Establish equivalent semantics and tests before extending this owner. |
| Native-memory output credit window | An admitted single projection loses admission when represented as one flat stage. | Admit the equivalent one-stage form with identical row, field, partition and credit limits; retain the existing multi-stage exclusion. |
| Empty filtered COUNT certificate | A completed count over an authoritative empty file returns zero but fails certification because it did not read data. | Certify the exact empty-source/completed-scan proof; reject missing or contradictory evidence without inventing data reads. |
| Decision evidence | File admission has explicit internal decline strings; the completed report identifies the unified pipeline and successful kernels but does not preserve every declined opportunity. | Keep missing decision coverage visible; reuse bounded native evidence owners when expanding reporting. |
| Planned optimizer surfaces | General runtime-filter/skew/cache and dynamic-feedback planning records do not establish execution. | Retain their planned status and independent implementation/evidence contracts. |

These observations are a starting inventory, not an exhaustive gap list. Source
review must cover the actual join, aggregate, set, window, expression, source,
retention, spill and writer owners before a whole-engine completion claim.

## First implementation unit: equivalent pipeline forms

Implemented locally at `9f4580c723f876f087d3e75fb56153b4b099ef20`. The
[original 10,000-case pass and requested 347-case pass](../benchmarks/native-optimization-coverage-2026-11-13.md)
preserve the exact measured source and complete-result verification. This closes
the first six integration gaps above, not the broader coverage request. PR #1683
merged separately as `72680743a34a5db95f748045ca7939f5c3c739fd`; that merge does
not include these later local changes.

Use a borrowed view of existing bound nodes and flat stages for file-kernel
recognition. The view is semantic inspection only: it does not execute, clone
bound programs, create a second engine, or restore whole-query retry. Keep
every existing type/null/order/float-policy check and memory reservation.
Unknown or effectful stages remain barriers. Flat stage traversal must remain
iterative so long public pipelines do not become recursively allocated trees.

Source demand walks flat stages backwards using the same rules as nested
nodes: direct field selections preserve demand; a range adjusts the requested
prefix; computed projections, filtering and completion/effect obligations stop
that propagation at their boundary. Earlier ranges still have their own
semantics and may limit the population an effectful later stage observes.

Column demand separately propagates through flat projections, filters and
ranges. It retains every computed expression, even an unused one, along with
hidden predicate inputs, and rebinds pass-through schemas after pruning. No
new demand means no repeated traversal/charging of the preceding stages.
Alias-only filter pushdown traverses both forms. Existing physical predicates
keep their embedded-derived rewrites; pinned Vortex conjunct extraction and
balanced AND construction prevent long flat filters from forming deep trees.

The single-stage native-memory output window retains the same one-projection
admission, exact partition, two-slot acknowledgment and byte bounds. Multi-stage
admission remains unextended work. An empty filtered COUNT uses
the authoritative source population and completed scan, with no fabricated read
or decode work; missing source/scan proof continues to block certification.

Classification for the Vortex-first check: `use_vortex_native_provider` through
existing owners. Pinned Vortex arrays, predicates, projection and file/scan
providers already own physical values and I/O. This change concerns ShardLoom's
bound relational declaration forms; no upstream dependency or provider API is
added. Vortex-native input/output, exact result semantics, source-generation
checks, cancellation, credited ownership and terminal failures remain required.

## Retained connection: downstream demand through Sort

The owned-source column-demand walker stopped at an already-bound Sort. A
downstream projection therefore could not remove unused payload fields from
the Sort input or source. The new connection combines downstream fields with
every sort key, recursively narrows the existing child, and rebinds Sort's
fields and raw-field observation indices. It does not push a row prefix across
sorting. Unused computed expressions, hidden keys, complete input consumption,
late failures, stable ties, null ordering and source validation remain required.

Two regression cases fail before the connection: an unused corrupt Parquet
column is read, and a nullable ordering case reads three columns instead of two.
After the connection, all 109 source-demand tests pass, including the four new
tests for those cases, late key/computation failures, empty results, and raw
vector output ownership. Complete default/native checks, the release build and
442 installed-package tests subsequently pass at `cac86e8b0`.

An independent reconstruction of all 10,000 frozen declaration identities finds
606 Parquet/Arrow IPC queries with downstream Project demand reaching Sort
through Limit. Retaining hidden keys and every computed projection means only
117 of those declarations can narrow their source columns: 82 Parquet and 35
Arrow IPC ordering queries. The other 489 require their existing fields. These
were static dependency results. The frozen 606-query observation then passes
all complete results and all required-column comparisons; 98 previously wide
cases narrow. Engine time is 85.218734162 to 45.164316757 seconds for that cohort.
The earlier 5,309 count described operators feeding Sort and must not be used
as the affected-query count. Physical-read increases and changed ranked-read
admissions remain in the [observation report](../benchmarks/native-optimization-coverage-2026-11-13.md#shared-source-demand-connections).

## Retained connection: cardinality and unary dependencies

The complete declaration audit finds fourteen filtered COUNT(*) cases choosing
an unnecessary row carrier and eighty-two list-expansion reductions retaining
an unused scalar field. The shared binder now chooses an already-required
predicate field for value-free demand, and propagates output demand through
bound expansion while retaining every expanded list. All bound unary requests
may propagate their explicit keys, values, weights and predicates to an owned
input. The correction also retains hidden predicates that composed unary
operators execute residually, without leaking them into output.

Name/type binding, computed expressions, zip-length errors, parent-null behavior,
source population, ranges, completion, cancellation and resource credits remain
with their existing owners. No prefix is pushed across population-changing
operators. Prepared Vortex and caller-source validation obligations remain in
force; the [inventory](native-optimization-inventory-2026-11-13.md#native-vortex-and-validation-obligations)
records their distinct selected-value and streamed-build boundaries.

At `c79f27a82`, eleven new regressions and all 120 source-demand tests pass,
followed by complete default/native checks and 442 installed-package tests. The
frozen 213-query cohort contains all 196 original expansion declarations and
all seventeen scalar argument-free counts, spanning five interfaces and three
formats. All complete results pass; all 96 targeted cases narrow and all 135
comparable source-column counts match. The 78 Vortex controls remain outside
that counter's scope. Cohort engine time is 19.627180451 to 18.502198420 seconds.

The two cohorts are disjoint and separately source-bound. Their totals are not
combined into a synthetic current 10,000-query result. The
[mechanism inventory](native-optimization-inventory-2026-11-13.md) records the
broader remaining work, including 3,706 Vortex declarations without a comparable
saved scan contract and missing per-opportunity decisions for other owners.

## Preserve the Sort payload contract during file-kernel admission

A bounded file SortRows kernel observes its ordering keys and winning payloads.
The bound relational Sort additionally validates its original declared payload,
including fields hidden by a later projection and rows outside a later range.
Checking only the final provider output schema therefore does not prove that
the two operations are equivalent. The regression reproduces skipped NaN
validation for narrow and wide records, and skipped malformed text when source
columns occupy separate Vortex layout children.

Admission now inspects the original bound Sort schema before selecting that
file kernel. Floating and rich payload types, and text not covered by an ordering
key, retain the existing shared native Sort. Integer, boolean and null payloads
do not introduce the additional value-domain checks; text ordering keys retain
the file kernel's complete decoded-key validation, including the same source
text carried under another alias. A projection before Sort
may already remove an unused field and remains eligible. A projection after
Sort cannot erase its validation obligation. Aggregate lowering retains its
independent output-validation contract.

The decision uses metadata only, for both nested and flat declarations. It
does not retry a failed kernel, run a second source scan during admission, or
change the source generation, native execution, resource or cancellation owners.
The Vortex-first decision remains `use_vortex_native_provider`: existing Vortex
arrays, scans and native payload validators supply the implementation. No new
provider abstraction or dependency is introduced.

This is a correctness correction, not a performance optimization or completed
Vortex source-demand connection. A valid wide source can do more work on the
shared Sort because its established validation was previously skipped. Further
work reduction must preserve the complete validation domain, parent masks and
failure behavior, and pass its own source-bound comparison. The six new tests
cover invalid and valid payloads, prior filtering/projection, text ordering,
flat/nested declarations, both physical layouts and exact resource refunds.

The [source-bound correction cohort](../benchmarks/native-optimization-coverage-2026-11-13.md#sort-payload-validation-correction)
contains 388 complete cases at `4d5fa4a46268380a634e78f057abba087f4042f5`,
one execution each. Answers, schemas, certificates and engine clocks were
independently replayed; every case reports the common native pipeline without
catch-all execution, external fallback, denied reservations or spill. All 21
prior file-Sort shortcuts were removed, while all 36 other file-kernel cases
remain. The ten owned-source controls retain identical column counts. The
337.804658081-second candidate sum corrects skipped validation and is not a
speedup claim. Its historical groups retain separate baselines from `cac86e8b0`
and `9f4580c72`; they are not a combined historical total.

At this source, 3,325 default and 7,145 native tests pass (39 native ignored,
including 4,172 passes and 36 ignores in the full native library suite), and
the installed package passes 442 tests with no skips. The source-bound checks
compare 5,691 identities against fingerprint
`1f1882408d996c1b5a9ccb42c89adceb84036b8592ac8d523b10b0e4637baaff`. The
[portable packet](../benchmarks/evidence/native-sort-payload-validation-2026-11-13/README.md)
preserves 120 members; relocation replay, 19 negative controls and optimized
Python refusal pass. Its verifier does not repeat the full oracle. This evidence
does not complete Vortex demand coverage or close item 19, item 20, RFC 0044 or
CG-1 through CG-23.

The [shared prefix connection](native-sort-prefix-validation-2026-11-13.md)
is the subsequent, separately measured optimization retained at `788a8925e`.
It preserves
complete validation while constructing only retained nested/decimal payload
rows and reuses the existing typed selection validator for flat payloads.
Its complete 388-case comparison decreases engine time from 337.804658081 to
327.589944482 seconds, with complete-source validation and portable proof.
The November 14 activation unit above then uses that accepted runtime as its
baseline; these are separate observations.

## Acceptance and remaining work

- Prove matching complete results and optimization admission for equivalent
  nested and flat declarations, including aliases, filters, aggregates and
  ranges where the existing kernel is semantically equivalent.
- Exercise null/empty inputs, rich or unsupported provider types, overflow and
  late errors, observations/application effects, changed generations,
  cancellation and resource refusal at the touched boundaries.
- Preserve independent output expectations and negative admission controls;
  a new assertion must fail on the original missed opportunity.
- Run focused native regressions, formatting, strict workspace lint and tests,
  and the relevant native/public-surface checks for the resulting implementation.
- Freeze any required performance protocol separately. Reuse immutable saved
  observations where they answer the question; do not relabel historical runs
  as evidence for new code or add unapproved benchmark repeats.
- Expand the mechanism/opportunity inventory through the remaining owners and
  close verified gaps before claiming complete optimization coverage. Keep
  missing connections, unimplemented mechanisms and unproved work savings open;
  an admission restriction alone does not close them.

This unit does not close item 19, item 20, RFC 0044 or CG-1 through CG-23. Existing
correctness and benchmark gates, including CG-5 and CG-6 for competitive claims,
remain independent. No external execution fallback, release, merge or package
publication is part of this coverage request.
