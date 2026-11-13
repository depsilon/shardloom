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

An optimization may legitimately be inapplicable because of semantic, physical,
resource, lifecycle or effect constraints. Such an exclusion needs an explicit
condition and a negative control. It must not conceal a missing implementation.
Two competing algorithms need an admitted selection policy; invoking both is
not a coverage requirement. An applied mechanism's work counters do not prove
its individual speed benefit.

## Scope and source baseline

The audit starts from the consolidated runtime at `4f2d1c842`, whose production
code is unchanged from `6e5881f38`. The intervening commit only removes a full
certificate dump from a synthetic test's failure message. PR #1683 has a separate
hosted acceptance boundary.

The maintainer explicitly includes existing and planned optimizations. Review
supported native sources, operators, data types, representations, output modes,
resource policies and public interfaces, and implement the planned mechanisms
through their existing design and evidence owners. General runtime filters,
skew handling and automatic cross-query reuse are included; they remain missing
capabilities until executable implementations pass their acceptance contracts.
Preserve rejected experiments and
the direct COUNT bridge's invalid baseline; this request does not turn those
observations into acceptance.

The public relation declaration currently has 22 variants. Its lowering uses
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
admission remains a separate existing exclusion. An empty filtered COUNT uses
the authoritative source population and completed scan, with no fabricated read
or decode work; missing source/scan proof continues to block certification.

Classification for the Vortex-first check: `use_vortex_native_provider` through
existing owners. Pinned Vortex arrays, predicates, projection and file/scan
providers already own physical values and I/O. This change concerns ShardLoom's
bound relational declaration forms; no upstream dependency or provider API is
added. Vortex-native input/output, exact result semantics, source-generation
checks, cancellation, credited ownership and terminal failures remain required.

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
  close verified gaps before claiming complete optimization coverage. Record
  unknown, missing and legitimate exclusions separately from executed work.

This unit does not close item 19, item 20, RFC 0044 or CG-1 through CG-23. Existing
correctness and benchmark gates, including CG-5 and CG-6 for competitive claims,
remain independent. No external execution fallback, release, merge or package
publication is part of this coverage request.
