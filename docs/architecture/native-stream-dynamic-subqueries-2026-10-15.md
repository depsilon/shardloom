<!-- SPDX-License-Identifier: Apache-2.0 -->

# Declared streaming inputs for dynamic subqueries

Status: integrated through [PR #1561](https://github.com/depsilon/shardloom/pull/1561)
after all 44 checks passed, with three installed-wheel lanes and
[preview/production verification](../benchmarks/evidence/native-stream-dynamic-subqueries-hosted-2026-10-16.json).
This contract was prepared before runtime edits. The
[acceptance report](../benchmarks/native-stream-dynamic-subqueries-2026-10-16.md)
retains all six Rust gates, 100 direct methods, 4,827 public cases and one
complete Full43 pass. The preceding `NATIVE-STREAM-SUBQUERIES` unit is integrated through
[PR #1560](https://github.com/depsilon/shardloom/pull/1560) at
`c30846afb0ab5abfff6fdb5d95ed20f1c10a9477`, with all 44 checks and
[hosted/production verification](../benchmarks/evidence/native-stream-subqueries-hosted-2026-10-16.json)
complete. This finite unit is `NATIVE-STREAM-DYNAMIC-SUBQUERIES` under
PERF-03/06/07/11/12 and CG-5/19/20/21. Preserve the six-area/eight-investigation
completion contract, all CG-1 through CG-23 and the later transport dependency.

## Decision and scope

Admit finite streaming input to the existing parameterized dynamic predicate
subqueries: IN/NOT IN, row membership, EXISTS/NOT EXISTS and quantified ANY/ALL.
The inner native plan may discover pivot columns separately for each outer
parameter. Its possible input identities must be declared before the plan can
demand those inputs. This is an extension of native declaration and ownership
handling, not a new subquery evaluator or a permission to read all inputs eagerly.

The expert comparator is an encoded-columnar engine maintainer checking source
admission, nested input lifetimes, parameter-specific schemas, memory release,
selected evaluation and complete publication together.

Vortex-first decision: `use_vortex_native_provider`. Reuse the pinned Vortex
0.85 array, dtype, validity and scalar surfaces through `vortex-local-primitives`,
and existing native replay storage through `vortex-write`. ShardLoom's current
schema binder, pivot state, correlated executor, native payload detachment and
credited replay remain the providers for this work. No new representation,
external engine, kernel, dependency or query-engine integration is needed.
Execution and Native I/O certificates must continue reporting actual copying,
replay, schema discovery and completion, with `fallback_attempted=false` and
`external_engine_invoked=false`. No zero-copy, zero-decode or speedup claim is made.

RFCs 0013, 0014, 0015, 0017, 0021, 0031, 0032 and 0033 and the preceding finite
streaming-subquery contract remain applicable. Source review covers the dynamic
binder, correlated executor, batch source-role classifier, deferred plan
references, SQL declaration/source collection and dynamic predicate lowering.

## Declaration and lifetime contract

Keep the existing opaque `defer_subquery` API and its resident-input behavior.
Opaque declarations remain inadmissible when streaming source roles cannot be
known. Add an explicit source-declaring preparation entry point for callers that
can provide the complete finite set of possible inner inputs. SQL/Python lowering
must derive that set from its existing parsed source walkers and resolved source
map; callers of those public front doors need no new syntax or mode flag.

The declaration has these requirements:

- It names only already registered native sources, uses existing source identity
  rules, and contains at most the existing 128-source limit. An empty set is
  valid for an inner plan that uses only `Outer` and source-free operations.
- Source identities and retained declaration storage consume the same metadata
  grant before allocation. Deduplicate identities without weakening the bound.
- The reference remains execution-scoped, single-use and valid only as the
  immediate relation of a correlated subquery. Cloning it does not authorize
  another consumer. Absent, foreign, reused, misplaced or unconsumed references
  retain deterministic rejection.
- Record the declared identities in immutable reference metadata available to
  source-role classification before binding can perform schema-discovery reads.
  Keep that metadata and its credit alive through the corresponding execution.
  A retained clone remains invalid in another execution and keeps its source
  metadata reservation until the last reference is dropped.
- Install the source restriction before invoking the per-parameter lowerer.
  Validate every native subtree before schema discovery or execution can demand
  data. A lowerer may choose among declared sources; it may not add or access a
  registered-but-undeclared source through a scan, schema-resolution subtree or
  nested declaration. Nested source declarations must be subsets of the parent
  restriction. Opaque nested callbacks must not escape a parent's restriction.
- Preserve existing complete-query reuse declarations before dynamic outer
  lowering. Do not discover replay requirements after an unrecorded producer
  has begun. Late declarations may only strengthen requirements when existing
  input state proves that the new requirement can still be satisfied.

Per-parameter binding creates fresh inner operation state and authoritative
output columns. It must release that state and its credits before the next
parameter where the current executor does so. Completed schema references,
input cursors and replay owners must not leak into another prepared execution.

## Source roles, demand and completion

Treat a declared deferred relation as an inner source occurrence for each listed
identity, without executing the callback merely to classify it. Reuse the
existing role merge rules:

| Situation | Required behavior |
| --- | --- |
| Independent inner source | Record native replay on demand for repeated parameters; do not preload it solely because it is declared. |
| Outer input before parameterized inner demand | Detach the current outer batch under credit, then verify that its original owner has been released before requesting another producer. |
| Source shared by outer and a possible inner relation | Complete raw replay before the first query read when an active producer could otherwise be recursively reopened. Reuse already completed replay after earlier dynamic discovery. |
| No selected outer parameter | Do not call the inner lowerer, evaluate inner expressions or build unused pivot/subquery/replay state. Validate unread finite inputs through their raw completion path. |
| A declared source is not chosen by the callback | Validate it before final success; do not evaluate a query over it merely to force completion. |
| An undeclared source is requested | Fail before demand of that source, preserve failure evidence and discard provisional results and owned staging state. |

All source and output completion rules from the static-inner unit remain in
force. Raw validation must detect bad schemas, ownership/session mismatch,
bounds violations, late errors and absent completion even for unused branches.
It must not evaluate skipped expressions or invent source/replay counters.

Replay, outer and retained-inner copies, parameter values, pivot state, schema
metadata, membership state and output share the admitted grant. At this unit's
original acceptance, native replay spill and pivot spill retained distinct
contracts, and replay spill did not make membership state spillable; resident
membership state refused pressure deterministically, cleaned up and allowed a
fresh execution. The later finite
[predicate-spill contract](native-subquery-membership-spill-2026-10-19.md)
admits explicit native spill for shared-relational `IN`, `EXISTS` and quantified
`ANY`/`ALL` predicates, including negation and indexed, parameterized or
declared-schema inner execution. Replay, inner pivot and predicate populations
remain separate owners.

## Acceptance frozen before implementation

The unchanged PR #1560 candidate already passes 32 baseline controls: sixteen
streaming refusals before any producer opens and sixteen exact-value resident
controls. They cover eight predicate/empty-outer cases with both independent and
shared source identities. Retain their full declarations, expected values,
producer events, binary identities and source hashes as counterfactual evidence.

1. Change those sixteen streaming refusals to exact-value success while retaining
   the resident controls. Exercise empty batches and multiple batch boundaries;
   demonstrate different pivot columns for different outer parameters.
2. Cover every existing dynamic predicate family, negation, all six quantified
   comparisons, empty and null inputs, duplicate values, row keys and declared
   typed payloads. Compare complete values and schemas with explicit expected
   results, not only row counts or an execution status.
3. Cover independent and same-source replay, nested deferred predicates,
   repeated sources inside an inner join/set, dynamic outer discovery and mixed
   resident/file/stream inputs. Exercise repeated prepared execution with fresh
   producers, retained outputs and original-owner witnesses.
4. Test empty outer input and false/null selected guards with an inner callback
   that would fail if evaluated. Unused producers must still complete and their
   late failures must prevent success or publication without allocating replay.
5. Test declaration bounds, unknown and registered-but-undeclared sources,
   nested restriction escape, undeclared scans hidden inside schema discovery,
   foreign/double/unconsumed references and late role changes. Verify rejection
   before demand of the forbidden source and recovery after failure.
6. The original acceptance proved memory denial for declaration/copy/pivot/
   membership state, replay with and without explicit spill, cancellation during
   discovery/replay, consumer failure and writer/fanout cleanup. It verified
   source-replay and pivot-state spill separately; do not infer one from the
   other's counters. The later predicate-spill contract has separate pressure
   evidence for predicate populations.
7. Run focused native tests, direct installed-binding tests and public
   SQL/Python consumers/writers, followed by the required workspace/native gates
   and one complete Full43 pass. Report engine time only in routine updates.
   Freeze a concrete acceptance protocol and source inventory before candidate
   acceptance runs; retain all failures and fixture corrections.
8. Independently inspect complete archived values, source/binary provenance,
   no-fallback reports, resource/cleanup evidence and CI coverage. Align source
   and generated documentation, preview/production evidence and the phase ledger
   before marking this finite unit integrated.

## Alternatives, risks and remaining boundaries

The first frozen direct candidate exposes a frontend gap before guarded dynamic
predicates reach native execution: scalar expression parsing treats relational
predicate values inside CASE branches as function calls. Reuse the existing
explicit-predicate recognizer and relational predicate binding in scalar value
positions, preserving CASE/COALESCE demand guards, source discovery and the
separate static-output restriction for scalar-value subqueries. Add focused
parser and execution regressions before rebuilding the candidate. This does not
add a new predicate evaluator or dynamic scalar dtype inference.

The same attempt retains 285 live Arrow results before the explicit 32-MiB
grant refuses another result. Releasing those owners refunds all credits and a
fresh query succeeds. Split the matrix's retention groups without raising the
grant. Its row-membership fixture also needs distinct source columns under the
existing parser contract; preserve that refusal and all original declarations.

The rebuilt candidate passes the four direct binding tests and 727 public cases
before a composition fixture calls a DataFrame-only pivot method on a scoped SQL
workflow. Correct the independently authored SQL fixture to combine its declared
source bindings directly; keep DataFrame and SQL construction separate. Retain
the failed run and source snapshots. The correction changes only two acceptance
modules, so the frozen CLI, binding and production sources remain applicable.

The next run passes 824 cases before the nested SQL fixture reaches its existing
conservative declaration-credit limit. Twelve separately predeclared diagnostic
calls confirm that all six nested predicate/source combinations refuse a 32-MiB
grant before producer demand and return complete exact values at 64 MiB. Keep
those six 32-MiB refusals as explicit acceptance cases and use the proven 64-MiB
grant for their successful counterparts. Other composition cases keep 32 MiB.
This retains the reservation policy and does not claim that credited bytes equal
physical memory usage or a whole-process memory ceiling.

The next attempt retains 842 successes, followed by an outer-pivot fixture that
feeds a null amount to the existing non-null SUM path. Its projected result uses
only the entity key, so use a non-null unit amount while retaining the null key.
A separate complete diagnostic of the remaining 229 cases finds 135 successes
and 94 failures: eight instances of that fixture and 86 Python declarations
that reject predicate values before native execution. Extend the existing scalar
branch renderer to preserve typed predicates in CASE, COALESCE and NULLIF,
including their source bindings and resource ceilings. Keep string arguments as
literal strings. Add declaration and public execution regressions; preserve the
failed source snapshots and distinguish this Python change from the unchanged
frozen native executable and binding.

Default-feature validation finds that the source-identity field has no reader
without the Unix native execution provider. Gate that field with the same
`vortex-local-primitives` and Unix conditions as its constructor and consumers.
The enabled native definition remains unchanged. A subsequent lint correction
changes one empty-vector assertion inside a test to an explicit equality assertion.
Retain both failed checks, the exact before/after sources and fresh complete
validation. These corrections do not change the production code exercised by the
frozen native CLI and binding; the public and direct results retain their original
source identities and a separately verified link to the final verification source.

Blindly allowing opaque callbacks loses the ability to establish replay and
source ownership before demand. Eagerly materializing every registered source
would add unnecessary work and can exceed the grant. Declaring a finite source
set and enforcing it preserves the current native executor and demand model.

The main risks are a source hidden in a nested expression, a restriction checked
after pivot discovery has already read rows, an original producer owner retained
across nested demand, and credits surviving one parameter or execution. The
source walkers, pre-demand checks and acceptance cases above target those risks.

Dynamic scalar-value subqueries still require a separate output-schema contract:
their output dtype is needed by the outer expression binder. This unit admits
the dynamic predicate families already supported for resident input. This unit's
acceptance did not claim new scalar dtype inference, arbitrary runtime source
discovery or general subquery-state spill; the later finite
[predicate-spill contract](native-subquery-membership-spill-2026-10-19.md)
admits only its scoped native predicates. Broad SQL parity, production readiness,
transport support and package publication remain outside both claims. Published
package artifacts remain immutable.
