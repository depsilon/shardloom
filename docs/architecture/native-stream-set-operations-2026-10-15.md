<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite streaming set operations

Status: local acceptance complete; hosted integration remains pending. This
cohesive breadth unit under PERF-03/06/07/11/12 and CG-5/19/20/21 follows the
destination work in PR #1557. The [acceptance report](../benchmarks/native-stream-sets-2026-10-15.md)
records all six Rust gates, 4,241 native tests, 87 direct binding tests, 1,121
public cases across 22 families and one complete 43-query regression pass.
The six-area/eight-investigation completion contract stays open. Native transport
follows the existing optimization and breadth work.

## Scope and existing behavior

Admit finite streaming producers through the four existing set operations:
`UNION ALL`, `UNION` (distinct), `INTERSECT` and `EXCEPT` (distinct). Reuse the
ordinary native binder, positional schema alignment, common-type admission,
null-equal full-row comparison and output ordering. `INTERSECT ALL` and
`EXCEPT ALL` remain unsupported; this unit adds no set variant or Python method.

The frozen pre-change CLI passes twelve baseline checks: eight explicit streaming
refusals before producer demand and four complete resident controls. Both
independent and repeated producers hit the blanket set admission restriction.
The existing native executor already processes both branches through `run_set`;
the pre-change admission walk refuses every `Set` node instead of counting its
source references. The baseline helper, complete values, errors and source/binary
identities are retained in `stream-set-baseline-480`.

## Reuse and provider decision

Vortex-first decision: `use_vortex_native_provider`. Keep pinned Vortex 0.85
arrays, dtypes, validity, casts, compact payload gathering and native source/sink
providers inside the existing feature-gated boundary. The existing ShardLoom
`RowSet`, `RowIndex`, `Table`, shared allocation and finite-input adapter already
supply row membership, ownership and completion. Upstream Vortex primitives do
not replace the engine's producer identity, once-per-execution replay, full-row
set semantics or publication contract. No new representation, executor,
dependency, query-engine integration or fallback is needed.

| Existing component | Use in this unit |
| --- | --- |
| `count_sources` | Traverse both set branches and saturate the source-use count at two, as for joins. Preserve depth and unsupported-subtree checks. |
| `Binder::set` and `run_set` | Preserve positional schema alignment, existing type admission, left field names and all four existing native algorithms. |
| `RowSet` / `RowIndex` / `Table` | Retain compact first-occurrence native rows under credits; hash matches still require full equality. Duplicate producer payload is released before the next demand. |
| Finite-input adapter and replay recorder | Maintain independent URI/ordinal/end state; repeated references retain one native replay and never reopen a producer. |
| Existing completed-result consumers and eight writers | Deliver provisional native batches, certify every source's end and final cleanup, then publish through the accepted sink contract. |
| Existing query run store | Spill explicitly declared source/output replay and already supported surrounding operators. It does not imply that set membership itself spills. |

This is admission and lifecycle integration, not a new set algorithm or an
optimization claim. Capillary batching, shared memory policy and PulseWeave
scheduling remain the existing mechanisms. Native Vortex remains the
highest-fidelity persistence target; compatibility limits and loss stay explicit.

## Semantic and ownership contract

`UNION ALL` emits left rows followed by right rows, preserving multiplicity.
`UNION` emits the first occurrence in that combined order. `INTERSECT` and
`EXCEPT` build right-side membership and emit qualifying distinct rows in
left first-occurrence order. Nulls compare equal for set membership. Preserve
the existing typed/nested key rules and rejection of unsupported key domains.
These internal stable-order observations do not create a general SQL ordering
guarantee beyond an explicit `ORDER BY`.

Traverse the complete plan before demand, including sets nested under or around
joins, sorting, grouping, windows, limits and admitted dynamic stages. Repeated
references must be declared before their first scan and use the already accepted
replay owner. Independent producers keep independent ordinals. A limit of zero,
an empty intersection or a filtered-out branch must still observe all admitted
producers' explicit completion; no successful partial result may hide late input
failure. Unsupported subtrees and excessive recursive depth remain early refusals.

Every retained payload must outlive its producer through its own credited native
owner. Output retained by a caller remains valid after producer and prepared-plan
teardown, and keeps its credits until the final output owner is released. Check
the producer's release witness before every subsequent demand and across switches
between sources. Error, cancellation and consumer closure release all engine-owned
state and preserve the original producer exception at the Python boundary.

## Memory and spill boundary

`UNION ALL` requires no distinct-row membership state for independent sources.
The other three operations retain distinct membership and emitted-row identity
under the same query allocation as input, output and surrounding operators.
Their supported bound is the available admitted memory for those owners, not an
unbounded count of input rows or a process-RSS promise. Long low-cardinality input
must complete with cumulative input substantially exceeding the grant while
retained distinct state fits. High-cardinality state must be refused
deterministically before uncredited growth, with complete cleanup and no final
success or destination publication.

Set membership itself remains resident and does not gain a spill algorithm in
this unit. Explicit spill permission remains useful for repeated-source replay,
result fanout and already admitted sorting/grouping/join/window stages in the
same plan; all share the existing store and quota. Test resident membership
refusal both with and without that permission. Describe this scope explicitly
instead of reporting replay runs as spilled set state. No new fallback or hidden
whole-input collection is permitted.

## Acceptance

The expert comparator is an encoded-columnar maintainer checking full-row set
semantics, type alignment, ownership release, finite source completion and
memory pressure across normal user workflows.

1. Cover all four operations over independent, repeated, mixed resident/file and
   empty producers. Compare complete nullable, duplicate, typed and nested values,
   schema and defined output order with an independent small reference.
2. Cover nested sets and composition with filters/projections, zero and nonzero
   limits, ordering, aggregation, joins, windows and admitted dynamic stages.
   Assert per-source demand, one original execution, replay counters and completion.
3. Check output ownership past producer/plan teardown, source release before every
   demand and final memory refunds. Long duplicate input must exceed the grant
   cumulatively while completing; distinct-state growth must refuse safely.
4. Exercise actual native source replay under pressure, quota/corruption failures
   and cleanup using existing mechanisms. Distinguish its runs from set state.
5. Cover initial and late cancellation, failure from either producer, late schema
   failure and consumer closure. Writers/fanout publish only complete results;
   existing and foreign destinations remain intact.
6. Exercise public SQL, DataFrame set methods, subprocess batches and the direct
   binding through collection, incremental output, all admitted local writers and
   mixed fanout. Reopen native and compatibility outputs and compare every value.
7. Complete focused/native/direct/public regression, the required workspace gates,
   one complete Full43 pass, source-bound portable evidence, documentation and
   hosted/platform checks. Preserve failures and original identities. An input
   ingest rerun is unnecessary for this admission change.

No package release, high-cardinality set spill, resume, distributed execution,
transport, process-wide memory cap or performance superiority follows from this
finite unit. Other stateful families, allocations and investigations retain their
existing roadmap owners.
