<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite streaming unary operation composition

Status: `NATIVE-STREAM-UNARY` is complete in PR #1559, following
completed set-operation integration in PR #1558. The
[acceptance report](../benchmarks/native-stream-unary-2026-10-15.md) records all
six Rust gates, 91 direct binding tests, 2,601 public cases and one complete
Full43 pass. Independent inspection of all 14,733 archive members passes.
All 44 hosted checks pass, including three installed-wheel lanes with 91 methods
each. The [273-member hosted packet](../benchmarks/evidence/native-stream-unary-hosted-2026-10-15.json)
retains source/merge identity, preview/production verification and the separate
postmerge Full43 observation. Keep native transport after the existing
optimization and breadth obligations.

## Scope and source basis

This finite breadth unit connects the nine existing, statically bound unary
kinds: DISTINCT, drop duplicates, duplicate masks, tail, sampling, scalar
rewrites, melt, explode and rolling. Preserve all their currently admitted
parameter policies and typed/nested domains. Dynamic pivot already has a
separate execution-time schema contract and must keep that path; this is not
permission to run a static pivot through ordinary unary completion.

The immutable inventory at revision `54f107d4` maps the ten supported binder
kinds (including pivot), public Python declarations, SQL lowering and 30 named
tests. The primary has checked all eleven source identities and every complete
test excerpt. These are source anchors, not streaming proof. The merged set
unit changes `local_primitive_relational_batch_input.rs`; every other inventoried
source retains its identity. Refresh all eleven hashes against merged revision
`e859b3a8b9d323587cee9641f5ebacd21e7e799c` before changing unary admission.

Existing architecture sources are `native-unary-workflows-2026-10-01.md`,
`native-unary-composition-2026-10-02.md`, the later typed/nested contracts, and
`native-stream-input-composition-2026-10-13.md`. The phase plan remains the queue
owner, under PERF-03/06/07/11/12 and CG-5/19/20/21. The expert comparator is an
encoded-columnar maintainer assessing operation order, unknown population size,
producer ownership, complete input observation and publication together.

## Shared implementation decision

Vortex-first decision: `use_vortex_native_provider`. Reuse pinned Vortex 0.85
native arrays/dtypes, scalar and validity access, compact native selection and
source/sink providers within the existing boundary. `BoundUnary`, its operation
states, `NativeBatch`, `OwnedRow` and `CompletedRows` own the existing semantics.
No new representation, kernel, executor, dependency or query-engine integration
is proposed. This unit connects finite input to an already composed native
operator family; it makes no zero-decode or zero-copy claim.

| Existing component | Required use and boundary |
| --- | --- |
| Complete-plan source counting | Recurse through supported non-pivot unary input before demand. Preserve depth checks, unsupported-kind refusal, dynamic pivot handling and repeated-source detection. |
| `Binder::bind` / `BoundUnary::for_relation` | Bind the actual preceding schema and existing canonical payload rules without consuming input. Do not widen unsupported kinds or dtypes merely to pass a fixture. |
| `BoundUnary::consume_relation` | Feed the same native operation state once and continue draining producer input after an operation's output stop condition. Empty output and a downstream zero limit must still expose late failure. |
| `NativeBatch::row_with_padding` / `OwnedRow` | Retain only admitted scalar payload or compact native nested selections, with credits attached. Verify that no producer owner survives the next demand. |
| Native result consumers and writers | Deliver provisional arrays or collect within the existing small-result bound; certify all finite sources and cleanup before final success/publication. |
| Existing query replay store | Handle repeated source references and fanout under explicit policy. Replay spilling does not add spill support to unary state. |

`consume_relation` already accepts an unknown input cardinality and never
counts rows by executing the input a second time. A conservative bound must
remain a bound. Direct file suffix-range avoidance, encoded providers and the
current capillary/PulseWeave/resource mechanisms keep their existing callers.

## Semantics and resource contracts

- DISTINCT and first-keep deduplication retain exact membership. Last-keep and
  remove-all deduplication retain the existing key, ordinal and candidate rules.
  Duplicate masks preserve first/last/remove-all behavior over the actual input
  stage. Future-dependent masks may retain identities for the input population.
- Tail over produced input retains the requested suffix in source order. A
  downstream limit may not move before that selection.
- Fixed-count sampling without replacement keeps its existing bounded candidate
  strategy. Unknown fractional populations and replacement may retain population
  state under the shared grant; expose deterministic denial when it cannot fit.
  Preserve seed, weight, tied-score, null/invalid-weight and source-order rules.
  Do not silently replace the current sampling algorithm with a different one.
- Rolling keeps existing null, minimum-period, centered/lookahead and output
  limits across batch boundaries. Scalar rewrites preserve typed binding, row
  ordinal and forward-fill state across batches. Melt and explode preserve
  expansion order, null/empty behavior and their admitted schemas.
- Every operation shares the admitted source, state, output and replay grant.
  Unary membership/population state remains resident; no new spill algorithm is
  proposed. Long input can pass only when its retained state fits. Explicit
  spill permission must not turn an oversized unary state into hidden resident
  intake or external execution.

## Acceptance

1. The frozen current runtime passes 18 baseline controls: nine streaming
   refusals before producer demand and nine complete resident controls. Preserve
   the exact source/binary, expected values and original envelopes. The sample
   baseline selects its full population and is not a sampling-algorithm proof.
2. Specify complete expected values before execution for each kind and existing
   option family. Include SQL and DataFrame declarations, empty/all-null input,
   renamed and narrow/large integer schemas, typed/nested payloads where admitted,
   duplicates, multiple batch sizes, and boundaries inside rolling/forward fill.
   Sampling requires an independent deterministic reference, including tied
   scores and invalid weights; comparing two engine frontends is insufficient.
3. Exercise operations before and after filters/projections, ordering, limits,
   aggregation, joins, sets, analytic windows, other unary stages and admitted
   dynamic pivot stages. Cover independent and repeated producers, preserve
   operation position, and assert one original execution plus explicit replay.
4. Test every compatible local writer and one-execution fanout through public
   streaming APIs. Reopen complete output, schema and values. Include typed empty
   output, source failure on both sides of a composition, late schema changes,
   initial/mid-input cancellation and early consumer closure. Final reports must
   remain absent after incomplete delivery; foreign/existing files survive.
5. Prove producer release before every subsequent demand, retained arrays after
   producer/plan/client teardown, final-owner credits and reusable clean session
   state after failures. Add native release-witness assertions, not only Python
   generator event checks.
6. Use cumulative input larger than the grant for bounded-state shapes. Add
   deterministic denial for high-cardinality selectors, future-dependent mask
   identities and unknown sample population state, with and without replay spill
   permission. Check native allocation peaks and cleanup without claiming an RSS
   ceiling. If a fixture does not reach its intended owner, revise the fixture
   with evidence rather than relabeling a different refusal.
7. Complete focused tests, six established Rust gates, direct native binding and
   full public matrices, one complete Full43 pass, portable source-bound evidence,
   generated docs/site and hosted/platform review. Preserve failed observations.

Independent references are predeclared for 54 sampling cases and 120 rolling
cases before candidate execution. Native pressure fixtures separately target
bounded state, wide membership keys, future-mask population identities and
repeated-source replay; their intended owners must be verified when executed.
The complete expected values, source identities and pressure owners now pass
local runtime acceptance and independent archive inspection.

This unit does not claim a new unary algorithm, timing improvement, package
availability, high-cardinality unary spill, execution resume, a process-wide
memory cap or completion of the six-area/eight-investigation contract.
