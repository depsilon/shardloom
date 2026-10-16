<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite streaming subquery composition

Status: finite runtime implementation and local acceptance complete for
`NATIVE-STREAM-SUBQUERIES`, following unary integration in PR #1559. The
[acceptance report](../benchmarks/native-stream-subqueries-2026-10-15.md) records
the exact proof and remaining hosted gate. The phase owners remain
PERF-03/06/07/11/12 and CG-5/19/20/21. Preserve the complete Competitive Engine
Track, the six-area/eight-investigation completion contract and transport's
dependency on that work.

## Scope and provider decision

Connect finite streaming producers to the existing native IN (including row IN
and negation), EXISTS/NOT EXISTS, quantified ANY/ALL and scalar subquery kinds.
Include uncorrelated, indexed correlation and parameterized `Outer` plans with
statically bound inner relations. Preserve existing SQL/Python declaration,
null, empty-set, comparison, scalar-cardinality and selected-branch semantics.
Compose with the already admitted native operators, execution-time outer
lowering and local result consumers/writers.

Vortex-first decision: `use_vortex_native_provider`. The pinned Vortex 0.85
array/dtype/validity/scalar surfaces already serve this boundary through the
`vortex-local-primitives` feature, with `vortex-write` for native replay runs.
Reuse native compact payload selection and the existing
credited replay store. The existing ShardLoom binder and `Subquery` state own
relational semantics; no new kernel, representation, executor, dependency or
query-engine integration is needed. Report the actual native copies and input
completion through the existing execution and Native I/O certificates, with
`fallback_attempted=false` and `external_engine_invoked=false`. This change
makes no zero-copy or zero-decode claim.

The source basis is merged revision
`ff9aa27338ca9f1ff5581f5a57603400f5aec926`, including
`local_primitive_relational_batch_input.rs`, prepared relational binding and
correlation, `native_relational_subquery.rs`, `native_payload.rs`, native replay,
and SQL/Python subquery lowering. The prior intake and two source inventories
retain immutable excerpts and hashes. The primary checked their source
provenance separately from this implementation decision.

This extends the finite contracts in `native-stream-input-composition-2026-10-13.md`
and the existing scalar/correlated subquery design. RFCs 0013, 0014, 0015, 0017,
0021, 0031, 0032 and 0033 govern streaming ownership, resources, semantics,
cleanup, expression handling and user workflows. The expert comparator is an
encoded-columnar maintainer checking nested demand, exact result semantics,
resource denial and publication as one contract.

## Source roles and demand

Admission must inspect the complete statically visible plan before its first
producer request. For each source, retain saturated use count and explicit
requirements for replay, detachment before nested consumption, replay completion
before query consumption, and validation if a conditional branch stays unused.
Merge roles conservatively across all occurrences of a URI; a source appearing
once syntactically inside a parameterized inner relation still requires replay.

| Situation | Required behavior |
| --- | --- |
| Independent, eager inner input | Execute once through the existing subquery builder; do not materialize the whole source into replay solely because it is an inner relation. A bounded inner aggregate must still process more input than the grant can retain. |
| Scalar, guarded or parameterized inner demand | Detach the outer source batch into credited native storage, release its original owner and verify release before the consumer can request inner input. |
| Repeated parameterized inner input | Record the raw producer once under existing explicit replay policy, then reuse the completed native replay for each binding. |
| A source occurs in both outer and inner relations | Complete its raw replay before its first query read. This permits nested reads of completed storage without reopening the producer or reading an incomplete replay. Count every completed replay pass, including that first query read. |
| Empty outer input or no selected branch | Validate only conditionally unused producers through their normal session, schema, row/byte bounds and end-marker checks. Do not evaluate skipped expressions, scalar cardinality or subquery state, and do not allocate unused replay storage. |
| Unsupported or incompletely declared shape | Fail before source demand where statically knowable, with the existing deterministic batch diagnostic and no fallback. |

Borrow the provider only for a producer request. A separate execution-wide
original-owner lease must reject reentrant demand while any original input
batch remains live; shortening a mutable borrow alone is not ownership proof.
Ordinary pipelines keep their current no-extra-copy path. Detach retained
subquery build batches through the existing `PreserveUnobserved` policy so this
ownership boundary does not evaluate previously unobserved invalid values.

Execution-time lowering must carry source roles into the same execution input
state before demand. It may strengthen a role only when its prerequisites still
hold. It must not retrofit replay after an unrecorded producer has started.
Completed prepared declarations remain reusable in independent executions;
execution-local source owners, cursors, caches and counters must not leak.

## Resources, completion and reports

Outer detachment, retained inner batches, membership/index state, scalar values,
replay and output share the admitted query grant. Source replay spill does not
spill subquery membership or other retained operator state. Such state remains
resident and must fail deterministically when its credited capacity is exceeded.
An outer copy can require temporary source-plus-copy capacity; denial must leave
no retained input, replay workspace or published output.

Count outer-boundary detachment, retained subquery build detachment,
validation-only batches and completed-before-query replay separately from
existing intake and replay counters. Counters report work actually performed,
including empty batches where the existing batch convention requires them.
Every declared finite source must reach a valid end before final success or
publication. Preserve provisional-result behavior, cancellation, late producer
failure, output retention, staging cleanup and immutable completion reports.

## Acceptance declared before implementation

1. Freeze the merged runtime, binary identities and exact expectations. Run
   fifteen streaming refusals before producer opening and fifteen resident
   controls, covering every family, null/empty/scalar cases, parameterized and
   guarded scalar evaluation, shared input and the three existing self/set
   subquery refusals. Preserve original failures and fixture corrections.
2. Add exact-value native, direct binding and public SQL/Python coverage across
   independent and repeated sources, batch boundaries, nulls, row membership,
   duplicate keys, all comparison modes, scalar zero/one/multiple rows and
   existing admitted typed/nested payloads. Explicitly test selected branches;
   this unit does not introduce new lazy OR semantics.
3. Prove original-owner release before nested demand and retained result
   independence. Exercise same-source recursive reads with resident and spilled
   replay, multi-level nesting, parameterized reuse, execution-time outer
   lowering and finite consumers/writers/fanout. Keep opaque dynamic-inner
   refusal tests.
4. Distinguish raw completion from query evaluation: empty outer and false/null
   guards skip divide-by-zero and multirow-scalar evaluation, while late failure,
   invalid schema/session/bounds and missing completion still prevent success.
5. Test bounded inner aggregation beyond cumulative grant, credited resident
   state denial with and without replay spill, copy denial, cancellation,
   consumer failure, failed publication and cleanup followed by a fresh run.
6. Run focused checks, all required workspace and native gates, direct/public
   acceptance and one complete Full43 pass. Routine timings report engine time
   only. Preserve full raw diagnostic evidence without inferring a speedup from
   a single pass. Align source/generated docs, review and hosted evidence before
   moving the completed finite scope to the ledger.

## Remaining boundaries

An opaque `DeferredSubquery` can discover parameter-dependent sources or dynamic
inner schemas only after execution begins. Its current closure API cannot
declare all source roles before first demand. Keep it explicitly unsupported for
streaming input in this unit. A subsequent source-declaration/lifetime contract
must admit that family; this is a sequencing boundary, not a permanent ban or
permission to preload all possible inputs. Keep that work visible under the
existing breadth owners.

No new subquery algorithm, state spill, external-engine fallback, transport,
package release, broad SQL parity or production-readiness claim follows from
this finite composition work.

## Local acceptance

The original preimplementation contract, its exact feature-label correction and
the baseline refusals remain in the immutable acceptance packet. The implementation
admits the declared static inner scope and carries source roles through execution-time
lowering. The SQL frontend also routes projected EXISTS through existing predicate
lowering, with a regression for bare, parenthesized, negated and correlated forms.

All six Rust gates pass: 3,220 workspace tests and 4,264 native-feature tests, with
the same 29 existing native exclusions. The direct binding suite passes all 96
methods without skips; five added methods cover 127 subcases. Public acceptance
passes 3,724 cases across 31 families, including 1,123 new finite-subquery cases.
One complete Full43 pass verifies all 43 results and records 61.819 seconds of
engine time. Independent inspection reopens all 14,066 archived members and the
complete saved values, schemas, clocks and failure history.

Shared and nested replay, selected evaluation, validation-only producers, native
ownership release, credited copy/state denial, cancellation and publication failure
have explicit tests. A bounded inner aggregate consumes cumulative input beyond
its grant without retaining source replay. Source replay spill remains distinct
from resident subquery state. Hosted integration and installed-wheel verification
remain pending; the immutable 0.6.0 packages are unchanged.
