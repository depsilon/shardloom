<!-- SPDX-License-Identifier: Apache-2.0 -->

# Structural work elimination across native operators

Status: maintainer-requested investigations, November 11, 2026;
`v1_candidate_pending_feasibility`, `claim_gate_status=not_claim_grade`.
This extends the [shared-pipeline program](native-shared-pipeline-investigations-2026-11-10.md)
inside item 20 and RFC 0044. H–N below are hypothesis labels, not new phase or
competitive gate IDs. The [phased plan](phased-execution-plan.md#planned) alone
owns execution order. Item 19's spatial/deep-traversal obligations, the remaining
analytical capability/performance tracks, and CG-1 through CG-23 stay open.

## Intake and evidence boundary

The supplied research checked accepted source
`591fdb42f6ffcd5e9003b4a5c130324d7a34a62c`. The source checks below were repeated
against that revision. They establish execution boundaries, not attributable
CPU shares, runtime activation or a performance result. Shared index PR #1678
and resident-window PR #1679 are integrated; their independent complete-cohort
results remain in their original packets. The original current-runtime 10k
observation remains invalid and the 517-case repair remains a separate cohort.
There is no new complete 10k total or additive savings claim.

The declaration exposure audit for H, I and J is complete below. Next match the
identified shapes to actual owner activation and removable work, then freeze
independent experiments. Declaration counts alone do not admit a candidate.
Rolling DISTINCT's first candidate was dropped and its hosted/retirement closeout
is complete. The [A join-mapping candidate](native-join-payload-mapping-2026-11-12.md)
also passes correctness but is dropped under the complete engine-time rule;
portable replay and all 30 corruption controls pass; hosted closeout is pending.
B's bounded source-bridge investigation
remains next. These foundations do not admit H/I/J by themselves. K and L have
separate semantic gates; M is conditional topology research; N requires actual
dictionary activation. Do not restart a previously rejected mechanism unchanged.

## Frozen declaration exposure audit

The [audit summary](../benchmarks/evidence/native-structural-exposure-2026-11-11/summary.json)
checks all 10,000 original request identities against their frozen declarations;
the [complete record](../benchmarks/evidence/native-structural-exposure-2026-11-11/audit.json.gz)
preserves each matching ID, AST path and sort ancestry. All nine inspected owner
files match accepted revision `591fdb42` exactly. This inspection executes no
queries, loads no native runtime and supplies no new engine-time result.

| Investigation | Exact declared exposure | Remaining admission evidence |
| --- | --- | --- |
| H, scalar correlated COUNT | 0 matching declarations; 392 membership declarations are uncorrelated IN/NOT IN | A separately versioned same-snapshot scalar workload with complete answers and small-demand/guarded-error controls |
| I, survivor batching | 2,201 requests have filter-to-aggregate chains through filters/projects; 1,046 of these contain computed expressions; 580 requests have join-to-join chains through filters/projects | Actual survivor sizes, expensive repeated preparations and credited retained backing storage; these shapes do not prove sparse batches |
| J, full-sort representation | 9,313 requests have a sort immediately beneath LIMIT; 195 have a sort without that immediate parent; 68 of those have one fixed-width integer/temporal key | Actual demand propagation, disordered full-sort activation, validation order and comparator cost; logical ancestry does not prove the physical route |

The 776 window and 392 ASOF declarations remain separate ordering owners. Do
not assign their whole family times to J. Counts describe matching requests,
not executions or disjoint timing cohorts; a request can match several shapes.

Existing release subquery reports expose input/indexed/probe/candidate rows and
detach batches, but no direct per-parameter inner-execution counter. Filter
delivery and join flushes have no generic intermediate-consumer invocation
counter. Root output-batch counts do not fill that gap. Debug-only observations
expose join output rows/cells, shared row-location searches, ordered-input
comparisons and heap-sort events; they do not report general full-sort comparison
or key-interpretation counts. Numeric-prefix comparison counts apply only to that
path. Add narrowly scoped attribution with its own overhead control when the
corresponding experiment is admitted.

The audit therefore keeps H/I/J as prioritized investigations after A/B, with
H requiring new declarations and I/J requiring measured activation. It does not
relabel all membership, join or ordering requests as affected. The executable
audit and source identities are retained; reproducing the declaration inspection
requires the original frozen harness and design, not a new benchmark pass.

## H: same-source scalar summaries

The current [decorrelation admission](../../shardloom-vortex/src/local_primitive_relational_decorrelate.rs)
admits `Exists` and `In`. The [parameterized executor](../../shardloom-vortex/src/local_primitive_relational_correlated.rs)
iterates selected outer rows, creates a singleton parameter, completes the inner
relation and finishes its state per selected row. Its separate uncorrelated
branch already shares completed results with demand guards.

Investigate scalar COUNT over the same immutable native snapshot with
nonnullable fixed-width equality keys. Prove source generation, inner population
and outer row identity; compute one native summary and attach it with original
multiplicity and ordering. Compare existing grouped-summary/lookup and window
owners according to required work; a window is not automatically cheapest.
[WinMagic](https://research.ibm.com/publications/winmagic-subquery-elimination-using-window-aggregation)
provides the research precedent for using window aggregation to remove repeated
access to tables shared by query blocks. It does not establish ShardLoom eligibility.

Do not collapse SQL null-equality behavior into a null window partition. Keep
outer filters separate from the inner population. An empty or unselected outer
domain must not introduce undemanded inner errors. SUM/AVG, nullable correlation,
dynamic/effectful sources and different populations require separate proofs.
Include one/two-row outer-demand controls, guarded failures and changed snapshots.

Measure inner executions, input visits, binding/state admissions, summary
construction and attachment, and complete engine time. Reuse the rewrite, bound
source identity, aggregate/window and row-owner contracts, with explicit admission
evidence. The [October 25 generic workflow drop](native-workflow-work-elimination-2026-10-25.md)
remains unchanged; this is a new precisely admitted scalar transformation.

## I: batches formed from surviving rows

The [transform path](../../shardloom-vortex/src/local_primitive_relational_transform.rs)
delivers selected arrays during each input callback. Ordinary
[join consumption](../../shardloom-vortex/src/local_primitives/native_relational_join.rs)
creates pending pairs per incoming batch and flushes its remainder at that batch's
end. Existing [replay capture](../../shardloom-vortex/src/local_primitive_relational_replay_capture.rs)
has a separate retained-fragment compaction boundary. None of these observations
by itself measures sparse downstream setup cost.

Separate source admission from consumer batching. At one admitted filter/join
boundary, retain bounded native selections/owners across input callbacks and
flush on useful-row capacity, credited backing-byte pressure, finite demand or
producer completion. Logical consolidation must remove actual downstream
preparation; an array list that still invokes each consumer separately is not
the proposed improvement. Count physical compaction and copying when necessary.
[Data Chunk Compaction in Vectorized Execution](https://people.iiis.tsinghua.edu.cn/~huanchen/publications/data-chunk-compaction-sigmod25.pdf)
studies this setup-versus-movement tradeoff and logical join compaction. Treat
its algorithms as research inputs, not measured benefits for this engine.

Charge pinned backing allocations, metadata, queues and overlap, not only selected
rows. Preserve row order/multiplicity, floating observation order, callbacks,
errors and bounded cancellation. Flush when demand can be met rather than pulling
an unnecessary suffix. Capillary owns hard admission; any PulseWeave feedback
uses completed work units and has a separate sampling-cost control.

Begin with selective filter → expression → aggregate and join → join workflows,
including dense, tiny, wide-payload and low-grant controls. Report actual survivor
distributions, consumer preparations/invocations, retained/peak backing bytes,
copies and complete engine time. Keep this distinct from rejected generic range
selection and parked ingestion topology.

## J: normalized keys for disordered full sorts

The [resident sort](../../shardloom-vortex/src/local_primitives/native_relational_sort.rs)
retains source ordinals, then uses [shared ordering](../../shardloom-vortex/src/local_primitives/native_relational_order.rs).
Full-sort comparisons locate source batches and apply key/null/direction policy
before source-ordinal tie breaking. Bounded Top-K and already-ordered input have
separate paths and remain controls.

First admit one completely validated homogeneous integer or temporal key.
Construct order-preserving fixed-width records with source ordinals, use one
specified bulk/radix strategy, and gather payload through the existing native
owner. Charge normalization, key records, sorting scratch, overlap and final
gathering. Preserve signedness, direction, null placement, stable ties and
temporal identity. UUID, multiple keys, string prefixes, floats and decimals are
later independently admitted extensions; prefix ties always require exact order.
[These Rows Are Made for Sorting](https://duckdb.org/pdf/ICDE2023-kuiper-muehleisen-sorting.pdf)
motivates examining representation and interpretation costs together. This is
not permission to materialize every payload row or copy another engine's code.

Keep the inexpensive ordered-input path. Attribute full-sort, window ordering,
ASOF build and initial spill-run consumers separately; do not assign their entire
family totals or Top-K time to this mechanism. Measure key interpretation/source
lookups removed, normalization work, scratch high-water, resident-to-spill
transitions and complete engine time. The [October 31 typed-comparison drop](native-typed-ordering-2026-10-31.md)
kept the heap algorithm and ordinal lookup; its rejection remains recorded. A
new representation plus algorithm requires a new frozen protocol and exact
error/validation-order proof before changing comparisons.

## K: direct physical Variant paths and shared prefixes

The [Variant expression owner](../../shardloom-vortex/src/local_primitives/native_relational_variant.rs)
detaches and validates selected values, reserves bounded workspace and uses the
provider. Shredded storage is normalized through the Arrow format bridge because
raw and typed paths otherwise differ in conversion semantics; UUID identity is
also a constraint. This bridge is compatibility work inside the native provider
boundary, not an external execution fallback.

Investigate one exact raw/typed-leaf conversion contract, selected typed children
with residual raw storage where required, and shared path prefixes over identical
owner/row domains. Build only requested outputs. Separate single-path avoidance
of unused reconstruction from several paths sharing traversal. The
[Parquet Variant shredding specification](https://parquet.apache.org/docs/file-format/types/variantshredding/)
defines typed/raw reconstruction and partial projection. Actual selective source
reads remain later work behind provider and validation proof.

Complete selected-value structural validation remains required. Preserve SQL
NULL/Variant-null/missing distinctions, raw/shredded parity, UUID/binary identity,
numeric/temporal conversions, lazy observation, first errors and credited
workspace. Test wide raw, shredded and partially shredded records with 1/4/16
sibling paths and large unused subtrees. Record normalized bytes/nodes, bridge
calls, traversal work and complete outputs/engine time. This extends C/E and the
rich-data owner; it supplies no attribution to the historical 10k by itself.

## L: successful finite demand and completion obligations

General callbacks currently return `Result<()>`. In
[LIMIT execution](../../shardloom-vortex/src/local_primitive_relational_transform.rs),
zero remaining output returns success from the callback while upstream execution
continues. Static prefix demand and `requires_completion` already exist.

Design a private continuation contract for one validated native-memory primitive
route with a total pure predicate, direct projection and finite demand. Distinguish
continue, demand satisfied, demand satisfied with completion still owed, and
error/cancellation. These are semantic states to prove, not prescribed public
enum names. Reuse the existing completion owner and drain only admitted work.
[Push versus pull-based loop fusion in query engines](https://doi.org/10.1017/S0956796818000102)
describes termination/interface tradeoffs; it does not justify a wholesale engine
rewrite or establish one execution style as universally faster.

Preserve suffix-expression errors where observable, source validation, retained
and recursive completion, observations, callbacks, effects and cleanup. Successful
short-circuiting must report partial consumption honestly and must not use
cancellation as a success signal. Include early/late/no matches and all completion-
required controls; measure rows never scanned/evaluated/probed, bounded drain and
complete engine time. The October 25 finite-demand drop remains historical;
only the newly proved stop/drain contract can admit this distinct experiment.

## M: different multiway plans for exact key regions

Ordinary row-producing joins execute the bound binary tree and enumerate right
duplicate chains. Investigate one pure three-input inner-equijoin family only
after measured intermediate expansion substantially exceeds final output.
Collect exact degrees during an already-required build, admit a bounded heavy
region, choose between two fixed legal native plans, and reconcile complete
duplicate-preserving output with original ordinals. Cost degree collection,
splits, extra builds, execution and reconciliation together.

[SplitJoin](https://duckdb.org/library/splitjoin/) studies heavy/light splits using
different plans with existing binary join engines. The [earlier ShardLoom multiway
candidate](native-multiway-join-2026-10-20.md) still enumerated R/S pairs and is
not retained. The new gate requires fewer candidate combinations visited;
deferred payload gathering alone does not satisfy it. Include uniform, unique,
tiny, skewed, duplicate/null and genuinely large-final-output controls. Keep
RowIndex identity, Capillary expansion admission and any finite PulseWeave choice
within their owners. This is conditional research, not broad adaptive join-order
support or renewed admission of the rejected topology.

## N: dictionary-domain text expressions into a consumer

Check actual dictionary activation and an expensive repeated text transformation
before implementation. The pinned Vortex 0.93
[dictionary scalar-function rule](../../vendor/vortex-array/src/arrays/dict/compute/rules.rs)
can move eligible work onto values, but declines oversized value domains,
fallible work over unreferenced entries, incompatible nonconstant siblings and
non-strict behavior with null codes. Reconcile those guards with the prepared
text owner and existing detached-code repair before admitting the provider.

Keep the transformed dictionary into a named group/filter consumer when legal;
full-output expansion is a control. A transform can merge previously different
values, and codes from different dictionaries are not shared semantic IDs.
Prove null/unused/invalid entry behavior, exact logical equality, ordering,
identity, credited lifetime, changed dictionaries and cancellation. Measure active
value transformations, code-domain work, expansion, retention and complete engine
time. This extends the existing encoded-run/result-preservation investigation
with a distinct dictionary activation and semantic gate.

## Common acceptance

Keep all mechanisms in existing Vortex-native execution owners and all five
public surfaces converging there. Check the pinned upstream provider before
implementing new abstractions; preserve no-fallback certificates and
Apache-2.0-compatible provenance. No external engine executes residual work.

Freeze exact requests, independently computed complete answers, source/runtime
identity, controls, resource/error contracts, work counters and acceptance before
each candidate. Version correlated summaries, sparse chains, physical path bundles
and skewed plans separately from the fixed 10k. Reuse saved observations only
when exact runtime/request/input/reference identity is verified. Counter reductions
establish the mechanism; correctness/resource/ownership/no-fallback gates precede
the complete-cohort engine-time decision. Lower retains; equal/higher drops.
Preserve all failures and family regressions, instrumentation overhead, original
rejections and separate construction/delivery/publication/cleanup evidence.
No new routine Full43/10k repetition, publication or superiority claim follows.
