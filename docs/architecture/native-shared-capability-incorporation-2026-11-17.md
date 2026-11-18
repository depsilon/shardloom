<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared native capability incorporation

## Decision and scope

The November 17 maintainer request accepts the supplied incorporation plan as
the next queue item, taking priority over the remaining backlog after the
deep-traversal PR checkpoint. The later November 17 request defers the remaining
deep-traversal lifecycle until after Foundry integration; it does not block this
incorporation plan or close item 19. The
[phase plan](phased-execution-plan.md#current-execution) alone owns execution
order. This work extends item 20 and its PulseWeave investigation F under the
existing native runtime; it introduces no new phase or CG identifier.

The maintainer's ordering clarification applies to the entire unfinished
remainder of item 20. Obligations absorbed into this incorporation plan execute
within it. All unabsorbed item 20 capability, workflow, performance and acceptance
work follows the new plan, with its remaining relative order preserved. Older
item 20 sequence notes do not authorize work ahead of this priority.

The objective is to keep an optimization available when the input format,
public interface, surrounding operators or output destination changes, wherever
its semantic, source-domain and ownership requirements still hold. Carry source
facts, native representations, resource ownership, reusable preparation and
execution evidence through the existing owners from intake to final delivery.

The supplied review cites main at `6688dd9b9` after PR #1697. Its integration
points were checked against the active deep-traversal worktree at `b83306a32`;
this records the planning baseline, not a new remote-main or runtime acceptance
claim. The [unit 1 owner map](native-shared-capability-owner-map-2026-11-17.md) now
reconciles the named capabilities and seven boundary families. Its accompanying
v2 reporting correction separates policy plans and task-role checks from actual
scheduler application; unmeasured work stays unavailable. Units 2–5 remain open,
including runtime target selection. [Item 23's adaptive acceptance](native-adaptive-acceptance-2026-11-02.md)
remains complete in its declared scope and must be reused, not reopened.

Classification: `v1_candidate_pending_feasibility` for new performance
mechanisms, explicitly prioritized by the maintainer. Existing correctness,
resource, lifecycle and no-fallback obligations remain required for supported
paths. Feasibility or a capability name does not establish a new release blocker
or a speedup. RFCs 0013/0014/0015/0016/0017/0021/0031/0033/0044 govern streaming,
resources, semantics, adaptation, recovery, expressions, I/O and shared execution.
CG-8/CG-14/CG-15/CG-20/CG-21 are relevant proof obligations; all CG-1 through
CG-23 remain independently open or closed under their own evidence.

## Intake disposition and ownership

The design extends the admitted operation in
[`NativeExecutionContext`](../../shardloom-vortex/src/resident_execution_context.rs)
and its resident source, memory, I/O, worker, operator and result owners. Nested
operators borrow that operation rather than constructing another admission
context. Workers receive owned inputs, leases and cancellation handles; mutable
global state and final ordered merging remain with the operator that owns them.
CPU jobs and OS reads retain their established join/drain boundaries.

| Supplied proposal | Disposition and responsibility |
| --- | --- |
| Connect named capabilities across intake, execution and delivery | Accepted into this item; map current implementations, operative controls, callers, legal applicability and remaining gaps before changing behavior. |
| Capillary lifecycle and finite PulseWeave feedback | Merged with item 20/investigation F and existing worker/continuation owners; select the first target from material attributable exposure. |
| Shared adaptive execution | Already addressed in item 23's eleven-owner acceptance; extend demonstrated gaps without counting existing behavior as new work. |
| Shared keys, `RowIndex`, completed expressions and compatible windows | Reuse accepted preparation, identity and state owners; preserve specialized algorithms and their semantic gates. |
| Compute once for output fanout | Already implemented in the scoped native replay/fanout path; extend pressure, conversion and lifetime integration where evidence shows a gap. |
| DynamicWorkShapingReport and Bayesian advisor | Keep report-only; truthful observations may feed reports, but advisory labels do not establish applied control. |
| Chrysalis and Resonance | Keep their existing later items and order; require them to consume this foundation instead of adding separate schedulers, identities or evidence systems. |
| Blanket encoding, full-sort, spill or removed source-pruning campaigns | Not revived. Preserve the November 16 dispositions and rejected experiments; reopening needs new material workload evidence or a named required capability. |

### Roles of the existing capabilities

Some names describe architectural contracts rather than single Rust types. The
first deliverable must map each to its concrete current owner and distinguish
implemented, advisory, queued and unavailable behavior.

| Capability | Responsibility to incorporate |
| --- | --- |
| Capillary Units | Actual admitted ranges, jobs, partials or continuations, with source/domain identity, resource leases and a legal completion/merge rule. |
| FlowInventory | Actual ready, queued, running, completed-but-undelivered and downstream-blocked work; completed retained results remain unfinished resource obligations. |
| ScarcityLedger | Compare legal choices using the owners' live allowances, including retained input, decode workspace, output expansion, queue residence and sink capacity. Estimates inform choices; real reservations remain authoritative. |
| EndoPulse | Adjust the next legal window, prefetch depth, in-flight allowance or inline/worker choice from completed observations. Coordinate with `AdaptiveWorkSizer`; two controllers must not independently mutate the same setting. |
| ProofBound | Check prerequisites before application and accept completed evidence from the executing owner. Permission, correctness evidence and performance claims remain distinct. |
| PulseWeave | Coordinate these choices through bounded, run-local state at useful continuation boundaries, within the existing admitted operation. |
| SourceState / VortexPreparedState | Preserve generation, interpretation, demanded fields, representations and preparation identity. Ordinary execution must not require a durable prepared artifact. |
| Scout ingress/triage | Reuse valid schema, shape and source-characterization findings. Samples may inform cost choices, never exact pruning or complete validation proofs. |
| Vortex Layout/Write Advisor | Keep supported layout/encoding choices distinct from flow control; connect conversion-window and writer settings to the owners that actually apply them. |
| Copy-budget and buffer lifecycle | Preserve allocation provenance, retained backing capacity, transfer eligibility and release conditions. Small views may pin large allocations. |
| Native keys / `RowIndex` | Share compatible equality, key preparation, stable identities and duplicate handling without forcing every algorithm into one container. |
| Completed expressions / compatible window state | Reuse only within matching generation, selected population, ordering, frame/filter, semantic options and numerical behavior. |
| OutputPlan / ResultBatchState / SinkArtifact | Map requested representation, retained results, conversion and publication obligations to existing native result, replay, fanout and writer owners. |
| Native I/O decisions / execution certificates | Record considered, admitted, executed or declined behavior and observed work at the owner that performed it; no inferred execution or savings. |

The [PulseWeave architecture](pulseweave-runtime-control.md) and
[I/O lifecycle architecture](io-reuse-and-fanout-architecture.md) supply these
names. [Dynamic work shaping](dynamic-work-shaping.md) and the
[Bayesian advisor](bayesian-performance-layout-advisor.md) remain advisory;
scoped runtime sizing and Vortex layout/write decisions are separate existing
implementations. No unfitted advisor enters the execution path.

## Five contracts across boundaries

These are obligations on existing types and owners, not a new wrapper around
every batch or a generic framework refactor.

1. **Source and population identity.** Bind every reusable fact or representation
   to source generation, row region and selected population, field mapping/schema
   interpretation, numerical/null/comparison policies and the retaining owner.
   Specify preservation and invalidation through aliases, selections and plan
   changes. Dictionary codes require their exact dictionary domain; regional
   facts do not prove global coverage. Reuse the
   [exact source-domain facts](native-source-domain-facts-2026-11-14.md) contract.
2. **Demand and completion.** Carry both downstream need and outstanding source
   validation, observable errors and peer-consumer obligations. LIMIT completion
   alone cannot authorize stopping reads. Extend the existing
   [source-demand analysis](../../shardloom-vortex/src/local_primitive_relational_source_demand.rs)
   and its conservative completion rules into admission and scheduling.
3. **Representation and resource ownership.** Follow input → queued work → active
   work → completed partial → merge/delivery → retained result. Charge shared
   backing allocations once through their final owner; a returned CPU job is
   not a memory refund. Existing
   [chunk jobs](../../shardloom-vortex/src/local_primitives/aggregate_chunk_jobs.rs)
   already retain permits and leases through ordered merging. Engine-accounted
   resources remain distinct from process RSS and unaccounted provider memory.
4. **Legal changes to work.** Owners declare whether ranges may split/combine,
   independent work may overlap, work may run inline/on workers, representations
   may transfer, partials may merge, production may pause or execution may finish.
   Specify row, error, reduction and source-callback order separately. Preserve
   the [survivor-batching rejection](native-survivor-admission-2026-11-15.md):
   altered first errors and extra source invocations are semantic changes.
5. **Application and outcome.** Proposed → admitted/declined → executed by an
   owner → completed outcome. Extend the existing bounded
   [typed decision collector](../../shardloom-core/src/native_io_decisions.rs)
   deliberately where scheduling needs more vocabulary. Admission is not
   execution; unobserved quantities stay absent. Do not add another evidence
   framework or require an event trace for ordinary operation.

## Incorporation across the supported engine

Each row must have a source-backed applicability disposition in the reuse map.
Implement demonstrated gaps through shared owners; existing acceptance or a
specific semantic/resource decline can satisfy a row. An unexplored opportunity
must remain unobserved, not be reported as implemented or declined.

| Boundary and existing owners | Incorporation and acceptance boundary |
| --- | --- |
| Intake, direct execution and optional preparation | Preserve bound field mapping when physical demand narrows, carry held-source facts, reuse completed validation only in its valid generation/interpretation, and pass field demand to read/conversion owners. Preserve natural row groups/pages/messages. Direct native execution remains independent of optional durable preparation; samples never replace required validation. |
| Reading, conversion and preparation-to-writer flow | Observe actual pending reads, conversion jobs and writer backlog. Account for simultaneous compressed input, decoded values, native arrays and writer workspace. EndoPulse proposes the next window; existing memory/worker owners admit or clamp it and record the actual setting and completed outcome. Preserve the cheap inline path. Smaller returned batches do not imply smaller indivisible provider decode allocations. |
| Expressions, selections and encoded computation | Carry selected-row identity; share accessor/expression preparation only under compatible domains and policies. Retain dictionary, constant and repeated structure and native selection mappings through eligible consumers. Lazy branches and error precedence constrain reuse; leave cheap scalar work inline. Extend existing expression/provider owners, not a second expression engine. |
| Grouping, joins, DISTINCT, sets and windows | Share compatible native keys/equality/preparation and immutable admitted partials, preserving duplicate multiplicity and ordered state updates. Charge join expansion and completed results. Reuse compatible window traversal only under matching frame/filter/order/numerics. An integer COUNT merge rule does not authorize reassociating floating reductions. |
| Ordering, bounded ranking, spill and replay | Carry exact comparator/null/tie facts, separate ranking keys/positions from wide payloads where admitted, retain merge/run credits and source-generation ownership, and propagate downstream pressure. Preserve current spill quotas and cleanup. Local order is not global order; this does not restart removed full-sort/spill-performance experiments. |
| Native results, conversion, fanout and publication | Retain native arrays until requested conversion, include delivery/writer buffers, propagate slow-sink pressure and share only compatible conversion work. Preserve field/validity/statistics fidelity and results after source closure. Record actual copy/transfer/conversion at its owner. Preserve staged publication: multiple local destinations are not one atomic transaction, and late failure may leave earlier complete outputs published. |
| Vectors, spatial/nested values and application batches | Use the same lifecycle with domain costs: selected string bytes, vector components/spans/workspace, spatial coordinates/structure, nested child spans/offsets/validity/expansion, or declared callback resources/effects/output ownership. Reuse constant query-vector/geometry preparation only when exact semantics match. No separate vector/spatial scheduler or changed arithmetic/validation is implied. |

Concrete starting references include the
[composed COUNT bridge](../../shardloom-vortex/src/local_primitives/native_relational_aggregate_count_partial.rs),
[native work sizing](../../shardloom-vortex/src/native_work_sizing.rs),
[ingest conversion owner](../../shardloom-vortex/src/vortex_ingest.rs),
[shared row index](native-shared-index-metadata-2026-11-10.md),
[window sharing](native-shared-window-state-2026-11-11.md),
[result fanout](../../shardloom-vortex/src/local_primitive_output_fanout.rs),
[vectors](native-vector-expressions-2026-11-08.md) and
[spatial values](native-spatial-values-2026-11-16.md). COUNT's current inline
bridge is an integration candidate, not proof of material removable work or a
reason to repeat its previously unsuccessful performance comparison unchanged.

## Cohesive delivery units

Units 1–5 form the prioritized incorporation item. Combine related changes into
complete, reviewable owner families; these numbers are not new phase IDs or a
requirement for one PR per row. Do not ship a large preparatory abstraction whose
actual application is indefinitely deferred.

| Unit | Concrete deliverable and completion criterion |
| --- | --- |
| 1. Reconcile controls and evidence | Produce one bounded reuse map for every named capability and the seven boundary families: implementation, concrete owner/callers, operative control, observation type, legal domain, gap and disposition. Correct ambiguous application reporting; every applied claim comes from its executing owner. |
| 2. Complete the shared Capillary lifecycle | Extend existing budgeted jobs/continuations for one materially exposed owner family through pending results, downstream retention, cancellation, refusal and final release. Preserve inline execution and specialized kernels. |
| 3. Connect finite PulseWeave decisions | Extend investigation F using useful existing legal controls, actual inventory/resource observations and completed-window feedback. Applied changes must stay inside the same ceilings and coexist with existing adaptive sizing. |
| 4. Preserve facts and representations through composition | Close demonstrated source/selection/expression/key/output propagation gaps. Existing and composed callers across applicable interfaces use the same capability; ineligible boundaries provide specific reasons. |
| 5. Close source-to-delivery ownership | Prove one complete relevant workflow through reader/conversion pressure, operators, native results and sink delivery/publication, then extend the same contracts to additional evidenced eligible owners. Reconcile every intake family without claiming unexplored coverage. |
| 6. Apply the foundation to queued designs | At their existing later items, implement Chrysalis then Resonance under these same contracts. Their distinct acceptance remains separate; this item records the dependency and does not pull those implementations ahead of combined acceptance. |

Select the first runtime target from saved evidence of material removable work
or a material pipeline imbalance. Composed COUNT, decode-to-consumer overlap and
conversion-to-writer flow are candidates, not predetermined winners. Freeze the
affected physical opportunities and nearby controls before implementation or
measurement. A conspicuous missing connection alone does not establish value.

### Completed-result cancellation correction

The first required lifecycle correction is the completed-result merge boundary
in `AggregateChunkJobs`, shared by count, distinct and dictionary preparation
owners. Three regression tests fail on the preceding implementation: a
completed result can enter its merge after cancellation, a cancellation during
merge can still return success, and a caught merge panic can leave sibling work
uncancelled. These are supported-path correctness obligations, not a proposed
performance mechanism or evidence for a wider scheduling window.

The correction checks cancellation before and after both borrowed and owned
merges, cancels siblings on error or unwind, and preserves the original merge or
recorded provider failure. Payload reservations and window permits remain held
through consumption and cleanup. Fourteen focused lifecycle tests pass, including
refusal/retry, ordered completion, worker failure, retained credit, normal
retirement and the new cancellation cases. The follow-up caller review found
that count workers had no enclosing-operation token, while dictionary and
distinct preparation directly shared that token with a stage that cancels on
drop. All eight production job owners now derive a stage-local token that
observes the enclosing operation without cancelling the parent on stage failure,
retirement or drop. The native pair-partition test cancels inside an active
worker, checks complete reservation refund, then executes the same prepared
request with a fresh token and compares the full result. Existing dictionary
and winner-distinct cancellation/recovery tests pass through the same change.

The [source-bound acceptance record](../release/evidence/shared-native-capability-lifecycle-2026-11-17/validation.json)
and compressed raw logs preserve all 200 distinct affected native tests, all
3,346 default workspace tests across 108 targets, formatting, default workspace
strict lint and enabled-native CLI/Vortex strict lint. They also retain the
original three-regression failure and the corrected call-site compilation
failure. Full enabled-native, installed-package and hosted acceptance remain
with the enclosing incorporation item. This closes neither unit 2's complete
selected workflow nor units 3–5;
finite feedback and performance target selection remain open. No new scheduler,
provider, arithmetic, source order or output representation is introduced.

### Chrysalis and Resonance consume the foundation

The [existing design](native-chrysalis-resonance-cells-2026-11-16.md) remains
queued. Chrysalis adds proof of valid partial native coverage using the existing
generation/interpretation identity, exact row/field demand, native owners,
Capillary realization, PulseWeave and writer/publication contracts. A filtered
selection never establishes complete original-region coverage.

Resonance adds bounded sharing of compatible live preparation using credited
immutable inputs and the same operation/scheduling ownership. Consumers keep
independent selections, mutable state, errors, results, cancellation and
completion. Slow consumers have bounded retention; one consumer finishing or
cancelling cannot terminate peers. Neither design creates automatic unrelated
request coalescing, a second cache identity scheme or a separate scheduler.

## Evidence and acceptance

Use one bounded incorporation matrix keyed by public interface, source
representation, physical owner/opportunity, capability, disposition and completed
outcome. The relevant interfaces are CLI SQL, Python SQL, typed relations,
DataFrame and embedded workflows; inputs include Vortex, direct columnar, text,
memory and producers where admitted. The scope follows changed owners and actual
exposure, not the Cartesian product or a query-level applied flag.

Collect bounded summaries of actual admitted/executed units, inline/worker
execution, in-flight/completed-pending work, accounted retained bytes, reservation
refusals, copies/transfers/decodes/preparations, decline reasons and terminal
outcomes. Record queue wait and downstream blocking only when measured. Logical
operand counts are not physical reads; transfers are not automatically avoided
device traffic. Missing measurements remain unavailable.

For feedback, compare existing fixed controls, the same controls with observation,
and observation plus adaptive decisions to separate instrumentation overhead
from control benefit. Include tiny work, resource pressure, slow consumers,
incompatible domains, negative admission, cancellation, late errors and final
owner release. Preserve complete results, schemas, row/error/callback/reduction
order and all original observations for the frozen affected cohort. Retain or
drop new performance choices under the existing
[benchmark policy](../skills/benchmarking.md#investigation-scope); no presumed
gain, numeric material threshold or blanket campaign is introduced.

Use the established engine clock for routine reports. Retain preparation,
construction, conversion/delivery and publication boundaries in diagnostic
evidence; moving work outside the query interval is not a speedup. Preparation
and reuse comparisons charge creation and validation across complete workflows;
repeated-use savings cannot be credited to a single cold request.

Before runtime changes, inspect the pinned Vortex providers and record use,
wrapper or explicit rejection at the selected owner. Vortex arrays, encodings,
scans, layouts and sinks remain native providers; no dependency or provider
expansion is selected by this planning update. Source, execution and output
aliases converge on the existing native runtime with
`fallback_attempted=false` and `external_engine_invoked=false`.

Validation starts with exact changed-owner tests and complete source-to-delivery
controls, then the corresponding public-interface, feature and installed-package
checks. Required cohesive-batch gates are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings` and
`cargo test --workspace --all-targets`, plus the enabled native configuration.
The implementation must record exact commands, source identity, complete results
and scoped performance decisions. Documentation-only queue integration uses
link/order checks and the existing architecture/completion trackers instead.

## Alternatives, risks and claim boundary

A new central scheduler or universal batch wrapper would duplicate operation
admission and specialize poorly; extend existing owners. Uncoordinated local
controllers could oversubscribe shared headroom or fight over a setting; retain
one authoritative owner for each control. Broad reuse without population and
ordering identity could change values, errors or resource lifetimes; require
explicit preservation/invalidation. Mandatory detailed tracing could cost more
than the selected control saves; use bounded observations and overhead controls.

The first owner/control and its comparison population remain evidence-selected
implementation decisions. This plan authorizes no benchmark campaign, external
engine fallback, hidden persistence, remote deployment, package publication or
production/superiority claim. It preserves removed/rejected investigations and
the optional-preparation/output-format contracts. Runtime completion requires
real application at eligible boundaries, justified declines elsewhere, correct
ownership through delivery and a scoped complete-operation comparison;
additional labels alone do not satisfy it. Until that evidence exists,
`claim_gate_status=not_claim_grade`.
