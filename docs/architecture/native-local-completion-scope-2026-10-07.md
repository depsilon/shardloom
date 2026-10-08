# Remaining local workflow and optimization scope

Status: maintainer-reaffirmed scope, October 7, 2026. This is a coverage contract
for the existing remaining optimization/breadth task. The
[phase plan](phased-execution-plan.md) owns implementation order and checklists;
this document is not a second queue or a new set of competitive gates.

The maintainer's October 7 remaining-work assessment groups the work into
stateful execution beyond resident memory, broader streaming and adapter
coverage, and operational acceptance of a defined local product. Finishing one
native provider, a streamed ordering connection, or a performance experiment
does not finish that body of work.

## Current baseline and completed boundaries

The assessment inspected main `0f7609da` and PR #1530 at `24bd2e5b`. Since that
inspection, [PR #1530](https://github.com/depsilon/shardloom/pull/1530) merged at
`c16f8da71581c1b1b874aaa18aefa95a8264d0ad` after all 39 hosted checks passed.
The [integration receipt](../benchmarks/evidence/native-fsst-hosted-2026-10-07.json)
records preview/production search and guide verification, the merged report
link, and exact preservation of all 947 accepted runtime assets. The first
completion-aware input implementation and its FSST correction are completed
work, not a request to implement them again. Published v0.4.0 is unchanged.

[PR #1531](https://github.com/depsilon/shardloom/pull/1531) subsequently completes
general aggregate spill and streamed global ordering/draining limits at
`9529bd78`. The [hosted receipt](../benchmarks/evidence/native-stateful-hosted-2026-10-08.json)
records all 39 checks, preserved runtime behavior and preview/production checks.
Those finite families move to the completed ledger; the remaining obligations
in the coverage map below continue to belong to the broader owners.

The subsequent [join pressure unit](../benchmarks/native-join-pressure-2026-10-08.md)
has complete local acceptance at `92ab9f20` for all seven native join kinds,
one single-use batch source on either side alongside ordinary sources, constrained
spill and owned cleanup/restart. Its public/direct/adapter/Full43 regressions and
independent packet inspection pass. [PR #1532](https://github.com/depsilon/shardloom/pull/1532)
merged at `25906290` after all 39 checks passed; the
[hosted receipt](../benchmarks/evidence/native-join-hosted-2026-10-08.json)
records preserved runtime bytes and actual preview/production verification. This
completes the finite aggregation/join pressure implementation and integration while preserving
the other coverage obligations below.

The subsequent [window pressure unit](native-window-pressure-2026-10-08.md) has
complete local acceptance at `80057ba6` for the existing analytic functions,
frames and exclusions, plus one finite single-use batch source. Native
file-backed and streamed pressure controls, complete public/direct/adapter and
Full43 regressions, and independent packet inspection pass. The
[acceptance report](../benchmarks/native-window-pressure-2026-10-08.md) records
the finite scope. [PR #1533](https://github.com/depsilon/shardloom/pull/1533) merged
at `a88ec5c9` after all 39 hosted checks passed. The
[hosted receipt](../benchmarks/evidence/native-window-hosted-2026-10-08.json)
records unchanged runtime/tree identity, primary source review, the unavailable
automated review due to account quota, and actual preview/production verification.
[Pivot pressure](native-pivot-pressure-2026-10-08.md), broader recovery and the
other five areas remain open.

Reviewed FSST/Zstd payload buffers, actual Zstd decoder/prepared-dictionary
workspaces, and primitive/Boolean/decimal builder output and finalization overlap
also remain completed under their finite acceptance records. Broader ownership
coverage remains open. A native grant is not a process-RSS limit.

## Coverage that must remain visible

| Area and existing owner | Intake disposition and concrete remaining obligation | Completion evidence |
| --- | --- | --- |
| Stateful pressure and recovery — PERF-03/06/10/12 | General high-cardinality grouping/DISTINCT and relational ordering are accepted in PR #1531. Oversized join build/intermediate state has complete local and hosted acceptance in PR #1532. Analytic windows have complete local and hosted pressure/workflow acceptance in PR #1533. Growing pivot domain/cell state and broader recovery retain explicit pressure contracts to complete. Reuse the shared native mechanisms only where each family's semantics permit. | Complete exact workloads beyond the admitted resident allowance; native reservations/runs and disk quota; cancellation, corruption, exhausted resources, owned cleanup and publication. Distinguish cleanup/restart from actual execution resume. An intentionally resident-only supported shape may close with a documented bound and deterministic growth denial, not a spill claim. |
| Remaining allocation coverage — PERF-03/06/08/09 | Merge into current resource inventory: child-decoder and selection scratch, structural metadata, compression contexts, dictionary training and other unreviewed reader/provider/builder allocations. Preserve completed provider fixes. | Valid credits before allocation, an implemented bounded/spill transition, or deterministic denial with cleanup for the supported workflow. Identify Python retention, conversion-library memory, allocator overhead and excluded providers separately. |
| Broader streaming — PERF-03/06/07/11/12 | Extend the existing single-use finite source deliberately: compatibility destinations and fanout; remaining bounded/spill stateful families and safe dynamic-schema admission; declared retention or spool for repeated batch sources/self-joins; exact decimal, binary, temporal and nested intake. Ordering/aggregation, complete drain for limits, joins and analytic windows are implemented and integrated. Preserve that completion contract when admitting further shapes. Dynamic pivot input still rejects before producer demand; ordinary pivot spill does not remove that admission boundary. These are separate missing contracts, not larger constants. | Single-pass input, bounded owners, complete schema/value/order checks, late failure and cancellation, sink cleanup/publication, and no silent replay. Preserve current 4,096-batch/2,048-row and per-frame bounds until separately justified. Ordinary file-backed datasets do not inherit these transport bounds. |
| Adapter and composition coverage — PERF-02/07/10/11/12 and CG-19/20/21 | Merge into the universal-workflow matrix: typed/nested format boundaries, partition/schema evolution, prepared/public parity and remaining operator/type result streams. Connect automatic compatibility preparation to the incremental transaction's external cancellation owner before removing its explicit prepare-to-Vortex step. | Read → transform → retain or spill → consume again → write → reopen, checking intended types and every value. Format existence alone is insufficient. Small-result collect may remain deliberately bounded. |
| Defined local support and release — PERF-12 and existing local-engine/release gates | Bind a specific release to declared workloads, operators/types/formats/resource conditions and supported OS/architectures. Add actual platform runtime evidence, pressure/failure/slow-consumer acceptance, recovery promises, known issues, compatibility, upgrade/rollback and install guidance. | The [local-engine exit criteria](../release/production-certification-gate.md#local-engine-preview-exit-criteria), workload-scoped runtime records, supported/unsupported matrix and normal publication gates. Current batch runtime acceptance is macOS arm64 with Unix facilities. A successful cross-platform build alone is insufficient. Package publication itself is already implemented. |
| Conditional performance investigations — existing campaign/PERF owners | Preserve all eight rows below, attached to the components they may improve. Evidence prerequisites decide whether to prototype; each retained implementation still needs its frozen complete-operation gate. | Complete output, mechanism counters, grants and controls, original failures, all samples and an explicit retain/drop/defer decision. No investigation is a promised speedup. |

Cloud connectors, distributed execution, table transactions, full SQL/DataFrame
parity and Foundry-specific integration retain their separate roadmap owners.
They do not all have to complete before a stable, explicitly scoped local engine.
CG-1 through CG-23 remain visible and unchanged; no finite row closes a whole
gate or authorizes fallback execution.

## Eight investigations, with current evidence prerequisites

| Investigation | Current disposition and next evidence |
| --- | --- |
| Selective rematerialization | Initial real-owner observation now exists: the [25-case packet](../benchmarks/evidence/native-rematerialization-owners-2026-10-07.json) finds reclaimable non-key derived payload and zero-reclaim alias/key/constant controls. This advances the original assessment's unmeasured status only for the owner screen. A complete-workflow retain/spill/regenerate comparison, pinned dependency cost, reconstruction headroom and bounded regeneration are still required before production policy. |
| Constraint-guided multiway joins | Open under the [state/structure campaign](native-state-structure-campaign-2026-10-07.md). Compare a native compatible-domain strategy with binary joins, including first-use preparation, exact multiplicity/null/order semantics and ordinary join controls. |
| Shared nested identities | Open, gated on measured repetition and recursive work. Charge canonicalization, collision resolution, retained representatives and row mapping before considering tokens. |
| Cost-aware spill merging | Open alongside spill-family work. Capture actual run sizes and stable lineage, replay adjacent schedules and respect admitted reader, memory and disk overlap. A new ordering connection alone is not this experiment. |
| Bounded-error learned indexes | Open under the [conditional-work campaign](native-conditional-work-campaign-2026-10-07.md); first identify a large ordered structure with repeated searches and certify exact bounded-error correction. |
| Physical-state cache admission | Open; demonstrate actual repeated reconstruction rather than repeated references to a still-live object. Include generation identity, admission/eviction cost and retained bytes. |
| Byte-credit output windows | Open; compare bounded producer/consumer overlap with current acknowledgements, including slow/failing consumers, queue credits, cancellation and provisional completion. |
| Changed-source preparation reuse | Open; identify verified reusable regions across changed generations with exact layout/encoding compatibility and source validation. No unchecked generation reuse. |

Adaptive decimal accumulation, conservative membership filtering, rejected
reservation/directory/locality prototypes and composed-COUNT prototypes remain
closed drops. A materially different mechanism with evidence is required to
reopen them. Separately paused large format/text experiments stay paused.

## Completion discipline

The next meaningful milestone is a declared set of complete workflows that
behaves predictably under constrained resources, followed by a release carrying
the accepted capability. Batch smaller fixes into that milestone. The maintainer
delegated release timing but explicitly asked that version bumps be substantial;
closing one experiment does not trigger a release train.

For every cohesive unit, keep a reuse map, native-I/O and execution evidence,
resource/failure tests, complete public workflow checks, applicable regression
gates, adversarial review and hosted integration. Update the canonical phase
checklist and completed ledger with exact scope. Do not call this entire task
complete because its latest subunit passed.
