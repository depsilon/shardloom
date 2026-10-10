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

The October 9 enterprise-workload feedback prioritizes natural-scale batch
execution and complete source/type/destination composition. The existing
analytical engine is the implementation foundation. Fixed cumulative input,
schema-width, generated-range and plan/expression ceilings are implementation
work under the phase plan's existing PERF-03/06/07/10/11/12 checklists: replace
them with resource-managed execution, scalable metadata/traversal and complete
workflow proof. Bounded transport, checked arithmetic and deterministic failure
remain necessary while those replacements are built. Operator strategy selection
should be automatic within the caller's authorized memory/storage/effect policy.

The feedback's six proposed packages are merged into existing owners:

| Package | Existing owner and implementation disposition |
| --- | --- |
| Natural-scale enterprise batch | PERF-03/06/10/11/12: replace whole-workload ceilings, complete accounting and select native pressure strategies within authorized policy. |
| Source, type and destination composition | PERF-02/07/10/11/12 and CG-19/20/21: shared source lifetimes, richer intake, repeated-source spooling, multiple producers, dynamic schemas, streaming writers/fanout and cancellable preparation. |
| Independent jobs and users | Shared scheduling/resource and remote/deployment owners: concurrent admission, isolation, priorities, cancellation, retry/publication and observability around the native worker. This is distinct from distributing one computation. |
| Incremental and continuous computation | Existing table/change, recovery and CG-22/23 owners: durable progress, change-aware state, event time, checkpoints and coordinated source/sink recovery. |
| Distributed individual computations | Existing distributed/runtime and CG-10 owners: partition exchange, stage ownership, skew, worker recovery, remote access and coordinated publication. |
| Analytical serving and federation | Existing prepared-session, API, catalog/connector and CG-11/20/21/23 owners: scheduling, protocols, identity integration and useful retained/indexed state. |

This is an intake mapping, not another queue. Prioritize the first two packages;
add operational concurrency alongside actual deployment needs. The other
packages remain implementation outcomes in the roadmap. Classify workloads by
execution behavior, resource shape and operating needs; embedded applications,
scheduled workers and shared services can assign hosting responsibilities to
different components. The current finite batch capabilities and later continuous,
distributed or shared-service capabilities retain their own workload evidence.

## Current baseline and completed boundaries

The assessment inspected main `0f7609da` and PR #1530 at `24bd2e5b`. Since that
inspection, [PR #1530](https://github.com/depsilon/shardloom/pull/1530) merged at
`c16f8da71581c1b1b874aaa18aefa95a8264d0ad` after all 39 hosted checks passed.
The [integration receipt](../benchmarks/evidence/native-fsst-hosted-2026-10-07.json)
records preview/production search and guide verification, the merged report
link, and exact preservation of all 947 accepted runtime assets. The first
completion-aware input implementation and its FSST correction are completed
work, not a request to implement them again. Published v0.4.0 was unchanged at
that integration checkpoint; the subsequent release milestone is recorded below.

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
The subsequent [pivot pressure unit](native-pivot-pressure-2026-10-08.md) has
complete local engine acceptance and independent packet inspection at `5665eee5`.
Two 16-MiB native spill executions return all 6,145 wide-key results, with resident
denial, a 512-MiB ample control, faults and owned cleanup/restart. Complete public,
direct, streaming, adapter and Full43 acceptance passes; its hosted closeout
is complete in [PR #1534](https://github.com/depsilon/shardloom/pull/1534), merged
at `d7898cb9` after all 39 checks passed. The
[hosted receipt](../benchmarks/evidence/native-pivot-hosted-2026-10-08.json)
preserves exact source/test-cleanup provenance and actual production verification.
Broader recovery and the other five areas remain open.

The subsequent [input growth unit](../benchmarks/native-input-growth-2026-10-09.md)
has complete local engine acceptance and independent packet inspection at
`61a813b7`. Cumulative input, top-level schema metadata and compact generated
ranges grow under the shared grant. Complete workflows exceed the former
batch/field/range bounds, and native plan checks exceed the former total-node
count. All source/public/streaming/pressure and Full43 regressions pass.
Support documentation and browser checks pass. Hosted integration completed in
[PR #1539](https://github.com/depsilon/shardloom/pull/1539), merged at `7d8230b2`
after all 39 checks passed. The
[hosted receipt](../benchmarks/evidence/native-input-growth-hosted-2026-10-09.json)
records unchanged runtime/tree identity and actual production verification.
Nested value-schema bounds,
deep traversal, richer intake and source/destination composition stay open;
this finite acceptance does not close the six-area/eight-investigation contract.

Reviewed FSST/Zstd payload buffers, actual Zstd decoder/prepared-dictionary
workspaces, and primitive/Boolean/decimal builder output and finalization overlap
also remain completed under their finite acceptance records. Broader ownership
coverage remains open. A native grant is not a process-RSS limit.

## Coverage that must remain visible

| Area and existing owner | Intake disposition and concrete remaining obligation | Completion evidence |
| --- | --- | --- |
| Stateful pressure and recovery — PERF-03/06/10/12 | General high-cardinality grouping/DISTINCT and relational ordering are accepted in PR #1531. Oversized join build/intermediate state has complete local and hosted acceptance in PR #1532. Analytic windows have complete local and hosted pressure/workflow acceptance in PR #1533. Growing relational pivot domain/cell state has complete local pressure/workflow acceptance, independent packet inspection and hosted integration in PR #1534. Other unadmitted state and broader recovery retain their own contracts. Reuse shared native mechanisms only where each family's semantics permit. | Complete exact workloads beyond the admitted resident allowance; native reservations/runs and disk quota; cancellation, corruption, exhausted resources, owned cleanup and publication. Distinguish cleanup/restart from actual execution resume. An intentionally resident-only supported shape may close with a documented bound and deterministic growth denial, not a spill claim. |
| Remaining allocation coverage — PERF-03/06/08/09 | Merge into current resource inventory: child-decoder and selection scratch, structural metadata, compression contexts, dictionary training and other unreviewed reader/provider/builder allocations. Preserve completed provider fixes. | Valid credits before allocation, an implemented bounded/spill transition, or deterministic denial with cleanup for the supported workflow. Identify Python retention, conversion-library memory, allocator overhead and excluded providers separately. |
| Broader streaming — PERF-03/06/07/11/12 | Extend the existing single-use finite source deliberately: compatibility destinations and fanout; remaining bounded/spill stateful families and safe dynamic-schema admission; declared retention or spool for repeated batch sources/self-joins; exact decimal, binary, temporal and nested intake. Ordering/aggregation, complete drain for limits, joins and analytic windows are implemented and integrated. Preserve that completion contract when admitting further shapes. Dynamic pivot input still rejects before producer demand; ordinary pivot spill does not remove that admission boundary. These are separate missing contracts, not larger constants. | Single-pass input, bounded owners, complete schema/value/order checks, late failure and cancellation, sink cleanup/publication, and no silent replay. Cumulative input and top-level schema metadata now use shared-grant admission; preserve the 2,048-row / 8-MiB frame boundaries and explicit finite completion. Ordinary file-backed datasets do not inherit these transport bounds. |
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
closing one experiment does not trigger a release train. On October 9 the
maintainer explicitly authorized v0.5.0 after safe cleanup and current pivot
PR/documentation closeout, with full fresh UAT required **before** the version
bump. That [fresh UAT](../benchmarks/release-candidate-fresh-uat-2026-10-09.md)
and pivot integration are complete. The original v0.5.0 GitHub prerelease is
published, but its Windows native build stopped the registry workflow before
upload. Those assets remain immutable; the
[interruption record](../release/v0.5.0-channel-interruption.md) preserves the
original outcome. The same milestone is now published and independently
verified as v0.5.1 through GitHub, TestPyPI, PyPI and Homebrew. Its
[publication record](../release/v0.5.1-publication-verification.md) binds the
source, platform builds and channel proofs without relabeling pre-bump UAT.
Publication documents merged in PR #1537 at `0877ea9c` after all 39 checks
passed; the [production observation](../release/channel-proofs/website-v0.5.1-deployment.json)
verifies the exact deployment, four live pages and six public documents.
`RELEASE-050` is complete. Resume all remaining areas and all eight
investigations above, prioritizing natural-scale batch execution and
source/type/destination composition. This is a substantial
capability release, not a declaration that the whole completion contract is done.

The maintainer's later October 9 instruction supersedes discretionary release
timing: after the now-completed v0.5.1 train, keep the version fixed
until all remaining ShardLoom work is complete. Do not start another patch or
milestone bump for an intermediate capability or investigation. Continue the
six areas, all eight investigations and their acceptance/documentation work
under the existing phase owners without additional release trains.

For every cohesive unit, keep a reuse map, native-I/O and execution evidence,
resource/failure tests, complete public workflow checks, applicable regression
gates, adversarial review and hosted integration. Update the canonical phase
checklist and completed ledger with exact scope. Do not call this entire task
complete because its latest subunit passed.
