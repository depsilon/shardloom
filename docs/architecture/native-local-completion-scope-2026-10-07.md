# Remaining local workflow and optimization scope

Status: maintainer-reaffirmed scope, October 7, 2026. This is a coverage contract
for the existing remaining optimization/breadth task. The
[phase plan](phased-execution-plan.md) owns implementation order and checklists;
this document is not a second queue or a new set of competitive gates.

The [October 18 reconciliation](native-roadmap-reconciliation-2026-10-18.md) records a 14-owner, 54-step snapshot. The subsequent [CPU investigation intake](native-cpu-investigations-2026-10-18.md) adds four conditional investigations and extends existing summary, membership and publication obligations under those same owners. The [phase plan](phased-execution-plan.md) owns current counts and ordering; historical assessments and experiment contracts below remain intact.

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

The later [exact typed-input unit](../benchmarks/native-typed-input-2026-10-10.md)
has complete local engine acceptance and independent inspection for exact widths,
binary, Decimal128, Date32, full-domain microsecond timestamps and recursive
lists/structs through resident, buffered and finite single-use workflows. Its
Python result correction preserves exact temporal storage values outside Python's
calendar. [PR #1540](https://github.com/depsilon/shardloom/pull/1540) merged at
`316f52de` after all 39 hosted checks passed; its
[hosted receipt](../benchmarks/evidence/native-typed-input-hosted-2026-10-10.json)
records the unchanged reviewed/merged tree and production verification. Broader
type/source/destination composition remains open; this extends the historical
scalar intake.

The October 10 direction prioritizes [required explicit resources](required-execution-resources-2026-10-10.md)
before data inspection or execution. That unit and [native Python ownership and
columnar exchange](native-python-surface-2026-10-10.md) completed together in
[PR #1541](https://github.com/depsilon/shardloom/pull/1541), merged at `88b44c13`
after all 44 hosted checks passed. The
[hosted receipt](../benchmarks/evidence/native-python-surface-hosted-2026-10-11.json)
records the identical reviewed/merged trees, three installed Linux/macOS wheels
passing all 75 native methods, and actual production verification. Original
local workflow measurements keep their own source identities. This completes
the native binding/resource unit, not the six remaining areas below.

The October 11 follow-up distinguishes broader streaming/source composition,
encoding-preserving cross-client transfer, high-level plan-construction overhead
and actual platform-runtime proof from that completed unit.
[Persisted-output reuse](native-persisted-reuse-2026-10-11.md) adds a specific
lifecycle under the same owners: write and close a native file, release the
producer, then reopen it in a fresh process for a different computation. Native
file/object intake already exists; useful output statistics and physical
preparation must be persisted, recognized and used without blindly inheriting
input properties. That finite implementation and its cost gates completed in
PR #1544. Encoded receiving copies and independent Vortex producer intake then
completed in PRs #1545/#1547, with the [hosted receipt](../benchmarks/evidence/native-encoded-hosted-2026-10-14.json)
preserving exact sources, 43/44 checks and six installed-wheel observations.
Useful existing workloads that still refuse resource admission remain
implementation work; an admission refusal does not close them. Full-domain
timestamp statistics/compression retains its separate provider acceptance. The
unbuilt earlier native-binding experiment was not a failed benchmark.

Reviewed FSST/Zstd payload buffers, actual Zstd decoder/prepared-dictionary
workspaces, and primitive/Boolean/decimal builder output and finalization overlap
also remain completed under their finite acceptance records. Broader ownership
coverage remains open. A native grant is not a process-RSS limit.

The [selection resource unit](native-selection-resources-2026-10-13.md) passes
local provider/aggregate-context refusal and retention checks, complete public
and Full43 regression, and its paired cost gate after rejecting the first
candidate. [PR #1550](https://github.com/depsilon/shardloom/pull/1550) integrates
its source-bound proof after all 44 checks pass, with unchanged accepted source
and verified production content. Other child-decoder scratch and metadata are
still separate work.

The [fixed-width decoder unit](native-decoder-scratch-2026-10-14.md) now passes
local resource/cost checks, complete public workflows and Full43. It admits the
reviewed child-buffer trees during native Chunked append and the FSST metadata
finish overlap, preserving direct append and fusion. Portable evidence is complete.
The [matched composition investigation](../benchmarks/native-composition-attribution-2026-10-14.md)
does not reproduce the historical increase under its declared thresholds and
does not establish the historical cause. The
[native helper correction](native-admitted-context-2026-10-14.md) preserves the
actual query context and provider errors. Its [combined acceptance](../benchmarks/native-admitted-context-2026-10-14.md)
passes the final direct/public workflows and 516 matched complete query calls
with no repeated declared cost breach. [PR #1551](https://github.com/depsilon/shardloom/pull/1551)
merged at `b6d9318d` after all 44 hosted checks passed. Its
[hosted receipt](../benchmarks/evidence/native-decoder-hosted-2026-10-15.json)
records the identical reviewed/merged trees, three installed Linux/macOS wheels
passing all 80 native methods, and production verification. The separate
[fresh unpaced acceptance](../benchmarks/native-fresh-unpaced-uat-2026-10-14.md)
keeps its own source, executable and timing protocol. Unreviewed decoder trees,
metadata and compression/training retain their owners.

The later [compression resource unit](native-compression-resources-2026-10-15.md)
passes complete local acceptance for named primitive/variable-binary compression
buffers, the pinned one-shot compressor and default dictionary trainer. Its
[report](../benchmarks/native-compression-resources-2026-10-15.md) retains the
18-cell lifecycle gate, complete public workflows, one Full43 pass and original
failures in 3,068 verified evidence members. PR #1555 merged after all 44 checks
passed; its [hosted packet](../benchmarks/evidence/native-compression-hosted-2026-10-15.json)
verifies the native and installed-wheel logs, accepted source and production docs.
Its original claim excludes structural frame/metadata storage. The subsequent
[frame metadata extension](native-zstd-metadata-resources-2026-10-16.md) now has
[complete local acceptance](../benchmarks/native-zstd-metadata-resources-2026-10-16.md),
including preallocation admission, shared clones and fresh receiving ownership.
PR #1562 completes hosted integration after all 44 checks, three installed-wheel
lanes and preview/production verification pass. Other metadata, codecs and unreviewed
provider trees remain open. The 0.6.0 packages are unchanged.

The subsequent [selected text views](native-selected-text-views-2026-10-15.md)
avoid constructing unselected views inside the admitted sparse Zstd filter.
The bounded target comparison and complete local acceptance pass, with all four
other parallel control medians slower within their frozen limits. The
[report and 2,305-member packet](../benchmarks/native-selected-text-views-2026-10-15.md)
retain complete values, resource/lifetime regressions, all 43 query timings and
five diagnostic profiles. PR #1556 is integrated after 44 successful checks,
three installed-wheel lanes and verified preview/production source. The later
[deployment addendum](../benchmarks/evidence/native-stream-destinations-hosted-2026-10-15.json)
confirms its completed production check. This finite provider change preserves
the remaining six-area and breadth obligations below.

The [streaming destination unit](native-stream-destinations-2026-10-15.md) is
integrated in PR #1557 after all 44 checks pass. All eight existing local writers
and one-execution fanout retain complete local acceptance; three installed-wheel
lanes each pass 84 tests, and preview/production source and navigation are
verified. Its finite closure preserves writer fidelity, explicit native replay
and non-atomic multi-file publication. The streaming set unit passes
[complete local and hosted acceptance](../benchmarks/native-stream-sets-2026-10-15.md): all
six Rust gates, 87 direct tests, 1,121 public cases and one complete 43-query pass.
Its 5,460-member packet preserves original failures and exact source transitions.
PR #1558 is merged after all 44 checks, three installed-wheel lanes passing
87 tests each, and verified preview/production source, deployment and navigation.

The [finite unary unit](native-stream-unary-2026-10-15.md) now passes local runtime
acceptance: all six Rust gates, 91 direct methods, 2,601 public cases and one
complete 43-query pass. Its [report](../benchmarks/native-stream-unary-2026-10-15.md)
preserves failed fixtures and corrections alongside complete evidence. Independent
archive inspection and documentation verification pass. PR #1559 is integrated
after all 44 hosted checks, three installed-wheel lanes and verified
preview/production acceptance, as recorded in the
[completed ledger](phased-execution-completed-ledger.md). Resident unary-state
limits and the broader obligations below remain separate.

The subsequent [static-inner subquery unit](native-stream-subqueries-2026-10-15.md)
and [declared dynamic-subquery unit](../benchmarks/native-stream-dynamic-subqueries-2026-10-16.md)
complete the scoped streaming subquery families. PRs #1560 and #1561 pass all
44 hosted checks and verified preview/production acceptance, as recorded in the
[completed ledger](phased-execution-completed-ledger.md). Their finite source,
schema and membership boundaries do not close the broader obligations below.

## Coverage that must remain visible

October 13 composition update: the
[streaming composition contract](native-stream-input-composition-2026-10-13.md)
supersedes this inventory's earlier single-source, repeated-source replay,
dynamic-pivot and disconnected compatibility-preparation limitations. Those
implementations and their current acceptance status are tracked there. The
unit is integrated in PR #1548 after all 44 hosted checks pass, with the accepted
source preserved and actual preview/production verification. The
remaining allocation, recovery, destination/fanout, platform and conditional
investigation obligations below remain separate; this update does not close a
whole PERF or CG gate.

The [operator recovery unit](native-operator-recovery-2026-10-13.md) is integrated
in PR #1549 after all 44 checks pass. Caller-controlled relational cleanup and
actual numeric-sort, single/compound COUNT and integer DISTINCT process-death,
owned cleanup and exact fresh restart are complete. Other state, allocation and
platform obligations below remain separate.

| Area and existing owner | Intake disposition and concrete remaining obligation | Completion evidence |
| --- | --- | --- |
| Stateful pressure and recovery — PERF-03/06/10/12 | General high-cardinality grouping/DISTINCT and relational ordering are accepted in PR #1531. Oversized join build/intermediate state has complete local and hosted acceptance in PR #1532. Analytic windows have complete local and hosted pressure/workflow acceptance in PR #1533. Growing relational pivot domain/cell state has complete local pressure/workflow acceptance, independent packet inspection and hosted integration in PR #1534. Other unadmitted state and broader recovery retain their own contracts. Reuse shared native mechanisms only where each family's semantics permit. | Complete exact workloads beyond the admitted resident allowance; native reservations/runs and disk quota; cancellation, corruption, exhausted resources, owned cleanup and publication. Distinguish cleanup/restart from actual execution resume. An intentionally resident-only supported shape may close with a documented bound and deterministic growth denial, not a spill claim. |
| Remaining allocation coverage — PERF-03/06/08/09 | Extend the current resource inventory beyond the completed selection/child-decoder, compressor/trainer and Zstd frame-vector units: other structural metadata, decoder/selection providers and unreviewed reader/provider/builder allocations. Preserve each completed unit's exact scope and evidence above. | Valid credits before allocation, an implemented bounded/spill transition, or deterministic denial with cleanup for the supported workflow. Identify Python retention, conversion-library memory, allocator overhead and excluded providers separately. |
| Broader streaming — PERF-03/06/07/11/12 | The composition unit is integrated for multiple finite producers, declared repeated-source/self-join replay, dynamic pivot discovery and exact rich-type native write/reopen. Ordering/aggregation, complete drain for limits, joins and analytic windows retain their existing acceptance. All eight existing local destinations and one-execution fanout are integrated in PR #1557; all four existing set operations are integrated in PR #1558; the nine static unary families are integrated in PR #1559; static-inner subqueries and declared parameter-dependent predicate inner subqueries are integrated in PRs #1560 and #1561. Remaining bounded/spill stateful families and unadmitted operator/type combinations retain separate obligations. Preserve accepted source-completion and ownership contracts when admitting further shapes. | A producer consumed once per execution, bounded owners, complete schema/value/order checks, late failure and cancellation, sink cleanup/publication, and declared native replay. Cumulative input and top-level schema metadata use shared-grant admission; preserve the 2,048-row / 8-MiB frame boundaries and explicit finite completion. Ordinary file-backed datasets do not inherit these transport bounds. |
| Adapter and composition coverage — PERF-02/07/10/11/12 and CG-19/20/21 | Merge into the universal-workflow matrix: typed/nested format boundaries, partition/schema evolution, prepared/public parity and remaining operator/type result streams. Automatic compatibility preparation now shares the incremental transaction's cancellation owner and native allocation; its local acceptance is recorded in the composition unit. `NATIVE-PERSISTED-REUSE` is integrated in PR #1544: output preparation survives writer close and producer teardown and is used by a fresh process running a different computation. Keep native retained-object transfer and independent-producer interchange separately evidenced. | Read → transform → retain or spill → consume again → write → reopen, checking intended types and every value. Format existence alone is insufficient. Small-result collect may remain deliberately bounded. |
| Defined local support and release — PERF-12 and existing local-engine/release gates | Bind a specific release to declared workloads, operators/types/formats/resource conditions and supported OS/architectures. Add actual platform runtime evidence, pressure/failure/slow-consumer acceptance, recovery promises, known issues, compatibility, upgrade/rollback and install guidance. | The [local-engine exit criteria](../release/production-certification-gate.md#local-engine-preview-exit-criteria), workload-scoped runtime records, supported/unsupported matrix and normal publication gates. Current full-size local UAT is macOS arm64 with Unix facilities. PR #1541 additionally proves 75 installed native methods on each named Linux/macOS wheel lane; that finite evidence does not complete the broader platform/workload contract. A successful cross-platform build alone is insufficient. Package publication itself is already implemented. |
| Conditional performance investigations — existing campaign/PERF owners | Preserve all eight rows below, attached to the components they may improve. Evidence prerequisites decide whether to prototype; each retained implementation still needs its frozen complete-operation gate. | Complete output, mechanism counters, grants and controls, original failures, all samples and an explicit retain/drop/defer decision. No investigation is a promised speedup. |

Cloud connectors, distributed execution, table transactions, full SQL/DataFrame
parity and Foundry-specific integration retain their separate roadmap owners.
They do not all have to complete before a stable, explicitly scoped local engine.
CG-1 through CG-23 remain visible and unchanged; no finite row closes a whole
gate or authorizes fallback execution.

## Eight investigations, with current evidence prerequisites

This dated section preserves the original eight investigations. The [October 18 CPU intake](native-cpu-investigations-2026-10-18.md) records four additions and the extensions to existing owners; neither list is a second execution queue.

October 17 sequencing update: after the current file-metadata correction and
fresh UAT finish integration, pause new capability expansion and prioritize
measured optimization of supported workloads. The six areas above and eight
investigations below retain their obligations; they are not prerequisites that
must all finish before optimization can resume. Correctness and resource fixes
needed by the selected workloads remain in scope. The canonical phase plan owns
the execution order and unchanged engine-time acceptance methodology.

The October 17 follow-up selects shared text/compound-key/state work first and
ingest duplication second, using the existing ship/drop process. Keep a short
production-breadth backlog beside that campaign: nullable-root correctness for
the default structural layout; native external-memory strategies for the
remaining resident membership/population families; and safe generation-checked
output replacement/publication with failure recovery. Replay spill does not
make consuming state spillable, and one-file publication does not establish
multi-destination atomicity. Address a correctness or ownership prerequisite
when the selected workload encounters it. The six areas and eight investigations
remain open; completed transfer and streaming foundations are not new work.

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

The October 9 instruction originally held the version after v0.5.1 until all
remaining work was complete. The maintainer's October 14 instruction supersedes
that hold: finish the current corrections and next PR, make one version bump,
then return to remaining work and optimizations. Those corrections are
integrated in PR #1551; [PR #1552](https://github.com/depsilon/shardloom/pull/1552)
prepares the resulting 0.6.0 release. Its
[publication record](../release/v0.6.0-publication-verification.md) binds GitHub,
TestPyPI, PyPI and Homebrew to actual artifacts and installation proofs.
Publication documents integrated in
[PR #1554](https://github.com/depsilon/shardloom/pull/1554) at `e8611a3c` after
all 44 checks passed. The [production observation](../release/channel-proofs/website-v0.6.0-deployment.json)
binds the deployed source, four live URLs and six public documents.
`RELEASE-060` is complete. The subsequent
[native compression admission](native-compression-resources-2026-10-15.md)
is complete: PR #1555 passed all 44 hosted checks, with its hosted packet
verifying the accepted source, native and installed-wheel logs, and production
documentation.
Historical UAT keeps its original source and executable identity. Earlier
published artifacts stay immutable; subsequent work under the six areas and
eight investigations does not receive a separate release for each unit.

For every cohesive unit, keep a reuse map, native-I/O and execution evidence,
resource/failure tests, complete public workflow checks, applicable regression
gates, adversarial review and hosted integration. Update the canonical phase
checklist and completed ledger with exact scope. Do not call this entire task
complete because its latest subunit passed.
