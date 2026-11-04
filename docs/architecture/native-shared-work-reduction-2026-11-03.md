# Shared work reduction across analytical requests

## Mandate and baseline

The November 3 maintainer request promotes six investigations after the
complete updated analytical observation. Development continues in a separate
worktree while the preceding PR completes its checks. Each existing PR keeps
its reviewed revision through integration, with corrections for actual failures
or review findings. The next review batches related investigations and retained
implementations into a coherent checkpoint.

The program remains RFC 0044 native execution under item 20. Relevant evidence
obligations include CG-5/6 correctness and benchmarks, CG-7/8 operators and
execution, CG-11 APIs, CG-13/14/15 encoded execution, resources and CPU
specialization, CG-16 certificates, and CG-19/20/21 native I/O and workflows.
The full CG-1 through CG-23 track retains its independent status.
[Phase plan](phased-execution-plan.md).

The frozen native baseline is the release-mode runtime from `9a8dd89c`,
with production-source equivalence checked at `90c17ec7`. All 10,000 primary
requests passed in 1,825.631237664 engine seconds. The saved Polars observations
supply the external reference. The comparable complete-request sums are
2,253.286915736 s for ShardLoom and 409.166984346 s for Polars. The difference
between ShardLoom's complete-request and engine clocks includes multiple kinds
of wrapper and delivery work. Each investigation must identify the work it
actually changes.
[Complete baseline](../benchmarks/native-expanded-updated-cohort-2026-11-03.md).

## Workstreams and decisions

| Workstream | Observed signal | Existing shared owner | Candidate and decision evidence |
|---|---|---|---|
| Window partition construction | Windows use 399.608 engine s in the updated cohort. Earlier prepared-window samples concentrate in partition ordering. | `Window`, `Table`, typed key owners, `RowIndex`, cancellable ordering and frame evaluation. | Build stable partition membership, preserve partition visitation order, and use the existing ordering check within each partition. Compare the complete declared affected cohort and its ordered, shuffled, nullable and high-cardinality controls. |
| Gather and owned output | Ordinary and multiway joins use 589.561 engine s. Existing profiles include substantial time inside output gathering. | `Gather`, `take_column_with_policy`, native payload copying and the host allocator, shared by joins, sets and group output. | Identify actual view construction, selection and final ownership costs. Reuse native provider operations or direct credited output construction where proofs remove intermediate work. Keep an implementation only after complete output, lifetime and matched timing checks. |
| Prepared validation and payload demand | Explicit narrower membership and ranking declarations use cheaper paths. Prepared wide Top-K retains an alternate-route deadline. | Bound scan demand, prepared-source identity, operator-specific payload validation, ranked positions and selected-payload readers. | Carry payload requirements separately from required checks. Prove selected-row and first-error behavior before widening admission. Compare equivalent original declarations with existing native resource bounds. |
| Public construction | The complete-request residual is 427.656 s. Earlier fresh declarations constructed 47 client-export and 31 plan dataclasses. | Existing lazy exports, client/context/query modules and shared report/value models. | Create a cheaper ordinary construction dependency path while preserving names, types, introspection and errors. Evaluate original fresh-process complete-request clocks on a declared cohort. |
| Prepared read ownership | Earlier reservation census attributes 511.0 MB to host buffers. Three requested fields occupy 13.1 MB of segment data in the inspected file. | Native `VortexReadAt`, the bounded read observer, pinned coalescer, allocator leases and consumer slices. | Capture requested segments, issued extents, backing identity, consumption and release. Choose any policy from that observed chain, with narrow and sequential controls. |
| General aggregation and sets | Grouped aggregation uses 123.297 engine s and sets use 133.725 s. Most set declarations compose reduction and bounded ordering. | Compact/general reducer state, typed keys, shared gather and finalization. | Test compact extrema and shared finalization improvements on their actual composed consumers. Report provider eligibility and complete-query outcomes separately. |

Each row begins as a candidate. A retained implementation requires actual
semantic, resource, lifetime and timing evidence. The earlier general nullable
expression deferral remains dropped. The named Unicode case has no useful
early-character-exit work to remove. Pruning and persisted-size findings retain
their existing measured scope.
[Earlier dispositions](../benchmarks/native-expanded-work-investigations-2026-11-03.md).

## Window contract

The resident implementation validates every ordering-key value during input.
It then allocates function outputs and, for each declared window group, admits
the ordinal workspace and validates the complete group's NULL-order policy.
Partition-local execution preserves these boundaries.

Eligibility requires the existing uniform flat comparison-domain proof.
The existing native ordering implementation handles the other admitted
domains. The candidate groups source ordinals with `RowIndex` and exact
typed key equality. Hash collisions always reach that equality check. NULL
partition values retain their current equality and ordering.

Partition representatives are ordered with the original partition comparator
before any frame is evaluated. This preserves lexicographic partition
visitation and first-error ordering. Each partition retains source-order
ordinals and passes through the existing cancellable order check and native
sort. Source ordinal remains the final tie-breaker. Frame arithmetic,
ranking, navigation, output order and spill execution keep their existing
owners.

All additional containers own reservations. The ordinary ordinal workspace
is admitted before the existing NULL-policy scan. A failed optimization
reservation must release its temporary state and return to the original
native ordering strategy before any frame output is written. Cancellation
and data errors retain their diagnostics. Include low-budget controls,
multiple order groups, duplicate ordering keys, empty and singleton groups,
NULL partitions, descending order, shuffled input and mixed/nested domains.

The pinned Vortex 0.90 provider supplies native arrays, take, casts, masks,
ordering metadata and lane-level indexed source/sink kernels. Existing
ShardLoom key and table owners consume those arrays. Stable flat partition
membership preserves ShardLoom's frame, first-error and resource contracts
through its existing exact row index.
[Provider inventory](vortex-public-api-inventory.md).

## Gather and validation boundaries

`Table::gather` bounds upstream chunk metadata by constructing a view of
touched source segments. The numeric candidate reads already resident
primitive leaves into the final credited allocation. Encoded leaves and casts
retain their admitted provider path. Small outputs own their payload and keep
credits through clones, slices and producer teardown.

Complete index checks precede selected-value checks. Duplicate matches,
outer-null extension, original numeric widths and floating policy retain
their existing result contracts. Selected decimal and nested payloads use
their shared recursive owner.

Prepared joins retain their original input schemas, match population and output
column validation order. A separate internal payload mask may omit checked
selected columns from the emitted record. Hidden invalid values on selected
matches still fail at the existing column position. Unmatched source rows stay
outside that selection. The same recursive validator supports nested parents,
decimals and floating policy with zero retained payload rows.

Mixed streaming operations also retain their earlier complete build-input
validation through `detach_join_build`. That boundary can reject an invalid
unmatched build row before probing. Tests distinguish that scope from pure
prepared input's selected-row output checks.

Resident aggregate and join results establish their full emitted-value checks
before reaching a downstream ordering consumer. Their bounded final ordering
can use the existing streaming prefix owner, including grouped set results.
The independent spill strategy retains its own ordering boundary.

## Observed prepared read ownership

A diagnostic-only build based on the complete native baseline linked each
allocation to attempted and completed filesystem reads, coalesced return
extents, callback slices, actual segment-future consumption and final backing
release. The original 5M multi-key reduction returned the complete expected
result on both source routes. The prepared route performed 125 completed
filesystem reads containing 511,757,156 bytes. Its returned data extents
contained 506,943,228 bytes, while 2,444 consumed segments occupied 12,462,016
bytes. Coalescing gaps account for the other 494,481,212 data-extent bytes.
Every observed native allocation had a final release endpoint.

The first policy trial reproduced those byte counts. Prepared execution
reconstructed its data reader through `DeferredFileSegments`, whose owned-map
factory still selected the original policy. The follow-up passes the explicit
limit through that factory and adds a real segment-read regression. The first
trial remains a distinct no-effect observation.

The corrected factory trial completed both original source declarations with
all 31 expected rows. The prepared route issued 1,835 filesystem reads totaling
17,287,268 bytes, including metadata. Its returned data extents contained
12,473,340 bytes for the same 2,444 consumed segments and 12,462,016-byte useful
union. The remaining data-extent gaps totaled 11,324 bytes. All 1,843 observed
allocations reached their final release endpoints. Peak native reservations
fell from 516,436,046 to 12,646,044 bytes. The direct Parquet control retained
its 19,762,336-byte peak.

This policy trades 1,710 additional completed read requests for 494,469,888
fewer returned data bytes in that prepared declaration. The diagnostic build
establishes these read and ownership observations. The ordinary release build
supplies the complete-cohort timing decision. Provider regressions cover
sparse, contiguous and overlapping requests, and exercise the owned-map
factory through actual asynchronous reads.

The candidate admits a neighboring read only when its added aligned extent
is at most four times that request's length. This uses the actual pending
request shape. Contiguous and overlapping requests preserve coalescing under
the existing maximum-size and distance limits. Native allocation credits,
read concurrency, source identity and cleanup remain with their current
owners. Production uses the existing Vortex-file feature boundary and the
pinned 0.90 provider. The diagnostic instrumentation stays in its separate
source and runtime.

The candidate's resource trace, broad-read controls and complete native
cohort must pass before retention. Instrumented observations establish work
and ownership, while the ordinary release runtime supplies timing evidence.

## Public construction compatibility

The direct lazy report-model prototype preserves named imports, class
identity, signatures, dataclass fields and a representative pickle roundtrip.
It breaks two real compatibility checks. A fresh `get_type_hints` call on
`ShardLoomClient.vortex_prepare` cannot resolve the deferred report name.
`inspect.getsource` cannot resolve the relocated class while its public module
identity remains `shardloom.client`. This prototype is dropped. The remaining
construction cost remains visible in the next complete observation.

## General aggregate decision

The existing general typed-state owner already contains signed, unsigned and
floating extrema. Its eligibility differs from the specialized file provider.
This batch reuses that owner and extends bounded ordering after fully validated
aggregate and join outputs, including composed grouped set results. Family
results must identify the complete consumers affected by that shared change.

## Read ownership evidence

Reuse the existing test-only `ObservedFileReadAt` where it fits the declared
diagnostic. Its completed bytes include coalescing gaps and repeated reads.
They describe filesystem requests and preserve the distinction from device
traffic and process RSS.

The pinned driver can coalesce registered and polled requests. Resolution
slices a shared `BufferHandle`. The diagnostic trace records both callback
delivery and actual future consumption so unused prefetch stays distinguishable.
The measured identity chain above supplements the earlier range model and
allocation-origin census.

Any diagnostic has its own prospective record, byte and deadline limits.
Instrumentation observations stay separate from scored release-mode runs.
A proposed coalescing policy must include broad sequential consumers and
source-generation, cancellation, draining and ownership checks.

## Public surface compatibility

Root exports are already lazy. Ordinary context construction still imports
the broad client and query modules. Inspect each shared model's actual
consumers before moving it.

Preserve public named imports, export inventory, object identity, class module
and qualified name, dataclass behavior, representative pickle round trips,
signature/type-hint introspection and first errors. Module-level lazy lookup
alone does not resolve bare names in a function's global namespace.

The comparison must load the candidate public client inside the original
fresh-request boundary. Running the historical client against a new native
library would omit this workstream. Preserve submitted SQL/plan identity,
resource settings, full delivery and native attribution across both clients.

## Verification and retain/drop protocol

Before scoring, freeze the affected cases, controls, original case identities,
routes, inputs, expectations, runtime and public-client sources, resource
bounds and selected clock. Reuse the current completed native observation
where its source and measurement contract match. Select any additional
diagnostic or comparison for a concrete unresolved question.

Native changes use complete-cohort engine gain/loss after correctness,
ownership, resources and no-fallback checks. Public construction uses its
declared complete-request clock, with native timings retained separately.
Preserve individual increases and every failed attempt. Every selected request
receives its declared attempt count. The maintainer subsequently authorized
finishing these investigations and executing another complete 10,000-case
native pass to identify the next targets. That pass uses the existing manifest,
original order, one attempt per request, the latest completed native baseline
and saved Polars results. Freeze the combined candidate after focused checks,
then keep other task work idle throughout that execution. Full43 retains its
separate authorization gate.

Use focused native and Python regressions while changing the owners. The
coherent checkpoint includes strict native checks, workspace formatting,
Clippy with warnings denied, workspace tests, public-interface and installed
package coverage, complete independent result comparison, portable replay,
corruption controls and source/runtime identities. The final report records
an implementation or an evidence-backed disposition for each of the six rows.

## ShardLoom technique review

Partition-local work and bounded output batches fit existing capillary work
units. Native memory admission controls temporary state and backing retention.
Metadata and comparison-domain proofs precede value work where they preserve
semantics. Read-policy decisions should consume actual useful work and live
ownership through existing dynamic admission owners. Public and engine clocks
retain separate evidence tiers. PulseWeave remains an existing shared
scheduling mechanism whose use requires a measured consumer benefit.

Vortex-native input and output, native owned buffers, explicit failure and
certificate-backed execution govern every row. Expected runtime evidence
includes `fallback_attempted=false` and `external_engine_invoked=false`.
