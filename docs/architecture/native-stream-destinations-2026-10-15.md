<!-- SPDX-License-Identifier: Apache-2.0 -->

# Streaming input through local destinations

Status: local and hosted acceptance complete under PERF-03/06/07/11/12 and CG-5/19/20/21.
The [acceptance report](../benchmarks/native-stream-destinations-2026-10-15.md)
records 3,219 default workspace tests, 4,229 native tests, 84 direct Python tests,
777 public cases across 20 families and all 43 complete query results. The
test-only review follow-through passes 4,232 native tests. PR #1557 merges after
all 44 checks pass, with three installed-wheel lanes passing 84 tests each and
complete preview/production source and browser verification. The
[hosted packet](../benchmarks/evidence/native-stream-destinations-hosted-2026-10-15.json)
preserves 99 verified members. These source changes do not publish a release.
The [phase plan](phased-execution-plan.md) owns sequencing. This unit follows
the selected-view acceptance and dropped dictionary-growth experiment; native
transport remains after the optimization/breadth completion contract.

## Scope and reuse

Admit the existing finite streaming inputs through every already supported
local writer: native Vortex, JSON, JSONL, CSV, Parquet, Arrow IPC, Avro and ORC.
Keep each adapter's actual type, fidelity and feature admission. ORC's current
nested/decimal/temporal exclusions remain explicit. Multiple destinations must
consume each declared input producer once and execute the relational plan once.
Results are provisional until every producer ends and final validation succeeds.

Vortex-first decision: `use_vortex_native_provider`. The pinned Vortex 0.85
arrays, dtype/validity, native file writer and reader remain the payload providers.
Reuse `completed_result::write_stream_admitted`, `NativeSinkPlan`, the native
text and compatibility writers, and the existing credited source replay and
`QueryRunStore` machinery. Vortex's individual writer does not supply ShardLoom's
single-use producer, shared grant or multi-destination publication contract;
extend the existing coordination around these providers. No new representation,
codec, dependency, execution engine or Arrow-based internal execution is added.

ShardLoom technique review: static admission avoids producer work when possible;
bounded native replay preserves the existing capillary batch and shared resource
owners. Explicit spill uses the existing dynamic pressure policy. PulseWeave and
query scheduling are unchanged; complete engine and boundary clocks remain
separate from workflow and evidence-storage overhead.

| Existing component and callers | Remaining gap | Shared extension |
| --- | --- | --- |
| Relational `write_with_input` and all eight bounded writers | Batch adapter refuses compatibility destinations | Remove that guard after proving the existing writer completion contract across admitted types. |
| Batch-input replay with compact credited native copies and optional native runs | Replay statistics and visibility are coupled to input sources | Share its recorder/schema/completed owner with output retention; keep input and output copy/replay counters distinct. |
| Relational consumption and spill certification | Spill is cleaned immediately after input completion | Allow an internal completion callback to finish staged consumers before the same spill owner is cleaned and certified. |
| `output_fanout` target plans, private staging and create-if-absent publication | Writer loop assumes repeatable execution | Separate staging from publication; publish only after the single execution, all adapters, input validation and native-run cleanup succeed. |
| Native binding and subprocess batch routes | Both reject streaming fanout | Pass the existing provider through the shared relational writer and common report formatting. |
| Native Python reader and Arrow input bridge | The CLI routes multiple source identities, but the binding admits one producer | Add source URI/ordinal demand to the existing Arrow adapter and retain one lazy reader per declared source under the same grant. |

Resident fanout keeps its existing explicit per-destination execution strategy,
which can stream a large repeatable source without retaining the complete
result. Single-use input cannot use that strategy and requires the replay owner
below. This semantic difference does not create separate writer algorithms.
Existing Rust single-output and resident-fanout signatures remain intact; the
consuming batch adapter gains a multi-output method and one report containing
one execution plus its adapter outputs. Python method names and arguments stay
the same. Public evidence distinguishes execution count and output replay.

The binding's reader routing must preserve independent schema, ordinal, factory,
completion and error ownership for each source. Reader metadata is credited;
conversion scratch for the previous batch is released before the next demand,
including a switch to another producer. Existing single-source Rust signatures
stay available. Collection, incremental delivery, single writes and fanout use
the same source-aware adapter. Repeated references still consume their original
producer once and use native input replay. This extends local input dispatch;
it does not add a transport, alternate execution path or new Python API.

## Ownership, admission and publication

Validate target count, duplicate/existing paths, workspace policy, resources and
cancellation before producer demand. Prepare every statically known output
schema before execution; schema discovery may consume input before a downstream
type becomes knowable, retaining the existing dynamic-schema failure contract.
No target is published on such a failure.

A single destination continues to stream directly into its bounded writer.
For several destinations, compact the executed result into the existing native
replay owner under the query allocation. Preserve visible values, validity,
schema and order; record copied native buffer bytes separately from capacity,
provider decode work and RSS. This is explicit result retention, not an
unbounded switch to resident input or a zero-copy claim.

Without spill permission, exhaustion returns the existing deterministic resource
error. With an explicit spill policy, use the same stable native run store and
quota as input replay and stateful operators. Empty sort keys preserve input
ordinals and tie order. Do not create an unaccounted second spill store or replay
the original producer. Bounded readers must release each block after its writer
consumes it. Typed empty output must retain its schema.

After all inputs end, finish the replay owner and stage every adapter from it.
Release replay readers/owners and complete owned spill cleanup before constructing
the successful execution certificate. Only then publish the staged outputs.
Revalidate original source generations and destinations immediately before
publication. Writer, source, cancellation, quota, corruption and cleanup errors
must discard owned staging and leave final targets absent.

Local multi-file publication is not group-atomic. Preserve the existing
create-if-absent order and partial-commit diagnostic: previously published
complete targets remain, and the failing target plus prior paths are identified
for inspection before retry. Preserve unknown files and replaced generations.
No automatic retry, overwrite, rollback or resume is introduced.

## Evidence and acceptance

The expert comparator is a columnar execution maintainer checking complete
values and ownership through source completion, replay, adapter fidelity,
shared memory/spill accounting, publication and cancellation.

1. Native regressions cover all admitted single writers, multiple mixed
   destinations, empty/all-null/mixed values, exact rich types, ordering and
   source-consumption counts. Preserve resident writer/source-replay behavior.
2. Exercise multiple producers, repeated source references and dynamic schema
   through mixed outputs. Validate every complete output and source completion;
   no successful prefix or undocumented scalar reconstruction is acceptable.
3. Prove invalid targets/types and initial cancellation before demand where the
   schema is static. Cover late producer/schema errors, failure in a later
   adapter, cancellation, quota exhaustion, corrupt native replay and owned
   cleanup. Keep the existing partial-commit recovery tests.
4. Prove resident refusal and complete native replay at the same constrained
   grant with explicit spill permission, including all rows in every output,
   source and output copy/replay counters, and no retained memory/run owners.
5. Cover direct binding and public Python/SQL/DataFrame workflows, including the
   subprocess batch route. Reopen binary outputs natively; check complete text
   values using the established compatibility contracts. Both fallback fields
   remain false and each output keeps its fidelity/materialization evidence.
6. After the cohesive implementation is stable, run required workspace format,
   lint and tests; native/provider and direct/public regressions; one Full43
   pass on the current resident input; documentation checks and hosted review.
   Freeze exact sources/executables and preserve all failures. No replacement
   ingest is needed for this output-lifecycle change.

This is capability and lifecycle work. New streaming-fanout timing is descriptive
until a matched comparison exists; no speedup or process-wide memory bound is
claimed. Broad resource/operational coverage, the other six-area obligations,
eight conditional investigations and CG-1 through CG-23 keep their existing owners.
No package release or transport implementation follows from this unit.
