# Persisted native-output optimization and reuse

Status: accepted implementation scope, pending runtime work. The canonical
[phase plan](phased-execution-plan.md) owns sequencing and closeout through
`NATIVE-PERSISTED-REUSE`, under PERF-02/07/09/10/11/12 and CG-19/20/21.
This document records the October 11 maintainer clarification and the reusable
contract; it does not certify unimplemented behavior or measured gains.

## Required lifecycle

A job writes a correct Vortex result, closes the writer and releases its
session. A later process opens that file, possibly after a local move or copy,
and performs a different computation. The new computation should discover and
use the valid physical work already represented in the artifact: encodings,
dictionaries, statistics, useful layout properties and reusable preparation.
This is dataset preparation reuse, with a new execution for each request.

The implementation extends ordinary native writers, readers and planning.
Users should not need a special ShardLoom Vortex mode. Prefer existing pinned
Vortex serialization, layout, statistics and metadata facilities; keep the
artifact compatible with the admitted Vortex format. Record the provider version
and schema for any ShardLoom-specific optimization metadata. Prefer one native
file rather than introducing unapproved sidecars or query-answer tables.

## Evidence and intake decisions

The source review compares main `88b44c13` with the timestamp branch at
`20c3960d`. The named input, ownership and computed-result writer mechanisms
are unchanged between those revisions. Timestamp-provider restoration is a
separate prerequisite; it does not by itself complete this output contract.

| Candidate | Disposition and source-grounded scope |
| --- | --- |
| Existing native file admission | Already implemented. Native preparation supports existing-file pass-through and layout-preserving copies; preserve generation, resource and copy-I/O regression coverage. |
| Retained same-owner result/source intake | Already implemented. `ResidentMemorySource::from_completed_result` retains the existing native session and owners. Python `from_arrow` recognizes ShardLoom `NativeResult` and `NativeSource` before considering an Arrow stream. |
| Persisted output reused by another computation | Accepted as `required_for_v1`. The current computed-result sink preserves dtype/validity and serializable arrays, but explicitly reports `source_layout_user_metadata_not_copied` and `file_statistics_not_recomputed`. `NativeWriter::new` supplies an empty file-statistics set. |
| Encoding-preserving foreign-owner transfer and repeated transfer reuse | Merged into PERF-02/03/07/11/12 as `required_for_v1`. The current bounded native transfer can materialize encodings when moving into the receiving allocation. |
| Consistent recognition of native objects across entry points | Merged into existing composition owners as `required_for_v1`. Clarify the current native special case without adding another execution route; any API rename needs an explicit migration decision. |
| Independent upstream-Vortex producer or cross-process encoded interchange | Accepted as `v1_candidate_pending_feasibility` under existing native-I/O/interchange owners. Identify a real producer and supported public protocol before choosing an interface. Own-result recognition is not evidence of arbitrary upstream-object support. |

Relevant implementation boundaries are
[`local_primitive_native_sink.rs`](../../shardloom-vortex/src/local_primitive_native_sink.rs),
[`local_primitive_native_writer.rs`](../../shardloom-vortex/src/local_primitive_native_writer.rs),
[`resident_memory_source.rs`](../../shardloom-vortex/src/resident_memory_source.rs),
[`resident_memory_transfer.rs`](../../shardloom-vortex/src/resident_memory_transfer.rs),
[`resident_session.rs`](../../shardloom-vortex/src/resident_session.rs),
[`resident_segment_reuse.rs`](../../shardloom-vortex/src/resident_segment_reuse.rs)
and [`session.rs`](../../shardloom-python/src/session.rs).
Generation-bound prepared-source segment reuse and retained in-memory results
remain distinct from reopening a persisted result for a later workload.

The existing shared `NativeWriter` is used by the native sink and query run
store. Pinned Vortex 0.85 already exposes configurable file statistics and an
accumulator around the output stream; its default write options select
`PRUNING_STATS`, while ShardLoom's wrapper explicitly selects an empty set.
Inspect and adapt that provider surface before adding a parallel accumulator.
Its current implementation collects top-level fields and rejects nullable
top-level structs; nested field semantics, variable-length statistic bounds,
allocation ownership and failure behavior need explicit coverage before
enabling it across computed outputs. Enabling a writer option alone does not
complete the downstream-use contract.

## Properties describe the actual output

Each operation must define which properties it preserves, derives, recomputes
or invalidates. Do not copy source statistics or ordering declarations merely
because input and output use the same format. A filter changes exact counts;
an aggregate changes row meaning and value ranges; projection/rename changes
field identity; joins and sorts can change ordering and partition boundaries.

| Property | Writer and reader obligation |
| --- | --- |
| Schema, dtype and validity | Preserve exact declared semantics, including nullable, nested, temporal and extension fields. Reopening must not narrow the supported value domain. |
| Encoded representation | Preserve admitted serializable encodings and dictionaries where possible; report any required materialization or encoding change. Native format alone does not imply identical physical buffers. |
| Row counts and statistics | Derive useful statistics from actual output batches under the shared grant. Distinguish exact values, conservative bounds and unknown values; test empty, all-null and filtered output. |
| Ordering and partitioning | Persist only properties established by the actual output operation and physical sequence. Bind field identity, comparison/null semantics and scope to the declaration. |
| Derived columns or auxiliary state | Bind reusable state to expression semantics, dtype, generation and provider/metadata versions. Validate compatibility before substituting it for repeated physical preparation. |
| Missing, invalid or incompatible optimization metadata | Preserve correct native execution with conservative planning or a deterministic artifact diagnostic where integrity is affected. Invalid metadata must never prune a matching row or change values. |

The reader should discover admitted metadata before payload work, and evidence
must distinguish metadata discovery from actual use. A field in a certificate
does not prove pruning, avoided decoding or avoided reconstruction. Statistics
can be accumulated while batches already flow through the writer; any additional
pass, retained state or preprocessing must be visible and resource-accounted.

The reuse map must cover existing ingest-prepared, source-backed, completed,
produced, streamed and spill-produced output paths. Extend their shared writer
and admission components where semantics agree. Keep justified physical
strategies explicit rather than forcing every output through one expensive
preparation policy.

## Ownership and interchange remain separate contracts

Same-owner retained objects can share their existing owners. A foreign-owner
copy must charge the receiving allocation and preserve exact values, validity,
schema and lifetime behavior. Extend the bounded copier to carry admitted encoded
buffers when that avoids expansion. Reuse an admitted transfer or prepared
source in later operations without reusing a previous query's final answer.

Immutable sharing across allocations requires explicit charging, lifetime,
revocation and isolation semantics. It is not an automatic consequence of both
objects being Vortex-native. The isolated receiving-allocation path remains a
valid supported contract.

For an independent upstream producer, inspect the pinned file, serialized-array
and native IPC interfaces and the producer's public export contract. Establish
schema/encoding-registry/version compatibility, alignment, buffer ownership,
resource limits, cancellation and corrupt-input behavior. Use a feasible native
protocol when it eliminates a real conversion. If unavailable, record the exact
external dependency and supported native-file alternative; avoid an indefinite
generic deferral. Measure alignment copies and receiver allocations explicitly.

The upstream [file-format](https://docs.vortex.dev/concepts/file-format),
[arrays](https://docs.vortex.dev/concepts/arrays) and
[serialization](https://docs.vortex.dev/developer-guide/internals/serialization)
documentation explains the relevant concepts. Current documentation is not proof
of pinned Vortex 0.85 API behavior, and its IPC discussion includes alignment
copies. No universal zero-copy or zero-decode claim follows from these references.

## Acceptance and cost gates

Acceptance executes the whole lifecycle through ordinary public surfaces:

1. Produce native output from both admitted external inputs and existing native
   inputs, including computed, empty, nullable/nested and pressure-sized output.
2. Finish and durably close the output, tear down the producing process, then
   copy or move representative artifacts without retaining an in-memory owner.
3. Open the artifact in a fresh process and execute at least two different
   downstream computations. Compare complete schemas, values, nulls and ordering
   with independent expected results.
4. Prove conservative pruning and actual reuse with counters for read, copied,
   decoded, materialized and retained bytes. Exercise absent, stale, malformed
   and incompatible metadata, changed field identities and relevant type limits.
5. Verify shared resource denial, cancellation, atomic publication, previous
   output preservation, cleanup and all owner releases across failure boundaries.
6. Run applicable focused writer/preparation/statistics and public/Python
   lifecycle tests, required workspace gates, complete public/format regressions
   and fresh full UAT before accepting retained runtime changes.

Freeze the performance protocol and retain/drop thresholds before timing.
Record Job A preparation/write cost, output footprint and metadata cost;
separately record first and repeated Job B/C read/compute cost. Include small
temporary outputs and cases that cannot exploit the new properties. Costlier
physical preparation should ship only for a declared workload or admitted
strategy whose complete lifecycle meets its gate. Do not hide write cost in
untimed setup or claim gains from already implemented native-file admission.

ShardLoom technique review: metadata-first planning, encoded native arrays and
late materialization apply directly. Charge capillary writer windows and metadata
before allocation, use admitted dynamic work shaping for optional preparation,
and retain separate write/first-use/reuse timing surfaces and evidence tiers.
PulseWeave or scheduling changes require observed work and scarcity evidence;
metadata labels alone do not establish their use.

Keep `fallback_attempted=false` and `external_engine_invoked=false`. Version
0.5.1 remains fixed under the existing instruction. This intake does not reopen
paused format experiments, publish packages, create another engine or remove
any of the six remaining areas, eight investigations or CG-1 through CG-23.
