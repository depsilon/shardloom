# Persisted native-output optimization and reuse

Status: local lifecycle, cost, source/package, complete public and fresh-UAT
acceptance pass, with independently verified evidence in the
[acceptance report](../benchmarks/native-persisted-reuse-2026-10-11.md).
Hosted checks, review and merge remain pending. The canonical
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
| Persisted output reused by another computation | Accepted as `required_for_v1`. At intake, the computed-result sink preserved dtype/validity and serializable arrays but omitted file statistics. The shared durable writer now derives supported statistics from its actual output; complete local lifecycle/cost/regression acceptance passes. Hosted integration remains pending. |
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
store. Pinned Vortex 0.85 exposes configurable file statistics and an accumulator
around the output stream; its default write options select `PRUNING_STATS`.
At intake, ShardLoom's wrapper selected an empty set. The implemented extension
uses the existing provider for durable output and keeps that empty-set policy
for temporary query runs. The unpatched provider collects top-level fields and rejects nullable
top-level structs; nested field semantics, variable-length statistic bounds,
allocation ownership and failure behavior need explicit coverage before
enabling it across computed outputs. Enabling a writer option alone does not
complete the downstream-use contract.

### Bounded provider decision

Adapt the pinned native file-statistics accumulator in `vortex-layout` rather
than create a second metadata format or rewrite serialized footers. Its unpatched
per-batch builders retain 1,024 initial entries per selected statistic and grow
with the file's batch count. They also aggregate nullable batch sums by skipping
nulls, so an overflowed batch can disappear from the reported file total.

The replacement keeps one running statistic per admitted top-level field and
uses the existing native `StatsSet` merge and scalar truncation operations.
Unknown observations remain unknown; all-null batches are distinguishable from
an uncomputable extremum. Truncated bounds remain conservative, and an absent
finite upper bound must invalidate the file maximum. Bounded string/binary
scalars own compact bytes instead of retaining a whole input allocation. Exact
counts and checked sum merging preserve overflow and null semantics. Nested
values remain exact native payload; unsupported nested extrema remain absent.

Reserve the field-dependent retained state and footer workspace under the
existing execution grant before creating a durable writer. Native provider
compute remains inside the admitted session. Keep ephemeral run-store writes
on their existing no-file-statistics policy. Ordinary durable writers enable
useful file statistics only after provider regressions, resource-lifetime tests
and the complete write/reopen lifecycle gate pass. The provider format, version,
layout strategy and no-fallback boundary remain unchanged. The linked acceptance
report records the passing local evidence and its bounded performance claims;
hosted integration remains a separate gate.

### Shared writer and property map

| Output family | Existing provider and extension |
| --- | --- |
| Ingest-prepared single array or bounded stream | `LocalVortexWriteContext` uses pinned native write options and its selected layout strategy. Reserve the bounded statistics/footer state under the same `LiveMemoryPool`; keep the existing source-generation metadata and staging validation. |
| Source-backed projection, filter or limit | `NativeSinkPlan::write_produced` submits the actual selected arrays to `NativeWriter::persisted`. Derive statistics from those arrays, so discarded rows cannot survive in exact counts or bounds. |
| Completed native results | The same sink reads retained result chunks and passes them through the shared durable writer. It does not import the original source's statistics or user metadata. |
| Produced, streamed and spill-produced results | The same sink receives bounded producer batches. Layout-reference reservations grow with emitted chunks; file-statistics state depends on fields, not chunks. Final spill output uses this sink; intermediate query runs keep file statistics disabled. |
| Existing native-file pass-through or byte copy | Preserve the original artifact, including its existing metadata and encoding. This path does not regenerate missing statistics or claim a new optimization. |
| In-memory file generation | Uses the lower-level native layout writer and retained memory owners. It is not a persisted file and does not establish the durable lifecycle contract. |
| Historical single-count payload writer | A fixed one-value native artifact uses upstream write options. Its bounded payload path is separate from the resource-admitted computed-result stream. It is not evidence of general result persistence. |

The durable sink reserves 64 KiB plus 8 KiB per top-level field before opening
staging. This covers up to five running scalar sets, merge temporaries, compact
64-byte variable-length bounds, finalized sets, serialization and the reopened
footer. Provider compute retains the session allocator; schema and layout
containers have separate reservations. The reservation transfers into the final
writer summary and remains live through validation. Terminal errors and writer
abandonment drain accepted work before releasing it.

| Operation | Properties established by its output |
| --- | --- |
| Projection or rename | Field order, names and declared dtypes describe the new file. Directly retained serializable arrays can retain encodings; statistics bind to the new field slots. Source layout and derived-column declarations are invalidated. |
| Filter or limit | Preserve the emitted row sequence and field semantics. Recompute counts and bounds from emitted rows; never retain exact input counts. |
| Aggregate or expression | New values, dtypes, names and nullability come from the result schema. Recompute supported statistics. Input ordering, partition declarations and derivation metadata are absent. |
| Join or union | Preserve the runtime's declared result schema and emitted rows. Input-side field positions and bounds cannot be reused as result properties. |
| Sort | Persist the actual emitted sequence. No reusable sortedness declaration is published merely from the operation name; comparator/null-order metadata needs its own validated representation and measured consumer. |
| Empty or all-null result | Keep the declared dtype and exact row count. Missing extrema remain unknown; null counts are exact where the provider can establish them. |
| Nested values or nullable root records | Keep exact native payload and validity. Unsupported nested extrema are absent. A nullable root retains one empty file-statistics set per field, avoiding unsafe unmasked child bounds. |

This change introduces no custom optimization-metadata schema, derived-state
substitution or sortedness/partition claims. Native file statistics use the
pinned provider's existing serialization and dtype validation. Existing derived
metadata remains governed by its own generation/version checks; the computed
writer does not copy it. Additional compression or layout preparation must pass
the lifecycle gate before it becomes a default cost for computed output.

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

### Frozen local lifecycle protocol

Before timing, use `scripts/run_native_output_uat.py --measure-persisted-reuse`
with 40,000 fixture rows, five measured alternating pairs after one warm-up pair,
and two admitted workers. Write separate 64-, 10,003- and 40,000-row outputs.
Each has renamed native integer columns and nullable Unicode strings. The
baseline is the frozen release from `20c3960d`, CLI SHA-256
`2476d1edbdec5977b1d076d06bf317ffe345bb47a30af7b3c53b9164f62c334f`.
The subsequent timestamp merge changes only the nested-field certificate text
and its regression test; it does not alter that baseline's writer algorithm.
Freeze the candidate source map, release build and executable hash separately.

For each newly closed output, time three different ordinary SQL computations
in fresh processes: an integer count/minimum/maximum aggregate, a disjoint-range
projection, and a surviving filtered/sorted projection that must read payload.
Run each three times before the independent full-value validation read. Preserve
all samples, exact independent fixture answers, publication footprint, native
read/decode/copy/resource counters and supervised process observations. Bind
every SQL source to its exact quoted native-file path, including relational
sorts. The candidate aggregate must use exact file-footer completion and return
zero scan arrays; the baseline must consume nonzero arrays. The control must
deliver nonzero native source rows before its filter and sort on both builds.
The public plain-projection report does not expose payload-request counts, so
its empty result is correctness and timing evidence only. The separate
`persisted_statistics_prevent_resident_projection_payload_requests` regression
reopens a moved computed output after deleting its input, uses the same resident
projection provider, and observes zero native segment requests for a disjoint
range and nonzero requests for a surviving range with exact expected values.
Segment requests are not physical device bytes. The fresh-process
integration test separately removes original input and prepared copies before
reopening moved or copied outputs, including empty, typed and spilled results.

The initial attempt stopped on its first warm-up output, before any measured
pair completed: the driver incorrectly required the aggregate's array counter
from a plain projection. Investigation also found that a placeholder SQL table
name did not bind the later relational sort to its output path. Preserve that
attempt and the route probes. Correct source binding and report interpretation
before restarting the complete cohort; keep the original queries, fixture,
pairing, sample count and all cost thresholds unchanged. A returned-array count
after filtering is not a count of physical reads and must not be used as one.

The retained-state proof and complete-value correctness gates are mandatory.
For every output size, the median paired cost margin must satisfy all of these:
write cost is at most the greater of baseline plus 10% or 5 ms; the sum of write
plus all first computations and the sum of write plus all nine computations are
each within 5% of baseline; the payload-reading control is within the greater of
5% or 5 ms; the output footprint increases by no more than the greater of 4 KiB
or 1%. These tolerances are frozen before measurements, not tuned from a result.
Failure keeps the item open for diagnosis or a narrower admitted strategy.

Report first/repeated compute and write clocks separately from actual supervised
workflow wall time. Sums of process clocks exclude harness validation, hashing
and archive work; the workflow clock includes them. OS page-cache state is
uncontrolled, and newly written files can already be cached. No cold-cache,
confidence-interval, cross-hardware or competitive superiority claim follows.

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
