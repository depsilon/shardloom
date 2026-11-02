# Shared native adaptive execution

Status: implementation contract for phase-plan item 23. No engine-wide support
or performance claim is established by this document. RFC 0044 remains the
resource ownership authority; RFCs 0013/0014/0016/0017 govern streaming, memory,
semantics and recovery. The phase plan remains the active queue.

The November 1 maintainer decision starts this work before item 18's combined
portable-evidence, documentation and website closeout. Item 18's completed
runtime observations remain immutable. The original sequence placed a matched
timing check after integration. The November 2 instruction supersedes that timing
intent: execute the new dynamic runtime once to establish its next baseline,
without rerunning the fixed runtime, sweeping resources or treating this wiring
work as a tuning experiment. Correctness and resource acceptance remain required.

The maintainer subsequently authorized merging the verified existing I/O PR
separately. [PR #1640](https://github.com/depsilon/shardloom/pull/1640) merged at
`b767bc971a6a16fa49fb9b74a3d4c6eb9aa20aaf` after all 58 hosted checks passed and
both review findings were fixed. That merge does not close item 23, the broader
local read-only format additions, or the combined portable-evidence/docs/website
work. The new dynamic baseline below precedes the remaining integration checks
and new table-format readers. Full43 stays on hold.

## Dynamic runtime timing baseline

The November 2 single pass completes all 347 original queries once and verifies
all 12,565 result rows against their independent references. Engine time is
**12.144043291 seconds**. This is the new dynamic-runtime timing baseline,
not a performance-retention contest. No old-runtime query, resource sweep,
competitor execution or Full43 run was performed. The existing outer memory/CPU
ceilings remain enforced while the new runtime selects work dynamically.

The [compact observation record](../benchmarks/evidence/native-adaptive-runtime-baseline-2026-11-02.json)
binds the source, runtime and retained complete result evidence. Builds, subagents
and other local task work were idle during query execution. Full input/runtime
hashes and broad inventories occurred outside execution. Complete workspace,
installed-package and hosted acceptance remain pending for
[PR #1643](https://github.com/depsilon/shardloom/pull/1643); this observation does
not complete item 23 or establish engine-wide ownership coverage.

## Source and preparation milestone

The source/preparation boundary has completed local correctness and installed-wheel
verification. It includes shared current-headroom sizing for native scans/replay,
text and the Parquet/ORC/Avro continuations; physical IPC admission; retained
provider/schema/structural owners through complete ingestion; ordered Parquet
tasks on the shared ingest runtime; mapped Parquet and generation-bound ORC facts;
and feature-bound runtime discovery. Ordinary files and collections now use the
same admitted preparation path in full native Unix builds. Existing dictionary,
derived-column, extent, fingerprint and complete-source checks remain visible.

The required default workspace tests pass 3,302 tests, reduced write-only CLI
tests pass 1,195, and the enabled public CLI/library/integration targets pass
1,238, with no failures or ignored tests in those sets. Formatting and default
workspace/native CLI+Vortex all-target strict lint pass. The isolated wheel built
from the exact release CLI/library pair passes all 290 native integration tests
with no failures or skips, verifies installed artifact identities and completes
uninstall checks. This includes the new mapped Parquet count/pruning controls
across CLI SQL, embedded workflow, Python SQL, DataFrame and typed native callers.
The public CI lane now explicitly selects both capability-discovery test targets.
The wheel verifier clears inherited CLI-source overrides before running tests.

The complete native library run recorded 3,828 passes, seven low-memory diagnostic
assertion failures and 33 existing ignored tests. The adaptive preflight already
denied the intended operation, but omitted the established memory-denial category.
After correcting the diagnostic, all seven affected tests and all seven shared
sizing controls pass, including their unchanged spill/output/lifetime/cleanup
assertions. The original failed observation remains separate from that successful
focused verification; it is not relabeled as a clean full-suite run.

The current provider sources match the exact passing standalone configurations:

| Provider | Current verified library configuration | Result |
| --- | --- | --- |
| ORC 0.9 | No default features | 204 tests and strict library/test lint pass. |
| Arrow Avro 59.3.1 | No defaults; Deflate/Snappy, then optional custom types/small decimals | 427 and 448 tests pass; strict library/test lint passes. |
| Arrow IPC 59.3.1 | LZ4/Zstandard, then default features | 154 and 145 tests pass; strict library/test lint passes. |

The detailed sections below retain intermediate implementation observations;
their earlier test counts are historical scope, not substitute current coverage.
Provider versions remain unchanged. IPC messages remain indivisible, and the
controlled ORC LZO route explicitly rejects its unbounded provider API. Named
capacity/ownership coverage does not establish a complete allocator or RSS bound.

This milestone does not close item 23. Remaining conversion allocations, stateful
operators, spill/merge work, writer/output buffers, primitive fanout, worker/queue
integration and typed applied/declined decisions retain their active obligations.
Complete cross-format/interface acceptance and the matched engine checkpoint
still precede broader local table readers and the combined evidence/docs/site
closeout. No new timing observation, version bump or publication is included.

## Resource and progress contract

One `LiveMemoryPool` continues to own source metadata, native/provider inputs,
operator state, replay, merges, queues and retained output. Caller memory and
CPU limits are ceilings. No controller increases them, creates a substitute
grant, releases a live payload's credit, or changes the caller's spill policy.

Shared sizing lives in `shardloom-exec`, with representation-specific costs in
`shardloom-vortex`. The expert comparator is a memory-accounted execution engine
whose operators preserve semantics and make progress under backpressure. A row
count alone cannot establish decoder, merge or writer memory safety.

At a legal continuation boundary an owner supplies:

- Its smallest indivisible unit and maximum admitted geometry, including any
  caller batch bound or provider limit.
- Additional fixed workspace and per-unit cost, including simultaneous input,
  replacement, decode, index and writer work. Already retained owners are in
  the live snapshot and must not be counted as newly allocated workspace.
- Data/schema/representation facts and observed value sizes. Estimates shape
  work but do not replace actual reservations or establish a process RSS bound.
- The available worker/queue ceiling and whether work can actually be split,
  coalesced, flushed, spilled or throttled at this boundary.

The common policy observes current accounted headroom, selects a positive unit
count within those limits, and bounds per-lane overlap before admitting more
parallel work. Pressure shrinks the next unit immediately. Regrowth is limited
to a factor of two after two completed pressure-free windows. A selection is
not completion: only an owner that used the selected geometry may record it as
applied. Empty inputs execute no invented minimum batch. State is local to an
operation/owner and never learned persistently across runs.

Before real allocation the existing owner still obtains its `MemoryLease` or
allocator credit. A concurrent reservation can invalidate a sizing snapshot;
the authoritative reservation may then reject. Never retry a decoder, callback,
effect or publication after it has consumed input. Any permitted pre-allocation
retry must be finite, have no effects and preserve the original diagnostic.
When even the minimum indivisible workspace does not fit, report the limiting
owner, required additional bytes, accounted headroom and caller ceiling. Do not
oscillate or wait for an impossible operation. Existing native spill is usable
only where its operator continuation and caller permission already exist.

Semantic/type limits, recursion depth, cancellation cadence, wire framing,
format blocks, reference-proof bounds and workload fixture sizes are not
automatically tunable. Do not remove them as incidental constants. Preserve
full validation, first observable errors, lazy value domains, numeric reduction
order, stable ties, multiplicity, source generations, exactly-once callbacks,
output lifetime and all publication/cleanup barriers. Semantically observable
application batch boundaries are fixed unless their contract admits variation.

## Conversion and derived-column continuation

The runtime continuation extends existing admission owners through derived
columns, relational output compaction and native spill work. Complete-source
ingestion now carries its existing pool into derived schema and batch construction.
Named schema/spec/vector, dictionary map, builder and replacement capacities
are reserved before construction; retained output capacity stays with the owned
batch through queues and conversion. A metadata facade also retains derived
schema/name/status credits when it outlives the executing reader. Projected
schema structures are admitted before Arrow projection; a complete identity
projection reuses the already admitted source schema. The controlled-Parquet
per-buffer transfer proof remains authoritative. A fluent source builder keeps
its public signature and propagates admission failure through its reader before
returning any data. Caller-supplied compatibility readers retain their separate
input-allocation boundary; they do not acquire a new provider guarantee.

Derived evaluation retains its complete input-batch validation and dictionary
domain order. A post-read slice is not proof of smaller derived work, and a
dictionary domain cannot be made smaller by slicing its keys. Admit this named
workspace explicitly before changing that continuation contract. Source sizing
and actual reservations remain distinct. These changes are implementation work,
not complete conversion, allocator, RSS or engine-wide acceptance.

The conversion continuation passes 178 focused reader, derived-column, schema,
ownership and transfer checks, plus strict native Vortex all-target lint. Controls
cover pre-demand schema denial, terminal batch denial, empty/null/large string
layouts, unreferenced dictionary domains, shared sliced keys, metadata and batches
outliving readers, queue cancellation, source changes and complete Parquet transfer.
The first compilation failure used an unavailable Arrow string-view method; the
corrected implementation uses the pinned provider's length iterator. No new
benchmark result follows from these checks.

### Completed-record output compaction

The existing completed-record buffer now consults the shared sizing policy before
coalescing records and before constructing actual index/gather windows. Caller
row limits and the existing byte/chunk ceilings remain upper bounds. Costs use
current headroom, observed payload widths, new schema/slot/offset structures and
replacement/index overlap. Already retained input owners remain charged in the
same pool. Credits for the pending reference table and newly constructed chunk
structures outlive their payloads through success and failure paths.

Compaction is optional for these independently owned completed records. A typed
`WorkDecision::Declined` allows forwarding them in order before allocating a
replacement when the minimum workspace does not fit. Invalid geometry and
overflow remain errors; mandatory work still uses the original deterministic
memory-denial diagnostic. A preview neither reserves memory nor advances feedback.
Certificates count actual completed compactions separately from executed decisions
to decline compaction, including the limiting headroom and required additional
capacity. The existing native provider and downstream validation policy remain
authoritative; this is not execution fallback or a new allocator/RSS guarantee.

Eight shared-policy controls pass, including typed refusal without completed-work
credit or feedback changes. All three focused output controls and both adaptive
scan controls pass, including retained-output shrink/regrowth, optional refusal,
consumer failure and cancellation. Strict native Exec/Vortex all-target lint
passes for this output continuation. Operator acceptance and integration into the remaining
sort, spill, writer, fanout and queue owners remain in progress. Spill block framing
must stay consistent within each run; its adaptive continuation is the creation
of a new run, with subsequent readers bound to that run's actual block geometry.

### Native spill framing and merge continuation

Each newly written or merged ordering run now selects its physical block rows
from current shared-pool headroom. The shared policy accepts a finite ordered
set of candidate costs because smaller blocks increase retained footer/layout
metadata. Its existing completion and growth feedback remain authoritative;
an advisory choice still neither reserves memory nor counts as applied work.
All ten shared-policy controls pass, including nonmonotonic metadata costs,
typed refusal, stale admission and completion-driven regrowth.

The original gather and payload-validation windows remain unchanged. A physical
block may combine portions of two completed windows; the adapter uses Vortex
chunked arrays and native recursive canonicalization for that bounded join.
The existing sequential native flat writer drains each leaf. Costing includes
both retained windows, new block work, fixed-list expansion/child coordinates,
schema/slice structures, run metadata, and future reader-block overlap during
merge. Actual shared-pool reservations, disk quota and source-generation checks
remain authoritative. Stored positional access and detached replay use the
completed run's recorded geometry, including after a final merge.

All five focused spill controls pass: pressured 17-row physical blocks across
32-row validation windows, complete nested values and stable ties through mixed
run geometries, positional reads/replay and retained block lifetime, refusal
before run creation, preserved first payload error, consumer refusal and merge
cancellation with cleanup. The first runtime attempt passed two controls and
failed three when fixed-list child-index scratch exceeded the starting estimate.
That observation remains a failure; the corrected estimate accounts for the
existing expansion and coordinate reservations before choosing a run size.
Current strict native Exec/Vortex all-target lint passes. The broader relational,
spill and query-run regression batch passes all 306 tests, with no failures or
ignored tests, including join/aggregate/window composition, recovery and all
eight existing output writers. This is owner-level integration evidence, not
the remaining complete engine acceptance.

This is actual physical run adaptation, not smaller gather/validation work.
An irreducible gather window can still deny before writing, and variable-list
cardinality/provider transients are not bounded by schema estimates. Resident
sort output, other operator state, writer/queue/fanout integration and complete
engine acceptance remained open at that checkpoint. No new timing or performance
claim follows.

### Writer, sort-output and conversion-queue continuation

The native writer now uses the same completed-record buffer and preserves its
existing 128-row/256-KiB compaction threshold. Its optional copy statistics count
actual native bytes and scalars without changing raw float payloads. Parquet,
IPC, Avro and ORC writers share a persistent sizing owner at the existing
conversion boundary. Complete original input validation still precedes slicing;
physical footer/codec minima remain explicit. Retained writer high-water credits
survive through close because the providers can retain capacities after flush.
Pre-write narrowing is finite and effect-free; a consumed write is never retried.

The writer continuation passes the native fragmentation controls and all three
new four-format pressure, minimum-denial, cancellation, consumer-failure and
no-publication controls. Earlier failures remain recorded: one changed the native
fragmentation threshold and was repaired; another test incorrectly required an
initial four-row window although observed costs lawfully selected one before
regrowth. Strict native Exec/Vortex lint passed before the later queue change.

Resident sort adapts actual output gathers only when its owner has already
validated input payloads. Costs include retained source/order owners, payload and
schema replacement, ordinal/offset arrays, touched segments and nested gather
scratch. Unvalidated sort preserves the original field-major validation window
and first-error order. Twenty focused sort controls pass, including full nested
values and stable ties under pressure, regrowth, retained output, minimum denial,
consumer refusal, cancellation and unchanged competing payload errors.

The conversion queue selects actual newly admitted tasks under its existing CPU
and queue ceiling. Each opaque source batch keeps its original complete task
credit; native copy overlap is included separately. Existing tasks/results stay
owned. If the smallest new task does not fit, admitted work or a ready handoff
may progress; an empty queue with no ready output fails deterministically. An
initial deferral declines writer lookahead so the retained first array can still
progress serially. Only a fully completed non-EOF admission wave advances growth
feedback and ordinary ingest evidence. A repeated poll of the same pending output
is not another refill boundary.

The current queue/sort/source regression selection passes 70 tests with no
failures or ignored tests. The first attempt passed 68 and exposed repeated-wakeup
admission that distorted regrowth; the corrected runtime passed that original
control. A subsequent new blocked-reader test timed out because its fixture did
not drive the shared runtime. That fixture is corrected and the final 70-test
observation is separate from both failures. These task-window reports count work
admissions, not rows, allocator coverage, RSS or timing. Complete workspace,
public-interface and installed-runtime acceptance remains open.

### Primitive fanout spill contract

Continue using the existing native result recorder, native ordering runs and
per-format writers. One producer executes once, all complete destinations stage
before publication, and replay uses the same resident memory grant. Explicit
sort/aggregate spill permission may admit output replay without silently enabling
spill for other requests. Do not re-lower specialized aggregate or tie semantics.

The source audit verifies that numeric sort, weighted UTF8 COUNT and exact integer
DISTINCT clean their original run stores before their first output callback.
Memory owners can still overlap replay. Preserve that lifecycle, and bind cloned
primitive spill declarations plus replay to one run-byte quota scope so a future
or custom overlapping producer cannot multiply its disk grant. The quota scope
is execution-local, created without filesystem probing; renewed cancellation
starts a fresh declaration scope. Existing declarations may change their public
quota before execution, but changing an active scope's ceiling must fail.

Charge each store's marker allowance and every in-progress write before writing,
retain actual partial-file charges on failure, and refund committed bytes only
after owned unlink. Namespace/source identity, cancellation and cleanup remain
the existing run store's responsibility. Reports must distinguish replay's runs
and cleanup from the shared producer/replay quota peak. Test concurrent store
overlap, short/failed writes, denial, identity-preserving cleanup, complete results
larger than resident retention, exactly-once production and no partial publish.
This extends ShardLoom admission around the pinned Vortex native writer; it adds
no storage format, provider, execution fallback or new quota permission.

The implementation now shares one producer and replay owner across all nine
primitive routes. Focused controls verify complete JSONL, Vortex and Parquet
results, empty outputs, unchanged source reads when adding a destination, shared
quota denial and refunds, cancellation, and reusable prepared owners. The final
physical-spill/fanout selection passes 10 tests and the additional spill/run-store
selection passes 107, with none ignored. A calendar cancellation regression
exposed retained text backing and insufficient merge-overlap costing; compact
physical windows and geometry-aware overlap accounting repair it without raising
the grant. The failed observations remain part of the evidence.

All eight public fanout tests pass, including CLI and DataFrame sort/aggregate
paths with explicit spill permission, three complete destinations and rejection
before publication. The legacy command now retains the full replay report,
including actual spill and shared-quota evidence, through its existing result
envelope. Existing vector-returning native APIs retain their signatures. Strict
native Exec/Vortex/CLI all-target lint and the release CLI/Python build pass.
Complete workspace, installed-package and hosted acceptance remain pending.

The November 2 maintainer instruction moves a single new-runtime timing
observation ahead of remaining integration checks and requests this runtime
batch as a PR now. This establishes the dynamic runtime's next timing baseline;
it is not a tuning experiment or a request to re-execute the fixed runtime or
sweep resource limits. Preserve the original 347 cases and engine clock, all
earlier observations and the Full43 hold. Complete result verification is required
before reporting a total. Typed capability-decision evidence and the broader
ownership audit remain open; this checkpoint does not close item 23.

## Source-grounded owner map

This map is grounded at `27137227`; rows identify actual execution owners.
Legacy scalar CLI constructors and report-only provider inventories do not prove
ordinary public workflow routing. `PublicRelationalSources` prepares supported
local inputs for the shared relational binder and demanded producer. CLI SQL,
embedded workflows, Python SQL, DataFrame and typed callers converge there.

| Owner | Existing execution/cost boundary | Integration and necessary limits |
| --- | --- | --- |
| Metadata | `universal_format_io/demanded/source.rs`, `metadata_admission.rs`, `generic.rs`, `local_dataset.rs`; footer callback and inventory/schema leases precede retained allocation | Keep incremental footer admission. Complete schema/footer and directory identity are sometimes irreducible; row resizing cannot hide their cost. Distinguish parser scratch from retained metadata. |
| Native scans | `local_primitive_relational_scan.rs::run/run_file`; `PreparedVortexSource`, `NativeExecutionContext`, one prepared Vortex scan task per ordered row range | Adapt actual row ranges before `scan.execute`, not only output slices. Memory/batch inputs use the same policy before projection. Preserve filter/projection and provider-native representations. Specialized primitive scans require their own verified owner coverage. |
| Selected Parquet | `universal_format_io/demanded.rs::ColumnarReader`; selected encoded chunks plus 8192 bytes and the sum of selected columns' uncompressed row-group extents are reserved before decoder construction | Apply the shared row target before each decoder batch. Whole selected chunks and page/decode extents are separate from output row estimates; keep their admission. Metadata-selected scalar count remains zero-decode. |
| Other local formats/datasets | `demanded/generic.rs::Reader`; concrete Parquet/ORC/Avro decoder controls, owned indivisible IPC messages, separate text controller, then shared schema alignment and native intake | Parquet/ORC/Avro apply next-batch targets. IPC admits named message/codec/dictionary/structure capacities and preserves their owners; slicing does not resize physical IPC work. Complete public-route acceptance remains open. Provider transient allocations are not covered merely by a retained Arrow credit. |
| Text | `universal_format_io/text_stream.rs`; schema discovery and runtime batches reserve before record parsing/building, with actual bytes observed | Adapt between records/batches. A complete record is irreducible; preserve full JSON syntax, compressed framing, source identity and coercion/error order. |
| Conversion | `resident_memory_arrow.rs`, `ingest_arrow_ownership.rs`, `vortex_ingest_shared_conversion.rs` | Complete validation precedes transfer/copy. Use existing ownership and shared capacity; slicing must not weaken full-input validation or count foreign retained buffers as free. |
| Relational operators | `local_primitive_prepared_relational.rs::run` and `local_primitives/native_relational_*` | Propagate actual shape at each operator's scratch/output boundary. Global state needs reservation or admitted native spill; reducing input rows does not make irreducible retained state bounded. Numeric reduction order and lazy errors remain fixed. |
| Replay/capture | `local_primitive_relational_batch_replay.rs`, `local_primitive_relational_replay_capture.rs` | Use actual schema and payload costs before detach/capture; keep native payload credits, replay identity and exactly-once source demand. Coalescing is limited by replacement overlap, not only source rows. |
| Spill | `native_relational_spill.rs`, `native_relational_stored_order.rs`, `query_run_store*`, family-specific spill owners | Select write blocks/merge work before allocating, including footer/input/output overlap. Keep quotas, source identities, reader drain and exact cleanup. Run formats and complete merge/key states may impose tested minimum workspaces. |
| Output | `native_relational_output_buffer.rs`, `local_primitive_relational_writer*.rs`, `universal_format_io/completed_result*`, `local_primitive_native_writer.rs` | Adapt compaction/producer buffers and downstream admission under the shared grant. Keep complete fanout computation before stage/publish, retained result lifetime and explicit format fidelity. Provider writer workspace must be separately admitted/limited. |
| Workers/queues | `compute_pool.rs`, resident serving/I/O owners, ingest Capillary/PulseWeave owners | Existing credits and bounded queues apply. Reuse common sizing for actual work-in-progress limits where concurrency exists; caller-driven relational work must not claim a compute pool it does not use. CPU includes caller/provider progress. |

## Vortex-first provider check

Decision: use existing native providers with ShardLoom admission. Pinned Vortex
0.90 scan preparation supports explicit row ranges, natural split boundaries,
ordered delivery and concurrency. Arrays, native allocation and source/sink APIs
remain the execution/storage boundary. `LiveMemoryPool`, existing leases and
the admitted native run store already supply ShardLoom resource ownership.
No replacement source abstraction, query engine or Arrow-first execution is
needed. Compatibility decoders and writers remain explicit translation owners.

Complete ordinary-file and collection preparation now shares the held source
owner used by demand readers. The Parquet preparation continuation reuses the
existing ordered row-group task planner and Vortex-backed `IngestRuntime`;
source, conversion and writer tasks retain one CPU grant. Tasks share the
immutable footer/schema admission and visit disjoint ordered group ranges.
The common sizing policy bounds the initial task window and actual decoder row
ceiling using current pool headroom, provider workspace, input chunks and queued
batch overlap. Actual reservations remain authoritative. Planning is not a
completed adaptive window, and these estimates are not allocator or RSS bounds.
Owned batches retain credits through handoff; existing single-file dictionary,
derived-column and physical-extent evidence remains part of preparation.
Public-route and broader regression acceptance of this continuation is pending.

The pinned Parquet 59.3.1, Arrow Avro 59.3.1 and ORC 0.9 reader builders accept a
batch size; their ordinary iterator APIs retain it for the constructed reader.
IPC framing is supplied by the file. Do not claim changing a post-read slice
changes those provider allocations. Provider changes, if required, need their
own source, ownership, provenance and regression proof.

Local provider continuation adds explicit next-batch controls to the already
vendored ORC and Parquet readers. ORC preserves a partially consumed selection
across batches, including changing limits and stripe boundaries; its former
partial-selection bug is retained as a failing regression observation. All 175
synchronous and 176 asynchronous provider tests, and both lint configurations,
pass after the correction. Parquet must change leaf preallocation hints as well
as the output-row limit, preserving selection/caching state and the original
configured ceiling. Three focused controls and strict native Vortex lint pass:
real primitive buffer shrink/regrowth with retained outputs, changing limits
through masks/selectors with nested nulls and prefix limits, and empty/invalid
limits. The shared demanded source now calls both provider setters before each
batch, including mapped Parquet and complete-ingestion readers. Three focused
source tests pass across selected Parquet, mapped Parquet and ORC: changing
headroom with retained native outputs, exact values/offsets and bounded regrowth,
EOF/zero-prefix completion without invented work, and minimum denial/cancellation
with refunds. Complete acceptance remains pending.
These controls cannot split a retained dictionary, encoded page, ORC stripe
stream, Avro block or IPC message. Those owners retain separate admission and
lifetime obligations; a row limit is not a provider-allocator or RSS bound.

The pinned Arrow Avro provider now has synchronous OCF continuation controls
and joins the shared source sizer. It admits and retains a complete block before
selecting output rows, exposes its remaining record count, and validates EOF
without inventing a minimum batch. Declared prefix limits cap actual decode.
Record builders start empty instead of recreating an unrelated 1024-row reserve
after each flush; the shared record decoder's single-object stream semantics
remain a separate regression obligation. The optional block reservation charges
encoded/decompressed/replacement overlap through the existing shared pool and
keeps block credit until the bytes are released. It covers the admitted plain,
Deflate and Snappy routes. Controlled Deflate now uses stack-owned miniz core
state and the credited output as its complete history, with explicit truncated
and trailing-input rejection. Independent decoded-payload credits cover value,
bitmap, offset, decimal/interval, dictionary and string-view storage. They admit
replacement overlap, transfer through Arrow buffer owners, and remain live
across slices and reader destruction. Native alignment recognizes those owned
buffers without charging their payload twice. Header offsets and raw metadata
now use the same factory before allocation, including replacement overlap.
Header clones share immutable storage and retain its credits after reader
destruction; discovery and query construction both attach the native owner.
OCF parsing now admits temporary capacity from source syntax, strings, depth
and pinned parser structure widths. A separate retained capacity covers codec,
projection, skipper, decoder and Arrow schema structures; named references
carry checked shapes without expanding their graph during preflight. General
schema resolution includes candidate pairs and nested-copy overlap. Controlled
expanded depth is limited to 128 before construction. These capacities are
conservative estimates of named structures, not minimum memory or RSS bounds.
Raw single-object schema stores/caches remain separate caller-owned boundaries.
The native file retains the schema's opaque provider credit. Its actual input
buffer has a separate shared-pool lease, and identity physical projections avoid
generating an unnecessary reader schema. Avro no longer uses the old fixed
discovery/row envelope. Sixteen native source/ownership controls pass, including
an operation starting with 384 KiB of additional headroom, independent
header/schema/block/payload lifetimes and complete refunds. The first integration
run's two failures are preserved: redundant projection work exceeded that
headroom, and a record-denial test needed to admit the newly covered schema
before imposing its unchanged one-byte record headroom.

Literal continuation controls also reproduced and corrected union offset state
leaking across batches, string-view null loss and nested empty-record flush
failure. A separate fragmented-header control reproduced a reset error after
flush; reuse now restores the magic length and preserves the allocation factory.
The complete pinned provider library suite now passes 427 tests with
none ignored or filtered, using original fixtures from the exact Arrow 59.3.1
release and its pinned Arrow-testing submodule. The complete optional
type-feature suite passes 448 tests, alongside strict provider/native lint and
sixteen native source/ownership controls. These are scoped provider and source
proofs; whole-engine acceptance remains open. The committed `formats` CI group
runs the complete ORC/Avro library suites and strict library/test lint with
reviewed locks; its eleven recipe controls preserve existing commands and
isolate Avro fixture and lint configuration.

Literal OCF regressions reproduced dropped zero-byte records, accepted partial
blocks at EOF and unchecked sync markers before the provider correction. The
reader now follows record counts, assembles split sync markers correctly,
rejects truncated/count-inconsistent frames and treats errors as terminal.
Strict native Vortex lint and eleven focused provider/source controls pass:
eight Avro framing/geometry/ownership checks and three shared source checks now
covering Avro alongside Parquet and ORC. The original failing reproducer remains
recorded. Broader source/provider verification is separate; these scoped checks
do not establish complete provider resource coverage or whole-engine acceptance.
The archive, original
and modified file hashes and preserved Apache license/NOTICE are recorded in
`vendor/arrow-avro/upstream-provenance.json`. The package version is unchanged.

ORC continuation now preserves the encoded `Bytes` owner, validates physical
compression framing, skips empty blocks without declaring logical EOF, and
treats a failed decoder as terminal. Decompressed payload capacity and allocation
replacement overlap use optional per-buffer credits from the existing shared
pool. Zlib, Snappy, Zstandard and LZ4 obey the declared expansion bound. The
controlled LZO route rejects before its unbounded allocating API; this is an
explicit provider limitation. Controlled Zstandard now delegates only its codec
work to the existing Vortex one-shot static workspace provider, through the same
pool allocator. One actual context remains with each reader/metadata operation;
it is reinitialized for each one-shot call and visible to the live row sizer.
The owner stores no raw pointer and never accesses old input/output after a call.
This is a codec boundary, not residual query execution. Its
safe frame entry point reuses the existing version/alignment/framing/legacy
checks and actual workspace owner; there is no new unsafe implementation or
dynamic-context retry. It accepts a caller-bounded destination, so ORC does not
need to invent a Vortex array or copy compressed bytes to use that provider.
Raw Deflate uses the already locked Apache-compatible `miniz_oxide` 0.8.9
bounded slice API with stack state; Snappy and LZ4 use caller-owned destinations.
File metadata uses the same controlled codec service, separately from its
parser and retained-schema admission. A multi-block source control reproduced
a later context allocation denial after output sizing with the original
temporary-context lifetime. The retained-context correction passes that control
and complete 40,003-value output under sustained pressure. Seventy selected
native controls pass; one pre-existing explicit compression cost screen remains
ignored. They cover same-pool denial, cancellation, malformed members, context
reuse with no unreserved capacity and stored-to-compressed preparation. This
does not yet close whole-engine acceptance.
The next-stripe preparation hook loads selected stream first blocks before the
shared reader selects row geometry. It preserves selections and logical read
position. Stripe footer parsing admits encoded/decoded/protobuf overlap,
validates stream extents, column encodings and timezone, and retains metadata
credit with cloned columns. Encrypted stripe state fails explicitly.
The current synchronous provider suite passes 197 tests, including new footer
ownership, denial and malformed-state controls. Earlier native controls pass
for all four ORC compression routes under changing pressure and with retained
outputs. The subsequent native integration passes nine combined adaptive/complete
reader controls and 91 source-demand/error-propagation controls. Decoded payloads
and string dictionaries now reserve before allocation;
Arrow slices retain the same credits. Checked offset totals precede nested child
decoding, and dictionary expansion admits its actual repeated payload. Timestamp
and decimal scratch and bounded byte/integer run buffers share that pool.
New controls cover retained primitive/boolean/string/list/map owners, dictionary
expansion denial, invalid lengths/keys and exact skips. Presence-error loss and
union over-skipping were reproduced and corrected. Reader schema/control and
stripe stream-map/decoder structures now have separate pre-construction shape
admission. Immutable root inventory and column clones share their owners.
Checked bounds use type/name counts and actual Rust structure sizes; payload,
run-buffer and codec owners retain their separate credits. These are conservative
source-derived bounds, not allocator/RSS or minimum-memory measurements.
The old fixed per-column workspace envelope is removed for ORC. Shared alignment
retains original payload credits and admits only its new buffers and metadata.
All 104 source controls pass, including five ORC routes starting with less free
headroom than the old fixed envelope required. The full asynchronous provider
suite passes 205 tests. These are scoped implementation results, not whole-engine
memory or timing acceptance.

The text reader now applies shared sizing before constructing each batch, using
its schema, live shared headroom and observed completed work. It may finish at a
record boundary before the selected capacity, keeping space for the next complete
record. The largest record from the held generation's complete discovery pass
guides that boundary; a large later record does not become a hard prerequisite
for an earlier prefix. Parsing, coercion and real reservations remain mandatory.
Only successful batches advance feedback, and returned buffers keep their credits.
Fourteen focused text tests and strict native Vortex lint pass, including changing
pressure with retained outputs, bounded regrowth, narrow/wide schemas, long UTF-8
values, late oversized records, minimum-workspace denial, empty inputs, decoder
failure, cancellation and refunds. Reported row geometry is selected capacity;
EOF/record/byte boundaries can yield fewer actual rows. Complete public-format and
whole-engine acceptance remain pending.

Shared schema alignment preserves the text reader's existing payload lease and
charges only newly retained buffers/metadata. Missing fields and Hive values
receive their own credit. The private text adapter includes downstream alignment
and native-intake estimates in its row target and respects a demanded prefix.
Forty-one focused adaptive controls and strict native Vortex lint pass, including
the existing provider tests, actual source integration and three alignment
ownership/denial controls. Complete source/public-format checks remain separate.
The subsequent integration controls pass: 91 shared source-demand tests, six
complete-ingestion tests, 15 text-reader tests and nine public-format tests.
These scoped checks do not complete the installed interface matrix or item 23.
Parquet alignment now retains the same selected-column uncompressed workspace
admission used by the specialized reader, instead of charging a fixed full row
batch for every column. ORC and Avro now use the scoped provider admission
described above.
Schema/value-span estimates do not bound retained dictionaries or page backing.
Unknown representations supply no observed per-row feedback. Scalar moments
now retain source-order state across physical input arrays, matching the grouped
path instead of introducing a reduction boundary at every decode batch.

The statistical semantic gate gives scalar aggregates the same persistent
source-order observation contract already used by grouped statistics. Physical
reader/scan batches must not introduce independent moment summaries and change
rounding when headroom changes. This is an explicit reduction implementation
change, not permission for arbitrary floating reassociation. It retains original
integer origins, scaling, null/complete-pair rules, nonfinite policy, cancellation
and overflow errors. Existing window-summary merges keep their own logical
membership contract. Acceptance requires identical scalar/grouped result bits
across complete ordered-input batch partitions, independent expected moments
within the existing operation-specific tolerance, wide-integer/special-value
controls, cleanup and the broader statistical/window/spill regressions. This
gate passes all 20 statistical controls, including windows, spill, nonfinite
values and failure cleanup. A new regression exercises seven statistical
measures over floating values and wide integers with nulls, paired-null
selection, empty batches, and five real input partitions. It checks independent
expected values and identical scalar/grouped result bits, source batch counters,
EOF, certification and complete refunds. The original per-array merge reproduced
one- and two-ULP differences and is preserved as a failed observation; those
differences did not violate the prior numerical tolerance. Whole-engine
acceptance remains pending; no timing result is implied.

The ORC continuation reuses an `Arc<FileMetadata>` within the held source
generation, with parsing workspace and retained metadata charged to that
source's existing pool. A borrowed admission callback must run before growing
decompressed footer buffers or decoding their protobuf structures. Its size
model covers metadata shape and overlap; it is not an allocator/RSS bound.
File/stripe counts require present fields, checked sums and consistent extents.
Absent file totals decline metadata-only count; malformed stripe extents or
inconsistent totals fail explicitly. Statistics retain presence independently
of their legacy display accessors. Only validated primitive integer, boolean,
exact string and null facts may exclude stripes before their streams are opened;
temporal, decimal, floating and inexact string bounds do not gain a proof path.
The existing shared predicate proof and consumer-demand union remain the
authority. ORC row-index selection, if admitted later, remains decode avoidance
after stripe loading and cannot be reported as early I/O pruning. Native source
identity, cancellation, residual evaluation, full ingestion and cleanup remain
unchanged contracts. Both provider readers seek into the validated stripe
inventory and stop at the selected range end. Focused validation now passes:
173 synchronous provider tests, 174 asynchronous provider tests, both provider
lint configurations, 88 shared source-demand tests, six complete-ingestion
controls and native workspace lint. The installed interface matrix, complete
resource acceptance and performance comparison remain pending.

`AdaptiveSizer` and `plan_dynamic_sizing_feedback` remain side-effect-free
planning surfaces. The common bounded adjustment arithmetic may be reused, but
their `executes_feedback=false` contract must not be changed to suggest runtime
application. Existing scoped PulseWeave application keeps its current evidence
and must identify any new shared runtime wiring separately.

## IPC ownership continuation

The pinned Arrow IPC provider exposes `FileDecoder` and `RecordBatchDecoder`
for caller-owned messages. The generic reader now uses their controlled owned
counterparts through the existing `OwnedLocalRead` and held source generation.
It retains one admitted schema and file block index rather than rebuilding the
footer for each reader. Streams read metadata and bodies separately without
re-reading a complete message to discover its body length.
Vortex remains the execution provider after validated compatibility intake.
Reuse the existing Vortex-owned static Zstandard workspace for this codec
boundary; no external query executor or Vortex query integration is involved.

Admit physical footer/schema/message bytes and their represented structures
before allocation. Retained dictionaries, decompressed buffers, alignment
copies and dictionary replacement overlap must use the same pool and preserve
owners through the complete native handoff. Preserve dictionary deltas and
projection, and reject invalid lengths, truncated framing and malformed data
deterministically. Controlled LZ4 must pre-admit the pinned frame decoder's
actual declared-block capacity and decode into a bounded destination, never
append arbitrary expansion before checking the declared output size. Source
framing and format validation remain provider/adapter work, not query execution.

IPC messages are indivisible decode units. A row target cannot claim to shrink
their compressed bodies, dictionary state or complete validation. Conversion
may use a separate internal window only where its existing observable batch
contract permits it; otherwise this physical minimum must be explicit and
tested. Do not record sliced rows as adaptive IPC decoding. A row-based fixed
allowance cannot establish physical message ownership. Array/schema structure
ownership, including bufferless/null outputs, is a separate handoff obligation;
buffer credits alone cannot prove it. This section is the implementation
contract, not completed IPC or whole-engine acceptance.

The first provider implementation passes all 139 standalone library tests with
both compression features, including 15 new controls. These cover independently
owned compressed/alignment buffers, unchanged values across clones/slices,
declared LZ4 block/history capacity, output-length lies, every truncation of a
checksummed frame, cancellation, denial/refunds, schema nesting and metadata
lifetime, and malformed field/message/block extents. `OwnedSchema` makes the
metadata lifetime explicit. The root graph adds a direct edge to its existing
`bytes` version; the new reviewed standalone lock preserves Arrow 59.3.1. The
provider CI command/environment controls pass 12 tests.

The follow-up provider implementation pre-admits array structures and temporary
construction/validation capacity, including nested logical-null masks. It also
admits dictionary replacements and delta concatenation, including repeated
list/union children, dictionary merge storage and shared byte-view payloads.
`OwnedFileDecoder` retains schema, dictionary and projection owners, and its
separate-message APIs verify metadata versions and exact body lengths. All 154
codec-enabled provider tests, 145 default tests and strict library/test lint
pass. The earlier 139-test observation remains the initial buffer/schema scope.

Ordinary Arrow `RecordBatch`/`ArrayRef` cannot carry an arbitrary structural
credit on bufferless arrays. The explicit owned-batch envelope now carries it
through internal readers, queues and validated native conversion, without a
custom unsafe Arrow array. Native file/stream discovery, retained block indexes,
message buffers, codecs and dictionary generations use the existing pool.
Their scoped controls pass and the fixed native IPC allowance is removed;
this does not close item 23 or establish a process-RSS guarantee.

The compatibility plan preserves `FlatLocalColumnarStreamSource.reader` and
ordinary `RecordBatchReader` callers. Add an owned complete-ingest constructor
to the existing private-reader `IngestColumnarStreamSource`; native preparation
uses that path. Its internal batch envelope carries structural/schema owners
through alignment, derived columns, source queues and conversion workers, even
when arrays have no buffers. Ordinary Arrow export remains explicit and does
not certify a detached caller's metadata lifetime. Reuse the same ordered source
task implementation for both item types; do not add a second scheduler. These
changes preserve validated-copy admission for arbitrary and non-Parquet inputs.
The private complete Parquet producer retains standard allocations so the
existing per-buffer uniqueness, alignment and capacity proof can admit transfer;
publicly replaceable reader metadata cannot grant that policy. Ordinary Arrow
export still retains buffer credits through the last payload alias. The envelope proves
lifetime only; provider admission and precise capacity coverage are separate
obligations, and no whole-engine ownership claim follows from this wiring alone.

The owned handoff now passes ten focused native controls: bufferless/null and
zero-column row counts, retained outputs after queued-reader teardown, derived
columns on either side of source prefetch, cancellation and primary errors,
alignment credits, all four binary complete-source owners, and a real
validated-copy conversion that denies overlapping input/output capacity. The
existing public Arrow source fields remain compatible. The selected broader
source/ingestion regression suite passes all 280 tests, with none ignored, and
strict all-target lint passes for the native Vortex and CLI crates. Complete
workspace/public-route validation remained pending at that handoff checkpoint.
Collection preparation uses the new owned constructor. The subsequent routing
change sends ordinary files through it as well in native builds; reduced-feature
preparation remains explicitly compiled through the compatibility path. Single-file
Parquet preparation reuses the existing dictionary/view schema and derived-column
recipe policy from retained footer metadata. Complete-source row hints come only
from validated Parquet/ORC counts or completed text discovery; IPC/Avro counts
remain unknown until read. Public-route acceptance of this continuation is pending.

Seven further native IPC controls pass across file/stream inputs, plain/LZ4/Zstd
payloads, projection and bufferless output, dictionary replacement/deltas,
retained output after decoder/prefetch teardown, denied capacity, cancellation,
source mutation and truncated framing. Prefix evidence reports complete physical
decode with zero adaptive-decoder completions; zero prefixes read no payload.
A 256-KiB shared grant accepts a small physical batch and rejects a 1-MiB
indivisible body before allocating it, with terminal errors and complete refunds.
The selected source/ingestion regression suite now passes all 287 tests and
native Vortex/CLI all-target strict lint passes. Ordinary preparation routing,
full workspace/public/package proof and complete resource acceptance remain open.

## Evidence and acceptance

Execution evidence distinguishes ceiling, observed accounted headroom, selected
units/lanes, actually completed work, shrink/grow decisions and physical limits.
Planning and inspection execute no I/O, callbacks or allocation probes. Reports
state reservation/estimate scope and never infer RSS or throughput guarantees.
No fallback or external executor is admitted.

## Capability continuity audit

The November 1 maintainer follow-up adds an end-to-end audit alongside items
18 and 23. A boundary must preserve an admitted fact or owned capability,
transform it with equivalent proof, or report why it no longer applies. Correct
result values alone do not prove that a source optimization, replay policy or
resource owner survived an adapter or public interface.

The end-to-end audit remains open; implementation progress is recorded below.
Shared-source improvements extend item 18;
they are distinct from item 23's adaptive geometry. This ordering does not
reopen rejected experiments or authorize unsupported execution families.

1. **Discovery consistency.** Replace the default registry's stale planned
   labels for implemented runtime surfaces with a shared, scoped source of
   capability facts. Human and machine output must agree. Build features,
   admitted runtime behavior and certification/claim status are different
   facts: a reduced-feature build must not advertise unavailable execution,
   and native execution must not imply world-class or universal certification.
   Inspection stays side-effect-free; test default and enabled builds.
   Implementation decision: the core catalog takes explicit runtime provider
   availability; core-only discovery has no attached providers. The CLI binds
   its existing feature and Unix resource-admission gates once and reuses that
   catalog in default, SQL, DataFrame, Python, adapter, compatibility and
   certification discovery. Local support is partial within existing query,
   type and resource admission; absent providers are disabled. Existing detailed
   certification fields keep their independent meaning. Additive
   `runtime_capability_*` fields and human summaries expose the same facts;
   declaration builders alone do not imply dataset execution availability.
2. **Parquet collection parity.** Preserve exact unfiltered cardinality through
   union-by-name and Hive collections using the same checked per-file count
   owner. Then carry row-group exclusion through each file's physical field
   mapping, typed-null absent fields and admitted partition constants. Preserve
   partition collisions, source order, generation/membership checks and the
   union of demand from all consumers. An individual branch cannot prune rows
   needed elsewhere. Require observed payload exclusion, not only equal values.
   Implementation decision: expose the retained Parquet file metadata through
   the existing source owner regardless of schema alignment. Use the same
   checked cardinality validator and ordered source-group selection. Resolve a
   predicate by physical field name in each file, or by an admitted nullable
   missing field or Hive constant; unsupported/missing statistics retain the
   group. Exclude a group only when every consumer's demand excludes it, before
   opening its payload reader. Keep physical positions across skipped groups,
   complete-source ingestion, generation checks and per-file schema alignment.
   The generic decoder still reports provider projection rather than the
   specialized selected-chunk ownership path. Ranking remains explicitly
   restricted to its existing compatible physical-schema contract.
   Acceptance includes exact full results and counters for union/Hive counts,
   reordered columns, null-only fields, matching/nonmatching/null partitions,
   mixed consumer demands, prefix controls, corrupt undemanded pages, source
   mutation and cancellation. Timing follows complete integration; no speedup
   is assumed from avoided reads alone.
3. **ORC source facts.** Retain and account for admitted file/stripe metadata
   under the held source generation. Treat metadata reuse, complete validated
   row/stripe totals, early stripe exclusion and decode-only selection as
   separate capabilities. Audit statistics exactness, missing/null values,
   string bounds and temporal semantics before producing predicate proofs.
   The pinned provider constructs a stripe and loads selected streams before
   its row-index predicate: that predicate alone does not prove avoided I/O.
   Count metadata requests, payload bytes, loaded stripes and decoded rows
   separately. This is query-lifetime reuse, not the rejected warm-source cache.
   Audit decision: the pinned converter currently collapses absent row/value
   counts, null flags and several extrema into default values. Preserve presence
   before admitting new metadata proofs; absence cannot establish zero. Exact
   counts need explicit file/stripe counts and checked agreement. String bounds
   must retain their exactness flags; legacy/UTC timestamp bounds and discarded
   submillisecond facts do not establish exact native temporal extrema. The
   [ORC v1 specification](https://orc.apache.org/specification/ORCv1/)
   distinguishes optional footer/stripe counts, stripe statistics and timestamp
   conventions. Retain provider metadata under the existing source generation
   and memory owner, and reuse it without rebuilding the footer for each reader.
   This source continuation is implemented with focused provider/source tests,
   including union-by-name field order and missing-field controls. Its complete
   interface/resource/timing acceptance remains open.
4. **Positive proofs, separately contracted.** Before adding a new runtime
   consumer, follow the [separate semantic and performance protocol](native-positive-predicate-proofs-2026-11-01.md) for all-match,
   no-match and unresolved regions. Exact coverage, types, null semantics and
   held generations must justify omitting evaluation or predicate-only reads.
   Fallible/effectful expressions, residuals and required validation remain
   obligations. Filtered counts can consume proven cardinality only under that
   contract. Preserve the dropped B3 composed encoded-reduction result; this
   audit neither revives B3 unchanged nor assumes an existing filtered-count
   implementation is waiting to be connected.
5. **Replay and output reach.** Carry explicit native spill admission through
   primitive fanout using the existing common replay owner and one resource
   grant. Require one producer execution, complete delivery, shared overlap
   accounting, source validation, cancellation and staging/publication safety.
   A result larger than resident retention must succeed with admitted spill
   and fail cleanly without it. Include output-window reach and consumer
   acknowledgment stalls in the source/operator/sink audit. Its accepted scope
   is resident-memory Int64 scan/projection through native Python iteration,
   two outstanding batches and 4 MiB referenced bytes; extending sources,
   types, composition or spill requires proof. The previous complete-workflow
   benefit did not establish an engine-time benefit.
6. **Decision evidence.** Extend the existing certificate owner with typed,
   bounded capability decisions at planning/operator boundaries: considered,
   admitted, declined and actually executed, with semantic/ownership/resource
   reasons and observed work where available. Cover metadata counts, pruning,
   ordering, correlated membership, replay/spill and representation retention.
   A test-only counter or a boolean decline is insufficient release evidence.
   Do not add per-row tracing or invent avoided-work quantities.

For each implemented extension, vary single-file/collection/union/Hive
representation, aliases/plain projections, all five interfaces, single/fanout
outputs and ample/admitted-pressure resources where applicable. Assert full
results, actual capability activation, justified declines, observed work
removal and complete-operation timing under unchanged limits. A physical or
semantic distinction may justify different paths and must be explicit.

Existing same-session source/result ownership, retained subplans, streaming
source replay and persisted native statistics are already implemented; they
are regression controls, not missing features. Persisted sortedness needs a
new property contract. Planning/reporting modules for general runtime filters,
skew adaptation and certified caching do not establish dormant executable
subsystems. Do not expand this audit on that assumption.

Acceptance requires every map row and the item 18 format/interface/composition
matrix to have executable proof or a necessary tested physical/semantic limit.
Include multiple ceilings and retained results, changing headroom, narrow/wide/
rich schemas, long records, many fragments/files/groups, slow consumers,
pressured writers, skew, cancellation, late errors, and cleanup. Test actual
decision changes and complete values, not just flags. Add irreducible-workspace
failures and preserve existing fast-path controls.

Run focused owners, pinned-provider checks when touched, default/native workspace
gates and installed/public interfaces before complete resource/correctness
acceptance. Then freeze matched complete-workflow timing checks against the
current accepted runtime. Preserve the Full43 hold and isolated single-pass
procedure; this contract starts no benchmark run. Documentation, website,
portable evidence, expert review, hosted checks and merge remain required.
