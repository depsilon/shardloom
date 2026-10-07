# RFC 0044: Resident Runtime and Resource Ownership

## Status

Accepted for implementation by the maintainer's 2026-09-05 request to implement
`performance-overhaul-2026-09-04.md`. Completion is tracked in the phased plan.

## Decision

Implement PERF-01 through PERF-12 through existing native execution families.
PERF-13 remains conditional on instruction-cost and compilation break-even evidence.
The historical 145.130-second full-query result and 271-second ingestion reference
are not a newly measured same-commit baseline. Targets in the supplied plan remain
unvalidated objectives until measured through complete public operations.

The engine owns persistent execution resources. Sessions own source generations,
prepared plans, and cache lifetime. Prepared calls execute kernels, not cached
answers. CLI and Python adapters converge on the same typed native execution.
No external query engine executes residual work. Vortex is native input, execution
representation, and durable output. Canonical Vortex kernels are permitted with
accurate decode and materialization accounting.

## Live Resources

A shared byte budget issues owned reservations before allocation. Reservations
are released on drop, including error and cancellation paths. Growth is checked
against remaining shared capacity before publishing a larger retained state.
Arithmetic overflow and over-budget requests fail explicitly. A reservation is
not a general-purpose allocator: every admitted producer and stateful operator
must charge owned buffers and capacity before making an enforcement claim.

Queue slots, queued bytes, active input, decoded buffers, operator state, merge
state, source caches, and output ownership are separate measured consumers of the
same budget. Moving a buffer transfers its reservation instead of charging a
second copy. Sharing a buffer retains its owner until the final reference drops.
Do not release memory credits merely because a task has returned a retained result.

Persistent workers pull bounded work dynamically. Logical partitions and reduction
order preserve floating-point semantics independently of physical worker assignment.
Task failures cancel further work and drain already submitted work before returning.
Caller-owned runtime shutdown joins workers. The Vortex I/O progress service and
compression workers participate in the engine's CPU budget.

## Source and Result Ownership

Prepared sources retain an open source handle and source-generation identity.
Replacement, truncation, mutation, and recreation invalidate prepared operations.
Path or size alone does not certify immutable data. Source identity is checked at
operation boundaries, and concurrent mutation fails instead of returning mixed data.
Owned Vortex arrays and selection views remain executable payloads; descriptive
reports cannot stand in for them. Result ownership outlives sessions when requested.

## Temporary Spill Decision

This RFC authorizes bounded, query-local, native temporary partition runs under an
explicit caller-owned workspace for PERF-06. It extends the prior staging-only
temporary-file restriction only for admitted query execution. Ingestion continues
to publish exactly one complete `.vortex` artifact with no answer sidecars or
persisted aggregate answers. Temporary runs cannot be reused as result caches.
Spill uses exclusive file creation, a byte quota, exact owned-path cleanup, and
validation of run schema and lengths. Errors/cancellation must release reservations
and remove owned runs. Crash recovery must identify owned runs without deleting
unknown files. Spill must fail deterministically when no workspace is admitted.
Existing synthetic-spill gates retain their meaning and do not prove query spill.

The numeric sort run geometry may scale with its existing merge reservation:
256..=1024 rows per native Flat leaf, with a bounded 2/4/8-way merge. Retain the
full per-leaf metadata charge and account for simultaneous input/output footers.
Poll one exact native run row-range task at a time so upstream machine-core
prefetch does not multiply retained run payloads. Validate the 131,072-row,
large-offset public query at 4 MiB; 1 MiB is an admission minimum, not a promise
that arbitrarily many run footers fit. This remains a scoped operator reservation,
not an RSS or whole-query bound.

## Memory-Visible Publication

PERF-11 may publish validated, immutable in-memory Vortex arrays without creating a
physical file. `visible_in_memory` completes only after validation and requested
computation/output have completed. It does not imply `durable` or `optimized`.
Durable ingestion retains the required write, flush, validation, atomic replacement,
and source-preflight boundaries. Batch size, output size, memory, and queue delay
are bounded explicitly. No enqueue acknowledgment is reported as a completed query.

## Vortex-First Provider Check

- Checked pinned Vortex 0.85 array ownership, VortexFile, CurrentThreadRuntime,
  CurrentThreadWorkerPool, Executor, and Handle source.
- Use upstream arrays, file readers/writers, and runtime handles as native providers.
- ShardLoom supplies admission, lifetime-bound credits, exact aggregate semantics,
  cancellation, and source-generation validation not supplied by those APIs.
- Avoid cloning upstream CurrentThreadWorkerPool owners: its Drop stops shared workers.
- Retain workspace `unsafe_code = "forbid"` and prefer audited safe provider APIs.
  The isolated pinned Zstd workspace exception below applies only inside the
  existing excluded `vendor/vortex-zstd` dependency patch.
- Use Vortex 0.85 `HostAllocator`/`HostBufferMut` for native buffer admission.
  `bytes` 1.11.1 (MIT, already locked transitively through Vortex) is made an
  optional direct dependency to use its safe `Bytes::from_owner` lifetime hook.
  This buffer ownership hook introduces no FFI or custom allocation implementation. The pinned provider
  requests logical length plus preferred-alignment capacity; reservations cover
  those bytes and survive immutable buffer clones/slices. Allocator metadata and
  upstream allocations bypassing this hook are not covered by that counter.
- No Vortex query-engine integrations or new external execution dependencies.

## Pinned Zstd Decoder Workspace Decision

`NATIVE-CODEC-WORKSPACES` extends the finite provider allocation scope under
PERF-03/06. The existing Vortex `Zstd` decoder may initialize its one-shot C
decoder context and optional prepared dictionary in buffers allocated through
the session `HostAllocator`. These are the actual codec workspaces, not dummy
reservations beside separately allocated C state. Preserve the Vortex encoding,
VTable, native frames, selection, validity and serialization identities.

The checked provider is `vortex-zstd` 0.85.0, `zstd` 0.13.3, `zstd-safe` 7.2.4
and locked `zstd-sys` 2.0.16 with Zstandard 1.5.7. The safe Rust wrappers do not
expose static decoder/dictionary initialization. The vendored Vortex patch may
therefore use a private borrowed wrapper over `ZSTD_estimateDCtxSize`,
`ZSTD_initStaticDCtx`, `ZSTD_estimateDDictSize`, `ZSTD_initStaticDDict` and
`ZSTD_decompress_usingDDict`, with these mandatory safety constraints:

- Pin the existing `zstd-sys` version and enable only its experimental bindings;
  do not enable experimental `zstd`/`zstd-safe` behavior. Verify linked Zstandard
  version 1.5.7 before using the audited static APIs. A different linked version
  fails explicitly until its contract is reviewed.
- Allocate, initialize and check the real workspace lengths and eight-byte
  pointer alignment before passing them to C. Reservations precede allocation
  through the existing allocator; allocation errors retain their typed source.
- Tie decoder/dictionary pointers to exclusive workspace borrows and the
  immutable source-dictionary borrow. Keep every backing owner alive and fixed
  until the final C call. Do not expose raw pointers or implement `Send`/`Sync`.
- Use one-shot decoding only, with exact input/output slice bounds and checked
  returned lengths. Prepare dictionaries by reference. Do not create an internal
  dictionary, use streaming decode, enable multiple dictionaries, install custom
  allocation callbacks or invoke C free functions on static state.
- Validate selected compressed frame boundaries without allocating another
  payload. Modern frames and skippable members retain their existing semantics;
  legacy Zstandard v0.1–v0.7 frames fail explicitly, including trailing legacy
  members. No legacy decoder or unaccounted dynamic-context retry is allowed.
- Empty selections do not construct codec workspaces. Temporary workspaces drop
  before returning decoded storage, including on malformed input, dictionary
  errors or denied allocation. Escaping payload credits retain their existing
  owner lifetimes.

This is a narrow unsafe-provider exception with local safety comments and a
[source-linked contract](../architecture/native-zstd-workspaces-2026-10-07.md).
It does not permit unsafe code in workspace crates, a custom allocator, streaming
codec state, compression/dictionary-training scratch, the inactive experimental
`ZstdBuffers` encoding, other codecs, general spill/recovery or a process-RSS
guarantee. Acceptance requires negative workspace/legacy/dictionary tests,
complete value and ownership checks, a frozen native cost screen and the existing
whole-engine regression gates. No speed or support claim follows from the design.

## Verification

PERF-04/09 numeric consumer follow-up admits a per-source-array native Primitive
execution after existing direct and dictionary accessors miss. The resulting
typed slice feeds the existing exact aggregate kernels instead of a row-wise
`StatValue` vector. Dtype, row order and nullable validity remain unchanged;
integer keys never pass through floating point. Actual native decode work and
typed value copies are reported separately from encoded execution. This bounds
the adapter's temporary representation by the source array, not process RSS or
arbitrary provider allocations. Public zero-decode aggregate requests reject
before opening the source. No alternate engine or failure-swallowing retry is
introduced. Retention requires exact encoded fixtures and paired measurement.

Test reservation overflow, contention, growth, drop, cancellation, pool reuse,
bounded queues, deterministic reduction, source replacement and mutation, result
lifetime, exact low-memory aggregation, spill cleanup, and writer atomicity.
Use renamed schemas, adversarial distributions, nulls, and non-ClickBench cases.
Record latency percentiles, actual active worker time, live/peak bytes, and completed
output. Run a clean same-commit full UAT once storage permits; retain historical
evidence separately. Do not mark unfinished packets complete from policy reports.

## Immutable memory-backed file generation prototype

The maintainer's additional 2026-09-05 planning packet authorizes an executable
prototype under PERF-07/PERF-11. This is a bounded immutable generation, not a new
live-update protocol or a serialized layout template. Existing tiny native-array
operations remain direct; callers explicitly choose the file-generation boundary.

Vortex-first provider check: wrap the pinned Vortex 0.85 `VortexFile`, `Footer`,
`SegmentSink`/`SegmentSource`, native Flat layout writer, cached reader tree, and
`FooterSerializer`. Typed intake uses the existing `ResidentMemorySource` and
reservation-owning allocator. The Flat serializer runs once; each resulting
segment is assembled into an admitted aligned owned buffer. Actual segment byte
copies are reported, rather than described as zero-copy. A real bound native
filter/project/ordered-limit query reads the immutable segments through
`VortexFile::scan`. No Arrow or external query engine evaluates the query.

Durable publication writes those same represented segment bytes, their declared
alignment padding, and the upstream footer serialization into one exclusively
created staging file. It performs flush, independent checksum readback, native
dtype/row-count reopen validation, then owned atomic publication. Publication does
not execute a second array serializer or rebuild a dictionary. Existing readers
retain the memory generation; publishing a durable backing does not mutate their
segment source or invalidate a cached reader tree. The generation remains
`visible_in_memory` until publication completes; file and directory synchronization
define the reported durable boundary.
This bounded publication requires an existing real parent directory and rejects
missing directory ancestry before creating staging output. It does not claim
durability for newly created ancestor directory entries.
The held parent directory's device/inode identity must match the admitted path
after opening, before commit, and after synchronization. Post-publication drift
returns an explicit published-but-durability-unconfirmed error.
The final durability boundary also checks that the published pathname and held
file still match the verified size/mtime and post-commit inode/ctime generation.
Replacement, unlink or in-place mutation returns a published-but-durability-unconfirmed
error and preserves the current destination. This check does not prevent another
writer from changing the pathname after publication returns.

Admission bounds segment count, serialized segment bytes, retained total bytes,
and metadata capacity before publishing the generation. Native allocations retain
their credits through the last buffer reference. Report actual intake copies,
segment assembly copies, serializer calls, segment requests/bytes, durable bytes,
readback bytes, file opens, and publication serializer calls separately. Codec
internals, provider allocations bypassing the allocator, parser storage, OS page
cache and total RSS remain excluded unless independently measured. File statistics
and user metadata not produced by this prototype are explicitly absent.

Acceptance requires exact independent values including empty input, nullable
Unicode, integer extremes, booleans and finite floats before and after durable
publication; lifetime survival after dropping the input/session; repeated queries
without encoding or source reopening; byte-bound rejection; cleanup on failed
write/validation; preservation of foreign destinations; and immutable generation
isolation. Benchmarks compare actual build/query/publish/reopen work before any
latency claim. Generic streaming ingestion, mutable generations, background
compaction and broad production performance claims remain outside this prototype.
