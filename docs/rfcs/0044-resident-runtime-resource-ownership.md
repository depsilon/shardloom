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

### Pinned Zstd compression and training admission

The November 1 item-23 implementation reuses the same static one-shot decoder
for controlled ORC translation, including compressed metadata. A safe slice
entry point supplies a caller-bounded output and the existing session allocator.
ORC retains one actual context buffer per reader/metadata operation so later
blocks cannot conceal that indivisible workspace from batch sizing. The safe
`DecoderWorkspace` owns only a writable buffer, never a raw pointer; each
one-shot call zeroes and initializes fresh static state under an exclusive
borrow. A mutex serializes this operation-local owner. Native Vortex frame
arrays retain their existing temporary-workspace lifetime. Controlled ORC
stream preparation also admits the actual bounded output buffer and context
when a stored first block has a later compressed block. A header-only inspection
does not consume data or advance validation errors; all-stored streams need no
codec context. The declared output capacity is conservative, not a proof of the
smallest possible workspace. Payload credits do not retain the context owner.
This does not widen the unsafe boundary, enable streaming/dynamic dictionaries,
or turn ORC into an execution provider. Native query evaluation and certificates
retain their existing owner. Required proof covers shared-budget denial,
complete members, legacy/truncation rejection, retained outputs and cleanup.

`NATIVE-COMPRESSION-RESOURCES` extends PERF-03/06 allocation coverage to the
existing Vortex `ZstdData` compression entry points. The
[compression contract](../architecture/native-compression-resources-2026-10-15.md)
owns the finite scope and acceptance. Reuse the pinned provider, compression
level, frame boundaries, default dictionary trainer and by-copy dictionary
loading. Preserve native encoding and decoded values.

Allocate the length-prefixed input, offsets, sample lengths, compression output
and dictionary output through the existing session `HostAllocator`. Admit
foreign compression/training heap storage before constructing its C state,
using a separate lifetime-bound reservation on that same allocator. This is
a reservation for a source-audited upper bound, not an extra allocated buffer.
The default allocator may explicitly remain unbounded; a custom allocator
without the reservation capability must reject it. ShardLoom's allocator uses
the existing shared `LiveMemoryPool` and preserves typed denial errors.

Keep the safe `zstd`/`zstd-safe` owners and synchronous one-shot calls. Only the
pure size/parameter estimators, default-level query and error predicate extend the vendored
unsafe exception; no raw compressor pointer, allocation callback, C-owned
pointer lifetime, streaming codec or new workspace-crate unsafe code is needed.
Check linked Zstandard 1.5.7 and default level 3 before applying its bounds. A reservation survives
all context/dictionary allocations and frees, including an error return.
Training uses the audited sequential default FASTCOVER configuration. Changed
codec versions or parameters require a new bound before admission.

Structural frame/metadata vectors and their infallible clones, Rust allocator
bookkeeping, C stacks, OS page cache and total process RSS remain outside this
finite allocation claim. The existing decoder workspace decision is unchanged.

### Pinned Zstd structural metadata admission

`NATIVE-ZSTD-METADATA-RESOURCES` extends the finite provider allocation claim
to Zstd frame-handle and protobuf-description vectors. The
[metadata ownership contract](../architecture/native-zstd-metadata-resources-2026-10-16.md)
defines requested-capacity admission before allocation, immutable shared
ownership for clones/slices/nullability casts, and fresh receiving ownership
for encoded transfer. Remove superseded sparse-selection and transfer charges
instead of charging the same vectors twice. Preserve raw caller-owned APIs as
an explicit boundary, native encoding/serialization, typed denial and existing
no-fallback certificates. Independent IPC encoding admission is unchanged.
Fixed Rust/Arc headers, allocator bookkeeping, other codecs and total process
RSS remain outside this extension. The contract's complete correctness and cost
gates must pass before retention; this design alone is not support evidence.
The [October 16 local acceptance](../benchmarks/native-zstd-metadata-resources-2026-10-16.md)
passes those gates with exact serialized identity and independently inspected
portable evidence. PR #1562 completes hosted integration after all 44 checks,
three installed-wheel lanes and preview/production verification pass; the
report links the separate hosted evidence.

### Pinned variable-binary buffer table admission

`NATIVE-VARBIN-METADATA-RESOURCES` extends the finite allocation contract to
the named text/binary buffer tables in its
[ownership design](../architecture/native-varbin-metadata-resources-2026-10-16.md).
Keep table reservations with shared array data across transformations. Admit
temporary construction/growth capacity and final storage before allocation;
use fresh receiving ownership and remove superseded outer table charges.
Reuse pinned VarBinView construction/builder and FSST/RunEnd/Zstd behavior.
Preserve raw caller-owned APIs, native bytes/serialization and typed errors.
Small version-preserving FSST/RunEnd patches require complete provenance and
license records. No new FFI, pointer operations or workspace unsafe code is
authorized. The design's complete ownership, cost and workflow gates precede
retention. Other provider/builder allocations and process RSS remain outside
this finite extension.

Safe host constructors and arbitrary view replacement must validate every
host-readable view, including null slots, before exposing native value access.
Within the excluded pinned providers, already validated construction and
semantics-preserving view selection may reuse the existing unchecked constructor
contract through explicitly unsafe helpers. Those helpers must retain structural,
dtype, validity and ownership checks; each caller must establish the original
view/value invariants in its safety comment. This permits no new raw-pointer
operations, FFI, allocation implementation or workspace-crate unsafe code.
Safe replacement may reuse the original validation when the entire host view
buffer is byte-identical and the payload table is unchanged. An admitted table
replacement may also reuse it when the complete host view buffer and every
ordered host payload buffer are byte-identical to the original, with the same
buffer count, dtype and validity. This includes attaching new reservation owners
without changing bytes. Table admission and structural checks remain mandatory;
the new table uses the explicitly selected allocator. Any changed or newly
host-readable content must pass ordinary validation. Device contents remain
opaque. The frozen complete-operation cost gates remain required.
The encoding data retains its construction value type. Public array-parts
reconstruction that pairs binary data with a UTF-8 dtype must validate the newly
required UTF-8 invariant, including null slots, before a source array can certify
later view or buffer reuse. This marker carries no buffer ownership and changes
neither serialized values nor metadata accounting.

## Verification

### Native schema final-owner cleanup amendment, November 17, 2026

Item 19's resource-admitted deep traversal needs stack-safe cleanup of the
existing Vortex 0.93 `DType`, including independently retained children,
partially built values and concurrent last owners. A root-only keepalive cannot
establish that contract. Use an intrinsic dtype destructor within the existing
excluded `vendor/vortex-array` patch. No public depth boundary changes until
serialization, values, physical verification and complete native lifecycles also
pass. Array ownership is a separate remaining obligation.

Keep the existing public dtype variants and serialized identity. Extract each
node's owning field into a private work item, replacing the original node with
`DType::Null`. Consume shared allocations with `Arc::into_inner`; a failed
`try_unwrap(...).ok()` is insufficient because simultaneous last-owner release
can drop a nested value outside the work loop. Traverse siblings through owned
iterators and preserve declaration order. Store struct/union field vectors
directly inside their already shared container, so last-owner extraction does
not require unwrapping an unsized `Arc<[FieldDType]>`. Lazy caches are released
without parsing absent values. Keep each provider metadata reservation alive
until its pending fields are released. Extension storage can be extracted by
safe `Arc<Self>` dispatch on the existing sealed, concrete extension owner.

This amendment permits one narrowly scoped unsafe ownership extraction helper
inside the excluded dtype provider. Its complete safety contract is:

- Accept only an exclusive reference to an initialized, unpinned `DType`.
- Replace that value with `Null` using safe `mem::replace` before any extraction.
  Put the removed enum in a local `ManuallyDrop`; never publish, store, clone,
  format, inspect again, or manually destroy that wrapper after extraction.
- Match every enum variant exhaustively. Each compound variant has exactly one
  owning field; transfer that field once with `ptr::read` from its valid,
  naturally aligned reference. The remaining fields are `Copy`. Every leaf
  variant contains only `Copy` fields. Adding a variant requires a fresh review
  of this exhaustive match and its ownership tests.
- The extraction block performs no allocation, callback, formatting, fallible
  operation or other potentially panicking operation between removing the enum
  and returning its fully owned work item. No pointer escapes the helper.
- The resulting work item has ordinary Rust ownership and is released exactly
  once. The original location remains `Null`, so its automatic field destruction
  cannot release a transferred child again. All subsequent traversal and Arc
  extraction use safe Rust.

The helper avoids placeholder allocations, a global or thread-local drop queue,
type-layout reinterpretation, raw Arc reconstruction and a parallel type model.
Workspace crates retain `unsafe_code = "forbid"`. This exception does not permit
unchecked file reads, a custom allocator, FFI, arbitrary extension metadata
destructors, or changes to public format/error admission. It follows the standard
library contracts for [ownership reads](https://doc.rust-lang.org/std/ptr/fn.read.html),
[local manual-drop storage](https://doc.rust-lang.org/std/mem/struct.ManuallyDrop.html)
and [concurrent Arc extraction](https://doc.rust-lang.org/std/sync/struct.Arc.html#method.into_inner).
The implementation uses APIs available before the supported Rust 1.96 minimum.

Cleanup may use a heap continuation vector. It must request no new execution
reservation during destruction. Before deeper values become public, native
schema admission must prepay its growth/overlap allowance and retain that credit
through final cleanup, including independently retained children. Raw provider
objects without a ShardLoom owner remain outside the resource-accounting claim.
This design is not a process-RSS or system-allocation-failure guarantee.

Acceptance requires small-stack subprocess tests for deep list, fixed-list,
struct, map, union and extension chains, detached children, weak references,
simultaneous final owners, expanded shared subtrees, partial construction and
unwinding. Count final drops and validate that lazy caches are not parsed during
release and provider credits survive their descendants. Run focused provider
and adapter tests, strict lint, workspace/feature/MSRV gates, and an interpreter
or sanitizer check where supported. Record unavailable tooling explicitly.
Owned pattern matches and promoted dtype references must be migrated without
changing values or public method results. No support or speed claim follows from
the ownership prototype alone.

### Native array final-owner cleanup amendment, November 17, 2026

The same deep-traversal obligation includes every independently retained native
array. The Vortex-first decision is `use_vortex_native_provider`: extend the
pinned provider's existing `ArrayRef`, typed `ArrayInner<ArrayData<V>>` and child
slots, without a parallel array representation or execution engine. This covers
declared slots; nested scalar values, statistics and serialization trees retain
their own lifecycle obligations. Existing public depth limits remain in place.

Keep the 16-byte erased Arc handle and its direct metadata reads. Its private
field becomes `ManuallyDrop<Arc<ArrayInner<dyn DynArrayData>>>`. Two private paths
may transfer that Arc: destruction and a consuming conversion. Each takes the
initialized field exactly once. The consuming conversion first puts the entire
handle into a local `ManuallyDrop`. After taking a field, neither wrapper may be
observed, cloned, formatted, returned or manually destroyed. Public safe APIs
only receive live handles; automatic field destruction remains inert on unwind.
This follows the standard library's
[manual-drop layout and take contract](https://doc.rust-lang.org/std/mem/struct.ManuallyDrop.html#method.take).

The sealed dynamic data trait selects the final-owner extractor monomorphized
for its actual `ArrayData<V>`. Invoke that extractor on the **same** Arc whose
data selected it, with no intervening mutation or caller-supplied type, encoding
ID or registry lookup. That relation is the complete safety precondition of the
private function pointer. Its implementation may call the existing unchecked
typed Arc downcast because the concrete type is exactly `ArrayData<V>`. There is
no new raw Arc reconstruction, unsized allocation manipulation or allocator.
All new unsafe operations stay inside this excluded provider: the two single-use
field takes, this same-allocation extractor call, and its existing typed downcast.
Workspace crates retain `unsafe_code = "forbid"`.

Use `Arc::into_inner` for final ownership so simultaneous last owners cannot
bypass the iterative path through a failed `try_unwrap(...).ok()`. At final
extraction, create a frame holding the slot iterator and retained lifetime owner
before dropping metadata and encoding data. Move all remaining owned fields into
one temporary tuple for destruction: if a field's destructor panics, its tuple
siblings finish before the frame unwinds. Both data and slot descendants must
be released before the lifetime owner; no data-versus-slot order is promised.
Consume present children in slot order with owned continuation frames. Use a
tail path when neither pending children nor a retained owner needs a frame.
On unwind, drain frames from innermost to outermost so ancestor credits outlive
their pending descendants. Cleanup performs no execution or canonicalization.

No new execution grant is requested during destruction. Before deeper shapes
are publicly admitted, traversal growth and overlap must be prepaid and retained
through final cleanup, including detached children. Raw provider objects without
ShardLoom ownership do not establish whole-process accounting. Arbitrary user
destructors, cyclic external ownership, and nested values hidden inside opaque
custom encoding data are outside the slot traversal guarantee. A second panic
while unwinding retains Rust's ordinary abort behavior.

Acceptance requires deep canonical struct, list, fixed-list and extension arrays
plus opaque slot chains on small stacks in subprocesses with explicit completion
markers. Cover typed/erased/parts conversions, raw typed ownership, weak references,
simultaneous final owners, detached children, shared DAGs, failed construction,
unwinding, exact payload/owner drop counts and descendant-before-owner ordering.
Preserve handle-layout checks and run provider/native/default/feature/MSRV gates.
Record unavailable interpreter or sanitizer tooling without implying such proof.
No deeper support or performance claim follows from cleanup tests alone.

### Native scalar-value final-owner cleanup amendment, November 17, 2026

Constant arrays, statistics and independently retained scalar values must share
the same cleanup guarantee. Extend the pinned provider's `ScalarValue` and
`Scalar`; keep every public variant and serialized representation. This is safe
Rust ownership handling and requires no further unsafe exception.

An intrinsic `ScalarValue` destructor takes tuple vectors with `mem::take`, union
children with `Option::take`, and the whole Scalar inside a Variant's existing
Box with `mem::replace`. The replacement is an allocation-free Null/None Scalar,
so no new Box is needed. Ordinary enum field destruction later releases this
inert box. Leaf payloads retain ordinary buffer destruction. Consume existing
children one at a time through owned iterators; never clone or eagerly expand
descendants during cleanup. Keep a Variant's extracted dtype in its continuation
frame until its child value finishes. Release continuations innermost first on
unwind, with child values declared before their metadata owners. Scalar's private
value field likewise precedes its dtype so normal destruction retains schema
credit through nested value release. This changes no public layout promise,
named construction, or explicit protobuf conversion order.

Preserve consuming accessors without deep copies: return tuple storage using
`mem::take`, replace text/binary buffers with their existing empty values, replace
a returned union with an empty union, and replace a returned Variant's boxed
Scalar with Null/None. Wrong-variant diagnostics stay the same. These replacements
need no new heap allocation. Compile all provider and native consumers to find
owned patterns affected by adding Drop; borrow read-only matches rather than
silently cloning nested values.

Before lifting any depth limit, retain the proved cleanup scratch allowance for
independently retained scalar values, array metadata and statistics, including
traversal overlap. Cleanup requests no new execution grant. Acceptance includes
deep tuples, unions, variants and mixtures on small-stack subprocesses, nulls,
wide siblings, partial construction, unwinding, exactly-once buffer destruction,
raw consuming-accessor results, ConstantArray/statistics ownership and dtype
credit surviving nested buffers on normal and one-panic paths. Test construction
must not imply proof of still-recursive validation, clone, comparison, formatting
or serialization. Those operations remain separate deep-lifecycle obligations.

### Native schema structural-operation amendment, November 17, 2026

Continue the deep lifecycle inside the existing pinned Vortex dtype provider.
Ordinary and nullability-insensitive equality and hashing use borrowed iterative
continuations; they do not build a second schema, clone child dtypes, serialize
extension metadata, or eagerly inspect later lazy fields. Preserve existing Arc
identity shortcuts, per-field parse order (including owned/view comparison
orientation), metadata-vtable dispatch and short-circuit order. Map sortedness
and union type tags remain trailing comparisons after their children.

Preserve the ordinary derived-hash field order and the existing
nullability-insensitive hash feed, including their different child-vector length
prefixes. Test against a shallow independent recursive representation on the
same compiler, as well as equality/hash consistency; do not assert portable
numeric hashes. Rust's [Hash contract](https://doc.rust-lang.org/std/hash/trait.Hash.html)
does not promise feed stability across compilers or architectures. Single-child
equality chains need no heap continuation; branching traversal retains only the
unvisited sibling ranges and trailing work. Hashing retains pending suffixes.
The provider's infallible trait boundary still requires ShardLoom admission to
prepay the proved scratch/overlap allowance before any depth increase. This
amendment does not establish that allowance or lift the current depth boundary.

Require small-stack deep equality/hash tests for each compound shape, independent
trees, shared subtrees, deepest-leaf mismatches, nullable equivalence and lazy
parse failures/short circuits. Nullability lookup follows only an unbroken
extension storage chain. Conversion clones extension vtables and metadata in
the original outer-to-inner order, then installs the converted storage inside
private uniquely owned drafts from inner to outer. These drafts temporarily
contain Null storage, cannot escape, never invoke validation or formatting, and
use safe `Arc::get_mut` to finish the same allocation. Preserve the erased
no-change identity shortcut and typed conversion's distinct result ownership.
Drain unfinished drafts innermost first on unwind. Test metadata clone/drop
order, NULL refusal and unchanged nested-child nullability.

Formatting, physical verification and serialization remain separate obligations.
No new unsafe code is needed for these structural operations.

### Native schema FlatBuffer write amendment, November 17, 2026

Replace recursive DType writing with borrowed parent continuations inside the
existing array provider. Keep the same generated FlatBuffer tables, enum tags,
metadata callbacks and builder. Write struct/union name strings and their vector
before visiting children, preserve key-before-value and declared sibling order,
write union tags after child offsets, and create an extension ID before its
storage child and metadata only after that child succeeds. Preserve lazy-field
panic boundaries and metadata errors. Do not preparse the complete schema or
reorder table/string creation: existing shallow buffers must remain byte-identical
when written into identical builder states.

Parent frames borrow schema metadata and retain only pending child offsets. Lists
and extensions need no per-node child vector. Struct/union vectors use their
declared child count and are released after their table is emitted. Source
ownership and the existing writer admission must cover the frames, offsets,
builder growth and overlap before deeper inputs are admitted. A small-stack
write test establishes writer stack safety only; the existing recursive physical
verifier and reader remain explicit blockers to a deep round-trip claim. Keep
those guards until their independent implementation and adversarial validation
are complete. No new unsafe code, schema version or external execution provider
is introduced by this writer amendment.

### Native text validity handoff candidate

The [text validation contract](../architecture/native-text-validity-2026-10-17.md)
permits safe borrowed and retained UTF-8 access inside the excluded pinned array
provider. A private receipt may reuse validation only when reviewed construction
checked the exact immutable values. A dtype label or public unchecked constructor
does not establish that receipt. Uncertified values retain ordinary UTF-8 checks;
host residency, bounds and dtype checks remain mandatory. Receipt-preserving
selection/replacement must retain the original value invariants. The two native
unchecked string conversions are confined to this contract, with no raw pointer,
FFI, workspace unsafe, new ownership or serialization boundary. Correctness,
release-mode rejection and complete-operation cost gates precede retention.

The same contract admits fused reconstruction of native length-prefixed text
records inside the pinned array provider. Exclusive decompression storage may
replace consumed length headers with zeros before freezing, preserving every
value byte. After freezing both allocations, the checked native constructor
validates the selected heap once and checks each view's bounds, prefix and
character boundaries, with independent inline-value validation. The allocator's
freeze operation is not a content-validity proof. Zeroed headers separate
character sequences across values. The existing private unchecked constructor
is permitted only after those immutable-content checks, retaining shape,
validity and allocator-owned table checks. No caller-supplied trust flag or
workspace unsafe code is admitted. Slices retain their full payload owner;
unused malformed text is not validated, and invalid framing remains an error.

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
