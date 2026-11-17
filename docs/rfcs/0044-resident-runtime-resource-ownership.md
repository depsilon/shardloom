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

### Native schema logical-read amendment, November 17, 2026

Replace eager list, fixed-list, extension-storage and map-key recursion with
ordered continuations over an already physically verified FlatBuffer. Struct
and union fields and map values retain their existing lazy views. A map key
retains its view and initialized cache rather than changing to an owned field.
Check both map child offsets before parsing its key; require extension IDs
before storage, and resolve metadata and the registry only after storage
succeeds. Preserve the extension context added by each failed storage ancestor,
including errors from nested map keys and lazy container construction.

The logical parser does not verify bytes and must never substitute for physical
verification. Its internal deep tests may use only known offsets returned by
the native writer; the public entry still uses the unchanged checked root and
its existing depth limit. Require shallow recursive-reference equivalence,
lazy cache/parse order, malformed logical declarations, extension callback
order and small-stack eager-chain tests. Final resource admission must prepay
continuations, created owners and unwind overlap before any guard is raised.
No new unsafe code or public transport capability is authorized by this step.

### Physical verifier continuation amendment, November 17, 2026

The pinned FlatBuffers 25.12.19 verifier owns checked scalar/offset access,
table/vtable validation, UTF-8 and terminator checks, cumulative apparent-byte
and table counts, depth accounting and error traces. Its
[table verifier](https://github.com/google/flatbuffers/blob/v25.12.19/rust/flatbuffers/src/verifier.rs)
holds a mutable verifier borrow throughout recursive child checks. No existing
public API can preserve that parent across an iterative child visit. Resetting
counters, skipping physical checks or duplicating a raw-byte verifier is not
an accepted replacement.

Vendor the same checksum-pinned Apache-2.0 crate, with no version or dependency
addition, to expose a narrow safe continuation. Suspending a table consumes its
handle and retains the checked descriptor, immutable buffer/options references,
depth and cumulative counter minima. Resuming consumes this private, non-Clone
token, checks the same buffer/options identity, exact pending depth and monotonic
counters, then restores the handle without re-reading or recounting bytes/tables.
The existing `finish` decrements depth exactly once. Resume misuse is a programmer
error; an input error aborts verification as before. No verifier reset or unsafe
code is introduced. Export the existing checked vector-range and error-trace
helpers without changing their algorithms or default limits.

Native generated schema verification may delegate to an explicit continuation
walker using these checked primitives. Preserve original field order, absent
field and union-tag behavior, offset arithmetic, string/primitive checks, expanded
alias work, all configured depth/table/apparent-byte limits and the complete
error trace. Complete each child before touching the next field or sibling.
Continuation storage must be prepaid by native admission before any public
depth increase; this amendment keeps existing public limits.

Require exhaustive generated-table coverage, shallow comparisons to independent
original recursive verifiers, malformed/truncated/mutated buffers and configured
budget boundaries, alias amplification, small-stack deep verification with
explicit test options, and exact dependency/license/source records. Check both
standard and no-default-feature FlatBuffers builds, affected native/compatibility
providers and the supported Rust version. Physical verification alone does not
certify logical values, array decoding, formatting or the complete runtime
lifecycle.

### Native schema formatting amendment, November 17, 2026

Schema `Display` bytes participate in prepared-source and layout identities.
Keep those bytes unchanged, including nullable suffixes, union tags, field
order, extension metadata and the alternate standalone-struct form. Replace
child recursion with borrowed ordered continuations. Resolve both map children
before displaying either, as the original argument evaluation does. Preserve
the struct/union join boundary: complete its child text before writing that
joined value to the caller. While already inside such an infallible string
buffer, nested joins may append to the same buffer; do not allocate one full
subtree string per ancestor. Keep extension metadata formatting callbacks and
lazy parse/error order. No child dtype clone or new schema representation is
needed. Native admission must cover the continuation stack, joined text and
metadata-format overlap before exposing deeper schemas.

`Debug` is diagnostic text, not the schema identity format. It must not parse
lazy fields merely to assess depth. Inspect only owned and already cached
schema children using a fixed-capacity borrowed stack. Preserve the previous
derived representation, raw lazy/cache information and formatter flags for
schemas with at most 32 child edges. A deeper owned/cached tree prints the
ordinary Rust non-exhaustive variant form, such as `List(..)`, instead of
recursively expanding it. Standalone field/map/union/extension debug wrappers
reach the same dtype guard. This is an explicit diagnostic presentation change,
not loss of schema data, a serialization change or an execution depth limit.
Opaque third-party metadata callbacks retain their own contract; native
admission covers the supported bounded metadata providers.

Require shallow recursive-reference Display comparisons, formatter variants,
lazy cache/failure and metadata callback ordering, failed output sinks,
deep and wide schemas and small-stack diagnostic/Display checks. Do not
truncate Display or use the bounded Debug form in identity calculations.
Public data depth limits remain until the rest of the lifecycle is admitted.

### Native scalar structural-operation amendment, November 17, 2026

Continue the same native-value lifecycle through `ScalarValue` cloning, equality
and hashing. Tuple elements, selected union children and row-specific Variant
scalars use ordered borrowed continuations. Clone only the existing native
containers; retain the ordinary shared buffer and dtype owners. Build Variant
copies directly from their already validated fields without revalidation or
an intermediate row representation. Partially built native values use their
intrinsic iterative cleanup on failure.

Preserve tuple length checks and element order, union tag-before-child checks,
and the Variant scalar's nullability-insensitive dtype comparison before its
optional value. Hashing must preserve the prior enum and Option discriminants,
tuple length prefixes, union tags, dtype feed and every leaf hash write on the
same compiler. Scalar's existing outer dtype/optional-value contract and the
standalone `UnionValue` wrapper remain unchanged. No new unsafe code or provider
dependency is needed. Native admission must cover continuation growth and
cloned container overlap before deeper values become public.

Require independent shallow derived-reference comparisons and per-write hash
checks, all primitive and null states, selected union tags, Variant dtype
differences, wide tuples, shared-buffer ownership and 8,192-level small-stack
clone/equality/hash/release tests. Validation, ordering, formatting, transport
and whole-runtime resource admission remain separate obligations; these checks
alone do not lift the public depth guard.

### Native scalar validation and diagnostic amendment, November 17, 2026

Replace recursive native scalar compatibility and detailed validation with the
same ordered borrowed traversal. Retain the inline null/primitive fast checks;
the compatibility walk must not construct diagnostic strings. Preserve lazy
field order, map key/value pre-resolution, union tag selection, Variant null
rules and storage-before-extension callbacks. Detailed validation keeps the
same first failing leaf and existing messages for ordinary shallow inputs.
The constructor's top-level legacy F16-bit conversion remains unchanged.

Failure presentation must not reintroduce stack recursion or multiply an
arbitrarily deep schema across every ancestor message. ScalarValue Debug retains
its original derived field spelling and flags through 32 value-child edges,
then uses a non-exhaustive variant such as `Tuple(..)`. It inspects values without
validating, cloning or formatting extension metadata. Detailed validation uses
the same bound before displaying a mismatched raw value. Preserve all existing
ancestor contexts when there are at most 32; for deeper failures retain the 32
nearest contexts and explicitly report the number of omitted outer contexts.
This is a diagnostic presentation bound, not a data admission limit. Ordinary
Display and persisted values are not truncated by this policy.

Require shallow success/error/callback parity against the original recursive
validator, lazy-error ordering, independent null/length/tag/type controls, deep
valid and invalid constructors on small stacks, bounded deep diagnostics and
cleanup after failure. Prepayment for continuation growth and bounded diagnostic
overlap remains mandatory before public depth admission changes.

### Native scalar ordering amendment, November 17, 2026

Replace recursive native Scalar ordering with borrowed ordered continuations.
Preserve dtype equality ignoring nullability before inspecting values, nulls
before present values, existing primitive/decimal comparisons, lexicographic
list and map ordering, exact struct shape checks and field order, and union
comparison only when both values select the same tag. Present Variant values
remain unordered. Extension storage is unwrapped only for tuple interpretation,
as in the original implementation; this amendment adds no ordering capability.

Map entry interpretation borrows the existing key/value dtypes instead of
constructing an intermediate entry schema. Resolve both map fields before
comparing entries, preserve lazy struct/union selection and stop at the first
unequal or unordered child. The same native dtype and value owners outlive all
borrowed frames. Require original shallow-reference parity, null and length
controls, lazy failure ordering, unchanged extension callbacks and 8,192-level
small-stack comparisons. Traversal scratch must be admitted with the complete
lifecycle before the public nesting guard changes.

### Native scalar Display amendment, November 17, 2026

Use one borrowed continuation engine for ordinary typed Scalar and raw
ScalarValue Display, including standalone list, struct, map, union and Variant
views. Preserve existing text, raw versus typed spelling, null distinctions,
legacy child F16 construction, lazy field order and validation/formatting callback
order. List child construction happens before its separator; struct fields and
map entries finish their temporary formatted text before the separator reaches
the caller. Keep debug-build unchecked-constructor validation where the original
view performed it, without adding that validation to release builds.

Nested field/entry joins may share one infallible String while retaining the
outermost field/entry write boundary. A formatting error inside that String must
still panic, as the original format operation did; an error in the caller's
formatter must still propagate at the same output boundary. Avoid cloning whole
child values or building one complete subtree string per ancestor.

The pinned ForeignExtDType's native display is exactly its raw ScalarValue and
has no unpacking side effects; route that known provider through the same raw
continuation. Other extension NativeValue formatters retain their native vtable
boundary and callback semantics. Arbitrary custom extension formatter recursion
is not made stack-safe by this amendment. Native extension admission must keep
its existing typed storage contracts.

Require an independent shallow recursive reference, typed/raw/standalone view
parity, null and legacy F16 controls, formatting flags, extension callback and
lazy failure order, failed-sink partial-output comparisons, and deep small-stack
Display and cleanup. Ordinary Display is not truncated. The existing public
depth guard and traversal/join/diagnostic prepayment obligations remain until
complete lifecycle acceptance.

### Native scalar initialization and inspection amendment, November 17, 2026

Use private ordered continuations for default/zero value construction, zero
inspection and approximate scalar byte counts in the existing native provider.
Preserve nullable defaults, non-null zeros, first-union-variant selection,
extension-storage behavior, and the final public constructor's validation.
Empty lists/maps remain identities without inspecting their child dtypes; a
zero-length fixed list must likewise avoid visiting its element type. Construct
fixed-list elements and struct fields in declaration order, stop at the first
missing default/zero, and release partial values through native scalar cleanup.

Zero inspection keeps list/map emptiness distinct from fixed-list/struct
conjunction. A null or undefined child makes a conjunction false; a first union
child propagates its own result, a later union variant is false, and a present
Variant containing null is true. Preserve short circuits and the original lazy
child and debug-build unchecked-constructor validation order without copying
whole child values. Extension storage remains an ordinary native child.

Approximate bytes retain the original nullable fixed-width estimates, decimal
widths, union tag byte, and per-container addition order and overflow behavior.
List element validation is eager before the first child estimate; struct
validation is per field; both map children are resolved/validated before its key
estimate. Retain those boundaries with borrowed children and local accumulators.
This estimate remains an estimate of uncompressed data, not an allocation grant.

Require shallow original-source references, complete leaf widths and nulls,
empty/wide controls, lazy failures and extension callback ordering, early exits,
and 8,192-level small-stack construction/inspection/cleanup. No new unsafe code,
dependency or execution fallback is allowed. Public depth admission still waits
for complete transport, traversal/allocation prepayment and lifecycle acceptance.

### Native array serialization and physical verification amendment, November 17, 2026

Keep the pinned native `ArraySerializationTree` as the serialization owner,
using a private flat preorder node vector and subtree-end positions instead of
recursively owned child trees. Each node retains its source array, selected wire
ID, metadata and native buffer handles. Visit each serializer once in original
depth-first child order, including providers that substitute wire IDs, buffers
or children. Complete serialization before the existing total-buffer check;
preserve the `u16` buffer-count limit and native buffer emission order.

Write the unchanged ArrayNode FlatBuffer through ordered parent continuations.
Intern each encoding and write its metadata before its children, then write
child offsets, buffer indices and source statistics in the original order.
Keep exact bytes, partial-builder results on errors, allowed-ID checks and
alignment/padding behavior. Temporary tree cleanup must not recurse through
children; source, metadata and buffer owners remain alive through their use.
This changes neither the physical file schema nor the execution provider.

Replace only ArrayNode's recursive physical verification with checked parent
continuations through the same pinned FlatBuffers suspension API. Encoding and
metadata fields precede ordered child offsets; buffer indices and statistics
follow them. Preserve optional-field handling, alignment/range checks, expanded
alias work, exact error traces and all configured depth/table/byte thresholds.
Default verifier limits remain unchanged; no unchecked buffer access is added.

Require independent original-source shallow serialization/verifier references,
wire-substitution and callback/error-order controls, native buffer identity,
padding/offset cases, malformed/truncated/mutated data, alias and threshold
controls, and 8,192-level small-stack write/verify/cleanup tests with explicit
test-only verifier options. Statistics/scalar transport and plugin-directed
logical array decoding retain their separate obligations. Admit all node,
continuation and builder overlap before changing public deep-data admission.

### Native schema JSON boundary amendment, November 17, 2026

The two direct native DType JSON writers (result-schema delivery and inert
application-schema wire-size validation) may use one private iterative adapter
for the existing `vortex.dtype.serde.v1` representation. Keep generic upstream
Serde behavior unchanged. Use Serde JSON for leaf values, names, IDs, tags and
metadata; emit compound syntax with the same compact writer boundaries and
ordered borrowed continuations. Do not create a nested `serde_json::Value` or
clone a complete schema merely to serialize it.

Keep names before dtypes, eager direct-field resolution before serializing a
struct/union's dtype sequence, map key before value, union tags after children,
and extension metadata callbacks after storage serialization but before its
metadata field key. Preserve complete bytes, callback failures, partial failed
sink output and existing caller diagnostic mapping. Existing fallible schema
preflight and 8-MiB output/wire-size limits remain in both callers.

Charge continuation capacity and reallocation overlap against the same caller
memory pool before allocating; release the frames before their credit, and
check cancellation as traversal advances. Do not infer a whole-process grant
for arbitrary extension callbacks. Require original generic-Serde parity for
all shapes and escaping, failed-sink/callback ordering, exact wire-size checks,
denial/cancellation/refund controls and 8,192-level internal writing/cleanup on
a small stack. Public depth admission still waits for complete lifecycle proof.

### Native protobuf object ownership and conversion amendment, November 17, 2026

Keep the pinned provider's protobuf schema and generated models. Add intrinsic
cleanup for the existing `pb::DType` and `pb::ScalarValue` owners in a handwritten
sibling module: take their optional oneof fields with safe Rust and traverse
owned child boxes/vectors through continuations. Every recursive model cycle
crosses one of these two owners. Independently retained children, partial
construction and unwind must be safe without a new execution grant in Drop.
No generated layout change, unsafe operation or new dependency is needed.

Replace native DType protobuf object conversion with ordered borrowed cursors
and owned partial-result continuations. Preserve names before children, lazy
field order, both map dtypes resolved before key encoding, key conversion before
value conversion, union tags after children and extension metadata after storage.
On reads, check a missing map value only after converting its key. Preserve
existing malformed-field errors and constructor panic behavior; unwind partial
native owners from the innermost continuation. Use the same native scalar model
for the following value-conversion work, preserving its existing leaf policies.

Scalar-value conversion uses borrowed protobuf/native value cursors and ordered
partial tuple, union and Variant continuations. A read cursor may own a shallow
native dtype header, sharing the existing schema children; a Variant continuation
retains its newly parsed dtype until final construction. Unwrap exactly one
extension storage layer per value invocation. Preserve struct width checks before
lazy field resolution, one entries dtype per map invocation, union tag and child
validation order, and Variant dtype parsing before missing-value checks. Keep
the original repeated validation and extension callback counts; removing recursive
calls does not authorize eliminating observable validation work. Leaf numeric,
decimal, byte/string and legacy F16 conversion policy stays unchanged. Partial
native owners unwind inside-out, including failure after an earlier deep sibling.

Require mechanically checked original-source shallow references, complete type
shapes, exact shallow protobuf bytes, malformed objects, lazy and extension
callback order, and 8,192-level small-stack conversion and temporary-owner
cleanup checks. Object conversion does not establish deep wire transport:
generated Clone/Debug/equality and Prost encoding/decoding keep separate
obligations. Prost's default decoding limit and public depth guards remain
unchanged. Before public deep admission, prepay protobuf model, continuation,
error and cleanup overlap through the existing native operation owner; raw
provider object APIs alone do not establish resource admission.

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
