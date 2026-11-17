# Vortex timestamp, statistics and ownership patches

## November 17 iterative logical schema reader

Eager list, fixed-list, extension storage and map-key parsing now uses an ordered
continuation stack over already verified bytes. Struct/union children and map
values stay lazy; map keys retain their view and initialized cache. Tests compare
the prior reader's values, raw lazy representation, error messages and extension
contexts. Metadata callbacks keep their inner-to-outer order, failed lazy parses
stay retryable, and internal 8,192-level success/failure cases pass on a 128-KiB
stack. A shallow recursive reference retains the Apache-2.0 source and copyright;
its eager map-key call is made explicit so it cannot invoke the new parser.

These internal tests use only offsets returned by the native writer. Public
reads still use the unchanged physical verifier and depth guard. This is not
deep file or runtime acceptance; see the
[logical-read amendment](../../docs/rfcs/0044-resident-runtime-resource-ownership.md#native-schema-logical-read-amendment-november-17-2026).

## November 17 iterative schema writer

The native FlatBuffer dtype writer uses borrowed parent continuations instead
of recursive child calls. It preserves generated table/string order, lazy field
inspection and extension metadata callback order. Shallow eager/lazy fixtures
match the prior recursive writer byte-for-byte, including populated builders and
partial buffers after metadata errors. Small-stack subprocesses write 8,192-level
list, fixed-list, map, struct, union, extension and mixed schemas. The unchanged
generic verifier still rejects those deep buffers; these writer tests do not
establish a public deep round trip or increase admission. See the
[writer amendment](../../docs/rfcs/0044-resident-runtime-resource-ownership.md#native-schema-flatbuffer-write-amendment-november-17-2026).
The test-only recursive reference retains the pinned writer's Apache-2.0 source
and copyright notice, with only its function entry and recursive calls adapted.

## November 17 schema structural operations

Ordinary and nullability-insensitive DType equality and hashing now traverse
borrowed children iteratively. They preserve Arc shortcuts, lazy parse order,
trailing map/union checks, extension metadata dispatch and the existing hash feed
on the same compiler. Independent shallow recursive references check equality
and each hash write; small-stack subprocesses cover 8,192-level independent trees,
deepest-leaf mismatches, shared subtrees and nullable equivalence.

Nullability lookup follows extension storage iteratively. Conversion preserves
outer-to-inner metadata cloning and rebuilds private, uniquely owned extension
drafts in the opposite order without revalidation or new unsafe code. Failure
drains unfinished drafts innermost first. Erased no-change conversion retains its
Arc identity; typed conversion still constructs a distinct owner. Native depth
admission, formatting, serialization and traversal prepayment remain separate
work under the [structural-operation amendment](../../docs/rfcs/0044-resident-runtime-resource-ownership.md#native-schema-structural-operation-amendment-november-17-2026).

## November 17 borrowed schema children

`DType::try_child_dtypes` borrows immediate children in native declaration order
through the existing fallible lazy field cache. It makes no dtype clones or
iterator allocations, and skipped fields are not parsed. Map and union accessors
now expose that same fallible borrowed inspection. ShardLoom can use one admitted
continuation cursor for schema and result-schema inspection without eager sibling
construction or recursive inspection calls. The iterator does not change provider
serialization, dtype destruction, or any admitted nesting boundary.

## November 14 encoded comparison observations

The comparison adaptor optionally records successful ALP, FastLanes BitPacked
and frame-of-reference dispatches in an explicitly installed session observer.
Empty/null shortcuts and declined kernel attempts do not count. The observations
are calls and logical operand rows, including repeated or nested work; they do
not establish unique rows, avoided reads or decoded bytes. The encoded kernels,
comparison semantics, serialization and provider version remain unchanged.

The caller establishes exclusive operation ownership and supplies the fixed
counter allocation's resource credit. Dropping the scope clears the session's
weak registry before releasing the state, including on a poisoned-registry path.
Any in-flight record retains its own state and credit until it finishes; it
cannot enter a later scope's counts. The registry deliberately uses the standard
mutex so poisoning can invalidate attribution after an unwind; the scoped lint
exception preserves this failure signal. Overlap, poison and counter overflow make
attribution unavailable without changing query results. Ordinary sessions that
do not install the observer retain the existing execution behavior.

## November 9 wide-schema admission

FlatBuffer-backed struct metadata reserves its name, field, lazy-cache and
lookup containers through the existing fallible host allocator before creating
them. Each parsed child struct retains a separate reservation. Fallible field
inspection reports first-touch allocation or parse failure; serialized cache
initialization prevents concurrent callers from building duplicate temporary
child schemas. Failure leaves the cache empty for a later admitted retry.

The reservation follows the struct container and its clones, and drops after
its fields, names and lookup table. Detached field-name or non-struct DType
clones are caller-owned; this patch makes no whole-process allocation claim.
ShardLoom retains its full schema credit on independent native and Arrow result
owners. No serialized type identity, encoding, dependency or execution provider
changes. The local regressions cover refusal, retry, concurrent first touch and
the last retained child owner.

## November 8 provider migration

The current provider is the published **0.93.0** archive. The dated sections
below retain the origin of the local contracts. Upstream now admits full-range
timestamp storage and integrates RunEnd into this crate. The port preserves
bundled timezone validation, physical-tick display outside the calendar range,
fallible allocation and shared-owner lifetimes. Earlier timings do not measure
this version.

RunEnd text/binary expansion in `src/arrays/runend/compress.rs` reuses the original
VarBinView array's shared data-buffer table and reservation. Its moved metadata
fixture retains exact Git and content identities in the supplemental provenance.
The separate `vortex-runend` package and standalone lock are retired.

Bounded minimum and maximum aggregates copy only their compact scalar bytes, so
small bounds cannot retain complete source allocations. The 0.93 bounded
aggregates conservatively label variable-length bounds inexact even when a short
value needs no truncation. No exactness claim is inferred from the value length.

## Provider refresh and native text validation

The October 24 refresh rebases these retained contracts onto the published
0.90.0 provider. Its upstream `BufferAllocator` hooks do not provide the
fallible refusal and external-heap ownership contract required by the existing
ShardLoom callers. `host_memory.rs` keeps that explicit boundary alongside the
upstream allocator; it does not replace every upstream allocation. Typed mutable
access is limited to a sealed set of plain numeric/view representations and
checks the actual pointer alignment before creating a slice.

Safe `VarBinViewArrayExt::utf8_slice_at` and `utf8_buffer_at` reuse a private
receipt from reviewed checked construction, deserialization or length-to-view
validation. They check logical dtype, selected bounds and host residency. A
generic builder or public unchecked constructor does not establish the receipt;
its selected bytes still pass UTF-8 validation. Selection and byte-identical
replacement preserve an existing receipt only for unchanged UTF-8 values.

The November 15 selected-dictionary preparation exposes this immutable evidence
through the read-only `has_validated_utf8` accessor. Admission still checks host
residency, dictionary code bounds and selection bounds independently. It does not
validate discarded values to obtain admission. New composed codes use ShardLoom's
existing credited native builder because upstream fixed-width take uses a separate
allocator. The existing UTF-8 receipt test checks the accessor for checked,
unchecked, selected and binary arrays.

The two string conversions reuse immutable native ownership inside this pinned
provider; no workspace unsafe code, new dependency or serialized field is added.
The [implementation contract](../../docs/architecture/native-text-validity-2026-10-17.md)
records the accepted combined implementation and its bounded comparison. The
0.90 port must pass new validation; that earlier measurement is not a performance
claim for this provider version.

The retained implementation includes a safe length-prefixed text builder. It constructs
only a requested contiguous range's views, normalizes consumed length headers
inside the exclusive decompression allocation, then freezes both allocations.
The checked constructor validates the selected immutable heap once, checks each
view's bounds, prefix and character boundaries, and checks inline values
independently. Original payload ownership and full allocator credit are retained;
malformed framing and selected text fail explicitly. Canonical Vortex Zstd text
uses this builder without changing compressed source bytes or serialization.

This is the Apache-2.0 `vortex-array` 0.93.0 published source with a focused
correction for full-domain timestamp scalars. Vortex's timestamp
extension uses Int64 storage, but the original scalar validator constructs a
calendar span that can panic on valid storage values. Its display path repeats
the conversion. This prevents safe extrema statistics and compression for the
full storage domain already admitted by ShardLoom.

The October 27 extension covers `src/extension/datetime/{timestamp,date,time}.rs`:

- `Timestamp::unpack_native` retains every Int64 value in seconds, milliseconds,
  microseconds and nanoseconds after validating storage and timezone metadata.
- `TimestampValue::fmt` uses checked calendar constructors. In-calendar display
  retains the unit and presentation zone; out-of-calendar values render exact
  ticks and metadata without panicking.
- Date formatting uses checked calendar arithmetic; time-of-day admission
  validates the half-open daily range before invoking a bounded constructor.
- Zone validation uses the already-pinned bundled `jiff-tzdb` dependency rather
  than a mutable host database. No dependency version is changed.

The persisted-output work also changes count and byte-size merges in
`src/stats/stats_set.rs` to checked `u64` addition. Overflow invalidates the
statistic instead of panicking or wrapping, and the representation is independent
of host pointer width. This is used by the bounded `vortex-layout` file collector.

The encoded-transfer work adds an optional opaque lifetime owner to typed array
data, construction parts and structural rewrites in `src/array`. New owners can
only attach to unique, previously unowned arrays. Array data drops before the
owner; clones, typed-parts round trips and owned child references retain it.
Owned slices retain a native parent view until execution, including empty and
repeated slices, instead of eagerly rebuilding metadata without its owner.
Arrays without an owner keep the previous slice path. Buffer extraction still
requires independently retained buffer ownership; raw dtype/data clones are not
covered. This adds no unsafe code, serialized field or encoding identity.
`src/arrays/slice/vtable.rs` reduces the retained view at explicit execution,
preventing repeated slice reconstruction when its child is already canonical.
Owned filters also retain their native parent until explicit execution.
`src/arrays/filter/vtable.rs` then applies the deferred reductions while keeping
the original view alive through canonical execution, preserving sparse-frame
skipping and metadata ownership together.
Structural buffer reconstruction preserves an owner supplied by its provider
and rejects conflicting owners. On the tested 64-bit target, the hook adds
16 bytes to each common array allocation; `ArrayRef` remains 16 bytes.
The [transfer contract](../../docs/architecture/native-encoded-interchange-2026-10-12.md)
requires lifetime, allocation, footprint and complete-lifecycle regression proof.

The extension identity,
metadata serialization, comparison rules, native arrays and file format are
unchanged by this timestamp correction. There is no Arrow intermediate, new query
engine or unchecked scalar construction in that correction. Ordinary calendar operations can
retain their own narrower domains; scalar storage admission is not a claim
that all calendar functions support every Int64 timestamp.

`upstream-provenance.json` records the reviewed archive, all original member
hashes, VCS metadata and verified Apache license. The archive is authoritative
because its recorded VCS state is dirty. `Cargo.toml.orig` remains unchanged;
the normalized manifest adds `publish = false`. The original crate-local lockfile
is recorded but not copied because the workspace owns dependency resolution.
Upstream copyright headers remain in every source file and each modified source
file has an explicit change notice. Two original trailing-whitespace lines are
also normalized: a blank documentation line in `src/arrays/patched/mod.rs`, and a
snapshot literal in `src/scalar_fn/fns/zip/mod.rs`. The latter uses an explicit
space escape in an ordinary string to preserve exactly the original expected
snapshot bytes. Neither normalization changes provider behavior.

The adapter regressions live in `shardloom-vortex/src/native_temporal.rs` and
the existing writer tests. The October 27 extension follows the
[temporal fidelity contract](../../docs/architecture/native-analytical-completion-2026-10-27.md#temporal-fidelity-implementation-contract).
The original acceptance contract is
[`native-timestamp-provider-2026-10-11.md`](../../docs/architecture/native-timestamp-provider-2026-10-11.md).
External readers built with the unpatched provider can still inherit its calendar
restriction; this patch does not certify them.

The provider provenance separately records the matching upstream test support
omitted from the published archive, with its Git and content identities. It also
records the original pinned upstream `clippy.toml` and the local removal of its
obsolete `legacy_session` prohibition for standalone provider checks. The
retained-owner helper accepts an optional
reference, and the primitive constant check uses typed fixed-width chunks;
these preserve their existing behavior while satisfying current lint rules.

Remove this patch when an approved upstream release preserves the full storage
domain and passes scalar, compression, statistics, pruning, writer, resource,
package and complete-workflow checks. Keep the existing feature gates and shared
resource owners when changing the provider.

In 0.90, conversion from nullable contiguous text validates only observable
values and sanitizes null views before raw access. An invalid byte in an unused
null range is not a rejected UTF-8 value. Invalid visible bytes still fail checked
construction. The new row-function fixtures exercise deferred execution errors
without creating an invalid value through an unchecked constructor.

## External memory reservations

The November 17 deep-traversal foundation adds intrinsic iterative `DType`
cleanup. It consumes existing list, field, map, union and extension owners with
`Arc::into_inner`, retains metadata reservations through descendant release,
and never parses an uninitialized lazy field while dropping it. Struct and
union containers own their field vectors inside the existing shared allocation.
A narrowly scoped ownership extractor is governed by the
[RFC 0044 cleanup amendment](../../docs/rfcs/0044-resident-runtime-resource-ownership.md#native-schema-final-owner-cleanup-amendment-november-17-2026).
The enum variants and serialized identity remain the same. Rust consuming
matches and references formerly promoted from temporary dtypes now borrow or
use immutable statics; this is a source-compatibility change for provider users
that move fields out of the public enum. The provider is an internal pinned patch.
The native public depth limits remain unchanged. The structural-operation
section above records subsequent equality/hash/nullability work; formatting,
serialization and resource prepayment remain separate acceptance work.

Array handles now release their declared child slots through an ordered final-owner
loop while keeping the 16-byte erased Arc layout. A sealed extractor recovers the
existing typed allocation and uses `Arc::into_inner` for concurrent last owners.
Retained owners survive metadata, encoding data and every pending child, including
unwinding. The private single-use Arc transfers and existing typed downcast are
specified by the [array cleanup amendment](../../docs/rfcs/0044-resident-runtime-resource-ownership.md#native-array-final-owner-cleanup-amendment-november-17-2026).
Opaque custom encoding internals are not covered by slot traversal alone.

Intrinsic `ScalarValue` cleanup consumes tuple, union and Variant children using
safe Rust transfers. Variant dtype ownership survives its value subtree; ordinary
Scalar destruction likewise releases the value before its dtype while preserving
the prior Debug field order. Consuming accessors transfer existing vector/buffer
storage without deep clones or new placeholder allocations. Provider users that
previously moved owning fields out of ScalarValue must use these accessors or
borrowed patterns. The [scalar cleanup amendment](../../docs/rfcs/0044-resident-runtime-resource-ownership.md#native-scalar-value-final-owner-cleanup-amendment-november-17-2026)
records this source-compatibility change. Focused small-stack subprocesses cover
8,192 nested levels, detached/raw owners, concurrent arrays, partial construction,
statistics and panic ordering. These cleanup checks do not establish deeper
validation, cloning, comparison, formatting, transport or resource admission.

The November 9 wide-schema follow-up makes named lazy field lookup fallible.
`GetItem` type binding and `Select` exclusion/simplification propagate lazy parse
and admission errors. A refused first touch leaves the cache retryable and
releases temporary credit. Existing caller-directed infallible field access,
equality, hashing and formatting remain compatibility surfaces; native cold
readers must resolve required metadata through fallible operations first.
Expression regressions cover refusal, retry and final-owner release.

`HostAllocator::reserve_external(len)` admits a provider's requested external
heap bytes and returns an opaque lifetime owner. The provider must release the
covered allocations before dropping that owner. Custom allocators refuse this
request by default; `DefaultHostAllocator` explicitly retains an unbounded
policy. ShardLoom's allocator adapter connects the owner to the existing shared
`LiveMemoryPool` grant. This reservation does not duplicate the provider's
payload in a Vortex buffer.
The provider supplies the bound; allocator bookkeeping, C call stacks, process
RSS and providers without reviewed bounds are outside this reservation claim.

## Variable-binary tables

`VarBinViewData` owns one shared reservation for its immutable data-buffer table.
Context-aware construction and deserialization admit temporary vectors and the
final slice before allocation, including conversion overlap. Changing views,
validity or nullability retains that owner while the same table remains live.
Empty canonical reductions discard unused storage. Raw table exports remain
caller-owned; raw payload buffers do not carry unrelated table credit.

The existing length/offset view-building loops also create admitted tables.
The restricted non-compacting builder admits old/new capacity overlap and its
final table; payload views and validity retain their separate caller admission.
Its all-kept append path avoids intermediate per-leaf tables. Legacy generic
builders, deduplication and compaction scratch retain their separate boundary.
The [ownership contract](../../docs/architecture/native-varbin-metadata-resources-2026-10-16.md)
defines the exact scope and verification requirements.
