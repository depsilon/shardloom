# Vortex timestamp, statistics and ownership patches

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

This is the Apache-2.0 `vortex-array` 0.90.0 published source with a focused
correction for timezone-free microsecond timestamp scalars. Vortex's timestamp
extension uses Int64 storage, but the original scalar validator constructs a
calendar span that can panic on valid storage values. Its display path repeats
the conversion. This prevents safe extrema statistics and compression for the
full storage domain already admitted by ShardLoom.

The timestamp changes are confined to `src/extension/datetime/timestamp.rs`:

- `Timestamp::unpack_native` retains every Int64 value for microseconds without
  a timezone after the existing storage type validation.
- `TimestampValue::fmt` uses Jiff's checked direct microsecond conversion for
  that case. In-calendar display stays unchanged; other values render exactly
  as `<integer>us since Unix epoch`.

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

Other temporal units and timezone policy are unchanged. The extension identity,
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
the existing writer tests. The full acceptance contract is
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
