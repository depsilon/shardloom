# ShardLoom Vortex buffer provider candidate

## November 17 explicit retained buffer owners

`Buffer::with_retained_owner` attaches a shared owner to the existing backing,
preserving pointer, length, alignment and bytes. Unlike an opaque nested Bytes
owner, the backing link is available to ordered iterative cleanup. Storage
releases before attached owners, and uniquely owned attachments release oldest
first. Clones, nonempty slices, typed conversion and Bytes/Arrow exports keep
the complete owner chain, including explicitly owned empty buffers. Ordinary
empty-slice detachment and immutable `try_into_mut` behavior are unchanged.

Safe `Arc::into_inner` extraction handles shared and concurrent final owners.
Local pending owners drain in reverse attachment order on a single panic as
well as normal completion; no global queue, new unsafe operation, byte copy or
cleanup execution grant is introduced. Native deep admission still must prepay
link/cleanup storage before changing public limits. Tests compare the previous
owner wrapper's shallow behavior and exercise 8,192 links, intermediate and
weak aliases, concurrent release and storage/owner panics on 128-KiB stacks.
See the [ownership amendment](../../docs/rfcs/0044-resident-runtime-resource-ownership.md#native-buffer-credit-continuation-amendment-november-17-2026).

## Provider source and preceding empty-owner contract

This directory contains the Apache-2.0 `vortex-buffer` 0.93.0 published source,
with the Cargo registry archive and original member hashes recorded in
`upstream-provenance.json`. The published archive is authoritative and its VCS
record identifies the upstream revision and dirty checkout. The upstream
generated `Cargo.lock` is recorded in the original member map but omitted from
this local source patch; workspace resolution remains controlled by the
workspace lockfile. `publish = false` marks the local provider as non-publishable.

The provider is being evaluated to preserve admitted ownership for aliases over
bare buffers. Upstream `Buffer::from_bytes` converts zero-length input to an
empty aligned buffer, dropping the explicit `Bytes` owner. The added safe
`ByteBuffer::from_bytes_aligned_with_owner` constructor preserves that owner through
empty clones and typed conversions. It shares the existing alignment and length
validation and changes neither the ordinary byte constructor nor empty-slice behavior.
Arrow imports, typed Arrow exports and conversion back to `Bytes` also preserve
an explicitly owned empty buffer, including when other buffer aliases exist.
ShardLoom uses it for fallible host-allocation and metadata credits. It adds no
payload allocation, copying, allocator implementation, or execution provider.

The retained local changes are covered by 933 unit tests, 17 documentation
tests and strict all-feature, all-target lint on October 24. ShardLoom's native
workspace and complete-workflow acceptance are recorded separately in the
provider-refresh plan; standalone checks do not establish those results.
