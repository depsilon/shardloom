# ShardLoom Vortex buffer provider candidate

This directory contains the Apache-2.0 `vortex-buffer` 0.90.0 published source,
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
