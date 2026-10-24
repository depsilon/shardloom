# ShardLoom fallible read admission

This is the reviewed Apache-2.0 `vortex-io` 0.90.0 published source. The
registry archive, original member hashes, license and upstream revision are
recorded in `upstream-provenance.json`. The published archive is authoritative;
its VCS record identifies a dirty checkout. `publish = false` prevents
publication of this local provider. Production dependency resolution belongs
to the workspace lockfile; ignored standalone-test lockfiles are generated
separately.

Vortex 0.90 separates its buffer allocator from the older fallible host
allocator. The local `vortex-array` boundary preserves ShardLoom's existing
refusal and last-owner contracts. This I/O patch connects that boundary to
`FileReadAt::open_with_host_allocator` and
`ObjectStoreReadAt::new_with_host_allocator`. The linked file provider uses
these constructors. Upstream buffer-allocator constructors remain available
for other callers; their presence does not establish ShardLoom admission.

The requested range is checked for overflow before allocation. Reads reserve
their destination before file or object-store work, expose only initialized
bytes, verify short/oversized responses and retain the allocation owner through
buffer clones and slices. Errors release partial allocations. The compatibility
buffer allocator zero-initializes its mutable read buffer before creating a
safe byte slice; it does not expose uninitialized memory through that slice.

The finite claim covers the destination read buffer, not transport-internal
response storage, object-store metadata, task bookkeeping, allocator overhead
or process RSS. This patch does not enable a remote connector, new user-facing
I/O mode or external execution engine. Array/file/scan providers remain subject
to their existing ShardLoom policy and certificates.

`src/fallible_read_tests.rs` exercises refusal before I/O, range overflow,
initialized storage, short reads and EOF, local-file and object-store reads,
parallel ranges and last-owner release. The standalone `object_store` library
suite links the reviewed array patch. The feature/target conditions avoid
installing unused file-only code on WebAssembly. The async-trait lint adjustment
is scoped to its generated future annotation.
