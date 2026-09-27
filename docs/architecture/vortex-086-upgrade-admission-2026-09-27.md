# Released Vortex upgrade admission — C1

Decision: drop the 0.86.x upgrade at the resource-contract gate and retain the
current 0.85.0 provider. No upgraded binary was benchmarked, and no performance
loss or gain is asserted. This is a specific API compatibility failure, not a
minimum-gain cutoff or a general rejection of upstream Vortex work.

ShardLoom's current native session installs `ReservedHostAllocator`. It reserves
before provider allocation, returns a typed recoverable error on budget denial,
and keeps credit attached to the final buffer owner through slices and clones.
The shared aggregate boundary distinguishes that typed denial from corruption
before applying an already-admitted pressure transition. The existing tests
check typed error propagation, full-allocation credit lifetime and zero leaks.
This contract is scoped to allocations through the provider, not all process RSS.

The released 0.86.1 [memory session](https://github.com/vortex-data/vortex/blob/0.86.1/vortex-array/src/memory.rs)
replaces the safe `HostAllocator`/`HostBufferMut` interface with
`BufferAllocatorRef`, backed by `allocator_api2::Allocator`.
Its [buffer allocation implementation](https://github.com/vortex-data/vortex/blob/0.86.1/vortex-buffer/src/allocation.rs)
sends allocator refusal through `handle_alloc_error` during allocation and
growth instead of returning a typed `VortexResult`. A custom allocator would
also require an unsafe trait implementation, contrary to the workspace's current
`unsafe_code = "forbid"` rule. Forwarding quota refusal as `AllocError` therefore
does not preserve the existing public failure contract. Falling back to the
global allocator would omit the reservation ownership being preserved.

External-byte ownership is a different boundary: `ByteBuffer::from_bytes_aligned`
still retains an existing byte owner, but does not intercept new allocations
inside native provider builders. It cannot replace the removed allocation hook.
No new unsafe wrapper, vendored provider fork, external execution engine or
unaccounted allocator substitution is introduced for this performance screen.

The version inventory reviews Vortex 0.86.0/0.86.1 and their Arrow 59.x alignment;
the current workspace remains on Vortex 0.85.0 and Arrow/Parquet 58.3.0. Format,
writer, scan and broader API migration are not certified by this admission
review. License/MSRV availability alone does not authorize weakening a runtime
resource contract.

Reopen this exact upgrade when an upstream fallible provider-allocation surface
can retain typed budget denial and buffer-lifetime credit, or an explicitly
approved replacement design proves those same contracts. Then run the API,
feature/MSRV, ownership/pressure/cancellation, complete query and ingest gates
before retaining a version change. No whole PERF or competitive gate closes here.

The exact 0.86.0 and 0.86.1 memory-session, allocation and mutable-buffer files
are byte-identical. The source audit traces the shared ExecutionCtx → typed
builder → allocation/growth path. Available try_* methods return iterator errors
or indicate unique-owner conversion, not allocation denial.

The [portable evidence](../benchmarks/vortex-086-upgrade-admission-2026-09-27.json)
archives exact tagged sources, registry/release inventories, retained adapter
source and the source audit. Hashes independently match all twelve local/upstream
contract files. The three retained-provider allocation tests pass in C2's final
native validation; no upgraded-provider build or test is claimed. Source bytes
remain verifiable without the audit's historical local commit label.

Provider classification: `blocked_until_vortex_or_shardloom_evidence` for the new
line; the existing `use_vortex_native_provider` disposition remains active.
No lockfile, runtime, native format, package or release changes are retained.
