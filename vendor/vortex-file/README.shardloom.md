# ShardLoom file metadata buffer patch

This directory contains the reviewed Apache-2.0 `vortex-file` 0.85.0 registry
source. `upstream-provenance.json` records the authoritative archive, all
original member hashes, license and dirty upstream revision. The workspace
lockfile owns production dependency resolution; the archive's crate-local
lockfile is recorded but omitted.

The changes in `src/footer/deserializer.rs` and `src/open.rs` use the existing
session `HostAllocator` for footer growth, copied DType/layout FlatBuffers,
cached-segment realignment and requested user-metadata payloads. Buffer owners
retain their allocator reservations through shared clones and lazy metadata
views. Already aligned cached slices remain shared. Allocation errors propagate
without substituting unbounded storage. The original infallible `prefix_data`
operation remains a documented raw compatibility surface; file opens call the
new fallible `try_prefix_data` operation.

File/array/layout identities, serialization, provider version and dependency
versions are unchanged. `publish = false` prevents publishing this local patch.
No external query engine or execution fallback is introduced.

The owning contract is
`docs/architecture/native-file-metadata-buffers-2026-10-16.md`. It covers the
named byte-buffer allocations, not structural context/segment collections,
schema field-name tables, layout child caches, statistics objects, writer
scratch, allocator bookkeeping or process RSS. Supplied parsed footers retain
their existing owners.

ShardLoom's native tests exercise the linked provider with real serialized
files, allocator refusal, exact grants, competing owners, retained clones,
malformed-input cleanup, aligned sharing and complete nested/wide values.
The standalone library test target preserves the upstream reader/writer
regressions and links the reviewed array and layout patches. Its ignored
crate-local lockfile starts from the workspace lockfile; any extra upstream
development dependencies belong to that test target only.
The nullable-root writer fixture now verifies reopened metadata, every logical
child value with the parent mask applied, and absent unmasked child statistics,
matching the already admitted layout provider patch instead of expecting its
former panic. The upstream default struct reader's raw nullable-root projection
is a separate limitation; this fixture does not claim that projection is fixed.
