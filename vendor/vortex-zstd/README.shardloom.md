# ShardLoom Zstd allocation and selection contracts

This is the reviewed Apache-2.0 `vortex-zstd` 0.90.0 published source, with the
existing ShardLoom resource and text-construction changes ported to its context
and serialization APIs. `upstream-provenance.json` records the authoritative
archive, original member hashes, license and preceding provider identity.
`publish = false` prevents publishing this local provider. The root lockfile
owns production dependency resolution; standalone-test lockfiles are ignored.

The October 26 dependency correction selects `zstd =0.14.3` and
`zstd-sys =2.1.2` (bundled libzstd 1.5.7). This removes the affected Rust wrapper
versions reported by the upstream context-reuse, seekable, legacy-stream and
WebAssembly allocator advisories. The audited C version, static workspace
ownership and legacy-frame rejection remain in place. The wrapper packages use
BSD-3-Clause licensing; their exact texts are included in the package notice
bundle. This update does not add streaming, seekable or WebAssembly support.

Retained changes admit codec/workspace bounds, training and frame metadata,
compressed storage, decompression allocations and variable-binary handle tables
through the shared fallible host allocator and external-memory reservations.
Clones and slices retain their concrete owners. Selected-frame decoding avoids
unreachable frames and validates selected values, framing and lengths. Checked
UTF-8 reconstruction reuses the retained array provider's validation receipt;
binary reconstruction preserves arbitrary bytes.

String packing retains the same valid-value length-prefixed payload but writes
only frame-start offsets, after resolving effective frame width. It no longer
allocates one offset per valid value or constructs a second frame index. The
existing allocator still admits payload, frame indices, sample sizes, codec
workspace and retained output independently. Exact-frame tests compare the
sparse index with the original per-value representation for dictionary and
non-dictionary compression, including nulls, binary values, slices and partial
frames. Frame-local payload packing is not part of this change.

`ZstdBuffers` has a separate draft wire edition. Registering its implementation
does not enable that edition. The ordinary ShardLoom writer retains frozen core
edition enforcement and rejects this optional representation. Its upstream
compression constructor is not a newly admitted ShardLoom codec route. Existing
frame-based native Zstd behavior remains separately covered by its resource,
selection, nullable and full-lifecycle tests.

The finite allocation contracts do not cover process RSS, C call stacks,
allocator bookkeeping or unrelated provider allocations. Upstream registration
does not certify all optional encodings or broader external-reader behavior.
Vortex remains native input/output; no external query engine is introduced.

Item 23 reuses the existing one-shot static decoder for ORC translation through
the safe `DecoderWorkspace` entry point. The caller supplies a bounded
destination and the same session allocator. ORC retains one actual context
buffer per reader/metadata operation, including across later compressed blocks,
so live batch sizing observes its cost. The owner stores no raw pointer and
each call reinitializes the existing borrowed static decoder. Native frame
arrays and `decompress_frame_into` retain temporary-workspace lifetimes.
Modern and skippable members, validation, legacy rejection,
alignment checks and cleanup use the same implementation as native frame arrays.
No new unsafe operation, dynamic context, stream window or query engine is added.
