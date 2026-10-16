<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Zstd frame metadata ownership — October 16, 2026

Status: complete local acceptance under PERF-03/06; hosted integration pending.
This finite unit extends [RFC 0044](../rfcs/0044-resident-runtime-resource-ownership.md)
after the completed compression, decoder and selected-text resource work. It
does not close PERF-03/06 or the [local completion contract](native-local-completion-scope-2026-10-07.md).

## Provider decision and scope

Classification: `use_vortex_native_provider`. Keep the pinned Apache-2.0
`vortex-zstd` 0.85.0 provider, `ZstdData`, native frame/dictionary metadata,
slice coordinates, `VTable` and the existing session `HostAllocator`. Extend
the existing vendored provider with ownership for its two frame-sized vectors.
The runtime stays inside the feature-gated `shardloom-vortex` provider boundary,
with the existing execution and Native I/O evidence, `fallback_attempted=false`
and `external_engine_invoked=false`. No encoding, wire format, query executor,
dependency version, unsafe code or alternate decoded representation is added.

The source review distinguishes existing owners from missing coverage. Sparse
text selection already reserves cloned and retained frame lists, and native
encoded transfer already reserves structural metadata. Their replacement must
remove the superseded charges. Independently supplied IPC does not admit Zstd;
this unit does not widen that encoding allowlist. Vortex file deserialization
and the existing native serialization tests do use the provider deserializer.

## Ownership contract

Store dictionary/frame handles and protobuf frame descriptions in one immutable
shared storage object. Array-data clones, slices and nullability-only casts
share this object. Slice coordinates remain per value. No frame-sized vector
copy or new frame-metadata reservation occurs for these operations. The last
storage owner drops both vectors before releasing its reservation.

Compression and deserialization use a reservation from the caller's allocator
before either vector allocates. Reserve the checked requested capacity
`frame_count * (sizeof(ByteBuffer) + sizeof(ZstdFrameMetadata))`, then use
fallible exact-capacity vector allocation and verify the reported capacities.
Hold this reservation through temporary construction, all errors, and the
final encoded array lifetime. Empty tables allocate no vector storage and need
no external reservation. Fixed object/Arc headers, allocator bookkeeping and
unreported allocation slack remain outside this requested-capacity claim;
it is not a process-RSS or OS-allocation-success guarantee.

Before protobuf decoding, traverse the immutable wire bytes without a heap
collection using the locked Prost 0.14.3 key, scalar and skip-field APIs. Count
only top-level repeated frame messages and read the final dictionary-size
value. Validate wire types, extents and the expected buffer count. Preallocate
the two admitted vectors, then merge into the preallocated metadata vector.
Unknown fields and legal repeated scalar fields retain Prost's semantics;
malformed messages fail explicitly. Check the final count/capacity and never
index a missing dictionary buffer. A metadata error releases all acquired
credits. This is bounded provider parsing, not a new IPC transport contract.

Sparse selected-text execution reads frame descriptions by reference. Its
existing maximum of 64 frames permits a fixed stack list of selected frame
numbers. Admit only the selected frame vectors through the provider, keep the
existing selection and decoded-output owners, and remove the superseded
clone/list reservation. Row mapping, eligibility, selected-only decoding,
source validity and output semantics remain unchanged.

An encoded transfer into another execution context creates a fresh frame table
using the receiving allocator and the already copied receiving payloads. It
preserves slice coordinates and retains no source allocator or source metadata
owner. The transfer plan continues to reserve its own schema, slots, temporary
buffer lists and bookkeeping; the provider owns the new frame vectors. Include
that provider reservation in the transfer's retained-metadata report without
reserving it a second time. Ordinary provider buffer replacement uses the
storage's allocator; raw caller-owned input uses the explicit default allocator.

## Compatibility and limits

Keep the existing raw `ZstdData::new` and `into_parts` signatures. These accept
and export caller-owned vectors without an allocator argument. They do not
establish a query-memory ownership claim. Export moves uniquely owned vectors
or clones shared vectors into caller-owned storage; document this boundary.
Remove the remaining production query call to raw `into_parts`. Test fixtures
may continue using it to inspect or deliberately corrupt metadata.

Serialization borrows metadata instead of cloning it before encoding. Its
returned byte vector still belongs to the existing serialization/output owner;
do not claim ownership from a reservation that expires on return. Compression
and decoding payloads, foreign codec bounds, file/IPC envelopes and the inactive
experimental `ZstdBuffers` encoding retain their separate contracts. No global
allocator, other-codec, general state spill, recovery or public transport claim
follows from this unit.

Shared immutable storage avoids a fallible clone API and repeated metadata
copies. Its fixed shared-owner overhead and conservative reservation can affect
tiny inputs or tight grants; cover those boundaries explicitly. Removing raw
construction/export APIs would break compatibility without helping admitted
query execution, so it is outside this change.

## Acceptance

- Prove denial before frame-vector construction, typed reservation errors,
  exact grants, competing owners and cleanup on parse/construction failures.
- Prove frame metadata and reservation identity survive clones, slices,
  nullability casts and producer/session closure until the last owner drops.
- Prove cross-context transfer owns fresh metadata under the receiving grant,
  frees the source grant when its last source owner drops, and cleans up on
  receiving denial. Preserve the transfer's structural accounting report.
- Preserve exact primitive bits, nullable Unicode/binary values, empty/all-null
  input, trained and untrained dictionaries, slice coordinates, compressed
  payload bytes and native serialization. Cover legacy metadata, unknown wire
  fields, duplicated scalars, malformed lengths, wrong wire types, missing
  dictionary/frame buffers and integer/count overflow.
- Preserve selected-text sparse/dense controls, no mask-cache allocation,
  selected row order and retained output lifetime. Check default and custom
  allocators, including explicit refusal of unsupported reservations.
- Freeze the current baseline source and optimized executable before runtime
  edits. Reuse the identical complete compression lifecycle harness and its
  18 predeclared cells, grants, sample/operation counts, serialized identity
  comparison and elapsed/CPU/RSS retain-or-investigate gates from the
  [compression contract](native-compression-resources-2026-10-15.md#acceptance).
  Preserve all samples and failures; repeat the complete reverse order only
  after a declared breach. This cost screen does not establish a speedup claim.
- Pass focused vendor/provider/transfer tests, default and native Rust gates,
  complete direct/public/format/streaming checks, and one Full43 pass on the
  retained source. Freeze source and executable identities, preserve failures,
  and independently verify portable evidence before hosted integration.

The [acceptance report](../benchmarks/native-zstd-metadata-resources-2026-10-16.md)
records six passing Rust gates, 67 standalone Zstd provider tests, 100 direct
methods, 4,827 public cases, the retained 18-cell lifecycle screen and one complete
Full43 pass. Independent inspection verifies all 13,728 portable members and
their complete outputs. The archive preserves this contract's original pending
version and its predeclared gates; this status update does not alter that record.
Hosted checks and installed-wheel evidence remain separate pending gates.

All CG-1 through CG-23 and the conditional performance/transport queue retain
their existing owners and proof requirements. No version bump is implied.
