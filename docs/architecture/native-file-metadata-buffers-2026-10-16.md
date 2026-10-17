# Native file metadata buffer ownership

Status: corrected runtime and preparation acceptance passed, October 17, 2026.
Three original-provider regressions and four PR-review regressions reproduce the
ownership/cache defects. All twenty focused ownership tests, 139 provider tests,
3,225 workspace tests and a clean complete native run of 2,924 tests pass, with
27 unchanged manual exclusions. Native public-route and preparation checks pass.
Corrected frozen release artifacts pass 100 direct methods, 4,827 public cases,
fresh ingestion, a complete single-pass Full43 and all 18 modular cohorts.
The [acceptance report](../benchmarks/native-file-metadata-buffers-2026-10-17.md)
retains the two earlier observations, complete results, source identities and
original failures. Final-head installed wheels and hosted integration remain PR
gates. This is a finite allocation class under
PERF-03/06/08/09 and the [remaining local scope](native-local-completion-scope-2026-10-07.md).
It does not close those owners or CG-1 through CG-23.

## Source finding and decision

The pinned `vortex-file` 0.85.0 reader uses the session allocator for native
read buffers supplied by ShardLoom. Its footer parser then makes additional
byte-buffer allocations through direct constructors: concatenating a larger
footer, copying serialized DType and layout sections, realigning cached
segments, and copying explicitly requested user metadata. The latter copies
are distinct from the admitted read buffer. Composite DTypes and lazy layout
children can retain the copied FlatBuffers after opening returns.

Route those byte copies through the existing Vortex session allocator. Retain
the resulting buffer owner through the existing shared buffer, schema, layout
and metadata lifetimes. Preserve native parsing, lazy children, file layouts,
statistics and alignment. Do not create a parallel footer parser or add a
second reservation after allocation.

The expert comparator is an allocator-aware native columnar reader: admission
must precede each covered allocation, denial must leave valid existing owners,
and the final consumer must own the reservation until its bytes are released.

## Vortex-first provider check

- Decision: `use_vortex_native_provider`.
- Provider: already-transitive Apache-2.0 `vortex-file` 0.85.0, its
  `VortexOpenOptions`, `FooterDeserializer`, and native DType/layout readers.
- Mechanism: `VortexSession` / `MemorySessionExt` / `HostAllocator` and
  `WritableHostBuffer`; ShardLoom's existing `ReservedHostAllocator` links
  allocation to the shared native memory grant.
- Feature boundary: existing `vortex-file-io` and native local execution
  features. The default build stays lightweight.
- Representation: serialized native metadata bytes become the same native
  DType/layout objects. No decode-to-Arrow execution is introduced.
- Residual execution: none. `fallback_attempted=false` and
  `external_engine_invoked=false` remain required.
- Evidence: denied/exact grants, competing owners, shared clones, malformed
  input cleanup, unchanged values and a complete native workflow regression.

## Covered allocations and compatibility

| Path | Required behavior |
| --- | --- |
| Footer extension after `NeedMoreData` | Check combined length, admit the new complete buffer while both inputs remain owned, copy, then replace the old buffer. Failure preserves the existing deserializer buffer. |
| Serialized DType and layout copies | Allocate the checked slice with FlatBuffer alignment through the session. Retained lazy children and clones keep the same buffer reservation. |
| Cached segments covered by the tail read | Keep an already aligned slice without allocation. When realignment requires a copy, use the session allocator and preserve the segment's required alignment. Only cache optional metadata when requested, and do not build a discarded tail cache for in-memory opens. |
| In-memory segment realignment | Admit required alignment copies through the receiving session allocator, including the copy preceding optional metadata resolution. Aligned caller-owned slices remain shared without a new allocation. |
| Explicit user metadata | Allocate copied metadata payloads through the same session; returned clones/slices keep their owner. Default opens continue to skip optional metadata resolution. |

Keep the existing infallible upstream `prefix_data` API as a raw compatibility
surface, and add a fallible allocator-aware operation for admitted file opens.
All provider-owned file-opening calls must use the fallible operation. Raw
caller-directed buffer construction remains outside this contract. No
ShardLoom public command, file format, encoding ID or package version changes.

This contract covers byte-buffer capacity requested from the existing host
allocator. It does not yet cover the independently allocated segment/context
tables, schema field-name tables, layout child caches, statistics objects,
writer serialization scratch, allocator bookkeeping or process RSS. Those
owners must not be described as covered merely because they reference one of
these admitted buffers. Supplied already-parsed footers retain their original
owner; importing them does not imply fresh receiving ownership of every
structural object.

## Provenance and policy

Vendor the published 0.85.0 crate only after verifying its registry archive
checksum against the workspace lockfile and every copied member against that
archive. The recorded VCS revision has a dirty marker and is not an identical
substitute for the published source. Preserve original member hashes, manifest,
copyright headers and the [Apache-2.0 license at the recorded revision](https://raw.githubusercontent.com/vortex-data/vortex/afb005379dd9a1b7dc7ae2cb49f4f465d91cc2b3/LICENSE).
Record modified files and include provenance in distribution license evidence.
Keep workspace dependency versions and feature policy unchanged; only the
already-transitive provider's source becomes a local patch.

## Acceptance

1. Reproduce admission and retained-owner gaps against the unchanged provider
   before implementing its correction. Test real serialized native files and
   inspect which allocation class each fixture exercises.
2. Cover a footer beyond the initial tail window, exact and one-byte-short
   grants, a competing owner, and release on allocation/parser failure. Preserve
   the original typed allocator refusal inside the provider.
3. Retain schema/layout and metadata clones after file/session teardown and
   prove that credit remains until the final owner drops. Check complete
   values, nullability and metadata bytes, including nested/wide schemas.
4. Cover aligned sharing and misaligned cached slices, optional metadata both
   enabled and disabled, and malformed input without fallback or hidden retry.
5. Run required workspace formatting, Clippy and tests, feature-specific native
   checks, and applicable standalone provider tests. Verify the exact lock graph
   and source provenance. Preserve failed regression evidence.
6. Complete public/native workflows, installed-wheel integration and PR checks.
   Make any performance claim only from the unchanged frozen comparison gate;
   allocation correctness alone is not a speedup claim.

The separately observed preparation I/O-lifetime path requires a deterministic
failure demonstration before changing its reader/session ownership. It is not
silently included in this byte-buffer contract.

## PR review alignment and cache correction

Four focused regressions fail against PR head `c4de1bfbc0de345500a25d065a12a95f61f1979c`.
In-memory segment requests previously realigned raw bytes before the admitted
metadata copy. Both live copies must receive credits, and clones of an aligned
copy must retain its owner. The existing native `BufferSegmentSource` now uses
the session `HostAllocator`; aligned input continues to share its original bytes.

Default file opens also populated optional metadata cache entries that could
never be consumed. Limit that cache to data segments unless metadata was
requested. Build the tail cache only for reader-backed opens: `open_buffer`
retains its own source and previously discarded the cache immediately. Preserve
segment IDs, bounds checks, complete metadata bytes and typed allocator refusal.
All four corrected regressions and renewed broader acceptance now pass at
`566643cd83a7b528bf225496ebd5b3b9a9fa4e5a`; the final commit only simplifies a
test assertion. The [review packet](../benchmarks/evidence/native-file-metadata-review-2026-10-17.json)
preserves original failures, exact source changes and current runtime proof.

## Provider test integration and separate nullable-root finding

The upstream file suite's nullable-root writer fixture expected a panic removed
by the already admitted file-statistics patch. Its replacement checks reopened
schema, row count, empty unmasked-child statistics, and every projected logical
child value for all-valid and mixed-root validity. This preserves the writer
fixture's contract while checking actual native readback.

A stronger default-root scan fixture separately reproduces a nullable-record
type mismatch. The existing structural reader expands the root into a
nonnullable packed record and applies the parent mask to children. This is a
root-projection semantics issue, not a footer-allocation correction. Preserve
the failing fixture and log for source/type-composition follow-through.
ShardLoom's persisted writer and spill store select `SequentialNativeFlatLayout`;
the existing persisted nullable-root regression checks every root value and is
required by this unit's broader acceptance. A dedicated nullable-root spill
proof remains absent; these checks must not imply that broader coverage.

The complete native run also exposes two outdated cleanup assertions in empty
projection and empty native-output tests: each still holds a cloned schema when
expecting zero reserved bytes. The corrections explicitly check live credit,
drop the retained schema, and then require zero. The original complete run
passed 2,918 tests and failed only those two assertions, with 27 existing manual
exclusions. Both focused reruns pass, covering 2,920 distinct native tests;
all production source identities are unchanged. Workspace checks pass with
3,225 tests, native public-route checks with 1,079, and preparation checks with
1,149. The original failed run and exact two test-only edits remain retained.
