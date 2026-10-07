<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Zstd Decoder Workspaces — October 7, 2026

Status: implementation contract for `NATIVE-CODEC-WORKSPACES`, under PERF-03/06
and CG-5/19/20/21. This extends [RFC 0044](../rfcs/0044-resident-runtime-resource-ownership.md#pinned-zstd-decoder-workspace-decision).
Runtime acceptance and hosted integration remain to be proved. Published v0.4.0
is unchanged.

## Decision and provider check

Use the existing native Vortex `Zstd` provider and its session `HostAllocator` to
own the decoder context and prepared dictionary, in addition to the previously
accepted payload, view and validity buffers. Reuse the query's reservation owner
and denial diagnostics. Do not introduce a replacement encoding, allocator,
session, executor or per-frontend decoder.

Classification: `use_vortex_native_provider`. The provider is the existing
Apache-2.0 `vendor/vortex-zstd` 0.85.0 patch behind ShardLoom's upstream-Vortex
feature boundary. `ZstdData::decompress_slice` is shared by canonical execution,
scalar access and native append. Its selection already identifies complete
compressed frames overlapping the requested values. This change preserves that
decode extent, logical dtype, nullable scatter and native serialization.
No Arrow execution representation or external-engine residual is introduced.
Existing native provider and no-fallback certificates retain their meaning.

The locked `zstd` 0.13.3 bulk decoder owns a private `zstd-safe` 7.2.4 context
and copies dictionary content through its dynamic dictionary loader. Neither
safe wrapper exposes the static initialization surfaces needed to use Vortex's
fallible allocator. The direct pinned `zstd-sys` 2.0.16 dependency exposes those
bindings without changing the locked C provider, Zstandard 1.5.7. The binding
crate is MIT/Apache-2.0; the C provider is used under its BSD-3-Clause option.
No C implementation is copied or reimplemented.

## Audited C contract

The authoritative sources are Zstandard's pinned
[1.5.7 header](https://github.com/facebook/zstd/blob/v1.5.7/lib/zstd.h),
[decoder](https://github.com/facebook/zstd/blob/v1.5.7/lib/decompress/zstd_decompress.c),
[dictionary implementation](https://github.com/facebook/zstd/blob/v1.5.7/lib/decompress/zstd_ddict.c)
and [legacy dispatch](https://github.com/facebook/zstd/blob/v1.5.7/lib/legacy/zstd_legacy.h).
Their copies in the locked registry source are the build inputs inspected here.

For this version, `ZSTD_estimateDCtxSize` returns `sizeof(ZSTD_DCtx)`.
`ZSTD_estimateDDictSize(..., ZSTD_dlm_byRef)` returns `sizeof(ZSTD_DDict)`;
the source dictionary remains borrowed. The static initialization APIs require
eight-byte alignment, never resize or allocate their workspace, and have no
corresponding free operation. One-shot `ZSTD_decompress_usingDDict` writes into
the supplied output, using these workspaces. Streaming window buffers and
internal dictionary creation are different APIs and are excluded.

The normal build enables legacy support, including versions v0.1–v0.7.
Those decoders cannot use a static context. Validate each member boundary before
calling the decoder so a legacy member, including one after a modern frame or a
skippable member, receives an explicit unsupported diagnostic. Do not reinterpret
the provider's generic allocation error as proof of query-budget denial.

## Ownership and failure contract

1. Preserve existing selected-payload admission and checked metadata extents.
   A selection with no compressed members does not allocate decoder state.
2. Validate modern/skippable member boundaries with the provider's frame-size
   parser after rejecting legacy magic. Reject malformed sizes or trailing bytes.
3. Check linked provider version, then request the measured context workspace and
   optional by-reference dictionary workspace from the same `HostAllocator`.
   Keep logical lengths plus the allocator's alignment allowance in the existing
   shared reservation accounting. Do not charge a second dictionary-content copy.
4. Check actual slice lengths and pointer alignment, initialize workspace bytes,
   and create private C pointers tied to Rust borrows. The wrapper is local to
   the synchronous decode and cannot outlive, resize, freeze or alias its backing
   workspaces or dictionary. No unsafe code enters workspace crates.
5. Decode each selected member into the remaining admitted output extent. Preserve
   checksums, dictionary semantics and exact returned-byte validation. C pointers
   never escape, and C free functions are never called for static state.
6. Drop both temporary owners on success or any error before returning retained
   output. Clones and slices of output keep only the payload/view/validity credits
   they actually retain. Concurrent calls share the existing grant and each owns
   its context; no global mutable state or decoder cache is added.

Inputs retain the existing known-content-size metadata contract. Valid modern
concatenations and skippable members remain accepted within that contract.
Legacy input is explicitly unsupported instead of using the previous dynamic
decoder. A linked version mismatch also fails deterministically. No fallback or
unaccounted retry is available for either case.

## Alternatives and boundaries

Reserving a guessed credit beside a dynamically allocated context cannot prove
that actual codec storage is admitted. Reporting `sizeof` after allocation is
too late. A custom C allocator callback would require a larger allocation/free,
reentrancy and failure-lifetime contract; fixed workspaces are sufficient for
this one-shot path. Replacing the Vortex encoding would risk native persistence
and direct-entrypoint coverage, so the existing concrete provider is retained.

This unit does not cover compression contexts, dictionary training, the inactive
experimental `ZstdBuffers` encoding, C call stacks, metadata container headers, other codecs or
allocations bypassing the Vortex hook. A memory grant still does not bound total
process RSS or prevent operating-system allocation failure. Operator spill,
resumable recovery, platform-wide support and package publication remain separate.

## Acceptance criteria

- Reproduce the old unaccounted-context behavior with a tiny decoded payload and
  a grant that fits that payload but cannot fit the context.
- Prove exact and one-byte-short grants, dictionary-stage denial, release after
  corruption/invalid dictionaries, overlapping calls and retained output.
- Cover empty/all-null selections, primitive extrema and floating-point bits,
  nullable Unicode/binary values, slices, scalar/append paths and serialization.
- Preserve modern concatenations and skippable members; reject legacy versions
  and malformed/truncated/trailing input without leaked credit or C fallback.
- Test workspace alignment/size refusal through a safe custom test allocator;
  inspect every unsafe call against its pinned contract.
- Compare frozen release executables through the existing complete native text
  lifecycle portfolio. Preserve all cells and samples, including Dictionary/FSST
  controls; investigate any Zstd cell over 10% in a reversed-order repeat.
  This is a resource-safety cost screen, not a speedup claim.
- Pass focused tests, required workspace/default/native gates, dependency policy,
  complete public/direct/format/batch workflows and retained-input Full43
  regression checks before claiming the finite unit complete.

The acceptance report must record source and binary identities, linked provider
version, the final allocation scope, any failed attempts and proof gaps. No
support label or broader PERF/CG gate closes solely from this design document.
