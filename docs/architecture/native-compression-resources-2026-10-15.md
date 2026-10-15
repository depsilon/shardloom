<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native compression resource admission — October 15, 2026

Status: local implementation and acceptance complete under PERF-03/06;
hosted integration remains pending. The [acceptance report](../benchmarks/native-compression-resources-2026-10-15.md)
records six broad gates, 72 vendor tests, 80 direct tests, 757 public cases,
one Full43 pass and 18 lifecycle cells within their frozen cost limits. The
portable packet preserves this contract's exact premeasurement text.
This source change follows the immutable 0.6.0 packages. It extends
[RFC 0044](../rfcs/0044-resident-runtime-resource-ownership.md#pinned-zstd-compression-and-training-admission).
The [phase plan](phased-execution-plan.md) remains the queue. This finite unit
does not close the [six-area completion contract](native-local-completion-scope-2026-10-07.md).

## Provider and decision

Classification: `use_vortex_native_provider`. Extend the existing
`vendor/vortex-zstd` 0.85.0 `ZstdData` primitive and variable-binary compression
entry points, which receive the caller's `ExecutionCtx`. Reuse its
`HostAllocator` and ShardLoom's shared memory grant. Canonical native arrays,
validity, frame boundaries, metadata identities and Vortex serialization stay
with their current provider. No alternate executor, encoding, Arrow execution
representation or frontend-specific compressor is introduced.

The locked graph is `zstd` 0.13.3, `zstd-safe` 7.2.4 and `zstd-sys`
2.0.16 with Zstandard 1.5.7. Check the linked version and default level 3,
including a same-version build with a changed default. Keep the safe Rust
compressor owner and default dictionary trainer. The pinned C source supplies memory estimators for
one-shot compression. Training's fixed sequential implementation supplies a
separate conservative bound. Both receive a reservation before any foreign
allocation, and keep it until their complete synchronous operation and cleanup
finish. Input preparation, output scratch and retained payloads use actual
allocator-backed buffers. There is no dummy buffer beside the C allocation.

The allocator's new external-reservation method returns an opaque lifetime
owner. ShardLoom binds it to `LiveMemoryPool`; allocator wrappers forward and
retain their existing owner. The ordinary Vortex default allocator explicitly
retains its unbounded behavior. Unknown custom allocators fail rather than
silently bypassing policy. Reservation denial keeps its original typed source.

## Compression bound

The authoritative [1.5.7 API contract](https://github.com/facebook/zstd/blob/v1.5.7/lib/zstd.h)
defines `ZSTD_estimateCCtxSize` for single-threaded one-shot `compress2` calls
at the selected level and all input sizes. Normalize level zero to the provider
default and clamp extrema to the same supported range used by its level setter,
before calling the estimator. This also avoids an unbounded estimator loop on
an arbitrary positive integer. No worker, long-distance matching, external
sequence producer, streaming buffer or advanced parameter is enabled.

For a trained dictionary, reserve the context bound plus
`ZSTD_estimateCDictSize(dictionary_length, level)`. The provider's
[local dictionary implementation](https://github.com/facebook/zstd/blob/v1.5.7/lib/compress/zstd_compress.c)
first copies the dictionary and later builds one prepared dictionary by
reference. The estimator's by-copy dictionary allowance covers the content
copy plus the prepared dictionary's workspace. Its parameter selection matches
the default local-dictionary creation path. Requested parameters are unchanged
across frames. Old compressor workspace is freed before replacement, so the
reservation covers one maximum workspace rather than accumulating frame costs.

Hold the reservation before creating the safe compressor and through its drop.
Reuse one admitted `compressBound(maximum_frame_bytes)` output scratch buffer;
copy each completed frame into exact-length admitted retained storage. Charge
the overlap. Compare actual context/dictionary size with the admitted bound
through the safe context introspection API in tests. This is supporting runtime
evidence; source and estimator contracts establish preallocation admission.

## Dictionary training bound

The pinned [default trainer](https://github.com/facebook/zstd/blob/v1.5.7/lib/dictBuilder/zdict.c)
uses [FASTCOVER](https://github.com/facebook/zstd/blob/v1.5.7/lib/dictBuilder/fastcover.c)
with `d=8`, four steps, frequency log 20, acceleration 1, split 0.75,
zero worker threads, no shrinking and compression level 3. Let `N` be the
sample count, `D` the requested dictionary capacity and `M` the largest sample.
Use checked arithmetic for the following simultaneous requested-heap bound:

- `(N + 1) * sizeof(size_t)` sample offsets.
- `10 * 2^20` bytes for the persistent U32 frequency table, trial U16 segment
  frequencies and trial U32 frequency copy.
- `4 * D` for the trial, two selection buffers and retained best dictionary.
- The C-layout size of two pointers, `size_t` and `ZDICT_cover_params_t` for
  the sequential trial argument.
- One context and one by-copy prepared dictionary, using the componentwise
  level-3 envelope: window log 21, chain log 16, hash log 17, search log 2,
  minimum match 4, target length 0 and `dfast` strategy.
- 128 KiB entropy output plus `compressBound(M)` trial output.

All four default level-3 parameter rows use `dfast`; size adjustment reduces
their table dimensions. Entropy finalization and trial scoring are sequential
and use the same unbuffered context estimator. The dictionary begins with
dedicated dictionary search disabled. The optional window adjustment is at
most 19 bits, within the envelope. Workspace replacement frees the old block
first. These properties bound the nested codec allocations without changing
the training algorithm. Counting both output buffers is conservative.

Check every sample extent and its total against the input slice, and leave
room for the C `nbSamples + 1` expression. Preserve FASTCOVER's input-size
domain: at least 8 bytes and strictly below `u32::MAX` on 64-bit targets or
1 GiB on smaller targets. Also require at least five training samples, a test
sample and at least eight bytes in the first `floor(3 * N / 4)` samples. The
pinned trainer subtracts eight from that partition's length after checking
only the total input size; reject a shorter partition before entering C. The
explicit no-dictionary entry point remains available for those tiny frames.
Hold the foreign reservation through the entire
safe training call, including failure. The admitted dictionary output and
sample-length buffers are separate owners and are not counted twice.

## Scope, alternatives and risks

The reservation approach preserves the existing safe compressor ownership and
by-copy loading. A static compressor rejects that dictionary loader. Custom
allocation callbacks would add pointer ownership, reentrancy, panic containment
and cleanup obligations without being required for this bounded provider.
Neither alternative is part of this implementation. Pure estimator/default queries stay
inside the existing excluded vendor patch; workspace crates remain safe Rust.

Conservative admission can reject a small compression at a high level even
when its eventual C state would be smaller. That tradeoff must be explicit and
checked against useful complete workloads. Credits cover requested payload and
foreign heap bytes, not process RSS, allocator metadata, C call stacks, OS
allocation failure or other codecs. Frame-handle and protobuf metadata vectors,
their infallible clones and imported arrays retain separate allocation owners.
The inactive experimental `ZstdBuffers` encoding is outside this unit.

## Acceptance

The frozen lifecycle screen compares the same optimized harness on the
pre-change `e8611a3c` source and the implementation. Its 18 cells combine
nullable Int64, UTF-8 and binary inputs, 4,096/65,536 rows, and canonical,
Zstd-without-dictionary and Zstd-with-dictionary encodings. Frame targets are
128/1,024 valid values respectively, the level remains zero and each producer
or consumer receives a 64-MiB grant. Each cell has one recorded warmup and
three samples of 25 complete operations. Fixtures and reference values are
constructed outside the operation clock. Session creation, encoding/training,
native message serialization and hashing, producer closure, fresh-session
reopen/decode, every-value/null verification and cleanup are inside it. This
is in-memory native serialization; separate public UAT covers file workflows.

Compare each cell's median sample total. An increase exceeding both 10% and
100 microseconds per operation triggers a complete reverse-order comparison.
The same repeated breach blocks retention for diagnosis. A native process CPU
increase exceeding both 10% and one second, or peak RSS exceeding both 25% and
64 MiB, also triggers that reverse-order comparison. Preserve all warmups,
samples, failures, source/executable identities and host observations. Compare
serialized hashes and sizes exactly between implementations; record admitted
peaks separately because the new code covers previously unaccounted storage.
No cache flush, pacing or battery/thermal admission is part of the protocol.

- Exact primitive bits, nullable Unicode/binary values, slices, empty/all-null
  input, dictionary and non-dictionary frames round-trip through native decode
  and file serialization. Preserve unchanged compressed bytes where framing is
  unchanged; record any justified framing correction separately.
- Deny each input, offset, sample, output and foreign-reservation stage before
  allocation. Test exact grants, competing owners, cleanup after errors and
  retention after the session closes. Check the original typed error survives.
- Test integer overflow, sample extents and foreign-reservation refusal by
  custom allocators. Preserve the provider's default training failure; do not
  retry without a dictionary when training fails.
- Freeze sources and executable identities for the native compression lifecycle
  cost screen. Compare complete operations and output sizes, retaining all
  samples and failures. Investigate a repeated cost regression before retention.
- Pass focused ownership/value checks, default and native Rust gates, complete
  public/direct/format/streaming workflows and one full 43-query pass for the
  retained implementation. No artificial pacing or repeated Full43 is required.
- Update provider provenance, support boundaries and the completed ledger only
  after actual local and hosted acceptance. No speedup or broad support claim
  follows from this contract; `fallback_attempted=false` and
  `external_engine_invoked=false` remain required.
