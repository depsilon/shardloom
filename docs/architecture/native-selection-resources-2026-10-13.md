<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Zstd Selection Resources — October 13, 2026

Status: local correctness, resource, cost and portable-evidence acceptance pass;
hosted integration is pending. The [acceptance report](../benchmarks/native-selection-resources-2026-10-13.md)
records the repaired candidate and both rejected cost cohorts under PERF-03/06/08/09
and CG-5/19/20/21, following RFCs 0014/0044. This finite unit follows the accepted
sparse text-frame optimization and preserves its shape policy and native decoder.

## Decision and provider check

Classification: `use_vortex_native_provider`. The pinned Vortex 0.85 array
session already supports execution-parent kernels with an `ExecutionCtx`.
ShardLoom already supplies a `LiveMemoryPool`, fallible host-buffer allocator
and buffer-retained metadata credits. Reuse those surfaces for sparse Zstd
selection instead of allocating frame lists and lazy mask caches in a
context-free `FilterReduce` rule.

Move the selection into the admitted session provider. Reserve temporary frame
and metadata collections before cloning them; construct selected row indices
through the existing allocator while reading the input mask's bits directly.
Keep only the touched compressed frames, using the same dictionary and unchanged
frame metadata. Decode through the existing Zstd provider, then gather native
binary views into an admitted output buffer. Temporary selection state must not
escape. Output buffers retain their own payload and applicable metadata credits.

Ordinary Zstd slices should borrow their contiguous frame range rather than
allocate a temporary frame-reference vector. Compute the two validity ranks
without creating a vector or populating a shared selection cache. No new encoding,
allocator, executor, public query option or dependency is introduced.

Aggregate UTF-8 canonicalization, native dictionary values and dictionary
preparation must retain the caller's existing execution context/session. The
first cost screen found legacy-session creation inside these helpers, bypassing
the installed selection kernel. Propagate the already admitted context through
decoding and validity work; keep context-free compatibility entrypoints explicit.
This extends no provider or datatype policy and does not claim complete coverage
of other legacy-session helpers or dictionary interning metadata.

## Contracts and boundaries

- Preserve UTF-8/binary dtype, source order, null behavior, native serialization
  and source owners. The existing sparse policy admits 2–64 frames, at most
  4,096 selected rows, density at most one eighth, and all-valid inputs.
- Preserve existing native execution for other shapes. There is no external
  executor, fallback attempt or newly admitted operator/type combination.
- Denial happens before each covered allocation. Every temporary credit drops
  on refusal, corruption or successful return; surviving output buffers, slices
  and clones retain the corresponding result credits independently of the input.
- Filter construction must not populate a mask's lazy index/range caches or
  allocate a second compressed-frame collection before execution admission.
- This is bounded provider accounting, not a process-RSS ceiling. General
  child-decoder scratch, compression/training, unrelated selection providers,
  allocator bookkeeping and other unreviewed metadata remain separately owned
  work. It does not close a complete PERF or CG gate.

The alternative of retaining the optimizer rule and reserving after reduction
is too late. Removing sparse frame skipping would discard an accepted useful
optimization. A new allocator protocol is unnecessary because the existing
session kernel and retained-buffer owners can express this finite lifetime.

## Acceptance

Prove construction-time cache avoidance, pre-allocation denial, exact and
one-byte-short grants, retry and full release. Preserve sliced/unsliced,
all-valid/nullable, empty/dense, Unicode/binary, dictionary and malformed-frame
controls. Verify sparse decode still skips unused frame payload and compare
every returned value. Exercise serialization and retained source/output owners.

Run focused provider tests, default workspace formatting/Clippy/tests and native
feature checks, then complete public/direct/streaming regressions and retained
Full43. Use frozen baseline/candidate executables for a complete-operation cost
screen of the existing sparse-text target and controls; investigate a regression
over 10% with reversed order. This establishes the cost of resource admission,
not a new speedup claim. Preserve original failures, source identities and
portable evidence before hosted integration. Version 0.5.1 stays fixed.
