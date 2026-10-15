<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Fixed-Width Decoder Scratch — October 14, 2026

Status: revised implementation passes local resource, cost, public-workflow and
Full43 acceptance, with [portable evidence](../benchmarks/evidence/native-decoder-scratch-2026-10-14.json)
complete. [Combined helper-context acceptance](../benchmarks/native-admitted-context-2026-10-14.md)
also passes, including all 516 matched query calls. Hosted integration remains
pending under PERF-03/06/08/09 and CG-5/19/20/21, following RFCs 0014/0044. This continues
the existing builder resource boundary and does not close general provider
memory accounting. Version 0.5.1 remains immutable; the single authorized
version bump follows integration of this work and the context correction.

## Provider decision

Classification: `use_vortex_native_provider`. Pinned Vortex 0.85 already owns
physical decoding, native append strategies, typed buffers and execution
contexts. Extend the existing `shardloom-vortex` Chunked wrapper and query-owned
`LiveMemoryPool`; no new provider, allocator, dependency or external executor.
The expert comparator is an encoded-columnar maintainer checking allocation
lifetimes, complete byte extents, work avoidance and failure cleanup.

Keep each leaf's natural `append_to_builder` call. A BitPacked leaf writes into
the final builder; primitive Patched appends its inner encoding and applies
patches to the appended range. Unsigned FoR over BitPacked uses native fused
decode. Pre-canonicalizing every child would discard these paths. The pinned
`builder_with_capacity_in` currently ignores its allocator argument, so changing
only that caller would not account for child allocations.

## Finite accounting contract

Walk nested Chunked inputs in their existing order, using the same final builder.
Before a reviewed leaf appends, reserve a conservative sum of its potentially
live native byte buffers and the specifically reviewed temporary collections.
Keep that credit until append returns, including errors. Values and validity are
copied into the already owned final builder; temporary credits must not escape.
Preserve output credits through independent buffers, clones and slices.

Review Primitive, Bool, Decimal and Constant leaves and physical trees of
BitPacked, Delta, FoR, RLE, ZigZag, RunEnd, Sequence, ALP, ALPRD and primitive
Patched. Include non-null Sparse and dictionary gather from canonical non-null
values with canonical non-null primitive codes. Require host storage and trivial
or canonical Boolean validity wherever the estimate relies on borrowing masks.
Physical children must satisfy the same closed-tree rules; unsupported trees
receive no new scratch-accounting claim. Existing output-only admission and
native behavior remain in force for those trees.

The estimator must distinguish canonical execution from direct append. Derive
each requested capacity from the pinned constructor and actual physical extent:

- Include preferred or carried alignment, padded Delta/RLE extents, builder
  finalization replacements and possible copy-on-write overlap.
- Preserve BitPacked direct append and Patched inner append without reserving
  a nonexistent full decoded child. For FoR fusion, include its one native
  temporary builder and finish replacement, plus patch children.
- Include ALPRD's mutable left parts and right/output buffer, ALP patch branches,
  native patch metadata decoding and sliced Sparse index normalization when
  the stored indices are canonical primitives.
- Include RLE's trusted-length offset collection. Canonical Boolean dictionary
  take includes its small-source byte-per-Boolean collection and word-rounded
  packed output. Do not use logical `ceil(rows/8)` for word-allocated storage.
- Reuse existing Zstd allocator ownership without asserting transitive coverage
  of an unreviewed parent around it or charging its buffers twice.

The pinned default buffer alignment is 256 bytes. The existing FSST integer
metadata estimate must also include the empty replacement allocated when a
BitPacked `PrimitiveBuilder` finishes. Prove this with malformed metadata and a
grant that covered the former estimate: denial must precede later payload
validation, and retry must reach the expected validation error without leaking.

Collection review uses Rust's
[trusted-length collection specialization](https://doc.rust-lang.org/src/alloc/vec/spec_from_iter_nested.rs.html)
and [small-vector capacity rule](https://doc.rust-lang.org/src/alloc/raw_vec/mod.rs.html),
alongside the pinned iterator implementations. This bounds requested capacities,
not allocator bookkeeping or possible allocator over-allocation. Source and
toolchain assumptions require regression checks on supported hosted platforms.

## Exclusions and proof

Arbitrary ScalarFn/Filter/Slice rewrites, general encoded dictionary values or
codes, nullable dictionary take/fill/AND chains, nullable Sparse/patch validity,
encoded validity and nested physical-to-Chunked intermediates require their own
closed allocation/lifetime review. A missing estimate is not a zero-byte proof.
Nullable Delta's transposed validity is one such exclusion. A nonzero Sparse
offset over encoded indices is also excluded because its subtraction can enter
logical parent rewrites. Trivial BitPacked validity masks are borrowed metadata;
they do not require a decoded constant bitmap allocation.
Compression/training, structural array objects, stack scratch, allocator metadata,
Python/conversion retention and total process RSS remain outside this byte-buffer
contract. Do not fill persistent mask index/range caches inside a temporary lease.
Depth/shape exclusions must preserve existing supported execution.

Acceptance requires complete decoded references, empty/null/offset/padded/strong
alignment cases, exact and one-byte-short grants, late errors, retry, concurrent
pressure and final-owner release. Add direct-append and FoR-fusion controls that
would fail if an unnecessary full child buffer were introduced. Verify explicit
exclusions without presenting them as accounted scratch.

Run focused tests, required workspace formatting/Clippy/tests, native feature
gates, public/direct/streaming regression and retained Full43. Freeze baseline,
candidate, workloads and cost criteria before timing complete operations that
exercise the changed routes. Preserve failures and source identities, perform
adversarial source/evidence review and complete hosted integration before closure.
No speed, general resource, platform or production-readiness claim follows from
this design. Vortex stays native input/output and fallback remains disabled.

## Cost protocol fixed before timing

Compare accepted production commit `22b2cde04973ce4462517d73fe67b5f7a1e8177c`
with the candidate using the same compiled fixtures and full-value verifier.
Both variants install ShardLoom provider admission. Freeze every relevant source,
the protocol and helpers before release builds; restore only the two changed
production files for the baseline build, then restore and hash-check all candidate
bytes before its build. The new scratch module has no inclusion hook in the
baseline. Preserve both executable identities and the exact build receipts.

The screen covers 18 shapes at 32, 4,096 and 65,536 rows, with 2 and 16 chunks:
108 cells. Include canonical/nullable Int64 and nullable UTF-8 controls, constants,
BitPacked, unsigned FoR over BitPacked, Delta, RLE, ZigZag, RunEnd, Sequence,
patched ALP/ALPRD, primitive Patched, non-null Sparse, and canonical-value
Boolean/Decimal/Int64 dictionaries. Each cell has nine alternating baseline and
candidate pairs. Every sample starts a fresh process, warms eight complete calls,
then times 1,024/128/16 complete calls respectively. Check all scalar values before
and after each batch and require final resource credits to return to zero.

The timed batch includes native Chunked execution, result shape checks and
destruction. Fixture/context construction and full-value checks are excluded;
separate whole-process CPU/RSS observations include them. Record ordinary host
activity and uncontrolled OS cache. Guard every build/cohort sequentially, with
12 GiB disk headroom, 100 GiB workspace, 192 MiB cohort logs, a 60-second
sample deadline and a 40-minute cohort deadline.

Use the existing builder retention rule: a cell's candidate/baseline median
ratio above 1.10 requires a fresh full cohort with reversed starting order.
Revision is required if the same cell exceeds both 1.10 and 5,000 added
nanoseconds per call in both orders. Keep every sample and failed observation.
This finite native-array screen does not replace public query/regression evidence
or establish negligible overhead. The separate FSST correction requires its
targeted failure/credit proof and wider acceptance.

## First candidate and inspection revision

The first candidate completes both orders: 3,888 fresh-process samples across
the same 108 cells, with complete values and zero final temporary credits. The
forward screen triggers confirmation in 82 cells; 13 exceed both cost thresholds,
and 12 repeat that material regression in reverse order. Those observations
reject the first candidate for acceptance. Preserve its source snapshots,
executables, raw samples, original launch failure and decision.

Source inspection identifies repeated work in the new estimator: each recursive
`workspace` call invokes Vortex's recursive `is_host`, then visits those children
again to calculate capacities. Canonical child alignment also collects a fresh
buffer vector. The revision checks each node's own buffer handles during the
existing closed-tree walk and borrows primitive/decimal handles directly.
Boolean inspection uses its native vtable buffer accessor. Validity inspection
still covers the BitPacked validity slot skipped by the capacity walk.

Capacity formulas, temporary lease lifetimes, final-buffer ownership, exclusions
and the cost rule remain unchanged. Device-buffer regression fixtures cover
canonical values, an encoded child, packed physical storage and validity; any
attempted device copy fails the test. This is an inspection-cost correction,
with a fresh source/build freeze and full cost comparison before broader
acceptance.

## Revised local acceptance

The revised 2,244-file source passes all 3,888 cost samples: 1,944 samples over
108 cells in each starting order. The first order has 55 relative-only triggers
and the reverse order has 54; neither has a cell exceeding both the 10% and
5,000-nanosecond limits. Independent recomputation checks every sample and its
log. The original rejected candidate and unchanged scoring contract remain
preserved. This is finite admission-cost evidence, not a speedup claim.

All six broad gates pass, including 3,219 workspace tests and 4,178 native
CLI/Vortex tests. The native run records 28 manual/platform exclusions separately.
All 80 direct binding tests and 757 public cases across 19 families pass. The
public evidence is reopened, including 22,002 expected-value rows, 677,251
pressure-test rows and 1,062 execution envelopes.

Retained-input Full43 passes all 129 calls and 1,143 returned rows. Complete
native processes total 229.881861 seconds; supervised suite duration is
301.007832 seconds. The sum of per-query minima is 71.766618 seconds and the sum
of medians is 76.788967 seconds. These clocks remain separate and unpaired;
they do not explain the earlier composition timing difference. The
[acceptance report](../benchmarks/native-decoder-scratch-2026-10-14.md) records
the source, scope and remaining integration work.
