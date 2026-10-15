<!-- SPDX-License-Identifier: Apache-2.0 -->

# Selected text views inside native Zstd filtering

Status: the frozen targeted comparison retains the candidate; all seven
Rust/provider gates, 80 direct tests, 757 public cases and one Full43 pass succeed.
The portable packet verifies all 2,305 members. Hosted acceptance remains pending.
See the [bounded acceptance report](../benchmarks/native-selected-text-views-2026-10-15.md).
This work remains under PERF-01/04/05 and the existing PERF-03/06 ownership boundary.
The compression/timing integration in
PR #1555 is complete in its [separate hosted acceptance unit](../benchmarks/evidence/native-compression-hosted-2026-10-15.json).
This candidate follows the
October 15 hotspot review; it does not close the six-area completion contract.

## Measured reason and provider decision

The [fresh single-pass UAT](../benchmarks/native-query-timing-uat-2026-10-15.md)
records Q23 engine execution at 3.671472292 seconds. Its caller reports
3.014874808 seconds constructing accessors, including 2.980844037 seconds in
native text execution and 0.005445529 seconds building selected dictionaries.
These are nested elapsed spans, not independent CPU measurements. A subsequent
diagnostic stack sample of the same native executable finds the admitted sparse
Zstd filter executing the retained frames as a full text-view array before
gathering selected views. Profiled durations receive no benchmark credit.

Classification: `use_vortex_native_provider`. The pinned Vortex 0.85 provider
already owns Zstd frame decoding, length-prefixed values, native views and the
fallible session allocator. Its [upstream array implementation](https://raw.githubusercontent.com/vortex-data/vortex/0.85.0/encodings/zstd/src/array.rs)
constructs views for the decoded extent; the published registry archive remains
the provenance authority recorded in `vendor/vortex-zstd/upstream-provenance.json`.
Extend that existing decoder boundary. No alternate encoding, allocator,
executor, query-specific route or frontend is added.

The October 13 [one-pass full-view experiment](../benchmarks/native-query-revisit-2026-10-12.md#october-13-zstd-view-reconstruction-dropped)
was dropped and stays dropped. This candidate changes the amount of output
constructed for a sparse selection. Unfiltered and dense decoding retain their
accepted implementation; the proposal does not reinstate that earlier rewrite.

## Reuse and finite contract

- Keep `native_provider_zstd_filter::prepare`, its 2–64-frame admission,
  4,096-row/one-eighth selectivity limits, all-valid boundary, frame pruning,
  remapped indices and source-owner lifetime.
- Reuse `ZstdData::decompress_slice` and its admitted decoder workspace and
  payload storage. Compressed bytes and the set of decoded frames are unchanged.
- Walk the retained length-prefixed values with checked bounds and counts.
  Construct an allocator-owned `BinaryView` only for an admitted selected row;
  preserve input order and exact UTF-8/binary bytes. Validate ascending, unique,
  in-range indices before decoding. Preserve deterministic failure for malformed
  lengths, row counts and buffer extents.
- Reuse the existing native view representation and bounded buffer-handle
  accounting. Attach output collection credit to independently retained views
  and data buffers without a second selected-view copy. Hold source metadata
  until decoding, output validation and ownership transfer finish.
- Preserve all existing unsupported-shape behavior. Null-bearing arrays,
  dense/full masks, primitive inputs and other encodings receive no new path.
  All user surfaces inherit the change through their shared native provider.

ShardLoom technique review: preserve encoded frame pruning and selected-row
materialization; use the current shared grant and native frame work units.
PulseWeave scheduling, CPU admission and provider worker counts are unchanged.
Separate engine, output, process and validation clocks. Source-bound evidence
must distinguish fewer constructed views from fewer decoded bytes.

The expert comparator is an encoded-columnar maintainer checking exact value
semantics, untrusted length/count arithmetic, allocator refusal, independently
retained buffers and complete-operation benefit.

## Acceptance and fixed comparison rule

First retain a regression that fails on the baseline because sparse filtering
allocates views for unselected rows. Cover dictionary/no-dictionary frames,
UTF-8/binary, inline/outlined values, empty/all-valid/null-bearing inputs,
nested slices and selection thresholds. Exercise malformed lengths and counts,
every changed allocation denial, retry, exact ownership release and source-owner
survival. Use decoded references only in tests. No new unsafe code is authorized.

Before candidate timing freeze the baseline and candidate source manifests,
build receipts, executable hashes, complete query/reference file hashes and
input generation. The baseline is PR #1555's native source at `419ac33f`;
its frozen timing-enabled CLI is reused only after source equality verification.
New candidate builds must prove that the changed provider was recompiled.

The targeted ship/drop comparison uses ten alternating pairs for Q23 with
requested parallelism 12 and five pairs each for Q23 at parallelism 1 and
Q22/Q19/Q29/Q2 at parallelism 12. Use the same 99,997,497-row native input,
24-GiB admission policy and complete public CLI calls. These targeted comparisons
are separate from the final Full43 UAT, which runs once per query.

Engine execution is primary: require a lower target median, positive median
paired saving and improvement in at least nine of ten pairs, with no minimum
percentage gain. Target median process CPU may rise by at most 5%. Each control
engine median may rise by at most the greater of 5% or 5 ms; each case's median
peak RSS may rise by at most the greater of 25% or 256 MiB. Keep complete process
scores secondary and record output, remaining process work, validation and
supervision separately. Retain every sample; do not replace failures or adjust
thresholds after measurement. A failed gate drops the performance candidate.

Serialize builds, tests and measurements under existing workload, storage and
process-group guards. Record ordinary host activity and uncontrolled cache state;
do not add pacing or battery/thermal admission. Require all complete values,
schemas, source generations, resource grants and false fallback fields to match.
Run focused native tests before the comparison; run required workspace/native
gates, public/direct workflows and single-pass Full43 for retained code at the
end of the cohesive batch. Document the decision, update README/field guide as
applicable, and complete hosted review/CI/integration before ledger closure.

No speed, zero-decode, whole-process memory cap, release or production-readiness
claim follows from this design or from diagnostic sampling.
