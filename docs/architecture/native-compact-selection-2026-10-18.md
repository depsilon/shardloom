# Native relational selection experiment

Status: candidate passes the frozen resource screen; broader acceptance is
pending. The assessment protocol below was frozen before the runtime candidate. This is one
bounded `PERF-10.compact_selections` experiment under RFC 0044, with PERF-03
resource ownership and PERF-01/12 attribution and acceptance. It does not close
those owners or CG-5/6; all CG-1 through CG-23 remain in the phase plan.

## Production opportunity and provider check

At `61afe0f5`, `local_primitive_relational_scan::run` reserves `array.len()`
`usize` ordinals for every admitted batch, including scans without a residual
predicate. `run_transform` does the same for expression filters. `select_batch`
then rediscovers contiguous rows and uses a native slice; other selections are
copied into an allocator-owned U64 take array. An 8,192-row scan therefore
reserves 65,536 ordinal bytes on this 64-bit target even when every row passes.
This is source-derived allocation accounting, pending complete-operation
measurement below, not a CPU hotspot or speedup claim.

The alternative B assessment found that the admitted Zstd compressor and decoder
already reuse their mutable context across frames within one operation.
`decoder_workspace::decompress_frames` prepares one dictionary for that sequence,
then releases it before output return. Reuse across slices would extend an
established ownership boundary; repeated preparation cost has not been measured.
Package B remains pending. Its absence of a measured cost signal does not imply
that preparation is free or that it should be dropped.

Vortex-first classification: **use_vortex_native_provider**. Pinned Vortex 0.85
already provides native array slices, typed take indices and membership masks.
Keep its slice/take providers behind `vortex-local-primitives` and the existing
native relational certificate. The proposed small local descriptor tracks an
increasing contiguous range until a gap requires the existing admitted ordinal
container. It is construction/admission state, not a new array encoding.
Retain the native slice through downstream count/projection consumers. For a
mixed selection, form the existing take indices once per delivered batch.

`Mask::from_buffer` can retain allocator-owned bits, but generic filter kernels
may call `indices`, `slices` or `threshold_iter`, creating shared lazy Vec caches
outside that allocator. Introducing that provider surface is a separate resource
audit. Ordered duplicate gathers remain explicit indices: membership cannot
represent `17, 3, 17, 8`. Global ordinals remain checked `usize`/U64; no local
region width becomes a dataset-size limit. No dependency, format, public API,
external executor, Arrow execution substrate or new datatype is introduced.

## Frozen assessment and acceptance

Baseline: `61afe0f5` plus the identical measurement-only test. Candidate changes
only the shared scan/filter selection construction and its native application.
Freeze each release test executable and source diff before measuring it. Build
with two Cargo jobs, incremental/debug information disabled, under the existing
local storage and workload guards; never overlap native jobs. Keep the original
baseline and every failed observation.

`native_selection_complete_cost_screen` runs fresh complete native-memory
intake, plan binding, execution, complete value validation and cleanup for 16,387
rows. Seven shapes (unfiltered, all, none, contiguous band, sparse, dense and
nullable membership) each feed two actual consumers: native projection and
native count. Repeat each cell three times in one sequential process. Record
first and subsequent observations, shared-owner peak bytes, retained bytes and
complete lifecycle timings. These diagnostic lifecycle clocks are not the
engine-owned Full43 clock. Correctness compares every projected value or the
exact count; certificates must remain certified with no fallback.

Select A only if its measured complete-operation peaks establish the proposed
allocation opportunity. Retain a candidate only if both all-pass consumers save
at least 32 KiB and 10% of baseline peak shared-owner bytes, all shapes preserve
values/order/NULL semantics and cleanup, and no cell gains more than 32 KiB peak.
This resource objective does not claim process-RSS coverage or a 2% engine win.
Investigate a median complete-lifecycle regression exceeding both 5% and 100 us
per operation with one reversed-order paired cohort of the frozen binaries;
drop the candidate if that regression repeats. Preserve every sample.

Behavioral tests must cover empty/all/none, sparse/dense transitions, sliced
bases, large global positions, duplicate/reordered gather controls, nullable
predicates/payloads, composed filters, cancellation, allocation denial and
last-owner release. Run the existing selection/typed/nested/relational
regressions, native-feature checks and required workspace fmt/Clippy/tests.
At the coherent batch boundary, run complete public/direct/streaming regression
and retained Full43 acceptance through the existing guarded runners. No new
ingestion is needed for an unchanged physical format. Keep version 0.6.0 and the
release hold unchanged; performance and release claims remain gated.

## Assessment observation and selected experiment

The frozen baseline screen passes all 42 complete-operation checks. All three
observations in each cell have the same shared-owner peak. Unfiltered projection
and count peak at 326,797 and 364,813 bytes; all-pass filtering followed by those
consumers peaks at 359,565 and 397,581 bytes. These include intake, binding,
native execution and output consumption. They are not isolated descriptor byte
measurements. The source audit identifies the unconditional 65,536-byte scan
ordinal reservation and an additional 16,384-byte filter ordinal reservation per
full output batch as concrete removable owners. This selects A for the bounded
experiment; it does not establish comparative CPU cost against package B.

The candidate retains a range through scan delivery and filter output, including
downstream native count and projection. At the first membership gap it admits
one ordinal buffer for the maximum remaining selection, no larger than the old
full-input buffer. This avoids introducing growth-overlap memory denial for dense
cases. The native take payload and ordered/repeated gather semantics remain as
before. A bitmap or narrower sparse-index representation is not claimed.

Baseline executable: `d104a45561db3512a5ddfcbe814249a6f8e0fcf7b8ba87e0ea42299915585edd`.
The frozen measurement source hash is
`a2b7f2b22ca0360c0ca53972046d6a0858ade192fff0863131cc00c7a429f1e5`.
The original pre-measurement protocol and complete source manifest accompany the
baseline receipt; this observation supplements rather than rewrites that freeze.

## Frozen candidate screen

The candidate passes all 42 complete-operation checks against the baseline's 42.
The two all-pass targets exceed the predeclared 32-KiB and 10% memory thresholds:

| Complete operation | Baseline peak bytes | Candidate peak bytes | Reduction |
| --- | ---: | ---: | ---: |
| Unfiltered projection | 326,797 | 261,261 | 20.05% |
| Unfiltered count | 364,813 | 299,277 | 17.96% |
| All-pass filter and projection | 359,565 | 278,287 | 22.60% |
| All-pass filter and count | 397,581 | 315,661 | 20.60% |

All fourteen cells reduce peak shared-owner bytes and release every reservation
after cleanup. None triggers the frozen complete-lifecycle regression condition,
so no reversed-order confirmation is required. These are finite resource and
throughput controls, not an engine-wide speedup or process-RSS claim.

The frozen candidate executable is
`de786f4ff841d9c7cf6bf32063b5efaba73472fbe369ea11c7fde4824129c939`.
The executed measurement source is unchanged: the candidate adds only two scoped
lint attributes, with a separately verified source bridge. The baseline,
candidate, complete observations, source manifests and failed development checks
are retained together. Broad native/public/workflow and Full43 acceptance must
complete before this finite change is recorded as accepted.
