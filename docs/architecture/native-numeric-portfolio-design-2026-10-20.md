<!-- SPDX-License-Identifier: Apache-2.0 -->

# Numeric compression portfolio investigation

Status: prospective finite experiment for `PERF-09.portfolio`, conditional row
11 under RFC 0044. No profile is admitted or retained. The
[conditional sequence](native-conditional-investigations-2026-10-19.md) remains
ahead of the 37 required substeps. The unresolved pre-writer measurement
prerequisite is carried forward separately. All broader owners and CG-1 through
CG-23 retain their gates.

## Opportunity and provider decision

The accepted October 18 full ingestion records 255,840 numeric compression
calls and 26,699,331,699 input bytes. Its 72,287,473,433 summed elapsed
nanoseconds describe overlapping codec spans, not CPU, a critical path or
recoverable time. Exact source equality ties that observation's numeric adapter,
stage timers, dependency lock and writer assembly to the current implementation.
This establishes a real caller and cost to investigate; it does not establish
that removing any compression choice helps.

Vortex-first classification: `use_vortex_native_provider`. Keep pinned Vortex
0.85.0 `BtrBlocksCompressorBuilder`, `NumericDataStrategy`, the production fast
load table strategy, bounded input ownership, edition admission and native
file/scan providers. The current numeric adapter runs after coalescing, excludes
integer/float dictionary selection through its output encoding allowlist, and
preserves already encoded arrays when their complete trees are admitted.

Compare exactly two profiles:

- `current`: the existing default builder followed by the existing edition and
  dictionary filtering.
- `reduced_numeric`: the same builder's `only_cuda_compatible()` preset followed
  by the same filtering, only inside the existing numeric data fallback. In the
  pinned provider this removes integer sparse/RLE and floating ALPRD/sparse/RLE
  choices. It also excludes optional Pco/delta choices when compiled, but those
  are absent from this default numeric scheme set. String/binary preset changes
  cannot reach this primitive-only boundary.

The provider method's name does not establish GPU use or support. All execution
here is on CPU. Do not add an encoding, provider fork or direct BtrBlocks
dependency. Dictionary codes/values, text, validity, statistics, encoded-array
preservation, source preparation and writer topology keep their existing paths.
The earlier five rejected Zstd text profiles are not repeated.

The proposed advantage is less codec search/encoding CPU. A possible cost is a
larger artifact or slower native consumers. Those costs are measured, not
inferred from array memory estimates. This is one global numeric profile
comparison; it does not select a favorable profile separately for each column
after seeing results or introduce field-name-specific production rules.

## Instrumentation and source boundaries

The profile is selected by a scoped test-only configuration captured when the
existing compressor is constructed. Worker calls carry that immutable captured
choice. Normal library builds have no profile switch. A per-column writer uses
the actual table factory with its single field; a whole-table writer does not
construct one compressor per field merely to attribute costs.

Test-only CPU clocks use the already locked `rustix` 1.1.4 safe `time` API on
macOS/Linux. Its Apache-2.0/MIT licensing is compatible; no unsafe application
code or runtime dependency is added. Record clock resolution and monotonic
sample checks before the screen. Thread CPU surrounds actual synchronous
numeric compression and native primitive canonicalization. These include their
local preparation/materialization work, not merely an inner codec instruction.
Process CPU separately surrounds complete column writing and file open/read/
canonicalization. Elapsed intervals remain separate. Do not label elapsed spans
as CPU, sum CPU as elapsed time, or claim a Full43 engine result from this screen.

Both profile arms have the same CPU instrumentation. Complete column cost
includes the actual strategy construction, encoding, statistics, serialization,
file writing, flush and close. Numeric CPU is subordinate attribution; a codec
CPU improvement alone cannot pass the complete writer cost gate. Source read
and Arrow-to-native input preparation are separately recorded and unchanged.

Read the first 32,768 rows of each of Parquet row groups 0, 113 and 225 from the
resident official source. Select every top-level source field whose actual
Arrow type is an integer or floating primitive, in original schema order. The
source has 105 fields and the existing source inventory identifies 77 primitive
numeric fields; deterministic preflight must confirm the complete field/type/
nullability list before measurement. Do not include text or synthesized derived
columns in the per-column screen. Whole-workflow admission later includes them.

Each producer invocation independently opens the source and prepares a fresh
batch with the current native owned-copy conversion. No compressed arrays,
statistics caches, contexts or prepared compressors cross profile invocations.
Each column is written as a separate one-field native file through the retained
262,144-row / 8-MiB table settings and a single current-thread driver. This
preserves the real source-batch numeric leaf shape, but is not a measurement of
the concurrent full table or full ingestion.

Keep a 512-MiB native memory grant and a 64-MiB per-column output cap. At most one
source batch and one active column writer exist in a producer. Temporary files
for one 77-column batch are capped in aggregate at 256 MiB and are retired after
the independent verifier completes. Source-reader, filesystem and diagnostic
allocations outside the native pool remain explicit; the grant is not an RSS
ceiling. Keep process, workspace, diagnostic and free-space guards unchanged.

## Complete verification and native consumers

After each producer exits, a fresh process independently reopens the source
prefix and every written native file. The producer is closed before readback;
the verifier retains no producer arrays or compression contexts. Compare every
ordered value and null to the Arrow source directly, without first converting
the reference through the writer's native import. Check integer extremes and
floating bit patterns in the synthetic regression cases. Preserve dtype,
nullability, field name, row count and complete per-column content digests.

Record full file bytes and `Footer::compressed_field_sizes()` values separately.
The provider attributes each segment once at its deepest common field owner;
field totals exclude footer, alignment and file metadata. Validate segment
offsets/lengths against the physical file and record encoding trees. Do not
extrapolate 77 single-column files into a whole-file size claim.

The first native read starts with a fresh native file/session owner. Record file
open plus complete ordered scan/canonicalization CPU separately from the
primitive canonicalization CPU within that scan. Repeat the complete read three
times, retaining every observation; the median is the repeated-read score.
Exact-value checking and content hashing occur after each timed decode. Every
result is checked, including the first read. Cache state is uncontrolled and
the source prehash has warmed source data; no cold-cache claim follows.

Also run three native `column = 0` and three `column != 0` count scans with a
literal of the exact column dtype. Compare each complete count with independent
typed Arrow iteration; NULL is excluded by both predicates. Their median process
CPU is the per-column consumer score. Preserve footer pruning statistics against
an independently prepared reference input and identify shared provider
statistics computation honestly. Check exact native allocation release after
every producer/verifier owner drops.

Before measurements, focused tests cover every primitive width, nullable and
all-null values, empty/one-row input, sparse/constant/sequence/random/run-heavy
integers, finite/nonfinite floats and signed zero, preserved dictionary/encoded
inputs, text exclusion, profile capture across task dispatch, scope restoration
after panic, pressure denial, output-cap failure and complete cleanup. A reduced
profile witness must actually change the scheme choice or encoded result in a
target fixture. A missing witness invalidates setup, not a zero-benefit result.

## Frozen screen and decision

Freeze cohorts A and B before either runs. Each has three paired repeats of each
of the three source regions. Alternate profile order and reverse it in B.
There are 36 producer/verifier workflows and 2,772 separately written artifacts.
Complete both cohorts before inspecting comparative costs. A failed workflow
invalidates that cohort; retain it, repair only the justified cause and start a
new complete attempt. Do not substitute a later faster sample.

For each cohort, form each field/region/profile median across its three paired
repeats, then sum those medians. The reduced profile advances only if all are
true in both cohorts:

1. Every exact-value, type, statistics, encoding-admission, source identity,
   ownership, process-drain and artifact-cap check passes.
2. Summed numeric compression thread CPU and complete-column-write process CPU
   each fall by at least 2%. Report the region and individual-column components.
3. Total physical column bytes and total complete file bytes are each no more
   than 2% above current. Apply each bound independently in every region as well
   as over the complete screen. Bytes are exact; the 2% allowance is a declared
   storage tradeoff cap, not noise tolerance or an accepted full-file increase.
4. Each region's summed first-read, repeated-read, equality-count and
   inequality-count process CPU is at most 1.05 times current, independently for
   each consumer family. The same limits apply to the complete screen.
5. No individual field/region/consumer has an increase exceeding both 10% and
   100 microseconds in both cohorts. Individual write costs have the same check.

These are new prospective gates for a different numeric CPU experiment. They
do not rewrite or relax the historical text screen's 30% size target or its
recorded rejection. A failed gate rejects this finite reduced numeric profile;
it does not establish that no costed portfolio can ever help. A successful
screen authorizes only the next fixed-scope workflow comparison, not production
retention. Do not promote only the favorable columns without a separate design.

## Subsequent workflow and retention boundary

If the screen advances, first freeze the complete bounded production workflow
comparison before implementing its harness or collecting its candidate costs.
It must include all source and existing derived fields, current source identity,
real owned conversion, the controlled staging/digest/commit path, unchanged
CPU/memory grants, ordinary serial/short-input controls and newly written-file
native consumers. Capture per-field physical bytes without replacing the
production shared table strategy with extra per-column compressor instances.
Keep instrumentation out of the complete-workflow cost arm and carry all screen
observations into its evidence.

Final retention requires the existing full-artifact guarded ingest and complete
first/repeated Full43 query lifecycle comparison, exact values/schema/statistics,
no-fallback certificates, native pressure/cancellation/publication tests, required
workspace/native gates and public/format acceptance. The prospective complete
workload speed target remains 2%; numeric CPU alone is insufficient. Freeze
precise complete-workflow scoring and resource/size/consumer controls before
that next experiment, preserving the original writer's lifecycle obligations.
If the finite screen rejects the profile, restore all experimental sources after
preserving compact results and exact changed-source evidence. No full-size
ingestion is needed merely to repeat a failed provider screen.

No source-specific production field rule, external execution engine, fallback,
new codec, GPU path, package release or publication is authorized here.
`fallback_attempted=false` and `external_engine_invoked=false` throughout.
