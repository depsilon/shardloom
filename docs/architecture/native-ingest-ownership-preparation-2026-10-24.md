<!-- SPDX-License-Identifier: Apache-2.0 -->

# Reduce full-ingest preparation and ownership-transfer work

Status: active October 25 after naming PR #1604 merges at `3239ade0`.
This work follows
[user-surface native execution parity](native-user-surface-execution-parity-2026-10-24.md)
and [controlled provider refresh](native-vortex-provider-refresh-2026-10-24.md),
then the [warm execution/source-reuse package](native-warm-source-reuse-2026-10-24.md),
and [public naming and terminology](shardloom-public-naming-2026-10-24.md).
The October 25 ordering decision places
[consolidated dependency maintenance](dependency-intake-2026-10-25.md) directly
after this package. The subsequent instruction combines both in PR #1605;
[complete-workflow work elimination](native-workflow-work-elimination-2026-10-25.md)
and the new [batch/window and shared CPU speed investigation](native-ingest-batch-window-scheduling-2026-10-25.md)
then precedes the
[connected programming model](native-connected-programming-model-2026-10-24.md),
then [universal I/O performance and coverage](native-universal-io-performance-coverage-2026-10-24.md),
then [rich-data lifecycle support](native-rich-data-lifecycle-2026-10-24.md),
under the subsequent October 24 ordering decision.
Attribution and complete-readback tools are validated; the first complete
control passes and selects controlled-reader ownership transfer. The candidate
implementation passes 23 focused ownership, actual-reader and timing tests,
3,256 default workspace tests and 3,295 native Vortex library tests. Formatting
and strict workspace/native lint pass; 32 existing opt-in native tests remain
ignored. The complete-ingest screen fails its frozen speed gate; the maintainer
retains the 61.2% copying reduction explicitly. Complete value/layout checks and
one isolated subsequent Full43 pass. Final combined dependency/package/CI
integration remains active. The grouped-preparation campaign
from closed #1595 is included in merged #1596.
The existing RFC 0044 resource contract and PERF-02/03/06/09 owners apply;
CG-1 through CG-23 remain independent gates.

## Ordering decision

The maintainer permits this work before parity only if it is smaller. The full
requested ownership/preparation work is not established as a smaller unit.
Its attractive narrow variant—eliminating repeated copies of the same held
regions—has no demonstrated material activation in the current complete ingest.
Do not turn that unmeasured possibility into a promised quick optimization.

Current conversion receives generic Arrow `RecordBatch` values from a boxed
`RecordBatchReader`. That interface does not carry proof that decoded buffers
were allocated under the native session. Reader provenance, full allocation
ownership, admission and last-owner release must be connected before adoption
can replace copying. The acceptance also includes real complete ingestion,
independent readback and consumers of the new artifact. Treat this as a
separate source/ownership work unit after parity, while keeping its initial
attribution finite. The later warm-source review places bounded existing-owner
reuse before this work; a retention candidate that depends on unproved reader
allocation provenance moves behind this ownership correction. Selective
first-use input belongs to universal I/O and is not a prerequisite for the
full-ingest work.

The October 25 [warm-source implementation](native-warm-source-reuse-2026-10-24.md#final-finite-decision)
reduces actual file opens but fails its frozen benefit/control gates and is
removed. Its fixed-width dictionary hook records no insertions or hits in the
measured workload. FSST compressor/symbol state, ALP-RD dictionary storage and
variable-width scalar/statistics owners remain outside its proved allocation
boundary. Carry those exact provenance gaps into this package before proposing
a materially different retention design; do not re-enable the rejected cache
or presume that an encoding ID proves full ownership. Public naming still
precedes this package.

## Current source and provider boundary

The baseline below describes the observation-only control. The selected
controlled-reader candidate is specified separately below and remains gated.

- [`ingest_arrow_ownership.rs`](../../shardloom-vortex/src/ingest_arrow_ownership.rs)
  copies every referenced value, validity and child-buffer region into the
  admitted native allocator, then validates the replacement arrays and batch.
  Copies retain their allocation credits through clones and slices. A custom
  Arrow owner's reported capacity can hide a much larger retained allocation;
  the existing hidden-owner regression proves why generic pointer adoption is
  insufficient.
- [`resident_memory_arrow.rs`](../../shardloom-vortex/src/resident_memory_arrow.rs)
  accepts caller-owned Arrow batches. Preserve its validated-copy behavior for
  arbitrary external owners. Repeated import from an earlier admitted batch is
  not, by itself, proof of provenance for a later session.
- [`vortex_ingest.rs`](../../shardloom-vortex/src/vortex_ingest.rs), in
  `StreamingColumnarVortexArrayWorker::convert_batch` and
  `record_batch_to_vortex_from_arrow_provider_profiled_with_memory`, sizes
  conversion headroom, projects the batch, makes admitted copies and imports
  them through the Vortex Arrow provider. Reader-owned allocations remain
  explicitly outside that copied-buffer accounting. Do not relabel a batch-size
  estimate as complete retained-allocation ownership.
- [`universal_format_io.rs`](../../shardloom-vortex/src/universal_format_io.rs)
  and its [budgeted projection helpers](../../shardloom-vortex/src/universal_format_io/budgeted_projection.rs)
  use pinned native file-codec readers. Existing projected readers and credited
  encoded source bytes do not prove that every decoded output allocation is
  admitted or that ordinary first use avoids durable preparation.
- [`vortex_ingest_numeric_encoding.rs`](../../shardloom-vortex/src/vortex_ingest_numeric_encoding.rs)
  already skips canonical-primitive dictionary probes when Dict is excluded,
  avoids an extra blanket statistics pass, and compresses numeric output after
  final canonicalizing repartition. Preserve these completed corrections.

Vortex-first direction: the inventory above records the pre-refresh 0.85 path
and is historical. Use the [controlled provider refresh](native-vortex-provider-refresh-2026-10-24.md)
as the current 0.90 API baseline; re-evaluate allocator propagation and
referenced-byte compression/coalescing estimates through the actual Parquet
import path before choosing a new ownership mechanism. Logical referenced
bytes guide compression/layout decisions; retained allocations still require
their full memory charge. Trace both through the real Parquet import path.
Use the admitted session allocator, native buffer owners, Arrow conversion,
compression and layout providers. Any
provenance wrapper must express ownership of those existing providers, not
invent another engine or claim that passing an allocator automatically routes
all upstream allocations through it. Recheck pinned decoder/buffer APIs before
choosing an internal transfer design. No query-engine integration is admitted.

## Finite implementation order

October 25 measurement preparation began while public-naming CI completed.
PR #1604 is now merged after all 44 checks pass. The observation control keeps
every validated copy and adds exact copied-region/charge counters and a nested
ownership work span. The scope includes observation, allocation, copying and
validation; it is not a pure memory-copy timer. Original owners and observer
map metadata remain outside native allocation accounting. Holding each observed
region for the entire batch prevents address reuse from being counted as a
duplicate. The new `compare_parquet_native_artifact` example extends the
existing bounded native artifact comparator with direct pinned Parquet decoding
and independent references for all seven derived fields. It is a read-only
correctness boundary, with no external query engine or production ingest helper.
The first complete control passes. The later retention and consumer decision
below records the candidate separately from that initial control.

The preliminary complete readback now validates the existing accepted file:
99,997,497 rows across all 105 source and seven derived fields, with independent
derived formulas, exact schema and complete value coverage. Two oversized-batch
attempts are retained; reducing readback batches to 8,192 rows resolves the
`Title` retained-capacity limit without raising memory ceilings or reducing
coverage. Source generations and final credit/task release pass. The
[measurement record](../benchmarks/native-ingest-ownership-2026-10-25.md#preliminary-complete-readback)
keeps this later proof separate from original provider-refresh acceptance.

The provider check uses pinned Arrow 59.3.1 buffer/data APIs and Vortex 0.90
session, allocator, Arrow, prepared scan and file providers. `arrow-buffer` and
`arrow-data` become optional direct dependencies at their already locked
Apache-2.0 version for typed owners and dtype layout validation; they add no
package or version to the graph.
Possible transfer still requires complete allocator ownership, alignment and
lifetime proof. Public generic Arrow intake remains a validated-copy boundary.

The [frozen observation protocol](../benchmarks/native-ingest-ownership-2026-10-25.md)
defines the copy-work screen, candidate selection, complete-ingest controls,
independent full readback and isolated query acceptance before measurement.
Naming integration is complete. Default workspace tests pass 3,256 cases;
the full native Vortex library suite passes 3,285, with 32 existing opt-in
tests ignored. Workspace and native strict lint plus formatting pass. The
reference-memory regression checks the separate Parquet retained-byte boundary
after accounting for the existing native conversion allowance. Earlier failed
fixture and oversized-batch observations remain recorded. This establishes the
control and readback tools. The first complete observation subsequently passes
all-value readback and physical inventory with byte-identical accepted output.
Its 115,652,962,945 copied bytes and zero repeated regions select controlled
reader transfer under the frozen materiality rule.

### Selected internal-transfer contract

The [combined local acceptance](../benchmarks/native-ingest-ownership-2026-10-25.md#combined-runtime-acceptance)
now binds the retained ownership implementation and reviewed dependency updates:
9,364 passing default/native test executions, 140 installed-wheel tests without
skips, and a final isolated Full43 of 46.973255495 engine seconds. The original
ingestion speed gate remains failed. The portable packet preserves complete
readback, earlier failures and both source/build identities; integration of
PR #1605 is separate from local acceptance.

Use an opaque ingest source that privately retains the actual ShardLoom-opened
Parquet reader and its ownership policy. The existing public flat source has a
replaceable reader, so a mutable Boolean, metadata label or pointer receipt
cannot establish provenance. An existing flat/custom source converts only to
the validated-copy policy. The opaque source exposes read-only metadata and
specific existing derived-column/prefetch operations, with no arbitrary reader
replacement. Keep existing request/source call patterns through a defaulted
source type and additive controlled-reader constructor.

Consume batches through projection and drop the original array references
before checking each backing buffer. Pinned Arrow's `Buffer::into_mutable`
requires a unique owner, zero allocation pointer offset and standard allocator
layout. Only after that proof may its full allocation capacity become the
native reservation charge. Require the actual Arrow dtype's Rust alignment,
a nonempty buffer and a capacity no larger than the existing copy charge.
Logical-offset, shared, opaque and oversized owners keep validated copies.
Immutably freeze the admitted owner and retain its reservation until the last
buffer/array clone drops; release storage before its credit.

Use existing Vortex host reservations and Arrow conversion, with unchanged
layout, validation, cancellation and publication. Preserve copied-byte fields
and add transferred-region/byte/full-charge observations. The observer may
retain the transferred immutable owner only after unique-ownership proof;
cloning before that check would prevent every transfer. Generic external Arrow
and custom-reader import continue to copy even for ordinary unique allocations.
Adversarial checks cover independent validity offsets, source/session drop,
shared children, excessive capacity, opaque owners, empty/alignment cases,
partial reservation denial and cancellation before complete-ingest timing.

### Remaining preparation and probe attribution

The complete control records one stream-shape validation and conversion per
source batch: 3,120 of each. Shape/type validation remains distinct from
replacement-array ownership validation. Existing adjacent derived outputs
already share traversal: URL/Referer length and domain pairs, and EventTime
minute and truncated-minute pairs. The traced source-state summaries consume
schema/source metadata; this audit finds no additional full-row derived-summary
pass to remove. The derived-metadata timer alone does not establish duplication.

Each of the 28 selected text fields reaches canonicalization and compaction
once per source batch. Compaction changes the retained representation; equal
logical values do not make the two operations duplicates. Their observed work
spans provide no material new preparation candidate in this control. Actual
numeric probe calls remain zero. Numeric compression runs after the final
canonicalizing repartition, so an earlier chunk's encoded result cannot replace
it without new equivalence proof. Preserve these paths and the accepted recipe;
the finite candidate remains the demonstrated ownership boundary. Selective
first-use normalization stays in the queued universal-I/O package.

### Ordered work

1. **Attribute copy work and select one ownership correction.** Trace actual
   bytes copied, repeated visible regions and retained allocations in the
   current complete ingest. Existing `ArrowConversion` timing includes several
   operations; it is not a standalone copy timer. The resident intake has a
   copied-byte counter, but the ingest callsite currently records aggregate
   conversion sizes. Extend the existing finite attribution only as necessary
   to distinguish copies from native conversion, without per-row logging or a
   new evidence framework.
2. **Prefer proven duplicate removal; otherwise connect trusted ownership.**
   A scoped repeated-region correction must still create validated, credited
   storage for external data. For a controlled-reader transfer, establish the
   complete backing allocation, alignment, offset, validity, lifetime,
   immutability and credit owner. Pointer equality and visible Arrow capacity
   alone prove neither provenance nor the full allocation. Never retain an
   uncharged opaque external owner to make a copied-byte counter smaller.
3. **Remove exact repeated preparation.** Identify actual repeated
   canonicalization, compaction, validation or derived-summary traversals over
   an unchanged domain. Reuse only work whose schema, values, selection,
   representation and lifetime are identical. Necessary validation remains;
   eliminating a repeated check requires showing that the same fact was
   already established for the same immutable data.
4. **Investigate remaining probe work only when material.** Reuse preparation
   or statistics only when compatible with the final emitted chunk. An encoded
   result from a pre-coalescing chunk cannot be reused for a different final
   partition. Existing numeric probe avoidance is not a new candidate.

Keep the successful file representation and current compression policy while
testing these work-reduction mechanisms. Stronger Zstd settings, the rejected
structural-text representation, the rejected reduced numeric portfolio and
generic pre-writer overlap do not become new candidates merely by rerunning
them. A later codec/layout change needs a materially different source-backed
mechanism and ingestion, artifact-size and subsequent-query evidence.

## Acceptance and failure boundaries

Freeze the candidate, control, input identity, resource policy, output recipe,
comparison schedule and benefit/control gates before performance measurement.
Use existing ingestion instrumentation and `scripts/run_clickbench_ingest_uat.sh`
with its storage, source-residency, generation, concurrency and cleanup guards.
Keep generated data outside synced folders. Stage durations can overlap and
must not be summed as exclusive elapsed time.

Prove sliced and nonzero-offset buffers, independent validity offsets, empty
aligned buffers, Unicode, long values, dictionaries/shared children, nested
values, malformed input, concurrent ownership, cancellation, partial allocation
denial and final credit release. Include the external hidden-owner control and
complete values/types/order after the original source and session are dropped.
Any ownership error must fail deterministically without external fallback.

Measure a real complete ingest through successful publication, preserving all
observations. Independently reopen and validate the entire resulting artifact
with the established complete-value/readback process. Record physical layout,
size, native encoding/statistics behavior and resource scope. Confirm its query
behavior with the current accepted native runtime and one isolated standard
Full43 after a retained coherent correction. A smaller file or faster copy
microbenchmark cannot override a failed complete-write or consumer gate.
Retain source generation checks, necessary validation, allocation credits and
publication guarantees. Do not add hidden caching or weaken admission.

## Measurement and scope discipline

### October 25 retention and consumer decision

The implementation at `20f03396` transfers 70,821,530,583 buffer bytes and cuts
copied bytes from 115,652,962,945 to 44,831,432,362, a 61.236% reduction in the
single complete screen. All 99,997,497 rows, 112 columns and 11,199,719,664
values pass independent readback. The full physical inventory matches, and the
15,715,228,660-byte file is byte-identical to the control and previously
accepted provider artifact. Publication, source generation, accounting and
cleanup checks pass with their documented allocator scope.

The initial screen's complete-ingest improvement is below the frozen 3% gate.
Its rejection remains recorded and no matched confirmation cohort is run.
After reviewing that result, the maintainer requests retaining the concrete
copying reduction and opening PR #1605. This is an explicit retention decision,
not a changed threshold or a proven ingest speedup. Earlier failed readback and
layout admissions remain preserved.

One isolated standard Full43 on the new artifact verifies all 43 queries, 381
complete result rows and exact schema continuity at **47.281897502 engine
seconds**. The previous accepted observation is **49.625514041 engine seconds**.
The new observation is lower, but separate executions/builds with uncontrolled
cache and host variation do not establish a causal query benefit from ingest
ownership. Query input content is byte-identical. No additional query execution
was needed for verification and the 10% regression trigger does not activate.

The [measurement record](../benchmarks/native-ingest-ownership-2026-10-25.md)
preserves the original screen and this subsequent decision. The completed
preparation/probe audit above finds no demonstrated second mechanism. The
separately queued [batch/window work](native-ingest-batch-window-scheduling-2026-10-25.md)
examines remaining policy interactions after dependency maintenance and the
new workflow work-elimination package; it does
not restart ownership transfer or the completed duplication audit.

The latest grouped-preparation Full43 is **49.503497253 engine seconds**, not
a new cold/warm comparison or a repeated five-surface result. The handoff's
48.693636581 query observation and earlier 58.450 ingestion figure remain
historical context; their sum is not a newly measured end-to-end workflow.
No ingestion cost or speedup is established by this source review.

The supplied competitor totals are unmatched reference targets with different
hardware, clocks and cache/run protocols. They do not establish a current
ranking, an official ClickBench placement or superiority. Verify exact primary
records if those comparisons are published; do not promote the supplied table
into acceptance evidence for this implementation.

The workflow work-elimination handoff now owns the earlier implementation of
one-execution multi-output writes. Universal I/O retains format-specific
selective Parquet/other-format first-use normalization, bulk writer policies and
format coverage, reusing that execution contract. One-off projected input and complete durable ingestion are
different workloads. A projected prepared artifact must never masquerade as a
complete source copy, and a selective-query gain is not a complete-ingest gain.
