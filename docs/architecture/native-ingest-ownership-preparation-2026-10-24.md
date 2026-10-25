<!-- SPDX-License-Identifier: Apache-2.0 -->

# Reduce full-ingest preparation and ownership-transfer work

Status: accepted October 24 maintainer handoff, queued after
[user-surface native execution parity](native-user-surface-execution-parity-2026-10-24.md)
and [controlled provider refresh](native-vortex-provider-refresh-2026-10-24.md),
then the [warm execution/source-reuse package](native-warm-source-reuse-2026-10-24.md),
then [public naming and terminology](shardloom-public-naming-2026-10-24.md),
and immediately before the [connected programming model](native-connected-programming-model-2026-10-24.md),
then [universal I/O performance and coverage](native-universal-io-performance-coverage-2026-10-24.md),
then [rich-data lifecycle support](native-rich-data-lifecycle-2026-10-24.md),
under the subsequent October 24 ordering decision.
This records scope and source review, not an implemented optimization or new
ingestion measurement. The grouped-preparation campaign from closed #1595 is included in merged #1596.
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

The universal I/O handoff retains selective Parquet/other-format first-use
normalization, one-execution multi-output writes, bulk writer policies and
format coverage. One-off projected input and complete durable ingestion are
different workloads. A projected prepared artifact must never masquerade as a
complete source copy, and a selective-query gain is not a complete-ingest gain.
