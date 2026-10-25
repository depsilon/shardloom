<!-- SPDX-License-Identifier: Apache-2.0 -->

# Ingest representation reuse, batch geometry and shared CPU scheduling

Status: item 16 closed without a runtime change on October 25. After current-main
ingestion and source review, the maintainer requests proceeding through the
version bump train if no worthwhile new change is established. No new candidate
has passed a benefit gate. The [decision](../benchmarks/native-ingest-window-decision-2026-10-25.md)
preserves the 49.303-second ingestion observation, the unchanged runtime's
47.889862794-second Full43 and the limits of this investigation. Existing
representation and scheduling policies remain. The **1.0.0 version train is
active**, followed by connected programming and universal I/O. The remaining
handoff below records the original investigation scope, not unfinished release
requirements or an exhaustive optimality claim.

Originally requested after consolidated dependency maintenance
and before connected programming. The maintainer subsequently combines
ownership/preparation and dependency maintenance in PR #1605, then inserts
[complete-workflow work elimination](native-workflow-work-elimination-2026-10-25.md)
immediately before this speed investigation. It extends existing RFC 0044
resource ownership and PERF-03/06/09 work, without a new engine or phase ID.
CG-1 through CG-23 remain independent.

## Start from the completed ownership work

The supplied handoff reviewed main at `3239ade0`, before the
[ownership transfer](native-ingest-ownership-preparation-2026-10-24.md).
PR #1605 already implements controlled-reader allocation transfer, records
copied/transferred bytes, audits preparation/probe duplication, and validates
complete ingestion and subsequent queries. Its initial screen cuts copying
by 61.2%, but fails the frozen complete-ingest speed threshold. The maintainer
retains that copying improvement explicitly, without a repeatable speed claim.
Do not repeat that implementation or restart the completed duplication audit.

The subsequent compact-structure addition asks whether source encodings can
survive normalization without expanding and rebuilding their values. This
representation investigation precedes batch/window tuning within this item.
The scheduling question remains whether the existing batch, memory-window and
task policies interact efficiently after ownership transfer. The current PR retains
those policies. Refresh the actual applied values on the integrated dependency
baseline before selecting a mechanism; the older handoff's nominal settings
are context, not a measured bottleneck or the next run's applied geometry.

## Existing machinery and source anchors

| Boundary | Current implementation | Remaining question |
| --- | --- | --- |
| Reader geometry | [`universal_format_io.rs`](../../shardloom-vortex/src/universal_format_io.rs): `product_columnar_stream_record_batch_rows` selects 65,536 rows, or 262,144 for the large-source policy. `ParquetExtentPlan::memory_admitted_batch_rows` reduces that using the largest row-group estimated bytes per row, a field-width floor and expansion headroom. | Does one wide region unnecessarily constrain other work? What rows and retained bytes actually arrive after admission? |
| Source tasks | The same source builds adaptive row-group tasks. [`IngestSourceReader`](../../shardloom-vortex/src/ingest_source.rs) bounds the ordered task window and each task's result queue, consuming the front task before advancing. | Are task lengths and readiness suitable for downstream conversion and compression? |
| Conversion window | [`vortex_ingest.rs`](../../shardloom-vortex/src/vortex_ingest.rs): first-batch sizing admits the existing conversion window; `StreamingColumnarVortexPrefetch` keeps tasks plus pending results within it and retains memory grants until handoff. Existing exact-copy sizing can recover lookahead when the broader estimate suppressed it. | Can a small byte-accounted window sustain useful work with less retained state or less starvation? |
| Shared CPU ownership | [`IngestRuntime`](../../shardloom-vortex/src/ingest_runtime.rs) uses one admitted executor for source, conversion and provider work. Clones create no extra threads; writer admission starts the remaining shared drivers. | Do runnable prerequisites and coarse tasks delay useful work within the same allocation? |
| Writer boundary | [`BoundedIngestLayout`](../../shardloom-vortex/src/ingest_bounded_layout.rs) owns one live child subtree and can concurrently pull exactly one following input. The existing shared-slot rule accounts for that lookahead. | Is there a measured recoverable subtree tail while eligible successor work is ready? |

These are source-confirmed controls, not proof that every boundary is slow.
Parquet uncompressed-page bytes are not decoded allocation capacity, and a
requested batch size is not an allocation guarantee. Source decoder/provider
allocations outside native credits remain explicitly excluded until proved.
Preserve the ordered reader's protection against accumulating later work behind
a slow earlier task.

## Priority 1: preserve one compatible source representation

Trace one real source-encoding → adapter → Vortex writer case before changing
queues. Investigate validated translation of dictionary codes/values, constant
or range structure, runs or integer deltas only where the current route expands
and rebuilds compatible structure. Ownership transfer in #1605 avoids a copy
after decoding; this candidate must demonstrate additional decoder, encoding
analysis or intermediate work avoided.

Reuse existing input adapters, Arrow/Vortex conversion and the native writer.
`vortex_ingest_dictionary_preservation.rs` already preserves eligible exposed
native dictionary chunks and dictionary-value owners; it does not prove that
every Parquet dictionary page reaches that boundary intact. Trace actual
provider behavior and dictionary epochs before selecting a missing case.
Compression framing, types, validity, dictionary scope, offsets, exceptions and
required format validation must all match. Arbitrary compressed Parquet pages
cannot be copied into a Vortex file. Keep ordinary native decoding for cases
without a proved compatible translation.

Freeze complete-value/schema/order, generation, corruption, cancellation,
credit and publication controls. Measure complete ingestion and output quality
with an activating encoded case and tiny/high-entropy/non-admitted controls;
record decode/encode work and retained allocations, not only copied bytes.
Retain the accepted codec/layout quality contract or explicitly compare the
entire changed representation and downstream consumption. No renamed file
format, hidden source dependency, independent format executor or new pool is
introduced. Extend a proven mapping through the later universal-I/O package
instead of duplicating this implementation for every adapter.

## Priority 2: batch and window interaction

Distinguish source decode batches, conversion work units, compression work
units, persisted blocks/segments and buffered writes. Current source-batch
subtrees couple some boundaries. Separating them requires implementation and
equivalence proof; changing a constant alone does not establish separation.
Check the pinned Vortex layout/provider APIs before introducing another owner
or abstraction. Initially preserve codec policy, required metadata and accepted
output quality, and isolate scheduling geometry from persisted layout wherever
the existing provider contract permits it.

Use source metadata to establish initial work size, then investigate a small,
stable region-aware policy informed by observed retained bytes and processing
cost. Compare a few justified sizes around the actual applied setting. Keep
skew, oversized-value, ownership and admission checks. Do not add a large search
framework, an autonomous tuner or user-facing queue controls.

Account for the complete outstanding state: source buffers, decoded batches,
conversion inputs/outputs, active compressor workspaces, completed output waiting
for ordered emission, writer buffers and metadata. A count of visible queued
batches is insufficient. Where full retained allocation is not measurable,
declare the exclusion and retain conservative admission instead of calling an
estimate exact ownership.

Seek the smallest window that sustains throughput. Investigate additional
admitted input only when compression lacks ready work; reduce unnecessary
lookahead when decoded input already waits behind busy compression. Preserve
progress under pressure, prioritize dependencies blocking ordered emission,
and release useless lookahead promptly at EOF. These are proposed responses
requiring evidence, not findings about the present workload. A window target
must not become a permanent limit on valid source size.

## Priority 3: task granularity and CPU scheduling

Attribute storage wait, Parquet decompression/decoding, conversion/derived-field
preparation, and output compression separately. Establish whether source tasks
run too long before producing useful input, expensive fields create a long
compression tail, ready prerequisites lose scheduling opportunities, or waiting
work retains a scarce resource needed for progress.

Improve task size, admission, yielding or priority inside the existing shared
runtime. Do not add independent pools that each spend `max_parallelism`, a
fixed reader/compressor split, or a second buffering layer. Increased CPU
activity is not a benefit when it represents copying, repeated compression,
polling or contention rather than productive work.

## Priority 4: conditional adjacent writer overlap

Preparing the following input already overlaps the active writer subtree.
Starting useful successor writer work before that subtree completes is a
different mechanism. Investigate a small overlap only after the new full-ingest
attribution establishes an idle tail and ready useful successor work.

Preserve source and output order, local EOF dependencies, exact statistics,
cancellation and cleanup. Charge completed later output while it waits. Require
a bounded progress argument before increasing the number of live children;
batch/window refinement precedes this more complex candidate.

## Earlier decisions and finite acceptance

The [September allocation candidate](../benchmarks/ingest-stage-balance-2026-09-12.md)
removed a conversion worker and reassigned capacity to the provider, then failed
its complete-workload comparison and was removed. The
[October overlap decision](native-ingest-overlap-observer-decision-2026-10-20.md)
found insufficient repeatable opportunity in its declared short-prefix scope;
the [earlier observers](native-ingest-overlap-attribution-2026-10-20.md) also
retain their overhead failures. Do not rerun those mechanisms unchanged. Those
finite outcomes do not establish optimal geometry for the current full ingest.

Reuse existing ingestion instrumentation, guarded runners and comparison
process. First record actual batch rows/retained bytes, window occupancy and
empty/full waits, runnable work versus admitted workers, useful stage work and
final drain behavior. Distinguish elapsed work from measured CPU consumption;
overlapping timers must not be summed as exclusive CPU or elapsed time. Check
observer overhead before selecting a candidate from added measurements.

Freeze a finite candidate and complete-ingest benefit/control gates before
timing. Retain a representation or scheduling change only for meaningful complete-ingest benefit
with correct values, schema/order, resource behavior, publication, output
size/layout and subsequent queries. No expected winning size, depth or speedup
percentage is established by this handoff. Preserve every failed observation.
Do not obtain a faster result by increasing the CPU allowance, skipping
verification, ending before work drains or moving preparation into the first
query. A retained coherent change receives one isolated standard Full43 pass;
routine query reports use engine time only.

The outcome may be a justified implementation or a finite negative decision.
The maintainer explicitly requires a speed check toward the end so the whole
item can be dropped if it fails: retain the frozen before/after complete-ingest
benefit/control evidence, then run one isolated Full43 on the proposed final
runtime and compare the same engine clock with the accepted baseline. Preserve
all observations and report before/after values and percentage differences.
Proceed to the 1.0.0 version bump after either decision.
Keep the controls internal: users specify resources, and the engine admits and
schedules work. Native Vortex, explicit side effects, safe ownership and no
external query-engine fallback remain mandatory.
