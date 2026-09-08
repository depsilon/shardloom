# Execution-aware native artifact topology

Status: **parked, not promoted**, following the maintainer's September 8 request
to stop investing if the benefit is not material. Source inventory and bounded
coalesced-job integration are implemented and reviewed on the experimental branch.
That runtime, its CLI option and its benchmark harness changes remain on
`codex/perf-remaining-work` at `be2a69f3`; they are excluded from the separate
retained-runtime shipping branch. Counts below describe that experimental
checkpoint, not the extracted shipping source.
Validation passes 3,266 native CLI/Vortex tests, 3,420 default workspace tests,
and 54 harness tests, plus formatting and default/native/minimal-native Clippy.
Nine native manual benchmark or fixture-regeneration tests remain ignored. The
focused public integration checks include exact floating results, nullable
grouping, prepared source reuse, and partial/complete metadata pruning. The
measured `a3c62434` grouping checkpoint requires revision and is not promoted:
Existing/Auto/Target1 each pass all 129 Full43 results, but best sums are
91.866821957/92.932750837/93.055419416 seconds. Auto and Target1 regress by
1.1603% and 1.2938%. The independent 4,096-row Existing/Auto matrix passes all
760 acceptance records, and the 131,072-row matrix also passes all 760 after
lossless evidence archiving restored log headroom. Both initial guard stops
remain recorded. The separately frozen `9152a92b` coalesced-job revision also
passes all 129 Full43 results in each of Existing/Auto/Fine, but best sums are
91.286390958/92.510021085/92.596168207 seconds: Auto regresses 1.34% and Fine
1.43%. None of its 13 selected query lanes has a material measured gain. See
[the coalesced-job decision packet](../benchmarks/native-topology-coalesced-2026-09-08.md).
Fresh held-out matrices for this revision were not run after that rejection;
the older held-out results do not validate the new revision. Further topology
experiments and staged local-reduction/Top-K implementation are paused. This refines the existing
PERF-03/04/05/07/09/10/12 items under RFC 0044, not a new phase-ID series or a
second active queue. The canonical execution order remains in
[the phased plan](phased-execution-plan.md). No competitive gate is closed.

## Contract

One logical native Vortex artifact exposes independently executable internal
physical regions through existing source splits, layout boundaries and Capillary
work units. The implementation must deepen the current native engine and shared
SQL/DataFrame/Python/CLI execution families, with one retained source generation,
bounded dynamic work admission, exact final results and explicit native evidence.
Regions are internal; users do not receive or manage separate files.

Inventory existing Vortex source-split discovery/admission, Capillary morsels,
layout/chunk boundaries, encoded consumers, preparation advisor, scan scheduling,
worker ownership, deterministic merges, statistics/pruning and partial operators
before adding a type. Record which paths execute and which merely describe work.
Use pinned native Vortex providers inside the existing adapter boundary. Reuse a
source split or layout locator when it expresses the region; do not add a second
partition model, scheduler, engine or benchmark-only executor.

A region needs a generation-scoped stable identity, safe row range and physical
locator, with optional physical-size/statistics/encoding capability evidence from
the real provider. These are conceptual requirements, not mandated new fields or
a prescribed public `NativeExecutionRegion` API. Byte offsets without safe native
execution boundaries are inadmissible. Empty and fully pruned sources remain valid.

The number of regions is independent of worker count. Existing bounded workers
dynamically consume safe regions; no thread is created per region. Region and
queue ownership, retained native buffers, compact state and ordered merge share
admission. Cancellation stops admission and joins/drains owned work before errors
escape. Skew must leave workers able to consume other ready work, while any
repartitioning preserves source coverage and reduction order.

Start with a simple costed granularity policy derived from actual native layout,
rows/size hints, operator shape, worker ceiling and memory envelope. Avoid one
giant region and tiny scheduling units that cost more than their useful work.
Existing physical boundaries can be coalesced or safely row-subdivided only where
the provider supports independent native execution. Unknown sizes remain unknown;
estimates do not certify allocator or RSS bounds.

Consult trustworthy existing region statistics before admitting expensive work.
A provably impossible predicate removes a region without decoding/materializing
its data. Unsupported or uncertain metadata remains conservative. Metadata I/O
must be distinguished from payload I/O. New stored summaries need demonstrated
runtime benefit that pays for their ingest, storage and validation costs.

## Source inventory and selected seams

The inventory was checked against the active native adapter and the pinned
Vortex 0.85 source. The integrated adapter has separate actual completion proof;
existing descriptor counters alone do not establish that it executed.

| Existing mechanism | Current behavior and integration decision |
|---|---|
| `scheduler_bridge::VortexMorselWorkUnit` | Already carries stable work identity, source row start/count and estimated bytes. Reuse it for internal regions rather than introducing another partition model. Unknown physical sizes must remain unknown in evidence |
| `ScanBuilder::full_file_splits`, `with_natural_splits`, `RepeatedScan::execute` | Discover safe native row boundaries after projection/filter binding, retain the complete boundary sequence, and independently execute an admitted range on the same held file. Filter-only fields participate in discovery. Coalescing changes region ownership without changing original native array boundaries |
| `ComputePool` and `aggregate_chunk_jobs` | Supply a bounded dynamic queue, leases, cancellation and ordered owned completions. Reuse this execution seam; do not create one thread per region |
| Legacy `execute_vortex_morsel_scheduler_with_observer` | Statically assigns descriptors to worker vectors; some query callers produce these descriptors after scanning. It does not establish independently scheduled native reads. Its queue reporting cannot substitute for live queue admission |
| `PreparedVortexSource::with_native_execution` | Holds source-generation validation and admission around native work. Enter once around dispatch, consumption and complete drain. Entering this gate independently in each region would serialize work |
| Aggregate `CountWorkers` and `AggregateScanRuntime` | Existing workers receive arrays after a central scan. Region workers must share the same CPU grant and replace the applicable provider/aggregate scheduling ownership before execution; nesting pools would over-admit CPUs |
| Existing scalar/grouped native consumers | Accept native arrays and already inspect actual encodings. Stage 1 preserves their original array boundaries and ordered invocation. Existing floating SUM/AVG cannot be freely reassociated merely because partial merges are deterministic |
| `file.can_prune`, `LayoutReader::pruning_evaluation` and embedded layout evidence | Whole-file statistics can avoid all payload work. The public layout reader can prove an original row range impossible using the exact bound predicate and an initially all-true mask. Await that proof before constructing `RepeatedScan` tasks, since task construction can register projection prefetch. Missing statistics retain the range; no-output tasks do not prove pruning |
| Layout inventory and reader-backed split evidence | Footer segment IDs identify buffers, not necessarily row partitions. Post-read arrays and inventory counts do not by themselves establish a validated, independently readable region directory |
| Existing sort candidates, comparison and row-reference second pass | Preserve exact multi-key/NULL/source-order semantics for later local Top-K. A selected payload must retain its global source-row identity; existing key materialization is not metadata pruning |
| Preparation and column-addressable writer experiments | Keep the existing writer and codec defaults during topology isolation. Only measured runtime gains can justify changing physical preparation advice |

The first staged implementation exposed an ordered-output hazard: holding one
worker for a whole coarse region parks later workers behind the frontier region.
The measured `a3c62434` adapter therefore uses bounded original-split steps on the
existing queue, releasing workers between steps. Region count, admitted regions,
outstanding split steps and CPU workers are distinct quantities. Coarse/fine
benchmarks must determine whether region granularity changes useful work rather
than merely changing a label on otherwise identical scheduling.

In that measured adapter, requested counts coalesce original natural splits into
groups. Even target one can execute several natural splits concurrently inside
that group. This preserves original read and array boundaries, so the requested
counts are grouping/admission levels, not automatically different physical read
granularities. The fixed-artifact packet must report natural split geometry and
actual queue windows separately; changing group counts alone cannot substantiate
finer independent execution. Its public group cap is redundant with the split
window. All 13 selected queries retain 1,170 original jobs and a 20-split window
at actual P10; Auto's 382 groups and Target1's one group do not change that work.
The other 22 requests explicitly retain their native route, while eight have no
region report. Selected runs prove no metadata-pruned split; Q40/Q43's 1,159
no-output tasks still executed. Local reduction and local Top-K remain false.

The measured `9152a92b` candidate makes each region a bounded job over
adjacent original splits, retaining every original array and its order. At
P10/W20 it allows two originals per job and at most ten outstanding jobs,
including the caller-held completion, within the same 20-output bound. Row
quantum and natural boundaries may increase the realized job count above a
requested coarse target. Auto realized 585 two-original jobs; Fine realized
1,170 single-original jobs under the same P10 and 20-original outstanding bound.
Both regress against Existing, so the branch option remains experimental and
must not be promoted as a performance improvement. Preserve its frozen evidence
without continuing the staged implementation by default. The earlier packet is
`topology-a3c62434-grouping-analysis.json`; its classified report is
[native topology grouping evidence](../benchmarks/native-topology-grouping-2026-09-08.md).

Range pruning must validate mask length and require an all-false proof for every
original split before declaring its containing region wholly pruned. Zoned
layout readers may load and decode complete auxiliary statistics tables and
cache predicate masks. Their metadata work is distinct from candidate payload
work and remains subject to native allocation admission. A skipped row range
does not prove that no physical bytes overlapping it were read for neighboring
surviving ranges. The provider's private predicate decomposition is not a new
ShardLoom abstraction: pass the existing bound predicate, accepting conservative
non-pruning for unsupported compound shapes.

## Staged acceptance under the existing PERF items

1. **Independent scheduling — PERF-03/07/10/12.** Wire actual native physical
   discovery/admission to the existing Capillary queue and unchanged native
   consumers. Prove overlapping independent scans, exact source coverage,
   cancellation, shared memory/CPU ownership and deterministic delivery. Do not
   infer independent region execution merely from arrays emitted by one central
   scan. Retain only after full correctness, no material Full43 regression, and
   meaningful gains on expensive operations or a measured, clearly identified
   next operator bottleneck. This packet isolates topology from new reductions,
   Top-K kernels and writer/codec changes.
2. **Local reduction — PERF-04/05/10/12.** Push existing admitted COUNT, SUM, MIN,
   MAX, AVG state and grouped aggregates into regions, sending compact partial
   state to a bounded merge. Preserve integer boundaries, NULLs, global key
   equality, overflow behavior and existing floating evaluation semantics;
   deterministic scheduling alone does not justify floating reassociation.
   Continue direct constant/run/dictionary and other encoded consumers wherever
   their actual representation is admitted. Unsupported region encodings may
   use only existing explicit bounded native materialization, with its evidence.
   Retain each added family only with measured work reduction and performance
   benefit, using its own packet against the retained scheduling baseline.
3. **Local ordered reduction — PERF-07/10/12.** For semantically admitted
   ORDER BY/LIMIT and filtered variants, form bounded local candidates and merge
   exact global Top-K. Include offset in retained capacity, complete multi-key
   ascending/descending and NULL ordering, duplicates/ties and projected payload
   row identity. Preserve existing source-order tie behavior. Global merge should
   consume approximately regions times retained-K candidates where this is exact;
   local truncation of partial grouped counts is not generally exact. Require
   complete-result and material performance improvement before retention.
4. **Execution-aware preparation — PERF-08/09/12.** Only after runtime evidence
   succeeds, feed measured region granularity, pruning, encoded work and merge
   costs into existing layout/codec advice. Compare the combined storage, ingest,
   scan parallelism, pruning and query lifecycle. Preserve numeric compression,
   single-artifact publication and public semantics. Do not rewrite ingestion or
   add speculative metadata before the runtime evidence warrants it.

Each stage has an immutable source/binary/artifact and benchmark packet with a
retain, revise or drop decision. Conditional non-admission uses the existing
native route and must be decided before work; runtime errors cannot silently
restart on an alternate engine or discard partially completed work.

## Required topology experiment and proof

Use the existing immutable 99,997,497-row workload. Compare current/coarsest,
small, medium and large safe native topologies. Counts near 1/4/8/16/32/64/128
are useful experimental levels only when they produce distinct admitted job or
read geometry. Do not run additional numeric labels on the rejected grouping
model merely to populate that list. They are neither a default nor an excuse
for artificial byte splits.
Hold artifact contents, schema, query semantics, worker configuration, memory,
build flags and benchmark runner constant. Record realized counts when requested
granularity cannot map exactly to physical boundaries.

For each accepted level retain all 43 queries times three executions, per-query
complete values, best-of-three sum, hot total, all 129 timings and observed memory.
Classify scan/reduction, filter, grouped aggregation, ordered bounded result and
high-cardinality/global-state queries by physical shape, not query-name special
cases. Separate evidence for topology, pruning, local reduction and encoded work,
and state what remains global. Keep every regression and failed/partial run.

Coarse/fine differential tests also vary worker counts, NULLs, empty/pruned/skewed
regions, integer limits, Unicode, duplicate sort keys and supported encodings.
Full43 complete-result validation and the independent held-out matrix remain
mandatory; a checksum or selected aggregate alone is not complete-value proof.

Reuse existing split and Capillary fields where they prove a required fact.
Add only missing counters for discovered/admitted/pruned/completed regions,
local-reduction/Top-K application, encoded/materialized region execution and
peak in-flight regions. Define actual completion and accounting scope; planned
regions and provider capabilities cannot be reported as completed work. Do not
create a large telemetry schema absent a measured diagnostic need.

No answer cache, external execution fallback, benchmark-column specialization,
forced Arrow conversion, multiple user-visible artifacts or unsupported success
is authorized. Arrow remains an explicit interoperability/test boundary. The
architectural success criterion is useful independent native region execution
and exact bounded coordination, not a predetermined benchmark number.
