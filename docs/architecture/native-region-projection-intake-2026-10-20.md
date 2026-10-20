<!-- SPDX-License-Identifier: Apache-2.0 -->

# Natural-region projection admission: finite count intake

Status: frozen prospective intake at `603e8ec1`, before the observation harness
or any new runtime behavior. This refines `PERF-03.regions` under RFC 0044 and
the conditional-first sequence. It does not admit a timing comparison, complete
the broader region owner, or close CG-1 through CG-23.

## Distinct mechanism and provider check

The rejected `9152a92b` region stream already evaluated metadata before building
scan tasks (`native_region_stream.rs::prune_split`). Repeating that mechanism or
its coalescing policies is excluded. The new hypothesis is narrower: delay the
existing provider's projection construction until its existing filter mask is
known, preserving the existing natural split stream and its ordering. There is
no proposed second metadata evaluation, partition model, worker pool or region
coalescing policy. This is a hypothesis, not measured benefit or an admitted patch.

Pinned Vortex 0.85 already supplies `ScanBuilder::full_file_splits`, field-aware
natural row boundaries, `split_exec`, layout pruning/filter/projection and native
`FileSegmentSource`. Classify the proposed boundary as
`use_vortex_native_provider` inside `shardloom-vortex` and its existing excluded
provider patch, behind the current native features. Source inspection finds:

- `vendor/vortex-layout/src/scan/tasks.rs::split_exec` constructs projection
  evaluation before awaiting the mask, including for filtered splits.
- `layouts/flat/reader.rs::array_future` immediately registers the segment request.
- `vendor/vortex-file/src/segments/source.rs` and `read/driver.rs` can coalesce
  nearby registered requests into a read initiated by a polled request.
- Registration alone proves no completed I/O. The source-level possibility must
  be checked against actual completed positional reads from the public aggregate
  path, including footer preparation and subsequent operation reads separately.

The existing `resident_session::read_observer` retains production generation,
allocation, operation-I/O ownership, coalescing and CPU admission. Its completed
range records are suitable for count attribution. The segment-request wrapper in
`resident_file_pruning_tests` changes registration timing and is excluded here.
Native pruning and scalar aggregation remain the executors; there is no external
engine, Arrow execution, new dependency or fallback. Results require the ordinary
native I/O certificate with `fallback_attempted=false`.

## Frozen inputs and calls

Write eight independent files using existing native Table/Chunked/Flat/Zoned
strategies. Each has 131,085 rows, nullable I64 `f` and `p`, and no file-level
statistics. Source generation and a complete independent value comparison occur
before observation. These are controlled layout fixtures, not a claim about all
ordinary ingests. All files and evidence stay under the existing local-only
investigation workspace, with unchanged storage and workload guards.

Let `z = floor(row / 4096)`, `v = row % 97`, and
`p = ((row * 37 + 17) % 100003) - 50001`. The sparse filter value is `100 + v`
when `z % 4 == 0`, otherwise `v`. The predicate is `f >= 100`; NULL is not TRUE.
Each complete query returns COUNT(*), COUNT(p), SUM(p), MIN(p) and MAX(p), checked
against the full independent input, including exact integer results and NULLs.

| Profile | Values and native layout | Role |
| --- | --- | --- |
| `sparse_zoned` | Sparse values; f zones and f/p chunks 4,096 rows | Target |
| `sparse_plain` | Same values/chunks, no zoned statistics | Target |
| `all_true_zoned` | Every f is `100 + v`; aligned chunks | Control |
| `all_false_zoned` | Every f is `v`; aligned chunks | Control |
| `mixed_zoned` | f is `100 + v` when `row % 4 == 0`, else `v`; aligned chunks | Control |
| `nullable_zoned` | Sparse values; f NULL in every `z % 4 == 1` zone or when `row % 11 == 0`; p NULL when `row % 13 == 0` | Control |
| `misaligned_zoned` | Sparse values; f chunks/zones 4,096 rows, p chunks 6,144 rows | Control |
| `skew_zoned` | Sparse values; f chunks/zones 4,096 rows, p chunks alternate 512/7,680 rows | Control |

The final partial chunk is retained. Natural boundaries are inspected with both
filter and projection fields and must be identical at CPU grants 1 and 4, cover
the entire generation exactly, and agree with inspected native field layouts.
Each profile runs twice at each grant, in fresh sessions with a 64-MiB native
memory grant: 32 observed complete queries. This is count attribution, not a
performance cohort; observer timings establish no engine speed or overhead claim.
Every source row and both field values are independently checked once per file.

## Physical attribution and eligibility

Traverse the actual footer layout, preserving each Field/Chunk/Transparent/
Auxiliary relationship. Join segment IDs to their physical byte ranges. Reject
unknown relationships, ambiguous segment ownership, overlapping physical segment
ranges, incomplete field coverage or unverified row spans. Treat zones and all
other auxiliary descendants as metadata, not projected payload. Footer bytes,
alignment gaps, repeated reads, preparation reads and operation reads remain
separate. A byte offset alone is never a native execution region.

A p segment is potentially avoidable for this hypothesis only when its complete verified row
span contains no TRUE predicate row. Count the intersection of each completed
operation read with those p ranges, keeping both read-byte multiplicity and
unique byte coverage. Do not count whole coalesced reads as avoidable, count
metadata as payload, subtract merely requested bytes, or assume the OS performed
physical device I/O. Preparation reads and already cached ranges are reported
separately and cannot satisfy the operation gate.
This is an upper bound: necessary neighboring reads may still span rejected
payload as a coalescing gap even after delayed registration. Only a later paired
comparison could establish actual removed reads.

Advance to a separately frozen candidate protocol only if both sparse targets,
in both grants and both repetitions, complete at least 65,536 avoidable p bytes
and those bytes are at least 10% of all completed post-preparation read bytes.
All 32 queries, full source checks, exact schemas, immutable generation checks,
natural-boundary checks and ownership checks must pass first. Any failed read,
observation limit, unknown attribution, denied native reservation or incomplete
drain makes the affected evidence invalid rather than zero work.

If the valid intake misses this threshold, record no eligible signal in this
finite scope and do not implement or time the candidate. A failure of the
measurement prerequisite remains inconclusive with its exact cause. Neither
outcome proves general topology optimality. Preserve every attempt and prohibit
an unchanged rerun or a relaxed threshold chosen after observation.

## Later obligations and restoration

Positive counts alone do not retain a mechanism. Before any candidate timing,
freeze full workflow, engine, memory and nonbenefiting controls in two independent
cohorts. Prove original batch boundaries, complete ordered coverage, nullable and
floating semantics, source generation rejection, delayed/skewed progress,
cancellation, provider failure and complete owned-work drain within existing CPU
and memory admission. A read reduction must pay for lost prefetch overlap and
construction costs. Delaying unsupported projection errors needs an explicit
semantic decision before a patch; no hidden fallback or weakened error contract.

The temporary count harness is archived with its exact source/build identity and
removed after the decision. Preserve compact complete outputs, original failures,
source/layout/read records and replayable count calculations. Retire generated
payloads and comparison binaries after verification, retaining the ordinary
tested runtime. Broader region-local reductions, ordered candidates, preparation
design and the original 100M-row topology comparison remain separately gated.
