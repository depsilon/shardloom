<!-- SPDX-License-Identifier: Apache-2.0 -->

# Exact regional aggregate investigation

Status: complete and **retained locally** at `c9baefd00`, following the completed,
dropped [per-measure experiment](native-metadata-measure-completion-2026-11-14.md).
The [complete comparison](../benchmarks/native-regional-aggregates-2026-11-14.md)
and [portable evidence](../benchmarks/evidence/native-regional-aggregates-2026-11-14/README.md)
pass all 1,974 fresh results, 40 lifecycle operations, ten strict requested-read
witnesses, 350 regional activation/coverage controls and 70 offline negative
controls. The 987-case fresh engine total decreases from 14.828472105 to
13.836163897 seconds; the separate lifecycle total decreases from 0.399843288
to 0.221834044 seconds. All 429 individual fresh regressions remain recorded.

The executed source fingerprint is
`55506cdaf0b2d835d868f754b9a72dc26016c315cb9373891114763741863620`.
All 5,745 captured source files match the preservation commit byte-for-byte;
the build itself retains its earlier parent and dirty-source identity. Default,
native, provider and installed tests pass. Constant-key grouping is next;
the broader metadata/optimization queue, item 19/20, RFC 0044 and CG-1 through
CG-23 remain open. This local acceptance does not merge or publish the change.

The following contract was fixed before measurement and remains the boundary
of the retained implementation.

## Contract and shared owners

An encoded-columnar engine maintainer should be able to prove that every input
row belongs to exactly one of three sets: excluded by exact predicate facts,
included through exact compatible aggregate partials, or assigned to the normal
native scan. No partial over a larger row set may answer a smaller set.

The initial candidate admits scalar COUNT, MIN and MAX over integer columns,
with COUNT(*) supported. It binds the entire original declaration first and
uses the existing aggregate state, merge and finalizer. HAVING, output names,
integer widths, nulls and output ownership retain their current contracts.
SUM/AVG, distinct, transformed measures, grouped requests, spill and floating
predicates remain on their current native paths. Constant grouping keys are a
separate next investigation.

The predicate domain is bounded integer comparisons, IS NULL, IS NOT NULL,
constant truth and conjunctions. A nullable comparison is never proved true
merely because its non-null bounds satisfy it. Unsupported or missing facts
leave a region unresolved. Binding and input-schema validation occur before
metadata can suppress input evaluation.

## Vortex-first provider check

- Decision: `use_vortex_native_provider`, with ShardLoom owning semantic
  admission, cross-column coverage, reservations and existing aggregate states.
- Provider: the pinned Apache-2.0 Vortex 0.93.0 `ZonedLayout`, its auxiliary
  stats-table child, typed aggregate functions, native layout readers and
  `ScanBuilder::with_row_range`. No dependency or query-engine integration is
  added. The implementation remains inside `shardloom-vortex` and its existing
  local-primitives feature boundary.
- A small read-only getter in the existing reviewed layout patch exposes the
  typed aggregate-function declarations already held by a zoned layout. It
  avoids treating a display name as proof of exact aggregate semantics. It
  does not change the format, writer policy, statistics or execution provider.
- The source is the already held Vortex file and its native segment source.
  Preparation, generation validation, cancellation and ownership remain with
  the existing resident session. No source reopen or answer cache is added.
- Only typed integer Min/Max and NullCount partials are consumed initially.
  Bounded string extrema, unknown aggregates, legacy statistics and approximate
  bounds are not silently promoted into exact answers.
- Metadata tables may be decoded by the native provider. Source payload arrays
  remain encoded until the existing residual consumer requires otherwise. No
  Arrow execution substrate or external residual evaluator is introduced.

## Coverage and execution

Walk the actual struct/chunked/zoned layout tree with checked offsets, depth
and cardinality limits. Validate every child length, every nonzero zone length,
the expected number of zones, and contiguous complete per-column coverage.
Unsupported layouts decline optional admission. Corrupt lengths or coverage
fail explicitly; they are not missing-statistics cases.

Choose region boundaries from the intersection of all required columns' zone
boundaries. Each region therefore contains only whole zones of every column.
Different zone sizes and chunk boundaries can coarsen the regions; they must
never lead to proportional allocation of a whole-zone partial. Short final
zones use their actual row lengths.

For a fully qualifying region, stage all measures before merging any of them.
If any required partial is absent, retain the whole region for the ordinary
native scan. All-null extrema contribute no value; COUNT(column) requires an
exact null count or a non-nullable input type. Integer extrema never pass
through floating-point conversion. Merge admitted contributions into the
existing scalar state, then scan unresolved disjoint ranges in source order
using the same projection, filter, provider concurrency and finalizer.

Range scans are created lazily, one live iterator at a time. No input-sized row
index expansion, new selection representation, parallel scan family or complete
input materialization is introduced. An error or cancellation returns no
successful partial result. A source-generation change is rejected at the
existing complete execution boundary.

## Resources and evidence

Query metadata and range/zone containers reserve their capacity from the native
session's existing shared pool before allocation. Metadata payloads keep their
native provider owners. Depth, fields, tables and total zones have explicit
bounds. Optional admission refusal leaves the ordinary scan intact; corruption,
allocation failure and execution errors are not reclassified as optional
refusals. Credits survive every retained container and release on all exits.

The regional proof distinguishes source rows answered from metadata, excluded
rows and residual source ranges. Their sum must equal the held source length.
Residual range lengths are not delivered rows: filtering may suppress rows.
Record metadata-table scans/rows, payload arrays actually delivered, complete
measure coverage, provider version and reservation scope separately. Existing
completed requested-read counters include metadata, repeats and coalescing
gaps; they are not device or unique bytes. The existing whole-file footer proof
keeps its own schema and is not used to claim that zoned metadata required no
reads.

PulseWeave admission and the shared byte budget bound optional work; capillary
units are existing native row ranges. Metadata-first execution avoids admitted
source work. Dynamic scheduling remains with the provider. Engine execution
time remains the comparison clock; no new timing surface is introduced.

## Verification and retention

Focused correctness must cover independently zoned columns, chunk boundaries,
short final zones, nullable/all-null/empty inputs, integer extremes, boundary
predicates, mixed answered/scanned/excluded regions, missing statistics,
unsupported shapes, mismatched counts, gaps/overlaps, cancellation, source
changes, reservation refusal and complete cleanup. Compare complete results
with independently computed values and verify actual source work observations.
Exercise the shared public SQL, embedded, Python SQL, DataFrame and typed-plan
surfaces, plus retained result/output ownership.

At the coherent source boundary run formatter, strict default/native workspace
Clippy and tests, relevant feature checks and installed-package tests. Include
the changed excluded provider in its required checks. Preserve every failed
attempt and exact source/runtime identity.

Before any measured query, freeze the complete comparison declarations,
references, source/input/runtime identities, expected activation/refusal,
resource and work witnesses, and the retention decision. Reuse prior baseline
observations only where all complete identities match; append separately
versioned actual zoned-layout controls. Correctness, no-fallback, ownership,
cleanup and observed work avoidance precede performance admission. Retain only
if the complete declared engine-time total improves under the standing rule;
equal/higher drops the optimization. Keep every family regression and failure.
Prepared-lifecycle observations have their own declared complete comparison.

No Full43, full10k, competitor repeat, writer-default change, merge, publication
or release is implied. Item 19, item 20, RFC 0044 and CG-1 through CG-23 remain
open beyond this bounded investigation.
