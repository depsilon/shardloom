<!-- SPDX-License-Identifier: Apache-2.0 -->

# Metadata and encoded-consumer investigations

Status: accepted into the investigation and implementation queue by the
maintainer's November 13 request. This work follows the active
[optimization-coverage implementation](native-optimization-coverage-2026-11-13.md),
including Vortex scan gaps, COUNT/cyclic connections, public breadth, native
runtime filters, skew handling and cross-query reuse. The
[phase plan](phased-execution-plan.md#planned) owns execution order.

The supplied research describes source checked at `72680743a34a5db9`, after
PR #1683. It is an intake, not fresh validation of later implementation. Recheck
the current source and pinned provider before selecting a design. Its historical
timings and earlier experiment results retain their original source attribution;
this queue update runs no benchmark and admits no performance claim.

## Decision and scope

Treat these as major investigations that can produce shared engine alterations,
provider integration, execution tests and complete public-operation evidence.
Finish each with an explicit implemented/retained or evidenced decline/drop
disposition. A literature review, declaration count, planning report or new
counter does not itself complete the work.

Use existing item-20/RFC-0044 source, aggregate, expression, key, selection,
memory and scheduling owners. Extend the existing
[structural investigations](native-structural-work-investigations-2026-11-11.md)
N for dictionary expressions and I for survivor batching; attach spatial work
to [item 19](native-rich-data-lifecycle-2026-10-24.md). Do not create duplicate
phase IDs or parallel copies of those investigations. This intake does not
automatically revive a previously dropped mechanism unchanged.

CLI SQL, embedded workflow, Python SQL, DataFrame and typed-plan interfaces must
converge on the same admitted engine implementation. Vortex remains native input, execution
representation and output. Other engines may supply independent correctness
references only. No Spark, DataFusion, DuckDB, Polars, Velox or Vortex
query-engine integration may execute residual work. Dependency and copied-code
provenance remain Apache-2.0-compatible and explicitly reviewed.

## Sequence and deliverables

The maintainer's subsequent November 13 efficiency request orders this work by
dependencies and shared validation. Begin only after the active activation unit
has a valid accepted baseline. Reconcile its new facts/counters with the existing
10k inventory, adding separately versioned missing cases for mixed aggregates,
real dictionaries, actual FSST inputs and encoded-float semantics. This is a
delta review of existing evidence, not another whole-engine inventory phase.

| Execution order | Work and existing owner | Why it belongs here |
| --- | --- | --- |
| First | Per-measure metadata completion, then exact regional partials and constant grouping keys | All extend the same aggregate state/finalizer. Prove residual measures first, then disjoint region coverage, then weighted group attachment. Each has its own acceptance decision. |
| Next | Dictionary-domain text transforms and consumers, extending N | Establish actual dictionary retention/expansion and exact consumer semantics before selecting a compressed-text alternative. |
| Next | Named exact source/domain facts, extending C, then ALP/FastLanes predicate admission | Reuse compatible proofs from metadata work and retain only facts an actual consumer needs. Do not build a general fact system in advance. |
| Next | Nonspatial predicate-specific zone evidence: native Bloom first, then necessary text signatures | Prove conservative upstream exclusion and measure surviving work before tuning downstream execution. ALP acceptance is not a semantic prerequisite for independent Bloom/text work. |
| Next | Survivor batching and separate logical-zone/read/consumer attribution, extending I | Settle bounded selections, pinned bytes, flush points and actual preparation reduction before matcher-lifetime or codec tuning depends on batch shape. A documented decline leaves the existing batch contract in force. |
| Next | FSST candidate prefilter, then a separate exact-table matcher-lifetime experiment | Use the accepted encoded consumer and batch contract; retain construction, dense-candidate and nonpredicate controls. Neither result authorizes a writer default. |
| Next | Required item-19 coordinate/offset spatial execution, then optional persisted spatial summaries | Exact geometry/type/invalid-value semantics must precede a summary that can suppress work. Functional support does not depend on successful text, codec or fusion experiments. |
| Last | Consumer-specific representation/codec choices and any new bounded CPU fusion | Attribute the work remaining after accepted upstream reductions. Reuse the established consumer contract; keep the prior layout/fusion drops intact. |

The descriptive sections below retain each original contract; the table and
phase plan own execution order. Within a work unit, reuse fixture generation,
source bindings, independent references and narrow regression checks where their
identities match. Run required broad/native/package gates at the coherent source
boundary. Freeze and decide each distinct performance experiment independently;
do not add their savings or silently repeat measured declarations. An unavailable
proof requires an explicit bounded decline or missing-evidence disposition and
does not hold unrelated ready work indefinitely.

Reconcile overlaps once: B remains the corrected direct COUNT bridge; C owns
reusable source/domain facts; N owns dictionary transforms; I owns survivor
batching. Active source-segment reuse is not join-build reuse, and post-key-
validation membership rejection is not source scan pruning. The older D and
recommendation-13 obligations therefore retain their distinct missing work.

### Per-measure metadata completion

Extend the existing footer aggregate and shared aggregate state/finalizer so
one unresolved measure does not discard independent exact metadata answers.
Begin with the current exact integer COUNT/MIN/MAX contracts. Bind the entire
declaration first, prove metadata answers and their domains, derive residual
measures and demanded columns, and finalize one complete result with unchanged
types, aliases, null behavior, ordering, errors and ownership.

Use mixed-column and shared-column cases, all-metadata and no-metadata controls,
empty/all-null data, missing/inexact statistics, changed generations and late
errors. Prove columns/reads/accessor preparation/aggregate updates actually
avoided. Do not claim saved bytes when that boundary is unobserved. The separate
[grouped COUNT bridge](native-direct-count-bridge-2026-11-12.md) and its invalid
baseline remain distinct; neither is silently relabeled as this mechanism.

### Dictionary-domain text execution — extend N

Reconcile the existing native dictionary owner and pinned provider rewrite with
ShardLoom's exact lower/upper/trim/length/regex semantics. Transform an admitted
values domain and retain codes into a named filter, grouped aggregate or distinct
consumer. Evaluate compatible multiple outputs sharing one owner after the
single-consumer path is proved. Full output expansion is an explicit control.

Keep dictionary entry count distinct from logical row count. Transforms may
merge values; unrelated dictionaries' codes are not semantic identities. Preserve
null codes, referenced versus unreferenced invalid values, fallible functions,
nonconstant sibling arguments, Unicode semantics, detached-code ownership and
changed dictionaries. Measure values transformed, code work, expansion and
retained memory through complete public operations. This is the existing N
investigation with a stronger source/consumer contract, not a duplicate item.

### FSST candidate prefilter and matcher lifetime

First use existing FSST inputs. Investigate a conservative candidate filter
followed by the exact symbol-aware matcher, alongside the existing direct
compressed matcher and admitted native decoded path. False positives may add
verification work; false negatives are forbidden. Strategy admission must
include actual pattern/encoded-data characteristics, construction and scratch
cost, dense candidates, escaped/symbol-boundary cases, nulls and exact semantics.
Do not import hardware-specific thresholds as universal constants.

Separately evaluate request-scoped matcher reuse across slices only when symbol
table contents, symbol lengths, pattern and options match exactly. Bound and
credit tables and identity checks; prove changes invalidate reuse. Record
candidate positions, exact matcher work, construction and retained bytes.
An x86 result is not Apple Silicon evidence.

Preserve the [prior FSST drop](fsst-consumer-screen-2026-09-26.md). A new consumer
mechanism can justify a new experiment; it does not authorize changing writer
defaults. Any later writer decision needs preparation, artifact size, reads and
nonpredicate consumers in its own complete acceptance workload.

### Exact regional aggregate partials

Reuse native zoned-layout aggregate/path and partial-state concepts. For an
identified region, distinguish no qualifying rows, complete qualification with
an exact compatible aggregate partial, and unresolved coverage requiring a scan.
Merge stored and newly scanned contributions through the existing state/finalizer.

Prove disjoint, exhaustive row coverage across independently zoned columns.
Whole-zone metadata cannot answer an arbitrary subset. Missing/inexact facts,
nullable predicates, changed generations and incompatible aggregate semantics
retain the required scan. Reconcile covered and scanned rows/regions exactly;
verify overlap/gap corruption, boundary predicates and metadata-poor controls.
Ordered floating SUM/AVG stay outside the initial integer/count/extrema domain;
reassociation needs a separate accepted rounding and error contract.

Only then investigate the grouped extension: every grouping key must be proven
constant over the same covered rows before contributing one weighted native
group update. Reuse Constant/RunEnd and existing aggregate states, with separate
coverage, null, grouping-equality and cost proof.

### Fact-backed ALP/FastLanes predicate admission

Check actual encoded-float exposure and the pinned comparison kernels first.
Carry only exact, reusable facts needed by a named comparison consumer: held
source generation, observed row domain, finiteness, exact typed-literal mapping,
null and exception semantics, and unchanged observable failure/lazy behavior.
Use existing encoded comparisons and selection masks when those proofs hold.

Do not simply remove the typed planner's float exclusion. Sampling does not
prove finiteness. Charge proof construction and validation; show whether facts
are reused and compressed execution/decoded-array avoidance actually activates.
If every request must decode the same full column to establish the proof, keep
that cost in the complete comparison. Missing proof retains the legal shared
native path with a specific admission reason.

### Predicate-specific zone evidence

Check the existing native Bloom aggregate implementation and its format,
layout and predicate admission before inventing another summary. Evaluate
equality/IN on unclustered identifiers, then required-literal/trigram signatures
for selective text. Persisted spatial bounds follow the exact native spatial
execution/type contract below and require actual workload exposure. Surviving
candidates still receive exact evaluation.

Include construction, persisted size, metadata reads, false-positive rate,
excluded regions and downstream work. Text case/character/pattern semantics must
prove that a tested signature is necessary for a real match. Preserve missing,
inexact and unsupported-summary behavior and generation identity. The
[prior unconditional text-zoning drop](../benchmarks/perf-text-layout-pruning-2026-09-05.md)
remains authoritative for that experiment; this intake admits no unconditional
new writer summary or claim that all metadata benefits every workload.

## Additional connected investigations

- **Codec and consumer cost together.** Inspect existing numeric
  BtrBlocks/cascading integration before proposing changes. Within selected
  consumer experiments, compare retained representation, preparation, size,
  bandwidth, decode and downstream compute together. Do not duplicate existing
  compression machinery or launch a generic codec sweep without exposure.
- **Separate logical zones, physical reads and CPU batches — extend I.**
  Attribute work at each boundary. The existing
  [column-addressable layout](perf-column-addressable-writer-2026-09-06.md)
  rearranges footer references, which is distinct from larger physical segments
  or cross-batch codec coalescing. Integrate survivor-driven consumer batches
  only with pinned-backing-byte, preparation-count, source-order, demand and
  dense/tiny/wide controls. Preserve the original layout observation.
- **Distinct bounded CPU expression fusion.** Attribute materialized
  intermediates/dispatch in multi-node projections or predicates before altering
  the shared expression evaluator. Preserve the
  [October 25 fused-measure design](native-fused-measure-work-elimination-2026-10-25.md)
  and [batch drop](../benchmarks/native-workflow-drop-2026-10-25.md). A new shape,
  mechanism and protocol are required; do not reintroduce that candidate
  unchanged or transfer a GPU-only speed claim to CPU execution.
- **Native spatial execution — item 19.** Begin with coordinate/offset arrays,
  conservative bounds and exact geometry kernels for surviving candidates.
  Connect spatial filters and joins to the shared engine with declared types,
  null/empty/invalid geometry semantics, selection and source ownership. Prove
  no geometry-object materialization is required in the selected native path.
  Spatial support remains required; any summary/codec optimization still needs
  its own evidence. It does not explain historical 10k timings.

## Evidence, compatibility and acceptance

Freeze each distinct experiment before measurement: exact public requests,
runtime/input identities, independently computed complete values and schema,
resource/effect contracts, activation counters, negative controls and the
complete-cohort decision. Extend rather than mutate the frozen 10k when coverage
requires plain/dictionary/FSST/ALP, metadata-rich/poor, clustered/unclustered,
selective/dense or rich/spatial cases. Count eligible and actual activations
separately. Reuse observations only when their complete identity is compatible.

Preserve all failures, rejected mechanisms and family regressions. Run the
required default/native validation and relevant installed public-surface proof
on the final source. During performance execution use one isolated pass with
other task work idle, existing storage/deadline/cleanup controls and engine time
as the comparison clock. Correctness, resource, ownership and no-fallback proof
precede retention: lower complete-cohort engine time retains an optimization;
equal/higher drops it under the standing rule. Functional spatial acceptance and
any separately specified resource objective retain their own explicit criteria.

Public syntax and result contracts stay stable unless a reviewed capability
extension is required. New diagnostics must distinguish planned, admitted,
executed, declined and unobserved work. Query-specific routes, uncontrolled
global caches and unconditional writer changes are outside this decision.
No release, publication, merge or completion of item 19, item 20, RFC 0044,
or CG-1 through CG-23 follows from queue admission. CG-5/CG-6 remain necessary
for competitive claims; commodity CPU execution remains the primary target.

## Research supplied for investigation

These are the maintainer-supplied source leads, not newly verified current
implementation claims. Read the relevant original and pinned provider source
when beginning its work unit:

- [Statistics are aggregates](https://spiraldb.com/blog/statistics-are-aggregates)
- [Pattern matching without decompression](https://spiraldb.com/blog/pattern-matching-strings-without-decompression)
- [Compile or prefilter for compressed LIKE](https://spiraldb.com/blog/compile-or-prefilter-like-over-strings)
- [Dictionary-domain computation](https://spiraldb.com/blog/what-if-we-just-didnt-decompress-it)
- [Composable storage](https://spiraldb.com/blog/vortex-the-storage-layer-for-the-composable-data-stack)
- [Zone maps](https://spiraldb.com/blog/zone-maps-or-queries-go-brrr)
- [Cascading compression](https://spiraldb.com/blog/cascading-compression-with-btrblocks)
- [Data layouts](https://spiraldb.com/blog/data-layouts-where-bytes-find-their-forever-home)
- [GPU kernel fusion](https://spiraldb.com/blog/fusing-gpu-kernels-with-dynamic-dispatch)
- [Native geometry](https://spiraldb.com/deep-dives/geometry)
