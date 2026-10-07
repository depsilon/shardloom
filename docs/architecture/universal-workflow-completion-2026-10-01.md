<!-- SPDX-License-Identifier: Apache-2.0 -->

# Universal workflow breadth and scale

Status: implementation plan for the October 1 product clarification. The local
acceptance records below document finite runtime units; they do not establish
hosted release completion or performance superiority. Active phase owners remain
PERF-02/03/06/07/10/11/12 in the [phase plan](phased-execution-plan.md).

## Product contract

ShardLoom is intended to be a first-class general-purpose data execution option.
Users describe a source, transformations or query, and a destination through SQL,
Python, DataFrame-style calls, or the CLI. Every supported workflow uses one pipeline:

```text
input adapter -> Vortex-native representation -> ShardLoom execution -> output adapter
```

Formats and external systems connect at the boundaries. Metadata-first planning,
pruning, encoded execution, capillary scheduling, PulseWeave resource control, native
spill and late materialization belong in shared runtime components. Applicability
depends on semantics, data, layout and resource pressure. Users must not need an
alternative execution mode to obtain ShardLoom's performance mechanisms.

`SourceState`, `VortexPreparedState`, `prepared_vortex` and `native_vortex` describe
source lifecycle and evidence. They are not separate product engines or a menu of
fast and slow modes. Cold and warm measurements still distinguish preparation from
repeated execution. Reuse validates source generations and does not reuse a query
answer. Existing diagnostic identifiers remain unchanged by this documentation work.

This follows the [universal input contract](universal-input-contract.md),
[RFC 0031 native I/O envelope](../rfcs/0031-universal-native-io-envelope.md),
[format-neutral front door](v1-front-door-runtime-scope.md#format-neutral-route-model)
and [RFC 0033 workflow contract](../rfcs/0033-user-data-workflow-etl-surface.md).
The native middle need not imply a complete persisted intermediate before execution:
RFC 0031 work/result streams and implemented bounded memory-visible intake fit the
same architecture. Broad streaming composition still needs work.

ClickBench is one regression and comparison workload. Its schema, row count, query
set and scenario names must not determine product capability. New support is admitted
by reusable operator and type semantics, with complete workflow evidence.

The October 2 maintainer clarification applies this reuse requirement to every new
shape and optimization lane. Each implementation packet must identify the existing
component and callers, document the remaining semantic gap, and extend that shared
component or its strategy before introducing another implementation. Different
format/frontend wrappers must not own duplicate scan, predicate, aggregate, join,
ordering, memory or delivery algorithms. Shared infrastructure alone does not prove
that general composition uses every specialized ClickBench strategy; the
[composition ownership map](native-relational-composition-2026-10-02.md#reuse-ownership)
records the current distinction. Reuse validation must cover existing callers and
an independently specified composed workflow, alongside measured retain/drop evidence
for performance changes.

## Current implementation and completion gaps

The [native family inventory](native-runtime-completion-2026-09-20.md#finite-availability-inventory),
[public front door](v1-front-door-runtime-scope.md),
[local sink scope](v1-local-output-sink-scope.md), and
[result composition contract](native-result-composition-2026-09-20.md) own exact
implemented support. This plan does not replace their capability records.

The [retained unary unit](native-unary-workflows-2026-10-01.md) and
[native relational unit](native-relational-workflows-2026-10-01.md) extend complete
collection and all eight local writers through their admitted flat-scalar families.
Relational source declarations also survive mixed-input joins, sets and nested
predicate helpers. The [October 2 unary composition unit](native-unary-composition-2026-10-02.md)
connects eight existing flat-scalar unary families to the shared relational
traversal and writers. Its frozen build passes 1,963 complete public checks and
129/129 Full43 comparisons. The [static nested continuation](native-nested-composition-2026-10-02.md)
implements owned list/struct payload transport and ordered/repeated explode; its
frozen build passes 2,459 public checks, including 496 nested checks, and all 258
paired Full43 comparisons. Fresh acceptance after the three review repairs also
passes the required Q21 memory repeat; the
[new report](../benchmarks/native-nested-review-full43-2026-10-03.md) preserves the
original measurements and the unreproduced memory flag. Runtime
[PR #1506](https://github.com/depsilon/shardloom/pull/1506) and the separate website
dependency [PR #1517](https://github.com/depsilon/shardloom/pull/1517) both merged
after all 40 hosted checks passed. The [dynamic pivot continuation](native-dynamic-pivot-composition-2026-10-03.md)
connects scalar pivot-domain discovery to the same binder, operators and sinks,
including separate correlated parameter scopes. Its frozen local runtime passes
3,305 public checks, including 846 pivot checks, all 24 selected local gates and
all 258 paired Full43 retained-result comparisons. Aggregate timing is unchanged
at the predeclared thresholds; the [acceptance report](../benchmarks/native-dynamic-pivot-full43-2026-10-03.md)
preserves the complete observation. The
[typed payload continuation](native-typed-payloads-2026-10-03.md) carries exact
binary, Decimal128, Date32 and timezone-free microsecond timestamps, including
static nested leaves, through the same native result and writer components.
Its frozen `8237a900` passes 4,109 public checks (804 typed), 202 direct-unary
checks, all 24 selected local gate categories and all 258 paired Full43 results;
the [typed acceptance report](../benchmarks/native-typed-payloads-full43-2026-10-03.md)
preserves exact scope and evidence. The dynamic pivot and typed payload units
merged in PRs #1507 and #1508 after all 37 hosted checks passed for each. Flat typed-key
hashing, equality, ordering, expression selection and aggregate extrema now have
local acceptance on `2f402226`: 5,733 public checks/12,282,897 rows, a 2,428-check
typed subset/4,595,372 rows, 202 direct checks/131,734 rows, all 24 local gate
categories and 258/258 Full43 results plus six reversed-order Q21 repeats. No
aggregate or RSS flags remain, and no speedup is claimed; see the
[typed-key acceptance report](../benchmarks/native-typed-keys-full43-2026-10-03.md)
and [immutable evidence packet](../benchmarks/evidence/native-typed-keys-2026-10-03.json.xz).
The [typed-expression continuation](native-typed-expressions-2026-10-03.md)
extends the existing binder and checked scalar helpers with literals, explicit
casts, exact decimal arithmetic/rounding and binary/calendar functions. Frozen
review source `895a45c9` passes 6,600 public checks/14,120,333 rows, including 868
expression checks/1,837,436 independently specified rows. The separate direct
matrix passes 202 checks, all 24 local gate categories pass, and Full43 passes
258/258 with no timing/RSS/aggregate threshold crossed. The Decimal branch
correction adds 90 checks and preserves the original cases and complete oracles;
no speedup is claimed. See the [acceptance report](../benchmarks/native-typed-expressions-full43-2026-10-03.md)
and [review packet](../benchmarks/evidence/native-typed-expressions-review-2026-10-03.json.xz),
which retain the original acceptance as a separate immutable observation.
The typed-key and typed-expression units merged in PRs #1509 and #1510 after all
37 hosted checks passed for each. The [typed-unary continuation](native-typed-unary-2026-10-03.md)
extends the existing retained state with exact binary, Decimal128, Date32 and
microsecond timestamp values for selectors, rewrites, melt, rolling COUNT and
scoped pivot policies. Frozen `948551d4` passes 9,300 public checks/14,125,745 rows,
including 2,700 new unary checks/5,412 rows, the 202-check direct matrix and all 25
local gate categories. All 258 paired Full43 results and 18 prescribed repeat
calls match; aggregate thresholds are not crossed, while Q9's timing/RSS
observation remains inconclusive. The [report](../benchmarks/native-typed-unary-full43-2026-10-03.md)
and [packet](../benchmarks/evidence/native-typed-unary-2026-10-03.json.xz) retain all
observations, independent unary oracles and the two interrupted public attempts.
The typed-unary unit merged in PR #1515 after all 37 hosted checks passed.
The [nested key/state continuation](native-nested-keys-state-2026-10-04.md)
extends the same key and retained-buffer owners to static lists, fixed-size lists
and structs, including admitted typed leaves. Recursive logical comparison,
COUNT/DISTINCT/MIN/MAX, selected expressions and scoped unary state pass local
acceptance on frozen `d65907f6`: 17,458 public checks/14,143,015 complete row
comparisons, including 8,162 new checks/17,270 rows and 406 independent
declarations; 202 direct checks; and all 25 local gate categories. All 258 paired
Full43 results and six Q28 repeats match. No aggregate/RSS screen is crossed;
the initial Q28 timing gain does not reproduce. The
[report](../benchmarks/native-nested-keys-state-full43-2026-10-04.md) and
[independently verified packet](../benchmarks/evidence/native-nested-keys-state-2026-10-04.json.xz)
retain every observation and all four interrupted public attempts. The unit
merged in PR #1516 after all 37 hosted checks passed. Exact head/merge identities
and review limitations for all seven units are in the
[completed ledger](phased-execution-completed-ledger.md). Passing checks do not
turn unavailable automated review into approval.

The [typed reduction and universal-route consolidation](native-typed-reductions-2026-10-04.md)
now has complete local acceptance on the revised engine: 20,445 public checks
and 14,282,070 complete row comparisons, 202 direct retained-workflow checks,
all 129 shared-engine Full43 executions, 22 core source gates, 144 admitted
semantic stages and nine golden workflow stages. The parameterized workload
harness adds 1,408 complete records plus a separate 32-record probe. The
[October 5 report](../benchmarks/native-typed-reductions-full43-2026-10-05.md)
links the independently inspected packet and records exact runtime, checker
and source-tree identity. Hosted integration completed in PR #1518.
The [analytic-frame continuation](native-analytic-frames-2026-10-05.md) has
complete local acceptance on `22f1e6ba`: 22,658 public checks and 15,349,350 row
comparisons, including 2,213 frame checks and 1,067,280 independently specified
rows. All 129 Full43 executions, 202 direct checks, 22 source gates, 145 admitted
semantic stages and nine golden stages pass. Its
[report](../benchmarks/native-analytic-frames-full43-2026-10-05.md) records the
successful independent packet inspection. Hosted integration completed in PR #1519;
both units are included in the [verified v0.4.0 release](../release/v0.4.0-publication-verification.md).
The [scalar-value continuation](native-scalar-subqueries-2026-10-05.md) has complete
local acceptance in current source after v0.4.0: 23,786 public checks, including
1,128 scalar checks, and all 129 Full43 runs. Its
[report](../benchmarks/native-scalar-subqueries-full43-2026-10-05.md) preserves
the frozen source and independent packet inspection. Hosted integration completed
in PR #1524 after all 39 checks passed, with the accepted tree preserved in main.
Published v0.4.0 packages predate this addition. The
[nested pivot continuation](native-nested-pivot-state-2026-10-06.md) now has
complete local acceptance: 27,373 public checks / 15,820,181 complete rows,
202 direct checks and all 129 Full43 runs. Its
[report](../benchmarks/native-nested-pivot-state-full43-2026-10-06.md) records
the frozen source, native schema and resource proofs. Hosted integration completed
in PR #1525 after all 39 checks passed, with the accepted tree preserved in main.
The subsequent local [provider resource unit](native-provider-resources-2026-10-06.md)
accounts for reviewed FSST/Zstd buffers through retained native owners. The
[batch adapter unit](native-bounded-adapters-2026-10-06.md) adds public
`from_batches` and `iter_batches`, with demand-driven resident intake and
acknowledged result delivery. Both have complete local acceptance at combined
commit `99e0a4b3` and merged in PR #1526 after all 39 hosted checks passed.
Actual Zstd decoder/dictionary workspaces subsequently merged in PR #1528.
The [native builder unit](native-builder-resources-2026-10-07.md) now has complete
local source/public/Full43 and packet acceptance at `53cd1582`, with hosted
integration pending. It owns finite Chunked value/validity/finalization buffers;
the [report](../benchmarks/native-builder-resources-2026-10-07.md) retains all
measured costs and remaining allocation boundaries.
Remaining adapters and resource/spill transitions retain their owners.

| Area | Existing foundation | Completion requirement | Owner |
| --- | --- | --- | --- |
| Sources and types | Local adapters, schema admission, Vortex preparation, native files/partitions, bounded generated and memory-visible inputs; binary, exact Decimal128, Date32 and microsecond timestamp payloads, including admitted nested leaves. | Broader typed/nested semantics, partition/schema evolution and source adapters; retain fidelity and source identity. | PERF-11; CG-19/20/21 |
| Operator composition | Native relational stages and shared unary families compose with ordered public declarations; static nested payload/explode, scalar pivot schemas and typed/nested state are merged. Computed aggregate arguments, exact decimal aggregate/rolling/pivot state and analytic frames are merged and published through the consolidated engine. Scalar-value subqueries and [nested pivot state](native-nested-pivot-state-2026-10-06.md) are merged with local/hosted acceptance in source builds after v0.4.0. | Broader prepared/public parity; use the existing twelve-family inventory and preserve explicit unsupported semantics. | PERF-02/10; CG-20/21 |
| Results and writers | Owned Vortex arrays, shared local writers and bounded native batches for executable flat-scalar aggregate/ordered output, including admitted spill output; bounded static nested output preserves types in six representable formats, including the four new typed leaf families, and supports CSV as explicit JSON-text translation. | Extend result streams through the remaining operator/type families and broader chains; preserve format-specific denials and fidelity. | PERF-07/11; CG-3/19/21 |
| Volume and pressure | Reservations, worker/queue admission, selected COUNT/DISTINCT/numeric-sort spill and cleanup. | One accounted resource envelope through reader, codec, operator, retained state and sink; broader native spill and recovery. | PERF-03/06; existing resource/recovery gates |
| Acceptance | Full43, renamed-schema checks, public calls and focused ownership/resource tests. | Complete workflows across schemas, formats, result sizes, skew and constrained resources; all public surfaces share execution. | PERF-12; CG-5/6/21 |

The 65,536-row / 128-top-level-field / 8-MiB limits remain on small computed-result
collection. The [October 1 result-stream unit](native-workflow-streaming-2026-10-01.md)
gives already executable flat-scalar aggregate and ordered local writes a separate
bounded batch boundary, with complete output above the collection row and byte
limits. The nested continuation extends admitted fields to bounded static
list/struct payloads, with recursive schema and child-buffer admission. These are
not input-size limits. Remaining operator state, spill and
composition gaps still need their own resource proof. Successful file ingestion
alone cannot establish a successful query and output workflow.

## Cohesive implementation sequence

The first runtime unit connects bounded native result composition and local output
for already executable flat-scalar aggregate and ordered-result families; its
[contract and acceptance](native-workflow-streaming-2026-10-01.md) record exact
coverage. Retained unary execution and admitted flat-scalar relational/unary
composition now have their own acceptance records. The static nested, scalar
dynamic-pivot, typed-payload, typed-key, typed-expression, typed-unary and
nested-key/state continuations are merged. The
[typed reduction/consolidation unit](native-typed-reductions-2026-10-04.md)
and [analytic-frame unit](native-analytic-frames-2026-10-05.md) are also merged and
published in v0.4.0. The [scalar-value unit](native-scalar-subqueries-2026-10-05.md)
has complete local and hosted acceptance in source builds after that release,
with PR #1524 merged after all 39 checks passed. The
[nested pivot state unit](native-nested-pivot-state-2026-10-06.md) also has complete
documentation and hosted integration in PR #1525. The finite provider memory
and batch-adapter continuations merged in PR #1526, followed by actual Zstd
workspace admission in PR #1528. Builder output/finalization ownership has
complete local acceptance and awaits hosted integration. Continue step 2 with
the remaining reader/codec scratch and operator spill/recovery obligations.
The October 7 [state and structure campaign](native-state-structure-campaign-2026-10-07.md)
prioritizes completion-aware single-use input through the shared native
filter/project/write path, plus a measured retained-intermediate target for
selective regeneration. Existing input batches remain resident until that
capability's implementation and acceptance complete. Distinct multiway-join,
nested-identity and stable spill-merge experiments retain their own gates;
completed hardware and composed-COUNT drops are not reopened.
Broader adapter and resource families continue under their ownership contracts.
The [local-engine maturity criteria](../release/production-certification-gate.md#local-engine-preview-exit-criteria)
require operational acceptance of a declared support envelope; package availability is complete,
and cloud/complete-SQL parity is not a blanket prerequisite for that local promise.
Freeze exact expressions, sinks and
pressure cases against current source at intake. Unsupported
extensions need a concrete remaining checklist rather than a permanent benchmark-only
designation. Availability work ships on correctness and resource proof; a speedup is
not required for completing a missing workflow.

1. **Connect complete results to consumers — PERF-07/11.** Carry native arrays,
   schema, validity, row order, selection and buffer ownership through bounded
   batches into downstream operators and local writers. Preserve typed empty results.
   Avoid scalar/JSON reconstruction and query re-execution. Retain small `collect`
   limits where the API explicitly requests in-memory delivery; streaming writes
   need an independent, bounded completion contract.
2. **Carry resources and failure handling through the workflow — PERF-03/06.**
   Account for reader/codec scratch, operator state, queued batches and writer
   retention; apply backpressure and cancellation across the chain. Reuse native run
   storage and ownership. Complete pressure transitions and spill-to-sink consumption
   for each family before claiming larger-state coverage. Denial must identify an
   actual resource or missing implementation boundary.
3. **Complete relational and type families — PERF-02/10.** Lower general joins,
   set operations, analytic windows and subqueries through shared native providers,
   keys, selections, scheduling, spill and result streams. Extend nested and
   extension-type handling with exact semantics and fidelity contracts. SQL, Python,
   DataFrame and CLI spellings converge on those implementations.
4. **Broaden ingress and destinations — PERF-11 and existing CG owners.** Expand
   format/schema/partition coverage and connectors through RFC 0031 envelopes. Reuse
   native providers first. Remote/table/effectful adapters retain credentials,
   request budgets, commit, cancellation and explicit-effect obligations; source-system
   SQL or another engine may never complete missing ShardLoom work.
5. **Certify each complete workflow — PERF-12.** Add the matrix below alongside
   Full43. Record real input, output, peak resource observations and failure behavior.
   Update capabilities, examples and support only after the workflow executes.

Steps 1 and 2 form one end-to-end implementation unit wherever ownership and spill
are dependencies. Group later families when contracts and verification are shared.
Do not create an implementation per format, frontend or benchmark query, or remove
safe guards before their bounded replacement exists.

## Workflow acceptance matrix

Freeze a finite case list and exact expected values before each implementation run.
Pairwise format coverage is a starting point; every exposed reader/operator/writer
composition must be tested or explicitly accounted for.

| Dimension | Required cases and proof |
| --- | --- |
| Full workflow | Read → filter/project → aggregate or order → consume again → write → reopen and verify complete values, schema, nulls and order. Include empty results and repeated calls. |
| Inputs | Native Vortex and each admitted local compatibility reader; alternate/renamed schemas, partitions and source mutation/invalidation. No ClickBench column-name special cases. |
| Types | Nullable/nonnullable supported scalars, varied strings, integer boundaries, floating semantics and format-specific representability. Nested/extension shapes require executable support and explicit loss rules. |
| Data volume | Small exact fixtures, results below/at/above each handoff bound, many batches, and larger-than-budget source/result/state cases. Prove native spill where completion under pressure is claimed. |
| Distribution | Low/high cardinality, skew, null-heavy and all-unique data, varied selectivity, wide records and small/large outputs. |
| Public surfaces | Equivalent SQL, Python/DataFrame and CLI calls share semantics, source generation, resources and sink contracts; transport overhead is separate. |
| Resources and failure | Slow sinks/backpressure, queue/state quota denial, cancellation at each stage, source replacement, corrupt spill, interruption/recovery and no leaked owners/files/processes. Record accounting exclusions and observed RSS honestly. |
| Correctness and performance | Complete outputs match independently specified results or external test-only oracles. Full43 remains regression evidence; separately measure first use, repeated queries, delivery and non-ClickBench workflows. |

Scale is governed by resource budgets and storage capacity, not an unlimited-data
promise. Do not reinstate the retired fixed 4-GiB target. Use host-appropriate budgets
and smaller test budgets to exercise pressure. Bounded fixtures can cross current
result limits without resuming paused large CSV/JSON or format performance runs.
Large builds and UAT stay serial under the [storage/process guards](local-development-storage.md).

All runtime acceptance retains `fallback_attempted=false` and
`external_engine_invoked=false`. Real Vortex output payload proof remains distinct
from placeholder artifacts. CG-1 through CG-23 stay visible and open until their
own evidence passes; CG-5/6 are required before competitive claims. Foundry remains
an optional CG-18/21/23 integration under RFC 0036. No package publication, benchmark
speedup or production certification is implied by this plan.
