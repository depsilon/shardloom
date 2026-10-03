<!-- SPDX-License-Identifier: Apache-2.0 -->

# Universal workflow breadth and scale

Status: implementation plan for the October 1 product clarification. This document
does not claim new runtime support or measured performance. Active phase owners
remain PERF-02/03/06/07/10/11/12 in the [phase plan](phased-execution-plan.md).

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
paired Full43 comparisons. Hosted acceptance remains pending. Wider types/adapters,
dynamic-schema composition and remaining resource/spill transitions require the work below.

| Area | Existing foundation | Completion requirement | Owner |
| --- | --- | --- | --- |
| Sources and types | Local adapters, schema admission, Vortex preparation, native files/partitions, bounded generated and memory-visible inputs. | Broader typed/nested schemas, partition/schema evolution and source adapters; retain fidelity and source identity. | PERF-11; CG-19/20/21 |
| Operator composition | Native flat-scalar relational stages and eight shared unary families compose with ordered public declarations; the static nested continuation adds payload transport and repeated explode. | Wider join/set/window/subquery semantics, nested key/unary-state semantics and dynamic pivot composition; broader prepared/public parity. Use the existing twelve-family inventory. | PERF-02/10; CG-20/21 |
| Results and writers | Owned Vortex arrays, shared local writers and bounded native batches for executable flat-scalar aggregate/ordered output, including admitted spill output; bounded static nested output has six representable destinations. | Extend result streams through the remaining operator/type families and broader chains; preserve format-specific denials and fidelity. | PERF-07/11; CG-3/19/21 |
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
composition now have their own acceptance records. Finish the static nested
continuation's acceptance, then continue through missing dynamic-schema and type
families with their ownership contracts. Freeze exact
expressions, sinks and pressure cases against current source at intake. Unsupported
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
