<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native unary operation composition

Status: implementation contract; no new availability is claimed. This continuation
belongs to PERF-02/03/07/10/11/12 and CG-5/20/21 in the
[universal workflow plan](universal-workflow-completion-2026-10-01.md). It follows
the accepted relational composition, resource and aggregate units. It does not
close those whole PERF or competitive gates.

## Contract and boundary

An admitted unary operation must consume the preceding native query stage and
deliver native batches to the next stage in the same execution. Filters,
projections, ordering, limits, joins, sets and aggregates retain their declared
positions. A prefix is not collected through JSON, persisted as an intermediate
file or executed again to discover its row count. SQL and Python/DataFrame
declarations converge on the existing native relational plan and executor.

This unit covers the existing flat-scalar DISTINCT, drop-duplicate,
duplicate-mask, tail, sampling, scalar-rewrite, melt and rolling semantics.
It includes their supported parameter policies and repeated composition before
and after ordinary relational stages. The current list/struct explode and
data-dependent pivot providers retain their existing standalone contracts.
Their wider composition requires, respectively, owned nested payload gathering
and execution-time schema binding. These are concrete type/ownership boundaries
for the following work, not inferred support from a flat-scalar result.
Mixed-domain melt values that produce Vortex Variant share the nested/type
boundary. This unit admits melt with an existing common scalar output type;
incompatible mixed domains are rejected during preparation.

The current small collection bounds, complete local writer boundary, source
generation checks and fixed per-operation CPU/memory allocation remain shared.
Unsupported unary state spill is still an explicit resource denial. A surrounding
admitted relational sort may use its existing spill provider; that permission
does not give unary state an unimplemented spill path.

## Reuse ownership

The expert comparator is an encoded-columnar engine maintainer reviewing operator
order, deterministic selection, schema binding and buffer lifetimes.

| Existing owner | Extension |
| --- | --- |
| `PreparedVortexUnary`, unary schema binder and operation states | Separate source-independent bound operation metadata from file preparation. Both the original prepared file call and relational composition drive the same `State::new/consume/finish` implementations. |
| `VortexRelationalPlan`, binder and synchronous traversal | Add the unary stage to the existing tree. Consume native arrays under the already admitted context; do not reacquire session admission or create a second runtime. |
| `NativeBatch`, reserved containers, `CompletedRows` and native allocator | Reuse scalar access, exact state ownership and bounded typed output. Retained output buffer credits survive the producer and callbacks. |
| Native source metadata and prepared readers | Use checked conservative row bounds where available; an unknown bound remains unknown. Reading rows to estimate a bound is not preparation. |
| SQL relation-source parser and native lowerer | Admit explicit unary table expressions around an existing derived relation, with bounded, validated operation arguments and recursively discovered real source leaves. |
| Python ordered-stage renderer and public facade | Render each unary stage at its true position and carry source declarations, resources and sink requests. Python does not implement selection or reshape algorithms. |
| Shared JSON collector and all eight local writers | Consume the same completed execution and preserve existing collection, publication, cleanup and fidelity contracts. |

Existing optimized flat calls must retain their applicable source pushdown and
metadata work avoidance. The contract does not assert that every composed shape
uses a specialized strategy. It requires explicit shared ownership and complete
semantic evidence for existing and newly composed callers.

Vortex-first provider decision: `implement_shardloom_kernel`. This connects
existing ShardLoom unary state to its existing relational traversal; it does not
introduce another query implementation. Vortex 0.85 `DType`, `ArrayRef`, ordered
scan, native scalar/validity access and allocator-backed result arrays remain the
native provider surfaces. Native `take` can retain source storage and does not by
itself prove compact output ownership. The existing scalar result builder is the
admitted output owner. There is no Arrow execution middle, external query engine,
new dependency or serialized-memory-file bridge.

## State, ordering and schema

- DISTINCT and first-occurrence deduplication reuse their exact key state and
  source-order delivery. Last/remove-all deduplication retains the existing
  candidate and ordinal semantics. Duplicate masks refer to the actual preceding
  stage, including its filtering and ordering.
- File-backed tail retains native suffix-range avoidance. A tail over a produced
  relation retains only the requested final rows, with reserved payload ownership
  and source-order delivery. A late downstream limit must not move before tail.
- Sampling preserves the existing seed, score, weight and replacement rules. A
  conservative input bound may size the same candidate strategy but is not an
  actual population count. Fixed-count sampling retains bounded candidates;
  replacement and any required complete population retention remain explicitly
  charged and reported. Unknown cardinality must not trigger a second execution.
  Complete small fixtures must expose score ties and filters that change the
  population, not merely repeat one random seed.
  Equal scores prefer the earlier input ordinal. This corrects the previous
  candidate-slot tie behavior: replacing the first minimum slot could change a
  weighted result when only the metadata bound changed. Extreme positive finite
  weights can produce tied infinite scores. Non-tied seeded results are unchanged;
  both native and legacy reference helpers use the same ordinal tie contract.
- Scalar rewrites retain their own row ordinal and forward-fill state across
  batches. Independent calls start fresh. Melt emits bounded expansion batches.
  Rolling keeps its existing centered/lookahead/null/min-period semantics and
  bounded window state over the immediately preceding stage.
- The shared schema binder validates each stage before execution, including empty
  output, nullable fields, renamed columns and integer widths. Unknown, duplicate
  or incompatible columns fail explicitly. The flat-scalar binder must not be
  widened to nested or dynamic output without that type's ownership contract.

SQL table expressions are a frontend spelling for these existing operations,
not a UDF/plugin execution facility or general plan import. Their operation
arguments must be fully parsed without opening a source. The existing statement
size, nesting and node limits apply. Source declarations attach only to actual
leaves; function arguments must not be mistaken for paths. The Python renderer
uses this same syntax, and malformed or conflicting declarations fail before a
writer creates output.

The SQL spelling is fixed as the following table expressions. Each input is one
parenthesized SELECT or set query, and each expression requires an `AS` alias.

| Expression | Arguments after the input query |
| --- | --- |
| `DISTINCT_ROWS` | A quoted comma-separated projection, or `'*'`. |
| `DROP_DUPLICATES` | Quoted key columns (`'*'` means all input columns), then `'first'`, `'last'` or `'false'`. Output retains every input column. |
| `DUPLICATED` | The same key and keep arguments; output is the boolean `duplicated` column. |
| `TAIL` | A positive integer row count. |
| `SAMPLE` | One quoted JSON object: exactly one of positive `n` or `fraction` in `(0, 1]`; optional UInt64 `seed`, boolean `replace`, weight-column `weights` and projection `columns` (default `'*'`). |
| `REWRITE` | The existing typed scalar expression-projection JSON payload. |
| `MELT` | The existing melt JSON payload. |
| `ROLLING` | The existing rolling JSON payload. |

For example, `SELECT * FROM TAIL((SELECT amount FROM 'input.vortex' ORDER BY
amount), 3) AS suffix` orders before selecting the final three rows. Payload
parsing reuses the current primitive parsers with an absent file URI. Source
leaves remain exclusively inside the input query. This syntax does not admit
nested/structured rewrites or dynamic pivot composition. Existing valid direct
file declarations keep their source pushdown and suffix-range strategies.

## Acceptance

Freeze independently specified values before the public matrix. Cover every
family and policy through a transformed input and a downstream consumer, with
both SQL and DataFrame spellings where exposed. Include successive unary stages,
transformed join/set/aggregate operands, source-order limits on both sides,
nullable/all-null/empty data, UInt64 boundaries, long UTF8, renamed schemas and
explicit compatibility-source declarations.

Preserve existing flat provider tests and compare complete prefix results when a
new downstream stage is appended. Sampling fixtures must include rejected zero
weights and extreme positive finite weights, tied scores, fixed/fractional counts
and replacement. Rolling
and forward-fill fixtures must cross native batch boundaries. Repeated execution
must produce fresh state and identical deterministic values.

Exercise slow and failing consumers, pre/mid-operation cancellation, source
replacement, narrow memory grants, retained arrays after callback/producer drop,
empty schemas, collection row/byte denial and complete larger output through all
eight local writers. Verify every value after reopen. A failed call must release
owned resources and must not return a success certificate or publish an output.
Inspecting routes remains inert and does not sample data or probe spill paths.

Run required workspace formatting, strict Clippy and tests, native provider/CLI
suites, Python, lean/MSRV and affected documentation gates. Freeze the final build
and complete public workflow matrix, then run Full43 as regression evidence
under the existing serial storage/process guards. Retain all failed observations.
Correctness and ownership establish availability; a speedup is not a gate for
this missing composition work.

Dynamic pivot composition, nested/extension payload ownership, additional adapters,
other state spill, native Python binding experiments, paused large text/format
performance runs and package publication retain their existing owners. All new
execution must preserve `fallback_attempted=false` and
`external_engine_invoked=false`. Real Vortex payload proof remains distinct from
placeholder artifacts, and CG-1 through CG-23 remain visible in the phase plan.
