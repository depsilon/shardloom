<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native scalar-value subqueries

Status: complete core-local acceptance on `8440b04f`; merged in
[PR #1524](https://github.com/depsilon/shardloom/pull/1524) after all 39 hosted
checks passed. The [completed ledger](phased-execution-completed-ledger.md)
records exact identities and review limitations.
The [acceptance report](../benchmarks/native-scalar-subqueries-full43-2026-10-05.md)
records 23,786 public checks, including 1,128 scalar checks, 202 direct checks,
all 129 Full43 runs and independent evidence-packet inspection. Current source
builds include this addition; the published v0.4.0 packages predate it. This is
`NATIVE-SCALAR-SUBQUERIES` under PERF-02/03/06/07/10/12 and CG-5/19/20/21 in the
[phase plan](phased-execution-plan.md), following the merged typed reductions and
analytic frames. The [universal workflow plan](universal-workflow-completion-2026-10-01.md)
owns broader operator, adapter and resource completion. This document does not
claim production support, performance superiority or package publication.

## Semantic contract

A scalar subquery has one statically bound output column. Zero rows produce a
typed NULL; one row produces that value, including a NULL or admitted nested
value; a second row fails with a deterministic cardinality diagnostic. Duplicate
rows still count as multiple rows. Never choose an implicit first row, MIN, MAX
or arbitrary value. Explicit aggregation, DISTINCT, ordering and LIMIT inside
the query retain their ordinary semantics before the cardinality check.

An uncorrelated value is evaluated on first demand in one execution. A correlated
query runs with a fresh native singleton parameter and fresh inner operator state
for each selected outer row. Repeated keys do not authorize a query-answer cache.
All calls retain source-generation checks. Correlation remains explicit through
the existing `outer.<column>` scope; nested scopes must not silently capture the
wrong outer row.

Expression placement includes projections, arithmetic/casts/functions,
comparisons/null tests and admitted aggregate/window arguments. CASE and
COALESCE preserve selected-branch evaluation: an unused branch does not execute
its inner query or raise a value/cardinality error. Every branch still requires
syntax, source, type and capability admission, including empty outer input.
Ordinary Boolean operations and NULLIF retain the existing evaluation contract;
they do not acquire a general short-circuit promise.

Output dtype binds from the inner schema, with top-level nullability widened for
the zero-row case. Preserve integer signedness/width, exact decimal metadata,
binary bytes, temporal identity and admitted list/struct children. Outer-row
values never determine the result dtype. Existing expression coercion and writer
fidelity rules continue to apply.

The current relational grammar owns allowed inner queries: local/memory inputs,
derived relations, joins, sets, aggregates, windows and admitted unary operators.
No new source format, execution mode or implicit external effect is introduced.
Scalar values whose schema depends on executing a dynamic pivot require separate
schema admission; reject them before data-dependent lowering can evade lazy
evaluation or empty-input typing. Lateral relations, implicit multi-level name
capture and broader SQL syntax retain explicit unsupported boundaries.

These zero/one/many semantics match the established
[scalar-subquery contract](https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-SCALAR-SUBQUERIES).
The [conditional-expression reference](https://www.postgresql.org/docs/current/functions-conditional.html)
informs the lazy branch cases; ShardLoom's existing binder and tests own its
specific admission and value-error boundaries. No external implementation code
or runtime executor is used.

## Shared ownership and Vortex-first decision

The existing SQL AST owns complete inner declarations and source discovery.
Expression IR may carry an explicit relational binding reference, resolved by
native lowering into the existing subquery plan family. Do not encode queries in
function names, replace SQL text with invented column tokens, read an inert
subquery's empty literal cache, or put raw SQL inside the runtime expression IR.
Unresolved references fail deterministically outside the admitting lowerer.

The native subquery binder, parameter runner and retained batch owners remain
the implementation owners. Extend their result kind to scalar values and their
selection policy to a bound evaluation guard. Lower lazy branch demand into
those guards using the shared expression kernels. Keep source resolution,
column pruning, outer-parameter scope, memory admission, cancellation, metrics
and output in the existing traversal; there is no second planner or executor.

| Existing owner | Extension |
| --- | --- |
| SQL scalar/predicate parser and relation AST | Typed references to owned inert query declarations; bounded recursive source and correlation discovery. |
| Native SQL expression/subquery lowering | Resolve each reference, carry lazy demand and preserve complete inner plan semantics. |
| Native subquery binder and parameter runner | Static one-column schema, zero/one/many behavior and fresh correlated state. |
| Native retained batches, selection and result builders | Retain at most one scalar result row per inner evaluation and deliver typed nullable values in bounded outer batches. |
| Python declaration builders and shared public workflow | Preserve every inner source declaration through expression composition; use the same SQL/native execution boundary. |

Python's `scalar_subquery(frame_or_sql_workflow)` retains the complete inner
statement and its declared input formats and schemas without executing it.
Column, predicate, aggregate and window composition carry those declarations to
the shared public workflow. Nested SQL expressions remain bounded and balanced;
top-level clause breakouts, statement separators and comments retain their
existing rejection. `count_distinct` preserves its string result for ordinary
columns and returns a source-carrying expression when its argument owns sources.

Static scalar-schema admission walks every query/expression declaration before
source resolution or either preparation route. A dynamic pivot in a scalar
branch is rejected even when the outer input is empty or CASE/COALESCE would
never select that branch. Runtime value errors still follow selected demand.

Vortex-first classification: `implement_shardloom_kernel` in the existing native
subquery owner. Pinned Vortex 0.85.0 provides DType/validity, native arrays,
constant arrays, chunked arrays and nullable selections. The local provider
source and [API inventory](vortex-public-api-inventory.md) were checked: those
array operations do not own SQL relational cardinality, correlated parameter
lifetime or ShardLoom's fallible query grant. Reuse the admitted Vortex selection
providers through `native_relational_batch` and its owned-buffer boundary. Do not
introduce another scalar container, decode-to-Arrow execution, a Vortex engine
integration or a new dependency.

Provider version and execution/Native I/O evidence remain in the existing native
workflow reports. Selected results may require the already declared compact
native-buffer copy; this is not a new zero-copy or zero-decode claim.
`fallback_attempted=false` and `external_engine_invoked=false` remain mandatory.

## Resources and failure handling

Retain only the first result row and reject a second result row before adding
more retained scalar state. This does not bound an inner aggregate/join/window's
own state: those operators still use their existing shared grant and spill or
denial policies. Build guards, selected-row indices and output descriptors under
the same reservations. Release inner state after each parameter; release result
credits with their final native buffer owner.

Cancellation, source replacement, memory denial, inner failure and consumer
failure abort the workflow. A later cardinality failure cannot turn previously
emitted provisional batches into successful collection or file publication.
Local writer staging and owned cleanup remain shared. No scalar result is
persisted as a hidden sidecar or cached across executions. This unit does not
claim complete reader/codec scratch accounting or a process-RSS ceiling.

## Acceptance

The expert comparator is a columnar-engine maintainer reviewing empty-input
typing, correlation scope, lazy value errors, cardinality, buffer ownership and
failure cleanup. Freeze independent expected results before public acceptance.

- Native tests: zero/one/two rows, duplicate rows, null/all-null/empty outer input,
  values split across batches, typed/nested payloads, projected/grouped/ordered
  inner plans, repeated correlated keys, schema/arity denials and unused branches.
- Ownership/resource tests: repeated execution, narrow grants, cancellation,
  source mutation, retained output buffers and failed consumers, including a
  cardinality error after earlier provisional output.
- SQL/Python/CLI parity: arithmetic and conditional composition, comparisons,
  aggregate/window arguments, derived/set inputs, explicit source declarations,
  source renaming and all representable local writers/readback. Preserve existing
  predicate-subquery and typed-expression coverage.
- Run the required workspace and native feature checks, focused Python checks,
  public complete-result matrix and shared-engine Full43 regression under the
  existing serial storage/process guards. Record exact source/binary identities
  and all failures; availability does not require a speedup.

Hosted checks and review are required for the actual implementation commit.
Nested pivot state, remaining adapters and broader resource/recovery transitions
retain their existing owners; paused format/text/native-Python experiments stay
paused.
