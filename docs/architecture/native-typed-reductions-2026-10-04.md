<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native aggregate expressions and decimal reductions

Status: implementation contract; acceptance pending. This continues the locally
accepted [nested keys and state](native-nested-keys-state-2026-10-04.md) under
PERF-02/03/07/10/11/12 and CG-3/5/19/20/21. The
[phase plan](phased-execution-plan.md) owns sequencing. The expert comparator is
a columnar engine maintainer checking decimal precision, aggregate semantics,
window state, source ownership and complete public result delivery.

## Decision and scope

Extend the existing native expression and reduction owners. SQL, Python,
DataFrame declarations and CLI execution must converge on those same owners.
This unit combines computed aggregate arguments with exact Decimal128 reductions
in scalar/grouped aggregation, source-order rolling and the existing scalar
pivot shape. It does not introduce another evaluator or a frontend-specific
aggregate algorithm.

COUNT, COUNT DISTINCT, SUM, AVG, MIN and MAX may consume an expression already
admitted by the shared scalar parser, binder and native kernels. Compile each
computed argument to a native projection before the existing aggregate node.
Bare-column calls retain their existing names and optimized routes; COUNT(*)
remains distinct from a missing or invalid expression. Resolve dependencies and
qualified names against the preceding input, including joins and derived
relations. HAVING uses the same lowering and private collision-safe aliases.
Keep explicit aliases and deterministic, bounded, collision-checked default
output names. Nested aggregate calls, effectful expressions and unimplemented
expression forms fail explicitly, including on empty input. Only COUNT retains
DISTINCT admission; this does not add SUM DISTINCT or AVG DISTINCT.

Use the existing exact decimal domain: precision 1–38, scale 0–precision, and
signed Decimal128 coefficients. Type derivation depends on the bound input
schema, never the observed values:

| Reduction over decimal(p,s) | Output type and behavior |
| --- | --- |
| SUM / rolling sum / pivot sum | decimal(38,s), with checked final precision. |
| AVG / rolling mean / pivot mean | decimal(38,max(s,6)); division must be exact at that scale. |
| MIN / MAX / rolling min/max / pivot min/max | Preserve decimal(p,s) and exact coefficient ordering. |
| COUNT / COUNT DISTINCT | Preserve their existing UInt64 result and NULL policies. |

Maintain a wide, checked integer total and count. A valid 38-digit coefficient
summed over at most UInt64::MAX observations and scaled by at most 10^6 fits
within signed 256-bit storage. Final Decimal128 output still requires precision
38. Intermediate totals therefore need not fit Decimal128: cancellation may
produce a valid SUM, and AVG may be representable when the intermediate SUM is
not. Reject an inexact average or final overflow deterministically; do not round,
convert through floating point or replace arithmetic failure with NULL.

Scalar/grouped SUM and AVG skip NULL observations and return NULL for an
empty/all-NULL group. Grouped input with no rows creates no groups; an ungrouped
aggregate retains its single-result-row contract. MIN/MAX and COUNT/DISTINCT
retain their existing semantics, including typed/nested extrema already admitted.
COUNT over a NULL expression must remain a validity calculation, not COUNT(*).
At the native projection boundary, an expression whose entire inferred domain
is untyped NULL receives a nullable boolean carrier. This preserves every NULL
row and provides one stable, persistable schema at empty and nonempty input.
The full expression is bound before that coercion; typed NULLs retain their
declared domain and unsupported expressions are not replaced by NULL.
Standalone SQL and DataFrame scalar projections submit their complete declaration
to native SQL admission, including aliases, bare NULL/boolean literals and untyped
NULL without a LIMIT. Preserve declared input schemas through collection and every
writer; bare columns and metadata COUNT(*) retain their existing routes. A quoted
path inside a projected string does not make an unbound table a local input.
An artificial derived relation is not required to reach the shared binder.
Unaliased NULL, TRUE and FALSE use lowercase keyword output names. A WHERE clause
cannot turn an unsupported scalar projection into a filter-only operation;
complete expression binding and evaluation remain required.
Grouped SELECT output uses that same ordered projection lowerer, preserving
aliases, declared order and omitted grouping keys. A specialized aggregate scan
is admitted only when its complete output layout matches the SELECT declaration;
other layouts keep the full SQL declaration for shared native projection.
Mixed raw and computed grouping keys reuse the existing native identity
expression to preserve their declaration order inside one key vector.

Columnar inputs retain field names admitted by the existing shared schema
reader. A qualified output field such as `q.id` must reopen through Parquet,
Arrow IPC and ORC and normalize to Vortex without another SQL-identifier check
in the CLI. This removes duplicate admission logic; the native provider and
schema owner remain unchanged. Avro record field names have a narrower
[ASCII identifier grammar](https://avro.apache.org/docs/1.12.0/specification/#names).
The pinned Arrow Avro provider rejects a dotted field before output publication.
Keep that explicit format boundary and use an explicit SQL alias, such as
`q.id AS id`, when Avro is requested; do not silently rename persisted fields.
The acceptance matrix checks both the rejection and the aliased complete result.

Rolling preserves source order, valid-observation min_periods, omission of
not-ready results, centered lookahead, limits and end-of-input flushing. Retain
the existing primitive floating calculation order. Decimal additive state uses
exact add/remove transitions across the bounded window; centered calculation
must not rescan the complete source or grow state with input length. Decimal
extrema use the same bounded window and exact comparison. New output remains
nonnullable when the existing min_periods policy only emits a valid observation.

A relational output range must reach a preceding rolling operator through
row-local projections and other output ranges after complete schema binding.
Its required prefix includes skipped rows and intersects existing limits.
Filters, sorting, other unary operations, aggregation, windows, sets, joins and
subqueries stop this propagation. Keep the outer range to apply its offset and
count. This avoids evaluating an inexact or overflowing rolling result outside
the requested prefix without truncating a filter's input or changing ordering.

Pivot retains domain discovery, index order, labels, fill, dropna, row limits and
margin behavior. Its COUNT continues to count rows, including NULL payloads.
Decimal numeric pivots retain the existing numeric pivot's non-NULL value
requirement; a NULL numeric value remains an explicit error. Missing cells stay
distinct from an observed NULL first/first-unique cell. Decimal margins merge
wide totals and counts before exact finalization, so a mean of means cannot
replace a weighted mean. Typed fills use the existing lossless common-type and
checked rescale rules; typed margins retain the existing UTF8-index requirement.

## Reuse and provider decision

| Existing component and callers | Required extension |
| --- | --- |
| SQL scalar declaration parser and native relational lowerer | Reuse expression IR, dependency traversal, source qualification and native projection for SELECT and HAVING aggregate arguments. Keep decoded reference behavior explicit and separate from native execution. |
| Native aggregate binder and `native_relational_aggregate` | Extend numeric type derivation and per-group decimal state; preserve exact keys, distinct membership, compact extrema, reservations and native output. |
| `RollingWindowState` and retained unary rolling adapter | Extend the shared window scheduling/state boundary with exact decimal observations and totals; preserve primitive callers and avoid duplicate scanning or delivery. |
| Generic pivot domain/cell state and completed pivot owner | Extend the existing cells, margin merge and completion with exact decimal state; direct and composed pivot calls share the implementation. |
| Core decimal validation and Vortex scalar arithmetic | Reuse declared-type checks and the pinned public wide-integer arithmetic surface; keep output construction with native decimal arrays. |
| Operation resources and native writers | Preserve one PulseWeave grant, reserved state/output, capillary batches, source generations, cancellation, native fidelity and failed-publication cleanup. |

Vortex-first classification: `implement_shardloom_kernel` for aggregate policy
and state transitions, using existing Vortex-native array and scalar providers.
Pinned Vortex 0.85.0 `aggregate_fn/fns/sum` starts from zero and returns NULL on
overflow; its decimal result precision grows by ten. `aggregate_fn/fns/mean`
uses its own precision/scale rule and integer division, and explicitly rejects
grouped decimal mean. Its grouped providers consume pre-grouped array ranges,
not the existing ShardLoom hash-group state. Those contracts cannot substitute
for the specified NULL, exactness, precision and error policies.

The public `vortex::array::scalar::DecimalValue` supplies I256 storage, widening,
checked addition/subtraction/multiplication/division, comparison and narrowing.
Use it inside the existing feature-gated `shardloom-vortex` boundary, with no
new dependency or wider public decimal domain. This is scalar coefficient
arithmetic, not Arrow-array execution. Reuse native source/array access,
validity and output constructors. Record partial materialization honestly;
no new zero-decode claim follows from native scalar state.

## Resource, failure and compatibility obligations

Reserve group/window/cell capacity and wide state before allocation, including
replacement and output overlap. Retain credits through the last result owner.
Do not enlarge every primitive state unconditionally when separate admitted
decimal state can preserve the existing representation. Inspect empty schemas
before source execution. Check cancellation within substantial work and release
state on arithmetic failure, denied growth, cancellation and consumer failure.

Preserve existing collection bounds, complete bounded writer delivery, source
generation checks and inert inspection. Exact decimal output must reopen with
the same values, precision, scale, NULLs and order through every representable
format. The pinned ORC decimal denial remains explicit before publication.
Existing sort spill does not authorize aggregate, pivot or rolling state spill;
those pressure transitions retain deterministic grant denial pending their
separate implementation. Reservations are not a whole-process RSS bound.

The source remains 0.4.0. Existing primitive floating SUM/AVG and numeric
rolling/pivot behavior remain compatible. No fallback, intermediate result file,
source replay, new execution mode, external effect or package publication is
introduced. Wider analytic frames, scalar-value subqueries, nonnumeric pivot
extrema, nested pivot state, adapters and general state spill retain their
existing universal-workflow owners and are subsequent implementation work.

## Acceptance

- [ ] Implement shared aggregate expression lowering and exact decimal state,
  with complete dependency/alias/empty-plan admission and unchanged bare calls.
- [ ] Extend rolling and pivot through their existing state owners, including
  centered boundaries, exact margins, fills, cancellation and constrained grants.
- [ ] Freeze independent full-value expectations for positive and negative
  public workflows: renamed schemas, joins/derived input, HAVING, lazy branches,
  constants, NULLs, dictionary/chunk boundaries, scales 0/6/38, precision limits,
  large-intermediate cancellation, representable averages with oversized totals,
  inexact averages, final overflow and writer rollback.
- [ ] Verify retained output lifetime, state release, all representable writer
  readbacks and unaffected prior public/direct-unary cases.
- [ ] Run required workspace/native/Python, lean/MSRV and affected documentation
  gates, then freeze source, binaries, oracles and portable evidence.
- [ ] Run paired Full43 under the existing serial storage/process guards and
  predeclared timing/RSS/aggregate screens, retaining prescribed repeats and
  every failed or inconclusive observation.
- [ ] Complete hosted review/gates
  and record the finite completion in the ledger.

Availability requires correctness, resource and failure proof. No speedup,
competitive superiority or broad gate completion is implied. CG-1 through CG-23
stay visible; real Vortex payload proof remains distinct from placeholder
artifact status. Paused large text/format campaigns and native Python binding
experiments remain outside this continuation.
