<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native aggregate expressions and decimal reductions

Status: implemented with complete local core acceptance on October 5; hosted
integration and documentation/website refresh remain pending. The
[acceptance report](../benchmarks/native-typed-reductions-full43-2026-10-05.md)
records 20,445 public checks, 202 retained-workflow checks, all 129 Full43
executions and independently inspected portable evidence. This continues the
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

The universal-route audit also removes fixture-specific public provider dispatch.
Array-backed inputs register immutable `ResidentMemorySource` values in the
existing relational preparation. File and memory providers deliver bounded native
arrays to the same scan consumer, operators, resource owner and writers. Memory
input does not serialize a temporary Vortex file merely to enter the engine.
Upstream Vortex 0.85 `ArrayRef`, bound expressions, slicing and native buffers
remain the providers. Its file scan needs a file layout, while its unsigned
piecewise-sequence representation does not cover the signed range contract;
the range input adapter therefore fills one admitted Int64 native buffer, with
checked endpoints, before ordinary relational execution. Generic typed input
keeps its existing bounds; generated ranges retain their one-million-row bound.
Input ownership, empty/null values, repeated execution, mixed-source composition
and writer parity require proof before accepting this convergence.

Bounded collection carries its actual ordered native result schema alongside
the JSON value payload, including on empty output. This is a
`use_vortex_native_provider` decision: Vortex 0.85.0's public DType Serde
serializer supplies `result_schema_json` with `result_schema_format` set to
`vortex.dtype.serde.v1`. Its existing Apache-2.0 Serde feature is enabled only
with native primitives; no provider version or package is added. The shared
collection sink reserves schema serialization and retains the result's credits.
Python conversion reads this schema instead of guessing types or field order
from values. JSON's explicit binary/decimal/temporal representations remain
documented; typed conversions restore those values only at the requested output
boundary. This does not preserve physical encodings or authorize more input or
operator types. Schema metadata never executes a query, opens another source,
or invokes an external engine.

Memory row declarations carry an ordered typed schema and nullable scalar cells
inside the existing bounded JSON request. The old percent-delimited generated
row grammar is removed. Schema fields and each row are bounded during parsing;
the existing 64-column, 65,536-row and 8-MiB input limits remain. Python row,
pandas and Arrow intake share this declaration. They accept an explicit schema
for empty input; inference examines non-null values across each column, with a
nullable boolean carrier for an entirely untyped NULL column. Signed integers
remain exact, and integer-to-float conversion outside the contiguous exact
range is rejected. This is input normalization only; filtering, expressions,
ordering, aggregation, composition and output remain with the native engine.

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

The shared native CSV sink also preserves the existing constructor-output
contract: admitted nested values become quoted JSON text cells. It streams the
same native structural traversal used by JSON/JSONL through CSV quote escaping;
it does not build a second row table or buffer a complete nested cell. A NULL
parent emits an empty cell, while an empty list emits `"[]"`. A single NULL
column emits a quoted empty cell so CSV readers retain that row. JSON field
order follows the bound native schema. This is explicit text translation and
does not persist nested logical dtypes or add nested CSV input inference. The
pinned ORC writer still rejects nested output before publication. Existing
batch, resource, cancellation and atomic-commit boundaries remain in force.

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

Public SQL and DataFrame operations no longer select traditional-analytics
benchmark scenarios from schema names or matching statement fragments. Those
shortcuts could apply fixture predicates and return summaries instead of the
requested rows. Complete declarations use the existing shared native binder,
primitive strategies and writers; explicit benchmark-scenario arguments are
rejected at the public boundary before source admission or sink publication.
Benchmark harnesses must declare workloads through that same engine. Prior
publication does not justify retaining an obsolete execution path.

Numeric keys compare integer and finite F32/F64 values exactly without converting
integer columns or literals to floating point. Compare the bounded integer part
and fractional sign; floats outside the complete i64/u64 domain order outside
that domain. Integral floats in that domain use the same equality hash as their
integer counterpart. Scalar predicates, NULLIF, joins and IN/ANY/ALL therefore
share exact equality and ordering, including above 2^53 and at the signed and
unsigned endpoints. NULLIF retains its first operand's dtype. This does not admit
a lossy common output type for mixed set branches or change arithmetic's checked
integer-to-float conversion policy. Primitive SUM/AVG retain their existing F64
accumulation and output contract in both general and specialized native owners.

Calendar helpers and comparisons against a declared date/timestamp convert UTF8
through the existing strict ISO cast kernel, preserving NULLs and rejecting
malformed values. Decimal comparisons bind a lossless common precision and scale
before comparing coefficients. No input-type inference depends on row values.

ARRAY and STRUCT constructors bind all children before execution, including on
empty input. Lists require a lossless common child type; all-NULL/empty lists use
the same nullable boolean carrier as other untyped NULL outputs. Struct names
are distinct and preserve their declared order. Constructors are limited to 128
children within the existing 24-level/4096-node expression budget. The provider
decision is `use_vortex_native_provider`: pinned Vortex 0.85 StructArray,
ListArray, ChunkedArray and take kernels construct native columns with bounded
scratch and allocator-owned payloads. No decoded row tree or Arrow execution is
introduced. Correlated HAVING reuses the same native outer-row parameter after
grouping, before filtering and limiting the inner relation.

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

The public workload acceptance also closes scalar and syntax gaps formerly hidden
by the removed benchmark runtime. Nonrecursive outer `WITH` declarations expand
to the same derived relations, joins and set plans, with 64 declarations, 256 KiB
expanded SQL and 24 nesting levels as admission ceilings. Self/forward references,
recursive CTEs, nested `WITH`, column lists and materialization hints fail explicitly.
Quoted data and exact file paths are never substituted. Group-key aliases compute
through the existing projection before aggregation; input column names take
precedence and ambiguous inputs remain errors.

Numeric remainder extends the shared expression IR and checked native column
kernel. Integers retain exact signed/unsigned domains, the remainder has the
dividend's sign, NULL propagates, and a zero divisor errors. Decimal remainder is
an explicit unsupported boundary. JSON extraction accepts literal `$` paths with
field and nonnegative index steps over UTF8 JSON. It preserves selected JSON text,
including integers beyond the floating-point domain; missing/type-mismatched paths
return SQL NULL, while JSON null remains JSON text. Parsing reserves bounded scratch
before container indexing and rejects inputs over 1 MiB. `STRPTIME`/`TRY_STRPTIME`
use the shared ISO calendar routines for literal UTC ISO-second, date-time-second
and date-only formats; malformed values become NULL only for TRY_STRPTIME. Other
format directives and timezone-database semantics remain explicit blockers.

For these additions the Vortex-first classification is `implement_shardloom_kernel`.
Pinned Vortex 0.85.0 `scalar_fn/fns/operators.rs` and primitive numeric operators
expose add/subtract/multiply/divide, without an elementwise remainder provider.
`scalar_fn/fns/variant_get` requires `DType::Variant`; it is not a UTF8 JSON parser.
The pinned array and datetime-parts runtime has no formatted timestamp parser.
The implementation therefore extends existing ShardLoom kernels over Vortex
columns, retaining native output, reservations, cancellation and certificates.
SQL, Python, DataFrame and benchmark callers share these kernels. Focused tests
cover nulls, malformed inputs, alias precedence, exact integers and CTE composition.
The modular public workload matrix also passes 1,408 complete records, including
704 native records, plus a separate 32-record input-state probe; the acceptance
report preserves its original execution identity and exact retention proof.

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

- [x] Implement shared aggregate expression lowering and exact decimal state,
  with complete dependency/alias/empty-plan admission and unchanged bare calls.
- [x] Extend rolling and pivot through their existing state owners, including
  centered boundaries, exact margins, fills, cancellation and constrained grants.
- [x] Freeze independent full-value expectations for positive and negative
  public workflows: renamed schemas, joins/derived input, HAVING, lazy branches,
  constants, NULLs, dictionary/chunk boundaries, scales 0/6/38, precision limits,
  large-intermediate cancellation, representable averages with oversized totals,
  inexact averages, final overflow and writer rollback.
- [x] Verify retained output lifetime, state release, all representable writer
  readbacks and unaffected prior public/direct-unary cases.
- [x] Run required workspace/native/Python, lean/MSRV and affected documentation
  gates, then freeze source, binaries, oracles and portable evidence.
- [x] Run complete Full43 through the shared public workflow and strict typed
  result protocol under the existing serial storage/process guards. Freeze all
  expected values, query text, input identity, executable and harness sources
  before execution; retain every failed or inconclusive observation.
- [x] Establish the consolidated engine as the control for subsequent paired
  optimization measurements. Earlier binaries without the complete typed result
  protocol are retained regression evidence, not a second live execution route
  or a source of speedup claims for this consolidation.
- [ ] Complete hosted review/gates
  and record the finite completion in the ledger.

The accepted native executable is built from `eb39c2eb`; the final assertion-only
checker source is `82766e8b`. Integration with the accepted stack at `ef6cd4c4`
preserves exactly the tested source tree. The packet records all original hashes
and failed observations; later source identities do not relabel prior runs.
Independent inspection with a separate streaming JSON parser verifies every
reported count, all 129 shared-family reports, the complete raw report inventory,
resource boundaries and absence of successful fallback execution.

Availability requires correctness, resource and failure proof. No speedup,
competitive superiority or broad gate completion is implied. CG-1 through CG-23
stay visible; real Vortex payload proof remains distinct from placeholder
artifact status. Paused large text/format campaigns and native Python binding
experiments remain outside this continuation.
