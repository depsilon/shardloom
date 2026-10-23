# Shared native provider and surface coverage

## Decision and scope

The October 23 maintainer authorization corrects reachability of existing native
execution capabilities across CLI SQL, embedded workflow, Python SQL, DataFrame
and typed relation declarations. Local acceptance is recorded in the
[surface report](../benchmarks/native-shared-surfaces-2026-10-23.md) and
[completed ledger](phased-execution-completed-ledger.md). RFC 0021, RFC 0022 and RFC 0044 govern
expression semantics, typed plans and resource ownership.

Equivalent declarations must receive the same eligible native providers after
source and policy admission. Surface choice must not force a general row path
when the existing metadata, encoded aggregate, count or sorting provider can
execute the complete operation with the same semantics. General relational
execution remains the native provider for shapes, types and compositions that
require it. Selection precedes execution; a provider error is never retried
through another engine or a different native strategy.

This work covers current capabilities and appropriate missing surface spellings.
The optional breadth backlog is unchanged. Full43 is one regression workload;
its query IDs and text must not participate in runtime selection.

## Reuse map

| Existing component | Gap | Shared change |
| --- | --- | --- |
| `VortexRelationalPlan` and `Expression` | Typed declarations bypass optimized file providers | Inspect native operator and expression semantics and produce existing `VortexQueryPrimitiveRequest` values when equivalent |
| `PreparedNativeOperation` and workflow provider preparation | SQL/workflow and typed plans enter different provider preparation paths | Reuse the same admitted source, provider, result and evidence owners for typed requests |
| `prepared_dispatch` metadata selection | Primitive versus extended-type admission is only reached by selected facades | Apply it to equivalent typed requests before execution; keep extended types in the relational binder |
| SQL source resolution and collection ownership | Python SQL parses with the general grammar before selecting its provider | Let the selected preparation own collection/reuse evidence; preserve collection rebinding and generation checks |
| DataFrame operations and native expressions | Some already implemented operations lack a surface spelling | Add bounded output offsets and shared scalar spellings with explicit types, null behavior and error tests |
| Capability and semantic catalogs | Report rows do not establish execution across front doors | Bind current families to executable conformance cases and explicit unsupported/not-applicable boundaries |

No Vortex version, dependency or query-engine integration changes are needed.
Existing pinned Vortex arrays, scan and source owners remain the provider
boundary. ShardLoom retains normalization, policy, resource and certificate
ownership. Native Vortex output remains highest fidelity; compatibility output
keeps its existing translation accounting.

## Semantic and ownership requirements

- Match operator structure and expressions, not SQL text, benchmark identifiers,
  column names or data-specific values. Reuse existing declared transformations.
- Preserve projection order and aliases, count versus count-distinct, exact
  integer comparison, empty/all-null aggregation, byte versus character length,
  temporal units, HAVING, ordering/null placement, limits and offsets.
- Validate the complete declaration. An optimized route must not discard an
  invalid expression, unknown column, overflowing range or unauthorized effect.
  Only normalize transformations with an established equivalence contract.
- Retain common admission before source access, memory accounting before owned
  allocation, source-generation checks, cancellation, explicit spill and result
  ownership. Collections are rebound when membership may change. Memory and
  streaming sources retain their current source-specific contracts.
- Keep unknown shapes on the ordinary typed native binder without claiming
  optimized coverage. Do not implement a second evaluator or render typed plans
  into SQL. Do not retry after runtime failure.
- Report the actual selected provider and native I/O/effect boundaries. Schema
  support, syntax admission and runtime support remain separate facts.

SQL grouping and reduction expressions with integer offsets pass through the
ordinary typed binder before optimized provider selection. A signed SQL literal
must not inherit an unsigned primitive's input-preserving arithmetic contract:
for example, subtracting three from a `UInt32` zero produces an `Int64` minus
three. Wider integers, floats and nullable values retain their ordinary typed
semantics and terminal errors. Existing unaliased aggregate names are preserved
when SQL moves to this binder. Explicit primitive declarations keep their own
declared contract.

Column-only file projections and infallible typed predicates also normalize into
the existing native scan after ordinary binding. Alias views preserve output
names and order. Computed projections, ranges, residual evaluation and non-file
sources remain barriers. Typed comparisons with NULL literals, nonfinite-capable
float domains or literals outside the physical integer field's range retain the
typed evaluator; their semantics must not be replaced by native IN-list NULL
membership or narrowed scan literals.

SQL's compact primitive predicate spelling must also preserve literal value and
type. Whitespace-sensitive text, quoted integers, delimiter-bearing strings and
empty IN-list members remain on the shared typed binder when the compact grammar
cannot represent them exactly. Python comparison, pattern and substring builders
preserve whitespace and quoting before admission.

Grouped SUM/AVG/MIN/MAX ordering can use the aggregate provider without explicit
NULL placement when each group's argument is provably non-null. The public
nullable result schema remains unchanged. Scalar reductions on empty inputs and
groups with nullable arguments keep the ordinary NULL-ordering requirement.

## Surface declarations

These additions belong to this source change, not to the already published
v0.7.0 artifacts.

- `LazyFrame.limit(count, offset=0)` keeps the range at its declared position.
  Filtering, aggregation or another range after it operates on that range's
  result. SQL `LIMIT ... OFFSET ...` has the same behavior inside derived
  relations and set operations. Counts and offsets must be nonnegative integers;
  their sum must fit the native range. A zero count still validates its input.
- Column comparisons preserve string literal content, including empty strings,
  surrounding whitespace, Unicode and escaped quotes. Pattern and substring
  helpers preserve their literal whitespace; their existing nonempty and
  wildcard-validation rules remain explicit.
- `ColumnExpression.extract_minute()` and `.date_trunc_minute()` expose the
  existing integer epoch-second and timestamp-text operations. Nulls propagate;
  integer truncation uses the preceding minute, including negative epochs, and
  rejects overflow. `TimestampMicros` keeps the existing `timestamp_minute()`
  spelling and unit contract.
- `.url_domain()` and `.regex_domain()` expose the existing distinct domain
  transformations. The latter implements the admitted fixed URL replacement;
  it does not admit arbitrary regular expressions. SQL `octet_length()` and
  Python `.byte_length()` count bytes; `length()` retains character semantics.
- `shardloom.native.Relation.unary(operation, options="*", keep=None)` declares
  an existing unary node without SQL serialization. Operation names are
  `distinct_rows`, `drop_duplicates`, `duplicated`, `tail`, `sample`, `rewrite`,
  `melt`, `rolling`, `explode` and `pivot`. Options use the same bounded column
  list, positive tail count or JSON operation object as the SQL binder. Only
  the duplicate operations accept `keep`. Dynamic pivot fields are bound by
  the existing native stage before downstream expressions run.

The typed declaration API and SQL binder share unary option validation. Unknown
operations, invalid keys, repeated keys, invalid keep policies and oversized
options fail during declaration, before opening a source. File collections keep
their membership-rebinding policy after provider selection.

## Executable family coverage

The new [five-surface conformance suite](../../python/tests/test_native_shared_provider_binding.py)
executes DataFrame collect, typed relation collect, native SQL collect, embedded
workflow collect and CLI SQL collect. It checks complete values, exact schemas
and no-fallback evidence. Native calls reject subprocess launches; CLI calls use
the explicitly selected executable. Its cases cover:

| Family | Added cases |
| --- | --- |
| Filters, projections and scalar expressions | Composition, aliases, nulls, unsigned and wide signed arithmetic, floats, Unicode byte/character length, epoch units and the admitted domain transformations |
| Aggregation, joins, sets and windows | Grouped reductions, left join, four set operations and analytic row numbering through all five entry points |
| Subqueries | IN, EXISTS, ANY and ALL through all five entry points |
| Unary operations | Thirteen parameter variants, typed memory input with mixed/empty/all-null data and repeated collection, plus file-backed five-surface execution |
| Dynamic schemas | Pivot followed by filter/projection/range, missing-field rejection and repeated execution |
| Ranges | Positive and zero counts, offsets before filtering/aggregation and repeated ranges |
| Source lifecycle | SQL directory and manifest membership changes must produce fresh counts |
| Rejection | Invalid unary declarations must fail before source access |

Existing native binding suites retain deeper operator/type matrices, memory and
batch intake, all eight local writers, streaming, spill, cancellation, deadlines,
source generations and result ownership. Construction-only tests remain distinct
from execution evidence. This family coverage does not enumerate every possible
combination of scalar, dtype, encoding, adapter and operator; unsupported
combinations retain their existing deterministic diagnostics.

## Acceptance evidence

Tests cover every currently advertised family at its applicable front doors,
with concrete positive and rejected cases rather than catalog presence alone.
Include composed filters/projections/aggregates/sorts/ranges, joins, sets,
windows, unary operations, subqueries, source adapters and result delivery.
Boundary cases cover nulls, empty inputs, integer extremes, Unicode, aliases,
temporal units, ordering ties, invalid resources and changing sources. A surface
may remain not applicable where its contract deliberately excludes an operation;
that boundary must be explicit and tested.

Local acceptance at `7c65f339` passes the focused and required workspace checks,
121 Python construction tests, 132 native-binding tests and 215 independent
small-fixture comparisons. Matched CLI/native Python release artifacts then pass
one guarded isolated Full43 per surface, sequentially, with all other task work
idle during queries. All 215 complete results and 43 cross-surface schemas match;
the aggregate-provider and typed scan-pushdown assertions also pass. The report
retains exact identities, engine-only timing, failures and partial runs.

The preceding baseline remains immutable: CLI and embedded workflow completed
43 queries; Python SQL completed 35 admitted queries; DataFrame stopped at query
19; typed native stopped at query 17. These observations motivate the correction
and remain historical evidence alongside the accepted correction. Single-pass
timing does not establish a repeatable performance improvement.
