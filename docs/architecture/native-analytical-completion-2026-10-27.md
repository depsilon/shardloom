<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared analytical completion and subsequent format parity

Status: implementation in progress under existing queue item 20. The October 27
maintainer instruction incorporates the remaining analytical capabilities,
shared performance corrections, diagnostics and acceptance into this active
work, followed by the remaining file-format and interface-parity work. This is
not a new phase or competitive gate.

## Completed foundation and scope

The 347-case foundation is merged in #1611 (`4d25a06e`), the first source-demand
reader in #1612 (`81ab0a4d`), and Parquet completion in #1613 (`04e7d92a`). Their
results, failures, protocols and runtime identities remain immutable. The
Parquet completion includes selected-chunk buffering, hidden dependencies,
safe source-order prefixes, directory inputs, content identity, source/output
guards and borrowed typed grouped SUM/AVG updates. These completed mechanisms
are not outstanding work.

The aggregate/grouping continuation completes a new 347-case checkpoint at
**31.962394957 engine seconds**, with all 12,565 complete result rows and schemas
passing. Its immediate baseline is **34.960427343 engine seconds** from the
[Parquet completion acceptance](../benchmarks/native-parquet-demand-completion-2026-10-27.md).
The observed reduction is 8.6%, with no case or family exceeding the frozen
material-regression thresholds. The 50.877676708-second observation remains an
earlier baseline. This single observation does not isolate a mechanism's benefit
or close the remaining competitor-performance gaps. The
[aggregate/grouping acceptance](../benchmarks/native-analytical-aggregates-2026-10-27.md)
records completed native, workspace, installed-wheel and portable checks;
PR acceptance is separate.

The [parent program](native-broad-query-program-2026-10-26.md) and the
[phase plan](phased-execution-plan.md#everyday-analytical-completion) retain the
full scope. Finish item 20 before continuing the remainder of items 18/19's
format, rich-type and interface contracts. The preserved `codex/universal-io`
branch is evidence and unfinished source, not an accepted prerequisite. Inspect
its changes against current main before reusing any implementation.

All five public interfaces lower to the same native implementation: CLI SQL,
embedded workflow, Python SQL, DataFrame and typed native relations. Preserve
Vortex-native input, owned arrays, spill and output. External engines remain
comparison sources only, with `fallback_attempted=false` and
`external_engine_invoked=false` in native execution. Package publication and
CG-1 through CG-23 retain their independent acceptance and authorization.

## Remaining completion checklist

- [ ] Correct demonstrated shared expression, aggregation, projection, ordering
  and join inefficiencies, with complete-query benefit and regression evidence.
- [ ] Complete statistical and richer aggregates, including their type/null,
  update/finalize, window, exact-state, reservation and spill contracts.
  The primitive statistical, exact-quantile, distinct-additive, selector,
  collection, FILTER and grouping-set work unit passes local acceptance.
  General decimal statistical/rounding and wider numeric/workflow guarantees
  remain separately open; the whole analytical requirement is not closed.
- [ ] Complete temporal units, timezone/duration/calendar fidelity and the
  as-of, calendar-window, resampling, gap-filling and interpolation workflows.
- [ ] Complete recursive composition, explicit materialized ownership and
  supported aggregate combinations through shared SQL/typed execution.
- [ ] Complete caller-visible non-finite/signed-zero policy, decimal rounding
  choices and exact widened-integer summation without changing existing defaults.
- [ ] Implement declared application batch functions with explicit schema,
  determinism, null, effect, ownership, memory and cancellation boundaries.
- [ ] Replace structural depth/width ceilings with scalable traversal, shared
  references and resource admission; test binding, execution and destruction.
- [ ] Complete meaningful physical-plan/work diagnostics and keep unavailable
  measurements explicitly unknown.
- [ ] Validate the six specified user workflows, five interfaces, fresh held-out
  variations, pressure/failure behavior, compact regressions and package gates.
- [ ] Reconcile capability/reference documentation, complete review and merge,
  and move item 20 into the completed ledger only when its obligations pass.
- [ ] Continue remaining format and interface parity through existing I/O and
  rich-data owners, including safe refresh and complete independent readback.

The six user workflows remain a statistical fund summary, an entity/currency
as-of valuation, a timezone-aware daily report across daylight-saving changes,
a parameterized multi-CTE query, an application batch transformation and safe
dataset refresh. Connector work follows the existing I/O owner; a fake remote
handle or mocked success does not establish real deployment acceptance.

## First coherent implementation: aggregate and expression execution

The retained observations identify a concrete first family: all 90 unfiltered
grouped cases lose to their retained competitor observations while 87 of the 90
filtered variants win. Their native reads still decode all one million source
rows. This is evidence for investigation of downstream work, not proof of an
isolated cause. Mixed aggregate arguments and wide/text projections are also
selected targets. Competitor observations use Arrow IPC and complete API time;
the native candidate uses Parquet. They are prioritization evidence, not a
matched-format or engine-clock superiority comparison.

Inspect existing reuse before adding it. Native projection reuse already binds
exact unary/binary/comparison outputs. Adjacent aggregate measures already
share a prepared input owner, and grouped SUM/AVG already binds physical type
and validity once per batch. Extend these owners; do not introduce a second
expression evaluator, answer cache or scheduling pool.

### Shared aggregate performance continuation

The next implementation class is inefficient general execution in the existing
native aggregate owner. Source inspection confirms that group ordinals already
share hashing and lookup across measures. The remaining numeric SUM/AVG loops
call a boxed update function for every row; adjacent measures still repeat
identical accumulation, and projected aliases of the same completed array do
not share their prepared value owner. Identical adjacent FILTER domains also
repeat selection. These are concrete repeated operations, not yet measured
attribution of the complete-query gap.

Use the existing original-width numeric owner for a monomorphized batch visitor,
with the same row order, NULL handling, cancellation cadence and failure state.
Admit adjacent primitive SUM/AVG state reuse only when the input array and filter
domain agree and every corresponding accumulator is bitwise identical before
the earlier update. Copy its completed count and sum at the later measure's
original position. A failed earlier measure must leave later states untouched;
different starting states, decimal/distinct reducers and different domains keep
their ordinary paths. For grouped input, admit the two passes over retained group
states (start comparison and completed-state copy) only when their combined
iteration count is no greater than the input row pass they replace. This avoids
repeated full-state traversal for high-cardinality or small later batches.
Reuse completed native array owners by exact identity,
never content guesses or expression names alone.

Vortex-first decision: wrap existing Vortex 0.90.0 array ownership, primitive
buffers, validity masks and native selection. The upstream grouped reducer
provider consumes already-grouped arrays; introducing grouped lists would add
work to this flat-input owner and has no new evidence overturning the retained
provider decision. No new provider, scheduler or external engine is admitted.
Require typed/encoded/null/error/pressure tests plus complete-query comparison
and neighboring-query regression evidence before claiming a performance benefit.

The first continuation checkpoint validates all 347 queries, 12,565 result rows
and complete schemas in **30.118456795 engine seconds**, against the unchanged
**31.962394957-second** aggregate/grouping checkpoint. The observed reduction is
5.8% overall, 12.9% for global aggregation and 7.5% for grouped aggregation.
Global aggregation passes the prospective 10% family-benefit gate. No family
crosses its regression threshold. The original `projection_limit_direct_31`
case does cross the case threshold (0.117059333 to 0.150251250 seconds), and that
observation is preserved rather than replaced in the total.

A separately frozen diagnostic runs that case and its LIMIT 11/100 neighbors
in three baseline/candidate pairs, with pair order AB, BA, AB. All 18 executions
validate; none reproduces the material-regression rule. For LIMIT 31 the role
medians are 0.117325541 and 0.109783958 seconds. This supports the disposition
"not reproduced in the bounded diagnostic," without assigning a cause to the
original observation or claiming statistical confidence. The original runtime,
full cohort, diagnostics and prospective thresholds remain immutable. The
native suite passes 3,403 tests with 32 existing opt-in ignores; broader PR and
package acceptance remains separate.

### Borrowed variable-width output construction

The next candidate addresses repeated native scalar lookup at existing payload
copy and result-gather boundaries. These boundaries already execute the selected
column to Vortex `Columnar`, but text/binary leaves still traverse scalar dispatch
and clone buffer handles on every sizing and write visit. Primitive numeric
leaves already bind their native storage and validity once. The saved projection
family contributes 6.116540710 seconds of the 31.962394957-second checkpoint;
this identifies a target, not an attribution of its complete cost.

Bind the existing Vortex `VarBinViewArray` and its validity mask after the same
selection/canonicalization boundary, then borrow values during result sizing
and construction. Preserve native constants without row-sized expansion. Reuse
the pinned provider's checked `utf8_slice_at` access and existing native byte
views; do not introduce an Arrow conversion, value cache, hash-domain preparation
or a second expression evaluator. NULLs, selected-row order, dtype, cancellation,
independent output buffers and their memory credits retain the current contract.
Do not eagerly validate an unselected dictionary domain or move a fallible
expression across a filter, sort or limit to obtain a faster result.

Vortex-first decision: wrap the existing native array, constant and mask providers
already admitted in Vortex 0.90.0. `Columnar` yields a canonical array or a native
constant; the vendored checked text accessor can use its immutable construction
receipt without cloning a buffer for each value. The new owner binds those
providers for existing ShardLoom output construction. It adds no dependency,
provider permission, query-engine integration or public capability.

Require exact UTF-8/binary, nullable, constant, dictionary, sliced/chunked and
nested-parent selection tests; retain output clones after source release and
check final reservation cleanup. Freeze the next complete-query comparison
against the preceding numeric checkpoint before running it, with projection and
string families declared as targets and the same material-regression rules.
Bounded streaming Top-K retention is a separate candidate: it requires proof of
complete payload validation and source release before reducing retention. The
existing ordinary versus streaming error-order distinction remains authoritative.

The first expression extension admits reuse of a fully completed, identical,
pure expression on the same input. Conditional/COALESCE/NULLIF branches retain
their selected-input evaluation scope: sharing a completed outer result does
not authorize reusing an inner result evaluated on a different row selection.
Preserve typed literal bits, evaluation/error order, source lifetime,
cancellation and memory ownership. Do not move fallible expressions across
filters, ranges or ordering solely to obtain a faster result.

The richer reducer contract adds stable population/sample moments and paired
moments, exact quantile/order-dependent state, distinct additive measures and
ordered/list/string results through existing native aggregate owners. Specify
each function's empty/all-null and small-sample result, pairwise null policy,
dtype, ordering/ties, overflow and interpolation before its implementation.
Fixed-size moments and variable-size exact/order-dependent state have different
reservation and spill obligations. Reuse existing ordered native spill where
applicable; unsupported resource configurations must refuse deterministically.
New functions require public lowering, windows where applicable, complete
typed results, diagnostics and conformance, not merely a private reducer test.

Population variance/standard deviation ignore NULLs, return NULL for no observed
values and zero for a singleton. Sample variants return NULL below two observed
values. `variance`/`var` and `stddev`/`std` name the sample forms. The initial
primitive-numeric statistical result is nullable Float64; decimal admission
requires a separately specified scale contract. Reject non-finite observations
and accumulator overflow under the existing strict policy. Preserve original
integer differences before Float64 conversion, including values above 2^53.

Use centered incremental and pairwise merge formulas, with compensated internal
updates and a retained original-width origin. The mathematical reference is
[Pebay, SAND2008-6212](https://www.sandia.gov/research/publications/details/formulas-for-robust-one-pass-parallel-computation-of-covariances-and-arbitr-2008-09-01/);
implementation is original. Compatible statistical measures share charged state;
existing basic reducers do not allocate that state. Test independent expected
moments, small samples, integer extremes, partition merges and resource cleanup.
Pairwise and window extensions must preserve the same output contract while
making their selected-row and null-pair rules explicit.

Covariance and correlation take two numeric expressions. Ignore a row whenever
either argument is NULL; an unobserved companion does not enter the finite-domain
check. `covar_pop` divides the centered cross-product by the pair count and is
zero for one pair. `covar_samp` divides by count minus one and is NULL below two
pairs. `corr` is NULL below two pairs or when either centered variance is zero.
All return nullable Float64 and reject observed non-finite inputs or accumulator
overflow. Preserve separate original-width origins for both inputs and merge
paired moments without forming raw squared or cross-product sums. Correlation
normalization must avoid overflowing the product of the two centered variances;
only a rounding-size excursion outside [-1, 1] may be clamped.

Centered summaries use a dynamically enlarged power-of-two scale per input.
Rescale both the accumulated value and its compensation before update/merge;
retain the original-width origin when comparing integer inputs. Restore units
only for the requested variance, standard deviation or covariance, with exponent
accounting to avoid an intermediate product overflow or underflow. Correlation
uses normalized centered products directly. Thus an unrepresentable variance
does not invalidate a representable standard deviation or correlation, and tiny
nonconstant inputs are not misclassified as constant. Non-finite observations
and unrepresentable requested results still fail under the strict policy.

The existing aggregate declaration gains an optional secondary column with a
builder; the existing constructor and single-input behavior stay intact.
Computed arguments remain ordinary native projections. Both inputs participate
in binding, source demand, spill records, metadata charging and public explain
declarations. The legacy primitive file provider declines new reducers before
attempting its narrower binding; the shared relational provider owns execution.
Statistics on identical input columns share state only when their null-selection
domains match. Basic reducers incur no statistical-state allocation.

Statistical windows use the same centered moments and pairwise merge. Never
subtract departed values from moments: cancellation can erase a small variance
after a large outlier leaves. Instead retain an aligned power-of-two block cover
of each of the existing at-most-three frame intervals. Reuse complete prior
blocks, observe uncovered fragments through the existing native resident/spill
lookup and merge in row order. A monotone moving interval needs logarithmically
many charged summaries and reobserves each row at most logarithmically many
times. Empty frames, peers, exclusions and selected null pairs retain ordinary
window semantics. The spill path uses the same state and native lookup, with no
unbounded in-memory copy of a partition or external execution provider.

SUM/AVG DISTINCT use exact native equality and membership on the original input
type before additive reduction. NULLs do not enter the membership set; empty or
all-null inputs return NULL. Primitive results retain ordinary SUM/AVG's nullable
Float64 contract, while decimals retain their existing exact scale/overflow
contract. "Exact DISTINCT" describes membership, not a new primitive summation
type. Grouped state uses the existing charged native set; ordered spill sorts
native values, deduplicates across run-block boundaries and updates the same
reducer. Window occurrence counts span all selected frame intervals, so one
value contributes once even when exclusions split its occurrences. Removing the
last selected occurrence removes its contribution; admitting its first adds it.
SQL `SUM(DISTINCT expression)` and `AVG(DISTINCT expression)`, typed measures
and function helpers must converge on these owners. Other DISTINCT combinations
remain explicit admission decisions, never silently ignored modifiers.

## Aggregate filter composition contract

An aggregate-local `FILTER (WHERE predicate)` admits only rows for which the
predicate is true, following the [PostgreSQL aggregate-expression convention](https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-AGGREGATES).
False and NULL exclude the row before argument observation,
distinct membership, local ordering or state updates. A group still exists when
all of its rows are excluded; each measure returns its usual empty-domain result.
Different filters identify different reductions, including shared moments and
quantile state, HAVING reuse and generated output names. Window frame membership
is established before the aggregate filter; excluded rows still determine frame
positions, peers and output rows.

The existing typed measure gains an optional Boolean `filter_column` dependency.
An untyped NULL predicate is also admitted as an empty domain, consistently with
ordinary Boolean expressions. The dependency belongs to both the public frame
declaration and the bound runtime, so projection pruning cannot discard it.
SQL predicates and DataFrame declarations project that Boolean through the
ordinary expression owner. Computed arguments use the same predicate as a lazy
CASE guard, so excluded rows cannot trigger argument errors or effectful work.
Typed callers explicitly own any preceding projection. Predicate subqueries and
parameters retain their ordinary scoped lowering and demand rules.

Vortex-first decision: use the existing Vortex native take/index provider for
selected aggregate input batches and existing typed Boolean keys for window
selection. ShardLoom owns aggregate/filter semantics and charged group mappings;
no new row-table evaluator, fallback or Arrow execution substrate is introduced.
Native ordered spill retains filter dependencies on base records and separates
distinct/quantile domains by both value and filter. Collection records apply
the filter before observing values or local order keys. Unfiltered reductions
retain their existing execution path and state sharing.
Stored frame-bound validation applies that same filter before its first value
observation. Hidden ORDER BY aggregates reuse the existing hidden HAVING
reductions and remain absent from the delivered output schema.

## Exact quantile contract

The next aggregate family is `median`, `quantile_cont` and `quantile_disc`.
They ignore NULL values, return NULL for no selected values and order the
original numeric values before any result conversion. `median` is the
continuous quantile at 0.5. A quantile level is a finite Float64 in [0, 1];
retain its bits in an equality-safe typed descriptor and reject a level attached
to an unrelated reducer. The level is a scalar declaration, never a source
column or an implicitly evaluated application expression.

For N non-NULL observations, continuous interpolation uses position q(N-1),
while the discrete form selects max(ceil(qN)-1, 0). These conventions follow
the linear and inverse-empirical-CDF definitions in the
[NumPy quantile reference](https://numpy.org/doc/stable/reference/generated/numpy.quantile.html).
Compute rank positions from the level's exact binary ratio and an integer count;
converting a large count to Float64 first must not move the selected ordinal.
The discrete result preserves the nullable input dtype. Continuous primitive
results are nullable Float64; interpolate selected endpoints without first
forming a potentially overflowing difference. Continuous Decimal128 results
use precision 38 and max(input scale, 6), matching the existing exact-average
contract: discard no fractional digits silently. The later explicit decimal
rounding contract must also cover interpolation. Exact rank selection does not
turn a Float64 result into exact real arithmetic.

Share one order-statistic owner between compatible levels on the same input.
Resident grouped execution charges retained exact values and rank metadata.
The ordered aggregate path counts non-NULL base observations, then selects the
required ranks from the existing native external value ordering; it must not
rebuild an unbounded in-memory group while consuming a spilled run. Retain
duplicate observations. Preserve original input ordinals as stable tie breakers.

Moving windows maintain a charged balanced index over the currently selected
observations, with reusable node slots as observations depart. Its update and
rank lookup costs are logarithmic in active frame cardinality. Existing native
stored partitions remain the source of original values and output gathering.
The active rank index has its own live-memory admission; spilling input does
not exempt it from that budget or claim a disk-backed rank index. Exclusions
share the union of selected intervals. Cancellation, denied growth and output
failure must release index state and owned native runs, and allow a subsequent
execution to succeed.

Acceptance includes independent complete sorted references, empty/all-null and
singleton cases, q=0/0.5/1, arbitrary finite levels, original-width integer
ordering above 2^53, decimals, duplicates, grouped/scalar/ordered paths, frame
units and exclusions, computed arguments, public lowering and invalid levels.
The original q=0.1 Float64 bits are significant; decimal interpolation may
require an explicit rounding choice when that binary fraction cannot be exactly
represented at the selected decimal scale.

SQL declares `QUANTILE_CONT(expression, literal_probability)` and
`QUANTILE_DISC(expression, literal_probability)`; `MEDIAN(expression)` fixes
the probability at 0.5. Probability bits participate in aggregate identity and
generated names, including HAVING/ORDER BY reuse. They never become source
dependencies. Typed aggregate/window descriptors expose `quantile_level`;
Python function helpers use the same declarations. Missing, nonfinite,
out-of-range or inapplicable levels fail before execution.

## Ordered value and collection aggregate contract

The next part of this same aggregate work unit adds `first`, `last`, `arg_min`,
`arg_max`, `list`/`array_agg`, and `string_agg`. These names refer to grouped or
scalar reductions; the existing positional window functions remain separate
declarations. Aggregate-local ordering uses the existing typed order keys,
with direction and null placement, and retains original input ordinal as the
final tie breaker. Omitting an order uses the input's established row order;
no claim of source-order stability across a changed input is implied. Different
local orders must remain distinct in expression identity and dependency demand.

`first`/`last` retain NULLs and return the original nullable input dtype.
`arg_min(value, key)`/`arg_max(value, key)` skip rows with a NULL value or key,
select on the original key dtype, and return the original nullable value dtype.
Equal extreme keys select the first row under declared tie ordering, then input
ordinal. Lists retain all selected values, duplicates and NULL elements, with
the original child dtype. Strings accept UTF-8 values, skip NULLs and join with
the declared literal UTF-8 separator, including an empty separator. Every one
of these reducers returns NULL for an empty admitted domain. These choices
follow the documented [ordered aggregate conventions](https://duckdb.org/docs/2.0/sql/functions/aggregates)
and [list/string null conventions](https://www.postgresql.org/docs/current/functions-aggregate.html);
implementation uses only ShardLoom/Vortex native owners.

Retaining one selected value and constructing a complete collection have
different resource requirements. Selectors retain only the candidate payload
and comparison keys needed per group. Collections charge retained native
payloads, ordinals, output offsets and string bytes; avoid a separate uncharged
row-object copy. Reuse the native sorting/run store for admitted spill, with
group and source ordinal identity preserved across runs. A complete single
list or string result must fit its output reservation even when input sorting
spills. Do not claim that input spill makes an arbitrarily large individual
result free of memory limits. Window integration preserves frame membership,
exclusions, original output types, cancellation and owned-file cleanup.

Keep aggregate-local order on the existing aggregate descriptor. Window order
continues to define frame positions; it cannot stand in for aggregate-local
order. SQL and Python expressions lower both orders to existing typed order
keys. Separators are scalar declarations and do not create source dependencies.
Within a window, the aggregate receives the selected rows in the window's
partition order. This is its input order when local ordering is absent and the
final tie order when local ordering is present. Stable source ordinals already
break ties in that partition order.

For selectors, compare keys against one compact candidate per group and retain
only its native value and comparison keys. Batch updates should choose the
winning row before gathering its payload, avoiding a separate allocation on
every candidate replacement. Collections retain compact native batch segments
and reserved row ordinals, using the existing native ordering comparator.
Ordered spill must preserve input ordinals for selectors and provide separately
ordered records for collection measures with different local orders. The
streaming collection reducer then retains only the values needed by its result,
without reconstructing an in-memory copy of all input comparison keys.
Window selectors share the existing resident monotone candidate queues and
stored binary summaries, with one comparator for key extrema, local order and
stable ties. An arg-min/max value contributes only its parent validity to key
selection; nested children remain payload until the winning row is gathered.

Collection state reuses the native segmented batch table. Per-measure group
ordinals refer to compact retained value/order columns; they do not copy each
row into a separate scalar object. Sort a group's reserved ordinal selection
once at finalization, then gather its native values. A stored aggregate assigns
each distinct collection ordering its own record kind, sorted by group, local
keys and original ordinal. Its reducer retains only the ordered value payload
needed to construct the result; it does not rebuild the input comparison keys.
List output uses the admitted Vortex list constructor with credited native child
arrays, offsets and validity. UTF-8 concatenation checks the total value and
separator byte count before acquiring an output buffer. Empty string values
count as observed values; they must remain distinct from a NULL or empty domain.

Collection windows retain a charged balanced index of selected partition
positions under the same local-order comparator. Reuse the existing balanced
order-statistic implementation with an explicit comparison boundary rather than
adding a second tree. Updates remain logarithmic; an in-order walk produces the
selected ordinal list in linear output work. Departed nodes are reusable. The
resident window path packs selected ordinals into a reserved flat buffer and
per-row offsets; it constructs native lists only for output batches. The stored
path flushes those ordinals as native list columns in its existing run store.
Both defer payload gathering to their existing source owners. Ordinals and
offsets are charged output state; avoid one retained native list object per
resident frame, whose repeated schema credit would dominate small frames.
Do not materialize a scalar payload tree to carry intermediate window results.
Empty selected lists represent a NULL aggregate result, while a nonempty list
containing NULL values remains a valid list result.

The pinned Vortex 0.90.0 aggregate registry has no flat relational list-collect
or string-concatenation provider. Its `ChunkedArray`, `ListArray`, native payload
gathering and allocator remain the representation providers; ShardLoom owns
group/frame membership, ordering, result admission and cleanup. COUNT observes
validity only, and paired moments test both parent validities before observing
either numeric value. Native spill movement preserves these observation domains;
actual comparison keys, admitted arithmetic and delivered values keep their
existing strict finite-value checks.

Aggregate-local FILTER is implemented through the shared reducers and windows.
Focused native/SQL tests cover empty domains, lazy arguments, all frame units
and exclusions, actual spill, pressure, cancellation, failed delivery and retry.
The nine-suite public matrix passes on both Vortex and Parquet through all five
interfaces, including filter-only source dependencies and complete output types.
Adjacent aggregate and window regressions pass. Grouping-set combinations are
also implemented and included in that public matrix. The broader analytical,
performance and acceptance obligations remain open;
this evidence does not close item 20 or the workspace/package release gates.

## Grouping-set composition contract

`GROUPING SETS` computes each declared grouping over the same input. Output keys
omitted from a set become typed NULLs; genuine NULL keys remain separate from
subtotal rows. Duplicate sets retain duplicate results. An empty set produces
one aggregate result even on empty input, while nonempty sets produce no groups.
`ROLLUP` expands prefixes, `CUBE` expands subsets, parenthesized key lists act as
units, and comma-separated grouping items combine by Cartesian product.
`GROUP BY DISTINCT` deduplicates sets. `GROUPING`/`GROUPING_ID` returns an unsigned
omission mask with the rightmost argument in the least significant bit; arguments
must reference grouping keys. These semantics follow the
[PostgreSQL grouping-set contract](https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS).
HAVING, output ordering and limits apply to the combined grouped result.

Implementation decision: extend the existing bound aggregate node with optional
grouping expansion, retaining the same native aggregate and ordered-spill owners.
Consume the input once and expand one native batch/set at a time. Preserve the
original argument columns and add private nullable group-key views plus a set
identity, so removing a grouping key never changes an aggregate argument and
duplicate sets never collapse together. Indicators are per-set native constants.
Private set identities and helper columns do not enter the delivered schema.

Determine empty input only after completing its producer. Seed only declared
empty sets, with every measure excluded by the existing Boolean filter owner;
the seed establishes a group without contributing a value or COUNT(*) row.
Ordinary nonempty input uses the same aggregate FILTER, DISTINCT, statistical,
ordered-selector and collection contracts. Computed arguments remain upstream
native expressions and are not reevaluated for each grouping set.

Reserve normalized descriptors before allocation, check expansion cardinality,
and charge transient native schema, constant arrays and retained group state.
SQL retains a compact grouping expression until ordinary declaration admission.
Count Cartesian products and cube subsets with checked arithmetic and include
their expanded metadata in that admission before allocating normalized sets or
opening sources; a short CUBE clause must not cause unbounded parser allocation.
Grouping indicators have at most 64 arguments because their result is UInt64;
this does not impose a 64-column limit on grouping sets themselves. Cancellation,
pressure denial, real native spill and cleanup must be tested. Source demand is
the union of actual group, argument, predicate and local-order dependencies.
Both SQL and typed/DataFrame declarations must reach this same owner; repeated
source execution through a UNION tree is not the implementation contract.

The pinned Vortex provider inventory supplies native arrays, constant/null
columns, selected views and persistence, not relational grouping-set execution.
This remains `implement_shardloom_kernel` over those existing native providers,
without a decoded row table or external query-engine integration. Implementation
and five-interface execution pass for both Vortex and Parquet, including duplicate
and distinct sets, empty input, genuine NULL keys, filtered moments/collections,
and complete output schemas. Native pressure/lifecycle and SQL indicator tests
cover shared input ownership, spill, post-aggregate expressions and windows,
invalid scopes and type admission. See the
[public aggregate contract](../reference/native-analytical-aggregates.md).

## Provider decision and semantic boundaries

Decision: `implement_shardloom_kernel` for relational expression binding and
aggregate state, over the existing admitted Vortex 0.90.0 arrays/providers.
The [provider inventory](vortex-public-api-inventory.md) distinguishes Vortex's
already-grouped array reducers from a flat relational group-by owner. Replacing
native grouping with a query-engine integration is prohibited. Reuse existing
Vortex masks, scalar/array dtype and selected-output ownership; explicitly
record any new provider adoption and representation transition.

Extend RFCs 0014/0015/0016/0021/0033/0044 at their current owners. Temporal,
recursive-state and application-function additions need their concrete contracts
in this implementation record before code. New dependencies require the
existing Apache-compatible provenance and feature-boundary review. No existing
unsupported path becomes an external-engine fallback.

## Acceptance and retention

The 347 existing cases are discovery/regression data. Fresh seeds, layouts,
null/type combinations and composed requests supply held-out acceptance. Reuse
the retained Polars observations without rerunning that engine. Freeze each
performance candidate's native baseline, exact source/build, workload and
numeric retention gates before measuring it. Evaluate the declared family and
whole-query controls; no function-count or aggregate suite total establishes a
benefit for every case. Preserve unfavorable observations and drop failed
mechanisms rather than changing thresholds afterward. The dropped item-15
implementation is not restored unchanged.

Use focused independent semantic tests while implementing, then required
workspace formatting, Clippy and tests, native-feature and installed-package
checks at coherent boundaries. Every capability must be exercised through its
real public surface with complete output and source/owner cleanup. Verify
pressure, cancellation, malformed/changed source, spill and output failures
where applicable. Use ClickBench only for a justified shared-runtime regression
check, with its isolated single-pass procedure.

Bulk builds, temporary fixtures and diagnostic evidence stay in local-only
storage under the existing limits. Resolve Cargo output before every new build
setup; do not assume this worktree inherits the primary checkout's configuration.
Retain current inputs, the accepted runtime and compact complete evidence,
then retire obsolete generated payloads after verified replacement.
