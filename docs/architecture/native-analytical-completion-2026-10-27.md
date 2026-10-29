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
records completed native, workspace, installed-wheel and portable checks.
PR #1614 merged at `690a671e` after all 57 hosted checks passed; its
[hosted receipt](../benchmarks/evidence/native-analytical-aggregates-2026-10-27/hosted.json)
preserves the accepted head and individual check results.

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

The October 28 maintainer continuation keeps the shared-numeric-values work
and the remaining item-20 obligations together in PR #1619. Hold that PR open
through implementation and combined acceptance. Use logical commits, prospective
measurement protocols and validation checkpoints inside that PR; individual
optimizations or public interfaces do not require separate PRs. Split only for
a concrete compatibility, dependency, generated-artifact or verification boundary
that makes the combined change harder to review or trust. This delivery decision
does not reduce the checklist or move item 18's I/O work or the 1.1.0 release into
item 20.

The October 28 performance objective is to pursue **under 10 engine seconds**
for the unchanged 347-query cohort within the remaining optimization work.
The latest retained candidate records 15.400097157 seconds, so this requires
about 35.1% further reduction; it is a stretch target, not an achieved result or
a reason to reduce correctness/coverage. Grouped aggregation contributes
5.595029744 seconds, projection 2.870433458 seconds, and ordinary plus multiway
joins 2.501059375 seconds. Use these complete family totals to prioritize
profiling of shared work, not as an attribution of removable overhead. The
shared-owner sequence below records the retained and dropped candidates.
Select further numeric/temporal specializations only from
demonstrated costs. Added capability cases retain
their own coverage and cannot change the original comparison denominator.
Larger-volume and complex-plan acceptance must measure resource behavior and
complete results; the encoded/pruning/streaming architecture alone establishes
no general advantage over Polars. Preserve the original Polars IPC/API-clock
observations and native Parquet/engine-clock observations as distinct evidence.
A later comparative claim requires matched input, resources and clock boundaries;
this objective does not authorize another competitor cohort.

The October 28 maintainer follow-up delegates the execution order to measured
impact. Prioritize shared grouped aggregation, projection and joins: their
retained family totals sum to 10.966522577 of the 15.400097157 engine seconds.
This is workload concentration, not a claim that all of that time is removable.
After each frozen candidate decision, target demonstrated repeated preparation,
validation, copying or operator work in those shared owners. Explicit materialized
ownership and shared-intermediate reuse can move earlier when a repeated expensive
subplan is demonstrated and resource/semantic admission is defined. Plan traversal
work targets measured repeated work or large-plan scaling; recursive CTE support
and relaxed structural limits are capability obligations, not assumed speedups for
the existing cohort. Complete the remaining capability and acceptance checklist
after this evidence-led performance work; no requirement is removed or deferred
into items 18/19 by this ordering decision.

The later October 28 [gain/loss decision](../benchmarks/evidence/native-item20-completion-2026-10-28/complete-cohort-gain-policy.json)
removes percentage benefit cutoffs for the remaining analytical performance
work. After correctness and resource checks pass, lower complete-cohort engine
time ships; higher or unchanged time drops. Advance the baseline as gains
compound. Keep original protocols and threshold results immutable and record
each changed disposition separately. Audit earlier positive-gain drops and
integrate still-applicable changes against the current implementation; do not
add historical savings together as if measured on the same source.

### Shared-owner reuse continuation

The October 28 maintainer suggestions refine the optimization sequence inside
this same item and PR. Preserve the 347-case discovery/regression denominator;
freeze fresh seeds, layouts and compositions for correctness. Each candidate
must identify the existing owner reused, the blocked or repeated execution
path, the work removed, prospective admission, activation/work counters,
complete errors/results, ownership and the frozen benefit decision.

1. The exact short/composite group-key candidate is retained; use its updated
   grouping profile before selecting another mechanism.
2. Typed complete-payload validation is retained for flat records and eligible
   recursive leaves through the existing numeric, temporal and variable-width
   owners. Rich/nullable records keep their recursive owner, field order, parent
   masks and selected-domain obligations. Missing parent coordinates and decimal
   leaves keep their original validation paths. This removes repeated scalar
   construction without relaxing complete-input validation and is separate from
   the dropped expression construction candidate.
3. Exact join-probe identity reuse and once-resolved gather coordinates were
   evaluated and dropped under the prospective benefit gate; do not repeat the
   same mechanisms unchanged.
   The ordinary no-residual `Join::consume` path still hashes/probes each left
   row, and `Table::gather` resolves rows while finding touched segments and
   again while constructing take indices. The candidate reused existing
   directory/index, table/gather and reserved-vector owners, distinguishing
   uninitialized entries from proved misses and preserving NULL policy, complete
   duplicate chains, output order and matched-row bookkeeping. Residual
   predicates stayed outside admission. The touched-chunks-only take contract
   and descriptor memory were included in its bounded work unit.
4. Audit specialized-consumer reachability before refactoring a shared batch
   consumer. Both general relational preparation entry points currently admit
   the file provider only for exactly one file source and no memory/batch
   sources; its input recognizer also requires `ScanSource::File(0)`. These
   source restrictions do not establish semantic equivalence. Start with exact
   count or another small typed family only after proving actual representation,
   dtype/null, arithmetic, ordering, error, memory, cancellation and output
   compatibility. File metadata shortcuts keep their original evidence needs.
5. Use the remaining profiles and representation evidence to choose ordered
   preparation workers or encoded unary expressions. Reuse `AggregateChunkJobs`,
   resident worker grants and native leases where admitted; retain ownership
   through queued, running, completed and unconsumed states. Do not duplicate
   the CPU grant or parallelize ordered arithmetic without equivalence proof.
   For unary dictionary expressions, first show actual dictionaries at that
   boundary, then preserve referenced-domain coverage, selected branches,
   Unicode behavior, NULLs and equality after distinct values map to the same
   transformed result. Do not manufacture a dictionary solely for this path.

PulseWeave inventory, scarcity and proof policies can govern measured choices
among admitted paths. Their presence in a report does not establish active
operator scheduling. Add adaptive feedback only after work/queue/wait/live-byte
evidence warrants it. Selected-row handoff between joins and broader output
credit windows remain conditional on measured copy or downstream pressure,
including retained source bytes and producer-close lifetime proof. This does
not revive previously dropped candidates, bypass provider admission, reduce the
remaining analytical capability checklist or close any competitive gate.

The exact group-key checkpoint is retained for combined acceptance at
19.754020577 engine seconds for 347 cases, followed by typed complete-payload
validation at 18.304092867 seconds. The latter reuses numeric finite/domain
validation without output buffers and canonical Boolean/variable views without
per-row scalar construction. It preserves the established recursive path for
rich values and nullable parents. Complete projection and ordering families
improve 13.60% and 22.92%, respectively, with a positive 7.34% whole-cohort
balance and no material regression flags. The
[disposition](../benchmarks/evidence/native-item20-completion-2026-10-28/payload-validation-disposition.json)
records 28 payload checks, seven numeric checks, strict native lint, two public
regressions across all five interfaces and complete independent replay of 160
fresh observations (28,940 output rows). Four original Boolean-column negation
declarations failed before execution; their corrected DataFrame declarations
use nullable Boolean equality before predicate negation. The original failures
and all 156 successful observations remain intact. This is a candidate checkpoint;
combined native/workspace/package, portable and hosted acceptance remain open.
The subsequent join identity and gather-coordinate candidate is dropped:
its single unchanged-cohort pass records 18.527516365 engine seconds, 1.22%
above the retained checkpoint. Ordinary joins improve 0.26349% and multiway
joins 0.02803%; neither meets the frozen 10% family benefit gate. All 347
complete results pass without material regression flags. The
[join disposition](../benchmarks/evidence/native-item20-completion-2026-10-28/join-reuse-disposition.json)
preserves the candidate, complete replay of 160 fresh observations and
7,331,860 result rows, initial failures and corrected continuations. Twenty-four
large CLI correctness observations use acknowledged streaming delivery without
an engine clock; they support correctness only. Both runtime mechanisms have
been removed. The independent six-kind join/ownership public regression remains;
workspace lint and all 3,274 workspace tests pass after removal. No replacement
performance pass was taken for that removal; the retained result at that point
remained 18.304092867 seconds.

The source and cohort reachability audit does not admit a broad provider bypass.
The existing grouped `CountWorkers` consume `GroupedAggregateStates`, whereas
general memory, Parquet-demand and intermediate arrays reach the shared
`native_relational_aggregate::Aggregate`. The 347 discovery queries contain
no count-only grouped aggregate: their grouped cases use SUM alone or mixed
SUM/AVG/MIN/MAX/COUNT. Seventeen direct-scan scalar count cases total
0.367273915 retained engine seconds; two further scalar counts consume joins.
Merely exposing grouped count workers to those inputs would not address the
measured mixed-aggregate cost. Scalar COUNT(*) already adds batch cardinality
without a row loop; COUNT(column) uses native validity. Source equivalence,
first-occurrence group order, exact arithmetic, cancellation and ownership still
need proof for any consumer extension.

The refreshed seven-case diagnostic profile on the retained group-cache and
typed-payload implementation passes complete results with unchanged inputs,
references, resources and isolated guards. It identifies a remaining validation
gap: a rich or decimal sibling routes the entire record through the recursive
owner, where eligible scalar leaves still used scalar construction. Temporary
instrumentation was removed before the candidate; diagnostic spans do not
replace full-cohort evidence.

The recursive-leaf extension reuses the same typed validator only after the
existing owner resolves the observed selection. Its
[prospective protocol](../benchmarks/evidence/native-item20-completion-2026-10-28/recursive-payload-validation-protocol.json)
and [disposition](../benchmarks/evidence/native-item20-completion-2026-10-28/recursive-payload-validation-disposition.json)
retain all 347 complete results and 12,565 rows at **17.729495249 engine seconds**,
a 3.14% complete-cohort reduction from 18.304092867. Projection improves 18.07%,
meeting the alternative family-benefit gate with positive overall balance and
no material case/family regression flags. Twenty-six payload checks, strict
native lint, four public regressions and 160 fresh five-interface observations
pass; complete replay checks 33,260 result rows against independent references.
The four public regressions also verify the retained six-kind join/ownership
test after the dropped join implementation. This remains a checkpoint inside
PR #1619; combined acceptance and the remaining analytical capabilities are
open. Grouping, projection and combined joins still exceed their one-second
targets. No new competitor observation or superiority claim is made.

The subsequent [validation supplement](../benchmarks/evidence/native-item20-completion-2026-10-28/recursive-payload-validation-followup.json)
records 5,011 passing native tests with 35 existing opt-in tests ignored,
strict workspace lint and all 3,274 workspace tests passing. Source inventories
match the measured candidate; intervening changes are documentation only.
Installed-package acceptance, the complete portable packet and hosted acceptance
of this new source remain open alongside the remaining item-20 implementation.

The [demand-driven hashing protocol](../benchmarks/evidence/native-item20-completion-2026-10-28/group-hash-demand-protocol.json)
and [retained disposition](../benchmarks/evidence/native-item20-completion-2026-10-28/group-hash-demand-disposition.json)
remove eager text-domain hashing before the existing exact membership cache.
Flat UTF-8/binary keys use on-demand hashing only in null-equal membership without
an other-set probe. Complete domain validation, late dictionary-code errors,
exact hash bits, first-occurrence ordinals and ordinary cache-decline lookup
remain intact. Vortex's checked borrowed text view validates UTF-8 without
cloning an unused byte owner. Matching joins, nested keys and retained unique
key payloads keep their established preparation. A focused 2,053-row repeated-key
case validates the full domain, hashes one input key and one retained output key,
and allocates hash storage only for that retained key.

All 347 cases and 12,565 rows pass at **17.42942424 engine seconds**, a 1.69%
complete-cohort reduction from 17.729495249. DISTINCT improves 10.80%, satisfying
the prospective family-benefit gate with positive overall balance; grouping
improves 3.31%. No material case/family regression flags occur. These remain
single observations with modest observed benefit, not statistical or causal
attribution. The final focused relational run passes 189 tests, strict native
lint passes, and four public regressions plus 160 fresh five-interface
observations pass. Complete replay checks 303,260 result rows against independent
references. Preserve the initial function-size lint failure and the public
launch with missing Arrow (zero tests), followed by their corrected passes.
No worker or CPU-grant change is introduced. Dictionary unary execution remains
conditional on actual encoded arguments reaching the function boundary; Parquet
page encoding alone is not that evidence. Combined item-20 acceptance stays open.
The full native gate passes 5,015 tests with 35 existing opt-in ignores;
workspace lint and all 3,274 default workspace tests pass on the same source.
Formatting passes. Combined installed-package, portable and hosted acceptance
remain separate from this local checkpoint.

The selected-child/direct-decimal validation candidate is **dropped** under its
unchanged prospective gate. All 347 queries and 12,565 complete rows pass at
17.670435985 engine seconds, 1.38% above the retained 17.42942424-second
checkpoint. Projection and ordering do not improve; no case or family exceeds
the material-regression thresholds. Four five-interface public regressions and
160 fresh observations (33,260 complete result rows) pass. The
[disposition](../benchmarks/evidence/native-item20-completion-2026-10-28/selected-payload-validation-disposition.json)
preserves the candidate source, complete results, source-linked focused checks
and all earlier failed attempts. The evidence freeze initially failed on a
helper path type before any query; the corrected freeze changes no runtime,
workload or benefit threshold. Full candidate suites were deliberately not run
after its benefit failure; the retained source passes its own broad gates below.

The candidate's malformed-text regression also exposed an existing
zero-retention scalar-validation defect. Keep the shared checked native UTF-8
access and its provider/payload regression tests while removing both proposed
optimizations and their counters. The
[correctness addendum](../benchmarks/evidence/native-item20-completion-2026-10-28/selected-payload-correctness-addendum.md)
records the root cause and the disproved initial hypothesis. The combined
candidate timing cannot isolate this repair's cost. No replacement performance
observation is taken merely for optimization removal: at this checkpoint,
17.42942424 seconds is the last accepted optimization measurement, not a measurement of
the additional correctness repair. The [post-removal validation record](../benchmarks/evidence/native-item20-completion-2026-10-28/retained-text-validation.json)
binds the six retained source files to passing formatting, strict native and
workspace lint, 5,016 native tests (35 existing opt-in ignores), and 3,274
workspace tests. The unchanged provider source retains its 146 passing tests,
one existing ignore and strict lint. Both rejected mechanisms and their modules
are absent. The earlier public and fresh-cohort results belong to the combined
dropped candidate; they are not new package acceptance of this retained source.
Combined item-20 acceptance stays open.

The [typed numeric expression protocol](../benchmarks/evidence/native-item20-completion-2026-10-28/typed-numeric-expression-protocol.json)
and [retained disposition](../benchmarks/evidence/native-item20-completion-2026-10-28/typed-numeric-expression-disposition.json)
remove per-row generic value construction for binary arithmetic over identical
I64/U64/F64 input/output storage. Exactly `COALESCE(column, finite non-null
same-type literal)` uses the existing numeric owner to construct an independent
native output directly, avoiding general selected-branch coordinates and gathers.
The shared arithmetic helpers, left-before-right preparation, finite-operand
observation before NULL propagation, first-error order, output allocation and
validity rules remain authoritative. Other widths, decimal/temporal values and
general lazy forms retain their existing owners. No dependency, provider,
public schema or external evaluator is introduced.

All **347 queries and 12,565 complete rows** pass at **16.249797458 engine seconds**,
6.77% below 17.42942424. Grouped aggregation improves 13.75% and global aggregation
21.31%, meeting the prospective family-benefit gate with a positive overall
balance. The single material case flag is
`aggregate_optional_value_group_key_category_mixed_arithmetic_all`: it adds
0.021396124 seconds while the cohort saves 1.179626782 seconds. Complete results
and physical read demand are unchanged; no family crosses its regression
threshold. Retain this observed tradeoff under the October 28 proportionate
regression policy, preserving the case for the continuing grouping work.
One observation does not establish its cause or statistical confidence. This
comparison includes the previously untimed UTF-8 correctness repair.

Nine focused numeric tests, 198 relational tests, four existing five-interface
regressions and 160 fresh five-interface observations pass. Complete fresh replay
checks 410,420 result rows and 17,364 input rows. Formatting, strict native and
workspace lint, 5,025 native tests (35 existing opt-in ignores), and 3,274
workspace tests pass on the measured source. The immutable patch reconstructs
all six changed source files from the recorded commit. Preserve the initial
lint failure and three failed focused-test attempts: cancellation, temporal and
error-order fixtures required correction; the invalid dictionary panic and
disproved constant-array refusal assumption are not claimed as runtime fixes.
Combined installed-package, portable and hosted acceptance remain open, as do
the speed targets and every remaining item-20 capability/workflow obligation.

The subsequent [integer-extrema checkpoint](../benchmarks/evidence/native-item20-completion-2026-10-28/integer-extrema-fusion-disposition.json)
retains one grouped traversal for adjacent integer MIN/MAX and eligible COUNT.
It reuses the existing prepared numeric owner, exact group ordinals and aggregate
states. Bounded inspection admits transparent single-chunk ownership wrappers;
encoded validity keeps COUNT at its original position. Unequal counts, possible
overflow, noninteger values and other unproved shapes retain ordered execution.
The complete 347-query observation validates all 12,565 rows at
**15.992156898 engine seconds**, saving **0.257640560 seconds** against
16.249797458. No material case or family regression flags occur.

The original 10% benefit gate failed. The maintainer then explicitly replaced
that rule with complete-cohort gain/loss and directed retention; all six source
files were restored byte-for-byte to the measured snapshot. The original gate
and results remain unchanged, and no query was rerun to change the disposition.
Ten focused tests, 207 relational tests, four public regressions and 160 fresh
five-interface observations pass, including 6,860 result rows and 17,364 input
rows. Formatting, strict native/workspace lint, 5,035 native tests (35 existing
opt-in ignores) and 3,274 workspace tests pass. The source-linked disposition
preserves six failed attempts and verifies the exact source patch, release
runtime, complete result replay and broad test inventories. Combined item-20
acceptance remains open.

The earlier-drop audit found two positive complete-cohort observations rejected
by percentage floors: borrowed variable output (30.118456795 to 29.225110351
seconds) and expression construction/reuse (23.079365459 to 22.941843204 seconds).
The [current-source recovery](../benchmarks/evidence/native-item20-completion-2026-10-28/positive-gain-recovery-disposition.json)
retains applicable borrowed variable output, single-evaluation text/binary
construction and exact completed-function reuse while preserving later numeric,
validation and ownership improvements. All 347 queries and 12,565 rows pass at
**15.400097157 engine seconds**, saving **0.592059741 seconds** against the
15.992156898 checkpoint. Historical savings are not added to this measurement.
One grouped-aggregate query adds 0.013006291 seconds with identical physical
demand counters; its original flag remains, with no family regression flag.
Single observations do not establish cause or statistical confidence. The
join-reuse and selected-payload candidates were slower overall and remain dropped.

The recovered source passes 95 focused expression tests, eight variable-output
tests, one exact temporal-identity test with its calendar feature enabled,
665 relational tests (four existing opt-in ignores), five public regressions
and 160 fresh five-interface observations. Independent replay verifies 412,220
complete result rows and 16,396 input rows. Formatting, strict native/workspace
lint, 5,045 native tests (35 existing opt-in ignores) and 3,274 default workspace
tests pass. Preserve both zero-test temporal launches and the rejected acceptance
attempt; the corrected exact feature-enabled test supplies the missing proof.
Combined package, portable and hosted acceptance remain open.

The maintainer-requested Full43 follow-up uses the original resident Parquet
directly through shared native source-demand execution, with no ingest or
preparation command. The updated standard runner declares its input format,
retains source-demand/ownership diagnostics and executes each query exactly once.
Historical Vortex-input Full43 timings retain their separate input boundary.

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
  Item 18 extends shared demand-aware defaults and owns the
  [cross-surface and format acceptance gate](native-universal-io-performance-coverage-2026-10-24.md#cross-surface-and-format-acceptance-gate)
  before the planned 1.1.0 bump. Item 19 then extends the same coverage for newly
  admitted rich types; this sequencing does not defer item 20's own acceptance.

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

The standalone candidate is **dropped** after its frozen single pass. All 347
queries, 12,565 rows and complete schemas validate in 29.225110351 engine seconds
against 30.118456795. The observed reduction is 3.0% overall, 8.9% for projection
and 6.9% for strings, with no material regression flags. Neither declared family
nor the complete cohort meets its prospective 10% benefit threshold. The source
overlay, tests, build and complete observations remain preserved; the borrowed
output accessor and its call sites are removed from the retained implementation.
The numeric checkpoint remains the accepted comparison baseline. No retry or
threshold change is used to promote this candidate.

### Bounded retention at streaming ordering boundaries

The subsequent candidate replaces complete payload retention only for
an immediate, bounded Sort/Limit over streamed input. The existing ordering
owner already selects a bounded ordinal prefix after consuming all input; it
still first copies every source payload into retained state. The source-input
contract requires each source batch to be released before requesting another.
Retaining references to all source batches is therefore not an admissible
optimization.

Reuse the existing fallible stable ordering, selected-row gather, native output
builders and credited state. Validate every input payload at the existing
streaming boundary, including values which will not survive the limit. Only
then validate all ordering keys and select the next prefix from the prior owned
prefix and the current source batch. Copy that prefix independently before
returning to the source. Tiny input batches can accumulate independently copied
pending rows up to four times the requested prefix before compaction, avoiding
repeated whole-prefix copies for one-row producers. The memory grant still
admits actual bytes; the row bound does not promise a fixed byte footprint.
Stable ties keep earlier source rows before later ones, including pending rows.
Consume the complete input even when the requested prefix is empty.

The initial admission is streamed input, no configured spill, ordinary validated
payload policy, a bounded prefix and supported flat payload leaves. Other plans
keep their existing native ordering path. Ordinary execution must retain its
existing later payload-validation order. Do not move projections or filters
across ordering or limits, introduce a second sorting implementation, or report
discarded rows as copied. Tests must cover every-row validation, stable ties,
null order, multiple/empty batches, offsets, zero demand, source release,
retained-output lifetime, memory refusal, cancellation and composed operators.
Adoption requires a separately frozen complete-query comparison and neighboring
cases. The implementation passes 3,407 native tests (32 existing opt-in ignores),
3,268 workspace tests, strict native/workspace checks and 29 public ordering
checks. Its separately frozen single pass validates all 347 queries, 12,565
complete rows and schemas in **28.017558877 engine seconds**, against the
unchanged **30.118456795-second** numeric checkpoint. The observed reduction is
7.0% overall and 40.5% for ordering; the declared family-benefit gate passes.
No family crosses its material-regression threshold. These observations do not
isolate the cause of changes in unaffected families.

The original `aggregate_metric_scalar_mixed_arithmetic_all` observation crosses
the case threshold, from 0.065623334 to 0.081370208 engine seconds. Preserve it
in the complete total. A separately frozen diagnostic tests that exact case and
its direct/COALESCE neighbors in three baseline/candidate pairs, ordered AB, BA,
AB. All 18 executions validate, and none reproduces the material-regression
rule. The flagged case's role medians are 0.080949125 and 0.067029125 seconds.
The disposition is "not reproduced in the bounded diagnostic"; its original
cause remains unassigned, and no statistical-confidence claim follows. The
[shared performance acceptance](../benchmarks/native-shared-analytical-performance-2026-10-27.md)
retains numeric reuse and bounded ordering after the 194-test installed-package
gate, complete portable replay, 18 corruption controls, isolated source-only
replay and verified generated-payload retirement. The borrowed-output candidate
remains removed. PR #1615 merged at `5a0b8c0f` after all 57 hosted checks passed
and automated review completed without findings; its
[hosted receipt](../benchmarks/evidence/native-shared-analytical-performance-2026-10-27/hosted.json)
retains that evidence. The broader item-20 obligations remain open.

### Pending shared-performance work after temporal-fidelity acceptance

Temporal-fidelity PR #1616 is accepted and merged at `51cf54fd` after all 57
hosted checks pass. Continue the existing item-20 queue in this order. These are
candidate packages, not claims
that the listed paths are currently performance bottlenecks. Preserve the
already-retained streamed prefix implementation, shared group ordinals,
typed SUM/AVG updates, completed-expression reuse, and all existing behavior.

1. **Broaden bounded streamed retention — accepted and merged.** Extend the existing
   `StreamingPrefix` owner to admitted decimal and list/struct payloads where
   ownership and complete validation can be preserved. Validate every incoming
   payload before key observation or row discard; retain independent output
   ownership and the existing source-release boundary. Attribute retained and
   copied **bytes**, including nested and variable-width payloads, in addition
   to row counts. Do not retain source-batch references or change ordinary
   execution's later payload-validation order.
   The [October 28 acceptance](../benchmarks/native-rich-bounded-retention-2026-10-28.md)
   records 27.374316659 engine seconds for all 347 cases against a fresh
   28.118399295-second temporal baseline, a 10.36% projection-family reduction,
   no candidate material-regression flags, complete 176-case held-out results,
   200 installed-package tests and portable replay. PR #1617 merged at
   `328d3bf0` after all 57 hosted checks passed and automated review completed
   without findings; prior observations and temporal-baseline flags are preserved.
2. **Share key and value movement.** Resolve join key columns once per batch
   and reduce repeated `Table::locate` work using the existing native table and
   batch owners. Admit dense integer or dictionary-code mappings only with
   explicit resource/range admission and exact equality, first-occurrence,
   NULL, signedness, width, duplicate and changing-domain semantics. Add
   exact original-width typed MIN/MAX updates, a membership-only path for
   zero-measure DISTINCT, and typed independently-owned join gather. Preserve
   row order, ownership credits, cancellation and spill behavior where already
   admitted; do not generalize earlier dropped counted-join or fused-measure
   mechanisms by renaming them.
   The [shared-key acceptance](../benchmarks/native-shared-key-lookup-2026-10-28.md)
   retains the bounded key directory, membership-only DISTINCT and current-batch
   join build addressing: 25.508976866 engine seconds for all 347 cases versus
   the reused 27.374316659-second checkpoint, with no material regression flags.
   Grouped aggregation improves 12.44% and DISTINCT 17.39%. Complete fresh
   correctness, native/public/package gates and portable replay pass. PR #1618
   merged at `685f16c7` with all 57 checks passing and no review findings.
   The subsequent [numeric acceptance](../benchmarks/native-shared-numeric-values-2026-10-28.md)
   retains original-width MIN/MAX and independently owned numeric gathering at
   23.079365459 engine seconds for all 347 cases and 12,565 complete rows.
   Global aggregation improves 25.44%, grouped aggregation 13.37% and joins
   10.43%; the original isolated case flag remains, and its bounded diagnostic
   does not reproduce it. Complete 80-case fresh correctness, 202 installed
   package tests, 30 corruption controls and isolated portable replay pass.
   Numeric hosted acceptance remains separate; continue the remaining item-20
   work, including the expression package, in the same consolidated PR #1619.
3. **Reduce expression construction and reuse work.** For UTF-8/binary
   results, replace repeated callback evaluation in sizing and writing with
   single evaluation using bounded, accounted staging or a native append path.
   The current reuse binder omits function outputs and does not bind argument
   subtrees; extend deterministic function reuse only when every semantic
   parameter is part of identity and lazy-domain selection, error order,
   cancellation and ownership remain unchanged. Trace expression-construction
   call sites before attributing complete-expression duplicate work. Profile
   numeric and temporal typed loops before selecting any specialization; do not
   infer that generic source shape alone warrants one.

The [October 28 expression protocol](../benchmarks/evidence/native-item20-completion-2026-10-28/expression-protocol.json)
records the prospective gate. The October 28 expression disposition drops
this candidate under that frozen benefit gate. The unchanged 347-case
comparison completed all 12,565 rows at 23.079365459 engine seconds for the
accepted numeric baseline and 22.941843204 seconds for the candidate, a 0.596%
reduction. No declared target family reached 10%; projection improved 2.897%,
while grouped aggregation, global aggregation and composite did not improve.
The undeclared strings-family result is not used for retention. There are no
material case or family regression flags. Held-out replay passed all 160
observations and 3,280,220 complete result rows. Candidate runtime and test
changes were removed, restoring the accepted numeric implementation; local evidence retains the
removal patch and source inventory for the consolidated acceptance packet.
The accepted numeric baseline is unchanged.
Full item 20 continues in consolidated PR #1619.

The exact group-key cache candidate follows its frozen
[prospective protocol](../benchmarks/evidence/native-item20-completion-2026-10-28/group-key-cache-protocol.json).
Its local 347-case observation passes all 12,565 complete result rows at
**19.754020577 engine seconds**, a 14.408% reduction from the numeric checkpoint.
Grouped aggregation falls from 9.969800537 to 6.837246991 seconds, a 31.420%
reduction. There are no material case or family regression flags under the
unchanged prospective thresholds. This is one observation per case, with no
statistical confidence or competitor-superiority claim. The candidate is retained
for the combined item-20 acceptance; installed-package, portable and hosted
acceptance remain separate.

The bounded cache extends the existing batch-local proved identities to one or
two integer, Boolean or raw short-text/binary keys. Sampling only admits the
cache; ordinary exact lookup establishes every first group. Tests cover collision
misses, normalized inline padding, NULL/empty/NUL distinctions, changed batches,
late long or unique values, all integer widths, unsupported domains and released
credits on cancellation. Native lint and the 3,481-test native suite pass, with
32 existing opt-in tests ignored. Five-interface grouping/DISTINCT and retained
reuse pass. Fresh independent correctness replay passes all 160 observations
and 1,273,610 complete result rows across two seeds and two layouts. The complete
comparison, failures, source snapshot and process evidence remain in the local
item-20 packet for consolidated portable acceptance.

The next typed-validation candidate uses this measured cache checkpoint as its
baseline without another baseline execution. Projection remains 4.497855210
seconds; ordinary and multiway joins total 2.767251541 seconds. The per-family
one-second targets and complete under-ten-second objective remain open.

The following construction and reuse details preserve the evaluated proposal;
they are not an accepted runtime change or a current-runtime description.

The single-evaluation boundary is the existing bound expression's UTF8/binary
output construction. Use native fallible host allocations, offsets, validity
and `VarBinArray` ownership. The pinned Vortex append builder grows through
infallible buffer allocation; it cannot by itself preserve ShardLoom's memory
refusal contract. Wrap existing native buffers with admitted growth and account
for the old/new overlap. Copy each completed scalar into owned output before
its scratch credit can be reused; do not retain uncredited per-row strings or
source slices. The callback order and first semantic failure remain stable;
actual allocation failures remain explicit. Primitive and prefix-validation
construction retain their existing owners.

Function reuse compares the complete bound function identity, including regex
pattern and fixed compilation policy, JSON path, parse tolerance/format,
temporal input/output domains, timezone, ambiguity and month-end policy.
Identical functions and full-input argument trees may reuse earlier completed
projection outputs. Selected branches of CASE/COALESCE retain isolated scopes;
no expression is moved before its original evaluation point. This is an
extension of the current reuse binder, not an answer cache or new evaluator.

Vortex-first classification: use the already admitted native host-buffer,
validity and variable-array providers, with ShardLoom construction and semantic
identity policy. No dependency, provider permission, Arrow execution path or
external query engine is introduced. Acceptance includes independently owned
buffers, producer closure, exact lazy/error behavior and complete-query proof.

For any future expression candidate, freeze a record naming the removed operation, existing
native owner reused, activation counter or profile, targeted cases and
unrelated controls, plus the current prospective whole-query benefit and
material-regression gates from the [shared-performance protocol](../benchmarks/native-shared-analytical-performance-2026-10-27.md).
Do not invent or revise thresholds after observing results. Preserve every
observation and drop candidates that fail their gates. Keep all 347 existing
cases as discovery/regression data; held-out acceptance uses fresh seeds,
layouts and composed variations. Include high-cardinality groups, sparse,
signed, mixed-width and duplicate keys, tiny batches, changing dictionaries,
larger join outputs, bounded K and full sorts with offsets, fresh skew/shuffle
variations, and memory denial, cancellation or spill cases where those paths
are already admitted. Preserve the original Polars observations and their Arrow
IPC input and complete-API clock. ShardLoom's retained engine observations use
Parquet; do not combine those clocks into a new speed ratio. No new performance
claim follows from this queue.

The counted-join, fused-measure, retained-output, borrowed-output and this
expression candidate have failed their respective retention gates. The
borrowed-output result is documented above; the others retain their drop
records. Broader item-20 obligations remain active,
including temporal/time-series workflows, statistical and richer aggregates,
numeric semantics, recursive composition, explicit batch functions,
diagnostics, resource/failure behavior and format/interface parity.

#### Typed numeric values candidate contract

The October 28 maintainer update permits retaining a substantial net workload
improvement while recording a small isolated timing regression as follow-up
work. Apply the [program's tradeoff policy](native-broad-query-program-2026-10-26.md)
to subsequent acceptance decisions. Original prospective protocols and complete
observations remain unchanged; a threshold flag does not by itself block
implementation when the overall benefit clearly outweighs its impact.

The next candidate retains the accepted shared-key runtime and its complete
25.508976866-second observation as the baseline. Its
[prospective protocol](../benchmarks/evidence/native-shared-numeric-values-2026-10-28/protocol.json)
is frozen before runtime edits. Reuse the complete 347-case cohort once, without
another baseline run; fresh correctness cases and activation tests remain
separate from the benefit observation.

Source inspection identifies two repeated operations in existing shared owners.
Numeric MIN/MAX currently dispatches the physical type, constructs a scalar key
and compares cloned generic cells for each row. Selected output copying already
uses native Vortex take/cast and then reconstructs each numeric value through
`StatValue`, `Value` and scalar writes. Retain native selection and independently
owned output buffers; remove the repeated scalar dispatch/conversion, not the
ownership copy. The earlier borrowed-output candidate remains dropped.

Bind original-width numeric slices and validity once for scalar/grouped MIN/MAX.
Preserve ordered updates, exact signed/unsigned limits, NULL skipping, finite
float checks, first-value ties including signed zero, checked counts, partial
state on failure and the original cancellation cadence. Other scalar, nested,
decimal, temporal and distinct reducer paths retain their existing contracts.
Copy selected numeric storage into the existing credited result buffers, with
the same NULL slot initialization, declared dtype, selected-row validation,
temporal tick checks and empty/clone/slice lifetime. Do not retain source buffers
or move validation across a lazy observation boundary.

Vortex-first decision: **use_vortex_native_provider** for the already-admitted
PrimitiveArray, validity Mask, native take/cast and host allocation; extend the
existing ShardLoom aggregate consumer and result builder. Pinned provider
`to_buffer`/`as_slice` methods retain their physical-type/host preconditions.
No provider, public schema, dependency, scheduler or external evaluator changes.
Tests must compare every original-width result and error/partial-state outcome
against the existing scalar route, exercise encoded/null/empty inputs, and prove
independent output lifetime and cleanup through ordinary consumers. The frozen
complete-query gate determines retention; source shape alone is not proof of a
bottleneck or performance benefit. The [local acceptance](../benchmarks/native-shared-numeric-values-2026-10-28.md)
meets the declared family-benefit gate with a 9.52% whole-cohort reduction;
the maintainer's net-benefit disposition and unchanged original flag are recorded.

#### Shared key lookup and addressing candidate contract

The accepted candidate extends `RowSet`, `RowIndex`, `Batch` and `Table` rather
than adding a second grouping or join implementation. A single integer key
may use an exact per-batch directory only when its observed range is at most
16,384 slots, at least four input rows amortize each slot, and the directory
fits within one sixteenth of available native memory. Native UTF-8/binary
dictionary codes use the same admission. Each first code/value visit still
uses ordinary exact hashing and equality; subsequent visits reuse only the
confirmed ordinal. Rebuild the directory for each batch, preserve first
occurrence, nullable codes and values, duplicate dictionary values and changing
domains, and release its credits before gathering retained payloads. Sparse,
small, composite or resource-declined domains keep existing native execution.

Zero-measure grouping uses existing membership-only insertion. Join build
hashing binds directly to the current batch and resolves retained row locations
only for comparisons that require them. Duplicate fanout, error/cancellation
boundaries and output order remain. Typed extrema and independently owned
numeric output construction are separate subsequent candidates in this package.

Vortex-first review checks the pinned 0.90 primitive arrays, masks, dictionary
codes/value domains and typed buffers already owned by the key layer. The
existing flat-row `RowIndex` remains the authority for group identity; upstream
grouped-array reducers require a different representation and do not replace
it. No external provider, new execution substrate or dependency is introduced.

Before implementation, `protocol-key-r1.json` freezes the accepted
27.374316659-second complete 347-case result as the baseline, with no baseline
rerun. The existing 10% complete-cohort or declared-family benefit rule applies
to `grouped_aggregation`, `joins`, `multiway_joins`, `distinct` and `composite`,
with unchanged case/family material regression thresholds.
Fresh correctness cohorts use seeds 202610281337 and 202610281509, shuffled and
clustered layouts, and dense-negative, sparse/extreme, high-cardinality,
nullable, skewed and changing-dictionary controls. Activation counters cover
directory lookups and confirmed-ordinal reuse, ordinal allocation, and retained
row-location searches. Complete independent results, five-interface and
resource/ownership evidence remain required before acceptance.

The local acceptance meets that contract with 160 fresh observations and
2,345,274 complete output rows, 25 declarations across five public interfaces,
3,442 native Vortex tests (32 existing ignored), 3,274 workspace tests, 1,032
native CLI tests and 201 installed-wheel integration tests without skips.
Twenty-three corruption controls and an isolated source-only replay verify the
saved evidence with Python assertions disabled. The original lint failure,
packaging limit refusal and interrupted control-test corrections are preserved.
No additional baseline, Polars or Full43 run was performed, and no package
publication or item-20/competitive-gate completion is implied.

#### Rich bounded retention candidate contract

Extend the existing streamed prefix owner, retaining its cap of 16,384 rows,
four-cap pending-batch amortization, no-spill admission and independent ownership.
The native payload copier gains a validation-only mode through the same field-
major traversal. Scalar writers retain zero rows; decimal storage validates its
existing wrappers without constructing value buffers; nested coordinates and
parent validity keep their existing semantics and memory credits. Identity
selection may be omitted only for a complete, nonnull row domain in this mode.
Hidden children and unused dictionary entries must remain unobserved. Ordinary
copying and private unobserved-measure movement keep their existing paths.

Vortex-first classification: `implement_shardloom_kernel` extends ShardLoom's
validation and ownership policy around already-admitted Vortex 0.90.0 providers.
Vortex has native take, validity, list/struct, decimal and array-buffer surfaces;
it does not enforce this complete-validation-before-discard contract or the
credited independent source-release boundary. Reuse those providers and the
existing native Sort owner, selection and gather. No new dependency, decoded
Arrow execution, external engine or scheduler is admitted. Existing provider
and no-fallback certificates remain required. Provider decoding/scratch is
accounted separately from owned retained payload.

Count the byte extents of the newly written owned payload buffers at each
ordering retention copy, including nested children, offsets, validity and repeat
compaction, including key columns when they are part of the copied record.
Exclude separately built keys, provider scratch, metadata and final delivery
from this narrow copy counter. Separately report the maximum owned payload-buffer
bytes held by a bounded-prefix stage between input callbacks; that maximum is
neither a plan-wide sum nor the inclusive memory-pool peak. Include activation
and full-validation row counts. Test byte equality on fixed and variable-width
fixtures, source release, retained child lifetime and error/cleanup boundaries.

Freeze a fresh temporal baseline and candidate with identical release settings,
since temporal fidelity changed after the retained 28.017558877-second
observation. Execute each of the unchanged 347 discovery cases once per runtime.
Keep the prior observation intact, and apply the existing 10% complete-cohort or
declared-family benefit gate with projection and ordering predeclared. Material
regressions require both 20% and 0.010 engine seconds per case, or both 20% and
0.050 seconds per family. Fresh seeds, shuffled/skewed layouts and composed rich
top-K requests provide held-out correctness acceptance. Freeze inputs, complete
expected results, source/runtime hashes and the protocol before queries. No
new external-engine observation, Full43 repeat or statistical-confidence claim
is part of this candidate.

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

## Temporal fidelity implementation contract

This continuation implements the temporal requirement of item 20 before the
dependent as-of and time-series operators. The comparator is a typed columnar
engine preserving the complete logical domain across native input, expressions,
ordering, grouping, spill and independently read-back output. Admitting a dtype
at one boundary alone does not establish support. This section is a prospective
contract; the completion checklist remains open until its acceptance passes.

### Representation and conversion

Keep the existing `date32` and `timestamp_micros` names and behavior compatible.
Introduce shared typed temporal metadata for date64 (epoch milliseconds at
midnight), time-of-day and timestamps in seconds, milliseconds, microseconds
and nanoseconds, timezone-bearing timestamps, and fixed durations. A physical
integer width must never identify a logical temporal type. Canonical new type
names are `date64`, `time(s|ms|us|ns)`, `timestamp(s|ms|us|ns[,zone])` and
`duration(s|ms|us|ns)`; `timestamp(us)` is the existing `timestamp_micros` alias.
SQL, Python and typed relations must resolve these through the shared owners.

Timestamps and fixed durations preserve their complete signed-64-bit tick
domain, including statistics and selected results outside a calendar library's
formatting range. Date64 values must be whole epoch days; time-of-day values
must be in the half-open interval from midnight to the following midnight.
Validate value restrictions at the ordinary payload validation boundary,
including discarded streamed payload, without evaluating unselected expressions.
NULL, empty, constant, dictionary, chunked, sliced and nested-parent cases retain
their established ownership and error contracts.

Timestamp timezone metadata describes an instant's presentation zone. A naive
timestamp is a civil value with no inferred system zone. Changing units is an
explicit exact conversion: use widened integer arithmetic, reject overflow and
nonzero discarded precision, and never reinterpret unchanged ticks as a new
unit. Explicit timestamp-to-date conversion floors to the applicable calendar
day. Converting a zoned timestamp to another named zone keeps the instant;
assigning a zone to a naive civil timestamp is a separate operation. Comparisons
and keys require compatible logical metadata or an explicit conversion; no
hidden cross-unit or naive/zoned coercion is added.

### Calendar and timezone policy

Use pinned Jiff 0.2.35 with the already-resolved `jiff-tzdb` 0.1.6 rules,
through an explicit native temporal feature. Query semantics use the bundled
rules directly and report the database version; they do not consult the host
timezone, environment, network or a mutable system timezone database. Jiff and
its database have MIT/Unlicense licensing compatible with Apache-2.0. The
default lightweight build does not enable this provider. Bind parsed zone rules
once per expression under the existing memory admission, not once per row.

Calendar conversion has an explicitly narrower range than raw timestamp
transport. Values outside it fail the requested calendar operation with a
deterministic error; their native storage remains usable. Preserve the existing
full-domain naive date/timestamp arithmetic. Reject leap-second text rather
than silently folding it. Local civil times in daylight-saving gaps or folds
are rejected by default; callers can choose the documented earlier or later
resolution explicitly. Fixed duration arithmetic and calendar day/month
arithmetic are separate operations. Calendar month-end handling is explicit
(`reject` or `clamp`); a fixed 24-hour duration is never relabeled a calendar day.
These rules also govern later daily windows, resampling and gap filling.

Bind the expanded calendar operations through the existing scalar-function
registry and native expression owner. `temporal_extract(value, part)` and
`temporal_trunc(value, part[, ambiguity])` preserve explicit units and apply the
declared presentation zone. Week truncation starts on Monday; day-of-week
extraction uses ISO Monday=1 through Sunday=7. Existing date/timestamp field,
day/second-offset and difference functions retain their established names and
extend to the corresponding admitted units. Whole-second differences truncate
toward zero only after widened subtraction, preserving the existing contract.

`timestamp_assign_timezone(value, zone[, ambiguity])` resolves a naive civil
value; `timestamp_convert_timezone(value, zone)` preserves an existing instant;
`timestamp_remove_timezone(value)` preserves its local civil fields. Fixed
`temporal_add_duration`/`temporal_sub_duration` and `timestamp_difference` keep
their typed units and reject precision loss or overflow. `calendar_add_days`
and `calendar_add_months` operate on dates or local timestamp fields with
explicit optional ambiguity/month-end policies. Defaults are `reject`;
month-end `clamp` and ambiguity `earlier`/`later` are explicit literal choices.
A zero calendar offset preserves the input identity, including a known fold
instant. SQL and Python declarations lower to these same bound kernels; they
do not perform calendar evaluation in their frontends.

### Vortex-first decision and boundaries

The pinned Vortex 0.90.0 provider already defines Date, Time and Timestamp
extension metadata, primitive storage, validity, scalar/statistics ordering and
Arrow-compatible temporal types. Use those native providers and preserve their
metadata through the existing native array owners. Expand the existing vendored
timestamp scalar correction to avoid calendar-range panics for every admitted
unit and timezone; this is a provider correction, not a second array system.
Vortex's current extension inventory has no fixed-duration type. Implement a
versioned ShardLoom duration extension over the same native signed-64-bit
storage and extension registry, with explicit Arrow duration translation.
Calendar intervals are distinct from fixed durations and require their own
admitted representation; do not smuggle months into a duration tick count.

Classification: `use_vortex_native_provider` for the existing temporal dtypes,
and `implement_shardloom_kernel` for ShardLoom conversion/calendar policy and
the absent duration representation. Provider recognition, source fidelity,
representation transitions and no-fallback evidence remain in their existing
certificates. Arrow stays an explicit import/export boundary. No external
query engine, additional execution evaluator, answer cache or scheduling pool
is introduced. Explain and capability discovery remain side-effect-free.

### Acceptance obligations

Exercise every admitted unit and timezone through public input, expressions,
selection/order/group/join, native persistence and compatibility readback.
Cover full storage endpoints, pre-epoch fractions, precision loss and overflow,
unknown zones, 23/25-hour days, half-hour transitions, gap/fold choices,
month/year/leap boundaries, invalid time-of-day/date64 values, nullable nested
parents, malformed metadata, pressure/cancellation and output cleanup.
Pin independent expected values; round-tripping through the same conversion
is insufficient. The timezone-aware daily workflow must cross both daylight-
saving transitions. Record installed-package and required workspace/native
checks before any support claim. Preserve existing 347-case observations;
rerun a complete cohort only for a justified shared-runtime regression check.

The [temporal acceptance](../benchmarks/native-temporal-fidelity-2026-10-27.md)
passes 6,301 native workspace tests (35 existing ignores), 3,274 default tests,
89 DateTimeParts provider tests, strict lint and formatting. The rebuilt public
matrix passes 22 tests without skips across all five interfaces, both daylight-
saving transitions, Vortex/Parquet/Arrow-file inputs, full-domain nested readback,
declared Python rows/batches, ADBC parameters, whitespace casts and failed-output
cleanup. Installed-wheel acceptance passes 199 tests without skips. Earlier
unsuccessful observations remain in the compact packet; portable verification
and 24 corruption controls pass from an isolated source-only copy. PR #1616
merges at `51cf54fd` after all 57 hosted checks pass. This closes the temporal-
fidelity work unit, not item 20 or its later as-of/time-series obligations.

Provider references: the pinned local Vortex datetime extension sources,
[Jiff timezone conversion and ambiguity semantics](https://docs.rs/jiff/0.2.35/jiff/tz/struct.TimeZone.html),
and the pinned `jiff-tzdb` crate's data-version and licensing records.

## Acceptance and retention

The 347 existing cases are discovery/regression data. Fresh seeds, layouts,
null/type combinations and composed requests supply held-out acceptance. Reuse
the retained Polars observations without rerunning that engine. Freeze each
performance candidate's native baseline, exact source/build, workload and
retention rule before measuring it. The October 28 maintainer rule ships a lower
complete-cohort engine total and drops a higher or unchanged total after
correctness and resource checks pass, with no percentage floor. Preserve family
and case observations and explicitly assess regression flags; no aggregate suite
total establishes benefit for every case. Keep historical protocols and their
original outcomes immutable, recording changed dispositions separately. The
dropped item-15 implementation is not restored unchanged.

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
