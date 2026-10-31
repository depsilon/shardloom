# Grouped-cost attribution: diagnostic contract

Status: the single diagnostic is complete inside item 20. All 48 cases and
1,920 complete result rows pass. The [attribution report](../benchmarks/native-grouped-cost-2026-10-31.md)
records 45.20% source intake, 23.51% key preparation/interning and 9.41% typed
updates in this sample. Proceed to the bounded state-access candidate; defer
general worker routing for the current workload. This decision does not
extrapolate those fractions to all grouped queries or advance the baseline.

The original pre-observation design and executed sources remain in the packet.
The accepted 347-query baseline remains 12.055132613 engine seconds. Grouped
queries contribute 5.389164698 seconds (44.7%); that total does not identify
the cost of updating aggregate state. No new optimization is admitted here.
PR #1629 retains the rejected experiment while this work uses its unchanged
accepted engine source on `codex/grouped-cost-attribution`.

## Question and bounded observation

Separate demanded-source intake, group-key preparation, group interning, state
reservation, measure preparation and typed/generic updates in the current
relational owner. Inspect the optimized state-access/count paths alongside that
evidence before selecting contiguous-state access, proof consumption or
complete-group worker routing. Existing event counts do not establish elapsed
cost. The older specialized `aggregate_timing` owner does not time this general
relational aggregation path.

Use 48 already declared direct-argument cases: signed_value, optional_value,
metric and all_null; group_key, category and their pair; SUM-only and mixed
SUM/AVG/MIN/MAX/COUNT; all rows and the original one-percent predicate. Each
case executes once, in its original corpus order, over the resident original
Parquet fact file with the existing independent reference and complete result.
No input regeneration, cache manipulation, artificial pacing, external engine
or routine repeat is authorized by this diagnostic.

These cases are representative attribution controls, not complete coverage of
the 180 grouped cases or a new performance baseline. Computed/coalesced
arguments, other aggregates, high-cardinality state and spilling retain separate
coverage obligations. A local relational test constructs the equivalent
scan/filter/aggregate/sort declarations. It does not time SQL parsing or claim
to be a new public SQL benchmark.

## Measurement boundary

Instrumentation exists only under `cfg(test)`. The manual diagnostic runs the
release-profile test executable, with debug assertions and per-row debug work
counters disabled. The ordinary shipping library has no new timers, counters,
feature, execution strategy or public report fields.

Use a fixed-size caller-thread scope stack and coarse batch/measure boundaries;
there are no per-row clock reads. Nested scopes pause their parent so recorded
phase intervals do not overlap. The recorder is inactive unless explicitly
started by the diagnostic. Its session and guards cannot move between threads.
Unit tests verify exact synthetic-clock accounting, nesting, idle behavior and
cleanup. Timing can perturb a diagnostic: retain scope counts and do not infer
unobserved production speedups from its fractions.

Record these disjoint portions of engine execution:

- Source intake: the existing local reader open/next/close and native conversion
  within `Execution::next_input`; provider progress and waits are included.
- Group preparation: bound native key views and key-cache preparation inside
  grouped `RowSet::intern_batch`/`ingest`.
- Group interning: ordinal allocation, hash/equality/index work and retained
  unique-key publication, excluding separately timed key preparation.
- State reservation: existing compact/generic group-state admission and growth.
- Measure preparation: bound measure columns, validity and numeric/key views.
- Typed updates: existing paired-extrema, completed-result reuse, proof checks
  and typed update traversal. This is an upper bound on work worker routing
  could remove; it includes serial admission and coordination-free work.
- Other updates: the ordinary noncompact measure branches.
- Aggregate remainder: cancellation, measure-filter selection and other consume
  work outside the named child scopes.
- Finalization: group gather and native result construction, excluding the
  synchronous downstream callback. Downstream sorting and other engine work
  remain in the unclassified engine remainder; serialization stays outside the
  engine clock according to the existing `QueryTiming` contract.

Use the existing engine clock for the denominator. The classified engine
intervals must not exceed it; preserve the unclassified remainder. Do not use
test-process duration, preparation, serialization or a sum of worker CPU times
as an engine substitute. Report missing/invalid attribution explicitly.

## Provider and correctness contracts

Vortex-first decision: `wrap_vortex_concept`. Observe the existing native array,
prepared-key and source boundaries around Vortex 0.90.0; no provider or kernel
is replaced. The current Parquet translation/demand path and native Vortex
execution/ownership remain intact. The test-only recorder is not an execution
provider, plan node or generic observability framework.

Retain complete schemas and values, original source generation/hashes, native
certification, explicit no-fallback status and allocation cleanup. Every case
must activate group interning and aggregate consumption. Bind 2 GiB and maximum
parallelism two explicitly; do not create a worker pool. Refuse a changed source,
incomplete input, unexpected provider or unsupported diagnostic declaration.

Freeze case/source/reference identities, the test executable, build/source
identity and runner before observation. Keep full hashing and recursive
inventories outside active query execution. Use the standard lightweight
workload/storage guard and bounded process-group supervision. Keep other task
work, agents, builds and packaging idle during the observation.

## Decision after observation

An attribution result can justify a next candidate or an evidence-based stop;
it cannot retain a performance change by itself. Evaluate the available work
against coordination, key preparation and source costs. Small/cheap/skewed
workloads must keep serial admission if workers cannot repay their overhead.
Any actual optimization needs its own frozen semantic/resource contract and
one complete unchanged 347-case gain/loss observation. Preserve this diagnostic
separately. Full43 remains held until item 18, and CG-1 through CG-23 remain open.
