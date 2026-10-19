# Native sampling populations under memory pressure

Status: implemented and locally accepted for the six variants below. The
[acceptance report](../benchmarks/native-sampling-population-spill-2026-10-19.md)
retains complete values, source identities and failed observations. Hosted,
installed-package, other-platform and competitive acceptance remain separate.
This finite RFC 0044
unit extends PERF-06.membership/recovery/strategy and CPU package E, with
PERF-03 ownership and PERF-07/11/12 output and workflow obligations. It follows
the locally accepted unary membership unit at `a2728ac5`. The 14 roadmap owners,
58 substeps and CG-1 through CG-23 retain their separate completion gates.
CG-5/14/16/19/20/21 apply here. No release or version change is included.

## Scope and observation contract

Admit the shared relational `BoundUnary` sampling population only when explicit
native spill policy is present, on the existing `vortex-write` Unix boundary:

- Fractional sampling without replacement when the input row upper bound is
  unknown, for weighted and unweighted inputs.
- Sampling with replacement, fixed count or fraction, weighted or unweighted,
  with either known or unknown input bounds.

These branches already retain every passing payload in the resident algorithm.
Fixed-count and known-bound fractional sampling without replacement keep the
existing bounded top-k path. Changing those branches to retain every payload
would change which invalid payloads are observed. Legacy `PreparedVortexUnary`
and the older local row-export sampler remain resident. Source replay and the
consuming sample population remain separate owners. With spill disabled, keep
the existing shared-grant denial. A granted workspace does not promise that any
individual record or public collected result fits the memory grant.

Run the producer once. Evaluate predicates before assigning a zero-based passing
ordinal or reading the weight. Observe each admitted payload at the same point
as the resident algorithm. Weights remain positive finite numeric values, and
replacement cumulative weights use the same sequential floating addition with
the same finite-total check before payload observation. The default seed and
all existing deterministic mixing, weighted scoring, draw and target helpers
remain unchanged. In particular, fractional target sizing uses the existing
per-row carry algorithm and its nonempty minimum; it is not replaced by a
floating multiplication and ceiling.

Without replacement, rank by score descending and passing ordinal ascending,
select the actual-population target, then return source-ordinal order. Positive
tiny weights can produce negative-infinity scores. Private ordering represents
the exact score order as unsigned bits, normalizing signed zero ties, so the
native sort key never rejects an otherwise valid weighted score. This encoding
does not alter the weighted formula. With replacement, unweighted draws retain
the exact deterministic modulo-index algorithm. Weighted draws select the first
cumulative weight greater than or equal to the exact threshold, clamped to the
last row. Equal cumulative sums from rounding must keep first-match semantics.
Replacement output follows draw order and retains repeated rows.

## Provider, representation and ownership

Vortex-first classification: `implement_shardloom_kernel` for the existing
sample semantics and population access, reusing admitted pinned Vortex 0.85
array, DType, validity, native file writer and positional scan providers.
The current native query-run store and typed relational `Ordering`/`StoredOrder`
already supply credited records, immutable generations, aligned block access,
quota and cleanup. Vortex arrays/files do not supply ShardLoom's seeded sample
contract, shared-grant policy or completion certificate. No new dependency,
physical format, general spool API or external execution provider is needed.
`fallback_attempted=false` and `external_engine_invoked=false` remain mandatory.

Build bounded private Vortex struct records with original payload DTypes and
hidden passing ordinal, score and cumulative-weight bits. Retain at most 256
pending rows, or the smaller requested output batch bound, and a 256-KiB shaping
threshold. All pending capacity, payloads, metadata, sort state, native I/O,
lookup blocks and output credits belong to the same execution pool. Flush
pending rows before native merge work; thresholds shape batches rather than
impose a maximum input or record size. Preserve scalar floating payload bits
through private movement and keep existing eager nested-row validation. Final
selected public values retain the normal value checks.

The existing ordering provider chooses resident state or native runs within
authorized policy. Fractional nonreplacement orders by score/ordinal, selects
the target after producer completion, then uses native ordering to restore
source order. Replacement retains ordinal order and uses aligned positional
blocks for exact draw access. Weighted lookup binary-searches monotonic stored
cumulative sums. A single charged block cache must validate the stored generation
on every access, including cache hits. Compact selected payloads before releasing
borrowed blocks. Finish with no retained run reader; public retained outputs keep
their independent credits. Cancellation, quotas, schema/generation failure and
cleanup use the shared native store. Abandoned-file recovery permits cleanup
and fresh restart, not interrupted-execution resume.

For this strategy the legacy unary state-item count is the logical retained
population, including native files; population retention is true for a nonempty
population. It is not a count of resident heap entries. Dedicated counters report
sampling stages, input and passing population rows, output rows, replacement
draws and lookup blocks. Actual run/merge/disk counters and shared reservations
remain separate evidence. The broad `unary_state_spill` all-family flag stays
false; `unary_sampling_population_spill` now records the locally accepted finite
scope with explicit operation/bound restrictions. The machine index links this
contract and its acceptance report, with hosted acceptance still pending.

## Verification and acceptance

The expert comparator is a native encoded-columnar maintainer reviewing exact
observation semantics, floating boundaries, bounded overlap and failure ownership.
This is required capability work; no throughput or competitive superiority claim
is attached to its completion.

- Complete both original 8,193-wide-row sampling pressure fixtures at their
  unchanged 16-MiB operator grant with permission. Preserve the no-spill denials
  and successful bounded fixed-count nonreplacement control. Add weighted and
  fractional replacement pressure with complete independent expected values.
- Compare all admitted variants against the resident path and independent
  ranking/draw oracles across seeds, bounds, chunk sizes, empty/singleton/null,
  duplicate payloads, hidden weight columns, predicates, fractional carry edges,
  large requested draw counts, tiny weights, rounded cumulative ties and overflow.
- Preserve complete typed/nested/decimal/binary/temporal payloads, signed zero,
  selected and unselected nonfinite scalar payload behavior, and large records.
- Cover producer/consumer/schema failure, cancellation, corrupt/truncated/replaced
  runs, cached hits, merge boundaries, quota/admission denial, actual process
  abandonment, owned recovery and fresh restart with zero leaked reservations.
- Verify Python/SQL/direct binding convergence, one producer pass, replay
  separation, retained outputs, all eight write/reopen destinations and fanout,
  including preservation of preexisting targets and unknown files.
- Run focused sampling, membership, ordering and provider regressions, native
  and CLI suites, mandatory formatting, strict workspace lint and all-target tests.
  At the coherent acceptance boundary bind source/build identities to a guarded
  full public matrix, complete Full43 engine observation and format workflows.

The principal risks are premature payload validation, changed floating/tie
semantics, a cache bypassing generation checks, uncredited retained ownership,
and finalization overlap that cannot fit the original pressure grant. Fix those
before advertising this scoped capability. Broader sampling strategies and
recovery families stay open. Finite non-scalar subquery predicate spill has a
separate [accepted contract](native-subquery-membership-spill-2026-10-19.md)
and [local acceptance report](../benchmarks/native-subquery-membership-spill-2026-10-19.md);
broader subquery modes and the remaining competitive gates stay open.

## Local acceptance

Source and compiled artifacts are frozen at `f363f289`. All 18 new native
selectors and 17 unary-membership regression selectors pass in the 3,010-pass
native suite. The original 16-MiB population pressure grant is unchanged;
direct-binding and wide publication workflows use 32 MiB, with a separate
128-MiB native-file readback grant. All 5,492 public cases across 38 families pass.
Independent inspection reopens 406 sampling value archives and all 11,518 rows.
The 66 failure/preservation checks distinguish 54 incomplete-execution faults
from 12 existing-destination refusals before producer demand.

Full43 passes all 43 queries once and all 381 result rows and schemas. Its engine
time is 50.415902622 seconds. All 18 modular cohorts match 704 native workflows
to 704 fresh independent oracle workflows. These are capability and regression
observations, with no speed claim. The report records broad checks, existing
skips, actual process-death cleanup/fresh restart, exact source binding and
portable evidence. No broader PERF-06 substep or competitive gate is closed.
