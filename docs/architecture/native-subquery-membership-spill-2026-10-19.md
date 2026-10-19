# Native subquery predicate state under memory pressure

Status: implemented and locally accepted for the predicate scope below. The
[acceptance report](../benchmarks/native-subquery-membership-spill-2026-10-19.md)
retains complete values, source identities and failed observations. Hosted,
installed-package, other-platform and competitive acceptance remain separate.
This finite RFC 0044
unit extends PERF-06.membership/recovery/strategy and CPU package E, with
PERF-03 ownership and PERF-07/11/12 publication and workflow obligations. It
follows the sampling acceptance at `2da183da`. The 14 roadmap owners, 58
substeps and CG-1 through CG-23 retain their independent completion gates.
CG-5/14/16/19/20/21 apply here. No release or version change is included.

## Scope and observation contract

Extend the shared relational non-scalar `IN`, `EXISTS` and quantified `ANY`/`ALL`
predicates, including negation and all admitted comparisons, when explicit
native query spill policy is present on the existing `vortex-write` Unix
boundary. Preserve ordinary uncorrelated, statically indexed correlation,
parameterized and declared deferred-schema inner execution. Static correlation
builds one indexed inner relation; parameterized execution builds a fresh inner
relation for each selected outer row. Do not equate those two strategies.

The no-spill route retains its existing resident state and deterministic grant
denial. Scalar subqueries retain their zero/one-row state, lazy selection and
more-than-one-row error. This contract does not admit dynamic scalar inference,
opaque deferred streaming sources, new source identities, runtime fallback,
general query resume, or an arbitrary collected result larger than its grant.

Preserve eager build for ordinary predicate subqueries and first-selected-row
build for guarded predicates. An unused guarded branch does not evaluate its
inner keys; existing raw-source completion/validation remains separate. For a
selected branch, validate every inner key before skipping a null correlation
prefix or using a predicate short circuit. Validate every selected outer key
before empty-group handling. Keep null parent/child semantics, exact mixed
numeric equality, signed-zero normalization, typed/nested keys and collision
comparison through the existing native key owners. Unselected scalar payloads
and unreferenced fields do not become newly evaluated predicate keys.

Preserve three-valued row equality: a false component dominates an unknown
component. Complete `IN` keys first probe exact candidates; a miss needs only
nullable candidates in the same exact correlation group. Nullable left `IN`
keys and quantified comparisons reduce the full matching group. Empty `ALL`
is true; other empty predicates are false; negate only the final nullable
result. Outer output order and inactive-guard values remain unchanged.

## Native representation and reuse

Vortex-first classification: `implement_shardloom_kernel` for predicate
semantics and index access. Pinned Vortex 0.85 arrays, DTypes, native validity,
projection, file writer and bounded reader are existing admitted providers;
they do not supply ShardLoom's correlation, SQL three-valued reduction,
shared-grant accounting or owned recovery policy. Reuse native `Batch` key
hash/equality/comparison, private typed records, `Ordering`/`StoredOrder` and
`QueryRunStore`. No new dependency, file format, generic spool, source adapter,
external engine or Arrow execution substrate is introduced.

Private records contain correlation hash, nullable complete-key hash, full
source ordinal and the required native key fields. User names live inside a
private payload struct. An uncorrelated `EXISTS` record needs only cardinality.
Validate all keys, then omit null correlation prefixes. Sort by correlation
hash, complete-key hash and ordinal; compare full keys after hash lookup.
Keep only the keys required by the predicate. Its private record construction
detaches retained keys directly; do not first duplicate the full streamed row.
Keep existing input batch/row detachment counters for batches accepted by this
private owner, without implying unreferenced columns were copied. Resident and
scalar subqueries keep full-row detachment. Retain complete ordinals without
a fixed row cap, and keep one bounded lookup block with lifetime-bound key
views. Drop that block before loading its replacement. Validate the run
generation on cache hits as well as reads and finalization.

The existing sorter chooses resident buffering or native runs within the
caller-authorized memory and storage policy. The shared run store owns quota,
merge overlap, reader credits, native schema/length validation and exclusive
cleanup. A successful parameterized probe releases its completed inner run
before the next selected outer row. Errors and cancellation release the same
owned state; abandoned-process recovery removes only proven owned runs and
permits a fresh restart, without claiming query resume.

Source replay, an inner pivot/order and predicate membership remain separate
owners even when they share the store. Record predicate strategy activations,
inner and indexed rows, selected probes, exact candidate visits and lookup
blocks separately from actual disk activity. Small projected key sets may
remain resident under the spill-capable strategy. Never infer membership spill
from source replay or an inner operator's run count. The separate
`relational_spilled_subquery_stages` field counts completed predicate populations
backed by actual native runs. Public SQL/Python `outer` references currently lower
to parameterized execution, while directly bound native plans can use one indexed
correlation population. Tests must expect the corresponding build/replay counts;
small per-parameter populations may stay resident while source replay spills.

Source replay records each input before invoking its consumer, so its flush and
merge work must coexist with the consumer's retained state. Under explicit spill
policy, use 64-row source-replay and predicate blocks. Cap both retained sort
thresholds at one eighth of the shared grant, never above the authorized spill
buffer threshold. The input, retained consumer keys, merge readers, selected
blocks and writer payloads coexist. The pinned Vortex push writer can retain
previously accepted output while the next block is gathered; its available API
does not expose a separate flush/drain boundary. These block and buffer bounds
reduce that measured overlap without changing the source, grant, record-size
admission, writer or memory credits. Actual reservations remain authoritative;
arbitrary record widths are not guaranteed to fit. Resident replay keeps its
existing bound. Other order consumers and output fanout keep their existing
thresholds and caller-selected output bounds.

The shared merge cursor also defers the next block read until another winner is
needed. Advancing the final selected row of a full output block must release the
cursor's reference rather than prefetch another payload before gathering that
output. Selected-row owners retain the consumed block for the gather; the next
step loads any pending head and keeps stable order, cancellation and both reader
generation checks. Direct private-key detachment avoids a redundant full-row
copy during this work, while producer-release witnesses enforce its ownership
boundary. The original 4-MiB parameterized fixture must complete unchanged;
adjacent replay, ordering and output-fanout regressions remain mandatory.
Development allocation traces and failed attempts are retained with acceptance
evidence rather than treated as successful pressure observations.

ShardLoom technique review: project required keys before retaining state,
preserve native encoded key owners where available, and use bounded record
and merge work units with shared dynamic admission. Reuse the existing native
run geometry rather than introduce a topology or provider cache. PulseWeave
does not alter predicate truth, guard demand or validation order. Actual work
and representation transitions remain in certificates; routine timing reports
use the engine clock. `fallback_attempted=false` and
`external_engine_invoked=false` remain mandatory.

## Verification and acceptance

The expert comparator is an encoded-columnar runtime maintainer reviewing
observation semantics, exact keys, bounded overlap and failure ownership.
This is required capability work, independent of a throughput threshold.

- Preserve the original 8,193-row wide-input test at 4 MiB and public mirror at
  8 MiB. No-spill still denies before opening the eager outer source; explicit
  spill-capable execution must complete. Because projecting a narrow key can
  avoid the original retention entirely, add wide-key fixtures at the same
  grants that require native runs and merges, including a hot correlation
  group, nullable keys and all predicate families.
- Check complete ordered outputs against independent three-valued oracles and
  the resident route across comparisons, negation, multi-column keys, duplicate
  and colliding candidates, empty/all-null inputs, mixed exact numerics,
  dictionary, binary, decimal, temporal and admitted nested keys.
- Preserve guarded demand, late invalid-key rejection, scalar cardinality,
  parameterized fresh state, dynamic schema/source checks and separate replay
  accounting. Verify repeated executions and surviving result ownership.
  Deferred public pivot/replay compositions retain their established 32-MiB
  grant; the wide predicate fixtures use 8 MiB. Both the prior sampling binary
  and this implementation deny the deferred fixture at 8 MiB and complete it
  at 32 MiB. This separate inner-owner requirement is not a predicate-pressure
  result, and the failed 8-MiB development attempt remains in the evidence.
- Exercise quota/grant denial, producer/schema/consumer failure, cancellation,
  corrupt/truncated/replaced runs, cached hits and merge boundaries. Include
  real process abandonment, live/unknown-owner preservation and fresh restart;
  every failure remains incomplete with refunded reservations and owned cleanup.
- Verify SQL/Python/direct binding convergence, retained collect, all eight
  write/reopen destinations, fanout and preservation of preexisting targets.
- Run focused semantics, original pressure and lifecycle checks, native and
  CLI suites, required formatting, strict workspace lint and all-target tests.
  At the cohesive acceptance boundary freeze source/build identities for the
  guarded full public matrix, complete Full43 observation and modular workflows.

Principal risks are changed guard or invalid-key observation, hash collisions
treated as equality, incorrect nullable multi-column reduction, cached reads
bypassing generation validation, parameterized runs accumulating, and wide-key
merge overlap exceeding the original grant. Resolve these before advertising
the scoped capability. Broader recovery, other resident families and competitive
acceptance remain separate obligations.

## Finite local acceptance

The original 8,193-row native pressure controls pass at 4 MiB; public wide-key
controls pass at 8 MiB. Additional wide-key fixtures require actual predicate
runs and merges. Deferred public pivot/replay compositions retain their separate
32-MiB grant, and independent direct native-file readback uses 128 MiB.
All 5,930 public cases across 39 families pass. Independent verification reopens
483 subquery value archives and all 1,701 rows, with 24 public failure/preservation
cases followed by fresh successful executions. Fourteen required native
selectors, all 60 subquery regressions, all eight writer formats, direct binding,
complete Full43 results and 18 modular cohorts pass.

Source and compiled artifacts are frozen at `a6108842`; all 2,208 source entries
and five verification helpers pass identity checks. The documentation closeout
bridge permits only the scoped machine-reference claim validator to change.
Runtime, Python and harness sources remain byte-identical. The report preserves
the pressure, lowering-expectation, fixture-grant, diagnostic and verifier
corrections without treating their failed attempts as accepted observations.
No broader PERF owner, release or competitive gate closes with this finite scope.
