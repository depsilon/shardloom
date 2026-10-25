<!-- SPDX-License-Identifier: Apache-2.0 -->

# Warm execution and reusable native source state

Status: implemented, measured, rejected and removed on October 25. The finite
pre-ingestion investigation is complete. Both the 93-call baseline and the
93-call candidate pass complete independent results and exact schemas. Only
one of four physical families passes the frozen benefit gate, and one first-use
control fails. No experimental cache, provider hook, reclamation API,
diagnostic-field change or candidate test remains in the runtime.

The [portable packet](../benchmarks/evidence/native-warm-source-reuse-2026-10-25/manifest.json)
preserves both cohorts, the earlier 129-call characterization, validation
failures and repairs, the final candidate patch, exact removal receipt and
build/input identities. Its
[replay tool](../benchmarks/evidence/native-warm-source-reuse-2026-10-25/verify_packet.py)
checks saved values and scores without executing a query or archived code.
Public naming is next. The existing ingestion ownership package owns the
remaining allocation-provenance prerequisite; broader source reuse is not
declared complete. CG-1 through CG-23 remain independent.

## Final finite decision

The candidate uses the accepted Vortex 0.90.0 provider and the same
99,997,497-row input generation. Each revision runs 49 isolated process groups:
three fresh-process samples per selected query, three same-plan calls, three
equivalent-new-plan calls and four related-query triples. Queries are Q1,
Q19, Q17, Q23, Q28, Q29, Q34, Q35 and Q20. All 93 calls complete fresh native
execution and export all result rows. Every group closes with zero reserved
memory. Other task-owned jobs remain idle during execution. Input hashing
precedes queries; OS cache is uncontrolled and no cache reset occurs.

Before candidate execution, the protocol freezes physical families, equal
query weighting, medians, controls and attribution. Warm samples pool calls
two and three from same-plan and equivalent-new-plan groups. Each query's
reduction is `1 - candidate_median / baseline_median`; a family uses the median
of its query reductions. Related samples use calls two and three of the related
groups. Acceptance requires at least 5% benefit in two distinct nonmetadata
families, including related-query benefit in one of those same families, with
observed source-work reduction.

These percentages describe the frozen **complete-call diagnostic** gate.
They are not engine times, Full43 scores or statistical-significance claims.
Positive values mean a reduction; negative values mean an increase.

| Physical family | Warm reduction | Related-query reduction | Warm gate |
| --- | ---: | ---: | --- |
| String heavy-hitter/top-k, Q19/Q23/Q34/Q35 | 2.06% | 8.57% | Fail |
| Numeric/UTF-8 heavy-hitter/top-k, Q17 | -3.15% | 1.61% | Fail |
| Transformed dictionary aggregate, Q28/Q29 | -2.11% | 2.06% | Fail |
| Selective integer projection, Q20 | 14.30% | 13.16% | Pass |

Controls allow an increase of at most the larger of 10% and 100 microseconds.
Q28's fresh-process median increases 18.52% and fails; the other 18 controls
pass. This fails the predeclared criterion without establishing the cause of
variation. No observations are discarded, gates relaxed or unchanged candidate
rerun.

Equivalent-new-plan source opens fall from 27 to 9 and related-query opens
from 12 to 4. Same-plan and fresh-process counts remain 9 and 27 respectively.
Total opens fall from 75 to 49 while both revisions complete 93 executions.
The candidate records **zero dictionary insertions and zero dictionary hits**
in this workload. Fixture tests prove eligible numeric-domain reuse, not its
activation in these measured queries. Baseline dictionary counters were absent
and remain unavailable, not zero.

**Reject and remove the candidate.** Correct answers and fewer opens do not
override failed benefit/control gates. Broader variable-width retention is
deferred until complete backing-allocation and metadata ownership can be
proved in the ingestion package. It is not an accepted cache waiting to be
enabled. The agreed stopping rule closes this finite implementation attempt
and preserves the next public-naming item before ingestion.

## Verification and restoration

The final candidate passes strict native workspace linting, a matched release
CLI/library build, 107 focused resident-session tests (seven existing opt-in
tests ignored), all 53 targeted cases that failed the earlier broad run, and
142 installed-wheel integration tests without skips. Coverage includes changed
generation/schema/dictionary, pressure/shrink, churn, cancellation, incomplete
publication, concurrent consumers, independent statistics, optional admission
refusal and surviving results. License/provenance checks cover all three
changed vendor crates. Earlier compile, lint and broad-test failures remain
recorded. The final candidate does not rerun the full native suite or advance
to public-workflow, fresh-ingest or Full43 acceptance after its failed gates.
Focused correctness is not broad acceptance.

The compact patch reconstructs all 63 changed/added files against
`82d9732eb7652671a01b0f223cf62c63f01ffe47`; every reconstructed hash is checked
through a private Git index. Removal restores 58 tracked implementation, test,
vendor and license files byte-for-byte and removes exactly four added source
files. This design document remains for the final update. The restored runtime
is identical to merged provider PR #1601 at
`3c3e2f14d8fe1b517fa53472a544d76452f6395a`. Unrelated user work is untouched.

After restoration, `cargo fmt --all -- --check`,
`cargo clippy --offline --locked --workspace --all-targets -- -D warnings`
and `cargo test --offline --locked --workspace --all-targets` all pass.
The default suite reports 3,246 passed, zero failed and zero ignored. Source
identities remain unchanged through these checks. The package-license check
also passes against the restored vendor tree. Portable replay checks all 315
saved executions and 3,285 complete rows, 19 validation receipts, exact source
restoration and ten deliberately corrupted evidence cases. It does not rerun
query execution or reinterpret the candidate's failed acceptance decision.

## Earlier accepted-runtime characterization

Before implementation, the separately requested adjacent-triple profile runs
43 queries three times on the accepted provider runtime: one fresh session per
query and three newly constructed plans. All 129 executions, 1,143 result rows
and schemas pass; all 43 sessions release memory. It records 129 source opens,
so it does not demonstrate cross-plan source reuse.

The engine totals are **50.525565878 seconds** for first-use calls and
**46.299759122 seconds** for the selected later calls. Selection follows the
frozen client-response rule: select the faster second or third call per query
and take the engine time from that same call. This is a sum of selected calls,
not one contiguous run, a true-cold result or a candidate speedup. All clocks
and all three observations remain together. These earlier numbers must not
be attributed to the rejected implementation. The accepted ordinary single-pass
Full43 remains **49.625514041 engine seconds**. No new whole-engine performance
or competitive claim follows.

## Original handoff and measured candidate design

The remaining sections preserve the accepted scope and the design that was
actually implemented and measured. Present-tense candidate descriptions below
describe that archived implementation, not functionality remaining in the tree.

Original status: accepted October 24 maintainer handoff, queued after
[user-surface native execution parity](native-user-surface-execution-parity-2026-10-24.md)
and the subsequent [controlled provider refresh](native-vortex-provider-refresh-2026-10-24.md),
before [full-ingest ownership/preparation](native-ingest-ownership-preparation-2026-10-24.md),
then [connected programming](native-connected-programming-model-2026-10-24.md),
[universal I/O](native-universal-io-performance-coverage-2026-10-24.md)
and [rich-data lifecycle support](native-rich-data-lifecycle-2026-10-24.md).
The subsequent [public-naming pass](shardloom-public-naming-2026-10-24.md)
also precedes ingestion and uses this package's settled lifecycle terminology.
This is source review and a finite implementation/measurement scope, not an
implemented cache or a measured warm Full43 result. The review uses #1595 at
`bc2d1b6a`, whose runtime is unchanged from the accepted grouped campaign.
RFC 0044 and the existing PERF-02 preparation, PERF-03 resource, PERF-06
ownership and PERF-09 evidence contracts apply. CG-1 through CG-23 remain
independent gates; this does not create another engine mode or phase ID.

## Size and ordering decision

The later provider-refresh instruction takes precedence over the original
immediate-after-parity placement. Recheck the source-owner design against the
admitted 0.90 scan provider before adding infrastructure. V1 can reuse readers
and dictionary state through a retained reader tree; production file execution
also creates operation-scoped source wrappers that reset that tree. Trace the
selected path before claiming cross-operation reuse. Retained V2 scan state is
an additional integration seam. The refresh owns version
compatibility; this package owns cross-plan source lifetime and fresh execution.

| Work | Relative scope | Queue decision |
| --- | --- | --- |
| Exercise existing retained plans, readers and complete executions | Small, bounded lifecycle/harness work; overlaps parity's resource/report checks | Reuse parity evidence, then characterize the three call patterns below |
| Share compatible file-source ownership across independent plans where reconstruction is demonstrated | Medium native ownership change, with generation, resource and close/release tests | First implementation candidate before ingestion; extend the shared owner beneath interfaces |
| Retain selected dictionaries, encoded chunks or decoded source blocks across calls | Medium to large; admission, invalidation, pressure and surviving-owner behavior are part of the mechanism | Conditional second candidate after attribution; not presumed smaller than ingestion |

The bounded first package belongs before ingestion because it can exercise
existing ownership without changing file production or proving adoption of
opaque reader allocations. Its implementation size is still conditional on
the source trace. If the useful candidate needs the same unproved allocation
provenance as full ingestion, record that dependency and move it behind the
ingestion ownership work. Do not let a speculative general cache hold the
rest of the queue indefinitely. No benefit percentage or delivery duration is
established by this review.

## What already exists, and what remains unproved

- [`run_clickbench_query_uat.py`](../../scripts/run_clickbench_query_uat.py)
  launches `run_profiled_command` for every query/run. Its recorded cache policy
  is `new_process_per_run_os_page_cache_uncontrolled_no_answer_cache`. Increasing
  repetitions does not retain the process's native source or plan owners.
- [`PlanCore::with_prepared`](../../shardloom-python/src/plan.rs) retains one
  eligible prepared operation per plan when resources and reuse requirements
  match. Each `collect` still calls `execute_native` or its input-aware variant.
  A new plan starts with no prepared entry. Releasing preparation drops that
  entry while separately held results remain valid.
- [`NativePlanState`](../../python/src/shardloom/_native_backend.py) belongs to
  an individual query/workflow. Its SQL key checks the backend, statement, source
  declarations and spill declaration; it is not a context-wide equivalent-query
  cache. The [Python binding](../../shardloom-python/src/session.rs) clones
  explicitly supplied native memory sources into separate plans. That does not
  prove automatic file-reader reuse across independently constructed queries.
- [`EmbeddedExecutionOwner`](../../shardloom-cli/src/native_runtime_owner.rs)
  retains one configurable controller. Its fields hold resource policy and
  runtime initialization, not a map of prepared file sources. The inspected
  Python/session path therefore establishes retained runtime and plan-local
  preparation, not general context-wide file-source reuse. Check the complete
  selected path before treating any particular reopen as a demonstrated defect.
- [`PreparedVortexSource`](../../shardloom-vortex/src/resident_session.rs)
  shares an owner containing the Vortex file, validated generation and runtime;
  file preparation enables the pinned layout-reader cache.
  [`NativeExecutionContext::file_view`](../../shardloom-vortex/src/resident_execution_context.rs)
  retains its parsed footer and held descriptor but replaces the segment source
  for each file-backed operation, resetting cached reader/plan state. In-memory
  sources retain the existing handle. A direct call to
  `prepare_file_with_cancellation` captures and opens a source again. The explicit
  compressed-segment reuse path closes its cache at the end of each admitted
  operation, including returned errors; it is not general cross-query block retention.
- [Native binding tests](../../python/tests/test_native_binding.py) and
  [prepared sampling tests](../../python/tests/test_native_prepared_sampling_binding.py)
  prove repeated same-plan execution, reuse reporting, exact output and released
  credit in their stated fixtures. Those tests do not constitute a retained-runtime
  Full43 characterization. A test named "reuse" is insufficient without observing
  the source/plan behavior it claims.
- The [October 19 physical-array experiment](native-physical-retention-2026-10-19.md)
  rejected and removed both retention candidates under their original gates.
  There is no accepted general physical-array cache from that experiment. The
  rejected codec-preparation mechanism also remains rejected.

## First package: useful ordinary context lifetime

### October 25 implementation candidate

The maintainer explicitly asks to implement and finish this package after the
first characterization; the baseline alone does not close it. The candidate
shares validated file preparation across independent plans in one native owner.
It retains the held generation, footer, parsed layout children and metadata,
without storing a prepared plan or a runtime back-pointer in the optional cache.
Eight recent file owners bound source churn. New plans may reopen a changed
generation; already prepared plans keep their existing deterministic invalidation.

The selected V1 extension retains only complete, unfiltered numeric/Boolean
dictionary value domains from the admitted native file provider. It uses an exact live layout
allocation inside one file-generation owner, retaining that allocation with the
entry to prevent address reuse. It keeps the existing native backing owners;
there is no copied serialized representation or payload revalidation. Checked
metadata construction preserves the existing value/slot invariants. Each
source's optional table has at most 64 entries. Once full, new domains are
declined until reclamation; a long sequential scan cannot replace the entire
working set before the next scan reaches it. A logical-size
hint and current reserved-memory headroom restrict retention; logical sizes do
not replace the real backing-allocation credits. The table and bounded array
metadata have their own credits and are released on pressure, including when an
active plan retains the source. Optional control reservations happen after the
required source has opened; refusal leaves the ordinary execution path available.

Every operation constructs fresh readers, expression-evaluation caches, masks,
selections, array statistics and result state. A private source template preserves
only statistics present before query evaluation. A cache hit shares payload
buffers and builds separately admitted metadata for that call; query-computed
statistics cannot reach the template or the next call. Full dictionary source
domains can be reused by different predicates and measures. Canonicalization failures, expression results,
query answers and pending reads are ineligible. A retained file has an unbound
segment source: only an admitted operation may supply a cancellable I/O owner.
Ordinary source replacement clears provider reader context; ShardLoom reattaches
it only within the validated generation boundary. V2 remains unselected.

The memory pool offers bounded weak reclamation hooks. It drops optional
references before a reservation is refused and before reducing a configurable
grant. Live consumers retain their own credits and remain valid. Reclamation
does not wait for consumers, replay a query or revoke an active owner. Session
close disables further retention and releases optional state without destroying
completed results. No new source-size limit or external execution path is added.

Vortex-first provider decision: wrap the existing `VortexFile`,
`LayoutReaderContext`, `DictReader` and native allocation owners. The existing
`with_segment_source` reset is preserved because it owns cancellation and I/O
cleanup. File/source opens and dictionary hits are observable independently of
completed executions. The candidate still requires focused lifecycle tests,
independent complete-result comparisons, the frozen benefit/control gates and
broad integration checks; implementation and counters are not acceptance or a
performance claim.

Reports distinguish actual provider opens (`resident_source_opens` and Python's
`prepared_source_opens`) from cumulative source preparations, which include
validated source-cache reuse. The existing Boolean that observed new plan
preparation is named `resident_source_preparation_performed_this_call`; it no
longer claims a physical footer open. Dictionary hits and cumulative insertions
are separate from completed query executions and the current retained-file
count. This deliberate report-field correction updates the existing consumers;
historical evidence retains its original schema and meaning.

The acceptance review excludes ALP-RD's separately reconstructed dictionary,
FSST's padded symbol/compressor state, variable-width scalar/statistics owners,
unknown encodings and mutable evaluation wrappers from this candidate. Their
allocation provenance is not established merely by a native encoding ID.
Variable-width and broader source representation retention remain conditional
on the ingestion ownership package. The candidate neither copies a serialized
cache representation nor relaxes external-input validation.

Trace and validate three distinct patterns in one ordinary context:

1. Execute the same prepared query again: keep eligible source, reader, bound
   expressions and preparation; recompute the answer.
2. Construct an equivalent query again: share compatible source state where
   available; do not assume cross-plan prepared-operation deduplication exists.
3. Execute different predicates, measures or groupings over the same source:
   share valid source state, with fresh selection, aggregate/join/sort and result
   state unless an independent existing contract explicitly permits reuse.

Record source opens, retained preparation, reconstruction/decode work and live
resource ownership using existing reports. Missing counters remain unknown.
Do not infer physical storage reads from logical bytes or OS-cache warmth.
Fix a demonstrated reopen/reconstruction below SQL, workflow, DataFrame and
typed frontends. Normal retained contexts should benefit naturally; no special
warm switch, separate frontend caches, second scheduler or alternative engine
is requested. A one-shot CLI cannot preserve private owners after process exit.

Native engine timing does not necessarily include source opening or all plan
preparation. Preserve its definition. Diagnostic complete-call latency may show
a preparation benefit that the engine field does not; keep these separate.
Reuse parity's completion, result-lifetime and reporting corrections rather
than implementing or measuring those twice.

## Conditional source-state retention

Select one expensive immutable owner that is currently produced, dropped and
rebuilt across calls. Prefer small structural metadata, dictionaries/value
domains or encoded chunks before expanded decoded blocks. Use the [controlled
0.90 provider refresh](native-vortex-provider-refresh-2026-10-24.md) as the API
baseline for any selected repeated-scan, buffer or allocator candidate; existing
repeated scans alone do not establish cross-query ownership. Extend admitted
native provider
boundaries without introducing query-engine integrations.

Keeping an already validated, already accounted owner is materially different
from copying and validating a new serialized cache representation on admission.
Prove that distinction for the actual candidate. A visible pointer, slice length
or reported capacity does not prove complete backing-allocation ownership.
Preserve required validation and external-owner safety checks.

Account active execution, caller-held results, reusable source state and required
workspace/metadata under the applicable shared grant. This is an allocation
contract, not a complete process-RSS ceiling. Avoid charging shared backing
storage twice, and keep its credits until its last live owner is released.
Reclaim idle optional references before they prevent ordinary execution;
eviction cannot invalidate active consumers. Reuse refusal follows the ordinary
native path and must not create a new source-size limit. Do not assume a
24-GiB grant provides 24 GiB of physical RAM or retain full compressed and
decoded copies by default. Use a simple measured admission policy resistant
to one-pass sequential churn; no autonomous tuning system is required.

Keys and validation must cover actual dependencies: source generation,
region/layout, field/type, encoding/dictionary identity and relevant semantics.
A pathname is insufficient. Test changed source/schema/dictionary, new
predicates/aggregates, resource shrink, cancellation, incomplete construction,
concurrent consumers and result/source survival after context close. Publication
of reusable state requires complete validation. Keep reuse inside its access
scope and preserve authorization, deterministic errors and no-fallback behavior.

## Finite characterization and acceptance

Freeze candidate, input/build identities, effective resources, lifetime,
comparison schedule, result contract and benefit/control gates before scoring.
Extend existing embedded acceptance tooling; do not build another benchmark
framework. Begin with Q19/Q17, selected Q23/Q28/Q29 text work and Q34/Q35,
plus metadata-only and changed-source/non-reusing controls. Query IDs remain
in the harness. Compare fresh processes, repeated execution of one retained
plan, equivalent new plans and related queries in the same context. Fully
compute and independently validate every call; reading the same completed
result again is not query execution. Release results between calls unless
retained-result pressure is the declared control.

Require a material complete-query or workflow benefit attributable to reduced
work, a related-query benefit for a source-sharing candidate, and acceptable
first-use, source-churn and constrained-grant behavior. A hit counter or only
identical-query improvement cannot establish general source reuse. Freeze an
appropriate first-use/control allowance and retain every failure. If no useful
mechanism passes, close the investigation with that result and proceed to
ingestion; do not repeat failed candidates unchanged.

Keep routine Full43 as exactly one isolated execution of each of 43 queries,
with engine-only reporting, standard storage/deadline/validation/cleanup guards,
and other task-owned work idle. A separate warm Full43 profile follows only
after a useful mechanism passes the focused gates. The official
[ClickBench caching rules](https://github.com/ClickHouse/ClickBench#caching)
allow source-data caching but prohibit answer caches and answer-equivalent
intermediate caches. Its hot result selects the faster second/third call only
when both succeed, from adjacent per-query triples. Its
[output rules](https://github.com/ClickHouse/ClickBench#output-suppression)
require client-visible processing and returned results. Retain that clock
separately for any official comparison; an engine-only total cannot replace it.
Keep both clocks and all observations attached to the same calls rather than
combining independently selected minima into one claimed run.

No machine-wide cache reset, artificial pacing or uncontrolled repeat-until-fast
loop is requested. Label the first call first-use/uncontrolled. Full-file
integrity hashing can populate OS pages; a separately authorized true-cold
experiment must place hashing before its designated reset. The provider refresh's current
**49.625514041-engine-second Full43** is fresh-process with uncontrolled OS cache,
neither a true-cold measurement nor retained-runtime warm characterization.
The earlier grouped-preparation 49.503497253 and 48.693636581 observations have
the same distinction. Do not project competitors' cold-to-warm ratios onto
these numbers.
