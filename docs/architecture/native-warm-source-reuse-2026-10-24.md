<!-- SPDX-License-Identifier: Apache-2.0 -->

# Warm execution and reusable native source state

Status: accepted October 24 maintainer handoff, queued after
[user-surface native execution parity](native-user-surface-execution-parity-2026-10-24.md)
and before [full-ingest ownership/preparation](native-ingest-ownership-preparation-2026-10-24.md),
then [universal I/O](native-universal-io-performance-coverage-2026-10-24.md).
The subsequent [public-naming pass](shardloom-public-naming-2026-10-24.md)
also precedes ingestion and uses this package's settled lifecycle terminology.
This is source review and a finite implementation/measurement scope, not an
implemented cache or a measured warm Full43 result. The review uses #1595 at
`bc2d1b6a`, whose runtime is unchanged from the accepted grouped campaign.
RFC 0044 and the existing PERF-02 preparation, PERF-03 resource, PERF-06
ownership and PERF-09 evidence contracts apply. CG-1 through CG-23 remain
independent gates; this does not create another engine mode or phase ID.

## Size and ordering decision

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
  file preparation enables the pinned layout-reader cache. A direct call to
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
domains or encoded chunks before expanded decoded blocks. Recheck pinned Vortex
0.85 provider, repeated-scan, buffer and allocator APIs; existing repeated scans
alone do not establish cross-query ownership. Extend admitted native provider
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
experiment must place hashing before its designated reset. The current
**49.503497253-engine-second Full43** is fresh-process with uncontrolled OS cache,
neither a true-cold measurement nor retained-runtime warm characterization.
The earlier 48.693636581 observation has the same distinction. Do not project
competitors' cold-to-warm ratios onto either number.
