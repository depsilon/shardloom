<!-- SPDX-License-Identifier: Apache-2.0 -->

# User-surface native execution parity

Status: implementation and five-surface correctness acceptance complete October 24;
the strict paired timing screen remains failed with a recorded finite disposition.
The [acceptance report](../benchmarks/native-surface-parity-2026-10-24.md) preserves
every cohort and the remaining Q34 timing uncertainty. This follows the completed
[grouped optimization campaign](native-grouped-preparation-2026-10-24.md), its
isolated Full43 and a PR preserving the decision. This is a focused native
execution parity pass under RFC 0044 and the existing user-workflow contracts,
not another public API redesign. CG-1 through CG-23 remain independent gates;
the optional backlog and rejected optimization mechanisms remain unchanged.

## Active implementation and frozen comparison

The implementation baseline is `16ff752d`, with the accepted G1/G2 runtime.
Two new alias/HAVING regressions fail provider admission on that baseline.
Selection now reads the normalized, typed tree, including scan predicates that
the binder pushed down. Pure alias chains compose output, ordering and HAVING
names; range, computed-projection, numeric and nullable barriers remain.
Provider metadata owns only its lowering scratch and concrete request copies,
releasing scratch after lowering rather than retaining four times the binder's
entire allocation. Low credit still selects the admitted general native path.

The first frozen 270-call comparison validates every complete result and passes
all accepted-CLI controls, but fails the frontend parity gate. Preserved reports
identify two actual work differences: aliased DataFrame `COUNT(*)` scans
99,997,497 rows while the equivalent workflow reads held row-count metadata;
pure filtered projections use the general relational scan while CLI collection
uses the resident projection provider. Other grouped-query timing differences
are not yet attributed to a frontend-specific mechanism.

The next coherent correction admits aliases and multiple unfiltered `COUNT(*)`
measures through the existing footer aggregate path, lowers complete pure
projection/filter pipelines to the existing resident projection producer, and
avoids constructing a payload reader for typed metadata-only count. Source
generation, cancellation, shared credit and completion checks remain. Computed
expressions, nonzero offsets and range/filter ordering barriers retain the
general native path when equivalence is not proved. Projection reports expose
actual relational scan delivery without inventing aggregate-provider counters.
No engine clock definition changes. Default and native workspace lint/tests
pass (3,243 and 6,053 tests respectively, with 35 existing native ignores);
the matched public checks also pass all 215 fixture results and 17 provider
tests. That preflight exposed remaining generic aggregate completion for a
single aliased count. The shared SQL selector now binds that complete shape to
the existing typed count provider. All five interfaces prove its metadata-only
path, including empty input, and the public checks pass again. Native CLI,
changed worker-test and required default-workspace checks pass on this final
correction; the unchanged broad native runtime retains its preceding proof.
The subsequent frozen 270-call cohort again validates every value and passes
all accepted-CLI controls. Workflow/Python SQL and CLI/workflow comparisons
pass the original timing screen; DataFrame and typed count/Q35 still fail it,
and the faster DataFrame Q28 observation also fails its symmetric allowance.
That cohort is not accepted as surface parity.

The preserved scan reports expose Q35's concrete difference: its typed lowering
attaches a constant group to the first source column, adding `WatchID` to a scan
that otherwise needs only `URL`. Moving raw keys ahead of computed keys also
changes the selected aggregate strategy. Lowering now anchors constants to
already-required keys/measures and preserves declared mixed-key order through
the same native identity expressions used by primitive SQL. Count evidence
now records the exact footer proof and empty physical read set without building
an unrelated per-column schema/layout inventory. Explicit layout inspection
remains available; generation, cancellation, completion and certificate checks
stay inside their existing boundaries. This removes reporting work rather than
moving it outside the engine clock.

Both new regressions fail before correction; all 31 native provider tests pass
afterward. The corrected source passes full native/default workspace lint and
tests (6,055/3,243 passed, zero failures; 35 existing native ignores), plus format
checks. The matched release build passes all 18 public provider tests, 215
independent five-surface fixture results, 172 preparation-only declarations and
adapter corruption/self-checks. The first public count assertion used a field
not exposed by every formatter; its correction checks the existing shared
footer-evidence fields without changing the runtime. That failed check remains
in the evidence. The corrected 270-call cohort validates every result and
passes every accepted-CLI control, all workflow/Python-SQL comparisons and all
workflow/typed comparisons. The count and Q35 differences pass, but DataFrame
Q34 (2.733800 versus 2.297242 median engine seconds) and workflow Q28 versus CLI
(2.405973 versus 2.157023) fail the unchanged allowance. The recorded strategies,
read sets and principal work counters match; Q34's slower samples show higher
worker partition-lock waits, while Q28's repeated difference lies in aggregate
updates. These observations locate investigation targets, not established causes
or grounds to discard samples. At that stage the final five-surface Full43 and
real-input performance disposition remained pending. Preserve all three completed cohorts,
intermediate fixture evidence and failures; no failed cohort is rerun unchanged.

Further physical-plan inspection finds a concrete Q28 work difference: SQL
adds a hidden HAVING count already present in the selected measures, while the
equivalent DataFrame/typed plan uses the selected count directly. Both CLI and
workflow SQL contain that duplication, so it does not by itself explain their
entire timing gap. Shared SQL normalization now reuses matching reductions by
function, distinctness and parsed argument, including repeated hidden measures.
Nested subquery reductions stay in their own scope. Regression tests distinguish
different expressions/distinctness, preserve private-name collisions and execute
quantified subqueries with passing and rejected HAVING results. The public
grouped test checks both alias and raw aggregate predicates across all five
interfaces, including the physical measure list.

The correction passes 1,444 native CLI tests (three existing ignores), 3,246
default workspace tests, strict lint and format checks. A first lint attempt
rejects two test assertion styles; the corrected assertions preserve behavior
and the failed receipt. Only three CLI Rust files differ from the preceding
full-native proof, so the unchanged Vortex/runtime retains its 6,055-test proof.
The matched release artifacts pass all 18 public provider tests, all 215
five-surface fixture results, 172 preparation-only declarations and the adapter's
complete-result/corruption checks. Its 270-call real-input cohort validates
every complete result and every accepted-CLI control. Q28 and all other cells
pass the original timing screen except workflow/DataFrame Q34: workflow is
2.936739750 median engine seconds and DataFrame 2.553339792. The screen remains
failed; the allowance and all calls are preserved.

Q34's preceding matched medians were 2.240855500/2.238672083 (pass), then
2.297241875/2.733799875 (DataFrame slower, fail). The latest reversal does not
establish a persistent frontend penalty. All six latest reports share 282 of
298 fields exactly: the same source/strategy, URL projection, 99,997,497 rows,
3,120 completed chunks, 31,290,298 partial entries, 9,038,838,007 hashed bytes,
nine active workers and no pressure transition. The sixteen varying fields are
timings, peak live memory and entry-credit claim/return counts. Slow individual
samples occur on both roles with higher partition-lock waits. This locates
shared-runtime variability; it does not prove its cause or establish statistical
timing equivalence. Source inspection confirms the common aggregate producer
and partition reconciliation. No further frontend-specific mechanism is justified.

Apply the original handoff's finite rule: record an unestablished repeatable
difference rather than changing code until a preferred interface wins. Stop
the paired loop, preserve its failed screen and the original measurements,
and perform the already requested single Full43 per surface for complete
values, schemas and native-path evidence. The one-off completion harness now
binds the exact reviewed attribution record and explicitly retains
`paired_timing_screen_passed=false` in its plan and receipt. This changes the
progression decision, not the runner, threshold, engine clock, runtime or sample
set. Full43 cannot override that failed screen.

The final matched build completes one isolated Full43 per surface: CLI SQL
**48.701809680**, embedded workflow **49.967501000**, Python SQL **50.826739087**,
DataFrame **51.412729914** and typed native expressions **52.287469919 engine
seconds**. All 215 complete results, 1,905 rows and 43 cross-surface schemas match
the independent reference, with no external execution. Source, full input hashes
and cleanup checks pass. These sequential observations complete the requested
value/schema/native-path acceptance; they do not establish a frontend ranking,
statistical timing equivalence or an overall speedup. Q34 shared-runtime timing
variability remains a limitation, and the original strict screen remains failed.
The linked packet replays all 1,080 paired results and 215 final results without
running an engine, retaining intermediate checks, failures and source patches.

Vortex-first check: this changes ShardLoom's policy/binding boundary, using the
existing Vortex `DType`, typed scalars, array views, scan expressions and prepared
file providers. It adds no execution substrate, Arrow materialization or external
engine. Source-generation checks and the shared operation owner stay in place.

Before scoring, freeze the same CLI/library build, input, independent reference,
24-GiB/12-lane grant and complete-result contract. Use exactly three alternating
pairs per comparison for Q1/Q17/Q19/Q20/Q23/Q28/Q29/Q34/Q35. Compare the four
embedded routes with matched fresh result ownership, then CLI with its separate
process boundary. Before measurement, apply the existing allowance of the larger
of 10% or 100 microseconds to median engine time: frontend parity checks the
absolute difference against the faster median, while the accepted-CLI control
checks only candidate regression against its control median. Compare the
accepted CLI binary with the candidate CLI on the same cells. Preserve every call and all available
strategy, worker, source, memory and reuse fields; missing measurements stay
unknown. Complete fixture conformance precedes the focused real-input cohort.
One final isolated pass per surface (43 queries once each) checks full values,
schemas and native paths after the coherent correction. This finite plan is not
permission to repeat a failed cohort unchanged or to rank sequential Full43s.

The subsequent [controlled provider refresh](native-vortex-provider-refresh-2026-10-24.md)
now runs immediately after parity, before the
[warm execution/source-reuse handoff](native-warm-source-reuse-2026-10-24.md).
Both reuse this pass's lifecycle, completion and resource evidence. Exercise existing
retained preparation here where required for parity; new source-sharing or
retention mechanisms belong to that bounded next package before full ingestion.

## Contract and evidence boundary

Equivalent computation, source data/types, resources, live ownership and result
contract must have equally capable native execution through CLI SQL, embedded
workflow, Python SQL, DataFrame and typed expressions. Frontend syntax must not
unnecessarily change decoding, pushdown, aggregation, workers or intermediate
materialization. Parsing, process startup and explicit result conversion are
different activities; identical total call duration is not the requirement.

The historical five-surface Full43 observations are 52.414 engine seconds for
CLI SQL, 61.010 for embedded workflow, 72.963 for Python SQL, 69.390 for DataFrame
and 69.154 for typed expressions. These sequential single passes predate the
final #1593 corrections and #1594 optimizations. They validate matching results
and schemas, but do not establish causal frontend penalties. The newer
48.693636581-second CLI observation is not a refresh of all five surfaces.
Preserve every reading and do not combine them into a current ranking.

The maintainer's source review covers main `06653efb` and #1594 `e1af0d68`; it
does not claim independent benchmark execution. Recheck the current source
before implementing the findings below. Historical CLI, workflow and Python
SQL reports already share selected strategy labels and available counters for
the investigated queries. Some DataFrame/typed wrappers lose those details;
physical read-column lists and complete active-worker observations are missing.
No single cause explains the complete historical spread yet.

Locate the first demonstrated difference along declaration, semantic binding,
physical selection, resource/scheduling, actual native work and result handling.
Correct that boundary. Keep the fastest correct path fast; do not slow CLI,
weaken semantics or move required work outside its engine clock to claim parity.

## Existing foundations to preserve

- Supported Linux/macOS application backends already choose native execution
  before execution unless explicit configuration selects the subprocess route.
  Failure is not permission to switch to an external engine.
- Python preparation, collection and writing already use `py.detach`; blanket
  interpreter-lock release is not an outstanding implementation task.
- #1583 repairs format-dependent selection; #1593 connects equivalent typed
  and DataFrame plans to existing providers. Extend those shared boundaries.
- Configurable sessions already own one resource allocation and reconfigure
  complete operations without making an override the next call's default.

## Ordered work and acceptance

1. **Normalize bound semantics and remove avoidable preparation duplication.**
   `local_primitive_relational_provider_plan.rs::lower` recognizes declaration
   shapes; its HAVING matcher expects Filter immediately above Aggregate.
   Provider preparation receives the bound root but lowers the original plan.
   Verify equivalent Aggregate → COUNT filter and Aggregate → rename COUNT →
   renamed filter as a regression case. Converge below syntax on normalized,
   fully bound semantics; do not render every typed plan into SQL. Preserve
   nullability, numeric literal types, overflow, byte versus character length,
   ties, offsets and expression errors, including recent nullable/mixed-numeric
   HAVING repairs. The adapter's four-times-bound-metadata reservation and
   half-remaining-credit rule are source-confirmed admission differences, not
   established causes of the historical timing spread. Share immutable bound
   information and account for actual necessary representations; keep resource
   checks and the general native path available.
2. **Reconcile native production, completion and result handling.** The CLI
   aggregate `collect_jsonl` uses the shared batch producer, synchronous JSON
   sink and `finish_with_timing`; embedded native execution retains result
   owners around an enclosing timer. Primitive aggregate native collection
   already shares `for_each_batch`. Trace finalization, required validation,
   result ownership and terminal completion; do not infer unrelated engines
   from different wrappers. Retained results, incremental batches, JSON and
   file writing should consume a shared producer without forcing full retention
   on streaming paths. Preserve engine-only reporting; any real timing-boundary
   correction must identify its changed definition and preserve historical
   values. A timer correction alone is not a performance optimization. Full43
   returns few rows, so ownership is not by itself an explanation for the spread.
3. **Compare actual resource use with matched lifecycle.** Record admitted CPU
   ceiling and actual workers, aggregate-worker versus provider-driver choice,
   scan concurrency/partition geometry, retained plan/result allocations,
   source opens and preparation reuse. Compare fresh and retained sessions
   separately; keeping prior results alive is a different workload. Correct
   demonstrated differences within the explicit grant, without another thread
   pool, silently expanded CPU/memory or violated result ownership.
4. **Carry one structured native execution report through wrappers.** Preserve
   normalized operation, provider/strategy and declined-alternative reasons;
   physical input/predicate columns; relevant rows/chunks, decode/copy work,
   actual worker activity and spill; preparation reuse, live resources, output
   contract, build identity and timing boundary. Reuse existing counters. Missing
   measurements remain unknown, not zero or an inference from intended behavior.
   No per-row logging or new large evidence system is requested.

This applies metadata-first preparation reuse, existing capillary work units
and dynamic admission through shared native families. PulseWeave is relevant
only where an existing producer uses it; no separate scheduler is implied.
Timing-surface separation and evidence-tier controls are required for honest
attribution. Native Vortex input/output, explicit sink conversion and
`fallback_attempted=false` / `external_engine_invoked=false` remain mandatory.

## Finite verification

Reuse the existing five-surface conformance suite and alternating paired-query
runner. Freeze the same current CLI/native-extension build, effective features,
compiler configuration, loaded extension identity, immutable input, resource
policy and result contract. Historical artifacts already document matched
release builds; a debug/release mismatch is not a diagnosed cause.

Start with Q19/Q23/Q28/Q29/Q34/Q35, a metadata-only count, selective projection,
and an alias/HAVING or sort/limit composition. Query IDs belong only in the
harness. Preserve Q28/Q29's explicit byte-length adapter and unchanged independent
reference. First match workflow, SQL and typed lifecycle/native-result handling
in the same Python environment, then compare CLI while retaining its separate
startup boundary. Freeze tolerances from existing policy and observed
repeatability before scoring, including an absolute allowance for tiny queries.

Use alternating comparisons with other task work idle, standard guards, no
artificial pacing, concurrent profiling, active recursive inventories or new
result-cache warmups. Different plans call for lowering/normalization repairs;
different admission calls for selection/resource repairs; different actual work
calls for preparation/scheduling/scan/result repairs. Persistent timing
differences with equal observed work require clock, code, allocation, CPU and
lifecycle investigation. If no difference repeats, record that result rather
than changing code until a preferred frontend wins.

After a coherent correction, run existing full cross-surface acceptance and the
required workspace/native checks. This is not permission for perpetual five-way
Full43 repeats. Completion requires equally capable equivalent declarations,
honest native completion/conversion boundaries without JSON/Python-row
intermediates, explainable grants and live ownership, no stable material engine
penalty attributable solely to frontend choice within frozen tolerance, and no
regression of the fastest correct implementation. It does not promise identical
stopwatch readings, general API redesign, optional capability expansion,
external-engine superiority or package publication.
