<!-- SPDX-License-Identifier: Apache-2.0 -->

# User-surface native execution parity

Status: accepted October 24 maintainer handoff, queued after the remaining
[grouped optimization campaign](native-grouped-preparation-2026-10-24.md), its
isolated Full43 and a PR preserving the decision. This is a focused native
execution parity pass under RFC 0044 and the existing user-workflow contracts,
not another public API redesign. CG-1 through CG-23 remain independent gates;
the optional backlog and rejected optimization mechanisms remain unchanged.

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
