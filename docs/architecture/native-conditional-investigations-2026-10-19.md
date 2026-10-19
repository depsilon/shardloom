<!-- SPDX-License-Identifier: Apache-2.0 -->

# Conditional investigation sequence — October 19, 2026

Status: active sequencing decision; no investigation is completed by this intake.
The maintainer requested all 21 remaining conditional investigations before the
rest of the queue, after the PR boundary. Public API/guide work is in
[PR #1577](https://github.com/depsilon/shardloom/pull/1577); finite native spill
work is in [PR #1578](https://github.com/depsilon/shardloom/pull/1578), stacked on
that API branch. Hosted failures remain part of PR ownership. No merge, release
or version bump is authorized by the sequencing decision.

The [phase plan](phased-execution-plan.md) remains the execution authority. This
table orders its existing IDs; it adds no phase, owner or obligation. There are
14 broad owners and 58 substeps: 37 required and 21 conditional. All CG-1 through
CG-23 remain visible in the phase plan and keep their independent evidence gates.
The earlier reconciliation and CPU intake remain historical scope records.

## Execution rule

For each row, first bind current production callers, provider/version, observed
cost, exact semantic/resource boundaries and the relevant
[owner verification contract](native-roadmap-verification-contracts-2026-10-18.md).
Then freeze a finite candidate, controls, complete-workflow measurements and
retain/drop criteria before changing runtime behavior or collecting candidate
timings. A possible idea or an existing regression pass is not an investigation
result. Existing rejected mechanisms stay rejected; no unchanged reruns.

Record an evidence-backed retained result, rejected result, or a specific missing
prerequisite. A rejection covers the evaluated mechanism and frozen scope. A
blocked prerequisite leaves the substep open and is revisited during the pass;
it is not accepted capability or a reason to restart unrelated required work.
Complete an investigation only when its stated finite scope has an auditable
decision, linked raw evidence and the applicable acceptance checks. Keep broader
owners open when their other obligations remain.

Minimal scaffolding or prerequisite evidence needed for a particular experiment
belongs with that investigation. Separate required capability implementation
stays deferred; this priority change does not waive provider, architecture,
security, memory, recovery or publication contracts. Freeze new workload-specific
selectors before an experiment rather than inventing success from generic tests.
Do not lower correctness or repeatability gates to force a positive result.

## Sequence and prerequisites

All rows initially remain pending. Related rows may share inventories or a
coherent validation batch, but each retains its own cost signal and decision.
Dependency changes may reorder rows within this pass, never silently remove one.

| Order | Existing conditional ID | Investigation boundary and prerequisite |
| --- | --- | --- |
| 1 | `PERF-03.codec_preparation` | Measure setup across actual codec calls; current contexts already reuse within an operation. Charge retained preparation, exclusive reset/lease, concurrency, reclaim and fresh-process decode. |
| 2 | `PERF-02.physical_cache` | Establish repeated physical reconstruction rather than references to a live owner; exact generation identity, retained bytes, lookup, admission and eviction are part of the comparison. |
| 3 | `PERF-02.builder` | Compare complete SQL generation/parsing/lowering with typed prepared construction through the shared plan boundary. |
| 4 | `PERF-05.nested_identity` | Measure repeated recursive keys before shared identity; include canonicalization, collision resolution, representatives, row mapping and nonrepeating controls. |
| 5 | `PERF-10.compact_selections` | Inspect a materially different production representation/caller opportunity. The contiguous-range candidate is already rejected; retain complete gather/NULL/ownership semantics and both count/projection consumers. |
| 6 | `PERF-10.predicate_exclusion` | Prove complete immutable-region no-TRUE evidence with full expression/source/semantic identity; include construction, lookup, invalidation and unrelated-query controls. |
| 7 | `PERF-06.merge_cost` | Capture real run sizes and stable lineage, then compare adjacent schedules within reader, memory and overlapping-disk grants. |
| 8 | `PERF-06.rematerialization` | Identify reclaimable derived owners and whole-dependency reconstruction cost; preserve lifetime/revocation, bounded regeneration and zero-reclaim controls. |
| 9 | `PERF-07.output_windows` | Establish a complete-workflow queue bottleneck before byte-credited overlap; retain slow/failing consumers, cancellation and provisional completion. |
| 10 | `PERF-09.overlap` | Establish recoverable idle time before extending pre-writer overlap; include ordered feed, bounded completed blocks, EOF, cleanup and publication races. |
| 11 | `PERF-09.portfolio` | Require genuinely different admitted profiles and per-column encode/decode/physical-byte evidence; do not repeat the rejected five text profiles. |
| 12 | `PERF-09.structural_payload` | Use actual original-payload structure for a small reversible representation; charge residuals/exceptions/validity, ordinary consumers, full artifacts and fresh-process reopen. |
| 13 | `PERF-02.changed_source` | Require trustworthy immutable file/region identity and compatibility; compare full refresh against rebuild including validation, copying and new metadata. |
| 14 | `PERF-10.multiway_joins` | Freeze an independently specified compatible-domain workload and ordinary binary-join controls; include preparation, multiplicity, NULLs and order. |
| 15 | `PERF-10.learned_indexes` | Identify a large ordered structure with repeated search cost before bounded-error prediction and exact correction; include construction and shifted/nonbenefiting controls. |
| 16 | `PERF-03.regions` | Require a newly justified mechanism; prove generation-bound natural coverage, admission, skew, ordered handoff, cancellation and drain independently of worker count. |
| 17 | `PERF-10.ordered_candidates` | Region and local-reduction gates precede local ordered candidates and global merge; preserve offset/limit, full keys, ties and absolute source identity. |
| 18 | `PERF-09.design` | Region evidence precedes joint preparation cost for storage, ingest, topology, pruning, encoded work and merge across varied planner inputs. |
| 19 | `PERF-12.topology` | A newly admitted mechanism precedes the immutable 100M-row, 129-call comparison. Preserve the rejected coalescing decision and original complete-output protocol. |
| 20 | `PERF-13.pgo` | Establish material instruction/dispatch cost, then freeze representative/held-out training, compilation/transfer break-even and portable-package correctness. |
| 21 | `NATIVE-ENCODED-EXCHANGE.experiment` | First complete the scoped pinned-provider protocol/ownership/security review; then compare a bounded producer/transport/receiver/real-consumer path with matched Arrow/Flight and ordinary Vortex IPC controls. Local transfer is not transport proof. |

The fixed-count/known-bound nonreplacement sampling baseline is preserved on
`codex/native-sampling-candidates` at `2bea9553`. It adds only the deterministic
reference generator and fixture; it does not add candidate runtime behavior.
The 32-case current-runtime pressure/control observation remains local in the
sampling evidence workspace. Resume that required family after this conditional
pass with its original grants, complete values and failure observations intact.

## Evidence and claims

Each completed decision links its frozen protocol, original/candidate sources,
compiled identities, complete values/types, actual activation, memory/storage
ownership, cancellation/failure/cleanup and raw cost controls. Preserve failed
observations and dropped implementations. Use native Vortex providers inside
approved boundaries; add no external-engine execution fallback. Baseline engines
are independent test/comparison oracles only.

Keep query engine clocks separate from preparation and supervision. Routine
Full43 reporting uses one execution of each query and the sum of
`engine_execution_seconds`. New representation or resource objectives retain
their own predeclared complete-workflow throughput controls. No broad performance,
production, package or platform claim follows from an intake, local microbenchmark
or isolated accepted candidate.
