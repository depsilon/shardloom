# Adaptive ingestion CPU budget

Status: implementation and acceptance in progress under PERF-03/08/12 and
CG-5/6/8/20/21. The October 2 maintainer request gives this unit priority over
the remaining native operator work. P4/P6/P8 name CPU grants, not phase IDs.
The [phased plan](phased-execution-plan.md) owns the active queue.

The September 30 [grant screen](../benchmarks/quiet-runtime-results-2026-09-30.md)
showed that larger grants can improve ingestion, but did not establish adaptive
use of those grants. The preceding fixed recipe reserves a caller, a source worker
and a converter before assigning the remainder to the writer. Idle stage owners
cannot execute ready work from another stage. Queue depth is not CPU utilization.

## Contract and reuse

Use one bounded, artifact-local CPU grant for admitted source tasks, conversion,
native statistics, compression and layout work. The caller participates in the
same executor as at most `P - 1` joined background drivers. Tasks retain logical
source order independently of which driver executes them. A blocked producer or
consumer yields its executor lane while its bounded queue and owned memory remain
charged. No live thread-pool resizing, extra automatic CPU allowance, tuning knob,
or cross-run learning is introduced.

Vortex-first provider check: use the pinned Vortex 0.85
`CurrentThreadRuntime`, `Handle::spawn`/`spawn_cpu`, `Task::poll_join`,
`VortexSession`, native array conversion and layout writers. The public runtime
explicitly supports cloned drivers over one shared executor; its CPU tasks use
that executor too. Reuse ShardLoom's `ResidentWorkerGroup` for immediate wake and
joined teardown instead of upstream detached worker ownership. Reuse the existing
Parquet metadata/task boundaries, source-generation guards, native allocation
leases, bounded source-batch subtrees, writer lookahead slot, statistics and
atomic publication. This is `use_vortex_native_provider` with ShardLoom admission
and lifetime policy, not a replacement source, codec or query engine.

| Existing component and callers | Remaining gap | Shared extension and proof |
| --- | --- | --- |
| Universal Ingest columnar adapters, reached by CLI and Python preparation | Dedicated producers retain CPU lanes while their bounded output is full | Ordered source tasks use the admitted Vortex executor; source rendezvous and full-queue tests prove every granted driver can do ready work. |
| `ResidentWorkerGroup` and pinned Vortex runtime | Source, converter and writer drive different executors | One artifact-local runtime owns `P - 1` joined drivers plus its caller; cloned handles cannot create overlapping driver groups. |
| Streaming conversion and ordered handoff | The same conversion algorithm is tied to a dedicated `ComputePool` | A dispatch strategy uses the shared runtime while reusing validation, native buffer ownership, failure precedence and ordered delivery; legacy custom-reader coverage remains. |
| Native bounded subtree writer, codecs, statistics and atomic publication | Provider work cannot borrow an idle source/conversion lane | The existing writer session receives the shared handle; complete native reopens, constrained memory, real codec work, cancellation and publication-collision tests cover the composed pipeline. |

The source and conversion windows bound unfinished work separately from driver
count. Conversion admission further limits its window using one quarter of the
supplied memory budget and the first observed batch: reserve the existing 2x
conversion headroom with a further 2x batch-size margin. This initial estimate
does not predict all future skew; every batch retains its explicit reservation
check and an oversized later batch fails with cleanup. Preserve complete ordered delivery, including skewed early source tasks,
empty/final batches and failures. Waiting for a source, handoff or mutex must not
occupy every executor driver needed to complete that wait. Failure cancels new
admission, drains submitted tasks, drops retained results and joins owned drivers
before returning. A narrower grant cannot silently reuse an oversized live owner.

This unit applies PulseWeave's bounded work inventory and scarcity principles to
actual admitted tasks; it does not promote the broader advisory feedback loop.
Admission uses the supplied CPU/memory envelope and natural source work, without
claiming that every workload exposes enough parallel work to consume every core.
I/O latency, serial format readers, memory pressure and final publication can
legitimately limit useful CPU work. Idle time is not itself an error. The target
is removal of avoidable stage reservations while useful admitted work is ready.

Blocking I/O services, compatibility-reader internals, provider scratch bypassing
the native allocator, OS caches and process RSS retain their explicit exclusions.
This is not a machine-wide or concurrent-session CPU quota. CLI and Python
continue to pass the same public grant through preparation and native execution;
the follow-on query keeps its own established resource contract. Vortex remains
native output with `fallback_attempted=false` and `external_engine_invoked=false`.

## Acceptance

Require deterministic progress and complete native reopen equality at P1, P2,
P4, P6 and P8; source-heavy, conversion-heavy and codec-heavy work; skew, bounded
queues, constrained memory, failure, cancellation, source mutation and joined
teardown. Prove that ready work can consume the grant when another stage is
waiting, rather than inferring this from configured thread counts.

Freeze the revision, features, binary and inputs before a guarded ingestion
comparison. Keep all native builds/tests/measurements sequential. Record every
P4/P6/P8 observation, CPU/RSS context, source and native result identity, and
complete query results over the newly ingested artifact. Use the existing storage
guard, source-residency checks, process deadline and exclusive workload lock.
Any physical-layout change requires full-value/schema/statistics/provenance
comparison and Full43 checks; historical byte equality alone is insufficient.
Do not retain a performance regression merely because a topology looks better.

The frozen comparison uses two calls per role at each of P4/P6/P8 in alternating
control/candidate order, reversing the grant and role order for the second pass.
Keep every call, median, fastest valid call, CPU time and RSS observation. Retain
the runtime only with at least a 5% geometric-mean improvement in fastest-valid
native ingest time across grants and no per-grant median regression above 5%.
The subsequent paired Full43 check uses the existing symmetric fastest-valid
rule and must preserve complete results without a total query-time regression
above 5% on the new artifact. Host interference or inconsistent repeats require
an explicit follow-up cohort rather than deleting unfavorable observations.

Required checks include workspace fmt/clippy/tests, native provider and CLI
feature tests, compatibility-reader-only and lean builds, affected Python and
public preparation tests, the focused CPU/lifecycle matrix and the frozen guarded
ingestion/query packet. Align public evidence descriptions, review the complete
diff, and move this finite record to the completed ledger only after acceptance.
No whole PERF owner or competitive gate closes from this unit; packages,
releases and paused large format/text experiments remain outside its scope.

## Implementation verification before the frozen comparison

Workspace fmt, clippy and all-target tests passed. The release-feature Vortex
suite passed 2,184 tests; the final ingest subset, source-sharing tests and added
overlapping-owner admission test passed after the ownership guard was added.
The release-feature CLI all-target suite and clippy passed, as did the lean
workspace build, compatibility-reader-only tests and Python suite. Initial
failures exposed the conversion reservation regression and stale diagnostic
expectations; their receipts remain in the local acceptance packet. Performance
and fresh-artifact query acceptance remain pending and are not implied by these
implementation checks.
