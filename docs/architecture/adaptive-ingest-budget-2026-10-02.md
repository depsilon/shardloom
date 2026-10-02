# Adaptive operation resource budget

Status: local allocation acceptance passed; hosted checks pending under PERF-03/08/12 and
CG-5/6/8/20/21. The October 2 maintainer request gives this unit priority over
the remaining native operator work. P4/P6/P8 name CPU grants, not phase IDs.
The [phased plan](phased-execution-plan.md) owns the active queue. The maintainer
clarified that allocations may vary far beyond P4/P6/P8 and are supplied at the
start of each operation. This is an engine contract across Rust, CLI and Python;
a Python repository in Foundry is an example deployment, not a special runtime.

The September 30 [grant screen](../benchmarks/quiet-runtime-results-2026-09-30.md)
showed that larger grants can improve ingestion, but did not establish adaptive
use of those grants. The preceding fixed recipe reserves a caller, a source worker
and a converter before assigning the remainder to the writer. Idle stage owners
cannot execute ready work from another stage. Queue depth is not CPU utilization.

## Contract and reuse

Treat the supplied positive CPU maximum and memory budget as ceilings. Select
local CPU parallelism from the minimum of the supplied maximum and the CPU
capacity available to the process, once when creating an operation owner.
Preserve requested and applied values separately in existing evidence. An
explicit one-CPU environment setting must remain one CPU. Defaults do not
override an explicit allocation. Do not shrink batch sizes using CPU lanes that
were never admitted, or allocate worker handles proportional to an unbounded
request. Reuse the same local CPU admission helper for ingestion and resident
query owners; existing operator work and memory limits can narrow it further.

Use one bounded, artifact-local CPU grant for admitted source tasks, conversion,
native statistics, compression and layout work. The caller participates in the
same executor as at most `P - 1` joined background drivers, where `P` is the
applied grant. Tasks retain logical
source order independently of which driver executes them. A producer or consumer
waiting on a bounded queue yields its executor lane while its queue slot and
existing native allocation credits remain held. No live thread-pool resizing, extra automatic CPU allowance, tuning knob,
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
count. Conversion has no fixed P4/P6/P8 recipe or 32-task ceiling: its CPU ceiling
is the applied grant. Conversion admission further limits its window using one quarter of the
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

Test selection independently of the test host across small, large, irregular
and extreme CPU requests (including 1, 3, 17, 64, 128 and `usize::MAX`), simulated
available capacities, and memory budgets. Prove that increasing available CPU
does not encounter an arbitrary fixed conversion ceiling, that memory can narrow
the admitted window, and that no arithmetic overflows or request-sized worker
allocations occur. Execute real pipelines with both small and over-host requests
and compare complete native values and query results. Sequential operations with
different grants must not retain the preceding operation's allocation.

Require deterministic progress and complete native reopen equality;
source-heavy, conversion-heavy and codec-heavy work; skew, bounded
queues, constrained memory, failure, cancellation, source mutation and joined
teardown. Prove that ready work can consume the grant when another stage is
waiting, rather than inferring this from configured thread counts.

The initial frozen prototype already completed twelve guarded P4/P6/P8 ingests
with exact artifact hashes. It showed a 1.458% geometric-mean improvement in the
fastest calls, below its predeclared 5% performance threshold, and increased RSS.
Preserve all observations as prototype evidence; it did not establish a speedup
claim. The clarified acceptance target is correct automatic selection for
arbitrary supplied budgets, not achieving an unrelated timing threshold.
The separate fresh-publication digest experiment was set aside and is not part
of this change. No additional full-size timing campaign is required to prove
the selection contract. Any later performance claim still needs a frozen,
guarded comparison with complete outputs and CPU/RSS evidence.

Required checks include workspace fmt/clippy/tests, native provider and CLI
feature tests, compatibility-reader-only and lean builds, affected Python and
public preparation tests and the broadened CPU/memory/lifecycle matrix through
ingestion and native query execution. Align public evidence descriptions, review the complete
diff, and move this finite record to the completed ledger only after acceptance.
No whole PERF owner or competitive gate closes from this unit; packages,
releases and paused large format/text experiments remain outside its scope.

## Allocation acceptance

The final allocation implementation passes the following local checks. Counts
overlap across feature configurations; ignored and skipped cases are not counted
as successful executions.

| Check | Result |
| --- | --- |
| Default workspace all-target tests | 3,443 passed |
| Release-feature native Vortex tests | 2,187 passed; 23 existing ignores |
| Release-feature CLI all-target tests | 1,566 passed |
| Compatibility-reader-only tests | 44 passed |
| Python suite | 705 passed; 144 existing skips |
| Strict workspace and release-feature Clippy | Passed |
| Workspace without default features | Passed |
| Minimum Rust 1.96 release-feature all-target check | Passed |
| Formatting, public-reference and documentation validators | Passed |
| Website generation, build and readiness | Passed |

The independent selection grid includes capacities through 4,096 and
`usize::MAX`; the conversion grid includes a 96-lane selection from a 128-lane
request and a memory-constrained reduction to three. Real source, conversion and
writer calls use requests 1, 2, 3, 4, 6, 8, 17, 64, 128 and `usize::MAX`, then
return to one, with alternating 2 MiB and 32 MiB accounted-memory limits. Each
fresh Vortex output is reopened and every nullable text and large integer value
is compared. Repeated native count and projection queries verify the fresh output
under the same supplied budget. Codec-heavy, full-queue, skew, cancellation,
mutation and failure cases are included in the broader native suite.

CLI environment and Python session/standalone transport tests cover explicit
one-CPU and over-host allocations, collect/count, all eight writers and fanout.
Session evidence reuse requires matching memory and CPU requests. Actual native
drivers are capped by this ten-CPU test host; the larger-capacity grid is policy
proof, not execution on a 128-core machine. Initial failures and their repairs
remain in the acceptance packet. No performance improvement, full utilization,
global quota or total-process-memory guarantee follows from these checks.

The immutable [portable evidence packet](../benchmarks/evidence/adaptive-operation-allocation-2026-10-02.json.xz)
binds the checks to runtime/SDK revision
`148de25a23b26e9301e4e74db8a68a7cdeef3008` and verifies 645 source-file hashes
unchanged through acceptance. Packet SHA-256:
`f4e3f0ad84bd8a89b4042d5f5af9414360cd126a2225c2fa5b8f11d595591516`.
It preserves the initial twelve complete-artifact comparisons, original CPU/RSS
observations, the failed timing criterion, failed development checks and repairs,
raw-log hashes and verified cleanup. Larger allocation selection and fresh-query
proof are separate from that historical timing cohort. Primary review found no
remaining actionable defect in the accepted diff; hosted status is recorded
separately after checks complete.

The initial hosted documentation gate found the new queue item missing from the
v1 inclusion matrix. The required row and the preceding resource unit's merged
status were aligned, and the complete public-status validator then passed.
This documentation repair leaves the accepted runtime and source hashes unchanged.
