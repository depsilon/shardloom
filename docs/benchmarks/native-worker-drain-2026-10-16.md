<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native worker failure and partial-startup cleanup

Two existing thread owners now finish cleanup before a failed operation escapes.
The exported morsel helper joins every worker after an observer error or panic.
The direct-thread Parquet reader owns each started worker immediately, so failed
startup closes its queue and joins earlier workers. Seven regression tests and
all seven selected local checks pass. Hosted integration remains pending.

This is a finite correction under PERF-03/06, the existing production-morsel
scheduling row and [RFC 0017](../rfcs/0017-fault-tolerance-cancellation-recovery.md).
It preserves worker counts, task ordering, output values and existing admission
limits. No new scheduler, provider, dependency or execution fallback is added.

## Failure contract

The [morsel helper](../../shardloom-vortex/src/scheduler_bridge.rs) previously
returned from its join loop on the first worker error or panic, dropping later
join handles while their workers could still hold state. It now uses standard
scoped threads and explicitly joins every handle. The first failure in worker
index order is preserved; state is merged only after complete success. A later
panic cannot replace an earlier ordinary error.

The [Parquet reader](../../shardloom-vortex/src/universal_format_io.rs) previously
collected handles before constructing its owning reader. Failure while starting
a later thread could therefore lose the earlier handles. Construction now
creates the owner first and uses fallible, named thread startup. The existing
queue-close/join destructor handles both startup errors and unwinding. No source
task is dispatched until all requested workers have started.

The source inventory covers fourteen lifetime-related implementation files,
including eight direct thread-owner files. Existing compute-pool and ingest
owners retain their own cancellation and drain contracts. The batch-protocol
stdin reader retains its child-process lifetime boundary.

## Counterexamples and verification

Four morsel regressions fail against the original scheduler, and two Parquet
regressions fail against the pre-fix startup ordering. Each failure records a
real memory reservation still held at return or unwind. Gates deliberately keep
another worker alive, and the caller samples reservations before notifying the
controller. The timeout only releases the test gate; elapsed time is not the
acceptance assertion. All tests then release their workers and confirm cleanup.

The corrected code passes all seven new tests: observer failure, worker panic,
both mixed failure orderings, partial startup error, partial startup unwind and
first-worker startup failure. Partial Parquet cases cover failure after one and
three started workers. They also verify that failed startup dispatches no tasks
and failed scheduling performs no merge.

| Local gate | Result |
| --- | --- |
| Workspace formatting and strict Clippy | Pass |
| Workspace tests | 3,225 pass; zero ignored |
| Compatibility-I/O strict Clippy and library tests | 824 pass; zero ignored |
| Release-user-surface strict Clippy and library tests | 2,904 pass; 27 declared manual exclusions |
| New regressions | All seven pass |

The native library command enables `release-user-surfaces`, including seven
existing encoded-scan tests absent from the narrower earlier local native run.
The totals are separate, overlapping suites. A test-harness Clippy failure was
corrected before the final checks; its source, log and receipt remain archived.
All final checks use the same four source hashes and record clean process drain.

The [packet index](evidence/native-worker-drain-2026-10-16.json) and
[compressed evidence](evidence/native-worker-drain-2026-10-16.json.xz) retain 74 members,
the four accepted source files, baseline sources, all original failed runs and
the seven successful check receipts/logs. The
[independent audit](evidence/native-worker-drain-independent-audit-2026-10-16.json)
reopens every member, verifies hashes and reparses successful and intentionally
failing test summaries. Primary review owns the lifetime and scope conclusions.
The patch is based on `38867f16a1d05f30461aad47ce96f4f7a6e3d46a`; archive SHA-256 is
`8dfa745a3b26e484a42601612ba1c4a8d3d27364fa4457091aec06061d25b1d9`.

## Scope and limits

The exported morsel helper has no direct query caller in the inspected tree.
The direct-thread Parquet reader is production code with `universal-format-io`
and without `vortex-write`; the release feature set exercises it as test code.
Native product ingest keeps its existing shared executor. This correction does
not establish a query speedup or wider allocation coverage.

Startup failures are injected at the Parquet spawner boundary. Actual operating
system thread exhaustion and morsel startup failure were not induced. Explicit
joins wait for synchronous callbacks or reads to finish or unwind; they cannot
interrupt blocked code or recover aborts. Automatic scoped-thread cleanup during
startup unwind guarantees worker-body completion, with the standard library's
thread-local-destructor limitation. Callback-created threads are outside this
owner. See [Rust scoped threads](https://doc.rust-lang.org/std/thread/fn.scope.html)
and [fallible thread startup](https://doc.rust-lang.org/std/thread/struct.Builder.html#method.spawn).

No Full43, public-workflow or installed-wheel run is claimed for this local
correction. Earlier buffer-table query evidence keeps its original source and
clock. Global CPU/I/O coordination, broader memory/spill/recovery obligations,
the six-area/eight-investigation scope and CG-1 through CG-23 remain with their
existing owners. Published 0.6.0 artifacts remain unchanged.
