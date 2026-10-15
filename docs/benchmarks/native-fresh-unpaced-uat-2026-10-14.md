<!-- SPDX-License-Identifier: Apache-2.0 -->

# Fresh local UAT without artificial pacing

Status: passed on the combined decoder/context source at
`9dd328f5cbdae08fa396910e7c6ce747ff16dda1`. The maintainer requested fresh ingest,
all ClickBench queries and the format/workflow matrix without battery/thermal
admission or artificial delays. This completed run retains the three executions
per query declared before it started. The subsequent maintainer direction makes
one execution per query the default for future UAT.

| Observation | Result |
| --- | --- |
| Fresh ingest from resident official Parquet | 99,997,497 rows; 54.603468 seconds native process time |
| Full43, three fresh processes per query | 129/129 complete-value checks; 194.125941 seconds summed native process time |
| Actual supervised ingest plus query workflow | 331.624211 seconds |
| Modular format/workflow acceptance | 18/18 cohorts; 1,408 comparisons, including 704 native executions |
| Modular supervised workflow | 575.760410 seconds |
| Entire supervised UAT, including hashes and modular checks | 932.043993 seconds |
| Native ingest CPU / peak RSS | 253.503950 CPU seconds / 2,789,490,688 bytes |
| Native query CPU / maximum per-process peak RSS | 628.516457 CPU seconds / 5,819,711,488 bytes |

Native clocks include process creation, complete output and exit. CPU work can
overlap across workers and is not elapsed wall time. The ingest-plus-query clock
includes the guards, query setup and per-call validation; it excludes initial
source hashing, later independent result readback, modular checks and final
artifact hashes. The entire UAT clock includes those stages but excludes evidence
packaging. No artificial wait, cooling interval or automatic retry was added.
Storage, exclusive-workload, timeout and owned-process cleanup guards remained
active. Watchdog sampling while a child is running is not an inter-query delay.

All 129 complete outputs were independently reopened and checked, covering
1,143 returned rows, exact schemas, resource declarations and native/no-fallback
certificates. Full43 uses retained native reference values as regression evidence;
it is not a fresh independent correctness oracle or an exhaustive all-cell ingest
comparison. The modular matrix uses an independent pandas oracle only for tests.
It covers eight raw/prepared inputs with collection and eight output formats
from CSV input, each across 22 workflows. This is not the full input/output
Cartesian product. Every native result preserves the external-execution boundary.

The host is macOS 27 arm64 with 10 logical CPUs and 16 GiB physical memory. The
native admission policy declares 24 GiB and maximum parallelism 12; these are
not physical-memory measurements or an enforced process-RSS ceiling. Resident
Parquet is prehashed, queries consume newly written Vortex, and OS caches and
ordinary desktop activity remain uncontrolled. Host load and process CPU/RSS
observations are retained without battery or thermal admission.

The release executable SHA-256 is
`fee87124c91b90f5f02ff1f9f00d9eaae7fd87724e152641a98bda2efb8725c9`.
It was built with `release-user-surfaces` from base `22b2cde0` plus the frozen
correction. All 2,245 selected source files exactly match the accepted `9dd328f5`
tree. The driver freezes those hashes and the existing documentation-only worktree
state, then checks them around each stage. The later one-pass harness change and
fixed decoder-test diagnostic are outside this immutable observation; neither
changes the production runtime measured here.

The subsequent harness update passes 41 runner/reference tests, including exact
43-call default coverage, explicit repeats, missing/duplicate calls and invalid
repeat counts. Formatting, workspace/native Clippy, all 3,219 workspace tests,
the focused native decoder exclusion test and four documentation/CI validators
also pass. Comparing all selected source hashes leaves only the four runner/test
files and the decoder test diagnostic changed after this observation.

PR #1551 subsequently merges at `b6d9318d42efb84275742fb3ff972d2adb350b41`
after all 44 hosted checks pass. The
[hosted receipt](evidence/native-decoder-hosted-2026-10-15.json) verifies all
2,245 selected source hashes against that merge, preserving the five-file
post-measurement boundary above and every production runtime source. Three
installed Linux/macOS wheel lanes each pass 80 native binding tests. These are
integration results, not timings for the subsequent 0.6.0 package.

The source Parquet is 14,779,976,446 bytes, SHA-256
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.
The fresh `unpaced-native-435-a1.vortex` is 15,713,610,513 bytes, SHA-256
`ceac4cf2cf8e0c8bc0e3da82fed82e4e24c8238c6f1a6768246d6d975c02bffc`.
Generation checks surround final hashing. All owned process groups drain and
the shared UAT lock is released. Prior inputs and published packages are retained.

The same executable previously recorded 333.392305 and 389.067850 seconds in
the [matched context comparison](native-admitted-context-2026-10-14.md). This
fresh 194.125941-second observation demonstrates a complete faster run of that
executable; it does not isolate which execution conditions caused the earlier
durations. The [historical investigation](native-performance-gap-2026-10-14.md)
preserves all prior measurements and its unresolved causal questions. The
maintainer authorized finishing the PR, making one version bump after integration
and resuming the remaining work. This observation does not close whole PERF/CG
gates or establish a comparative speedup.

The [portable evidence index](evidence/native-fresh-unpaced-uat-2026-10-14.json)
binds the [archive](evidence/native-fresh-unpaced-uat-2026-10-14.tar.xz) and
[member manifest](evidence/native-fresh-unpaced-uat-2026-10-14.manifest.json.xz).
All 13,345 members, representing 284,782,310 original bytes, were reopened and
verified byte-for-byte. A separate
[audit receipt](evidence/native-fresh-unpaced-uat-2026-10-14.verification.json)
confirms every member and the final coverage/cleanup records. The packet includes frozen drivers/helpers, source/build
identities, query/reference definitions, ingest/query outputs, modular fixtures
and reports, host observations and stage/outer cleanup receipts. Bulk inputs and
the executable are represented by verified identities. Replay requires binding
the recorded local paths to resident inputs and choosing new output names;
completed evidence paths are immutable.
