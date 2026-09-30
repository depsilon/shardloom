# Performance profile after the September 26 intake

All **29 intake experiments have a retain/drop decision**. The last retained
change reuses bounded dictionary preparation and native provider progress.
[R2.b's final acceptance](dictionary-preparation-screen-2026-09-30.md) passes
258 complete Full43 comparisons and six separately scoped Q35 follow-up calls.
The maintainer subsequently authorized PR cleanup, a new release train, then
a new finite ship/drop packet. The format pulse remains paused; broader PERF
obligations and CG-1 through CG-23 retain their own acceptance gates.

## Current query measurements

The final runtime's paired cohort is `paired43_20260930T090652016000Z`.
Summing each query's best of three complete calls gives **54.660584 seconds for
control and 53.171336 seconds for candidate**, a **2.72% reduction**. **23 of 43
queries have a best call below one second.** Q29 improves **29.67%**, from
6.862137 to 4.825826 seconds, saving 2.036311 seconds.

Preserve the negative observations: Q35 is 0.364757 seconds slower (13.75%),
Q34 is 0.166203 slower (6.56%), Q23 is 0.116176 slower (2.68%), and Q19 is
0.033426 slower (0.77%). A single reversed-order Q35 follow-up remains slower:
best 2.696266 → 2.813690 seconds (4.36%), median 2.824452 → 2.844280 seconds
(0.70%). Its smaller gap does not replace the original observations or establish
their cause. Retention accepts the material Q29 and overall reduction with this
remaining Q35 attribution target.

The [machine-readable inventory](../benchmarks/query-profile-2026-09-30-final.json)
contains all 43 queries, every candidate sample, observed RSS and scoped stage
timings. The [immutable evidence](../benchmarks/evidence/dictionary-preparation-admission-2026-09-30.json.xz)
retains all 264 complete comparisons, source manifests, validation logs, and the
separate interrupted metadata-admission run: 203 saved records and 204 exact raw
outputs before its log-storage guard stopped it. That partial run has no
complete-suite score. Earlier 270- and 258-comparison bundles and profiles remain
unchanged historical evidence. No cohorts are combined into one score.
References are retained regression oracles, not a new independent SQL oracle.

Each call includes native CLI startup, complete output and exit. The host is an
Apple M5 with 10 CPU cores and 16 GiB memory; OS cache is uncontrolled and unrelated
host concurrency is accepted. The requested 24 GiB policy is not a physical RSS
limit. Runtime is `93ee6b39fd09ce657adf078a711206a89ccb2ab9`; binary SHA-256 is
`e3351a1192b3de6de7a0db8db9afa7aa8a78d53b245336aee7c3b19eb01096d9`.
The optimized Vortex input has 99,997,497 rows, 112 columns and 15,682,956,116
bytes, SHA-256 `31cc61cfc347cf19a0328c196d59cd1eb431679311294cdc92263fef31062b35`.

The 20 remaining queries at or above one second are:

| Query | Best of three (s) | Median (s) | Observed peak RSS range (GiB) |
| --- | ---: | ---: | ---: |
| Q29 | 4.826 | 4.881 | 1.41–1.43 |
| Q23 | 4.444 | 4.474 | 2.01–2.16 |
| Q19 | 4.381 | 4.404 | 3.99–4.01 |
| Q6 | 3.256 | 3.287 | 1.08–1.08 |
| Q35 | 3.018 | 3.141 | 4.63–5.03 |
| Q34 | 2.700 | 2.759 | 4.54–5.04 |
| Q17 | 2.511 | 2.532 | 2.87–2.88 |
| Q28 | 2.279 | 2.311 | 0.39–0.39 |
| Q10 | 2.255 | 2.302 | 0.59–0.59 |
| Q16 | 2.180 | 2.195 | 2.14–2.15 |
| Q33 | 1.852 | 1.869 | 2.56–2.57 |
| Q26 | 1.691 | 1.699 | 0.24–0.24 |
| Q27 | 1.657 | 1.704 | 0.29–0.30 |
| Q15 | 1.465 | 1.474 | 1.49–1.56 |
| Q14 | 1.333 | 1.442 | 1.68–1.70 |
| Q12 | 1.312 | 1.324 | 0.28–0.28 |
| Q22 | 1.254 | 1.275 | 1.08–1.09 |
| Q5 | 1.250 | 1.261 | 1.70–1.71 |
| Q9 | 1.063 | 1.070 | 3.96–3.99 |
| Q32 | 1.027 | 1.028 | 2.03–2.28 |

## Attribution and reuse opportunities

These are measured stages to attribute, not exclusive CPU bottleneck diagnoses.
Worker elapsed spans overlap caller/provider work and cannot be added to infer
elapsed time or savings.

| Target | Current signal | Reuse boundary / next evidence needed |
| --- | --- | --- |
| Q23 | Provider span 4.205–4.625 s dominates the 4.222–4.646 s accessor span. | Split native filter, decompression and canonicalization work. The provider already propagates selections and orders conjuncts; [R6.c](progressive-provider-selection-audit-2026-09-30.md) drops a duplicate scanner. |
| Q29 | Dictionary worker 2.144–2.214 s overlaps caller provider work 2.412–2.429 s and ordered consumption 2.144–2.171 s. | Reuse the shared first-seen builder and transformed consumer. Measure remaining caller work before adding workers; more dictionary parallelism is not established by these spans. |
| Q19 | Dictionary construction 1.240–1.495 s, provider 0.878–0.884 s and binding 1.017–1.023 s. Routing/submission is 0.622–0.629 s; the earlier 5ea cohort's larger span is not repeated here. | Attribute remaining dictionary and binding work. Bounded preparation is a possible shared component, but triple-key ownership, CPU admission and exact merge need their own proof. [R4's sort replacement](triple-sort-screen-2026-09-30.md) was slower and stays dropped. |
| Q6 | Exact text DISTINCT takes 3.256 s; dictionary construction 1.153–1.174 s and provider 0.825–0.839 s. | Attribute global exact union before extending the dictionary worker. Existing preunion is already implemented and receives no duplicate credit. |
| Q28 | CounterID grouping with URL-length measures; caller updates 2.182–2.214 s. | Inspect the remaining measure accumulator work through shared bound accessors and kernels. Preserve complete HAVING and aggregate semantics. |
| Q34/Q35 and Q17 | Dense string-count pages now remove sparse payload capacity; Q34/Q35 still peak near 5 GiB, and Q17 takes 2.511 s. Q35's worker elapsed spans rise across canonicalization, counting, waiting and reconciliation in the slower cohort. | Reconcile actual live source, directory, arena and result ownership with reservations; attribute waiting/reconciliation separately. Do not reopen the inactive sketch/indexed-heap idea without an admitted workload. |

The modularization already retained in this cycle is concrete: source-backed
UTF8 dictionaries and borrowed reads; one mixed-DISTINCT partial builder for
serial and worker paths; ordered `AggregateChunkJobs` completion that retains
ownership and credits through consumption; shared stable `DensePages` and exact
allocation between compound and single-string counts; and an explicit shared
CPU grant for dictionary preparation and native provider progress. Ordinary and
serving file execution now also share the I/O and provider-reader drain contract,
fixing cancellation lifetime without adding another execution stack. Future work
should extend those contracts only when semantics and ownership match. A generic
worker/sink/codec framework is not justified by this packet.

## Other profiling surfaces

The September 30 run measures queries only. The following records retain their
original dates, runtimes and workload limits; none is a new ingest, serving or
format benchmark, and their clocks must not be added into a new combined total.

| Area | Latest relevant saved evidence | Remaining measurement or scope |
| --- | --- | --- |
| Ingest CPU and persisted storage | The [September 27 matched ThinLTO screen](compiler-profile-screen-2026-09-27.md) has best full-size ingest 87.708583 → 81.104577 s (7.53% lower), with byte-identical 15,682,956,116-byte artifacts. Separate ordinary-release acceptance is 96.662409 s; later unpaired Parquet observations in the [JSONL](jsonl-typed-builder-screen-2026-09-27.md) and [JSON](json-typed-builder-screen-2026-09-27.md) screens are 84.536593 and 94.978078 s. | No fresh ingest measurement in this query cycle. Attribute compression/statistics/conversion and per-column encode/decode/lifecycle cost before another structural candidate. Historical 95.923669 s and 18.59 GB refer to an older artifact. |
| Memory and allocation | This cohort records process RSS for every query. Q34/Q35 approach 5 GiB; Q19 and Q9 approach 4 GiB. R2.b bounds two retained chunks and reserves exact dictionary metadata plus native `nbytes` estimates. | Reconcile source/provider, arena, aggregate and result live allocations with reserved bytes. Reservations do not establish a process RSS ceiling. |
| Spill and recovery | [Native runtime completion](native-runtime-completion-2026-09-20.md), [owned-result composition](native-result-composition-2026-09-20.md), and [borrowed spill predecessors](spill-key-owner-screen-2026-09-27.md) retain exact spill, cancellation/retry/cleanup and ownership fixtures; the predecessor screen halves merge-head copies. | Broader compound/DISTINCT transitions, skew, storage faults and production-scale pressure remain separately scoped. R2.b deliberately excludes spill and does not broaden supported operator families. |
| Prepared and public calls | The [September 12 distribution](../benchmarks/native-completion-boundaries-2026-09-12.md) covers 2,232 calls on a 32-row fixture. Prepared integer extrema/AVG worker p50/p95 are 0.778/0.908 ms; Python is 1.191/1.274 ms. | Persistent transport and response handling are included; startup/import are excluded. Current tests refresh ordinary/prepared/owned correctness, not those latency distributions or all operator availability. |
| Concurrent serving | The [September 20 debug fixture](concurrent-native-serving-2026-09-20.md) schedules 96 calls on 8 clients with native writes. Normal arrivals complete 96/96 in serving mode versus 39/96 in exclusive mode; burst completes 45/96 versus 30/96. | Exact release identity is not pinned for those load receipts. Production p50/p95/p99, queue delay, write contention, cancellation and fairness still need their own evidence. |
| Result export and I/O | The [September 27 plain-Vortex retry](plain-vortex-format-comparison-2026-09-27.md) passes 172 collect/export cases and 129 complete readbacks. Totals are 104.727 s collect, 106.164 s Vortex, 110.733 s Parquet and 106.698 s Arrow IPC. | One sample per query/sink with uncontrolled cache and mixed historical comparison binaries; not a paired format-speed ranking. Large result copies/serialization remain measurable. The interrupted optimized-reference pulse and large CSV/JSON/JSONL tests stay paused. |

After release, rank the next finite list by potential avoided work and breadth,
including renewed exclusive ingest CPU/persistence attribution. Current query
signals are Q35's remaining slowdown, exclusive source work in Q23, caller work
in Q29, dictionary/binding/global-union work in Q19/Q6, and live memory in Q34/Q35. Those opportunities are separate from completing broader operator and
production-serving obligations. Reopening a dropped candidate requires new
evidence that changes its admission or mechanism; useful small gains remain
eligible. [Recorded artifact cleanup](local-artifact-cleanup-2026-09-30.md)
preserves evidence and active inputs while retiring superseded executables.
