# Performance profile after the September 26 intake

All **29 intake experiments have a retain/drop decision**. The last retained
change reuses bounded dictionary preparation and native provider progress;
[R2.b's full acceptance](dictionary-preparation-screen-2026-09-30.md) passes
258 complete Full43 comparisons. This refresh closes the finite packet and
records the next attribution opportunities; it does not start another campaign,
release train or format pulse. Broader PERF obligations and CG-1 through CG-23
retain their own acceptance gates.

## Current query measurements

The final paired cohort is `paired43_20260930T062939105987Z`. Summing each
query's best of three complete calls gives **54.016214 seconds for control and
51.649986 seconds for candidate**, a 4.38% reduction. **23 of 43 queries have
a best call below one second.** Q29 improves 30.38%, from 6.756227 to 4.703962
seconds. Preserve the negative observations: Q34 best is 0.125991 seconds
slower, Q35 is 0.007605 seconds slower, and Q37 is 0.010394 seconds slower.
Their full samples and medians remain in the acceptance record.

The [machine-readable inventory](../benchmarks/query-profile-2026-09-30.json)
contains all 43 queries, every candidate sample, observed RSS and scoped stage
timings. The [immutable evidence](../benchmarks/evidence/dictionary-preparation-2026-09-30.json.xz)
also retains every control call, complete outputs, source manifests and validation
logs. Independent audit verifies all 270 results across the two Q29 screens and
Full43, plus the inventory's 43 entries. Those are retained regression references,
not a new independent SQL oracle.

Each call includes native CLI startup, complete output and exit. The host is an
Apple M5 with 10 CPU cores and 16 GiB memory; OS cache is uncontrolled and unrelated
host concurrency is accepted. The requested 24 GiB policy is not a physical RSS
limit. Runtime `d726aaf6d041e8f87f82fe6bf17dcc4b002603a1` was rebased without
changing its source tree. Binary SHA-256 is
`af27c8018b16920a022f8ff2cf7b58e3f7b7c24241c799beae5760c3bab6e926`.
The optimized Vortex input has 99,997,497 rows, 112 columns and 15,682,956,116
bytes, SHA-256 `31cc61cfc347cf19a0328c196d59cd1eb431679311294cdc92263fef31062b35`.

The 20 remaining queries at or above one second are:

| Query | Best of three (s) | Median (s) | Observed peak RSS range (GiB) |
| --- | ---: | ---: | ---: |
| Q29 | 4.704 | 4.718 | 1.38–1.39 |
| Q19 | 4.381 | 4.389 | 3.98–4.02 |
| Q23 | 4.276 | 4.312 | 2.16–2.20 |
| Q6 | 3.260 | 3.273 | 1.07–1.09 |
| Q35 | 2.555 | 2.579 | 4.92–5.00 |
| Q34 | 2.554 | 2.605 | 4.55–5.04 |
| Q17 | 2.500 | 2.673 | 2.86–2.88 |
| Q28 | 2.242 | 2.270 | 0.39–0.39 |
| Q10 | 2.226 | 2.239 | 0.59–0.60 |
| Q16 | 2.152 | 2.173 | 2.14–2.15 |
| Q33 | 1.819 | 1.843 | 2.56–2.58 |
| Q26 | 1.678 | 1.687 | 0.24–0.24 |
| Q27 | 1.647 | 1.671 | 0.28–0.30 |
| Q14 | 1.346 | 1.454 | 1.68–1.69 |
| Q15 | 1.321 | 1.466 | 1.37–1.43 |
| Q12 | 1.295 | 1.322 | 0.28–0.28 |
| Q5 | 1.199 | 1.206 | 1.70–1.71 |
| Q22 | 1.194 | 1.195 | 1.00–1.01 |
| Q9 | 1.055 | 1.056 | 3.95–3.98 |
| Q32 | 1.010 | 1.013 | 2.00–2.28 |

## Attribution and reuse opportunities

These are measured stages to attribute, not exclusive CPU bottleneck diagnoses.
Worker elapsed spans overlap caller/provider work and cannot be added to infer
elapsed time or savings.

| Target | Current signal | Reuse boundary / next evidence needed |
| --- | --- | --- |
| Q23 | Provider span 4.063–4.113 s; dictionary construction only 2.7–3.0 ms. | First candidate for further attribution: split native filter, decompression and canonicalization work. The provider already propagates selections and orders conjuncts; [R6.c](progressive-provider-selection-audit-2026-09-30.md) drops a duplicate scanner. |
| Q29 | Dictionary worker 2.099–2.256 s overlaps caller provider work 2.359–2.390 s and ordered consumption 2.048–2.120 s. Caller join wait is only 4.8–7.1 ms. | Reuse the shared first-seen builder and transformed consumer. Measure remaining caller work before adding workers; more dictionary parallelism is not established by these spans. |
| Q19 | Dictionary construction 1.233–1.240 s, provider 0.872–0.879 s, binding 1.003–1.019 s and routing 0.622–0.625 s. | Bounded preparation is a possible shared component, but triple-key ownership, CPU admission and exact merge need their own proof. [R4's sort replacement](triple-sort-screen-2026-09-30.md) was slower and stays dropped. |
| Q6 | Exact text DISTINCT takes 3.260 s; dictionary construction 1.157–1.164 s and provider 0.832–0.837 s. | Attribute global exact union before extending the dictionary worker. Existing preunion is already implemented and receives no duplicate credit. |
| Q28 | CounterID grouping with URL-length measures; caller updates 2.145–2.204 s. | Inspect the remaining measure accumulator work through shared bound accessors and kernels. Preserve complete HAVING and aggregate semantics. |
| Q34/Q35 and Q17 | Dense string-count pages now remove sparse payload capacity; Q34/Q35 still peak near 5 GiB, and Q17 takes 2.500 s. | Reconcile actual live source, directory, arena and result ownership with reservations; attribute waiting/reconciliation separately. Do not reopen the inactive sketch/indexed-heap idea without an admitted workload. |

The modularization already retained in this cycle is concrete: source-backed
UTF8 dictionaries and borrowed reads; one mixed-DISTINCT partial builder for
serial and worker paths; ordered `AggregateChunkJobs` completion that retains
ownership and credits through consumption; shared stable `DensePages` and exact
allocation between compound and single-string counts; and an explicit shared
CPU grant for dictionary preparation and native provider progress. Future work
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

The next research list should start from exclusive source work in Q23, remaining
caller work in Q29, dictionary/global-union work in Q19/Q6, and live memory in
Q34/Q35. Those opportunities are separate from completing broader operator and
production-serving obligations. Reopening a dropped candidate requires new
evidence that changes its admission or mechanism; useful small gains remain
eligible. [Recorded artifact cleanup](local-artifact-cleanup-2026-09-30.md)
preserves evidence and active inputs while retiring superseded executables.
