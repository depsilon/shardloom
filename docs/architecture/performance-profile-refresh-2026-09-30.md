# Performance profile after the September 26 intake

All **29 intake experiments have a retain/drop decision**. The last retained
change reuses bounded dictionary preparation and native provider progress;
[R2.b's full acceptance](dictionary-preparation-screen-2026-09-30.md) passes
258 complete Full43 comparisons. This refresh closes the finite packet and
records the next attribution opportunities; it does not start another campaign,
release train or format pulse. Broader PERF obligations and CG-1 through CG-23
retain their own acceptance gates.

## Current query measurements

The corrected runtime's final paired cohort is `paired43_20260930T080424645879Z`.
This records `5ea34b11`; the later metadata-admission review correction still
requires its own frozen-runtime validation before these become final PR numbers.
Summing each query's best of three complete calls gives **53.998755 seconds for
control and 52.957013 seconds for candidate**, a 1.93% reduction. **23 of 43
queries have a best call below one second.** Q29 improves 28.62%, from 6.718184
to 4.795345 seconds. Preserve the negative observations: Q19 is 0.377876 seconds
slower (8.85%), Q15 is 0.132677 slower, Q17 is 0.103884 slower and Q34 is
0.099316 slower. All samples and medians remain in the acceptance record.

The [machine-readable inventory](../benchmarks/query-profile-2026-09-30-reviewed.json)
contains all 43 queries, every candidate sample, observed RSS and scoped stage
timings. The [immutable evidence](../benchmarks/evidence/dictionary-preparation-drain-2026-09-30.json.xz)
also retains every control call, complete outputs, source manifests and validation
logs. The earlier 270 comparisons and [original profile](../benchmarks/query-profile-2026-09-30.json)
remain historical evidence. The corrected cohort adds 258 complete comparisons;
the two cohorts are never combined into one score. References are retained
regression oracles, not a new independent SQL oracle.

Each call includes native CLI startup, complete output and exit. The host is an
Apple M5 with 10 CPU cores and 16 GiB memory; OS cache is uncontrolled and unrelated
host concurrency is accepted. The requested 24 GiB policy is not a physical RSS
limit. Runtime is `5ea34b1132e66523b849f5c6eb23c65b8b4d8f70`; the later
footer-observer correction is test-only. Binary SHA-256 is
`f9712816543a5309b2267dbf2e9e5e0e2fd8893d09d2bebef7cae3aed2793f80`.
The optimized Vortex input has 99,997,497 rows, 112 columns and 15,682,956,116
bytes, SHA-256 `31cc61cfc347cf19a0328c196d59cd1eb431679311294cdc92263fef31062b35`.

The 20 remaining queries at or above one second are:

| Query | Best of three (s) | Median (s) | Observed peak RSS range (GiB) |
| --- | ---: | ---: | ---: |
| Q29 | 4.795 | 4.830 | 1.42–1.43 |
| Q19 | 4.649 | 4.654 | 3.99–4.03 |
| Q23 | 4.332 | 4.335 | 2.13–2.30 |
| Q6 | 3.273 | 3.279 | 1.07–1.08 |
| Q34 | 2.851 | 2.907 | 4.37–5.00 |
| Q35 | 2.670 | 2.784 | 4.89–5.11 |
| Q17 | 2.601 | 2.606 | 2.86–2.89 |
| Q10 | 2.261 | 2.274 | 0.59–0.59 |
| Q28 | 2.247 | 2.254 | 0.39–0.39 |
| Q16 | 2.171 | 2.177 | 2.13–2.15 |
| Q33 | 1.806 | 1.818 | 2.56–2.57 |
| Q26 | 1.661 | 1.665 | 0.24–0.24 |
| Q27 | 1.649 | 1.654 | 0.28–0.29 |
| Q15 | 1.435 | 1.445 | 1.47–1.55 |
| Q14 | 1.430 | 1.444 | 1.68–1.71 |
| Q12 | 1.279 | 1.281 | 0.27–0.28 |
| Q22 | 1.231 | 1.259 | 1.07–1.11 |
| Q5 | 1.213 | 1.225 | 1.70–1.71 |
| Q9 | 1.070 | 1.072 | 3.94–4.02 |
| Q32 | 1.007 | 1.016 | 2.07–2.30 |

## Attribution and reuse opportunities

These are measured stages to attribute, not exclusive CPU bottleneck diagnoses.
Worker elapsed spans overlap caller/provider work and cannot be added to infer
elapsed time or savings.

| Target | Current signal | Reuse boundary / next evidence needed |
| --- | --- | --- |
| Q23 | Provider span 4.104–4.140 s; dictionary construction only 3.0–3.2 ms. | Split native filter, decompression and canonicalization work. The provider already propagates selections and orders conjuncts; [R6.c](progressive-provider-selection-audit-2026-09-30.md) drops a duplicate scanner. |
| Q29 | Dictionary worker 2.114–2.240 s overlaps caller provider work 2.345–2.379 s and ordered consumption 2.151–2.199 s. Caller join wait is only 4.8–9.5 ms. | Reuse the shared first-seen builder and transformed consumer. Measure remaining caller work before adding workers; more dictionary parallelism is not established by these spans. |
| Q19 | Dictionary construction 1.217–1.226 s, provider 0.851–0.864 s and binding 0.990–1.014 s. Routing/submission rises to 0.947–0.981 s versus matched control 0.607–0.614 s. | Attribute this observed slowdown first; timing alone does not establish its cause. Bounded preparation is a possible shared component, but triple-key ownership, CPU admission and exact merge need their own proof. [R4's sort replacement](triple-sort-screen-2026-09-30.md) was slower and stays dropped. |
| Q6 | Exact text DISTINCT takes 3.273 s; dictionary construction 1.162–1.168 s and provider 0.831–0.839 s. | Attribute global exact union before extending the dictionary worker. Existing preunion is already implemented and receives no duplicate credit. |
| Q28 | CounterID grouping with URL-length measures; caller updates 2.150–2.160 s. | Inspect the remaining measure accumulator work through shared bound accessors and kernels. Preserve complete HAVING and aggregate semantics. |
| Q34/Q35 and Q17 | Dense string-count pages now remove sparse payload capacity; Q34/Q35 still peak near 5 GiB, and Q17 takes 2.601 s. | Reconcile actual live source, directory, arena and result ownership with reservations; attribute waiting/reconciliation separately. Do not reopen the inactive sketch/indexed-heap idea without an admitted workload. |

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

The next research list should start from Q19's increased routing/submission span,
exclusive source work in Q23, remaining
caller work in Q29, dictionary/global-union work in Q19/Q6, and live memory in
Q34/Q35. Those opportunities are separate from completing broader operator and
production-serving obligations. Reopening a dropped candidate requires new
evidence that changes its admission or mechanism; useful small gains remain
eligible. [Recorded artifact cleanup](local-artifact-cleanup-2026-09-30.md)
preserves evidence and active inputs while retiring superseded executables.
