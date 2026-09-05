# ShardLoom: material performance overhaul

**Decision document and implementation backlog — 4 September 2026**  
**Repository:** `depsilon/shardloom`  
**Audited snapshot:** `af9d96af9cb370e521e22b9b675f1a324da52cb1`

## 1. Executive decision

Make ShardLoom a resident, representation-aware compute runtime, rather than optimizing individual command routes indefinitely. Keep one native semantic implementation, with two execution policies: an inline, bounded-work latency path and a parallel, streaming throughput path. Make ingestion, memory management, scheduling, encoded operators, and result delivery share the same resource and ownership contracts.

The most consequential work is: (1) a real resident typed execution API; (2) persistent, byte-budgeted scheduling with parallel state reduction; (3) generalized encoded-key aggregation that remains exact under a small memory budget; (4) a lower-amplification ingestion pipeline; and (5) a physical execution representation that carries actual arrays and ownership, not just descriptions of encoded data.

Sub-millisecond performance should be a measurable target for explicitly bounded resident operations. It is not a defensible completion guarantee for arbitrary-size fresh ingestion, global computation, durable output, or remote delivery. The objective is to remove avoidable work, then approach the measured bandwidth and compute limits of each workload—not rename an acknowledgment or lazy handle “end to end.”

**Audit boundary.** This is a static review of selected current source files, implementation ledgers, and checked-in benchmark records, supplemented by upstream documentation. I did not build or execute ShardLoom or rerun ClickBench. The connector could not return the oversized `local_primitives.rs` as a complete source file; conclusions about its latest kernels use the current implementation ledger and available change evidence. Code findings below are not measured attribution of every query's wall time. All future performance numbers are proposed acceptance targets, not predictions or achieved results.

## 2. The evidence baseline

| Observation | Evidence | Interpretation |
|---|---|---|
| Latest documented full-43 best-of-three total: 145.12991075002356 seconds; unshifted query geomean 1.0210800800967954 seconds | September 4 implementation record [S2] | Newest reported query evidence found; not an official ClickBench ranking |
| Q34 16.828901875007432 seconds and Q35 16.949815000058152 seconds | Same full-43 record [S2] | Together 23.27% of that summed query-time budget |
| Checked-in clean-ingest UAT: 271 seconds ingest, 189.196504 seconds sum of best query runs | September 3 JSON [S3] | Older than the latest kernel improvement; do not substitute its query total for the latest record |
| Input 14,779,976,446 bytes; output 38,147,848,068 bytes | Same JSON [S3] | 2.581× final-file expansion relative to compressed Parquet; investigate per-column causes, not just total ratio |
| Q01 best time 17.144 milliseconds | Same JSON [S3] | Establish public-call overhead and open/metadata costs independently of actual count work |
| 129 query invocations total 572.658241 seconds; ingestion plus all invocations 843.658241 seconds | Same JSON [S3] | Distinct from the synthetic load-plus-best-runs sum of 460.196504 seconds |
| Shared morsel execution creates threads per execution, preassigns work round-robin, and serially merges states | `scheduler_bridge.rs`, approximately lines 1200–1390 [S4] | Concrete redesign target; inspect actual production callers before attributing all current timings to it |
| Queue enforcement indicator is a policy comparison; per-work-item memory admission is not a complete live-memory reservation | Same file, approximately lines 800–950 [S4] | Replace declarative resource claims with enforcement and measured occupancy |
| Persistent Python worker parses JSON argument arrays and calls CLI dispatch repeatedly | CLI `main.rs`, approximately lines 162–265 [S5] | Process reuse is useful, but a typed resident session removes a further layer |
| Latest count-only string histogram route already eliminates a broad second pass under an admitted memory envelope | September 4 implementation record [S2] | Do not propose “add a first-pass exact histogram” as new work |
| That histogram route needs at least 16 GB configured memory, while normal local defaults are 4 GB and two workers | [S1], [S2] | Smaller-memory execution is a first-class acceptance environment |

The September 3 stage fields report approximately 1.283 seconds of source decode, 0.396 seconds of Arrow-to-Vortex conversion, 110.037 seconds of derived metadata, 100.702 seconds of compression, and 130.036 seconds of encode/write. These are overlapping or nested measurements, not additive exclusive wall-time components. They justify better attribution, not a claim that removing one field saves its full duration. [S3]

The effective compressed-source ingestion rate is approximately 54.54 MB/s. That is **not** a measurement of disk bandwidth: decoded byte volume, compression, metadata generation, and queue stalls can dominate it.

A useful prioritization bound: even eliminating Q34/Q35 entirely would improve the latest summed query total by only about 1.30×. Likewise, in the older synthetic load-plus-best-query budget, ingestion accounts for 58.89%. A serious overhaul needs both broad operator coverage and ingestion improvement.

### A verified external reference, not a ranking claim

The September 4 ClickHouse entry for `c6a.4xlarge`, single-node CPU, untuned, records 244 seconds load, 15,258,856,189 bytes stored, and Q34/Q35 hot minima of 1.863/1.940 seconds. Summing its 43 hot minima gives 17.641 seconds. [S10]

Those are useful reference points, not a valid speed ratio against Dylan's Desktop UAT: hardware, storage, runtime lifecycle, input choice, and measurement controls differ. I did not reconstruct the full live filtered leaderboard. Establish the actual leader from a pinned, eligible comparison cohort when running the benchmark; do not call the above entry “number one.”

## 3. Define the latency promise before implementing it

For a fresh 14.78 GB input to be physically read in one millisecond, input bandwidth alone would have to exceed 14.78 TB/s. Persisting the existing 38.15 GB output in that interval would require 38.15 TB/s of write bandwidth, before normalization, compression, or durability barriers. These are arithmetic requirements from the observed file sizes, not hardware specifications.

Similarly, reading 100 million eight-byte values means moving 0.8 GB. Even at a hypothetical sustained 200 GB/s, that is four milliseconds of data movement. Compression and valid metadata can remove some of that work for particular queries; arbitrary exact computation cannot assume those shortcuts.

Use the following **proposed, unvalidated targets** on declared hardware and at a declared offered load:

| Work class | Completion boundary | Initial objective |
|---|---|---|
| Resident prepared count or similarly bounded metadata operation | Native call entry to completed scalar result | p99 below 100 microseconds |
| Resident tiny supported query | Public local Python call to requested small result | p99 below one millisecond |
| Small typed batch: initially at most 64 KiB / 4,096 rows, bounded simple transform, at most 4 KiB returned | Ingest API entry through validation, visible snapshot publication, query and actual result | p99 below one millisecond, memory-visible mode only |
| New SQL text | Parse, bind, admission, execute and return | Report separately from prepared execution; do not hide compilation or complex planning |
| Bulk ingestion | First source access through complete valid durable artifact and required checks | Throughput, total elapsed time, peak memory, write amplification |
| Large exact scan/join/aggregate | Client request through complete requested result | Work-normalized throughput and latency; no constant sub-ms promise |
| Durable streaming commit | Request through the declared durability boundary | Separate measured SLO; never substitute memory visibility or enqueue acknowledgment |

The small-batch envelope is a starting experiment, not a universal size threshold. Test different schemas, null densities and variable-length payloads. Include queue dwell and batch formation where they fall inside the actual product operation. Publish latency under mixed load as well as isolated latency.

Maintain distinct states: `accepted`, `validated`, `visible_in_memory`, `durable`, and `optimized`. These names are a proposed API model, not current capabilities. An optimized-layout task may be deferred only with an explicit state and cost; full ingest cannot be declared complete while validation or required durable data remains unfinished.

## 4. Architecture to build

The following are proposed architectural responsibilities, not claims that modules with these exact names already exist:

```text
Python / SQL / DataFrame / Rust / CLI
                |
       typed logical plan and admission
                |
      resident Engine + Session + Snapshot
                |
      representation-aware physical plan
         /                         \
 inline bounded-work path       throughput pipelines
         \                         /
        shared native kernels and ownership model
                |
      columnar results / explicit output adapters

Sources -> incremental normalization -> Vortex arrays
                         |                |
                 visible snapshot    single-file durable writer
```

A persistent Vortex artifact remains the durable product output where requested. A source-backed in-memory Vortex array path should serve transient computations without an obligatory write/reopen cycle. Vortex arrays are already an in-memory representation, distinct from the file format. [S11]

This is also the technically useful version of a “shell Vortex” idea: an **in-memory prepared execution capsule** containing schema, immutable source identity, split catalog, bound kernels, memory credits and reusable runtime handles. Do not represent missing data as an empty valid file or finalize a placeholder and overwrite it later.

### 4.1 Resident typed execution

Extract reusable runtime entry points from command handlers. CLI commands and existing Python transport become adapters to the same implementation. Keep open-reader metadata, source handles, reusable codec context, prepared plans, thread pools, and bounded scratch arenas resident where beneficial. Avoid repeated discovery, argument construction, JSON parsing, CLI dispatch, and equivalent admission work on a prepared call.

This is a continuation of existing persistence and source-state work, not its replacement by another engine. The current source-state coverage matrix explicitly scopes reuse to a prepared/native batch lane and includes preassembled result payloads. Such near-result reuse must not be silently promoted into uncached leaderboard timing. [S6]

Cache keys must include source generation/snapshot, schema and semantic settings, parameters where relevant, and tenant/security context where applicable. An immutable open handle plus a validated generation is stronger than treating a pathname or size/mtime pair as a complete data-integrity guarantee. Invalidation tests must replace, mutate, truncate and recreate sources while prepared handles exist.

For local Python, prefer a thin native binding and columnar result ownership over command-string marshalling. Evaluate an audited safe binding/provider first. The workspace currently forbids unsafe code, so custom FFI and hand-written SIMD require a conscious, narrowly scoped design decision rather than silently relaxing the policy. [S7]

Arrow's C data interface is useful for compatible same-process arrays with explicit lifetime management; it is not a cross-process pointer transport. A retained subprocess mode needs actual IPC or properly owned shared memory. Exporting a Vortex encoding into canonical Arrow may still require conversion, so “C interface” does not by itself imply zero decompression. [S12]

### 4.2 A real physical result representation

The inspected `VortexColumnarResultColumnStorage` includes scalar vectors, row-reference vectors, and an opaque-encoded variant containing descriptive metadata rather than an owned array payload. That particular representation is not sufficient, on its own, to establish an array-owning zero-copy execution path. Other operator storage must be checked before broader conclusions. [S8]

The production data path should carry actual Vortex array handles, compatible Arrow buffers when necessary, dictionary domains, validity, selections, row references, and owners that keep source buffers alive. Report descriptors remain useful, but separate them from executable payloads. Materialize strings or rows only at a consuming operator or requested output boundary.

Instrument allocation count, copied bytes, decoded bytes, and buffer reuse. A zero-copy claim must name the exact boundary and applicable types. Ref-count traffic and tiny-array allocation can still be expensive even without copying payload bytes.

### 4.3 Move proof formatting, not safety, out of the critical path

Keep mandatory schema validation, exactness checks, resource admission, source identity, bounds checks, cancellation, and durability requirements. Replace runtime construction of rich diagnostic strings and large nested evidence maps with typed counters and compact event records. Format optional evidence after computation or on explicit inspection, without hiding any required work from public timing.

Bound diagnostic output independently of the user's actual result. A rejected exact-mirror experiment exhausted disk while writing JSON evidence; that was a reverted experiment, not a claim about current normal behavior. Full histograms should not be dumped by default to prove a counter. [S2]

Do not expect removing a Boolean condition to eliminate the bulk of a 100-million-row workload. Prioritize repeated I/O, allocation, formatting, duplicate traversal, and unproductive data conversion if profiles show them.

## 5. Replace scheduling policy reports with resource-controlled execution

The inspected scheduler's actual implementation spawns OS threads per invocation, assigns all runnable work round-robin, and reduces worker states serially. Its queue-wave and queue-enforced fields are derived policy values, not a bounded producer-consumer queue in that function. [S4]

Build one persistent compute scheduler under a unified resource governor. It should have an inline path for tiny work, dynamically assigned morsels for large work, real byte-budgeted queues, backpressure, cancellation, and parallel partition reduction. Wire actual operator state into it; do not count metadata-only `observe_morsel` activity as proof that expensive operator kernels ran in parallel.

The live reservation must include compressed inputs, decoded arrays, operator state, partition buffers, merge state, output, and retained source buffers. A per-morsel estimate check does not constrain a growing hash table. Reserve before allocation, reconcile estimates against actual growth, and reduce concurrency or spill before exhausting the budget.

Use an adaptive work target based on bytes and measured kernel cost, not one universal row count. Small and wide rows differ; string dictionary density and selectivity also matter. Choose morsels large enough to amortize dispatch but short enough to avoid excessive skew and to allow I/O/cancellation progress. Test an initial size sweep; do not encode an arbitrary magic constant as the universal answer.

Integrate the Vortex runtime deliberately. Its documented current-thread runtime can stop polling I/O while a calling thread runs CPU-heavy work; optional background workers mitigate this but change the thread budget. This is a specific reason to measure I/O starvation and runnable workers, not simply add more threads. [S13]

Choose either a controlled I/O progress service or bounded cooperative progress that preserves I/O liveness, and account for writer/compression workers in the same quota. A configured count on each of several pools is not the machine-wide CPU limit.

### Parallel reduction and determinism

For associative exact integer state, partition by key and combine independent partitions concurrently. Do not serially funnel the entire high-cardinality state into a single final accumulator. Handle skew with over-partitioning and dynamic ownership rather than endlessly subdividing every task.

Floating-point behavior requires an explicit decision. Worker-index order is not enough for reproducibility when scheduling changes which rows belong to which worker. Use stable logical partitions and deterministic reduction independent of worker count, or an exact/reproducible accumulation method where required. Compensated summation alone does not guarantee bitwise-identical results. Preserve existing semantic and byte-stability contracts unless an API change is explicitly approved.

The implementation record still identifies major families requiring shared-scheduler conversion, including Q10/Q14/Q17/Q19/Q33/Q34/Q35. Finish the production wiring, then delete superseded route-local schedulers after parity and performance pass. [S2]

## 6. Generalize encoded aggregation beyond the latest URL optimization

### What is already done

The latest count-only string top-K path builds exact dictionary-weighted counts, binds dictionary values to stable string IDs, promotes the histogram into exact top-K finalization, and avoids a broad second pass. It still updates the older heavy-hitter sketch so it can revert to the retained recount algorithm when the histogram budget is exceeded. The public admission requires a large source, predicate-free count-only shape, and at least 16 GB configured memory. [S2]

The opportunity now is not another branch named for Q34. It is a shared encoded aggregation engine supporting strings, numeric-plus-string composites, exact distinct and related operator shapes at different memory envelopes.

### Proposed algorithm family

Within each active partition, bind a chunk dictionary once. Evaluate dictionary-dependent expressions once per referenced dictionary entry. Accumulate typed code-indexed state when the dictionary is small; use a compact partitioned hash representation when it is not. Combine weighted counts or other valid mergeable state without repeatedly materializing strings per row.

Dictionary IDs are meaningful only within their domain. Remap across chunk dictionaries or use explicit domain identity plus collision-checked canonical key binding. Hash equality is not string equality. Do not treat unreferenced dictionary entries as observed distinct values; preserve null-specific COUNT and DISTINCT semantics.

Partition state before it becomes a huge global interner. Keep long strings in lifetime-owned slabs or referenced immutable buffers; keep compact IDs in hot state. Materialize final strings only when comparison semantics or final output require them. Composite keys must preserve signedness, null tags, numeric width and string identity without assuming that the Cartesian product of dictionary domains is small.

### Eliminate redundant contingency work safely

Measure the cost of updating the heavy-hitter sketch alongside the promoted exact histogram. A worthwhile alternative is a single exact path that transitions, under byte pressure, to partitioned exact runs and bounded partition reduction. That removes dual maintenance without requiring approximate results. This is conditional on a tested native spill contract and legal temporary-workspace use; disabling the sketch without a correct overflow transition is not acceptable.

Where exact in-memory state is predictably too large, select sketch-plus-verified-recount or external exact partitioning up front using measured cardinality and memory estimates. A sample may inform resource policy, but never establish that unseen groups cannot matter. The final result remains exact.

For aggregate top-K, merging each worker's local top-K is not generally exact: a globally winning group may be only moderately frequent on every worker. Use complete partition counts or a sound upper-bound proof for excluded groups, including ties. In contrast, ordinary row top-K under a shared total order can merge local row top-K sets safely; keep these algorithms distinct.

### Do not repeat rejected experiments unchanged

The implementation ledger already documents regressions from per-eviction reclaimable string-arena work, sparse dictionary candidate binding, certain count-only proof-selection checks, and Q17 signature/compact-partition approaches. Q35 constant-key pruning and Q17 candidate-partition reuse are also already present. [S2]

If retained-string memory remains material after the latest histogram change, test bulk epoch/slab reclamation or a different ownership representation. Reclamation should happen at bounded checkpoints, not necessarily on every candidate eviction. Require both live and peak resident memory measurements plus end-to-end latency; freeing bytes is not automatically faster.

## 7. Rebuild ingestion around the actual critical path

### 7.1 Keep two honest use cases

For durable ingestion, produce and validate the complete requested single-file artifact. For transient compute, permit source normalization into native in-memory Vortex arrays followed directly by computation. Do not bill schema registration, a footer lookup, or a query over a projection as completion of a full-source ingest.

For the durable path, target:

```text
source reads -> decode / validation -> Vortex array ownership
             -> useful metadata and codec selection
             -> parallel compression / encode tasks
             -> bounded ordered write -> final valid commit
```

Avoid row-wise reconstruction and full intermediate datasets where a compatible array can be consumed directly. Preserve Parquet dictionary structure where reader APIs and semantic conversion support it; verify this with actual encoding inventories and byte counters rather than a broad “zero-copy Parquet” claim.

### 7.2 Profile metadata as computation

The derived-metadata stage is large enough to deserve exclusive timing and allocation profiles. Identify exactly which expressions, indexes, summaries, inventory operations and serialization steps it performs. Retain mandatory file statistics, correctness metadata and broadly useful physical information. Remove duplication; make optional expensive layout work earn its cost over realistic lifecycle workloads.

Deferring metadata to the first query does not automatically improve ingest-plus-query time. Measure at least one-query, five-query, full-suite and repeated-use cases. Never omit required validation from an ingest-success boundary.

Do not build query-answer tables, aggregate summaries or materialized views under a “metadata” name. The current ingestion contract requires one final `.vortex` artifact and disallows answer sidecars and precomputed aggregate summaries. Query-local histograms created inside timed execution are different from persisted answers. Any relaxation of these product rules is an explicit RFC and a separate benchmark class. [S9]

### 7.3 Reduce compression and write amplification jointly

Attribute stored bytes, encode CPU, decode CPU and query read bytes by column and encoding. Investigate duplicated representations, derived columns, dictionary reconstruction and unnecessary text expansion. The 2.581× ratio is a diagnostic, not proof that Vortex intrinsically compresses worse than Parquet or that compression should be disabled.

The ledger already records a rejected narrower-compression experiment that exceeded an 80 GB artifact guard. Do not repeat “compress fewer strings” blindly. [S9]

Benchmark available Vortex encodings and compression settings against actual data distributions, including dictionary/run-end/bit-packed forms, FSST for suitable strings, and low-level Zstd choices. Upstream supports these encoding families, but the best policy depends on the consuming kernels and whether they can stay encoded. [S11]

Optimize a lifecycle objective such as:

`acquisition + ingestion + sum(query costs) + requested export + required maintenance`

under memory and storage constraints. Compression must pay for itself in fewer bytes moved or lower retained memory, not merely a smaller file at any CPU cost.

### 7.4 Bound work while preserving one-file publication

Separate compression concurrency from ordered output. Permit completed blocks to wait in a bounded byte-limited reorder buffer, apply backpressure before retaining too many arrays, and diagnose head-of-line blocking. Validate contiguous offsets, dictionary ownership and footer references in the assembled file.

Preserve existing streamed-digest and source-identity optimizations rather than reintroducing full rereads. The current physical-segment inventory must be interpreted carefully: tens of thousands of physical column/encoding segments are not necessarily tens of thousands of tiny row groups. [S3], [S9]

Measure resident-source ingestion separately from acquisition/hydration of a cloud-backed placeholder. Both are useful, but they answer different questions. Keep source preflight and atomic replacement protections. A target must not be destroyed before source availability and safe staging are established. [S9]

For durable completion, test the exact platform-specific flush, close, rename and directory-persistence contract with crash injection. No sub-ms claim may rely on ignoring that boundary. A continuously queryable append/journal mode requires its own publication and recovery design; do not assume the current finalized-file API already provides it.

## 8. Generic optimization—not 43 special-case routes

Use a typed physical intermediate representation whose nodes explicitly state input encoding, output form, validity and selection semantics, ordering, partitioning, memory ownership, and required execution guarantees.

Dispatch at plan or batch boundaries, not through expensive dynamic scalar logic for every row. Share fused filter/project/aggregate kernels. Preserve constant and run representations when the operator can exploit them; selectively canonicalize only when required by a native operator.

A native canonical-array kernel is not an external-engine fallback. Vortex itself has encoding-specific execution and canonical representations. Do not let “encoded-native” marketing become a reason to reject otherwise valid native computation, or claim encoded execution after the data was fully expanded. [S11]

Build and measure precompiled typed kernels first. Only then consider tiered compilation: an immediate vectorized path, with compilation of a sufficiently hot/expensive pipeline when anticipated savings exceed compilation cost. Cache code using semantic plan identity and CPU features. The workspace already has LTO and PGO-oriented profiles; merely adding another profile is not a performance result. [S7]

A PGO corpus must include held-out schemas and distributions, not only the 43 target queries. CPU-specific builds need a portable baseline and explicit dispatch/packaging rules. Do not lead with custom JIT, unsafe intrinsics, GPU offload or distributed fan-out before byte movement and single-node execution are efficient. A later accelerator path must include launch, transfer and output costs and use native kernels rather than another engine as a hidden implementation.

For universal execution, add costed choices for scan/filter, hash versus sort aggregation, exact distinct, hash/merge joins, top-K versus sort, and blocking versus streaming windows. Use representation width, cardinality, selectivity, ordering and skew as inputs. Restrict optimizer rewrites involving volatile functions, side effects or non-equivalent floating semantics.

## 9. Benchmark and correctness design

### Three scorecards

1. **Official ClickBench-compatible results.** Pin the repository and configurations, preserve full query semantics and outputs, and apply its current rules and score calculation.
2. **Product latency.** Prepared and unprepared native/Python calls, tiny batch ingest-to-visible results, cold initialization, memory pressure and mixed-load tail latency.
3. **Universal lifecycle efficiency.** Acquisition, normalization, durable ingestion, queries, exports, memory and storage across formats and workload families.

ClickBench hot timing uses the smaller of runs two and three; its relative query score applies a 10 ms additive shift before geometric aggregation. Its combined weights are 60% hot runtime, 20% cold runtime, 10% load and 10% size. [S14] The shift makes a 1 ms to 0.1 ms improvement only about a 1.089× per-query score change, rather than 10×. Optimize for product latency independently rather than expecting the leaderboard to reward it fully.

Select the fastest eligible entries in a fixed hardware/deployment cohort, then reproduce them on the same host configuration. Keep GPU, clusters, tuned setups and differing durability semantics distinct. Include ClickHouse as a verified reference and retain Polars/DataFusion as explicit comparison targets for the broader project goal, not runtime dependencies.

Keep caches of valid source data distinct from answer reuse. The normal ranking lane must exclude result caching and near-result preassembled state. A resident product session may still use legitimate open-file, source-buffer and metadata reuse. Measure true cold initialization independently, including any newly moved preparation cost. [S6], [S14]

### Minimal measurement record

Each run should identify commit, build profile, toolchain, enabled features, CPU/ISA, operating system, memory quota, storage, requested workers, actual active workers, input digest/snapshot, schema, source residency, query parameters, output format, cache controls and durability mode.

Record monotonic client wall time, engine wall time and exclusive stage spans separately from summed CPU work. Track source/decoded/encoded/output bytes; copies and allocations; compressed/decoded cache hits; state entries and bytes; queue occupancy and waits; scheduler busy/idle/I/O-starved time; spill and merge bytes; final result rows and error status.

Use the official required repetitions for a publishable submission, plus paired repeated experiments for engineering retention decisions. High-rate small-query tests need enough samples for p99 and an open-loop offered-load test so queuing is not hidden by coordinated omission. Randomize baseline/candidate order, monitor thermal or storage drift, and report confidence intervals or uncertainty rather than relying on one best run.

### Workloads that stop ClickBench overfitting

| Family | Required cases |
|---|---|
| Ingestion | Tiny and large inputs; CSV, JSONL, Parquet, Arrow and Vortex; dictionary and direct strings; nested data; invalid input; cold and resident sources; durable outputs |
| Aggregation | Low to almost-unique cardinality; skew and uniform distributions; long UTF-8; nulls; integer overflow; floating reproducibility; exact distinct; compound keys |
| Relational | Multi-table joins; many-to-many fan-out; skewed keys; filters on either side; sort; windows; nested projection; decimals and timezone semantics |
| Resource bounds | Two workers / 4 GB as well as the 12-worker / 24 GB benchmark configuration; throttled CPU; memory pressure; spill; cancellation; repeated-session leaks |
| Serving | Small queries while ingestion or a large aggregate runs; long sessions; cache eviction; snapshot replacement; bounded output backpressure |
| Universality | Renamed/reordered columns, changed literals, non-ClickBench datasets and unfamiliar operator compositions; selected standardized join/decision-support queries without claiming certified benchmark status |

Use reference engines only in tests as differential oracles. Production execution remains native. Check correctness independently of performance; output success and no-fallback flags are not, by themselves, evidence that all values are correct.

## 10. Ordered implementation backlog

All target figures below are **acceptance objectives to calibrate after baseline runs**, not forecasts. New module names are proposals. Reuse or extract current components instead of adding a parallel permanent architecture.

### PERF-01 — Reconcile and instrument the public baseline

**Priority:** P0. **Dependencies:** none.  
**Existing anchors:** `docs/benchmarks/clickbench-100m-current-branch-uat.json`, ClickBench scripts, public Python/CLI route.  
**Implementation:** regenerate one coherent same-commit clean-ingest plus query record; keep latest kernel-only evidence separately; instrument exclusive spans, bytes, actual worker work, memory and result delivery. Add a tiny-operation latency ladder: native kernel, resident native API, existing worker, public Python, fresh process.

**Acceptance:** all 43 results checked; no cross-run synthetic metric labeled measured elapsed time; output and required checks included; a profile identifies the largest exclusive costs in each expensive family and in ingest. A baseline report, not another admission schema, is the deliverable.

### PERF-02 — Resident typed session and prepared execution

**Priority:** P0. **Depends on:** PERF-01.  
**Existing anchors:** `shardloom-cli/src/main.rs::handle_python_worker`, Python `client.py`, `session.py`, `context.py`, current runtime entry points.  
**Implementation:** extract typed native entry points and snapshot-bound prepared handles; reuse source metadata and runtime resources; route CLI through the same API. Prototype Python binding under the existing safety policy.

**Acceptance:** identical results and public errors; prepared native metadata result p99 target below 100 microseconds; local Python bounded result target below one millisecond; no request-time subprocess/thread creation or repeated CLI parsing on the new resident path; cold preparation cost reported separately.

### PERF-03 — Enforce bounded scheduling and shared CPU budget

**Priority:** P0. **Depends on:** PERF-01; integrates with PERF-02.  
**Existing anchor:** `scheduler_bridge.rs::execute_vortex_morsel_scheduler_with_observer` and its policy/report methods.  
**Implementation:** persistent workers, inline tiny work, dynamic morsels, real byte credits, bounded ready/reorder queues, cancellation/draining, and I/O progress under the full engine quota.

**Acceptance:** observed live queued bytes remain within the configured bound, adversarial allocations cannot exceed the admitted budget without a declared error/spill policy, no per-query OS-thread creation after initialization, no I/O starvation in a mixed CPU/I/O test. A queue-enforced flag must be backed by an enforcement test.

### PERF-04 — Wire real aggregate kernels and parallel merges

**Priority:** P0. **Depends on:** PERF-03.  
**Existing anchors:** production `local_primitives.rs` families and shared scheduler state interfaces.  
**Implementation:** migrate the documented incomplete families, use logical partitions and parallel reductions, and retire redundant route-local scheduling.

**Acceptance:** real operator work—not only descriptor visits—runs on the requested admitted pool; correctness at 1/2/4/8/12 workers; skew tests; deterministic semantic contract maintained; target at least 2× improvement for demonstrably CPU-bound multiworker families, with any bandwidth-bound exceptions explained by measurements.

### PERF-05 — Partitioned exact dictionary aggregation

**Priority:** P0. **Depends on:** PERF-01, PERF-03; production generalization follows PERF-04.  
**Existing anchors:** count-only string histogram, numeric/UTF-8 grouping, interner and top-K finalizer in `local_primitives.rs`.  
**Implementation:** domain-safe code binding, compact state, per-partition key ownership and late string materialization. First profile whether duplicate sketch maintenance is material. Implement a correct memory-pressure transition before removing it.

**Acceptance:** a stretch target of another 2× reduction in the latest Q34/Q35 pair; improvement in at least one non-URL string/composite family; correctness on distinct-every-row and adversarial skew; full observed memory bounded; no query-text/column-name dependency. Failure to reach the target is diagnostic, not permission to weaken exactness.

### PERF-06 — Shared native spill and memory-pressure transition

**Priority:** P1, but prerequisite to disabling contingency work when exact state can overflow.  
**Depends on:** PERF-03 and an explicit temporary-workspace contract review.  
**Implementation:** memory-accounted partition runs and merge, relocatable key/buffer ownership, cleanup and crash/cancellation behavior. Reuse across aggregation, exact distinct, sort and joins.

**Acceptance:** exact completion at the normal 4 GB envelope for supported large-state cases; no hidden external engine; bounded temporary storage; atomic published output unaffected by failed spill. If current one-file/workspace policy excludes these temporary runs, obtain an explicit RFC decision before implementation.

### PERF-07 — Separate report descriptors from array payloads

**Priority:** P0/P1. **Depends on:** PERF-02.  
**Existing anchors:** `columnar_result_dataplane.rs`, native result/sink boundaries, Python result delivery.  
**Implementation:** real typed array/buffer handles and ownership in the execution path; keep summaries separate; add columnar delivery where requested.

**Acceptance:** no unnecessary scalar-vector or row-dictionary intermediate on admitted columnar paths; buffer lifetime and cancellation tests; measured reduction in copied/decoded bytes; complete requested output, not a lazy handle mislabeled completed output.

### PERF-08 — Ingest exclusive profiling and duplicate-work removal

**Priority:** P0, parallel workstream. **Depends on:** PERF-01.  
**Existing anchors:** public prepare/load route, ingest UAT script and optimization ledger.  
**Implementation:** attribute metadata, codec and writer spans exclusively; inventory derived representations; remove duplicate traversal/conversion and unnecessary reopen work; preserve validation and source preflight.

**Acceptance:** coherent same-source durable-ingest result; first milestone target at most 135 seconds versus a revalidated approximately 271-second baseline on the same host; no artifact explosion, omitted validation or shifted first-query cost. This is a program objective, not a promised outcome of profiling alone.

### PERF-09 — Adaptive physical encoding and ordered writer pipeline

**Priority:** P0/P1, parallel with execution work. **Depends on:** PERF-08; shared budgeting from PERF-03.  
**Implementation:** per-column cost/size attribution; encoding sweeps; ownership-preserving arrays; bounded parallel compression and ordered output; single-file integrity and commit validation.

**Acceptance:** lifecycle improvement over representative one-query and repeated-query use, with storage and peak memory disclosed; no regression to the rejected huge-artifact profile; second stretch ingest milestone around 70 seconds only if hardware floors and profiling support it. Do not retain a smaller artifact that worsens the agreed lifecycle objective.

### PERF-10 — Native fused operator IR

**Priority:** P1. **Depends on:** PERF-04 and PERF-07.  
**Existing anchors:** `shardloom-plan`, `shardloom-exec`, existing native kernel dispatch.  
**Implementation:** physical representation and ownership types, reusable kernels, costed pipeline fusion, selective canonicalization and adaptive aggregation/ordering choices.

**Acceptance:** renamed-schema and non-ClickBench tests use the same optimized mechanisms; no extra provider labels without executable kernels; reductions in decoded/intermediate bytes and dispatch costs are measurable; semantic parity across encoded/canonical input forms.

### PERF-11 — Honest sub-ms ingest-to-result mode

**Priority:** P1. **Depends on:** PERF-02, PERF-03, PERF-07 and a publication-semantics RFC.  
**Implementation:** pre-registered typed schemas, resident bounded buffers, explicit memory visibility, no compulsory physical file for transient work, immediate small-batch execution with a maximum batching delay.

**Acceptance:** stated 64 KiB/4,096-row starting envelope and bounded output tested at p50/p95/p99; all input validation and requested computation included; no fsync/durable claim in this mode; behavior under concurrent bulk work and cancellation measured.

### PERF-12 — Universality and competitor acceptance suite

**Priority:** P0 baseline skeleton, P1 expansion. **Depends on:** PERF-01; runs on every other packet.  
**Implementation:** fixed eligible ClickBench cohort, product latency harness, non-ClickBench semantic/performance families, and normal versus benchmark resource configurations.

**Acceptance:** 43/43 correct plus held-out family checks; no result-cache contamination; performance gains on at least two non-ClickBench families before calling the program a universal-engine improvement. Beating a competitor means reproducible lower scores under matched conditions, not a cross-host ratio.

### PERF-13 — Tiered compilation and hardware specialization

**Priority:** P2. **Depends on:** PERF-10 and evidence of remaining instruction/dispatch bottlenecks.  
**Implementation:** useful PGO training and held-out validation first; optional cached native pipeline compilation and CPU dispatch after a measured break-even analysis. Accelerator paths, if justified, remain separately accounted.

**Acceptance:** improvement including compilation and transfer costs at real reuse counts; portable package remains correct; no silent relaxation of the unsafe policy, arithmetic semantics or no-external-engine contract.

## 11. Delivery order and stop rules

**Wave A:** PERF-01 and the held-out test skeleton. Establish current truth and call-path cost.  
**Wave B:** PERF-02/03/07, while PERF-08 profiles ingestion. The first architectural milestone is a real resident runtime and actual bounded execution, not a claim registry.  
**Wave C:** PERF-04/05/06 and PERF-09. This is the highest-likelihood region for large total-work improvements.  
**Wave D:** PERF-10/11/12. Generalize the wins, establish actual sub-ms product envelopes, and validate ordinary memory limits.  
**Wave E:** PERF-13 only where profiles still justify it.

Keep baseline and candidate behind a temporary measured feature switch while validating. Delete the losing implementation or retain it only as a named algorithmic choice with a demonstrated domain; do not accumulate indefinitely overlapping routes. Extract the exact old/new call chains before changing giant modules. Splitting source files helps review and testing, but is not itself a runtime speedup.

A change is not retained merely because a status says “executed,” a kernel registry grew, a queue policy exists, or a microbenchmark improved. It needs correct public results, changed work counters consistent with the hypothesis, measured latency/throughput or resource benefit, and no unacceptable held-out regression. Do not require every micro-optimization to save seconds; do require the overall program to be driven by dominant costs.

Suggested program-level progress gates are approximately 2× lower same-host suite time, then 5× lower, with ingestion evaluated independently and jointly. These are goals to test. Final competitive acceptance should use the pinned official score and matched lifecycle metrics; none of these intermediate multipliers alone proves leaderboard leadership.

## 12. Sources and verification anchors

All repository references below are pinned to the audited ShardLoom commit unless otherwise noted. Public reference pages were consulted on 4 September 2026. Link line ranges identify the reviewed region, not a guarantee that all surrounding code was audited.

- **[S1] ShardLoom README:** https://github.com/depsilon/shardloom/blob/af9d96af9cb370e521e22b9b675f1a324da52cb1/README.md
- **[S2] Phased execution plan, especially shared scheduling and string aggregation packets, approximately lines 800–1140:** https://github.com/depsilon/shardloom/blob/af9d96af9cb370e521e22b9b675f1a324da52cb1/docs/architecture/phased-execution-plan.md#L800-L1140
- **[S3] Clean-ingest current-branch UAT JSON, first 160 lines reviewed:** https://github.com/depsilon/shardloom/blob/af9d96af9cb370e521e22b9b675f1a324da52cb1/docs/benchmarks/clickbench-100m-current-branch-uat.json#L1-L160
- **[S4] Scheduler implementation, reviewed lines 1–240, 800–1080 and 1200–1510:** https://github.com/depsilon/shardloom/blob/af9d96af9cb370e521e22b9b675f1a324da52cb1/shardloom-vortex/src/scheduler_bridge.rs#L800-L1510
- **[S5] CLI worker implementation, first 280 lines reviewed:** https://github.com/depsilon/shardloom/blob/af9d96af9cb370e521e22b9b675f1a324da52cb1/shardloom-cli/src/main.rs#L1-L280
- **[S6] Scoped source-state reuse matrix, first 200 lines reviewed:** https://github.com/depsilon/shardloom/blob/af9d96af9cb370e521e22b9b675f1a324da52cb1/docs/architecture/source-state-reuse-coverage-matrix.md#L1-L200
- **[S7] Workspace build and safety policy:** https://github.com/depsilon/shardloom/blob/af9d96af9cb370e521e22b9b675f1a324da52cb1/Cargo.toml
- **[S8] Columnar result representation, first 200 lines reviewed:** https://github.com/depsilon/shardloom/blob/af9d96af9cb370e521e22b9b675f1a324da52cb1/shardloom-vortex/src/columnar_result_dataplane.rs#L1-L200
- **[S9] Ingestion optimization ledger, first 180 lines reviewed:** https://github.com/depsilon/shardloom/blob/af9d96af9cb370e521e22b9b675f1a324da52cb1/docs/architecture/clickbench-ingest-optimization-ledger.md#L1-L180
- **[S10] ClickHouse September 4, 2026 c6a.4xlarge result, Git blob b8fd328dc73c5d42ff6af84f6cb6998b8fe93908:** https://github.com/ClickHouse/ClickBench/blob/main/clickhouse/results/20260904/c6a.4xlarge.json
- **[S11] Vortex array model and encoding families:** https://docs.vortex.dev/concepts/arrays
- **[S12] Apache Arrow C data interface:** https://arrow.apache.org/docs/format/CDataInterface.html
- **[S13] Vortex runtime/threading model:** https://docs.vortex.dev/developer-guide/internals/async-runtime
- **[S14] ClickBench methodology and scoring, Git blob c47d32ed5d8bbe33422c0a73781cf52a270b27b4:** https://github.com/ClickHouse/ClickBench/blob/main/README.md
- **[S15] Vortex Scan API, including its explicit in-development caveat:** https://docs.vortex.dev/concepts/scanning
- **[S16] DuckDB's engineering description of parallel/external aggregation, an algorithmic reference rather than a runtime dependency:** https://duckdb.org/2024/03/29/external-aggregation

Upstream's Scan API and runtime abstractions are useful integration directions, but documentation explicitly notes unfinished parts of the scan surface and I/O-progress pitfalls. Check compatibility against the repository's pinned Vortex 0.85 before relying on a particular provider interface. [S7], [S13], [S15]

**Bottom line:** build a small resident hot path and a high-throughput native execution core that share actual arrays, bounded resources and exact semantics. Make the latest successful dictionary optimization a reusable mechanism; make ingestion pay only for useful representation work; and judge the result through matched competitive benchmarks plus honest product and lifecycle measurements.

