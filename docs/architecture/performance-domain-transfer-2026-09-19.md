# Differentiator reuse and material performance proposals

Status: research and ship/drop proposals, inspected at `b254bbce` on September 19,
2026. The maintainer requested README polish, reuse of ShardLoom concepts, and
additional domain-transfer research targeting drastic gains and subsecond latency.
The subsequent maintainer request authorizes ship/drop implementation and bounded
complete-operation screens in the sequence below. Work is in progress; no candidate
is retained or measured faster until its acceptance gate passes. Broader capability
completion stays paused. The original documentation-only evidence remains separate
from this implementation follow-up.

## Decision

Prioritize removing intermediate representations, whole passes, and unnecessary
state before another codec, scheduler, or small hash-table tuning experiment.
The strongest proposed sequence is:

1. Q29: feed the retained weighted consumer from owned string-count partials,
   avoiding construction of a copied dictionary and per-row codes where possible.
2. Q36: carry the already-proven physical grouping key into existing worker
   admission, including the actual integer width and output reconstruction.
3. Q13: admit exact filtered counts to complete-key partitions when the predicate
   contract proves equivalence, removing the current exact recount pass.
4. Q23: attribute lazy provider execution inside accessor construction, then
   preserve selection and ownership through that boundary if it removes work.
5. Q19/Q33: pursue a larger state/partition algorithm change only after measuring
   key traffic, probes, reduction, and distribution. Another packed-key or seen-set
   rewrite alone does not qualify.

Each is a proposed shared operator-family change. Query IDs identify evidence,
never runtime dispatch conditions. The README now explains the retained concepts
and their limits; these proposals are deliberately kept out of its support claims.

## Evidence and the subsecond objective

The [43-query inventory](../benchmarks/profiling-research-inventory-2026-09-19.json)
is an extraction of existing saved evidence, not a new benchmark. It records all
three native process times, peak RSS, first-run envelope hashes, SQL, and selected
first-pass counters. Missing counters remain absent rather than becoming zeros.
The source is `4f2c7b97007864d0396b10bdc5dc2bbfef52df38`, binary SHA-256
`3af1c45d90b6466a0af26ee205cfff4219bffce61880b444bfd30b28a5f6feeb`.
All 129 complete-result comparisons passed; the per-query best-of-three sum is
91.825940 seconds. This was P12/24 GiB, a fresh process per query, uncontrolled OS
cache, and timing through complete public CLI output and exit. It is neither a
resident-session score nor a measurement of the latest hardening commit.
The [combined UAT](../benchmarks/combined-performance-uat-2026-09-12.md) separately
records 95.923669-second ingest and an 18,591,586,804-byte native artifact.

| Target | Best process time | Run 1 process time | Run 1 accessor span | Run 1 group-update span | Actual retained mechanism |
| --- | ---: | ---: | ---: | ---: | --- |
| Q29 | 9.300 s | 9.705 s | 7.169 s | 1.917 s | Chunk UTF8 dictionary, dense transformed weighted partials |
| Q19 | 8.705 s | 8.853 s | 3.104 s | 5.118 s | Typed numeric/minute/string-code keys; 56,384,822 groups |
| Q36 | 6.544 s | 6.803 s | 0.089 s | 5.966 s | One physical key already represents four logical expressions |
| Q13 | 6.337 s | 6.394 s | 2.111 s | 1.840 s | Heavy-hitter candidates followed by exact dictionary recount |
| Q33 | 5.150 s | 5.456 s | 0.289 s | 4.120 s | Near-unique key directory, then retained-key measure pass |
| Q10 | 4.749 s | 4.749 s | 0.295 s | 4.324 s | Exact integer-pair preunion plus other measures |
| Q23 | 4.460 s | 4.460 s | 4.219 s | 0.012 s | Filtered scan; three text accessors; 7,128 aggregate input rows |
| Q18 | 3.536 s | 3.705 s | 3.065 s | 0.490 s | Source-order compound count with dictionary-code reuse |

These instrumentation spans are not exclusive CPU attribution. In particular,
accessor time can include deferred Vortex execution, string decoding, allocation,
hashing, and dictionary construction; it is not a measurement of URL parsing.
Do not divide a run-1 span by a best-of-three duration or add overlapping work spans
to infer utilization. Q29 has 1,798,248 groups before HAVING, 74 after HAVING, and
25 final rows. Treating it as a 74-entry aggregation understates its work.

Here, **subsecond means a complete admitted query below one second**, with its
lifecycle named. Moving Q29, Q19, Q36, Q13, or Q33 below that boundary requires
approximately 89%, 89%, 85%, 84%, or 81% less elapsed time respectively, relative
to their saved best samples. Those are arithmetic requirements, not forecasts.
Small reducer wins cannot deliver that outcome by themselves.

Use a bandwidth/work lower-bound screen before promising such a result: measure
bytes actually fetched and traversed, effective bandwidth on this machine, and
unavoidable serial work. If the remaining required traffic alone exceeds the
latency budget, the next idea must avoid traffic or change the admitted lifecycle.
The [Roofline model](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2008/EECS-2008-134.pdf)
motivates this bound; its floating-point model does not directly predict string
hashing, decompression, or random-access latency. OS RSS is not a traffic counter.

## Mechanisms transferred into existing ShardLoom concepts

External sources below supply mechanisms, not code, dependencies, or benchmark
predictions. No external engine or GraphBLAS implementation would execute a query.

| Source domain and grounded mechanism | Transferable relation | ShardLoom concept/artifact to reuse | Evidence needed | Where the analogy breaks |
| --- | --- | --- | --- | --- |
| Sparse algebra: operations combine represented values and weights ([GraphBLAS specification](https://graphblas.org/graphblas-api-cpp/)) | Preserve multiplicity without expanding each observation into another representation | `StringCountPartial` into the existing weighted transformed consumer; proposal A | Same weights, source domain, first-occurrence/chunk order, and owned lifetime | Floating addition is not associative; NULL and byte/string semantics are not interchangeable with sparse zero |
| Compilers: eliminate or move work only after dependency/effect analysis ([LLVM passes](https://www.llvm.org/docs/Passes.html#licm-loop-invariant-code-motion)) | Carry a proven simplification through every later eligibility decision | Existing functional-dependency keys into worker admission; proposal B | Same grouping equivalence, arithmetic errors, reconstructed output and ranking | Moving a fallible expression can hide an error on a non-winning group |
| Compressed database execution: keep operators aware of their representation ([Abadi et al., SIGMOD 2006](https://15721.courses.cs.cmu.edu/spring2016/papers/abadi-sigmod2006.pdf)) | A filter/count should consume codes, masks and weights before building another dictionary | Existing native filter, partial count and ownership seams; proposals A/C/D | Actual representation transitions and avoided work, with complete results | A chunk dictionary built after decoding is not native dictionary execution |
| Cache-aware algorithms: partitioning can replace scattered state access with localized work ([CIDR 2019 partitioning study](https://www.cidrdb.org/cidr2019/papers/p133-zhang-cidr19.pdf)) | Pay a bounded redistribution cost only when it removes more probing/merge traffic | Existing complete-key partitions, `AggregateChunkJobs`, `QueryRunStore`; proposals E/F | Key traffic, cache/probe behavior, partition skew, scratch and merge costs | Extra passes can cost more than hashing; partitioning is not universally best |
| Information retrieval/analytics: candidate bounds followed by exact evaluation ([Siddiqui et al., PVLDB 2023](https://www.vldb.org/pvldb/vol17/p644-siddiqui.pdf)) | Defer expensive measures only when an exact exclusion proof survives | Retained ProofBound heavy-hitter and late-measure routes | Candidate completeness, tie rules, and measured extra-pass cost | Q13/Q33 already use this pattern; near-unique groups and weak bounds defeat pruning |
| Network scheduling: carry service deficits across variable-sized units ([Deficit Round Robin](https://openscholarship.wustl.edu/cse_research/339/)) | Bound how long one admitted producer monopolizes service while accounting for work | Existing worker grants, FlowInventory and ScarcityLedger; proposal H | Queue residence, service time, p99, progress and throughput at declared load | Bytes are not CPU time; a codec call is not preemptible like a scheduling boundary |

Current [Vortex execution documentation](https://docs.vortex.dev/developer-guide/internals/execution)
describes dictionary scalar-function pushdown and fused parent operations. The
local dependency is **Vortex 0.85.0**: its `vortex-array` dictionary
`compute/rules.rs`, `compute/mod.rs`, and `execute.rs` already expose dictionary
scalar-function reduction, filter reduction, and canonical execution. The local
source was inspected as well as current docs; current docs alone are not a promise
that every described API or fusion is available in the pinned release.

## Proposed ship/drop experiments

“Ship candidate” below means worth a bounded implementation screen after its
profiling gate passes. None is ready to ship merely because it appears here.
The source symbols are in the inspected commit and can be found by name if line
numbers move. Proposed thresholds are decision rules, not predicted gains.

### A. Owned weighted string partials for transformed aggregation — first

**Target:** Q29, then compatible string measure families. PERF-04/05/07/10.

The current `aggregate_direct_utf8_chunk_dictionary_accessor` in
[`local_primitives.rs`](../../shardloom-vortex/src/local_primitives.rs) canonicalizes
UTF8, allocates `Arc<str>` per distinct value, builds row IDs, and later counts those
IDs for the dense transformed consumer. In contrast, the retained
[`StringCountPartial`](../../shardloom-vortex/src/local_primitives/string_count_partial.rs)
owns a `VarBinViewArray` and value-index/count slots without a copied string
dictionary or per-row codes. It already exposes `for_each_count` and
`preserve_existing_key_order`.

**Delta:** adapt that existing owned partial to feed
`update_dense_general_direct_from_transformed_dictionary`'s weighted measures.
Count each exact source value once per chunk, then pass its borrowed bytes and
weight to the existing domain transform and accumulators. Preserve native Dict
and Constant routes before canonical execution. Keep original chunk and weighted
update order; a shared consumer does not authorize floating reassociation.
Reserve counts and retained native buffers before admission and transfer ownership.

**Screen:** split accessor time into canonicalization, UTF8 validation/hash,
allocation/copy, and row-code/count work. Compare allocations, copied string bytes,
row IDs written, hash bytes, ordering work, and full process time. Source-byte
owners must remain alive through MIN selection and downstream handoff.

**Ship gate:** at least 1.0 s complete-query saving, with a stretch objective of
2 s or more, unchanged exact results and no material RSS increase. **Drop** if
partial construction/order restoration replaces the removed work, allocation
pressure worsens, or the proposed gain rests only on the 74 post-HAVING groups.
This is representation elimination using retained machinery, not another transform
memo, Arc-only key tweak, or second full scan for MIN.

### B. Preserve physical-key proofs through worker admission — second

**Target:** Q36 and equivalent deterministic derived-key COUNT families. PERF-04/10.

Q36 already reduces four logical keys to one stored key. Its saved group-update
span is 5.966 s. The shared
[`aggregate_count_workers.rs`](../../shardloom-vortex/src/local_primitives/aggregate_count_workers.rs)
precheck accepts one/two logical group columns and reconstructable constants;
`numeric_state_admitted` further requires one identity group column. Native
single-key admission currently accepts non-null I64/U64, whereas ClientIP is
logically I32. These are concrete eligibility boundaries, not proof that increasing
the thread count alone will help.

**Delta:** let an existing physical-key/reconstruction proof govern admission,
preserving the actual integer dtype through the retained native numeric owner.
Keep all exact keys in partials; use existing workers and deterministic reduction.
Evaluate required derived expressions with the same overflow/error behavior before
discarding groups. Do not specialize for ClientIP or recognize Q36 text.

**Screen:** actual admitted/completed kernel jobs, update/merge time, widened/copied
bytes and peak state at P1/2/4/8/12. **Ship gate:** at least 1.0 s end-to-end saving
without moving the bottleneck into a giant merge or multiplying state. **Drop** if
the reconstructed proof is insufficient, merge dominates, or the change only
alters scheduler labels. Radix reduction is a separate later hypothesis if this
screen identifies scattered state access as the remaining cost.

### C. Exact filtered counts without a recount pass — third

**Target:** Q13; reuse on compatible Q34/Q35-style families. PERF-04/05/10.

Q13 already uses dictionary histograms and exact heavy-hitter recount.
`string_count_topk_first_pass_exact_histogram_route_enabled` explicitly rejects
`!predicate_free`. Its nonempty SearchPhrase predicate provides a concrete
candidate for a more precise contract, rather than simply removing that guard.

**Delta:** carry an admitted deterministic key-only filter or certified selected
array into the existing complete-key partial/partition route. Count only surviving
rows with the same NULL semantics. Complete exact partitions can select final top-K
without re-reading the source. Native predicate pushdown remains active.

**Screen:** first-pass complete-key state, second-pass arrays/bytes avoided,
dictionary-domain binding, mask density and pressure transitions. **Ship gate:**
at least 1.0 s saving with the recount truly removed and memory within the admitted
budget. **Drop** if retaining all filtered keys causes excessive memory/spill or
the count provenance cannot prove predicate equivalence. Under pressure, retain
the existing explicit native transition rather than losing or double-counting rows.

### D. Selection-preserving accessor execution — profile before implementation

**Target:** Q23, then Q18/Q26/Q27 where the same mechanism is observed. PERF-07/10.

Q23 records only 7,128 aggregate input rows but 4.219 s inside accessor creation
for SearchPhrase, Title, URL and UserID. This is evidence of a large boundary,
not proof that millions of unnecessary dictionary entries were rebuilt. Lazy
provider reads/decompression may legitimately be charged there.

**Delta if proven:** retain the Vortex filtered-scan selection and native array
owners through field extraction, evaluate only needed values, and reuse an
already-paid canonical buffer instead of executing or rebuilding it again.
Inspect `logical_field_from_native_array`, `aggregate_column_accessor_in_context`,
and upstream filter/scalar-function reduction before inventing a new accessor.

**Screen:** separate deferred I/O/decompression from dictionary construction;
count source versus selected values, actual payload bytes, repeated field execution,
and owner lifetimes. **Ship gate:** at least 1.0 s reduction on Q23, or a comparable
material gain on a separately declared family. **Drop** if every read is necessary
and unique. Do not repeat the rejected full residual row-filter scan: it regressed
Q23 to about 24.826 s in the historical plan.

### E. Complete-key worker partitions for the existing tri-key state — conditional

**Target:** Q19. PERF-03/04/05/06.

`AggregateNumericMinuteStringKey` is already compact and reuses dictionary IDs.
The open issue is its 56.4-million-group update/state cost, not the absence of a
packed key. Extend the retained job/partition contract to typed triple keys,
binding each string code to the correct owned value domain. Permit no local top-K
truncation before complete-key reconciliation. Carry owned reservations into any
admitted spill transition; do not duplicate the string pool per worker.

**Screen first:** probes, inserts, duplicate reduction, dictionary ownership,
partition/merge bytes, skew, and complete-lifecycle savings. If local preaggregation
does little, test direct partitioning rather than hashing every near-unique key
twice. The [2025 global-hash-table study](https://arxiv.org/html/2505.04153v1)
is counterevidence to assuming partitioning always wins: algorithm choice depends
on cardinality, contention and resizing. It does not justify a new concurrent
table or alternate scheduler by itself.

**Ship gate:** at least 1.0 s query saving; a separately declared resource result
may qualify with at least 30% lower peak RSS and nonregressing elapsed time.
**Drop** packed-key-only, per-worker full-state duplication, and thread-count sweeps
without a changed work mechanism. A broad tri-key/nullable/spill expansion is not
implicitly activated by this research.

### F. Exact partitioned duplicate reduction for near-unique pairs — conditional

**Target:** Q33; only later consider Q10 exact-pair DISTINCT. PERF-04/05/06.

Q33 already has `NumericPairNearUniqueCountDirectory` and a late-measure second
pass. It records 99,997,493 groups from 99,997,497 rows, and peaked at 5.43 GiB
across the saved runs. Propose a different work pattern: bounded complete-key
partitions followed by exact adjacent-key reduction, so the retained directory
need not hold almost one hash entry per row. Keep existing late measures.

This requires every full pair to be reconciled within its partition. A hash,
sample, Bloom filter, or sketch may guide work but cannot establish uniqueness.
Select partition top-K only after exact partition completion using the complete
global tie comparator. Include sorting, redistribution, merge, and temporary-run
bytes in the cost. Q10 already eliminates 78,074,526 duplicate input rows through
exact pair preunion; copying that idea into another row is not new work.

**Ship gate:** at least 1.0 s saving, or at least 30% RSS reduction with nonregressing
complete time under an explicitly separate resource objective. **Drop** if extra
traffic outweighs directory removal, near-unique sampling affects correctness,
or it reduces to another previously rejected seen-set variant. No universal
radix, bitmap, or compact-state replacement is proposed.

### G. Fuse ingest traversals only where the owner and statistics contract match

**Target:** ingest CPU, memory and storage lifecycle. PERF-08/09.

Reuse native array ownership and exact statistics while a producer already has
the relevant values in hand. The compiler/dataflow transfer is removal of a
demonstrably duplicate traversal, not moving codec work to a differently named
owner. The retained [ingest profile](../benchmarks/ingest-stage-balance-2026-09-12.md)
shows compression/statistics work but its overlapping spans cannot establish
exclusive savings. Identity-projection and discarded-fingerprint work are already
removed, and retained numeric probe improvements must remain.

**Screen:** exact producer/consumer reads, validity, Unicode length versus byte
length, extrema, source generation, numeric probe and native writer requirements.
Only reuse facts with matching semantics and lifetime; inspect upstream statistics
and writer providers first. **Ship gate:** at least 10% complete ingest improvement
(roughly 9.6 s against the combined observation as a sizing guide), no material
artifact growth, full value/schema proof and fresh-artifact queries if bytes change.
The applicable control still comes from the control ledger. **Drop** if exclusive
removable work cannot meet that gate. Writer overlap, a codec portfolio, and the
slower smaller text artifact stay parked.

### H. Bound queue residence through existing grants — conditional serving work

**Target:** short-query p99 under native write/aggregate contention. PERF-03/11/12.

Transfer deficit accounting from packet scheduling into the existing admitted
worker grants only if traces show monopolized service. A grant consumes measured
work/byte credits and yields at an existing safe chunk boundary; waiting work
retains explicit ownership and cancellation. Reuse FlowInventory/ScarcityLedger;
do not introduce a second pool or scheduler. A large codec call cannot be made
preemptible by relabeling it as a capillary unit.

**Screen:** queue residence versus active service, owner occupancy, allocation
pressure, result completion, cancellation delay, and throughput. The new
[hardening fixture](runtime-hardening-cleanup-2026-09-19.md) covers bounded same-session
native writer contention, not a globally shared Parquet ingestion pool.
**Proposed ship gate:** at least 30% lower p99 at a fixed declared arrival rate,
row/byte envelope and CPU budget, without more than 5% throughput loss; declare a
subsecond p99 objective before the run. **Drop** if service time, rather than
queueing, dominates or a second control system is required.

### I. Extend owned result delivery only for payload-dominated workflows

**Target:** remaining computed results, export and public call latency. PERF-02/07/11.

Retained COUNT/DISTINCT owned-result routes already avoid row/JSON reconstruction.
Extend one measured missing result family through `OwnedVortexResultBatch` and
existing sinks. Track actual copied/decoded bytes and lifetime-bound credits,
including slices and writes after input close. Keep native execution, worker
transport, Python handling and complete output clocks separate.

**Proposed ship gate:** at least 20% and 100 ms lower complete latency on a declared
large-payload workload, targeting subsecond completion where feasible, with no
hidden copies or ownership regression. **Drop from the heavy Full43 queue:**
optimizing rendering of its tiny 10/25-row outputs without measured dominant cost.
Native Python binding remains parked unless transport is shown to dominate.

## Explicit drops and boundaries

| Idea | Disposition and reason |
| --- | --- |
| Reimplement Q36 dependency elimination, Q19 key packing, Q13 heavy hitters, Q33 late measures, or Q10 preunion | Drop as duplicate work; the current evidence already records them |
| Another Q29 parser-only SIMD tweak, transform memo, capacity-only change, or late-MIN full rescan | Drop without a different cost mechanism; accessor construction dominates the recorded first pass and the rescan previously regressed |
| Persist benchmark answers, global GROUP BY results, or query-specific sidecars | Reject; violates preparation/result and single-artifact boundaries |
| Bloom filters, approximate DISTINCT, or sampled uniqueness as exact answers | Reject; candidate pruning requires conservative proof and exact completion |
| Universal bitmap state or Roaring conversion | Defer; dictionary-code domains differ across chunks, sparse integer spans can explode, and Q17 bitmap membership already regressed. The [Roaring research](https://roaringbitmap.org/publications/) motivates density-aware containers, not automatic suitability for counted tuples |
| FSST/default codec sweep | Defer; previous lifecycle evidence did not retain it. The [FSST paper](https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf) supports equality with a shared symbol table, not arbitrary compressed substring/regex execution or free cross-table equality |
| Topology/coalescing, generalized local reductions, execution-aware preparation | Remain parked under the current plan; the proposed workers must reuse existing actual jobs |
| PGO/JIT/GPU or a new global concurrent hash table before attribution | Defer; no demonstrated dominant instruction cost or break-even case, and CPU-native work avoidance remains first |
| Brand-new metadata indexes or derived columns as the first response | Defer; first test consumers on retained bytes. Any later generic structure needs ingest/storage/query amortization and source-generation proof |

## Provider decisions and acceptance sequence

Use the pinned Vortex array/Dict/Constant/VarBinView/filter/scan/writer providers
already isolated in `shardloom-vortex`. A/C/D are proposed
`use_vortex_native_provider` adaptations with existing ShardLoom consumers; B/E/F
are proposed `implement_shardloom_kernel` extensions because aggregate admission,
complete-key reduction, SQL semantics and resource ownership remain ShardLoom's
responsibility. G/H/I remain `blocked_until_vortex_or_shardloom_evidence` as new
optimization candidates pending the specified dominant-cost screen. These
classifications grant no new support or implementation permission.

For each admitted candidate:

1. Freeze the current source/binary and applicable artifact identity. Reuse the
   historical inventory to choose attribution; do not relabel it as a same-commit
   control or rerun unchanged Full43 merely to choose a hypothesis.
2. Add only the missing counters needed to falsify that candidate. Preserve all
   runs and name exclusive CPU versus elapsed spans honestly. Compare same-run
   stage evidence; account for measurement overhead.
3. Establish semantics on renamed, nonbenchmark fixtures: NULL/empty/Unicode,
   integer bounds, float accumulation, collisions, reordered dictionary domains,
   ties/OFFSET, source invalidation, all-unique/skewed data, allocation denial,
   cancellation and spill where admitted. Require actual dispatched work evidence.
4. Screen a bounded complete public operation against applicable saved evidence.
   Stop marginal/negative candidates. A plausible material gain earns alternating
   same-source, same-artifact repeated control/candidate runs with frozen binaries;
   report paired deltas and dispersion, not just a best sample.
5. Retention requires complete results, the per-card material threshold, scoped
   resource limits and no unexplained regression. Runtime-only candidates should
   preserve artifact bytes; layout changes need their own ingest and fresh-artifact
   acceptance. Do not sum overlapping candidate savings.
6. Run required Rust/feature checks and independent held-out acceptance for the
   changed behavior. Run broad Full43 once for a retained coherent runtime batch;
   full ingest is needed for changed writer/layout behavior, not every query edit.
   Large work remains serial under local storage and process guards.

All candidates preserve `fallback_attempted=false`, `external_engine_invoked=false`,
Vortex-native input/output, and explicit decode/materialization evidence. No new
phase IDs are introduced. PERF-01 through PERF-13 and CG-1 through CG-23 retain
their canonical status; CG-5 correctness and CG-6 benchmark evidence remain
necessary for competitive claims. The 116 open phase items are not a mandate to
implement this entire research menu.

## Documentation verification

The nine static public-claim, public-status, local-sink, source/prepared-state,
user-surface graduation, workspace-version, front-door, Vortex-runtime, and docs
productization validators pass. The architecture tracker passes its existing
`--allow-blocked` invocation while correctly retaining 116 unchecked phase items.
Reports are under the machine-local `readme-performance-research-20260919` evidence
directory. All 43 query records, 129 timings, SQL/source identities, peak-RSS
maxima and 43 first-run raw envelope hashes were checked against the saved packet;
all 75 local Markdown link targets in the changed documents exist. An independent
source review found no actionable issue in the priority proposals or README claims.
No runtime code, dependency, benchmark control or support gate changed, so no
Cargo build/test suite or new benchmark was run for this documentation batch.
