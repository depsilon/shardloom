# Performance plan reconciliation and remaining implementation

Status: source/evidence inventory at merged `a8775c4f` on September 12. The
maintainer subsequently requested continued implementation until the plan items
are exhausted, including the older phased-plan epics. This document reconciles
all 56 execution-checklist entries in PERF-01 through PERF-13, all 43 unchecked
nested entries in the five older production epics, and the September 12 ingest
sequence. At this source revision those account for all 116 unchecked boxes in
the canonical plan: 18 top-level items, 55 PERF subitems and 43 older subitems.
One of the 56 PERF checklist entries is already checked. This does not mark
the whole program complete or replace the canonical
[phased plan](phased-execution-plan.md). No new phase IDs or competitive-gate
closures are introduced; CG-1 through CG-23 retain their existing status.

The maintained ingest baseline is **95.447305458 seconds**, native `572bd52c`.
The 118.604707-second allocation candidate was dropped. The extra
104.044137-second unchanged-control observation is historical evidence and does
not replace the baseline. Do not rerun/change that control until a candidate
first shows credible material improvement. Preserve all failed samples and the
existing artifact/log/source/process guards.

## Work that can proceed now

1. **Connect aggregate workers to exact native spill under pressure.** The
   weighted COUNT accumulator's `transfer_drained_epoch` is test-only, while
   `weighted_count_spill_query` feeds a serial source accumulator. Existing
   `AggregateChunkJobs`, string/compound partitions and native runs provide the
   components. Implement a production transition that stops submission, drains
   workers, transfers the committed prefix once, consumes each untouched suffix
   once, and continues into the same admitted native run store. Keep source
   generation, ordering, cancellation and memory credits intact. Start with the
   already admitted non-null UTF8 COUNT family, then its optional integer key;
   exact grouped distinct needs its own pair-deduplication transfer contract.
   This closes a real bounded-completion gap without restarting topology work.
2. **Extend prepared aggregates through existing consumers.**
   `PreparedVortexAggregate` admits only identity COUNT/COUNT DISTINCT/SUM over
   integer group/measure fields. Ordinary consumers already implement MIN/MAX
   and other measures. Start with integer MIN/MAX, preserving ordinary result
   types, NULL/empty behavior, exact comparisons and fresh state on every call.
   Then evaluate existing text grouping and additional measures as explicit
   families. Reuse `AggregateLowering`, the held source and the public worker's
   handle path; extend native/worker/Python/fresh-process acceptance. This is
   source/lowering reuse, not answer caching or new aggregate mathematics.
3. **Complete file-backed serving and pressure acceptance.** The resident
   admission mutex is held across complete operations. Add a bounded same-session
   long-scan/short-count workload that measures queue delay, completion, p50/p95/p99,
   cancellation and shared ownership. Existing mixed in-memory intake samples do
   not cover it. If serialization causes material short-call delay, design a
   bounded admission change through the existing runtime with explicit CPU/I/O
   progress. Do not add independent unbudgeted session lanes.
4. **Carry additional computed results as owned native arrays.** The direct
   native sink admits source projection/filter and source-column expressions.
   Extend one existing computed aggregate/result family to `OwnedVortexResultBatch`
   and the retained native/compatibility sinks, avoiding a row/JSON roundtrip.
   Prove dtype, validity, ordering, result lifetime and bytes actually avoided.
   Multi-source joins/windows remain separate follow-on families.
5. **Finish existing operator families outside the PERF shorthand.** Generalize
   the retained literal-integer constant-key pruning to provable string/NULL
   constants with exact output reconstruction; define a three-key grouped worker
   state through the retained aggregate jobs/partitions; and connect one admitted
   metadata predicate to actual query consumption with conservative no-match
   proof. These are explicit remaining items in the older registry, scheduler
   and metadata epics. Keep their tests and execution certificates coupled to
   the corresponding implementation, not a second registry or scheduler.

These tasks need implementation and focused acceptance; absence of an existing
benchmark is not a reason to leave them indefinitely deferred. Initial bounded
tests should prove exact behavior and work/resource reduction before expensive
acceptance. Full43 or large ingest runs are not prerequisites for writing these
tests or integrating existing ownership/spill machinery.

## Source and evidence anchors

| Surface | Current source/evidence |
|---|---|
| Prepared aggregates | [admission and execution](../../shardloom-vortex/src/local_primitive_prepared_aggregate.rs), [immutable lowering](../../shardloom-vortex/src/local_primitive_aggregate_lowering.rs), [native acceptance](../../shardloom-vortex/src/local_primitive_prepared_aggregate_native_tests.rs), [public call harness](../../scripts/run_resident_call_path_uat.py). |
| Shared work and exact keys | [chunk jobs](../../shardloom-vortex/src/local_primitives/aggregate_chunk_jobs.rs), [COUNT workers](../../shardloom-vortex/src/local_primitives/aggregate_count_workers.rs), [compound partials](../../shardloom-vortex/src/local_primitives/compound_count_partial.rs), [string partitions](../../shardloom-vortex/src/local_primitives/string_count_partitions.rs), [compound partitions](../../shardloom-vortex/src/local_primitives/compound_count_partitions.rs), [integer distinct pairs](../../shardloom-vortex/src/local_primitives/exact_distinct_pairs.rs). |
| Native query spill | [weighted query](../../shardloom-vortex/src/local_primitives/weighted_count_spill_query.rs), [accumulator and test-only transfer](../../shardloom-vortex/src/local_primitives/weighted_count_spill_accumulator.rs), [transfer tests](../../shardloom-vortex/src/local_primitives/weighted_count_spill_accumulator_tests.rs), [distinct query](../../shardloom-vortex/src/local_primitives/exact_distinct_spill_query.rs), [shared run store](../../shardloom-vortex/src/local_primitive_query_run_store.rs), [sort spill](../../shardloom-vortex/src/local_primitive_sort_spill.rs). |
| Session, arrays and sinks | [session admission/source generations/owned result](../../shardloom-vortex/src/resident_session.rs), [native sink admission](../../shardloom-vortex/src/local_primitive_native_sink.rs), [memory intake](../../shardloom-vortex/src/resident_memory_source.rs), [owned intake](../../shardloom-vortex/src/resident_memory_owned_intake.rs), [memory file generation](../../shardloom-vortex/src/memory_file_generation.rs). |
| Encoded execution and pruning | [Constant/RunEnd consumers](../../shardloom-vortex/src/local_primitives/encoded_numeric_reduction.rs), [native numeric ownership](../../shardloom-vortex/src/local_primitives/native_numeric_owner.rs), [prepared scan/pruning](../../shardloom-vortex/src/local_primitive_prepared_scan.rs), [retained segment reuse](../../shardloom-vortex/src/resident_segment_reuse.rs). |
| Older production epics | [metadata types](../../shardloom-vortex/src/metadata_summary.rs), [conservative metadata proof](../../shardloom-vortex/src/metadata_pruning.rs), [registry admission](../../shardloom-vortex/src/specialized_kernel_registry.rs), [result contracts](../../shardloom-vortex/src/columnar_result_dataplane.rs), [retained operator implementations and tests](../../shardloom-vortex/src/local_primitives.rs). |
| Ingest lifecycle | [CPU admission](../../shardloom-vortex/src/ingest_cpu_lanes.rs), [bounded subtrees](../../shardloom-vortex/src/ingest_bounded_layout.rs), [pipeline tests](../../shardloom-vortex/src/vortex_ingest_owned_tests.rs), [ordinary publication](../../shardloom-core/src/security.rs), [current ingest packet](../benchmarks/ingest-stage-balance-2026-09-12.md). |
| Retained acceptance | [final runtime](../benchmarks/retained-runtime-acceptance-2026-09-08.md), [numeric/aggregate continuation](../benchmarks/perf-numeric-aggregation-2026-09-06.md), [native continuation](../benchmarks/perf-native-continuation-2026-09-06.md), [native export](../benchmarks/retained-native-export-2026-09-08.md), [retained ingest](../benchmarks/retained-ingest-owner4-2026-09-08.md). |

The [Rust source migration note](../reference/resident-native-results.md#rust-streaming-source-migration-held-file-generations)
documents the required `FlatLocalColumnarStreamSource.source_identities` field,
Unix Parquet generation-check scope and focused mutation-test evidence. The same
reference states the [actual serving-test scope](../reference/resident-native-results.md#shared-session-serving-scope);
same-session scan/count completion does not close ingest contention or FIFO
fairness acceptance below.

## Checklist reconciliation

Numbers below identify entries by order within each existing PERF execution
checklist, not new work IDs. **Retained, scoped** means the named implementation
and evidence exist; it does not close other family coverage. **Open implementation**
or **open acceptance** identifies concrete remaining work. **Measured drop** and
**dependency** apply only to their stated scope. The top-level PERF checkboxes
remain open where their rows still require implementation or acceptance.

### PERF-01 — baseline and timing

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Same-native-code ingest and all 43 queries | Retained, scoped: `572bd52c` ingest is 95.447305458 seconds; a freshly written byte-identical artifact passed September 12 Full43, 129/129 complete results. The new query totals are 102.485398 best, 103.076700 hot and 313.564493 all executions. Dates/cache/physical reference differ; do not claim a paired ingest-plus-query speedup or repeat the baseline to close stale wording. |
| 2. Separate wall/work/output/validation clocks | Retained native process and public call clocks, work-span disclaimers, validation and archive clocks. Open attribution for new operator families; expose only measurements needed by their actual bottleneck. Work spans and stack occupancy are not exclusive CPU time. |
| 3. Latency for newly admitted families | Open acceptance attached to prepared MIN/MAX and later result/spill families; extend the existing three-surface harness and retain separate native/fresh-process clocks. |
| 4. Dominant costs and raw evidence | September 12 ingest attribution is complete and retained. For the next query family, inspect its existing counters/profile and preserve all candidate records; no additional ingest profile is required just to maintain this checklist. |

### PERF-02 — prepared execution

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Remaining operator families | Open implementation: MIN/MAX, additional admitted scalar/text measures, then ordered/computed results through the current prepared source/lowering seam. Retained COUNT/COUNT DISTINCT/SUM and projection/filter must remain unchanged. |
| 2. Native Python binding decision/migration | Dependency: prototype remains parked and excluded from shipping. Existing subprocess reuse is not an in-process binding. Reopen only with a measured dominant transport cost and an explicit binding design, dependency/license review and exact-interpreter acceptance; do not start a speculative rebuild. |
| 3. Separate call-path latency and invalidation | Retained nine-case 1,674-check scope. Open acceptance for each new family, plus same-session file-backed contention; cover replacement/truncation/mutation, handle mismatch and fresh aggregate state. |

### PERF-03 — one resource budget and scheduling

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Remaining I/O/codec/operator admission and progress | Open implementation: first carry worker-state reservations through the native spill transition; then address identified allocator-bypassing source/codec owners through existing grants. One artifact pool and constructed CPU counts do not establish a global RSS ceiling. |
| 2. Executable split/layout/Capillary inventory | Completed source inventory and experiments distinguish original jobs from group labels. Use existing `AggregateChunkJobs`/ComputePool and held source; do not repeat an inventory-only implementation. |
| 3. Generation-bound natural regions and dynamic scheduling | Measured drop for both `a3c62434` grouping and `9152a92b` actual coalesced-job revisions. Their source is preserved outside shipping. No new topology sweep absent a materially different, source-grounded reason. |
| 4. Coarse/fine correctness and ownership | Scoped packets passed Full43; the coalesced revision's independent held-out matrix was skipped after its negative decision. Dependency on a new admitted topology candidate; older matrices do not certify a different revision. |
| 5. Queue bytes, growth denial, cancellation and mixed reuse | Open acceptance/implementation: production spill transition, file-backed serving contention and retained-result ownership under concurrent pressure. Existing queue and ingest teardown tests are useful but not whole-session mixed-load proof. |

### PERF-04 — aggregate workers and deterministic reduction

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Production grouped families on shared workers | Retained single-key COUNT, integer/text compound COUNT and scoped integer grouped DISTINCT. The historical Q17/Q34/Q35 wording understates this. Open implementation for remaining multi-measure, text-distinct and expression-key families; admit by types/semantics, not query numbers. Start with production spill handoff for retained COUNT workers. |
| 2. Region-local mergeable states | Dependency on retained topology evidence; parked with that experiment. Ordinary worker/partition improvements remain independently actionable. |
| 3. Additional complete-key logical partitions | Compound and exact-distinct partitions already extend beyond UTF8 COUNT. Open nullable/additional-measure families; preserve all keys until final selection and carry explicit key validity. |
| 4. Overflow/ties/skew/floating behavior at 1/2/4/8/12 | Scoped retained matrices exist. Extend them to each newly admitted family and mid-partition spill transfer, especially all-equal/tied keys, skew, empty work and u64 count overflow. Keep original floating accumulation boundaries. |
| 5. Actual work and superseded scheduling removal | Retained source/worker evidence and dropped allocation cleanup exist. For each extension report completed kernel jobs, committed rows and merge/spill work; remove its rejected code after the decision. |

### PERF-05 — dictionary aggregation under pressure

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Referenced entries with domain-safe equality | Retained string/compound partials keep dictionary codes in their native value domain and compare bytes plus integer signedness. Open nullable/new-key domains only; test equal codes in different dictionaries and forced hash collisions. |
| 2. Partition owned state and late strings | Retained exact string/compound partition ownership and late final selection. Open complete array-result handoff and newly admitted measures. Universal compact/slab replacement is not required: its tiny measured gains and repeated-input regressions justify the scoped drop. |
| 3. Exact overflow transition | Open production worker-to-run implementation. Existing in-memory pressure/replay and explicit serial native spill are not that transition. Reuse drained prefix/suffix transfer and quota accounting; preserve PR #1428's removal of dual maintenance. |
| 4. Null/unique/skew/composite/non-URL validation | Retained non-null renamed-schema coverage; extend independent fixtures to nullable grouping and disk handoff. Include Unicode, all-unique domains, reordered dictionary values and distinct payload owners. |

### PERF-06 — shared native spill

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Aggregate/distinct/join native runs | Sort, explicit weighted COUNT and integer grouped DISTINCT native runs already exist. Open worker-pressure transitions for COUNT then DISTINCT. General join spill is open implementation: first specify an admitted equality-join key/payload schema, NULL/multiplicity semantics and bounded build/probe/merge contract, then reuse `QueryRunStore`; do not treat serial aggregate spill as join support. |
| 2. Cancel/recovery/quota/corruption/owned cleanup | Existing run-store and per-family tests cover meaningful failure cases. Open production-worker cancellation during write/merge and cross-family crash-recovery acceptance. Recovery must preserve foreign runs/files and validate namespace, schema, length and checksum before cleanup. |
| 3. Large-state exact completion within budget | Open acceptance for real worker-pressure routes and later join family. Use a resource-sensitive deterministic dataset large enough to force multiple runs/merges, complete output comparison and OS RSS alongside reservations. No fixed 4 GiB process target is reinstated. |

### PERF-07 — executable result ownership

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Remaining results and compatibility sinks | Retained source projection/filter arrays and measured native-array IPC/Parquet export. Open computed aggregate/expression results, then multi-source materializing families: carry owned arrays/validity/selections through existing sinks instead of rendering rows and reconstructing them. |
| 2. Close/cancel/slice/copy/decode acceptance | Scoped native source/result and memory-generation tests exist. Extend each new result route through input/session drop, hidden clones/slices, output-limit denial and cancelled publication; measure actual copies/decodes rather than descriptor counts. |

### PERF-08 — ingest work elimination

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. September 12 attribution/allocation screen | Complete measured drop. Preserve 95.447305 baseline and 118.604707 rejected candidate, all receipts and retained tests. |
| 2. Safe repeated representation removal | Open source investigation, not permission to delete required work. Compare same-generation representations and ownership at existing conversion/persistence seams; implement only demonstrable redundant work. |
| 3. Normalization/metadata/codec/write/finalization attribution | Existing counters and owner sample supply bounded attribution. Completion includes ordinary publication but not fsync durability. Add missing attribution only for a concrete next candidate; do not rerun the entire profile by default. |
| 4. Duplicate representation/traversal inventory | Identity projection rebuild and discarded fingerprint work are already removed. Input copy, probe, canonicalization and final encoding have different contracts. Inventory a specific remaining repeated traversal and its consumer lifetime before changing it. |
| 5. Remove measured duplication | Dependency on finding valid duplication in entries 2/4. The historical helper-only estimate was below the 10% gate; broad helper rewrites and answer caches remain rejected. This does not defer the separate source-mutation/publication correctness work. |
| 6. Same-source publication and first/repeated queries | Fresh-artifact Full43 and transitive full-value/metadata identity are complete for the retained bytes. Open independent statistics checks and ordinary publication fault/durability acceptance; changed physical output requires its own new-artifact proof. |

### PERF-09 — encoding and bounded writer

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Writer-batch overlap | Dependency on recoverable subtree-tail evidence. The sample does not establish it, and near-zero handoff waits do not justify another live batch. Do not implement overlap merely because allocation failed. |
| 2. Per-column bytes and encode/decode cost | Physical inventory and elapsed codec work exist. Open exact costed-profile acceptance when a concrete profile is proposed; elapsed overlapping spans are not CPU totals and provider estimates are not unique bytes. |
| 3. Execution-aware preparation | Dependency on retained topology/local-operator evidence. No writer rewrite or new count sweep while that prerequisite is absent. |
| 4. Shared array/compression ownership | Owned native input buffers and footer references are retained; provider/source/codec exclusions remain. Open bounded accounting integration for observed omitted allocations, with allocation-denial and progress tests. Do not count the dropped owner rearrangement as solving this. |
| 5. Ordered completed blocks and backpressure | Retained conversion window and one-subtree writer bound. Open pipeline skew/transient-memory acceptance. Any future overlap must also bound later completed segments held behind sequence ordering. |
| 6. Integrity/atomicity and lifecycle decision | Retained numeric compression, ingest value proof and EOF/error/destination tests. Open source mutation, exact publication races and honest durability boundary; unconditional text zoning and smaller-batch layout speed claims remain dropped. |

### PERF-10 — native fused operators

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Encoding/order/selection/ownership contracts | Retained numeric owners, encoded Constant/RunEnd reductions and native scan results. Extend those contracts with every new prepared/result/spill family rather than introducing a parallel physical IR. |
| 2. Metadata pruning before payload work | Existing prepared/aggregate scans consult native file pruning and report full-input pruning. Open differential statistics/pruning fixtures for new predicates/layouts; topology-specific region admission remains tied to its dropped experiment. |
| 3. Region-local ordered candidates | Dependency on retained topology and local reductions. No Top-K implementation from the rejected scheduling packet; preserve exact ties, offsets, NULL ordering and source-row identity if resumed. |
| 4. Filter/project/aggregate fusion | Open extension through existing lowered scan/encoded consumers. Prepared MIN/MAX and computed array results are concrete first compositions. Preserve residual predicate semantics and avoid mandatory Arrow conversion. |
| 5. Cost native operator choices | Open implementation: begin with an existing admitted aggregation pressure choice and measured state/run geometry, then scoped sort/Top-K/join/window choices. A cost model needs truthful width/cardinality/order inputs and explicit unsupported outcomes, not a generic controller added ahead of families. |
| 6. Renamed/nonbenchmark/byte-work validation | Retained independent operator matrices and export evidence cover specific routes. Extend compositions and byte/copy counters with each new family. FoR/BitPacked feasibility showed under-1% fixture gains and small-input regression; no broad expansion without a different material hypothesis. |

### PERF-11 — memory-visible ingest-to-result

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Input/output/memory/queue pressure and cancel | Retained bounded scalar borrowed/owned intake, exact values, lifetime and denial tests. Open coordinated contention/cancellation across session work and memory-generation publication, with original caller/provider exclusions explicit. The in-memory API has no blanket preemptive cancellation guarantee. |
| 2. Remaining bulk-load p50/p95/p99 | Existing 1,000-sample isolated/mixed typed-intake profiles are scoped. Add larger declared row/byte envelopes, wide strings and real queue delays with complete results; separately test file-backed serving. No full-size ingest control is needed for these fixtures. |

### PERF-12 — held-out and comparative acceptance

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Scoring/cohort versus product sessions | Retained Full43 definitions, fresh-process clocks and cached-product separation. Keep independent-oracle versus retained-result comparisons explicit; these packets do not establish official benchmark certification or superiority. |
| 2. Coarse/small/medium/large topology comparison | Scoped Existing/Auto/Fine and earlier Target1 comparisons complete; measured revisions were dropped. Do not fill unused topology counts merely to tick a box after rejection. |
| 3. Operator response classification | Existing topology packets identify actual unchanged/coalesced jobs and lack of material selected-query benefit. Preserve those results and global-work limits. Deeper encoded/pruning attribution is conditional on a new retained implementation. |
| 4. Held-out ingest/aggregate/relational/ownership/serving | Open acceptance: nullable and spilled aggregates, source mutation/publication, file-backed fairness, computed result ownership, then admitted join/windows. Existing 380+80 semantics, 80 required-worker and 1,674 resident checks supersede stale partial-matrix wording only within their scopes. |
| 5. Ordinary/benchmark resource envelopes | Open real memory-pressure and serving completion; use truthful owner counts, queue/state/run peaks and RSS. Keep the existing workspace/log guards and do not resurrect the removed fixed process target. |
| 6. Paired uncertainty, complete values, nonbenchmark gains | Scoped retained export and query families already have evidence; extend it to a credible new candidate. Screen ingest against 95.447305 first and run fresh controls only after credible improvement. Correctness/resource capability can be implemented and validated before a speedup exists. |

### PERF-13 — PGO

| Checklist entry | Reconciliation and next action |
|---|---|
| 1. Remaining instruction/dispatch cost after fusion | Dependency on a dominant measured public cost; current ingest sample shows codec/statistics work, not a demonstrated PGO opportunity. |
| 2. Representative/held-out training | Conditional, not started. Tool lookup and tiny instrument/merge/use smoke are complete; they do not justify a workspace sweep. |
| 3. Build/transfer break-even and portability | Dependency on entry 1 and a bounded candidate. Preserve no native-CPU-only package claim; include build/code size and untrained workloads if resumed. |
| 4. Retain benefit or reasoned drop | Current disposition is conditional defer after feasibility, not measured PGO speedup or whole-family implementation. No additional runtime engine/path is permitted. |

## September 12 ingest sequence and adversarial cases

This maps the separate [ingest sequence](ingest-performance-implementation-2026-09-12.md)
without treating all its unchecked rows as new tuning opportunities.

| Sequence entry | Current disposition / concrete remaining work |
|---|---|
| CPU-stage profile, matched-owner comparison, evaluate fixed allocation (three entries) | Complete; candidate dropped and allocation removed from shipping. No further tuning or control rerun required. |
| CPU-stage tests across narrow grants and each boundary | P1/2/3/4/5/8 codec admission and full values retained; basic EOF/source/conversion failure/teardown covered. Add true source-heavy versus codec-heavy skew, codec-blocked cancellation and omitted owner pressure. |
| Overlap: identify useful idle time | Conditional evidence missing. Keep deferred; this is not a blocker for independent query/result/spill work. |
| Overlap: byte-admitted futures, ordered/EOF drain, end-to-end tests (three entries) | Dependency on useful overlap evidence. Existing ordered conversion/subtree tests are retained; no claim that multiple writer subtrees currently execute concurrently. |
| Repeated work: attribute copies/conversions/probes | Partially measured. Add exact copy/traversal counts only at a specific reuse seam; estimates and work spans cannot prove duplication. |
| Repeated work: same-generation representation reuse | Existing identity-projection bypass and memory-generation single serialization are scoped implementations. Ordinary intake-copy removal needs equivalent immutable owner/credit transfer; it is not justified solely by memory-generation behavior. |
| Repeated work: preserve compression/statistics/readback | Retained invariant. Test any new representation against required dtype, NULLs, order, complete values and metadata before performance acceptance. |
| Repeated work: ownership/Unicode/extrema/encoded/drop tests | Existing scoped coverage; extend to actual reuse candidates and ordinary source mutation. No speculative broad rewrite is required. |
| EOF/error/cancel matrix | Retained delayed source/final partial/empty batch, source/conversion error and cooperative prefetch cancellation. Open codec-completion blocked, skewed later completed work and faults after partial segments. |
| Transient memory/skew matrix | Open focused implementation/tests for overlapping original inputs/native copies/scratch/footer references and variable-width/page skew. Reservation release does not substitute for transient peak observation. |
| Source/publication races | Open correctness work: mutate/truncate/replace during ingest; same-size mutation; target replacement at commit boundaries; precise post-publication failure status. Existing destination-appeared test covers only one point. Ordinary publication flushes/renames without file/directory fsync; either preserve this explicit scope or implement/test a separately stated durable contract. |
| Fresh-artifact queries/metadata | Current retained bytes pass all 129 complete results; SHA identity links complete native values/schema and physical metadata. Independent statistic correctness remains open; add conservative-pruning differential tests rather than regenerating the same 18.6 GB artifact. |
| Serving fairness | Open bounded file-backed small-query stream during long work. Measure separately from exclusive ingest, with the same shared grants and explicit source ownership. |
| Held-out shapes | Open extension beyond the retained renamed nullable/precision-sensitive fixtures: numeric/text-heavy, low/high cardinality, skew and source-mutation cases. Use existing source/runtime paths. |
| Five measurement/retain-drop steps | Active rules: predeclare candidate and stop gate; focused/broad appropriate checks; reuse baseline until credible improvement; complete changed-output proof before retirement; preserve source/build/receipt identities and limitations. These rules recur per coherent change; they are not five unperformed benchmark runs. |

## Remaining production epics in the phased plan

The following tables cover every unchecked nested row outside PERF-01 through
PERF-13. `L` locators refer to line numbers in `phased-execution-plan.md` at
`a8775c4f`; they identify existing rows, not new phase IDs. Each of the five
top-level epics remains open for its stated gaps. Their `required_for_v1`,
provider-feasibility and unsupported remote/distributed classifications remain
unchanged. Historical query numbers identify evidence only; implementations must
admit ordinary types, encodings and operator semantics.

### CLICKBENCH-PRODUCTION-WRITER-PHYSICAL-DESIGN-1

The older 271/301-second baselines and 38.1 GB representation are historical.
Current writer decisions must preserve the retained 95.447305-second baseline
and 18,591,586,804-byte representation. The retained source, conversion and
provider workers, bounded subtrees and measured numeric compression supersede
the old premise that the writer has only policy constants. They do not prove
every requested stage shares complete transient-memory accounting.

| Existing unchecked row | Reconciliation and concrete next action |
|---|---|
| L729. Decoupled ordered pre-writer pipeline | Retained bounded source/conversion stages, sequence order and one-subtree writer; the separate September 3 derived-prefetch/wider-queue candidate was dropped. Remaining work is the September 12 skew, codec-blocked cancellation and ownership matrix. Add another stage or live subtree only after demonstrating useful waiting and bounded storage for completed later work. |
| L760. Dictionary-lifted derived construction | Scoped dictionary-derived value/validity paths exist. Forced source dictionary layout, plain-value replay cache and typed plain-UTF8 rewrites were rejected. Open implementation only at a real source dictionary or bounded native derived-metadata seam that preserves the retained writer representation; first prove unique-value transforms/code replay and complete NULL/Unicode/temporal parity. Do not retry row-level caches or change source text layout merely to enable lifting. |
| L835. Single resource governor | Retained `IngestCpuLanes` assigns constructed owners through the existing resource grant; P4 allocation reshaping was measured and dropped. Open accounting/progress work for source/provider scratch and transient copies. Test narrow grants, denial, cancellation and writer progress. Live idle-lane borrowing is a distinct design obligation, not permission to add another scheduler. |
| L879. Layout/codec portfolio admission | Numeric compression has a retained lifecycle packet. The proposed automatic sample-write selector is still open implementation: reuse existing profile/advisor inputs, evaluate isolated bounded candidate writes, make selection deterministic and clean rejected candidates. Start only with distinct source-grounded candidate profiles; a report wrapper or topology-count sweep does not satisfy it. Whole-artifact ramp and changed-output query proof remain required before publication/retention. |
| L935. Source-to-commit bounded pipeline | Partially retained by current source/conversion/provider pipeline and single-artifact staging. Complete source mutation/publication-race and transient ownership acceptance under PERF-08/09; ordinary rename publication does not promise file/directory fsync durability. This is the same pipeline work as L729, not a duplicate implementation. |
| L938. Parallel CPU pre-writer work and clocks | Real source/conversion/provider owners and work clocks are retained. September 12 owner sampling establishes occupancy, not exclusive stage CPU time. Integrate attribution for a specific changed stage only; the negative codec-owner transfer does not justify tuning by requested parallelism or relabeling owner counts as utilization. |
| L941. Writer retain/drop gate | Retained guarded runner, explicit candidate screen and evidence discipline now implement this rule. Use 95.447305 seconds and the current artifact, preserve failed receipts, and require credible candidate improvement before another control. No new benchmark is needed to close the stale 271-second wording. |
| L943. Remove September 2 rejected tuning | Completed cleanup; the rejected 360-second patch is not the retained runtime. Its removal does not require a new writer experiment. |
| L945. Planner matrix | Existing advisor, small/large source, dictionary, row-count and resource tests cover portions of this matrix. Open a single table-driven acceptance review across small, numeric, wide text, dictionary-heavy, missing-row-count and constrained-memory profiles; add missing combinations and assert real chosen plan/admission, not merely report presence. |
| L948. Stage/resource/artifact harness fields | Existing records contain actual owner allocation, bounded queue/work spans, artifact size and fallback facts. Open precise transient-memory and stage-progress observation for the uncovered cases above. Do not expand every historical field into continuous telemetry or claim the stack sample measures stage CPU utilization. |

### CLICKBENCH-PRODUCTION-SEGMENT-METADATA-PRIMITIVE-1

`metadata_summary` and `metadata_pruning` already contain typed physical facts
and conservative proof. Prepared scans already use native file pruning.
`VortexSegmentMetadataPrimitiveReport` also contains availability/admission
posture strings; those strings do not prove ngram summaries, frequency maps or
candidate directories exist or were consumed. Extend this seam before adding
another metadata model or touching the artifact writer.

| Existing unchecked row | Reconciliation and concrete next action |
|---|---|
| L1022. False-negative-safe string absence | Open bounded implementation. First admit an existing exact dictionary/min/max proof for one supported literal predicate through the real scan consumer. If that cannot avoid enough work, evaluate a conservative dictionary-derived string summary with explicit byte budget, generation/provenance and incompatible/missing-summary read-through. Test empty strings, NULLs, Unicode, case semantics, short literals and hash collisions before a persistence proposal. Retain new metadata only with measured avoided reads/decode and acceptable ingest/storage cost. |
| L1038. Stable transform/frequency summaries | Runtime dictionary transforms, weighted counts and complete-key partitions are retained; persisted cross-chunk transform/frequency identities remain open. First specify same-generation identity, NULL/overflow and exact count contracts for two existing consumers, estimate bounded metadata bytes from real dictionaries, then implement only if joint ingest/query work can improve. A cached answer or a code reused across unrelated dictionary domains is forbidden. |
| L1057. Candidate segment directories | Open per-segment metadata work; the retained in-memory numeric-pair duplicate directory is a different object. Start with an existing exact duplicate/key statistic and test conservative strategy selection for all-unique, all-duplicate, skew and missing summaries. Add physical metadata only if it safely eliminates reads or materially improves an admitted exact operator; absent/approximate facts must retain full work. |
| L1073. Consumption by pruning/grouping/Top-K/advice | The predicate-pruning bridge already executes: [prepared scan](../../shardloom-vortex/src/local_primitive_prepared_scan.rs) calls `VortexFile::can_prune` before constructing a scan, returns no arrays for a proven empty input, and retains source-generation validation. The ordinary scan in [local primitives](../../shardloom-vortex/src/local_primitives.rs) does the same; surviving filters enter Vortex's native file/zone-stat pruning. Pinned Vortex 0.85 binds only exact file statistics and reads through when proof is unavailable. Do not add another metadata model or dispatcher to recreate that bridge. Shared grouping, Top-K, candidate and advisor consumption remains open; availability/admission strings do not establish those consumers. |
| L1076. False-negative tests | Open independent matrix extending existing min/max conjunction tests: NULL count, byte length, dictionary membership, domain absence, empty/all-NULL/mixed-NULL and missing/corrupt/incompatible metadata. Compare complete results with pruning disabled on independent fixtures. A byte-identical artifact or matching full values does not independently certify physical statistics. |
| L1080. Query-consumption evidence | Reuse actual scan/source fields for total/read/pruned work and add only missing exactness/provenance at the consumption boundary. Where bytes are estimates, label them; zero materialization after whole-input pruning must be proven by the executor, not inferred from an admission report. |
| L1082. Targeted then full acceptance | Conditional on the specific implementation above, not a request to rerun all historical lanes now. Use semantic-class fixtures first and exact query outputs; retain a metadata candidate only with work benefit and joint lifecycle evidence, then run the applicable full acceptance. |

### CLICKBENCH-PRODUCTION-MORSEL-SCHEDULER-THREADLOCAL-MERGE-1

The checked scheduler contracts and later `AggregateChunkJobs`/COUNT workers
already provide bounded work, shared admission and deterministic completion.
The correct extension is another concrete state family in that machinery.
String and integer/text COUNT no longer need a speculative heavy-hitter sketch
to satisfy the old packet's intended parallel exact result.

| Existing unchecked row | Reconciliation and concrete next action |
|---|---|
| L1142. Thread-local state families | Retained scoped COUNT, compound COUNT and integer grouped DISTINCT state families. Open three-key/multi-measure/null-aware states, then independent row-reference Top-K/predicate-count families where existing execution still serializes useful work. Define merge/cancellation/ownership semantics with the first real consumer; do not add an unused generic trait hierarchy. |
| L1144. Mergeable string heavy hitters | Superseded in the retained non-null COUNT scope by exact complete-key string partitions and final Top-K, with shared jobs and no candidate recount required. Open nullable/extra-measure scope and worker spill transfer; do not restore sketch/dual-maintenance overhead solely to match historical wording. |
| L1159. Numeric-plus-UTF8 candidate/recount | Retained scoped compound COUNT workers, dictionary-domain binding and exact partition reconciliation supersede the old Q17-only motivation. Open NULL keys, additional measures and pressure-to-run completion using the same owned compound state. |
| L1175. Three-key grouped state | Open concrete family: extend existing numeric-minute-string direct execution into the shared worker/partition contract, retaining independent value-domain identity for strings and exact numeric/temporal semantics. Prove deterministic merge and bounded growth on all-unique, duplicate-heavy and skewed data; admit a radix/duplicate strategy only after that strategy has exact independent proof. This need not wait for topology changes. |
| L1190. Packed-pair and grouped DISTINCT workers | Integer grouped DISTINCT workers and exact pair union are retained; mixed-measure packed-pair preunion and near-unique pair selection are also real scoped kernels. Open production spill transfer and remaining parallel packed-pair/multi-measure shapes. Do not confuse a serial near-unique directory with worker parallelism, or reimplement the retained directory. |
| L1208. Deterministic merge for every family | Open family-by-family acceptance beyond retained scopes. Extend NULL ordering, duplicate/tied values, source identity, limit/offset, integer overflow and original floating accumulation policy at each new merge seam. Generic scheduler determinism alone is insufficient. |
| L1214. Actual idle/starved/backpressure observation | Open selective execution observation where jobs really run. Existing submitted/completed/worker/source counters are useful; attach wait/backpressure observations only when diagnosing a candidate and distinguish source overlap from operator work. Do not count the generic observer's synthetic contract test as native operator utilization. |
| L1216. Convert the listed hot families | Partially retained for single/compound COUNT and scoped DISTINCT. Three-key, numeric-pair multi-measure and remaining scalar/grouped state integration are explicit residuals. Record per-family execution proof and remove superseded route-local work only after parity, rather than introducing a second scheduler to tick the epic. |
| L1221. Skew and tie cases per state | Open extensions for newly admitted families and production spill handoff. Use intentionally slow/empty/fully pruned morsels, reordered completion, unequal dictionary domains and ties crossing worker boundaries; assert complete results, bounded retention and credit release. |

### CLICKBENCH-PRODUCTION-SPECIALIZED-KERNEL-REGISTRY-1

The registry already has typed contracts and deterministic admission.
`admit_vortex_specialized_kernel` explicitly performs no I/O and executes no
kernel. Traditional fixture integration exists, while broad local-query runtime
families still need a truthful admission-to-execution bridge. Keep one registry
and existing consumers; a selected descriptor cannot certify executed work.

| Existing unchecked row | Reconciliation and concrete next action |
|---|---|
| L1280. Reclaimable string arena | Measured drop for the generation-checked hot-eviction arena: historical Q34/Q35 times regressed to 32.117/32.430 seconds despite much lower active storage. Retained partition/dictionary ownership is a different solution. Reopen reclamation only for a demonstrated remaining owner lifetime problem and a lower-overhead/off-hot-path design, not a universal slab rewrite. |
| L1303. Constant-key canonicalization | Literal-integer ordinal grouping is retained and shares the physical string route. Open generalized string/NULL constants and other provably constant expressions: retain logical output columns independently, preserve dtype/NULL/empty/group semantics, reject uncertain constancy and test ordering/rendered results. This is a concrete planner/runtime completion item, not a new Q35 optimization. |
| L1326. Exact string histogram replay | Dictionary weighted updates, bounded exact histograms and complete-key partitions are retained for admitted families; the latter already removes its second recount scan. Sparse candidate-code replay and several broader histogram candidates were measured drops. Open only the remaining consumer/metadata reuse and nullable/extra-measure scopes; require a real remaining recount or transform cost before another replay experiment. |
| L1494. Packed numeric-plus-UTF8 Top-K | Retained dictionary-code binding, candidate-to-numeric partition reuse and later compound worker partitions close major parts of the old packet. Open bounded nullable/extra-measure/result-array scope. Preserve exact value-domain equality and tie ordering; rejected bitmap/typed micro-variants are not prerequisites. |
| L1592. Weighted transformed-domain aggregate | Retained interned transformed-domain keys and weighted dictionary/general updates exist. Open remaining fused measure/result ownership or shared exact transform metadata only where it removes measured work. Use arbitrary admitted transforms/schemas with NULL/Unicode/overflow proof; do not restore rejected transform memoization helpers or add answer caching. |
| L1765. Encoded predicate/selected-row aggregate | Retained column-scoped residual-AND candidate refinement and dictionary/FSST/native predicates avoid already-proven work. Open selected-measure ownership and grouped result handoff through existing selections; prove which measure payload is read/decoded only for selected rows. This shares the result epic's implementation. |
| L1897. Fixed-width three-key grouped Top-K | Retained cached-worst and numeric-minute-string direct grouped updates are scoped. The three-key worker/merge family at L1175 and exact native result assembly remain open. One implementation can close both rows with state memory, complete keys/ties and actual worker evidence. |
| L1955. Duplicate-promotion packed-pair aggregate | Retained exact near-unique directory stores seen packed keys, promotes duplicates and fills singleton ties before late measures; focused native tests and historical 6.794-second acceptance exist. The later per-row ordered singleton-window variant was dropped. Open nullable/parallel/pressure extensions only where needed; no Bloom/radix implementation is required merely because the retained exact directory uses a different valid representation. |
| L2025. Exact DISTINCT family split | Retained packed pair preunion, chunk group partials and later pure grouped-integer DISTINCT workers cover real families. Open remaining multi-measure/dtype/NULL/pressure families with complete pair identity and explicit spill contract. Existing historical speedups are not evidence for a new family. |
| L2102. Register production kernels | Open actual local-query execution linkage for uncovered families: derive the typed candidate from real dtype/layout/operator facts, pass existing admission, invoke the existing consumer and certify completed work. Start with the next implemented family; do not replace its ordinary native path with a report-only dispatcher or fail previously supported behavior on a descriptor mismatch. |
| L2107. Decoded-reference kernel parity | Existing scoped operator and registry tests are not a complete cross-product. Add each real bridge's empty/NULL/cardinality/Dict/primitive/unsupported cases, including mismatch between advertised and actual encoding. Use complete values/order, stable unsupported evidence and no external runtime fallback. |
| L2112. Correctness/performance/work retain gates | The current retained/drop packets implement this discipline for measured kernels. Apply the same rule to new bridges: descriptors alone cannot close a production kernel; a capability/correctness improvement needs explicit proof and honest timing scope, while a speed claim requires reproducible benefit. |

### CLICKBENCH-PRODUCTION-COLUMNAR-RESULT-DATAPLANE-1

The shared result contracts, source selection/projection arrays and direct
native-array compatibility export are retained. `NativeSinkPlan` still admits
source projections/filters and source-column expressions; that does not cover
every computed aggregate/Top-K result. Extend the retained owned-array/result
handoff rather than introducing another result data plane.

| Existing unchecked row | Reconciliation and concrete next action |
|---|---|
| L2167. Selected-row aggregate and retained-row handoff | Open computed-family integration. Carry an existing predicate selection into selected column/measure ownership, construct the aggregate's exact output arrays and render only at the declared user-row boundary. Test actual avoided payload work; this is shared with PERF-07/10 and L1765. |
| L2183. Exact aggregate sink parity | Open adapters for admitted DISTINCT, compound/three-key Top-K, transformed grouping and pair late-measure state. Start with one coherent scalar/grouped family, preserve dictionary/string ownership and validity until sink assembly, and replay Vortex/IPC/Parquet complete values. Preserve compatibility metadata-loss reports. |
| L2198. Broad columnar route coverage | Projection/filter is retained and measured export is scoped. Computed aggregate/ordered/distinct outputs remain open, followed by expression and multi-source families with their own semantics. Track each actual route from state to sink; an existing `ColumnarResultBatch` descriptor does not close them. |
| L2204. Ordering/NULL/wide payload/output parity | Open per-new-family acceptance covering ties, output projections, NULL rendering, wide payload, grouped/distinct results, JSON/CSV and blocked remote delivery. Include input/session drop, sliced/retained owners, output limits and failed/cancelled publication; compare complete results across direct native and rendering paths. |
| L2206. Targeted then full result acceptance | Retained source-array IPC/Parquet export has its own packet; it does not certify computed aggregate sinks. For a new family, first prove fewer materialized rows/bytes or conversions on representative small and large outputs, then applicable targeted/full correctness and timing acceptance. No unconditional rerun of historical query numbers is required. |

## Completion rule

Work is exhausted only when each applicable open implementation/acceptance row
has a concrete retained result or an explicit, scope-specific measured drop or
real prerequisite. A lack of pre-existing benchmarks is not such a prerequisite.
The immediate spill, prepared-family, result-ownership and adversarial acceptance
work remains executable under RFC 0044. The older epics also retain explicit
constant-expression, three-key state, metadata-consumption and result-family
work as mapped above. General join spill needs a concrete native contract before
coding; topology-dependent work and PGO keep their stated gates.

Source review for this inventory ran no native builds, tests or benchmarks.
Implementation owners must attach focused correctness/resource proof, required
checks and material work/performance evidence at their next coherent completion
point. Preserve one Vortex-native engine/artifact, existing schedulers, explicit
unsupported behavior, no external fallback, no answer cache and complete results.
