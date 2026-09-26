# Performance candidate intake — September 26

Status: execution authorized by the maintainer on September 26;
`claim_gate_status=not_claim_grade`. Reviewed against merged `main` at
`6db17c9f`, after 0.3.0 publication. The initial intake contained no runtime
change or new performance measurement. R1.a is now retained under its
[storage gate](derived-dictionary-preservation-2026-09-26.md): 15.64% fewer bytes,
complete values and query results, and bounded regression follow-up. R6.a is
retained under its [query gate](native-sort-block-screen-2026-09-26.md): the final
paired Q26/Q27 calls save 1.49/1.27 seconds, with complete Full43 validation and
bounded Q35 follow-up. R7's numeric-residual and conditional-dictionary variants
are [dropped at the bounded analytical screen](cross-column-storage-screen-2026-09-26.md);
no native format or runtime change is retained. R1.b's raw and compressed source
dictionary variants are [dropped after exact native screens](source-dictionary-screen-2026-09-26.md);
the retained source-text writer remains unchanged. R6.b's FSST/predicate variant
is [dropped after exact native screening](fsst-consumer-screen-2026-09-26.md)
for larger samples and predominantly slower native predicates. R1.c's proposed
transform sharing is [already present in the inspected active paths](shared-domain-expression-audit-2026-09-26.md);
the duplicate proposal is dropped, with RunEnd-specific expansion still requiring
a distinct measured target. R5.a is next. The
[phased plan](phased-execution-plan.md#planned) owns execution order and progress;
the labels below identify experiments, not new phases.

The five supplied packets are expanded into **29 separately decidable experiments**,
plus three deferred directions and the existing shipped/dropped exclusions.
Aliases and repeated analogies are consolidated; independent mechanisms keep
their own entries. Labels retain the earlier R/C family mapping, with suffixes
for formerly bundled experiments. They are not phase IDs or priority numbers.

**Priority is potential scale of avoided work and breadth of benefit**, with
necessary dependencies respected. Evidence strength controls admission, not a
blanket preference for easier, smaller query fixes. A structural idea without
attribution receives a bounded screen; it is not automatically postponed behind
a query lane. These are hypotheses, not forecasts of seconds saved. Storage
bytes, ingest time, query-suite time, composed workflows and concurrent throughput
remain separate outcomes.

## Evidence boundary

The latest [composition acceptance](native-result-composition-2026-09-20.md)
records 63.459160 s for Full43, with all 129 complete outputs checked; it is an
unpaired observation. The last [full-ingest observation](../benchmarks/combined-performance-uat-2026-09-12.md)
is 95.923669 s and the retained artifact is 18,591,586,804 bytes. The
[physical inventory](native-storage-reduction-2026-09-12.md#initial-physical-evidence)
belongs to a different, 18.643 GB reference artifact: 3.531 GB of plain derived
domain strings and 7.595 GB of framed Zstd text. Refresh the exact retained-file
inventory before admitting a storage change.

The [Q29](q29-utf8-dictionary-validation-2026-09-20.md),
[Q10](compound-group-storage-2026-09-20.md) and
[Q19](q19-complete-key-partitions-2026-09-19.md) records show complete calls of
7.815263, 4.447893 and 6.102660 s respectively. Each bounds that lane's possible
absolute saving; optimizing one stage cannot erase the whole call. Shared
consumers may benefit other lanes, but that needs separate evidence. These are
different experiments, not components to subtract from the 63.459160 s suite.

## Ordered experiment inventory

Every row is an admission/ship/drop decision. A failed screen records the reason
and advances; acceptance of a screen does not imply retention of its prototype.
The common gates below apply, with no duplicate credit for overlapping savings.
After a structural change, reprofile its downstream candidates before implementing
them. Dependencies may move a necessary provider/build check earlier, in isolation.

### Structural opportunities first

| Order / ID | Independent experiment | Benefit surface and admission evidence |
| --- | --- | --- |
| **1 — R1.a** | **Encoded derived generation → persistence → consumption** | Ingest, bytes and repeated queries. Refresh physical bytes/encoding survival and recover the rejected text recipe. Remove expanded helper strings and read-side rebuilding together; ingest/storage/suite gate. |
| **2 — R6.a** | **Fused encoded block execution** | Multiple scan/predicate/aggregate families. Identify a costly expand → intermediate → reread boundary; consume exact decoded blocks or encoded values directly with existing kernels. Suite/query/workflow gate. |
| **3 — R7** | **Cross-column residuals and conditional dictionaries** | Storage and read traffic beyond independent codecs. Screen actual bytes including references/exceptions and single-column access cost; test the two encoding schemes separately. Storage gate. [Corra/C3](https://vldb.org/workshops/2024/proceedings/CloudDB/clouddb-2.pdf). |
| **4 — R1.b** | **Selective preservation of economical source dictionaries** | Ingest, stored text and broad string consumers. Trace where incoming codes/domains are expanded; compare against retained Zstd including construction/remapping. Ingest/storage/suite gate. |
| **5 — R6.b** | **FSST paired with its encoded predicate consumer** | Text storage and predicate families. Prove the actual provider reaches the admitted encoded kernel; charge training, write and other consumers. Storage/suite/query gate. |
| **6 — R1.c** | **Shared expression evaluation over dictionary/run-end values** | Multiple derived expressions and predicates. Find repeated length/domain/predicate work on the same native domain; reuse codes, truth tables or exact multiplicities without row expansion. Suite/query/ingest gate. |
| **7 — R5.a** | **Direct owned-array handoff between operations** | Large intermediate results and composed workflows. Remove documented memory-file serialization using the same downstream kernels. Workflow/memory gate; not an automatic Full43 gain. |
| **8 — R9.a** | **Prepare/seal native fragments once and reuse encoded payloads** | Large-ingest conversion, serialization and publication. Extend the existing segment lifecycle only where a measured duplicate representation/copy remains; test reuse without adding overlap first. Ingest/workflow gate. |
| **9 — R9.b** | **Bounded writer-subtree overlap** | Complete ingest. Attribute recoverable batch-tail idle time and ready work before changing concurrency; retain the existing writer and sequence machinery. Ingest gate. |
| **10 — R8** | **Shared scan/decode producer for concurrent queries** | Concurrent throughput and latency. Measure duplicated source work, then serve compatible consumers with separate state and cancellation. Serving gate, not the isolated-query score. |
| **11 — C2.a** | **Pre-bound executable block recipes** | Repeated setup/dispatch across query families. Bind physical encoding/type/validity/kernel contracts once when compatible; measure setup remaining after existing prepared lowering. Suite/query/workflow gate. |
| **12 — R5.b** | **Transfer reservation-owned pages/buffers across stages** | Intermediate/spill copying and live memory. Identify a specific remaining copied owner; transfer its lifetime and credits without rebuilding content. Memory/workflow/suite gate. |
| **13 — R5.c** | **Bounded read/compute/output overlap in native workflows** | Scans and result-producing chains. Find complementary stages and recoverable idle intervals; this is separate from ingest writer overlap. Workflow/suite gate, including queued bytes. |
| **14 — C4** | **Compact immutable dictionary directories** | Repeated dictionary lookup/building and retained memory. After useful dictionaries survive, identify a necessary fixed-key directory; charge construction and reuse break-even. Memory/suite/workflow gate. [PtrHash/PTHash](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.SEA.2025.21). |

R1.a/b/c are distinct decisions: persisting derived values, preserving incoming
dictionaries, and sharing computation over a domain can win or fail independently.
Likewise, R9.a's byte reuse does not require R9.b's concurrency, and R5.a's
serialization removal does not require page redesign or stage overlap.

### Broader CPU, provider and input-family opportunities

| Order / ID | Independent experiment | Benefit surface and admission evidence |
| --- | --- | --- |
| **15 — C3** | **AMAC-style lookup interleaving** | Shared irregular lookup phases. Establish dependent memory stalls in an ownership-stable read-only phase; compare bounded interleaving widths on the same structure. Suite/query/workflow gate. [AMAC](https://www.vldb.org/pvldb/vol9/p252-kocberber.pdf). |
| **16 — C6** | **Exact packed numeric predicates** | Expensive numeric scans still expanding data. Check existing Vortex kernels first and charge any new layout/transposition. Suite/query gate. [BitWeaving](https://15721.courses.cs.cmu.edu/spring2016/papers/li-sigmod2013.pdf). |
| **17 — C2.b** | **Finite cost-based selection among exact implementations** | Workloads where one correct hash/sort/dictionary/tile choice is consistently wrong. Use measured physical characteristics and bounded preparation/offline selection. Suite/workflow gate including selection cost. |
| **18 — C1** | **Isolated Vortex provider upgrade** | Potential scan, unpacking, dictionary and allocator improvements across families. Workspace selects 0.85; lockfile resolves 0.85.0. Review API/format/resource compatibility against verified [0.86.0](https://github.com/vortex-data/vortex/releases/tag/0.86.0)/[0.86.1](https://github.com/vortex-data/vortex/releases/tag/0.86.1), holding ShardLoom policy fixed. Suite/query/ingest gate. |
| **19 — C5.a** | **Existing ThinLTO build** | Cross-crate optimization across workloads. Hold source, dependencies, artifact and runtime fixed. Build gate; not a new runtime architecture. |
| **20 — C5.b** | **Trained PGO** | Broad residual CPU costs. Instrument/train/merge/use, with representative ingest/query/pressure training and disjoint evaluation. Compare against C5.a's selected control; build gate. |
| **21 — C5.c** | **Explicit CPU-targeted build** | Hardware-specific kernels/code generation. Compare separately from portable distribution, with unchanged validation/error semantics. Build gate. |
| **22 — C7** | **Direct JSON/JSONL parse into typed builders** | A separate input workload; no Parquet-ingest saving. Profile temporary object construction, then test direct typed construction and bounded buffer reuse with identical validation. Ingest gate. [On-Demand parsing](https://github.com/simdjson/simdjson/blob/master/doc/ondemand_design.md). |

### Targeted query-family improvements after the structural screens

| Order / ID | Independent experiment | Cost signal and admission evidence |
| --- | --- | --- |
| **23 — R2.a** | **Source-backed UTF8 dictionaries, Q29 first** | Historical accessor span 5.554641 s; 25.77 million entries and 3.121 GB copied cumulatively. Recheck after representation work; preserve IDs/order and promote escaping values. Query/memory gate. |
| **24 — R3.a** | **Winner-only exact DISTINCT, Q10 first** | Measure the winning groups' share of rows/pairs; prove DISTINCT cannot affect selection and charge every rescan. Query gate. |
| **25 — R3.b** | **Mixed-measure exact-DISTINCT workers** | Existing single-measure/order guard excludes Q10. Preserve every row's ordinary measures independently of pair deduplication; extend result/order contracts. Query gate. |
| **26 — R4** | **Sort/reduce in existing triple-key partitions, Q19 first** | About 56.38 million groups in the retained record. Separate producer work from map updates and price all input/sort scratch. Query/memory gate. |
| **27 — R6.c** | **Progressive provider selection, Q23 first** | [Attribution](performance-ship-drop-2026-09-19.md): 4.205 of 4.231 accessor seconds is provider execution. Prove selective masks avoid later frame execution; distinct from general fused consumption. Query gate. |
| **28 — R10** | **Dense single-string COUNT payloads, Q34/Q35 first** | [Prior screen](q34-q35-reconciliation-screen-2026-09-20.md) leaves full records in sparse slots. Measure simultaneous capacity/records/arenas/growth before extending compound pages. Memory/query gate. |
| **29 — R2.b** | **Bounded parallel dictionary preparation** | Only if R2.a and upstream representation changes leave material setup work. Consume in original chunk order and bound all retained owners. Query gate. |

## Necessary contracts

- **Representation (R1/R6):** use existing Vortex concepts/providers first.
  Preserve source generation, dictionary epochs, exact transformed-value equality,
  null/error behavior and actual encoded-kernel evidence. Several source values can
  map to one derived value. Keep other codecs unchanged while testing each variant;
  FSST is selective, not a global replacement. Record input → expression → batching
  → persistence → scan → consumer encodings.
- **Cross-column/static lookup (R7/C4):** require shallow acyclic read dependencies,
  exact residuals/nulls/overflow and reader compatibility. A perfect hash needs full
  key membership verification for arbitrary input; it does not replace strings/codes.
  Existing row codes remain preferable to unnecessary lookup.
- **Ownership/pipelines (R2/R5/R8/R9):** retain reservations through every consumer,
  queued output and cancellation/drain. Preserve typed empty schemas and selection/
  validity. Copy small escaping survivors when borrowing would pin large buffers.
  File-backed addressing remains for persistence/spill/pruning. Shared consumers
  keep independent state/errors/cancellation and bounded slow-consumer retention.
  Fragment reuse must produce one valid artifact with actual statistics/footer.
- **Aggregates (R3/R4/R10):** preserve complete keys, overflow, floating update order,
  nulls, offsets/ties and observable errors. DISTINCT deduplication cannot remove
  ordinary measure contributions. Deferred evaluation needs complete dependency and
  error proofs. Preserve the triple-key nullable timestamp admission proof; choose
  hash/sort before committing state. Price extra indirection, transient growth and
  spill rather than relying on cumulative allocation counts.
- **CPU/provider/input (C1/C2/C3/C5/C6/C7):** schema equality alone cannot authorize
  recipe reuse across encoding/validity/dictionary/resource changes. Select only
  correct paths and reset mutable tuning state. Drop AMAC if bandwidth/compute
  dominates; no unsafe references across table mutation. FastLanes bytes are not
  BitWeaving bytes. Provider changes need compatibility/provenance review.
  Preserve malformed/skipped JSON validation, duplicate keys, exact numbers,
  escapes, missing/null and schema behavior. No hidden engine delegation.

## Deferred directions, explicitly retained in the inventory

| Direction | Reopening condition |
| --- | --- |
| Speculative prefetch | Measured avoidable I/O wait and a bounded useful prefetch window. Predictions cannot exclude rows or groups without an exact proof. |
| GPU/device execution | A substantial admitted operation with favorable residency and sufficient work after charging transfers, packing, synchronization and scratch. No CPU-workload speedup is assumed. |
| Direct random-access extent placement | After fragment reuse/overlap, measured assembly or ordered-emission copying still limits the lifecycle; charge alignment, gaps and reservation overhead. |

## Ship/drop gates

These are acceptance objectives, not predicted results. Existing **query** and
**memory** gates are retained; the other numerical gates below are proposed for
this intake and must be frozen with the workload before an experiment begins.

- **Suite:** for changes spanning multiple query families, at least 10% lower
  Full43 best-sum and 5% lower geometric mean, with no material family regression
  and confirmation on independent workloads. Use matched complete calls; do not
  add unrelated historical improvements or require every lane to save one second.
- **Query:** at least 1 s saved per complete target query, including CLI startup,
  full output and exit. **Memory:** at least 30% lower OS peak RSS with no complete
  time regression. Report reservations and actual RSS separately.
- **Ingest:** at least 10% lower complete durable ingest, without a larger artifact
  or material query regression. **Storage:** at least 15% fewer total artifact
  bytes with no ingest or affected-query regression for default retention. If
  preparation becomes slower, keep it conditional until an explicit reuse workload
  demonstrates lower total `ingest + N complete queries` cost; record break-even N.
- **Workflow:** at least 20% and 100 ms saved in the complete composed operation,
  including construction, output and release; or the memory gate. This lower
  absolute threshold cannot replace the one-second gate for a heavy query.
- **Serving:** at least 20% more completed requests/second at the same admitted
  load/resources, without worse p95/p99 or short-query progress. Freeze a sufficiently
  sized arrival trace, sample count and tail tolerance before running; include queue
  time, errors, cancellation and final reservation release. Best-call timing is not
  the statistic for this gate.
- **Build:** at least 5% lower Full43 best-sum **and** geometric mean with no material
  family regression, plus nonregressing ingest and held-out workloads. Measure a
  provider upgrade by its targeted gate instead when that is its actual objective.

For query comparisons use the same symmetric fastest-valid-run rule for both
roles, retain every sample and full output, and compare matched source/artifact/
policy identities. Faster valid samples establish achievable performance; slower
loaded samples do not alone veto retention. A regression screen flags a query
slower by both 10% and 150 ms for focused paired investigation, not automatic
rejection of a noisy observation. Do not substitute overlapping worker spans for
exclusive wall savings. Record CPU, live memory, read/decode/copy work and route
activation alongside elapsed time.

Each admitted experiment: focused semantic/ownership/failure tests → isolated
paired screen → retain or remove → cohesive PR after full acceptance. Retained
query/provider changes require complete Full43 and independent renamed/null/skew/
collision/arithmetic fixtures; writer changes additionally require full guarded
replacement ingest, full values/schema/statistics and native reopen checks. Run
workspace fmt/Clippy/tests and applicable native-feature gates at the cohesive
boundary. SQL/Python/CLI must reach the same optimized family. Once the finite
packet is exhausted, refresh profiling before generating another list.

## Consolidation and exclusions

| Supplied idea | Disposition |
| --- | --- |
| FlashAttention-style intermediate removal, exact unpack/consume fusion, native “pass-through” | Merged into R1/R2/R5/R6. The transferable mechanism is fewer intermediate bytes, not attention arithmetic or lossy quantization. [FlashAttention](https://arxiv.org/abs/2205.14135). |
| PagedAttention-style ownership; captured execution; continuous batching/source reuse | Merged into R10 and R2/R5/R9 ownership, C2 preparation, and R8 source sharing. Existing dense pages and prepared sessions are already addressed foundations. [PagedAttention](https://arxiv.org/abs/2309.06180), [graph preparation/replay](https://docs.nvidia.com/cuda/cuda-programming-guide/04-special-topics/cuda-graphs.html). |
| Speculation, prefetch, learned selection | Only C2's bounded exact-path selection is admitted for screening. Speculative prefetch is deferred until avoidable I/O wait is measured; predicted winners cannot exclude unseen keys without proof. |
| Q13 recount removal, Q18 early key filtering, Q19 partitioning, Q33/Q36 reductions, Q17 dense state; memory-file segment publication; numeric post-coalescing compression | Already addressed in the retained implementation. Extend them; do not count them again as new candidates. |
| Q29 owned weighted partials, Q23 dictionary rewrite, Q34/Q35 growth-only/probe tweaks, rejected text-storage recipe, conversion-owner reassignment | Dropped variants stay dropped. New rows above change the mechanism and require their own evidence. |
| Generic topology/coalescing, universal compact/Roaring state, broad codec sweep, native Python binding | Remain parked without a newly measured dominant cost. R1/R6 selectively reopen representation work; C5 narrowly reopens build screening, not all former experiments. |
| GPU/FPGA, nested whole Vortex files, raw untyped bytes, foreign compressed-page wrapping, another in-process binary protocol | Deferred device work lacks a measured resident target; reject the container/protocol alternatives for this packet. No demonstrated avoided work justifies these new boundaries. |

## Implementation anchors and provider check

Checked source at `6db17c9f`:

- [Ingest strategy](../../shardloom-vortex/src/vortex_ingest.rs),
  [input/derived preparation](../../shardloom-vortex/src/universal_format_io.rs),
  [bounded layout](../../shardloom-vortex/src/ingest_bounded_layout.rs), and
  [test-only text portfolio](../../shardloom-vortex/src/vortex_ingest_text_codec_portfolio.rs).
- [Exact-DISTINCT admission/results](../../shardloom-vortex/src/local_primitives/exact_distinct_workers.rs),
  [UTF8 dictionary ownership](../../shardloom-vortex/src/local_primitives/utf8_chunk_dictionary.rs),
  [triple partitions](../../shardloom-vortex/src/local_primitives/triple_count_workers.rs),
  [string partitions](../../shardloom-vortex/src/local_primitives/string_count_partitions.rs),
  [compound partitions/pages](../../shardloom-vortex/src/local_primitives/compound_count_partitions.rs).
- [Native composition contract](native-result-composition-2026-09-20.md),
  [source/output reuse](io-reuse-and-fanout-architecture.md),
  [existing build profiles](../../Cargo.toml),
  [Cargo LTO](https://doc.rust-lang.org/cargo/reference/profiles.html#lto) and
  [Rust PGO procedure](https://doc.rust-lang.org/rustc/profile-guided-optimization.html).

Vortex-first decision: use/wrap pinned native Dict, VarBinView, scalar execution,
Filter/Mask, layout sequence and segment-sink concepts before adding abstractions.
Local 0.85.0 source confirms a native dictionary layout and an FSST `LikeKernel`
for admitted constant case-sensitive patterns; this does not prove ShardLoom's
current scans reach it. File readers do not accept arbitrary owned-array injection
through the current constructor. R5 needs a shared input boundary, not a second
planner. R7 remains `blocked_until_vortex_or_shardloom_evidence` for any new layout.
Adapters/providers remain feature-gated and version-recorded in `shardloom-vortex`;
materialization, ownership and execution certificates must expose the chosen path.
No new dependency or copying of external implementation code is approved here.

All candidates preserve one Vortex-native middle, exact outputs, explicit resource
and side-effect contracts, `fallback_attempted=false` and
`external_engine_invoked=false`. No whole PERF item, CG-1 through CG-23 gate,
production-fairness claim or competitive claim closes from this proposal.
