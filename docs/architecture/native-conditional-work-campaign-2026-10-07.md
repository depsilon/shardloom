# Conditional exact work campaign

Status: active experiment queue for the maintainer's second October 7 candidate
set. The [adaptive decimal screen](../benchmarks/native-adaptive-decimal-2026-10-07.md)
is **dropped**: its 1.09% primary-score improvement misses the frozen 3% gate,
and no primary cell reaches 3%. Exact outputs pass; the candidate is removed.
The [conservative membership screen](../benchmarks/native-join-membership-2026-10-07.md)
is also **dropped**: its 6.88% mostly-absent gain cannot override 4.17% and 3.28%
high-match control regressions. No candidate has been retained. Four other tracks
remain gated. The separate completion-aware input unit now has
[local engine, packet and documentation acceptance](../benchmarks/native-input-completion-2026-10-07.md);
hosted integration remains pending. It makes no retained performance claim for
the six candidates in this intake.
The reviewed control runtime is `53cd1582`, whose 941 runtime
assets match builder snapshot
`a7308b726410569306bae14bf60ddd07f57bead73d44d423f5274b5b17c10c5f`.
Published v0.4.0 and the broader PERF/CG status remain unchanged.

This extends the [state and structure campaign](native-state-structure-campaign-2026-10-07.md).
It does not reopen accepted Zstd workspace accounting, rejected directory/locality/
reservation-transition or composed-COUNT prototypes. Completion-aware input,
rematerialization, constraint-guided joins, nested identities and stable spill
merging retain their distinct contracts. Research results from other systems are
not predictions of ShardLoom performance.

## Order and ownership

Builder evidence and hosted integration are complete in PR #1529, with the
accepted runtime unchanged. Subsequent source changes use a separate snapshot.
The adaptive exact decimal screen under PERF-04/10/12 is closed and dropped,
with all source, failures and 1,200 timed complete calls preserved. Conservative
membership under PERF-02/03/10/12 and CG-14's conservative-proof obligation is
also closed and dropped, preserving 1,050 timed calls, complete exact outputs
and both control regressions. Complete hosted integration for
completion-aware input under PERF-03/07/11/12, then measure genuinely retained
derived owners for the separate rematerialization track. Run native workloads
serially; independent source research does
not explain away a control regression.

Learned indexing needs an established large ordered-access target. Cache admission
needs a reconstruction/reuse trace. Stable-region preparation needs measured
changed-source preparation cost. Credit-window output needs a separately frozen
delivery experiment; input lifetime is handled by the separate completion-aware
input unit. These are
prerequisites, not implicit ship decisions. Batch any retained work into a
substantial validated milestone before considering another version bump.

## Current source and experiment contracts

| Track and current boundary | Predicted mechanism | Acceptance and refusal conditions |
| --- | --- | --- |
| **Adaptive exact totals.** `shardloom-vortex/src/local_primitives/native_decimal_reduce.rs:8` stores `DecimalValue` plus count; `Default` begins at I256. `add`, `remove` and `merge` publish only after checked arithmetic. `finish` validates scale, exact division and Decimal128 precision. | Start checked I128 state, promote the unchanged old sum before repeating an overflowing operation in the existing I256 domain. Measure narrow/wide operations and promotions as well as complete grouped and rolling SUM/AVG. | Preserve count/error ordering, atomic failed updates, oversized intermediate means, large cancellation and removal after promotion. Keep output dtype fixed. Test final scale multiplication and division in the existing wide domain. Include ordinary financial values, all-wide and repeated-promotion-pressure controls. An inline enum does not establish fewer bytes per group; claim CPU work only unless state size is separately measured. |
| **Conservative membership.** `native_relational_join.rs:60` builds a retained right table/index; line 91 probes with native hashes, exact equality and duplicate chains. `native_relational_index.rs:39` already rejects empty hash buckets cheaply. `shardloom-plan/src/optimizer.rs:880` declares Bloom-like kinds; its CG-14 foundation at line 1592 is report-only. | After the eligible inner-equijoin build is sealed, a complete native-key filter may avoid entering the existing exact index for definitely absent hashes. Count rejected probes, actual index visits/comparisons, construction work and memory. | Charge construction and retained/scratch bytes; keep the actual existing index as control. Include mostly absent, high-match, duplicate, skew and adversarial-hash cases, plus complete output order and null/numeric equality. Never serve negatives from an incomplete/failed build. A declared filter type is not execution proof. No current join-spill I/O saving is claimed; that needs an admitted spill path. |
| **Bounded-error ordered indexes.** `native_relational_batch.rs:185` locates retained segments with `partition_point`. `local_primitive_sort_output_stream.rs:138` builds at most 512 unique ordinals per window and binary-searches them per output cell. `resident_segment_reuse.rs:421` searches a sorted SegmentId directory. | A compact model could narrow exact searches over a sufficiently large, repeatedly searched ordered owner. Record model bytes, correction interval, preparation and full search cost. | These source sites are leads, not evidence that a model is worthwhile. The 512-row window is small. Require a measured large target before prototyping; do not sort extra input just to qualify. Prove absent keys, duplicates and exact lower/upper bounds across integer boundaries, including rounding in model evaluation. Sampled training without established bounds is insufficient. |
| **Reuse-aware admission.** `resident_segment_reuse.rs:201` explicitly scopes a generation-validated cache to compressed native segment bytes in one prepared execution; hits clone a live buffer. `local_primitives.rs:30519` separately caches transformed dictionary keys per execution. | Retain demonstrated physical reconstruction work under an unchanged byte budget, with less hit-path bookkeeping where measurements justify it. | First trace source generation, representation, owner lifetime, misses and actual reconstruction after release. Repeated references to an already-live owner are not reconstruction savings. Compare no added cache, a simple bounded policy and a research-inspired policy. Charge live evicted references and metadata; protect operator headroom, source validation and fresh aggregate state. No query-answer cache or cold one-shot speedup claim. |
| **Byte-credit result window.** `shardloom-cli/src/python_batch_protocol.rs:221` writes and flushes native JSON output, then waits for that batch's exact acknowledgement. Its input builder at line 171 still finishes resident collection first. | Begin with a fixed small reserved byte window, comparing complete delivery with current stop-and-wait; consider measured-rate adaptation only after a useful fixed window is established. | Outstanding serialized/native owners, wire limits, per-batch sequence, cancellation and the final acknowledgement all remain explicit. Test fast, slow, bursty and failing consumers, time to first provisional result, complete delivery and peak retention. Do not pull side-effectful producers beyond the API's demand promise. Input completion is a separate capability. |
| **Stable-region preparation.** `prepared_source_binding.rs:63` binds the sorted source-generation inventory; that hash is not file-content authenticity. `vortex_ingest.rs:3179` already has a distinct append-only CSV/JSONL refinement decision with verified prefix, line boundary, static configuration and prepared-artifact checks. | Where repeated changed-source preparation is costly, first reuse already aligned logical regions with verified identities/recipes, then investigate row-aware stable boundaries if insertions cause measured reuse loss. | The existing append-only decision is not proof of general stable-region execution. Preserve all-column row alignment, schema, dictionary/codec dependencies, statistics and derived fields. Boundary hashes are not equality proof. Charge full source reads where change metadata is unavailable, and copying into a self-contained Vortex artifact. Include insertion/deletion, schema/policy changes, collisions, cancellation and complete reopened artifact fidelity. No faster first-ingestion or sublinear-read claim. |

The source observations above are finite inspections of the recorded builder
control, before the new opt-in input mode. They do not establish an
LRU bottleneck, a large learned-index opportunity or a benefit from additional
buffering. Vortex-first provider review and a concrete owner/lifetime contract
remain prerequisites before adding an abstraction or dependency.

## Research mechanism and transfer limits

- The [ICL mixed-precision solver paper](https://www.netlib.org/utk/people/JackDongarra/PAPERS/haidar_fp16_sc18.pdf)
  uses low-precision factorization and higher-precision refinement, including
  GPU-specific experiments. Borrow selective expensive arithmetic only. Exact
  integer promotion has a different proof: no discarded information, approximate
  SQL result or iterative numerical correction is permitted.
- The [PGM-index paper](https://www.vldb.org/pvldb/vol13/p1162-ferragina.pdf)
  constructs bounded-error position models with an exact search step and
  worst-case guarantees. Its reported index-space comparisons do not describe
  underlying key storage or an entire database. ShardLoom needs its own exact
  duplicate, boundary and native-owner proof.
- [Binary fuse filters](https://arxiv.org/abs/2201.01174) trade compact static
  membership, construction and lookup cost. Negative answers may remove work only
  when construction covers the complete relevant normalized key set. The
  [ZOR paper](https://drops.dagstuhl.de/storage/00lipics/lipics-vol371-sea2026/html/LIPIcs.SEA.2026.24/LIPIcs.SEA.2026.24.html)
  now has a SEA 2026 proceedings version beyond the February preprint. Its
  deterministic continuation requires the auxiliary remainder for false-positive-only
  semantics; pure ZOR permits false negatives and is ineligible here. The paper
  retains slower construction than optimized Fuse/BuRR. Newer is not a retention rule.
- [S3-FIFO](https://junchengyang.com/publication/sosp23-s3fifo.pdf) uses quick
  demotion through simple queues; [SIEVE](https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo)
  reduces hit-path maintenance. Their cache workloads do not prove a native
  reconstruction opportunity or authorize consuming query-completion headroom.
- [Google's BBR deployment report](https://cloud.google.com/blog/products/networking/tcp-bbr-congestion-control-comes-to-gcp-your-internet-just-got-faster)
  motivates delivery/delay feedback and bounded queues. A local acknowledged
  result stream is not an Internet congestion path. Fixed credit windows precede
  adaptive control; delivery throughput remains separate from kernel time.
- [FastCDC](https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia)
  and the [SeqCDC preprint](https://arxiv.org/abs/2505.21194) address boundary
  detection costs in deduplication. SeqCDC's abstract reports different gains
  against accelerated and unaccelerated baselines and uses SSE/AVX. Neither those
  measurements nor arbitrary cuts through compressed bytes establish an arm64
  Vortex preparation improvement. No implementation code is copied from these sources.

Wavefront alignment is not admitted as a grouping or substring optimization by
analogy. Earlier FlashAttention, factorization, rematerialization and hardware
ideas keep their existing identities and dispositions.

## Causal evidence and disposition

Each candidate needs a frozen source/binary, native eligibility, exact independent
oracle, measured mechanism, same-binary preselected control where feasible,
complete-operation clock and quantitative retain/drop rule before timing.
Freeze fixtures, scales, grants, warm-up/order and confirmation independently for
each performance surface. Then confirm any retained production implementation.

Separate first-use construction from generation-bound reusable preparation,
pressure/spill behavior, incremental delivery and changed-source preparation.
Include all-wide, high-match, no-reuse and continuously saturated controls as
appropriate. A large component improvement is not an engine improvement unless
that component materially affects the complete operation. Retain raw failures,
negative controls and unexplained regressions; do not retrofit thresholds.

The decimal decision applies to its measured prototype and frozen workload; it
does not disprove adaptive exact arithmetic in every workload. Do not reopen it
by changing thresholds or relabeling component gains as complete-operation gains.
Likewise, membership's approximately 98.6% avoidance of exact lookups on mostly
absent probes does not override its complete-operation control failures. Its
source snapshot, original failures and every sample remain portable; a future
different placement needs its own admission proof and frozen experiment.

No candidate in this intake receives a shipped status, retained performance claim,
publication or broad capability promise. Keep all CG-1 through CG-23 owners
visible, preserve native Vortex input/output and explicit no-fallback execution,
and use the existing architecture/testing/hosted gates for any retained change.
