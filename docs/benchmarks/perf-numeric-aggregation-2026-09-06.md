# Numeric and Aggregation Performance Continuation

## Outcome and frozen sources

The corrected candidate retains original-width native numeric owners, typed
integer grouping loops, exact block entry credits and local comparison counters.
Its full43 best-time sum is **121.366621 seconds**, compared with the repeated
corrected control's **127.799219 seconds**: **5.03% lower**. Geometric mean is
**7.10% lower**, and the other 41 queries excluding Q34/Q35 total **3.84% lower**.
Every one of the 129 complete public results matches the retained reference.

This is a measured improvement on this workload and machine, with individual
regressions. It does not establish engine superiority, universal operator
improvement, a sub-100-second suite, or completion of a whole PERF/CG gate.

PR #1433 merged as `f257395bbe5f09215d42e1a57b4e5e473ac9e981`. Its second
parent, `c71a558e879cc24490eb370cc0f6182575bce943`, has the identical tree
`10bbaa0ad095e00a65a358c4106196cff8d60406` and is the frozen control.
The corrected candidate is `e02647482ce22923bc08cc055e6f988263b028ff`.
The initial candidate `2afc15308834db4e5d325ca5003ca26d166fd44c` remains
unretained experimental evidence.

| Three executions of each of 43 queries | Original corrected control | Repeated corrected control | Initial candidate, rejected | Corrected candidate |
|---|---:|---:|---:|---:|
| Sum of per-query bests, seconds | 126.743874 | 127.799219 | 131.776075 | 121.366621 |
| All 129 executions, seconds | 386.648342 | 387.066088 | 399.029875 | 368.319535 |
| Geometric mean of per-query bests, seconds | 1.063348 | 1.067895 | 1.030364 | 0.992041 |
| Complete result checks passed | 129 / 129 | 129 / 129 | 129 / 129 | 129 / 129 |

The repeated control is the primary comparison because work resumed after an
overnight pause. Against the original control, the corrected candidate is also
lower: best-time sum 4.24%, geometric mean 6.71%, and all-run time 4.74%.
The two controls bracket neither every timing sample nor all machine variability;
no confidence interval or statistical-significance claim is made.

## Breadth and remaining regressions

Against the repeated control, Q33's mixed numeric-pair grouping improves from
**7.534702 to 4.560801 seconds** (39.47%). Q34/Q35 together improve from
**10.018738 to 8.105395 seconds** (19.10%). The other 41 queries total
**117.780482 to 113.261225 seconds** (3.84%).

Nineteen query bests improve and 24 regress against the repeated control; against
the original control those counts are 23 and 20. The largest remaining absolute
regressions against the repeated control are:

| Query | Additional seconds in the per-query best | Relative change |
|---|---:|---:|
| Q28 | 0.325846 | +15.23% |
| Q19 | 0.268004 | +3.57% |
| Q29 | 0.254788 | +2.72% |
| Q16 | 0.249332 | +10.87% |
| Q3 | 0.220345 | +26.46% |
| Q7 | 0.114916 | +5.97% |

The short-query subset was fixed using the original control's 18 queries with
best time below one second. Against the repeated control, its best-time sum is
3.26% lower and its geometric mean is 4.73% lower, but its **all-54-run time rises
from 18.616726 to 18.774023 seconds (+0.84%)**. Short-call metrics are mixed.

## Why the first candidate was rejected

Retaining original-width arrays eliminated the extra numeric payload copy, but
the first implementation sent narrow integer keys through generic inner loops.
High-level strategy labels stayed unchanged. Q19 worsened from 7.329225 to
11.954621 seconds, and Q33 from 7.179843 to 11.404849 seconds against the original
control. Improvements in the string counts did not compensate for those losses.

The correction uses eight borrowed integer-width views and dispatches before
the single-key/pair loops. It restores specialized numeric/minute/string and
near-unique pair reductions while preserving signedness, selections, dictionary
domains and lazy nullable/materialized lookup. Numeric/string scans reuse the
typed views as well. The new tests cover all 64 width-pair combinations and
verify the specialized strategies with renamed narrow columns and mixed measures.
The rejected binary, complete results and timing records remain preserved.

## Work and ownership evidence

The control reports **66,700,699,008 extra typed payload bytes copied** across
129 runs; the corrected candidate reports **zero** on the migrated accessor
boundary. Pointer/lifetime tests independently verify retained original-width
buffers. This is an accessor work counter, not measured hardware memory traffic
or a claim that every numeric copy in the engine disappeared. Dictionary gather
copies remain outside this packet. Native decode, filtering and validity work
may still allocate.

Call/row totals are not coverage-equivalent between snapshots. The control's
Filter-over-host-Primitive shortcut applied its mask into a typed vector while
returning default numeric work. Eligible filtered arrays now go through
instrumented native Primitive execution. The counter therefore includes newly
observed filtering/canonicalization, not only compressed decoding. Increased
calls or call-rows alone do not establish more source scans or query passes.
No per-array trace attributes every observed delta to that instrumentation change.

The candidate reports 174,984 accessor calls and 8,491,828,338 call-rows across
the suite. Summed inclusive accessor spans are 9.927559 seconds versus
23.745984 seconds in the repeated control. The legacy
`decode_and_typed_copy_nanos` field now measures native execution and owner
setup; these spans cannot be presented as isolated decompression CPU or
subtracted from elapsed query time.

Each corrected Q34/Q35 run reconciles exactly **18,342,019 groups** and
**99,997,497 row weights**. Admission uses **74,760–74,776 block claims**, compared
with the control's per-new-key shared entry claims. Blocks contain at most 1,024
credits. Each run records **9,969,050 actual byte comparisons**, published in
**103,376–103,934 nonzero boundary flushes**. Grants minus refunds equal committed
groups; final outstanding credits, credit waits, handoffs and retries are zero.
Actual peak workers are nine under the requested ceiling of twelve.

Claim/return counters count bookkeeping operations; granted/refunded counters
count credits. Wait counters count condition-variable wait attempts, including
timed or spurious wakes, rather than worker counts or elapsed time. Comparison
totals include actual rechecks after releasing a partition lock, so other
concurrent distributions need not produce identical totals. During execution,
published group counts are lower bounds; after all reducers drain they are exact.
Byte reservations for tables and string storage remain independent of entry credits.

Configured-context reuse covers the migrated aggregate primitive-owner route.
Legacy dictionary-gather and standalone residual-expression helpers keep their
existing context boundary. Pinned Vortex 0.85 Filter/FoR/bit-packed kernels and
primitive builders can allocate outside HostAllocator; even
`builder_with_capacity_in` currently ignores its allocator argument.
Tests separately establish context forwarding, real corrupt-Zstd error
propagation, admitted-buffer credit lifetime, and that observed provider gap.
No decoder-wide budget enforcement or RSS bound is claimed.

## Validation and methodology

The corrected source passes formatting, default workspace clippy/tests, and
combined native-feature clippy/tests: **3,403 default tests and 2,988 combined
native tests**, with one existing native ignored test. All 14 focused numeric
tests pass, as do 16 credit tests, eight held-out harness tests and the three
repository contract validators. Full suites cover the existing pair/minute
specializations, exact handoff, cancellation, collisions and memory pressure.
Temporary output fixtures now include process and atomic sequence identities to
avoid timestamp-only collisions under concurrent tests.

Final-candidate paired held-out acceptance passes **640 / 640 checks**: 600
complete-value checks and 40 expected overflow diagnostics across 16 cases,
five worker settings, both binaries, one warmup and three measured samples.
All 160 warmup records are excluded from timing comparisons. The 240 measured
pairs total **1.86% more process time** for the candidate; their median
baseline/candidate ratio is 0.97945. These roughly 5–6 ms operations establish
bounded exactness, not a throughput or scaling gain. Reported typed-copy bytes
fall from 3,434,400 to zero with 105 native accessor calls in both variants.
The unchanged 4,096-row fixture does not measure partition-credit contention.

Queries use the same 99,997,497-row immutable artifact, 24 GB and twelve requested
workers on a ten-logical-CPU Apple Silicon machine, macOS 26.5.1, Rust 1.98.0.
Every run starts a separate CLI process; OS page-cache state is uncontrolled.
The timing boundary includes process creation, complete public output and exit.
No other large build or benchmark overlaps timed runs. Full43 compares complete
retained ShardLoom outputs and is regression evidence, not an independent oracle.

The fresh control ingest completed in **97.583358 seconds**, with peak RSS
**3,334,176,768 bytes**, producing **18,643,482,956 bytes** under 24 GB and two
requested workers. These are control ingest measurements; this query-consumer
packet does not claim a new ingestion gain. The watchdog observation span of
121 seconds is not the native operation time. The artifact's independently read
SHA-256 is `93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2`.
Numeric compression is retained and the artifact is unchanged.

## Reproduction and evidence

Use the existing guarded runners, the same input/reference paths, and these
frozen source builds with `cargo build --release -p shardloom-cli --features
release-user-surfaces`. Cargo outputs belong outside synced folders.
Run `scripts/run_clickbench_query_uat.py` with 24 GB, twelve workers and the
recorded complete-result reference. Run `scripts/run_heldout_operator_uat.py`
with 4,096 rows, three measured samples plus one warmup, and workers 1/2/4/8/12.
Exact commands, artifact generations, sample times and hashes are in the JSON
sidecar and raw summaries.

All paths below are under `/Users/dylan/LocalData/shardloom/`:

- Control binary: `clickbench-100m-uat/binaries/control-c71a558e`,
  SHA-256 `4f542e41d2da57cdc7f8807b15b75f25d7f703bb19ac6502ac24e833d66be6b8`.
- Corrected binary: `clickbench-100m-uat/binaries/candidate-e0264748`,
  SHA-256 `f03af856dbf47176c279e47a6cea87f99ae4cc2a31e04f933df301a76cd1972d`;
  82,997,904 bytes versus the control's 82,343,440 bytes (+0.79%).
- Original/repeated control logs:
  `clickbench-100m-uat/logs/full43_20260906T011906703636Z` and
  `clickbench-100m-uat/logs/full43_20260906T095430988650Z`.
- Rejected/corrected candidate logs:
  `clickbench-100m-uat/logs/full43_20260906T094652700314Z` and
  `clickbench-100m-uat/logs/full43_20260906T101049247366Z`.
- Ingest: `clickbench-100m-uat/logs/ingest_cli_uat_gated_20260906T011503Z`.
- Final paired acceptance:
  `clickbench-100m-uat/logs/heldout_operators_20260906T101752539379Z`.
- Analysis, validation logs and manifests: `perf-next-20260906/`.

Older owned stdout records were losslessly gzipped, verified by complete
round-trip hashes, to preserve the existing 256 MiB log guard. Archive receipts
map original paths/hashes to stored files; run-one reference files remain directly
available. Storage, source-residency and process guards stay enabled.

The [implementation contract](../architecture/perf-numeric-aggregation-2026-09-06.md)
records the remaining planning suggestions under their existing PERF gates:
segment reuse, broader compound/distinct parallelism, compact state,
query-oriented layouts, memory generations, ingest overlap and later PGO.
