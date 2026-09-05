# Native text layout pruning: retain/drop evidence

Decision: **drop unconditional default text zoning**. Retain the test-only zoned
candidate, bounded completed-read observer, correctness tests and reproducible
evidence. The final public matrix passes all 129 complete-value checks, but 40 of
43 query bests are slower than C7: best total is 2.80% higher, geometric mean is
3.32% higher, and one-ingest-plus-best arithmetic is 2.11% higher. The retained
numeric compression and earlier C7 query/ownership work remain unchanged.

The [machine-readable packet](perf-text-layout-pruning-2026-09-05.json) preserves
both raw native profiles (448 cases including warmups), paired metrics, source and
binary manifests, ingest contract evidence, all 387 query samples across the
three comparison variants, log hashes and cleanup receipts. This local engineering
packet does not close PERF-08/09/12 or a competitive gate and makes no official
benchmark or overall superiority claim.

The candidate restores native text-zone statistics around selected-field Zstd
writers. It retains C7's numeric compression, typed query consumers, ownership
and bounded source-batch writer. Native `RepartitionStrategy -> ZonedStrategy ->
existing Zstd leaf` supplies row-aligned bounded text extrema and null counts;
native Vortex scans consume that metadata. No external query engine, answer
cache, text dictionary redesign or predicate rewrite is introduced. Public ingest
admission remains unchanged and there is no new user-facing tuning knob.

## Frozen sources and boundaries

| Surface | Source revision | Binary SHA-256 |
| --- | --- | --- |
| Both paired writer compositions in one release test executable | `14e5414605d4fd10deef18d50fb78bfaccb00d8a` | `f6471f45d55190acd04a05d691cc9d6ad5a887132b1c3c72f5c813e74d2b267d` |
| Promoted public CLI for 100M ingest/query | `849e3b354745bf9187d4a3fe55c171d1de624a77` | `430419b86b6ebda032979585b3f3b4ed5333381ffaa6b0c1b043cc5b1278e5d1` |

Both clean-source manifests retain Cargo.lock SHA-256
`709618aebdc8761c3d74019db95fd90a57efdeed11eebdce086b86d53f8977ba`.
The control revision is `f607e4c84d74b1ccc4423c4691b29f9d1c53eb7d`, carrying the
C7 implementation documented in the [PR #1433 packet](perf-drop-ship-2026-09-05.md).
The paired executable includes the unzoned control and zoned candidate together;
it does not compare different query implementations. The later promoted source
moves the zoned helper into ordinary writer dispatch and retains the unzoned
helper only for paired tests. Applied-strategy and stage-plan reports identify
the added zoning. That local promotion was measured and then dropped. The production writer and
text-zone tests are restored to the `14e54146` configuration, which keeps zoning
as a test-only candidate. Final restoration identity and post-restoration checks
are recorded separately below.

The pinned provider is Vortex 0.85.0 with `release-user-surfaces`. The recorded
host is an Apple M5, 16 GiB RAM, 10 physical/logical CPUs, macOS 26.5.1 (25F80),
and Rust 1.98.0. Source manifests, complete raw samples and summaries live under
`/Users/dylan/LocalData/shardloom/perf-text-layout-20260905/`; large artifacts and
logs stay outside the source checkout.

## Paired release experiment

Both geometries pass all 448 complete query records: 336 measured and 112 warmup.
Each geometry runs three measured samples plus one warmup for each writer and
text order, with alternating writer order. Every output scalar is independently
checked, including nullable UTF8 and integer identifiers above binary64
precision. The comparison pairs identical source-value hashes and identical
queries; raw predicates are compared with raw, guarded with guarded. Warmups
remain in the evidence but are excluded from medians.

The diagnostic geometry has 32,768 rows and four 8,192-row zones. The matched
`production_rows` geometry has 1,048,576 rows in four 262,144-row source batches,
using C7's bounded per-batch root and configured 262,144-row zone size. Both
writers report one background CPU driver plus the caller; each query requests
parallelism two. Requested query parallelism does not prove two active provider
workers. Both writers use the same selected Zstd codec and frame policy and
independently rebuilt, value-identical source arrays for each variant. Clustered and bijectively permuted text
orders preserve the expected values; identifiers remain in ascending physical
order. These forced writer fixtures are below the ordinary 10M-row admission
threshold and are not themselves public large-source admission proof.

The following medians are from the matched four-batch geometry:

| Text order | Baseline artifact bytes | Zoned artifact bytes | Baseline writer lifecycle ms | Zoned writer lifecycle ms | Lifecycle change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Clustered | 25,488,884 | 25,491,252 | 47.583 | 49.089 | +3.2% |
| Shuffled | 25,714,188 | 25,716,980 | 47.940 | 50.112 | +4.5% |

Writer lifecycle includes native write/flush, file synchronization, independent
full SHA-256 readback and native footer reopen. Source preparation, writer setup
and geometry inspection are separately retained and excluded. File sync and
reopen do not claim durable directory publication. The diagnostic lifecycle
medians are 11.097 -> 11.022 ms clustered and 11.132 -> 10.236 ms shuffled; their
different geometry should not be substituted for the configured-geometry cost.

For raw predicates with text used only as a filter:

| Text order | Selected logical zones | Baseline completed read bytes | Zoned completed read bytes | Baseline native-array return ms | Zoned native-array return ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Clustered | One | 25,550,427 | 8,562,499 | 6.309 | 3.546 |
| Clustered | Half | 25,550,555 | 17,057,207 | 6.501 | 4.140 |
| Shuffled | One | 25,775,519 | 25,777,095 | 6.181 | 6.100 |
| Shuffled | Half | 25,775,519 | 25,777,095 | 6.119 | 6.192 |

Clustered one/half selections avoid 66.5%/33.2% of completed file-read bytes and
reduce native-array return time by about 44%/36%. Shuffled selections span all
physical zones, add 1,576 read bytes and show mixed timing changes. Complete text
projection has its own retained measurements; identifier-only all-row projection
does not measure the cost of reading text.

`native_array_scan_ns` ends when native arrays return. Scalar canonicalization
and complete reference verification are separately timed. These are not full
scalar/JSON or Python/CLI query latencies. The observer counts successful
`read_exact_at` ranges through final drain, including footer, coalesced and
repeated bytes. Logical request registration alone is not the read proof. OS
cache state is uncontrolled: these are filesystem read calls, not physical-device
or cold-cache measurements.

## Production null guard: dropped

The positive WHERE-only expression
`and(is_not_null(field), gte(field, nonnull_literal))` preserves the tested
selected rows and permits native all-null-zone pruning. It remains a test
diagnostic, because it causes repeated file reads. In the diagnostic baseline,
completed `[offset, length]` ranges are:

```text
raw:     [[9495509, 65535], [8, 9558468]]
guarded: [[9495509, 65535], [8, 9558468], [8, 9558468]]
```

The entire 9,558,468-byte text range is read twice; total bytes rise from
9,624,003 to 19,182,471. The guard saves only 164 bytes for the clustered all-null
zone. The configured-geometry filter-only controls also nearly double their
reads. Native scan planning splits `And` into conjuncts and evaluates them
sequentially; Flat readers create fresh array requests, shared in-flight requests
retain weak references, and the default segment cache is a no-op. The pinned
source paths and three-valued-logic limits are recorded in the
[design note](../architecture/perf-text-layout-pruning-2026-09-05.md).

No production guard, generic NULL-to-FALSE expression rewrite, unbounded cache or
hidden materialization is used to obtain the zoning result.

## Single public 100M-row ingest

The guarded public `prepare dataframe` run at
`ingest_cli_uat_gated_20260905T233502Z` succeeds on the existing ClickBench source,
whose actual row count is 99,997,497. This is one local run, not a paired ingest
comparison or a distribution of repeat timings.

| Measurement | Observed value |
| --- | ---: |
| Native process wall, creation through output and exit | 99.512655583 s |
| Artifact bytes | 18,650,731,388 |
| Native child peak RSS | 2,767,257,600 B |
| Native child user CPU | 190.434717 s |
| Native child system CPU | 13.400843 s |
| Requested native shared memory limit | 25,769,803,776 B |
| Peak admitted reservation | 7,211,965,315 B |
| Final admitted reservation / denied reservations | 0 / 0 |

The native provider's inclusive writer span is about 90.550 s. Outer publication
accounts for 8.098 s and includes `LocalReuseFileFingerprint::from_path`, whose
SHA-256 helper reads the entire file in 8-KiB buffers through EOF, followed by
native footer/layout inspection. Both the writer digest and the independently
readback-derived prepared-state digest equal
`2faf1b9bd64c8c3e7c6e831903d123ebbfa45db01fdaa85577d5510da5cccb75`.
This outer readback is included in the process wall time. Only the optional
inner writer reopen is skipped (0 ms). The checksum and footer proof do not
constitute a complete all-value roundtrip; query acceptance is separate.

The run reports the zoned selected-text strategy with bounded source-batch
subtrees and `fallback.attempted=false`. The shared reservation covers copied
native input buffers, prefetch admission, native host allocator allocations and
root layout references. Original source/Arrow owners until conversion, reader
internals and codec/metadata allocations bypassing the host allocator remain
excluded. RSS is an OS high-water observation, not the reservation limit; summed
CPU overlaps wall time and must not be added to it. Existing local storage,
source-residency, concurrency and cleanup guards remain enabled.

## Complete public query comparison and decision

The candidate run `full43_20260905T233742622294Z` passes **129/129**
complete returned-value comparisons. C7 `...T223708789324Z` and main baseline
`...T201730795323Z` also pass 129/129. The comparison refuses partial matrices,
missing complete-value validation, failed guard records, duplicate runs, nonfinite
times, inconsistent stored scores or cross-variant result digests. All 387 raw
native timing samples, CPU/RSS observations and result hashes remain in the JSON.
Correctness here uses retained ShardLoom results; the bounded paired fixtures use
an independent scalar oracle.

| Variant | Best43 s | Hot43 s | All129 s | Best geometric mean s |
| --- | ---: | ---: | ---: | ---: |
| Text candidate | 135.373971 | 136.152504 | 412.776164 | 1.127162 |
| Prior C7, retained C5 numeric artifact | 131.686635 | 132.309902 | 399.893906 | 1.090906 |
| Main baseline, original artifact | 145.598184 | 145.789386 | 442.771066 | 1.019390 |

Best43 sums each query's minimum of three samples. Hot43 sums the minimum of
samples two and three; it excludes the first sample without claiming controlled
cache warmth. All129 sums every measured native process wall sample. Each query
uses a new process with uncontrolled OS page cache.

Relative to C7, the candidate is 2.8001% slower on best total, 2.9042% slower on
hot total, 3.2214% slower on all-sample total and 3.3235% slower on best geometric
mean. Forty bests and forty-one medians are slower. The largest absolute best
losses are Q9 +0.839s, Q33 +0.498s, Q29 +0.400s and Q35 +0.321s. Only Q12, Q13 and
Q15 improve their bests, by 0.007s, 0.008s and 0.054s. None crosses the report's
declared large-regression threshold of at least 1.20 times C7 on either best or
median; that threshold does not excuse the consistent aggregate regression.

The candidate remains 7.02% lower in best total than the old baseline, but its
best geometric mean is 10.57% higher. That historical comparison does not justify
replacing the better retained C7 configuration.

| Variant | Single ingest s | One ingest + best43 s | One ingest + hot43 s | One ingest + all129 s |
| --- | ---: | ---: | ---: | ---: |
| Text candidate | 99.512656 | 234.886627 | 235.665160 | 512.288819 |
| Prior C7 / C5 artifact | 98.344770 | 230.031406 | 230.654672 | 498.238676 |
| Main baseline | 155.098148 | 300.696333 | 300.887535 | 597.869215 |

These totals are scalar sums of **one** recorded ingest plus the selected query
times, not separately measured combined workloads or repeated-ingest estimates.
C7 uses the C5 ingest that produced its retained artifact. The old baseline query
artifact is a separately retained byte-identical copy of that baseline ingest;
the historical identity/hash receipt is preserved. One-ingest-plus-best is
2.1107% slower than C7/C5. No causal RSS or CPU improvement is inferred from these
single local ingest observations.

The selective clustered fixture proves native read avoidance, while the public
workload demonstrates its cost when promoted unconditionally. The default
promotion is dropped; a future workload-aware policy needs its own cost and
resource admission plus complete paired lifecycle acceptance.

## Restoration and artifact retention

Production restoration returns the writer to the unzoned C7 policy; the numeric
compressor, query consumers, aggregate partitions and ownership code remain
retained. The restored writer and text-zone test file match `14e54146`; final
differences from `f607e4c8` are test-only helpers, modules and files. The I/O
fixture also distinguishes the intended validation feature from its actual
compiled feature flag. Source comparison verifies no net production runtime
change from `f607e4c84d74b1ccc4423c4691b29f9d1c53eb7d`. The restoration commit is
`c5b9c050b9519b7a3241c728cb5788d020a1ad06`, not another measured performance
variant. Final checks cover this exact Rust content; some commands began before
the commit while documentation changes were present. Their attribution is
recorded separately below.

The rejected `perf-text-zoned.vortex` artifact has been removed. Before removal,
its held file handle and pathname were checked against the validated generation's
device, inode, size, mtime and ctime. The cleanup receipt is in the JSON packet.
The retained numeric artifact and source Parquet were untouched. Frozen binaries,
ingest logs and query evidence remain. All 129 completed candidate stdout files
were losslessly gzipped with original SHA verification and readback equality:
19,160,844 raw bytes become 2,140,785 compressed bytes. Links in this packet refer
to retained logs/evidence, not to a still-existing rejected artifact.

## Validation and replay

Two ported text-zone tests and nine bounded-observer tests pass. Before the public
measurement, the promoted candidate passed 2,960 native tests, native all-target
clippy and the minimal-writer feature clippy check. These are candidate-source
checks. On the restored source, default workspace clippy and all-target tests
pass: 3,403 tests across 102 suites, no failures or ignored tests. User-surface,
contribution-governance and CI-gate validators also pass. Final formatting and
restored native all-target clippy pass. The restored native all-target suite
passes 2,960 tests across 82 suites, with no failures and one intentional existing
fixture-regeneration helper ignored. All final required gates are green; their
commands, log sizes and SHA-256 hashes are retained in the JSON packet.
The promoted source's historical
ordinary-dispatch tests inspected actual stored zoning using small advisor-forced
fixtures; those dispatch expectations were reverted with the default promotion.
Their scope did not imply that the fixtures met the real 10M-row threshold.

At the recorded source, the paired release executable was built with:

```sh
CARGO_TARGET_DIR=/Users/dylan/.cache/shardloom/cargo-target \
  cargo test --release -p shardloom-vortex --lib \
  --features release-user-surfaces --no-run --message-format=json
```

Run each geometry separately, with no concurrent builds or benchmarks:

```sh
SHARDLOOM_TEXT_IO_GEOMETRY=diagnostic SHARDLOOM_TEXT_IO_SAMPLES=3 \
  /Users/dylan/LocalData/shardloom/perf-text-layout-20260905/text-layout-tests-14e54146 \
  text_layout_filesystem_reads_and_lifecycle_are_exact_and_bounded --nocapture
SHARDLOOM_TEXT_IO_GEOMETRY=production_rows SHARDLOOM_TEXT_IO_SAMPLES=3 \
  /Users/dylan/LocalData/shardloom/perf-text-layout-20260905/text-layout-tests-14e54146 \
  text_layout_filesystem_reads_and_lifecycle_are_exact_and_bounded --nocapture
```

The public CLI was built at its separate frozen revision with:

```sh
CARGO_TARGET_DIR=/Users/dylan/.cache/shardloom/cargo-target \
  cargo build --release -p shardloom-cli --features release-user-surfaces
```

Replay public ingest only through the safety wrapper, using a fresh local target:

```sh
scripts/run_clickbench_ingest_uat.sh \
  --binary /Users/dylan/LocalData/shardloom/clickbench-100m-uat/binaries/candidate-text-zoned-849e3b35 \
  --source /Users/dylan/LocalData/shardloom/clickbench-100m-uat/sources/hits.parquet \
  --input-format parquet \
  --target /Users/dylan/LocalData/shardloom/clickbench-100m-uat/vortex/perf-text-zoned-replay.vortex \
  --memory-gb 24 --max-parallelism 2 --max-artifact-gb 20 \
  --max-runtime-seconds 1800
```

The exact measured invocation is retained in the ingest run's `prepare.cmd.txt`;
`prepare_summary.json`, `native_timing.json` and `stdout.json` retain results and
scope fields. The paired `summary.json` retains measured and warmup samples,
actual layout geometry, source-value hashes and raw-log identities. Keep these
artifacts with the source manifests when reproducing or amending this packet.
