# Retained native-array compatibility export: bounded lifecycle evidence

Retain the admitted native-array Arrow IPC/Parquet export at
`e739edeb59c7c3ae0184a671b776e1e30c854214`. At 65,536 rows, complete-artifact
median latency falls from 30.098875 to 8.531292 ms for IPC and from 35.657083 to
10.120542 ms for Parquet against the existing scalar-row exporter with equivalent
output checks added: legacy/candidate ratios of 3.528x and 3.523x. Every complete
value comparison passes. This decision applies to the existing bounded export
profile; broader PERF-07/10 result composition and acceptance remain open.

The small case is mixed: at 4,096 rows, complete IPC latency falls 11.82%, while
Parquet rises 1.45%. The original legacy API is faster than the candidate API at
that size because it omits sync, checksum and reopen checks. Source-generation
and publication guarantees also differ. This packet does not establish a general
small-result, public-call, large-data, cold-storage or process-RSS improvement.

## Measurement identity and workload

- Source tree: `745eafb84060734b10141c19382fbc813a4095b8`.
- Release test binary SHA-256:
  `7ddeaea87ea38c2ea73161825e1beaafb103eb57af836b608feb64338da834a9`.
- Local receipt:
  `/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/retained_compatibility_e739edeb_20260908/manifest.json`.
- Host: Apple M5, arm64, 10 logical CPUs, 16 GiB RAM; macOS 26.6.2 (25G83).
  Native execution requests one worker, with zero candidate provider background
  workers and a 256 MiB memory policy. `release-user-surfaces` is enabled;
  Vortex provider 0.85, Arrow/Parquet 58.3.
- Deterministic local Vortex fixtures: 4,096 and 65,536 rows, five renamed and
  reordered fields, complete output, no predicate or limit in the timed query.
  Fields are nullable UTF8, nonnullable Int64, nullable bool, nullable Float64,
  and nonnullable UInt64. Values include Int64 extrema and adjacent identities
  above 2^53, nulls, empty strings, Unicode and literal punctuation.
- Source SHA-256: 4,096 rows
  `73df8d6a66bb3c1dc56927b4a3963043ffaf56873d4629e7deb0bb836c142309`;
  65,536 rows
  `be1a157ea031adf912d67c07d6834eaa865044cc713f3042d9f3db64bd687cbc`.

The existing ignored release fixture runs directly against the original exporter
and the candidate, avoiding dispatch substitution of the control. Each size and
format has one warmup pair followed by seven pairs with alternating order. Files
are warm local inputs; there is no cache eviction or cold-device measurement.
Fresh candidate timing includes source preparation and joined source teardown.
Candidate IPC is uncompressed; candidate Parquet uses uncompressed PLAIN pages,
no dictionary or statistics, and one row group per admitted batch. The legacy
writer keeps its existing physical policy. Artifact sizes therefore matter.

`API` below measures the actual API call. `Complete` additionally includes sync,
SHA-256 and schema/row-count reopen for the legacy output, which its API does
not perform. The candidate already includes those checks, source-generation
validation and atomic create-if-absent publication. Adding output checks does
not make legacy source/publication safety equivalent. Complete independent
schema/value comparison runs outside both clocks. The native test passes with
32 paired records (64 fresh calls) and four prepared series (32 writes): all 96
complete value checks pass, with no guard failure or fallback execution.

## Fresh-call medians

All times are milliseconds; medians exclude the four warmup pairs. Ratios divide
the legacy complete median by the candidate complete median.

| Rows | Format | Legacy API | Candidate API | Legacy complete | Candidate complete | Ratio | Legacy bytes | Candidate bytes |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 4,096 | IPC | 1.759625 | 4.304167 | 4.881042 | 4.304209 | 1.134x | 199,226 | 199,762 |
| 4,096 | Parquet | 2.153208 | 5.000166 | 4.929000 | 5.000250 | 0.986x | 215,447 | 193,298 |
| 65,536 | IPC | 24.428250 | 8.531125 | 30.098875 | 8.531292 | 3.528x | 3,224,890 | 3,240,674 |
| 65,536 | Parquet | 30.493417 | 10.120417 | 35.657083 | 10.120542 | 3.523x | 3,560,213 | 3,140,468 |

At 65,536 rows the candidate also improves raw API medians, by 2.863x for IPC
and 3.013x for Parquet. At 4,096 rows its added lifecycle work dominates the
original API comparison. Candidate preparation is included above; its medians
are 0.213584/0.210000 ms for small IPC/Parquet and 0.295125/0.309750 ms for the
larger case. Every fresh candidate opens its source once and releases all owned
bytes after drop. Legacy ownership counters are unavailable, not zero.

## All seven measured pairs

Samples 1, 3, 5 and 7 run the candidate first; samples 2, 4 and 6 run legacy
first. `L` means legacy and `N` means native-array candidate. Times are ms.

| Rows | Format | Sample | L API | N API | L complete | N complete |
|---:|---|---:|---:|---:|---:|---:|
| 4,096 | IPC | 1 | 2.094000 | 4.718959 | 5.693500 | 4.719000 |
| 4,096 | IPC | 2 | 1.929709 | 3.209667 | 4.938167 | 3.209750 |
| 4,096 | IPC | 3 | 1.718500 | 3.012000 | 3.889500 | 3.012084 |
| 4,096 | IPC | 4 | 1.759625 | 4.304167 | 5.022167 | 4.304209 |
| 4,096 | IPC | 5 | 1.891209 | 4.099916 | 4.756042 | 4.099958 |
| 4,096 | IPC | 6 | 1.662042 | 4.440958 | 4.041792 | 4.441083 |
| 4,096 | IPC | 7 | 1.620750 | 5.008417 | 4.881042 | 5.008500 |
| 4,096 | Parquet | 1 | 1.976667 | 4.766125 | 4.929000 | 4.766208 |
| 4,096 | Parquet | 2 | 1.959666 | 5.006750 | 5.834250 | 5.006833 |
| 4,096 | Parquet | 3 | 2.488208 | 5.100541 | 4.680958 | 5.100583 |
| 4,096 | Parquet | 4 | 2.123292 | 4.996958 | 5.853000 | 4.997000 |
| 4,096 | Parquet | 5 | 2.172833 | 4.992167 | 4.474291 | 4.992250 |
| 4,096 | Parquet | 6 | 2.153208 | 5.000166 | 5.887500 | 5.000250 |
| 4,096 | Parquet | 7 | 2.176125 | 5.022959 | 4.627666 | 5.023000 |
| 65,536 | IPC | 1 | 29.126416 | 8.531125 | 35.304041 | 8.531292 |
| 65,536 | IPC | 2 | 26.156542 | 8.928709 | 31.668792 | 8.928792 |
| 65,536 | IPC | 3 | 23.770583 | 8.636209 | 29.510500 | 8.636292 |
| 65,536 | IPC | 4 | 24.428250 | 7.772458 | 30.098875 | 7.772542 |
| 65,536 | IPC | 5 | 24.186625 | 8.500500 | 29.912083 | 8.500542 |
| 65,536 | IPC | 6 | 23.965708 | 7.473750 | 29.841375 | 7.473833 |
| 65,536 | IPC | 7 | 25.254042 | 8.943792 | 31.248667 | 8.944167 |
| 65,536 | Parquet | 1 | 30.648041 | 9.148750 | 35.657083 | 9.148791 |
| 65,536 | Parquet | 2 | 30.475959 | 10.127292 | 35.747292 | 10.127375 |
| 65,536 | Parquet | 3 | 29.667875 | 10.061833 | 35.069875 | 10.061916 |
| 65,536 | Parquet | 4 | 29.629792 | 9.066542 | 34.328292 | 9.066583 |
| 65,536 | Parquet | 5 | 30.493417 | 10.120417 | 32.739792 | 10.120542 |
| 65,536 | Parquet | 6 | 30.636291 | 10.872917 | 35.926000 | 10.873000 |
| 65,536 | Parquet | 7 | 30.771709 | 10.156167 | 35.710667 | 10.156250 |

## Repeated prepared writes and resource scope

Each series retains exactly one source open through eight actual executions,
including warmup; execution counts advance from 1 to 8. No answer is cached.
Preparation and final joined close are separate from each complete write clock.
Owned bytes after final drop are zero in all four series. The prepared medians
are not uniformly lower than fresh-call medians, so this packet proves ownership
and reuse without claiming an additional retained-handle latency win.

| Rows | Format | Prepare ms | Close ms | Measured complete writes, samples 1–7 (ms) | Median ms |
|---:|---|---:|---:|---|---:|
| 4,096 | IPC | 0.224084 | 0.048042 | 3.303958, 4.050208, 4.198333, 3.853792, 3.264167, 4.110166, 5.094166 | 4.050208 |
| 4,096 | Parquet | 0.214625 | 0.043583 | 4.911750, 4.842375, 4.711958, 4.924125, 4.747875, 4.746750, 4.851666 | 4.842375 |
| 65,536 | IPC | 0.324834 | 0.056792 | 9.043750, 9.041708, 7.738833, 8.530709, 8.614959, 8.740000, 8.132417 | 8.614959 |
| 65,536 | Parquet | 0.333916 | 0.051292 | 9.630708, 10.151583, 9.909500, 7.551000, 8.630166, 10.001125, 10.020459 | 9.909500 |

Both formats submit 2 native/Arrow batches at 4,096 rows and 32 at 65,536 rows.
Maximum observed Arrow batch bytes are 99,368 and 101,040 respectively; admitted
writer envelopes are 7,105,984 and 7,119,360 bytes. Parquet's maximum sampled
in-progress state is 97,428/99,105 bytes, excluding transients and closed
row-group metadata. These are candidate-scoped counters, not allocation or
memory comparisons against the legacy path. The entire native test process
peaks at 222,625,792 RSS bytes across both variants; that cannot establish a
per-variant RSS improvement or a process-memory bound.

## Replay and remaining boundary

The source fixture is
[`columnar_compatibility_release_lifecycle`](../../shardloom-vortex/src/local_primitive_columnar_compat_sink_bench.rs).
The [architecture note](../architecture/perf-columnar-compatibility-export-2026-09-06.md)
defines admission, source ownership, publication and failure contracts. Run its
release replay command only under the repository's serial storage/process
guards, pinning source and binary identity as this receipt does. The manifest
retains warmups, raw nanoseconds, all output checksums, independent verification
time, work counters and the exact binary command; no benchmark was rerun to
write this report.

The decision adds no new admission: at most 65,536 source/output rows, 32 flat
scalar columns, 2,048 rows per batch, 256 batches, 64 KiB per string, 8 MiB Arrow
batch expansion and 128 MiB output. Predicate, limit, empty-result and failure
behavior have separate correctness coverage; the measured performance workload
is an unfiltered full projection. Broader materializing/aggregate/multi-source
results, public CLI/Python transport, production-size resource acceptance and
all remaining PERF/CG gates remain open. Native Vortex stays the highest-fidelity
persistence target; these outputs are explicit compatibility translations.
