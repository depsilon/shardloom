# Current runtime end-to-end observation — September 30

This is the latest complete local timing observation for merged main
`65c4e7b3935d1db38d3a1defc13642e1ce0aabbb`: one fresh Parquet-to-Vortex
preparation followed by one sequential pass through all 43 ClickBench queries.
The maintainer requested this repeat after identifying concurrent host work as
the likely explanation for the earlier slower cohort. No runtime code changed.

## Measured clocks

| Boundary | Seconds | Meaning |
| --- | ---: | --- |
| Native ingest | 65.806017 | Public prepare process creation through complete output and exit. |
| Native Full43 | 55.251837 | Sum of 43 complete public query processes, one call per query. |
| Combined native work | 121.057854 | Ingest plus those 43 query process durations. |
| Observed workflow | 136.696318 | Guarded ingest launch through final query validation/archive, including supervision and intervening checks. |
| Complete validation driver | 154.678434 | Includes initial input/reference hashes, final output hash and cleanup. |

The final complete-artifact hash takes 5.757626 seconds outside the workflow
clock. Native durations include CLI startup, complete result output and exit.
The Full43 total is one measured pass; it is not a best-of-three aggregate or
a paired speedup. The earlier 74.985410-second best-of-three cohort and its
75.507636-second control remain unchanged in the
[experiment record](../architecture/performance-post033-intake-2026-09-30.md).
The lower repeat is consistent with contention affecting the earlier cohort;
the observations do not isolate its cause.

## Workload and identity

- Apple M5, 10 logical CPUs, 16 GiB physical memory, macOS 27.0 arm64.
- Resident official Parquet: 99,997,497 rows, 105 source columns,
  14,779,976,446 bytes, SHA-256
  `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.
- Fresh native Vortex output: 112 columns, 15,682,956,489 bytes, SHA-256
  `5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
  Every output byte matches the retained current reference, including derived
  columns, file statistics and embedded source provenance.
- Frozen release-mode runtime source:
  `8385055894ba8c74bd4501ecbd833ae650a07895`; binary SHA-256
  `7f92839c2598b29e06ce244b61b53d84197c885a59a262b5961877b882a6fd9a`.
  All 514 Rust/Cargo files match the measured build and merged main. The build
  uses `cargo build --locked --release -p shardloom-cli --bin shardloom
  --features release-user-surfaces`. These are post-release main measurements,
  not new measurements of the already published 0.3.3 distributions.
- Ingest parallelism is 4; query maximum parallelism is 12. The requested
  24 GiB memory setting is an admission policy, not physical RAM or an RSS cap.
  Observed peak RSS is 2,152,415,232 bytes for ingest and at most
  4,905,385,984 bytes among the queries.

## Controls and correctness

The executed driver uses the existing guarded ingest runner, native timing
supervisor, storage guard and owned-process-group cleanup. Ingest has a
600-second native deadline and 17 GiB artifact reservation. Each query has a
120-second deadline. The unchanged ceilings are 12 GiB free disk headroom,
100 GiB UAT workspace and 256 MiB accumulated UAT logs.

The shared UAT lock and native-workload guard exclude other ShardLoom queries,
native builds and tests during the run. Ordinary desktop applications continue
running. Fifteen snapshots retain OS-reported process CPU/RSS and load averages;
they are contextual observations, not exclusive CPU attribution. The highest
recorded non-run process CPU value is WindowServer at 22.7% in those snapshots.
Do not label this an exclusively idle host or infer a causal CPU share.

Cache state is uncontrolled. The full source and retained-reference hashes run
before the workflow clock, and the queries consume the freshly written Vortex
file without purging the OS cache. There is no query-answer cache. Keep this
protocol explicit when comparing future observations.

All 43 complete results match the retained canonical JSON SHA-256 exactly,
without float tolerance. This is regression evidence against retained results,
not a new independent SQL oracle. Primary verification covers all 172 raw query
files, 43 lossless stdout archives, timing records and recorded PIDs. All 44
native processes exit and the owned lock is released. Only the new duplicate
payload is removed after complete byte equality and saved cleanup evidence.

The native Vortex provider and no-fallback boundaries are unchanged. Every
public call reports successful native execution and false fallback/external
engine flags. This observation adds no provider, scheduler, representation,
operator capability or production/superiority claim.

## Query observations

Twenty-three queries complete below one second. The largest observations are:

| Query | Seconds |
| --- | ---: |
| Q29 | 4.986442 |
| Q19 | 4.582344 |
| Q23 | 4.520918 |
| Q6 | 3.428899 |
| Q34 | 3.011716 |
| Q17 | 2.690377 |
| Q35 | 2.684497 |
| Q16 | 2.331291 |
| Q10 | 2.304157 |
| Q28 | 2.147939 |

The [machine-readable record](current-runtime-e2e-2026-09-30.json) retains all
43 timings, CPU/RSS, runtime identities, controls and clock definitions. The
[portable evidence](evidence/current-runtime-e2e-2026-09-30.json.xz) preserves
the executed driver and helpers, raw results, original/portable hashes,
reference results, process snapshots and cleanup receipts. Full data payloads
and executable binaries remain local according to the
[recorded cleanup policy](../architecture/local-artifact-cleanup-2026-09-30.md).

For a future authorized observation, follow the
[local operating procedure](../architecture/local-development-storage.md)
and keep a new immutable run identity. Reuse the actual public prepare and SQL
routes, the complete query file and reference checks. Do not replace historical
samples or promote a single pass into a comparative score. Broader PERF and
CG-1 through CG-23 gates, paused format workloads and publication scope remain
unchanged.
