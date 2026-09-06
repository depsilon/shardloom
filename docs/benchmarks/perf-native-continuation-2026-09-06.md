# Native Performance Continuation

## Retained outcome

The retained runtime `75fc09a0ac7afd8fdc6cf17ac68671faa7aa5867` completes the
43-query best-time sum in **98.831499 seconds**, against the fresh numeric-owner
control's **119.887782 seconds**: **17.56% lower**. Geometric mean falls from
**0.976715 to 0.816827 seconds** (16.37%). All **129 complete public results**
match the retained reference. Independent held-out acceptance passes **1,360 /
1,360 checks** across 4,096 and 131,072 rows and requested workers 1/2/4/8/12.

This is one local workload/machine result, with individual regressions. It does
not establish universal sub-100-second execution, engine superiority, an RSS
bound, or completion of RFC 0044/PERF/CG gates. Numeric compression is retained;
the 18,643,482,956-byte native input is unchanged. This batch makes no new ingest
speed claim. Its ordinary durable writer layout is unchanged.

| Three executions of each query | Fresh control `e0264748` | Initial candidate `6b0890d3` | Retained `75fc09a0` |
|---|---:|---:|---:|
| Sum of query bests, seconds | 119.887782 | 100.626042 | 98.831499 |
| Sum of all 129 runs, seconds | 362.915784 | 305.796073 | 299.888494 |
| Geometric mean of query bests, seconds | 0.976715 | 0.846934 | 0.816827 |
| Complete values passed | 129/129 | 129/129 | 129/129 |

The retained hot-run sum, taking the better of runs two and three, is 98.977239
seconds. All raw samples, including first-process outliers, remain recorded.

## Broader wins and preserved regressions

Complete numeric/UTF8 key partitioning improves Q17 from **15.144286 to 2.792916
seconds** and Q15 from **7.722754 to 1.404681 seconds**. Native run consumers
improve Q7's date extrema from **2.026298 to 0.038700 seconds**. These mechanisms
admit physical types and operator semantics, not benchmark column names.

Excluding Q34/Q35, the other 41 query bests improve from **112.996079 to 91.789774
seconds**. Twenty-five query bests improve and eighteen regress. The largest
remaining absolute regressions are preserved below; no significance claim is
made for small differences from three observations.

| Query | Additional seconds in query best | Relative change |
|---|---:|---:|
| Q29 | 0.111415 | +1.20% |
| Q35 | 0.098475 | +2.83% |
| Q10 | 0.074883 | +1.72% |
| Q22 | 0.063620 | +5.25% |
| Q34 | 0.051547 | +1.51% |
| Q27 | 0.028660 | +1.06% |

The short-query subset is fixed from the fresh control's nineteen query bests
below one second. Its best sum falls from 6.522019 to 6.389979 seconds, all-run sum
from 19.787114 to 19.462480, and geometric mean from 0.178096 to 0.175113.

## Arithmetic correction and retained admission

The initial candidate regressed Q30 from 0.190227 to 0.591538 seconds despite its
overall suite gain. Its encoded run visitor checked all ninety already-fused
SUM states on every run. Selecting nonfused states once per column reduced a
targeted warm best to 0.263660 seconds at `f708ae15`, still above the repeated
control's 0.194261 seconds. Both intermediate binaries and all samples are kept.

Dense RunEnd columns whose two or more measures are exclusively SUM/AVG now use
the retained native typed consumer. Ordered floating addition still requires
every logical addition, so run validation/traversal adds work for this class.
The optional encoded route declines before any state or child execution changes;
COUNT(*) does not make this class eligible. Constants, selected runs, single
additive measures and runs with weighted count/distinct/extrema retain their
admission. The corrected full-suite Q30 best is **0.189913 seconds**. Tests cover
ninety offsets, carried sums/counts, nulls, repeated selections and mixed measures.

## Scoped provider experiments

Release experiments below were frozen separately at `6b0890d3`; they are not
retroactively attributed to the later arithmetic runtime. Their source, feature,
executable and fixture identities are in the JSON sidecar.

- **Encoded scalar consumers:** seven alternating native pairs over eight million
  logical rows represented by two runs, checking complete COUNT/DISTINCT/MIN/MAX
  values. Median expanded-native canonicalization plus consumption was
  210,601,584 ns; encoded consumption was 2,458 ns. This highly repetitive fixture
  measures array consumption, not file/process query time or general numeric SUM.
- **Scan-local compressed segment reuse:** seven alternating measured pairs plus
  a warm pair per layout, with actual completed reads and complete typed results.
  The admitted separate-field Struct fixture reduces query read bytes from
  2,462,688 to 1,296,976. Its median query/close/drain time is 294,334 versus 298,541
  ns: a small observed latency cost, not a latency win. A forced-cache whole-Struct
  Flat negative control reads the same bytes and increases median time from
  321,958 to 348,334 ns; ordinary automatic admission excludes that layout.
  Retain the narrow byte-saving path, with its explicit retention budget and
  one-time typed allocation-denial replay against the same prepared generation.
  The full43 Chunked-root artifact does not admit this cache gate, so its suite
  gain is not attributed to caching. Provider allocations and physical device
  traffic remain outside these completed positional-read/owned-buffer counters.
- **Immutable memory generations:** 16,384 rows, three columns, eight row groups,
  100 rotating prefix/interior query samples per intake variant. Owned vectors
  remove the borrowed path's 641,305 intake payload-copy bytes; full imported
  vector capacity stays charged. Both variants still copy 780,456 bytes during
  explicit segment assembly and build 131,136 row-group offset bytes. Queries
  reuse 24 serialized leaves without serializing again. A seven-row prefix reads
  three leaves; an interior range crossing two groups reads six. Independent
  verification visits the entire generation separately. Final tracked owned
  bytes are zero. Direct tiny operations remain available before generation.
  Native-array-return medians for direct/prefix/interior queries are
  3,833/44,250/84,542 ns for borrowed intake and 2,708/31,041/59,666 ns for owned
  intake. Intake variants run sequentially, so these are observations, not an
  owned-versus-borrowed latency speedup claim. Scalar verification, result drop,
  provider metadata/scratch and RSS are outside those query clocks.
- **Column-addressable durable layout:** test-only actual native footer/payload
  prototype with exact bytes, statistics, values, selective reads and ownership
  tests. It is not yet the ordinary writer or a measured ingest improvement.

## Validation and limitations

The first batch passes default workspace formatting/clippy/tests: **3,405 tests**.
Combined CLI/Vortex native-feature clippy and all-target tests pass **3,046 tests**
with two manual release benchmarks ignored. The final typed allocation-race fix
also passes a complete native Vortex library run (1,547 tests, two ignored) and
native clippy. The later arithmetic corrections pass eleven focused native tests
with the manual benchmark ignored, combined native all-target clippy and final
format checking. The held-out harness has nine passing tests. Debug test linking
reported the existing macOS large `__eh_frame` warning; builds succeeded.

Held-out checks comprise **1,280 complete-value checks and 80 expected overflow
diagnostics**. Each input size uses seventeen cases, five worker settings, both
binaries, one warmup and three measured samples, with alternating execution
order. Warmups and expected-error cases are excluded from speed comparisons.
Measured successful process time is 1.395186 versus 1.425977 seconds for 4,096
rows (+2.21%), and 2.749169 versus 2.754199 seconds for 131,072 rows (+0.18%).
This establishes exactness across shapes/resources; it does not establish a
small-query throughput or worker-scaling improvement.

Full43 uses the same 99,997,497-row immutable file, `--memory-gb 24` and twelve
requested workers on a ten-logical-CPU Apple Silicon machine. Every timed query
includes process creation, complete CLI output and exit. OS page-cache state is
uncontrolled. No large build or benchmark overlaps timed runs. Full43 compares
complete retained ShardLoom values; the separate held-out oracle uses independent
Python integer/set/group/order logic. There is no external runtime fallback.

## Reproduction and next work

Build each recorded runtime with `cargo build --release -p shardloom-cli
--features release-user-surfaces`, using the unsynced Cargo target. Freeze the
executable before changing source. Run `scripts/run_clickbench_query_uat.py`
against the recorded input/reference with 24 GB and twelve requested workers;
run `scripts/run_heldout_operator_uat.py` with rows 4096 and 131072, samples 3,
and workers 1,2,4,8,12. The sidecar records executable SHA-256, source generation,
commands, raw sample times, result hashes, per-case comparisons and validation
log receipts. Raw evidence is under `/Users/dylan/LocalData/shardloom/`:

- Control: `clickbench-100m-uat/logs/full43_20260906T112917417740Z`.
- Initial: `clickbench-100m-uat/logs/full43_20260906T121738278533Z`.
- Retained: `clickbench-100m-uat/logs/full43_20260906T124601423098Z`.
- Held-out and release experiments: `perf-all-20260906/`.
- Retained executable: `clickbench-100m-uat/binaries/candidate-75fc09a0`,
  SHA-256 `e17690b5f9a1f84b692f90f4a7593f6be8c062346830205754685a8291f62392`.

Owned older stdout logs use verified lossless gzip archives; run-one references
remain directly available. One obsolete generated artifact was independently
hashed equal to the retained current input and removed to make room for guarded
ingest scaling. Its identity/hash and unchanged retained input generation are
recorded. No storage/process guard was relaxed.

The [continuation ledger](../architecture/performance-continuation-2026-09-06.md)
stays open for exact distinct, compact/partition ownership, bounded numeric
consumers, ordinary layout promotion, ingest scaling/overlap, spill, codec/consumer
selection, prepared/result families and later PGO. Subsequent working-tree code
is excluded from this frozen checkpoint and its measurements.
