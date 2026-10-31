# Exact adjacent Parquet reads — rejected experiment

The candidate is **dropped** under the unchanged complete-cohort rule. One
isolated execution of each of the original **347 queries** returned all
**12,565 expected rows and schemas**. Native engine time was **12.028866368
seconds**, against **11.997152207 seconds**: an observed increase of
**0.031714161 seconds (0.26%)**. The accepted baseline remains 11.997152207
seconds and the under-10 target remains open. No repeat was run.

The three modified runtime files are restored and the two experimental native
test files and public fixture are removed. All 2,655 recorded product inputs,
including four separately checked Python type files, match the accepted
grouped-state source. Candidate implementation, fixtures, original failures and
complete observations remain in the evidence packet. Maintained offline replay
and rejection tests remain in the repository and CI.

## Mechanism and observed work

The [prospective contract](../architecture/native-adjacent-parquet-reads-2026-10-31.md)
was committed before implementation; its original bytes remain frozen in the
packet. The experiment combined only nonempty, exactly adjacent demanded
compressed Parquet chunks in metadata order. A single existing charged read
owned the combined buffer, while each decoder retained its original bounded
slice. The maximum combined capacity was 4 MiB, further bounded by available
memory and the current shared I/O envelope. Actual admission remained
authoritative; no reservation/read failure was retried through another path.

This removed read calls, without reading gaps, skipping validation, changing
selected bytes, introducing workers/caches, or delegating execution. Pinned
Vortex's coalescing limit informed the cap, while its gap allowance was explicitly
rejected. Native Vortex and Arrow retained their existing input owners.

The 45 targeted public read checks observed **720 requests versus 1,200
individual chunk requests**, with exactly **472,450 compressed bytes**. Empty
files retain encoded column headers, which must be included in those counts.
Across the unchanged 347-query workload, requests fell only from **94,124 to
93,473 (0.69%)**. Exactly 16 queries made fewer requests; every query retained
its original compressed byte count, totaling **1,648,747,757 bytes** per cohort.
Many demanded keys and measures are separated physically, so the targeted
fixtures' 40% request reduction does not describe the broader workload.
These are successful source-read observations, not device or cold-cache counts.

## Complete engine observation

Each entry sums native engine time over one execution per case. Positive
changes mean the candidate was slower.

| Family | Cases | Prior engine seconds | Candidate engine seconds | Change |
| --- | ---: | ---: | ---: | ---: |
| Projection | 26 | 1.017473002 | 1.024450669 | +0.69% |
| Predicates | 16 | 0.403261831 | 0.405509499 | +0.56% |
| Nullable expressions | 8 | 0.128082915 | 0.127477332 | -0.47% |
| Numeric expressions | 4 | 0.135670584 | 0.134498917 | -0.86% |
| Strings | 5 | 0.510266417 | 0.515772542 | +1.08% |
| Temporal expressions | 6 | 0.385259374 | 0.383518124 | -0.45% |
| Global aggregation | 67 | 1.322157172 | 1.330808869 | +0.65% |
| DISTINCT | 4 | 0.103582626 | 0.104440083 | +0.83% |
| Ordering | 10 | 0.346410833 | 0.345958040 | -0.13% |
| Joins | 14 | 1.144240711 | 1.152899836 | +0.76% |
| Sets | 4 | 0.004275125 | 0.004297375 | +0.52% |
| Grouped aggregation | 180 | 5.371484657 | 5.393083748 | +0.40% |
| Multiway joins | 2 | 0.916937626 | 0.911235459 | -0.62% |
| Composed report | 1 | 0.208049334 | 0.194915875 | -6.31% |
| **Complete cohort** | **347** | **11.997152207** | **12.028866368** | **+0.26%** |

No case or family crosses the existing diagnostic material-regression flags.
Those flags do not override the maintainer's binary rule: a higher complete
engine total drops the candidate. Faster selected cases cannot retain it.
This single observation has no statistical confidence or isolated causal claim;
it does not establish that coalescing always slows other workloads.

## Correctness and reproducibility

Eight new native controls cover exact ranges, separation/order/empty demand,
the combined-size boundary, original decoder limits, last-alias memory refunds,
later malformed metadata, memory denial, cancellation, source truncation,
failed reads, the actual shared I/O envelope and real nested/dictionary Parquet
files across multiple groups and files. The complete candidate passes 6,587
native tests across 119 suites with 36 existing opt-in ignores, 3,290 default
workspace tests across 107 suites, strict default/native lint and formatting.

Fresh public acceptance uses 30 declarations over 0, 37 and 2,053 fact rows,
five interfaces and Parquet/Arrow/Vortex: **450 complete comparisons and 143,235
rows** across nine fixture sets. It covers adjacent/separated projections,
rich/nullable payloads, bounded ordering, mixed grouping and ordinary/multiway
joins. References and declarations were frozen before execution. The tested
CLI/library also pass **285 installed-package tests without skips**, including
installation, isolated loading, integration and uninstall verification.

The first public attempt preserves all 450 value comparisons but fails 15
empty-Parquet read-count assertions plus the completion assertion. The fixture
had skipped its encoded empty row group when predicting reads. Independently
regenerated Parquet files match the original bytes, and the exact correction
removes only that skip. Saved-value replay proves all 450 results and all 45
corrected read predictions before the successful public rerun. CLI/library
bytes are unchanged. The initial Rust test compilation and two lint failures
also remain preserved. A later restoration-inventory check initially omitted
the manifest's exclusion of Python type files; the corrected proof checks their
accepted Git bytes separately and preserves that failure and helper source.

The original 347-case harness, five resident Parquet inputs, references, order,
2 GiB grant and maximum parallelism two remain frozen. Observation and
supervision code is unchanged. All other task jobs and agents were idle during
queries; full hashes and recursive inventories ran outside execution. There
was no new competitor observation, cache manipulation, pacing or Full43 run.
Full43 remains held until item 18's universal I/O acceptance.

The [portable acceptance](evidence/native-adjacent-parquet-reads-2026-10-31/acceptance.json)
replays complete results, physical work, source/build/package identity,
ownership, no-fallback evidence and the original failed attempts. Eleven
corruption controls reject altered values, read counts/extents, missing controls,
changed fixture corrections, source identities, clocks and harnesses. A copied
source-only replay passes with original-data access, engine imports, subprocess
execution and network access forbidden. The
[verification packet](evidence/native-adjacent-parquet-reads-verification-2026-10-31/verification.json)
retains those checks and the accepted-source restoration proof. Replay executes
no queries.

The [retirement record](evidence/native-adjacent-parquet-reads-retirement-2026-10-31/cleanup.json)
removes 12,351 generated regular files and three environment symlinks after
repeated identity and closed-handle checks. Rejected binaries/wheel, completed
package environments, duplicate packet/source-only copies and archived raw
outputs are retired. The accepted grouped-state runtime/wheel, original five
Parquet inputs, compact observations, both public summaries, failures and
source evidence remain. Complete replay passes afterward; no payload backup,
storage-limit change or additional query execution occurs.

```bash
python -B scripts/verify_adjacent_reads_packet.py docs/benchmarks/evidence/native-adjacent-parquet-reads-2026-10-31
python -B -m unittest discover -s scripts -p test_adjacent_reads_packet.py -v
```

The broader item-20 owner queue, item 18 and CG-1 through CG-23 remain separate
obligations. No package publication or version bump is part of this experiment.
