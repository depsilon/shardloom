<!-- SPDX-License-Identifier: Apache-2.0 -->

# Expanded analytical query observations, November 3, 2026

Polars 2.1.0 returned the expected result on **10,000 of 10,000** distinct requests.
ShardLoom 1.1.0 returned the expected result on **9,995 of 10,000**. Five membership
requests reached the 120-second deadline. All five had a nullable probe, an
unrestricted inner input and a grouped reduction after the predicate. The two
membership archetypes account for **1,373.841091356 engine seconds** across
387 completed results and all five primary failures.
[Family metrics](evidence/native-expanded-query-2026-11-03/metrics.json) ·
[Case table](evidence/native-expanded-query-2026-11-03/cases.jsonl.gz).

## Working from a case ID

Both measured passes are closed. The captured Polars results are the standing
comparison baseline. The case table has one row per frozen request with
family, archetype, input profile, public interface, physical formats, result
status, clocks and selected work counters.

For a deeper investigation, the following command reads the exact recorded
worker response for a case from this checkout. The example selects the 1M
prepared group that counted `signed_value`. Choose `polars`, `native` or
`alternate` for the lane. The alternate lane exists on the twelve 5M
surrogates. A guard-stopped attempt can have empty stdout, in which case
the command prints its supervision record.

```sh
python3 - v2_single_key_reducer_a56dde8b9bd5d428 native <<'PY'
import gzip, json, lzma, sys
from pathlib import Path

root = Path("docs/benchmarks/evidence/native-expanded-query-2026-11-03")
case_id, lane = sys.argv[1:]
with gzip.open(root / "cases.jsonl.gz", "rt") as cases:
    seq = next(i for i, line in enumerate(cases) if json.loads(line)["id"] == case_id)
part, offset = divmod(seq, 250)
with lzma.open(root / "packet" / f"cases-{part:04d}.jsonl.xz", "rt") as stream:
    row = next(json.loads(line) for i, line in enumerate(stream) if i == offset)
assert row["seq"] == seq and row["id"] == case_id
packet = row[lane]["packet"]
stdout = packet["files"]["one.stdout.json"]["utf8"]
print(stdout or json.dumps(packet["supervision"], indent=2))
PY
```

The same packet contains the complete `one.job.json`, stderr and child timing.
The independent typed expectation is in `row["expected"]["value"]`. The
offline verifier validates their hashes and rechecks the results.
[Reproduction and replay](../../benchmarks/traditional_analytics/query_surface/README.md#reproduce-the-expanded-run).

## Membership follow-up

The post-release change passed **392 of 392** requests from the two affected
archetypes, including the five original deadlines. For the 387 pairs with a
valid engine time on both builds, the total fell from **1,373.841091356 to
402.009008583 seconds** (70.74%). The five recovered requests completed in
14.024, 2.074, 2.144, 2.233 and 2.686 engine seconds, in their original
order. Their original times remain unavailable. One candidate observation
per request was compared with the saved 1.1.0 pass under the same bounds.
[Comparison and proof identities](evidence/native-expanded-query-2026-11-03/membership/index.json) ·
[Offline replay](evidence/native-expanded-query-2026-11-03/membership/replay.json).

| Inner input | Timed pairs | Original engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| All rows | 129 | 1,206.368 | 139.913 |
| Empty | 134 | 81.693 | 133.634 |
| Restricted | 124 | 85.779 | 128.463 |

On an indexed membership miss, the remaining candidates have a null on at
least one side. A row comparison can still be false when another component
differs. Once an eligible comparison returns unknown, later candidates cannot
produce a true result. Both resident and ordered membership now stop there.
Build and probe key validation, correlation checks, exact-match precedence
and the separate quantified-comparison rules still apply.
[Resident reduction](../../shardloom-vortex/src/local_primitives/native_relational_subquery.rs) ·
[Ordered lookup](../../shardloom-vortex/src/local_primitives/native_relational_subquery_lookup.rs).

The candidate was built on the measured release source `ff84e23d` with the two
operator files and their regression test changed. All 41 native subquery tests
passed. The new control returned the same predicates in both execution paths
and visited three ordered lookup candidates for three probes. All 392 public
observations passed full result comparison and the original memory and
no-fallback checks. The complete affected cohort met its declared acceptance
rule. Only 103 of the 387 timed pairs individually improved. The evidence
does not isolate the cause of the higher empty and restricted totals.
[Regression test](../../shardloom-vortex/src/local_primitive_relational_subquery_tests.rs) ·
[Raw follow-up proof](evidence/native-expanded-query-2026-11-03/membership/proofs.json.xz).

Across all 392 affected requests, the patched engine took 452.098 seconds
through complete delivery. The same requests took 16.449 seconds in the
original Polars pass (27.48×). This comparison includes the five recovered
requests. A patched total for the full 10,000 is unavailable.

## What ran

The catalog preserves the original 347 executable declarations and adds 9,653.
Each produces distinct SQL with symbolic source paths. Across 18 families and
104 archetypes, the new declarations cover 13,799 declared pairwise and selected
three- through five-factor interactions. One native surface and format are assigned
per primary case. Every surface executes its own public operation. The historical
19 pending declarations remain separately identified.
[Catalog and reproduction instructions](../../benchmarks/traditional_analytics/query_surface/README.md).

| Population | Cases | Fact rows per dataset | Input characteristics |
| --- | ---: | ---: | --- |
| Primary | 8,924 | 1,000,000 | Original balanced, sorted fixture |
| Smoke | 132 | 257 | Independently seeded small fixture |
| Held out | 880 | 100,003 | Unseen combinations on shuffled, null-heavy data |
| Scale | 64 | 5,000,000 | Skewed data, including the 12 paired source observations below |

Polars ran first. ShardLoom then prepared the three new profiles and ran
its 10,000 primary requests. Twelve predeclared 5M-row cases also ran through the
prepared Vortex source. Each request had one attempt per declared route, zero warmups,
two threads, a 2 GiB native memory limit and a 2.5 GiB process-group RSS guard.
The host was an Apple M5 with 16 GiB of memory. Filesystem cache state was
uncontrolled. Independent expectations, SQL identities, input generations,
hardware, package identities and the release CLI/library hashes were sealed
before scoring. The original cases retain their historical reference results.
New reference results use a separate SQLite implementation of the declared semantics.
[Frozen evidence index](evidence/native-expanded-query-2026-11-03/packet/index.json).

The comparison adapter later restricted its whole-right-input summary to
negative membership, where SQL null and empty-set semantics require it.
Positive membership uses only a deduplicated key match. This avoids
duplicating matching rows on older Polars versions that prune the unused
summary aggregates. The frozen observations retain the original adapter
identity and independently checked outputs from Polars 2.1.0.

The 2,323 Python SQL, 1,976 CLI SQL, 1,953 embedded workflow, 1,889 typed relation
and 1,859 DataFrame requests account for the 10,000 native attempts.
Offline replay checked returned schemas, complete values, ordering under the
declared contract, floating-point policy, native execution boundaries and all
failure classifications. The five deadlines appear in the original packets as
three engine errors and two CLI adapter errors. Both CLI exceptions name the
120-second subprocess timeout. There were zero unsupported, unexecuted or
value-mismatch cases. The five failed requests returned no usable engine time.
The 9,995 successes cover 3,192.078920629 engine seconds. A complete 10,000-case
native total is unavailable.

## Matched comparisons

On the 9,995 successful pairs, Polars finished faster 7,333 times and
ShardLoom finished faster 2,662 times. The complete-delivery sums were
408.593 seconds for Polars and 3,618.219 for ShardLoom (8.86×).
Of ShardLoom's wins, 1,583 used the same source format and 1,079 used
prepared Vortex. Polars finished fastest on all 12 paired 5M-row cases.

Polars reports declaration through complete Python-row delivery. It does not
expose a separate engine clock in this harness. On the 6,118 successful pairs
that read the same Parquet or Arrow IPC files, the sums on that common request
clock were 1,816.127 seconds for ShardLoom and 245.390 seconds for Polars
(7.40×). For the 3,877 successful pairs using prepared Vortex on ShardLoom and
its equivalent Parquet or IPC source on Polars, the corresponding sums were
1,802.093 and 163.203 seconds (11.04×). These ratios cover the successful
pairs and their particular mix of requests. Preparation is measured separately.
[Metrics and denominators](evidence/native-expanded-query-2026-11-03/metrics.json).

Polars had the lower aggregate request time in each family. ShardLoom won
143 of 264 global aggregations, although the slower requests brought its
family total to 1.72× Polars. The largest release gaps were in subqueries,
ordering and windows. The composite family had a single observation.

<details>
<summary>Complete-delivery results for all 18 families</summary>

| Family | Successful pairs | ShardLoom wins | Polars seconds | ShardLoom seconds | Ratio of sums |
| --- | ---: | ---: | ---: | ---: | ---: |
| Projection | 26 | 11 | 0.82 | 1.18 | 1.44× |
| Global aggregation | 264 | 143 | 9.32 | 16.06 | 1.72× |
| Predicates | 410 | 124 | 15.71 | 36.07 | 2.30× |
| Grouped aggregation | 1,756 | 636 | 67.04 | 195.23 | 2.91× |
| Temporal | 595 | 284 | 24.37 | 73.36 | 3.01× |
| CTE composition | 392 | 156 | 16.34 | 49.74 | 3.04× |
| Nested | 196 | 51 | 8.42 | 28.95 | 3.44× |
| Distinct | 200 | 59 | 7.62 | 30.14 | 3.95× |
| Composite | 1 | 0 | 0.05 | 0.20 | 4.09× |
| Sets | 789 | 138 | 34.94 | 165.45 | 4.74× |
| Numeric | 201 | 51 | 7.75 | 37.79 | 4.88× |
| Joins | 2,612 | 428 | 110.07 | 545.56 | 4.96× |
| Strings | 399 | 94 | 15.64 | 82.20 | 5.26× |
| Nullable | 205 | 54 | 8.03 | 49.44 | 6.16× |
| Multiway joins | 580 | 86 | 25.41 | 185.23 | 7.29× |
| Windows | 776 | 284 | 32.71 | 549.02 | 16.79× |
| Ordering | 206 | 35 | 8.47 | 181.45 | 21.42× |
| Subqueries | 387 | 28 | 15.88 | 1,391.14 | 87.63× |

</details>

The 12 scale cases are independent surrogates for the maintainer's 5M-row
summary. The original SQL and datasets were unavailable. Times below are
milliseconds. The first two timing columns use the native engine clock.
The other three use the comparable public request-through-delivery boundary.

| 5M-row case | Direct engine | Prepared engine | Polars request | Direct request | Prepared request |
| --- | ---: | ---: | ---: | ---: | ---: |
| Scan and sum | 48.5 | 46.7 | 44.9 | 60.2 | 51.8 |
| Selective filter | 336.2 | 130.8 | 98.5 | 349.0 | 141.5 |
| Broad filter | 177.5 | 297.1 | 98.3 | 189.1 | 302.2 |
| Low-cardinality group | 90.1 | 131.1 | 59.3 | 101.8 | 136.3 |
| High-cardinality group | 1,513.4 | RSS guard | 156.1 | 1,525.2 | RSS guard |
| Multi-key group | 203.9 | 241.7 | 65.1 | 215.7 | 247.1 |
| Distinct | 339.6 | 549.7 | 61.9 | 351.3 | 555.0 |
| Top-k with hidden payload | 320.5 | Deadline | 170.5 | 333.6 | Deadline |
| String filter | 583.6 | 524.1 | 56.0 | 595.3 | 529.2 |
| Nullable aggregate | 59.6 | 54.8 | 47.0 | 71.1 | 59.9 |
| Join and group | 2,031.2 | 2,173.5 | 146.9 | 2,048.1 | 2,179.0 |
| Rolling window and rank | 20,669.8 | 44,222.4 | 185.0 | 20,683.0 | 44,228.0 |

Direct Parquet completed all 12. Prepared Vortex completed 10. The high-cardinality
group exceeded the sampled RSS limit. Prepared top-k hit the execution deadline.
Neither failure returned a completed result or an engine clock. The rolling window
ran over all five million rows, with 20.670 direct engine seconds and 44.222
prepared engine seconds. The join delivered 5.5 million scan rows on both
routes. The selective filter admitted the bound native file provider on Vortex,
while the prepared window required relational execution. Provider selection
and physical work therefore need inspection at the composed operation.
[Complete scale observations](evidence/native-expanded-query-2026-11-03/surrogates.json).

The wide top-k surrogate orders an unfiltered population by signed value and
row ID, returning 31 rows with a decimal, text, flag, list and timestamp.
The direct route delivered 5,000,031 scan rows. The batch-source ranked read
requires an input producer; prepared-file execution cannot enter that branch.
The failed prepared request returned no final operator report. The retained
filtered-position experiment therefore supplies no timing result for this
case. The high-cardinality prepared group likewise produced no final group
report before the RSS guard stopped it. Its supervising receipt recorded a
3,433,742,336-byte child peak. Those two outcomes remain open.
[Ranked source admission](../../shardloom-vortex/src/local_primitive_relational_ranked.rs).

Two successful 1M-row prepared group cases differ from the failed group by one
query factor each. Changing its aggregate argument to `signed_value`, or its
function to `min`, admitted the native file provider and selected
`capillary_ordered_topk` after `direct_accessor_general_group_update`.
The two child peaks were 889.7 MB and 889.3 MB. Their reported native peak
reservations were 326.5 MB and 325.9 MB. Those measurements identify a
group-state and process-memory comparison worth tracing at scale. The failed
5M case provides no final certificate assigning its memory to that owner.
The case IDs are `v2_single_key_reducer_a56dde8b9bd5d428` and
`v2_single_key_reducer_75e1579c0df46929`.
[Per-case native evidence](evidence/native-expanded-query-2026-11-03/cases.jsonl.gz).

## Preparation

Preparing the 5,000,000-row fact file converted 542,171,434 bytes of Parquet
into 608,932,302 bytes of Vortex. The CLI reported 11,691 ms for preparation
and 11,599 ms for the inclusive provider encode/write stage. That stage
includes compression. The 168.3 ms of stream Arrow-conversion work and
158.5 ms of Arrow-ownership work overlap. The receipt also records 46 ms
for reopen verification. The captured spans do not separate compression CPU
from storage writes inside the 11,599 ms. Changes aimed at the writer need
that distinction and a complete readback before a gain can be attributed.

The fact file's single preparation process took 11.747 seconds. At the observed
0.207-second request-through-delivery saving per selective query, 57 such reads
would pay back that preparation cost alone. The result depends on the repeated
query mix. The broader filter and the rolling window were slower on the prepared
route in this observation. All 15 new output parts passed full IPC readback,
including all five scale tables.
[Preparation data](evidence/native-expanded-query-2026-11-03/preparation.json) ·
[Raw CLI receipts in the packet](evidence/native-expanded-query-2026-11-03/packet/index.json).

The checked-in baseline and follow-up replayed with Python's site packages
disabled. Local verification passed 105 query-surface tests with the pinned
Polars and native interfaces, 41 release-profile subquery tests and 3,318
default workspace tests. Formatting and strict workspace Clippy passed.
[Validation receipts and logs](evidence/native-expanded-query-2026-11-03/validation.json).

The subsequent operator investigation compared 55 affected case-route
requests on matched parent and candidate builds. The separate prepared 5M
group challenge completed under the original RSS guard.
[Operator results and remaining gaps](native-expanded-operator-investigations-2026-11-03.md).
