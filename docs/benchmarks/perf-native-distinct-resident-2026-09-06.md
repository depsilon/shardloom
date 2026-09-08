# Native distinct, resident calls and bounded experiments

This checkpoint measures runtime commit `48182c5a1a1bd2155a4df6e331776da380094952`
against retained runtime `75fc09a0ac7afd8fdc6cf17ac68671faa7aa5867`. It continues
RFC 0044 and PERF-01 through PERF-13 without closing those phases or competitive
gates. Native Vortex execution and numeric compression remain enabled. No
external engine executes these queries.

## Complete public query suite

The 99,997,497-row native input is unchanged: 18,643,482,956 bytes, SHA-256
`93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2`.
Both builds use 24 GiB requested memory and 12 requested CPU lanes on the same
10-logical-CPU Apple ARM machine. Each query runs in three fresh CLI processes;
the operating-system page cache is uncontrolled. Timing includes process start,
completed public output and process exit.

| Measure | Retained control | Candidate | Change |
|---|---:|---:|---:|
| Sum of 43 query bests, seconds | 98.831499 | 91.662289 | −7.25% |
| Sum of hot query bests, seconds | 98.977239 | 91.910407 | −7.14% |
| All 129 raw runs, seconds | 299.888494 | 278.363702 | −7.18% |
| Geometric mean of query bests, seconds | 0.816827 | 0.777501 | −4.81% |
| Complete returned-value comparisons | 129/129 | 129/129 | All pass |

These comparisons use retained ShardLoom outputs, not an independent oracle.
The [machine packet](perf-native-distinct-resident-2026-09-06.json) preserves all
query samples, process RSS, source/build identities, regressions and raw-summary
hashes. The earlier [checkpoint](perf-native-continuation-2026-09-06.md) records
the preceding improvement and its own limitations.

Q9's native integer group/value exact distinct workers reduce its best from
8.412124 to 1.116068 seconds. This has a material memory tradeoff: observed
process peak RSS rises from approximately 1.04 GB to 4.27 GB. Those peaks include
provider allocations outside ShardLoom's explicit state pool. The native path
preserves original integer widths, complete distinct pairs and global selection;
it does not apply local top-K before final reduction.

Twenty-one query bests improve and twenty-two regress. The largest absolute
regressions are Q33 (+0.123552 s, 2.74%), Q34 (+0.105360 s, 3.04%), Q28
(+0.074715 s, 3.10%), Q14 (+0.060719 s, 0.79%) and Q36 (+0.046380 s, 0.82%).
The other 42 query bests together are slightly slower; Q9 accounts for the
overall gain. This packet does not establish a general throughput improvement
for every operator.

A first candidate run stopped at the unchanged 256 MiB allocated-log guard
after 83 passing checks. It has no full-suite score. Generated stdout was
losslessly compressed with SHA verification, preserving the reference directory;
the complete retry above then passed all 129 checks. The interrupted attempt and
archive receipt remain separate evidence.

## Independent held-out operators

The 19-case matrix completes 1,520 checks across both binaries, 4,096 and 131,072
rows, requested lanes 1/2/4/8/12, and complete independently generated values or
expected diagnostics. Aggregate sums of case/lane p50 values change by +1.04%
and +0.85%, respectively. There is no small-operation speed claim.

The two added exact distinct cases request an explicit ascending integer tie
order. This frozen candidate admits its parallel distinct route only for the
count-descending order alone, so those held-out cases exercise the retained
typed implementation. Their passing results do not prove the new worker route's
held-out performance. A later narrow tie-order admission and actual-route
assertions require another measured matrix. The control also raises requested
one lane to two; this candidate preserves an explicit one-lane ceiling.

## Resident calls

Six independent 32-row cases complete 1,116 calls across fresh CLI, persistent
worker and Python-client surfaces, including warmups. Real filtered counts reuse
one prepared source and execute the native scan on each call; answers are not
cached. Persistent-worker filtered-count p50 changes from 0.682584 to 0.482625 ms,
and Python-client p50 from 1.118292 to 0.857875 ms. Fresh-process calls remain
mostly flat or slightly slower. Complete sample distributions and preparation
boundaries remain in the local evidence.

A separate native prepared filtered count performs 10,000 measured executions:
p50 38.750 μs, p95 47.000 μs, p99 53.167 μs. The count is independently checked as
eight rows on every call, with one prepared open and 10,001 completed executions
including warmup. Final owned reservations refund to zero. This is a small
resident scalar result, not an arbitrary-query latency guarantee.

## Measured experiment decisions

All four release experiment processes pass complete values and ownership checks.
Their source is frozen at the same commit; none promotes a universal new default.

| Experiment | Completed evidence | Decision |
|---|---|---|
| Bounded FoR/bit-packed consumers | 21 paired trials, 8,000,003 rows | Keep feasibility and correctness coverage. 8K/32K block gains are below 1%; 1K regresses 3.47%. No throughput promotion. |
| Compact string state | 63 pairs, three distributions and slab sizes | 16 KiB high-cardinality state reduces measured arrange-and-reduce time 18.1% and admitted peak 24.2%; repeated values regress 27.3% with 14.44x admitted peak. Narrow candidate only. |
| Fixed partition ownership | 84 pairs across distributions and lane counts | Eight owners beat eight dynamic workers by 7.8–32.1%, but lose to one owner by 1.57–4.00x. Do not promote from this packet. |
| Native text codec portfolio | 27 actual files, 10,800 complete query calls | Dictionary improves categorical reuse-100 lifecycle by 18–28% at 4.23–5.83x artifact bytes; unique Dictionary is slower/larger. FSST has no sustained lifecycle gain. Preserve retained Zstd. |

Compact-state timing includes retained partial disposal inside the control call
but candidate routing teardown outside its update clock. It is not an isolated
table-speed measurement. Experiment process RSS cannot be assigned to individual
variants. Codec lifecycle sums use actual chronological query prefixes, not a
median query multiplied by reuse. Provider allocations, fixture/oracle ownership
and process memory remain distinct from explicit state reservations.

## Reproduction and evidence

The machine packet links the source summaries and release analysis by path and
SHA-256. Local evidence is under
`/Users/dylan/LocalData/shardloom/perf-all-20260906` and the guarded UAT root
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat`. Large generated artifacts
stay outside synced source directories.

The measured CLI SHA-256 is
`6da89a5e531a330763123dbcbe79473e7e664e09bee2de5c0380c386a3a61f8e`.
The release test and resident example have separate pinned binary manifests.
Root workspace format, default tests/lints, native tests/lints, minimal native
feature lints and repository governance checks passed before freezing the source.
The native test run records 3,137 passing tests across 83 suites, with seven
explicitly ignored release experiments; default validation records 3,406 tests
across 102 suites. Measured experiments run those selected ignored tests
separately in release mode.

Public aggregate spill, native-array compatibility export, broader prepared
aggregates, the isolated Python binding, ingest scaling, physical column layout
lifecycle and PGO remain subsequent work. Their active changes are not included
in these timing claims.
