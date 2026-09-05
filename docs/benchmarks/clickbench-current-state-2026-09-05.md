# Current-State ClickBench UAT: 2026-09-05

## Result

Fresh ingest and query runs use the frozen release binary from
`fe730810425e2a2aa2ba657ee0b46ba6afd32d4a`. The subsequent 0.2.3 package-version bump,
test repairs, and documentation do not change the measured execution code.
The release-validation fixes to empty/inferred JSON text-stream schemas and
Python wrapper collection are outside the executed Parquet-ingest/native CLI query paths.

| Metric | Result |
| --- | ---: |
| Queries / runs passed | 43/43; 129/129 |
| Best-of-three query total | 148.349 s |
| Hot query total (minimum of runs 2/3) | 148.674 s |
| All raw query-run seconds | 451.005 s |
| Best-of-three geomean | 1.044 s |
| Hot geomean | 1.045 s |
| Complete native ingest | 162.781 s |
| Artifact bytes | 38,147,848,068 |
| Input rows | 99,997,497 |
| Ingest peak RSS | 10029154304 bytes |

The historical **145.130 s** is a query total, not ingest. This current sample is
**3.219 s (2.22%) slower**.
Same-day merged-state query controls were 142.563 s and 150.033 s. This sample
does not establish a query-speed gain or a regression outside observed variation.
Fresh ingest is 14.839 s
(8.35%) faster than
the same-day 177.620 s merged-state ingest control; the historical ingest
reference was 271 s under an older measurement envelope.

## Reproduction And Boundaries

- Structured evidence: [complete run bundle](clickbench-current-state-2026-09-05.json).
- Binary SHA256: `b0dfa9379f868aea1ecfa91bde746b624a86adb7f7b2b1f7e35e3638b5be5fea`.
- Query SHA256: `4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b`.
- Host: macOS 26.5.1 arm64, ten logical CPUs, 16 GiB physical memory.
- Requested configuration: 24 GB memory; two ingest workers, twelve query workers.
  Requested settings are not host hardware specifications or an RSS guarantee.
- Clock: native process creation through complete output and process exit.
  The ingest watchdog's 181 s observation window is not the ingest duration.
- Cache: fresh process per query run; OS page cache uncontrolled; no answer cache.
- Complete typed returned values were compared with retained ShardLoom outputs.
  Q20 additionally uses an independent Arrow/Parquet Int64 equality reference.
  Other queries are regression checks, not independent SQL-oracle certification.
- Every run retains `fallback_attempted=false` and `external_engine_invoked=false`.
- The 4 GB process-wide memory acceptance remains failed in the separate
  foundation evidence. This 24 GB configuration must not obscure that gap.

Raw logs under the local-only UAT root:
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/`

- Ingest: `ingest_cli_uat_gated_20260905T152036Z`.
- Query: `full43_20260905T152413537665Z`.

Commands are preserved per run. Use `scripts/run_clickbench_ingest_uat.sh`
and `scripts/run_clickbench_query_uat.py` with the binary, input, configuration,
and reference paths recorded in the structured bundle.

## Full Timing Matrix

All values are seconds. Best is the minimum of all three runs; Hot excludes run 1.

| Query | Run 1 | Run 2 | Run 3 | Best | Hot |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q01 | 0.017 | 0.016 | 0.016 | 0.016 | 0.016 |
| Q02 | 0.050 | 0.028 | 0.027 | 0.027 | 0.027 |
| Q03 | 0.591 | 0.591 | 0.587 | 0.587 | 0.587 |
| Q04 | 0.316 | 0.313 | 0.331 | 0.313 | 0.313 |
| Q05 | 1.409 | 1.405 | 1.401 | 1.401 | 1.401 |
| Q06 | 3.957 | 3.901 | 3.901 | 3.901 | 3.901 |
| Q07 | 1.991 | 1.982 | 1.970 | 1.970 | 1.970 |
| Q08 | 0.116 | 0.115 | 0.117 | 0.115 | 0.115 |
| Q09 | 8.758 | 8.550 | 8.550 | 8.550 | 8.550 |
| Q10 | 3.799 | 3.786 | 3.806 | 3.786 | 3.786 |
| Q11 | 0.840 | 0.844 | 0.848 | 0.840 | 0.844 |
| Q12 | 1.633 | 1.622 | 1.640 | 1.622 | 1.622 |
| Q13 | 6.450 | 6.384 | 6.439 | 6.384 | 6.384 |
| Q14 | 8.515 | 8.468 | 8.527 | 8.468 | 8.468 |
| Q15 | 8.529 | 8.511 | 8.501 | 8.501 | 8.501 |
| Q16 | 3.327 | 3.285 | 3.293 | 3.285 | 3.285 |
| Q17 | 15.920 | 15.907 | 16.036 | 15.907 | 15.907 |
| Q18 | 3.468 | 3.520 | 3.524 | 3.468 | 3.520 |
| Q19 | 7.663 | 7.251 | 7.288 | 7.251 | 7.251 |
| Q20 | 0.032 | 0.032 | 0.032 | 0.032 | 0.032 |
| Q21 | 0.809 | 0.726 | 0.748 | 0.726 | 0.726 |
| Q22 | 1.354 | 1.296 | 1.363 | 1.296 | 1.296 |
| Q23 | 4.560 | 4.551 | 4.549 | 4.549 | 4.549 |
| Q24 | 0.885 | 0.826 | 0.849 | 0.826 | 0.826 |
| Q25 | 0.437 | 0.408 | 0.411 | 0.408 | 0.408 |
| Q26 | 2.860 | 2.721 | 2.781 | 2.721 | 2.721 |
| Q27 | 2.741 | 2.736 | 2.732 | 2.732 | 2.732 |
| Q28 | 2.169 | 2.180 | 2.213 | 2.169 | 2.180 |
| Q29 | 10.528 | 9.541 | 10.308 | 9.541 | 9.541 |
| Q30 | 0.295 | 0.294 | 0.286 | 0.286 | 0.286 |
| Q31 | 1.925 | 1.906 | 1.910 | 1.906 | 1.906 |
| Q32 | 2.215 | 2.219 | 2.177 | 2.177 | 2.177 |
| Q33 | 7.754 | 7.247 | 7.160 | 7.160 | 7.160 |
| Q34 | 14.264 | 14.534 | 14.524 | 14.264 | 14.524 |
| Q35 | 14.707 | 14.492 | 14.533 | 14.492 | 14.492 |
| Q36 | 5.847 | 5.853 | 5.604 | 5.604 | 5.604 |
| Q37 | 0.177 | 0.167 | 0.169 | 0.167 | 0.167 |
| Q38 | 0.099 | 0.095 | 0.095 | 0.095 | 0.095 |
| Q39 | 0.070 | 0.068 | 0.068 | 0.068 | 0.068 |
| Q40 | 0.634 | 0.626 | 0.634 | 0.626 | 0.626 |
| Q41 | 0.051 | 0.047 | 0.047 | 0.047 | 0.047 |
| Q42 | 0.031 | 0.030 | 0.030 | 0.030 | 0.030 |
| Q43 | 0.040 | 0.038 | 0.038 | 0.038 | 0.038 |
