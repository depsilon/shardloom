# Native coalesced jobs: September 8 decision

**Decision: park, do not promote.** The maintainer requested that exploration
stop if it does not seem materially worthwhile. Scheduling-only changes show
no material benefit on this workload, so further implementation and long
acceptance runs for this direction are paused. This does not reject the general
idea of local reduction; that staged implementation is unwired and unmeasured.

## Same-binary complete-result comparison

Frozen source: `9152a92b7761bb9a124268f25e726a8094c637a7`.
Binary SHA-256: `5fa36ae99ceca0782b228851c2e9b5dd8186f0ac1a07d4e4c86dc943a89932ab`.
All runs use the unchanged 99,997,497-row native artifact, the same 43 queries,
requested parallelism 12, effective source parallelism 10, and a 24 GiB memory
envelope. Each query runs three times in a fresh process; OS cache is uncontrolled.

| Policy | Actual jobs on 13 selected queries | Best total (s) | Hot total (s) | All 129 (s) | Best change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing | retained source route | 91.286391 | 91.378922 | 277.882761 | control |
| Auto | 585 | 92.510021 | 92.754329 | 281.020142 | +1.34% |
| Fine, target 65536 | 1,170 | 92.596168 | 92.700429 | 281.648438 | +1.43% |

Each packet passes **129/129 complete returned-value comparisons**. The strict
analyzer re-read all saved outputs, checked identities, hashes and ownership
counters, and retained every per-query sample. This is regression validation
against retained ShardLoom output, not an independent correctness oracle.

Auto has two original natural splits per job and a ten-job outstanding window;
Fine has one split per job and a twenty-job window. Both retain the original
read and array boundaries, a twenty-original output bound and nine background
workers. Thus these are distinct actual scheduling geometries. Neither creates
new independently readable physical boundaries in the artifact.

Of the 13 selected queries, 11 best times regress with Auto and all 13 regress
with Fine. Auto's two improvements save only about 1.3 ms each. Selected-query
best totals are 33.790022 seconds for Existing, 34.647305 for Auto and 34.713228
for Fine. Auto's largest selected losses are Q19 (+284 ms), Q29 (+204 ms) and
Q36 (+135 ms). Changes in the 22 explicitly retained routes and eight requests
without region reports are not evidence of topology gains.

Total native CPU across all 129 executions is 594.880248/608.221879/612.071902
seconds for Existing/Auto/Fine. Maximum per-process RSS is
6,867,451,904/6,824,542,208/6,907,478,016 bytes. These are whole-suite observations;
queue limits do not measure all provider-owned memory. Selected runs report no
metadata-pruned regions, and local reduction/Top-K flags remain false. Existing
central encoded consumption must not be confused with region-local execution.

## Evidence and proof limits

[Complete analysis and per-query results](/Users/dylan/LocalData/shardloom/perf-all-20260906/topology-9152a92b-coalesced-analysis.json),
SHA-256 `1cb0747c70f2bcbbb4dee66c9e5e5fbfaeb1c6367263d5229c5688c271e5fe40`.
The analysis records harness identities, source generation and these packet hashes:

| Packet | Summary SHA-256 |
| --- | --- |
| `full43_20260908T111446827523Z` | `d7ff8b47a6696b66ffe7fe2843ef8ab8e5368555c02f0bb1851357cf0a8ccfe2` |
| `full43_20260908T112013700806Z` | `5adec379593f725f0463c0f689e270aead2304a03304821b329d159f48faabb1` |
| `full43_20260908T112716345321Z` | `49b36e0e3c933d45ae71184a545ed6bb7fa49386c1acd9bee1b8e8e9cf72bafe` |

Packets reside under `/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs`.
The frozen checkout is `/Users/dylan/LocalData/shardloom/perf-topology-coalesced-evidence`.
Its `scripts/analyze_native_topology_uat.py` accepts the Existing summary as
`--control`, both candidate summaries as repeated `--candidate`,
`benchmarks/clickbench/queries.sql` as `--queries`, and a new exclusive `--output`.
It reproduces this comparison from retained outputs without executing queries.

Implementation validation passed 3,266 native tests, 3,420 default workspace
tests and 54 harness tests, plus formatting and three Clippy configurations;
nine manual native benchmarks/fixture tests remained ignored. Fresh 4,096-row
and 131,072-row independent held-out matrices for `9152a92b` were **not run**
after the negative performance decision. Earlier `a3c62434` held-out results
apply only to that older revision. This candidate is not fully accepted or a
shipping performance improvement. No competitive gate closes.

The scalar timing identifies substantial central work for the measured scalar
query, but it does not demonstrate a local-reduction speedup. The scalar fold,
its unresolved evidence/ownership integration prerequisite, and the separate
sort-source ownership patch remain preserved outside active source. They are
not shipped and must not automatically restart a long implementation campaign.
