<!-- SPDX-License-Identifier: Apache-2.0 -->

# Fresh native UAT after roadmap reconciliation

Fresh ingestion validates all 99,997,497 ClickBench rows, all 43 queries pass
complete-value and schema checks, and all 18 format/workflow cohorts pass on
merged runtime `6780e7df564c55f3e256822be1d0383cfabff0c9`.

**Full43 engine time: 54.358416752 seconds (43 queries, one execution each).**

This is one regression observation with a retained native reference. It is not
a matched speedup comparison. The [roadmap reconciliation](../architecture/native-roadmap-reconciliation-2026-10-18.md)
accounts for every historical substep, and the [cleanup record](../architecture/local-artifact-cleanup-2026-10-18.md)
preserves scoped retirement evidence. The next package version remains a separate
maintainer decision.

## Fresh execution and verification

| Gate | Result |
| --- | --- |
| Source and build | Fresh release CLI and binding; all 2,416 frozen source files match the merged runtime |
| Fresh ingestion | 99,997,497 validation rows and 99,997,497 projection rows; native output reopened |
| Full43 | 43 calls, one per query; all 381 complete result rows, schemas and native certificates reopened |
| Format/workflow matrix | 18 cohorts; 704 native records and 704 independent pandas correctness-oracle records |
| Clocks and process cleanup | All 43 timing/PID companions, complete timing CSV and compressed output identities verified; native children absent |
| Dataset identity | Full source/output SHA-256, residency and stable generations checked before and after execution |
| Evidence packet | All 14,712 archived members reopened against original sizes and hashes |

The modular scope is 22 workflows: all eight admitted inputs with collection,
plus CSV input to all eight outputs, each with raw and prepared inputs. It does
not claim the full input/output Cartesian product. Pandas is an independent test
oracle only and never executes ShardLoom work.

The earlier 4,827-case public matrix across 36 families and 100 direct-binding
methods are inherited through the explicit final test-only source bridge and
PR #1574 hosted acceptance; they were not executed again by this fresh UAT.
The fresh merged CLI is used for ingestion, Full43 and every native modular row.
No runtime source changes are made by the documentation reconciliation.

## Reproduction and identities

The machine is macOS 27 ARM64 with 10 logical CPUs and 16 GiB physical memory.
Query admission is 24 GiB with maximum parallelism 12; this is not an enforced
whole-process RSS ceiling. Input/output files are resident and prehashed. OS
cache state is uncontrolled. Builds and workloads run sequentially under the
existing process/storage guards, with no artificial inter-query pacing or retry.

The retained original Parquet has 14,779,976,446 bytes and SHA-256
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`. The newly generated Vortex output has
15,713,610,513 bytes and SHA-256 `06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`.
The new output is byte-identical to the previous accepted Vortex generation;
only that superseded payload is retired after full identity and open-handle checks.

The release CLI uses `release-user-surfaces` and SHA-256
`96e78f3575e17e2b88b3b9b520dcfded3695ab05c586192b174b16297123f0ef`. The separate release binding has SHA-256
`59b6abffd123922f082c8558e417b911cb0eb78ec58ee006ead351259b091dff`. The pinned Vortex 0.85.0 provider and
native no-fallback boundary are unchanged. Result evidence preserves
`fallback_attempted=false` and `external_engine_invoked=false`.

The [portable index](evidence/native-roadmap-fresh-uat-2026-10-18.json) identifies
the partitioned archives and compressed complete member manifest. They retain all runtime
sources, exact build/launch commands, source bridges, reference values, full
ingest/query/modular results, host observations, cleanup receipts and executable
helpers with their local import closure. Bulk input/output files, executable
binaries and redundant fixture payloads have explicit exclusion identities.
Local paths are provenance and must be remapped explicitly on another host.
The packet also preserves an initial post-run verifier path error and its
correction: retained historical logs were incorrectly treated as one active run.
The corrected check validates each real run under the original limits; no query
or workflow was repeated and no storage limit changed.

Reproduction starts with `prepare_roadmap_fresh_launch_663.py` after the guarded
release builds and source freeze. The launch record retains the exact sequential
ingest, Full43 and modular commands and runner source. The ingestion command uses
`scripts/run_clickbench_ingest_uat.sh`; `verify_roadmap_fresh_uat_663.py` independently
reopens every result and verifies stable full dataset hashes. The final
`package_roadmap_fresh_uat_667.py` records and verifies the portable evidence.

## Per-query engine times

Each row is one execution. The [complete timing CSV](evidence/native-roadmap-fresh-uat-2026-10-18-query-timings.csv)
retains all other diagnostic clocks and resource observations separately.
The readable CSV uses LF line endings; its parsed records are identical to the
original runner CSV preserved in the evidence archive.

| Query | Engine seconds |
| --- | ---: |
| Q01 | 0.000061000 |
| Q02 | 0.191171917 |
| Q03 | 0.878807542 |
| Q04 | 0.390875042 |
| Q05 | 1.436772625 |
| Q06 | 1.173535542 |
| Q07 | 0.000479292 |
| Q08 | 0.147972542 |
| Q09 | 1.410692791 |
| Q10 | 2.015151917 |
| Q11 | 0.836877292 |
| Q12 | 1.488585583 |
| Q13 | 0.602865292 |
| Q14 | 1.723276207 |
| Q15 | 1.561677917 |
| Q16 | 1.863362000 |
| Q17 | 2.543721126 |
| Q18 | 0.353636375 |
| Q19 | 5.066029708 |
| Q20 | 0.269804957 |
| Q21 | 1.167402292 |
| Q22 | 1.433397041 |
| Q23 | 3.698508042 |
| Q24 | 1.234934417 |
| Q25 | 0.352940792 |
| Q26 | 1.358040791 |
| Q27 | 1.471783916 |
| Q28 | 2.488788084 |
| Q29 | 3.097886459 |
| Q30 | 0.277180958 |
| Q31 | 1.341217708 |
| Q32 | 1.649347583 |
| Q33 | 2.032772918 |
| Q34 | 3.112900416 |
| Q35 | 2.266303000 |
| Q36 | 0.421650125 |
| Q37 | 0.423568583 |
| Q38 | 0.468212167 |
| Q39 | 0.221983583 |
| Q40 | 0.904918292 |
| Q41 | 0.573695750 |
| Q42 | 0.204255500 |
| Q43 | 0.201371668 |

The sum excludes UAT preparation, external verification, packaging and cleanup.
No production-readiness, comparative superiority, process-RSS, new format or
package-release claim follows. Broader PERF owners and CG-1 through CG-23 remain
open under their existing evidence gates.
