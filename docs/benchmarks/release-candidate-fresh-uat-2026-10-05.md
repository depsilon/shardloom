<!-- SPDX-License-Identifier: Apache-2.0 -->

# Fresh UAT for the resumed 0.4.0 candidate

The accepted shared-engine and analytic-frame executable passes fresh replacement
ingest, all 129 ClickBench calls and the complete parameterized workflow matrix.
This is local correctness, availability and timing evidence. It does not certify
package publication, complete the universal-workflow queue or establish a speedup.

## Source and machine

| Item | Identity |
| --- | --- |
| Runtime source | `22f1e6ba5a5ca7855a85862e89612fe8fbc5e06b` |
| Native executable SHA-256 | `fd46e887249c6b6eeba7fb7ed26f6beb10149f785461d277e49b3338a296f5f0` |
| Modular harness source | `9234ba67b6bd1155bdd154f86d058b52ac2003a7` |
| Recording source | `b705fe5d5cd536dd8f68da4e09997cdaa51b18b5` |
| Machine | Apple M5, 10 logical CPUs, 16 GiB physical memory, macOS 27.0 arm64 |
| ClickBench policy | 24-GiB requested memory policy, 12-worker maximum |

The recording source changes a documentation metadata test after the measured
work. All engine Rust/Cargo and public Python runtime sources match the frozen
build. The packet records both test fingerprints and links the repair receipt;
it does not relabel the executable as a new build. The machine's physical memory
and the requested policy are separate quantities, and reservations do not
guarantee total-process RSS.

## Fresh ingest and full query regression

Replacement ingest reads the resident 14,779,976,446-byte Parquet source and
writes a new 15,713,610,545-byte native Vortex artifact. The native stream,
prepared output footer and input count each report 99,997,497 rows. All 43 SQL
queries then run three times against that new artifact, with complete result
comparison and explicit no-fallback evidence in every report.

| Measurement | Native process seconds |
| --- | ---: |
| Replacement ingest | 53.069432 |
| Query calls grouped by repeat 1 | 72.552567 |
| Query calls grouped by repeat 2 | 69.180389 |
| Query calls grouped by repeat 3 | 69.119042 |
| Sum of the minimum for each of 43 queries | 68.219750 |
| All 129 query calls | 210.851998 |

The harness executes query 1 three times, then query 2 three times, and so on.
The repeat groups are sums of calls, not three chronological full passes.
Ingest and queries are separate guarded commands; adding their native durations
does not produce an observed end-to-end wall clock. Native timing includes
process startup, complete output and exit. Harness validation and monitoring
are excluded. Each query starts a new process, with uncontrolled OS page cache
and no answer cache. Native peak RSS is 2,634,022,912 bytes for ingest and
5,276,205,056 bytes across the query calls.

All 129 results match the retained complete native reference. That is regression
evidence, not a fresh independent SQL oracle or a comparison of every source and
output cell. The independent frame and operator fixtures remain in the
[analytic-frame acceptance](native-analytic-frames-full43-2026-10-05.md).

Full post-run hashes preserve exact artifact identities. The Vortex generation
matches the one checked throughout Full43, and both files remain unchanged
during hashing. Hashing occurs outside the measured work.

| Artifact | SHA-256 |
| --- | --- |
| Resident Parquet source | `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f` |
| Fresh native Vortex output | `cda1452a82a450063e89abd64ea99ccbfcaedf1269b92896bd7b8c5645a5053a` |

## Parameterized complete workflows

The fresh matrix passes 1,408 records, including 704 native candidate records,
in 18 serial cohorts. It covers all 22 declared workloads, 1,000 fact rows and
20 dimension rows, with these two complementary selections:

- Eight input formats with collected results, each using raw and prepared input.
- Eight output formats from CSV, each using raw and prepared input.

Inputs are CSV, JSON, JSONL, Vortex, Parquet, Arrow IPC, Avro and ORC. Outputs
are Vortex, JSON, JSONL, CSV, Parquet, Arrow IPC, Avro and ORC. This is not a full
Cartesian input/output matrix. The comparison uses independently computed pandas
values only as a test oracle; Vortex comparisons read equivalent original CSV
in pandas and do not claim same-format performance. Native execution stays in
the shared Vortex-native family.

The complete guarded comparison takes 602.746226 seconds, including fixture
preparation, independent reference work, native work, output readback and
validation. It is not a native query-performance score. An earlier attempt
stops during fixture creation because its Python environment lacks PyArrow;
it executes zero queries. The complete rerun uses the recorded comparison
dependencies and preserves that failure. No incomplete runs are combined.

## Query CPU observation

The native children consume 708.412989 CPU seconds over 210.851998 process-wall
seconds, equivalent to an average of 3.36 busy CPU cores across the full run.
Usage varies substantially: Q24 averages 8.31 core equivalents, while Q26 and
Q27 average 1.58 and 1.67. A configured concurrency limit is not measured CPU
utilization. These values include all native child threads and startup/output
work; heterogeneous cores also make them different from throughput utilization.

Source inspection finds a synchronous candidate-processing and final-selection
boundary after the parallel scan in the sorting path. This is a profiling lead,
not measured attribution of the low CPU use. Metadata work avoidance, I/O,
memory stalls and serial processing can all lower the observed ratio. No
worker-scaling experiment or CPU optimization is claimed by this release UAT.

## Retained proof

The [portable packet](evidence/release-candidate-fresh-uat-2026-10-05.json.xz)
retains complete values, all 129 query reports, 18 workflow manifests, 11,311
workflow process logs, original failure observations, hardware/artifact
identities, source manifests and guard receipts. Its compressed size is
2,036,460 bytes with SHA-256
`8a5011434abc70a9a3593f4ee8428d195e8fa68cc1ecd46a60371ec76bab7cda`.
The uncompressed JSON is 209,432,141 bytes with SHA-256
`1e0b37e0db7ce6f9df9e9e5c168a18e48bf7c897978d514fd471447137533453`.

The [independent streaming inspection](evidence/release-candidate-fresh-uat-inspection-2026-10-05.json)
verifies both hashes, the complete query/workflow counts, original top-level
native-family reports and no-fallback fields. It preserves its initial
double-counting of route fields copied into certificates and the corrected
inspection of the unchanged packet. The packet builder likewise retains its
initial harness-identity shape error and corrected complete mapping check.

All large work remains serial under the existing process and storage guards.
The earlier completed Full43 logs were compacted with original JSON byte and
file-identity verification; summaries and failed observations remain intact.
The compaction receipt is included. Storage ceilings were not raised. Real
Vortex payload proof remains distinct from placeholder artifact status, and
CG-1 through CG-23 retain their independent obligations.
