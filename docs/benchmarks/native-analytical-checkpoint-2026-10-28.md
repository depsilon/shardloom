<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared analytical execution: current UAT checkpoint

The current runtime at `12b5e98cae672b9fbc2f999ff948b74cee328d4d` passes a complete
direct-Parquet Full43 at **651.558161209 engine seconds**: exactly one execution
of each query and all **381 result rows** independently checked against the
unchanged reference. It reads the original **99,997,497-row** Parquet file through
native source demand, with **no ingest, preparation command or prepared-input
write**. The [checkpoint receipt](evidence/native-item20-completion-2026-10-28/full43-checkpoint-r4/checkpoint.json)
links the source, runtime, full results and current validation. The
[result archive](evidence/native-item20-completion-2026-10-28/full43-checkpoint-r4/results.tar.xz)
retains the 43 compressed raw results, independent reference, original/submitted
SQL, source snapshots, check logs and installed-package proof.

The maintainer requests merging PR #1619 at this checkpoint and continuing the
remaining analytical work in later cohesive PRs. This supersedes the earlier
instruction to keep the PR open through all item 20. The
[remaining checklist](../architecture/native-analytical-completion-2026-10-27.md#remaining-completion-checklist)
stays active, including the under-ten-second 347-query objective and the wider
capability, workflow and whole-program acceptance obligations. This checkpoint
does not publish a package or close a competitive gate.

## Execution and correctness boundary

The standard `scripts/run_clickbench_query_uat.py` runner receives
`--input-format parquet`, `--repeats 1`, `--memory-gb 24`,
`--max-parallelism 12`, `--timeout 120` and the independently frozen reference.
The runtime is a release build with `release-user-surfaces` on macOS 27 arm64,
a 10-core host with 16 GiB physical memory and Python 3.13.13. The 24-GiB setting
governs tracked native reservations; it is not an allocator-wide or process-RSS
limit. The exact command and all identities are retained in the archive.

Original Parquet identity: 14,779,976,446 bytes, SHA-256
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.
Independent reference identity:
`fc5780da415dc33bd06ed0879db07fc040db5dd419e3c37ecde7cd40a315696b`.
CLI identity:
`038bc3716aa5c7dda7ea62b3c8f46cd0186584d55664317c930922a63bbb1e70`.

All other task work was idle during query execution. Full input hashing and
recursive inventories occurred outside that interval. OS cache state was
uncontrolled; required preflight hashing may warm it. Storage limits, the
lightweight concurrency guard, deadlines, complete validation and cleanup stayed
enabled. Source identity is unchanged before/after the pass. Each native result
reports completed input, independent output ownership, zero prepared-input bytes
and no external engine or fallback execution.

The runner's pinned Parquet dialect makes output names, integer epoch semantics,
UTF-8 byte lengths and group ties explicit. Original SQL and submitted SQL are
retained; the independent reference and comparison tolerance are unchanged.
Offline replay compares every result and derives each engine clock from its
saved native timing report. No query is repeated for packaging or documentation.

## Failures and corrections retained

The first direct-Parquet attempt stopped on result naming at Q3. The second
validated Q1–29 before Q30 reached its unchanged deadline. Typed mixed-width
integer expressions corrected the demonstrated scalar path. The third validated
Q1–32 before Q33 refused aggregate-state growth under the unchanged memory limit.
Leased compact grouped COUNT/SUM/AVG state corrected that resource failure.
These attempts remain incomplete and receive no Full43 total.

In the final complete run Q30 takes **36.271361250 engine seconds** and Q33 takes
**53.009211958 engine seconds**. The earlier focused Q33 observation remains
**66.174310876 seconds**; it is not substituted into the Full43 sum. The wrapper's
incorrect Full43-only flag check and the offline held-out serialization correction
remain recorded, with original results preserved and no repeats to repair reporting.

The unchanged 347-query cohort passes all 12,565 rows at **15.304317834 engine
seconds**. The memory correction adds **0.124005871 seconds** to the preceding
15.180311963-second checkpoint and fails the optimization gain/loss rule. It is
retained as a required resource correction, without a speedup claim. Earlier
retained improvements and dropped candidates keep their prospective decisions;
their historical savings are not added together.

## Current-source checks

- Formatting and strict workspace/native lint pass.
- Default workspace tests: **3,274 passed**.
- Native-feature suite: **5,057 passed**, with 35 existing optional ignores.
- Seven public regressions pass across CLI SQL, embedded workflow, Python SQL,
  DataFrame and typed native execution.
- Fresh held-out correctness: **160 complete observations**, 1,584,120 result
  rows and 131,116 independently reopened input rows pass.
- Installed native wheel: **207 integration tests pass without skips**;
  isolated load and uninstall pass. Its CLI/native library hashes match the
  measured runtime. The local wheel retains the development version and is not
  a published replacement for the immutable 1.0.0 release.

The archive has complete raw Full43 evidence and current check/install logs.
The 347-case and held-out summaries link their full local replay through the
existing disposition; this archive does not claim whole-item20 portable replay.
Large input, executable and wheel payloads are excluded while exact identities
are retained. Hash consistency is not signed provenance. Hosted checks and merge
acceptance are separate from these local results.

## Source-aware follow-up

Direct execution is correct without ingest. Repeated use can still benefit from
explicit Vortex preparation and its retained encodings, layout and metadata.
The current relational preparation selector admits an additional file strategy
only for one prepared Vortex source with no memory or batch sources; Parquet
demand enters as batches. Its decoded batches use the optimized shared relational
operators, while eligible prepared Vortex plans can additionally select encoded
file count, aggregation, sort and projection strategies after semantic checks.

All 43 current observations report the batch-source route. For example, Q1
delivers 99,997,497 rows for COUNT, and Q24 decodes all 105 demanded columns over
226 row groups before its selective result. These are concrete work observations,
not an attribution of the entire timing difference. The historical prepared-Vortex
Full43 of 46.973255495 seconds has a different source representation and earlier
runtime; no matched speedup or regression ratio is calculated.

Next work should trace avoidable decoding/materialization, metadata shortcuts,
predicate/selection pushdown and access to equivalent native strategies through
the shared planner. Exercise ordinary public declarations against direct sources
and explicit prepared/reused sources, using the same current runtime when making
a comparison. Preparation remains visible and separate from query engine timing.
Source capability and proven semantics determine the strategy; benchmark IDs,
public-interface-specific engines and hidden preparation retries do not.
