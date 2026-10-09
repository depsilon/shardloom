<!-- SPDX-License-Identifier: Apache-2.0 -->

# Fresh UAT before the 0.5.0 version bump

The accepted native pivot executable passes fresh full ingest, all 129 ClickBench
query comparisons and the 18-cohort input/output workflow matrix. The run and
independent packet inspection completed while source and package versions were
still 0.4.0. These are local correctness and timing observations for the recorded
source build; package installation and publication require their separate gates.

## Source and machine

| Item | Identity |
| --- | --- |
| Runtime source | `5665eee5b86a585ea50b7b54cbbd43f82154222f` |
| Native executable SHA-256 | `22eb39060595afbde22e9ed4a0f1ca87c3504c58fe68f22641c336048eee8404` |
| Frozen UAT source | `83cd67d663a241dbe11ffaf16113f9ecb4f1fe98` |
| Original runtime source assets | 1,015 |
| Machine | macOS 27.0 arm64, 10 logical CPUs, 16 GiB physical memory |
| ClickBench policy | 24-GiB requested memory policy, 12-worker maximum |

The UAT tree preserves 1,013 runtime-source assets and admits exactly the two
previously reviewed suite-summary harness repairs. All native and Python product
execution sources, case declarations, expected values and executable bytes match
the accepted runtime. The packet includes the original and repaired hashes.

After this packet was sealed, the
[test-diagnostic cleanup](evidence/native-pivot-test-diagnostics-2026-10-09.json)
changed two assertion failure messages in one test-only file. It preserves both
bound predicates, fixtures, expected values and structured synthetic counters.
That later tree has 1,012 original assets, two harness repairs and the one
test-message repair; this report does not relabel the earlier UAT tree or binary.
Subsequent version changes and release artifacts need their own source binding.

The physical RAM and requested policy are separate quantities. Tracked native
reservations do not establish a total-process RSS ceiling.

## Fresh ingest and complete query regression

Ingest reads the resident 14,779,976,446-byte Parquet input and creates a new
15,713,610,545-byte Vortex artifact. Input count, native stream validation and
prepared footer each report 99,997,497 rows. All 43 queries then execute three
times against that new file, with complete returned-value comparisons and
explicit no-fallback evidence.

| Measurement | Native process seconds |
| --- | ---: |
| Fresh ingest | 52.009961 |
| Query calls grouped by repeat 1 | 75.878297 |
| Query calls grouped by repeat 2 | 72.985721 |
| Query calls grouped by repeat 3 | 72.504476 |
| Sum of the minimum for each of 43 queries | 71.470899 |
| Sum of the median for each of 43 queries | 73.459554 |
| All 129 query calls | 221.368494 |

The harness executes each query three times before moving to the next query.
Repeat groups are sums of calls, not three chronological Full43 passes. Each
native duration includes process startup, complete output and exit. The separate
guarded ingest and query stages take 61.469506 and 252.387183 seconds, including
their monitoring and validation. Adding stage durations does not create an
observed combined end-to-end wall clock.

The source was hashed before ingest, and the query input had just been written.
OS cache and ordinary desktop activity were uncontrolled; no cold-cache claim
is made. Each query uses a new native process and no answer cache. Maximum
observed child RSS is 2,453,323,776 bytes for ingest and 5,825,331,200 bytes across
the query calls. These are observations, not enforced memory limits.

All 129 complete results match the retained native regression values, with the
existing `1e-12` finite-float tolerance. This is neither a fresh independent SQL
oracle nor an all-cell comparison between the large input and output files.
The independent operator fixtures and pressure proofs remain in the
[pivot acceptance](native-pivot-pressure-2026-10-08.md). This cohort is unpaired
and makes no speedup, regression attribution or official benchmark-rank claim.

Post-run hashing verifies both file generations and complete byte identities
outside native timing. Both files remained unchanged during hashing.

| Artifact | SHA-256 |
| --- | --- |
| Resident Parquet source | `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f` |
| Fresh Vortex output | `c541453175a8d947f8ab8073db91cca79e7988ba9a7e143be881e5c750e1c72f` |

## Complete format workflows

All 18 serial cohorts pass: 1,408 comparison records, including 704 native
records, across 22 declared workflows with 1,000 fact rows and 20 dimension rows.
The matrix combines two complementary selections:

- Eight input formats with collected results, using both raw and prepared input.
- Eight output formats from CSV, using both raw and prepared input.

Inputs are CSV, JSON, JSONL, Vortex, Parquet, Arrow IPC, Avro and ORC. Outputs are
Vortex, JSON, JSONL, CSV, Parquet, Arrow IPC, Avro and ORC. This is not a complete
Cartesian input/output matrix. Independent pandas values are a testing oracle
only; candidate records execute through the shared Vortex-native family.
Vortex references use equivalent original CSV in pandas and are not same-format
performance comparisons. Each declared writer result is reopened and compared.

The guarded matrix takes 477.470542 seconds, including fixture preparation,
reference work, native execution, output readback and validation. It is not a
native query-performance score. The prior complete 32,497-check pivot portfolio
and 442-case streaming suite remain linked acceptance for the same executable;
this UAT does not claim to have freshly rerun those suites.

## Retained evidence and storage

The [portable packet](evidence/release-candidate-fresh-uat-2026-10-09.json.xz)
contains all 129 raw query reports and complete values, all 18 workflow reports,
11,311 workflow process logs, original failures, source and artifact identities,
the recording/inspection helpers and their guard receipts. It is 1,795,420
compressed bytes with SHA-256
`6ecddbaf0aecec01ad01dd11f969942aa761062f7ce070603763d1ecd3906e02`.
Its 208,428,741 uncompressed JSON bytes have SHA-256
`26f5085bcf61bc0e757cdc751d8e58840c75d19d073278a6d8e5a1d35c313224`.

The [independent streaming inspection](evidence/release-candidate-fresh-uat-inspection-2026-10-09.json)
checks both identities, complete query/workflow counts, native-family reports,
no-fallback fields, required false claim boundaries and absence of private local
paths. Its frozen inspector has 60 positive and negative contract checks. The
recorder reopens original reports and comparison outputs; the inspector separately
parses the resulting packet without using the recorder's counters as authority.

The first admission attempt stopped before ingest, target creation or query work
at the unchanged 252-MiB log threshold. The exact completed prior pivot Full43
cohort's 516 original JSON/companion files were compacted, with identity, closed
handle and complete archive-member verification. This recovered 3,362,816
accounted bytes and preserved the original summary and unsuccessful observations.
The complete second attempt then passed admission. No incomplete native runs
were combined, and no storage ceiling was raised.

All three UAT stages ran serially under the existing storage and process guards;
their children were reaped and process groups drained. Real Vortex payload proof
remains distinct from placeholder artifacts. This finite pre-bump acceptance
does not complete the broader local-workflow queue, certify production support,
or discharge CG-1 through CG-23.
