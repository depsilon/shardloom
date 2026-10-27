<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native source demand: local Parquet acceptance

The source-demand work unit is implemented and locally accepted. Eligible fresh
single-file Parquet queries bind before data reading, request the required columns
and eligible row groups, and feed owned Vortex batches to the existing native
operators. They do not first write a complete prepared input artifact. The same
implementation serves CLI SQL, embedded workflow, Python SQL, DataFrame and typed
relations. Explicit preparation and checked prepared-artifact reuse remain.
This source change is not yet merged or released.

The corrected 347-query checkpoint passes every complete result in
**50.877676708 seconds of engine time**. All 347 queries use source demand and
write zero prepared-input bytes; **344/347 (99.1%)** physically reduce column
demand, and 21/347 (6.1%) exclude row groups. One isolated Full43 pass also
passes, at **46.783400285 seconds of engine time**.

The [design and scope](../architecture/native-source-demand-2026-10-26.md) and
[portable evidence](evidence/native-source-demand-2026-10-26/README.md) are the
contracts for these claims. This closes the first source-demand work unit within
item 20, not the broader analytical program, item 18, or CG-1 through CG-23.

## What changed

Metadata admission retains a checked source generation before native binding.
The shared bound plan derives output and hidden predicate, grouping, join and
expression dependencies. The Parquet adapter applies physical projection and
conservative statistics exclusion, while native operators still evaluate rows.
Repeated references combine their demands and use the existing native replay
owner. A reader advances only on consumer demand and releases its original
batch before requesting another.

Query access validates demanded data and the held generation, including after
the final consumer. It does not certify unread columns or excluded regions.
Full ingestion retains its full-source validation contract. Directories,
explicit schemas, content-digest fingerprinting, explicit `prepare_once` and
primitive options retain their existing routes. There is no automatic retry
through preparation or an external engine after a demanded-read failure.
Vortex remains native execution and output; Parquet supplies metadata and
decoding only.

## Frozen comparison and acceptance

Before runtime edits, 39 observations were frozen: two fresh full-preparation
observations for each of nine cases, nine already-prepared Vortex controls,
nine same-Parquet Polars observations and a three-query sequence. The fact
source contains 1,000,000 rows, 38 logical columns and 123 Parquet row groups,
with Zstandard compression and complete typed readback against the original
generated data. A fresh source means no prepared artifact, not a cold OS cache.

The prospective rule, whose SHA-256 is
`5b3811e67a32a8ca83cce2bfd6019ca6a55a40671b5671aecbcba03d9ee2d7bc`,
requires at least six cases, including the statistics-prunable case, to improve
complete workflow cost by both 25% and 0.250 seconds against the better original
observation. All cases must remain within the larger of 10%, 0.100 seconds or
the observed baseline spread. Prepared controls allow the larger of 10% or
0.020 seconds; the sequence must improve by at least 10%. These finite
thresholds are not confidence intervals.

All nine first-use cases pass the benefit rule in the final runtime, as do
the prepared controls and sequence. The following complete-workflow measurements
answer the requested first-use question; they are not engine-only measurements.
They include source preparation where required and complete Python-row delivery.

| Focused case | Original best fresh workflow (s) | Final demanded workflow (s) | Retained same-Parquet Polars workflow (s) |
| --- | ---: | ---: | ---: |
| Single-row, statistics-prunable predicate | 0.831594 | 0.012256 | 0.101566 |
| Narrow projection | 0.843480 | 0.114060 | 0.041000 |
| Alias and filter | 0.836453 | 0.185264 | 0.037690 |
| Rich projection | 3.462364 | 0.768756 | 0.044589 |
| Wide projection | 4.571158 | 2.806585 | 0.045533 |
| Unprunable selectivity control | 0.890951 | 0.034028 | 0.040166 |
| Grouped numeric aggregation | 0.916599 | 0.207498 | 0.043136 |
| Nullable aggregation with selective predicate | 0.839941 | 0.078505 | 0.038887 |
| Repeated-source union | 0.834207 | 0.003453 | 0.030864 |

The three-query sequence improves from 3.607888459 to 1.061574750 seconds of
complete workflow cost. Each sequence query opens a fresh session over the same
source; this proves repeated demanded execution, not incremental persistence.
Polars remains faster in six of the nine focused observations. Its engine clock
is unavailable and is not substituted with API time.

Six cases on a separately frozen seed (`7261026`) pass complete correctness
and declared physical-column bounds. The second runtime reuses that seed as
regression coverage; it is not described as another unseen holdout.

## Broad checkpoint and the correction it exposed

The maintainer requested a current 347-case checkpoint before the work unit
was finished. It uses the original case declarations and independent references,
with the same logical five-table dataset: 1,000,000 fact rows, 100,000 entities,
10,000 products, 200,000 relationships and 731 calendar rows. All five Parquet
files receive complete typed conversion checks. Each checkpoint executes every
case exactly once through Python SQL, with a fresh process/session, two lanes
and a 2 GiB native memory grant.

| Observation | Complete results | Engine time (s) | Queries with fewer physical columns | Queries excluding row groups |
| --- | ---: | ---: | ---: | ---: |
| First Parquet candidate | 347/347 | 296.288798449 | 34/347 | 21/347 |
| Corrected Parquet candidate | 347/347 | 50.877676708 | 344/347 | 21/347 |

Both checkpoints return the same 12,565 complete output rows and use demanded
input with zero prepared-input writes. Initially only 49 distinct cases (14.1%)
excluded columns or row groups. The corrected count is 344 (99.1%). The three
full-width cases are `projection_wide`, `predicate_or` and `predicate_not`;
the latter two preserve conservative residual-predicate barriers.

The first checkpoint exposed a shared binding gap: expanded identity projections
kept all source fields until native binding removed those projections. Aggregates
and computed projections then inherited unnecessarily wide scans. The correction
derives demand from the already-bound operator dependencies. It does not remove
fallible expressions or move computation across filtering, limits or residuals.

A broader regression test caught an attempted narrowing of an unused nonfinite
prepared-Vortex join payload. That would have weakened its existing validation.
Extra narrowing is now admitted only for owned demanded sources; prepared files
and caller batches preserve their prior validation scope. The original failure,
the targeted regression and the final passing suite are all retained.

The two Parquet checkpoints share the five input files, logical cases and
execution boundary. Their difference is an observed improvement after the
binding correction, not a statistically isolated speed prediction. There is
one observation per case, no cache manipulation and no claim of superiority.

The earlier 347-case result, 86.828000503 seconds of engine time, used prepared
Vortex and predates this work. It remains separately attributed. That cohort
paid 2.041 seconds of native preparation plus 0.056 seconds of publication once
for all five tables, from Arrow IPC. Adding those recorded components gives
88.925000503 seconds; it is not a new measurement or a same-Parquet comparison.

The original broad Polars observations remain fixed: 13.989659277 seconds at
the complete API boundary, starting from Arrow IPC. The planned new Polars pass
was canceled before execution. No new broad Polars query ran. Input and timing
boundaries differ from the new native checkpoints, so those retained competitor
numbers do not establish a matched same-format performance ratio.

## Physical work and resource evidence

The final single-row case reads two of 38 columns and one of 123 row groups:
38,435 actual data-read bytes, 570,317 metadata-read bytes, 8,192 decoded rows,
and 131,072 copied native input-buffer bytes. The grouped numeric case reads
two columns across all 123 groups and reports 2,429,631 data-read bytes. The
wide control reads all columns/groups and 107,525,511 data-read bytes.
These are observed positional reads, not unique byte coverage or decoded bytes.
Metadata counts describe initial admission and can repeat with reused descriptors.

The reader reports at most one retained original input batch. Native/read-buffer
credits, predecode workspace admission, cancellation, EOF, final source-generation
checks and I/O drain are covered. Native pool peaks and worker CPU/resource
observations are retained in the raw records. Upstream decoder allocations are
not allocator-limited, and the grant is not a whole-process RSS bound.
First-output latency, exact decoded-byte allocation and exact process peak RSS
remain unavailable; no zero is substituted for them. There is no background
preparation after completion.

## Verification and reproduction

Final validation passes:

- Required workspace formatting, strict Clippy and all-target tests.
- Native-feature strict Clippy; all-features, no-default-features and minimal
  reader feature checks. The minimal reader check retains five pre-existing
  unused-code warnings; it is a successful compile, not a warnings-as-errors claim.
- 568 native relational tests, including all 16 source-demand tests. Four
  separately frozen cost screens remain intentionally ignored.
- 151 public workflow tests, including seven source-demand route tests.
- 184 installed-wheel integration tests, no skips, plus actual load, artifact
  identity and uninstall proof. The expanded Parquet test checks 12 workloads
  across all five interfaces against independent complete typed results.
- 21 focused observations, six seed-regression observations and all 347 broad
  cases for each candidate, plus the 39 original observations.
- One isolated Full43 pass per candidate. The final pass compares all 43
  complete results and schemas (381 output rows) from 99,997,497 source rows.
  Its 46.783400285 engine seconds stay within the frozen regression trigger
  against the accepted 47.889862794-second prior result.
- Offline replay of 873 distinct complete observations, plus 11 deliberately
  corrupted-evidence rejection checks.

Public-status and documentation-productization checks also pass. The broader
release architecture tracker remains `blocked` solely because the phase plan
has 76 unchecked roadmap items, with no missing required inputs. Its original
nonzero result is retained; this work unit does not claim full-roadmap readiness.

Typed empty/null data, unsigned/decimal/temporal/nested data, absent or unsuitable
statistics, hidden columns, source changes, demanded/undemanded corruption,
late corruption after provisional batches, cancellation, memory refusal,
large streamed output, native replay/spill and mixed source owners have regression
coverage. Consumer-driven pull and release checks cover backpressure; no dedicated
sleep-based slow-consumer timing experiment is claimed.

The final release CLI SHA-256 is
`7c96ba5afa964d16260c3ebc8a23db4a5a761e5aa6fe2f0b4f192f180f342c72`;
the native library is
`3f79617b82ba05dd7223708233ccbbe01b2632265232f382cecebd37a389eed9`.
The build starts from `4d25a06e` with the exact retained source overlay;
earlier runtime and failure identities remain separate.

Run the repository's offline verifier with Python 3.10 or later:

```bash
python scripts/verify_native_source_demand_packet.py \
  docs/benchmarks/evidence/native-source-demand-2026-10-26
```

It verifies bounded archive objects and compares every complete result with its
retained reference without executing queries, importing packet code or requiring
the original absolute paths. The packet retains the measurement helpers,
immutable declarations, source snapshots, build/test/package receipts, raw
results and failures. Large data, binaries, installed environments and compiler
outputs are excluded. Reproducing execution requires regenerating the recorded
inputs and building the recorded source; digest consistency is not signed
execution provenance.

The [cleanup receipt](evidence/native-source-demand-2026-10-26/cleanup.json)
records retirement of 37,250 generated regular files and 12 environment links,
representing 10,266,036,786 regular-file bytes. Current source inputs, the final
CLI/library and installed-tested wheel remain. Complete compact results and
failures remain in the packet; no payload backup or storage-limit change occurs.
[Post-retirement replay](evidence/native-source-demand-2026-10-26/post-retirement-replay.json)
confirms all 873 results still verify after every retired path is absent.

The retained change is Parquet's first physical adapter for the shared demand
contract. Other formats, incremental persistence, automatic strategy learning,
background preparation, object-store expansion and broader capability parity
remain outside this accepted scope.
