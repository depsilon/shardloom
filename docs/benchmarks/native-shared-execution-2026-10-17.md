<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared numeric and dictionary execution

The retained changes reduce the matched 43-query sum of per-query two-sample
means from **57.310191503 to 54.899272149 engine seconds**, a **4.206790%**
improvement. Both paired totals improve. All 172 complete results and schemas
match, with no query exceeding the predefined material-regression rule.

The original frozen acceptance threshold was 5%, which this result failed.
After seeing that result, the maintainer explicitly accepted improvements
greater than 3% and requested a PR before ingestion work. The original failed
gate, measurements and later acceptance decision are separate records. No
measurements were repeated or substituted to meet the revised bar.

This is a two-sample matched comparison, not a standard single-pass Full43
result. Local acceptance and portable evidence readback pass; final-head hosted
checks remain merge gates. The
[implementation contract](../architecture/native-shared-execution-2026-10-17.md)
records each experiment, admission decision and rejection. Published 0.6.0
packages and the broader PERF and CG-1 through CG-23 obligations are unchanged.

## Retained shared mechanisms

- Bind original-width native integer access outside the single-integer grouped
  count loop. Explicit selections retain their order, duplicates and checked
  bounds; signed and unsigned identities remain distinct. Borrow the original
  Vortex slices without copying a value column.
- Store complete two-integer sort records in 17 bytes instead of a padded
  24-byte representation. Big-endian fields preserve the existing tuple order
  and every identity bit. Partition routing, exact count reduction and result
  selection continue to use the complete key, with capacity credit adjusted to
  the actual representation.
- Reduce admitted transformed dictionary measures inside two bounded existing
  preparation jobs. Numeric partials and source-index UTF8 extrema retain their
  dictionary owner until ordered caller merge. Persistent winners acquire their
  normal ownership only when adopted. Smaller CPU grants, encoded Dict chunks
  and declined cardinality samples retain their existing native consumers.

Admission depends on types, operators, available credits and resource policy.
There are no query numbers, column names, aliases or data values selecting these
paths. Native canonicalization remains on the caller; the later experiment
that moved it into workers failed its resource gate and is excluded. Vortex
metadata, pruning, encoded providers, source order, cancellation, overflow and
no-replay behavior remain explicit. No dependency or external executor is added.

## Whole-workload evidence

| Aggregation | Control engine seconds | Candidate engine seconds | Candidate minus control |
| --- | ---: | ---: | ---: |
| Sum of all 43 per-query means | 57.310191503 | 54.899272149 | −2.410919354 |
| Paired repetition 1 | 57.423112668 | 55.239131917 | −2.183980751 |
| Paired repetition 2 | 57.197270338 | 54.559412381 | −2.637857957 |

Both builds run the same resident input through the ordinary public native CLI,
using fresh processes and counterbalanced ABBA/BAAB order within each query.
All queries and slower samples remain in the result. A query flags only when
its mean increase exceeds the larger of 10% and 0.15 engine seconds and both
paired differences are positive. No query flags. There is no cache purge,
artificial delay, automatic retry or best-sample selection. These observations
do not eliminate host variation or establish results on other workloads.

The earlier two-mechanism batch measured 1.923964% improvement and failed its
5% threshold. Adding source-index dictionary extrema produced the separate
4.206790% comparison above. Individual focused gains are not added together.
The final runtime is restored to every selected source hash of measured build
618 before broad validation; rejected worker-canonicalization code is absent.
After acceptance, two adjacent comments are corrected to describe one or two
preparation workers. The [exact comment bridge](evidence/native-shared-execution-comment-bridge-2026-10-17.json)
verifies that every other byte, all source line counts and the remaining 2,411
selected source hashes are unchanged.

## Experiment record

Each comparison uses its own unchanged decision rule. The complete record has
1,242 checked calls across these attempts, including the 18 completed calls
from an interrupted screen that receives no performance credit.

| Attempt | Shared mechanism | Complete calls | Recorded disposition |
| --- | --- | ---: | --- |
| 598 | Hash-table chunk dictionary | 48 | Drop: target gain and consistency |
| 599 | Bound text and numeric access | 70 | Retain numeric family; drop text family |
| 600 | Compact complete numeric-pair records | 46 | Retain for batch |
| 601 | Bound group preparation | 50 | Drop: unchanged-path control |
| 603 | Dense canonical string counts | 60 | Drop: target and control |
| 605 | Separate integer maps and bound groups | 74 | Drop: consistency and controls |
| 607 | One-write integer hashing and static group preparation | 82 | Drop: control |
| 609 | Occupied string-count table | 66 | Drop: target gain and consistency |
| 611 | Two retained mechanisms, all 43 queries | 172 | Original 5% gate fails |
| 612 | Compact string lookup directory | 90 | Drop: target gain and consistency |
| 614 | Expanded native preparation workers | 98 | Drop: resource efficiency |
| 616 | Numeric dictionary partials | 18 of 98 | Discard: canonical target not admitted |
| 618 | Dictionary partials with source-index extrema | 98 | Retain for batch |
| 619 | Three retained mechanisms, all 43 queries | 172 | Original 5% gate fails; later accepted above 3% |
| 621 | Co-located native preparation and reduction | 98 | Drop: resource efficiency |

Plans conditional on failed components were not run. The incomplete 616 screen
excluded the canonical target's UTF8 minimum measure; its stopped cohort and
cleanup evidence remain. Later canonical-SQL fixture checks establish that
618 and 621 exercise their intended routes before timing. The revised
whole-workload bar does not rehabilitate either resource-gate failure.

## Validation and reproduction

The full-query comparison independently reopens complete results, schemas,
native certificates, raw engine clocks and companion records. Every value uses
the retained native regression reference; paired schemas match the accepted
control. Independent format/workflow oracle checks are a separate gate.

Focused checks cover all integer widths and signedness, selected-row order and
partial error state, complete-key tuple ordering, exact capacity charges,
dictionary collisions, precision-sensitive weighted arithmetic, source-backed
minimum/maximum ownership, cancellation, pressure retirement, ordered merging
and source failures without replay. The retained patch adds 12 native tests and
extends three existing ownership, cancellation and source-failure cases.

Formatting and strict workspace Clippy pass. Workspace tests record 4,374
successful executions with no exclusions. Native-feature tests record 4,037
successful executions and 27 existing manual exclusions; native all-target
Clippy passes on the identical measured source. These are execution counts
across configurations, not unique methods.

The fresh release binding passes all 100 direct methods with no skips. All
4,827 public cases across 36 families pass. Independent readback checks 13,004
compressed JSON files, 6,330 native envelopes, 39,979 explicit expected value
rows and 1,578,741 pressure-case rows. The format/oracle matrix passes all 18
cohorts: 704 native workflow records and 704 pandas reference records. It checks
all eight inputs with collected output and CSV input with all eight outputs,
in both raw and prepared form, across all 22 workflows; it does not claim every
input/output combination. Installed-wheel, final-head CI/review and hosted
deployment proof remain separate merge gates.

The control is the accepted native text reconstruction implementation merged
in PR #1567 at `5bcee25a`; its measured build is `9ffac6b7` with unchanged
runtime sources. The candidate is the retained patch against `5bcee25a`, with
2,412 selected source hashes and a frozen release executable. Both use
`release-user-surfaces` with Vortex 0.85.

| Artifact | SHA-256 |
| --- | --- |
| Control CLI | `b02196fde4f2fff291cb6562a18d6d31b8aab405c92418c16f65a81c1fa544f5` |
| Retained candidate CLI | `a0b0b95c3440806d8445ca8b353e32a97906c65889273a0a8cf15c282a22406a` |
| Native input | `06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22` |

The accepted Vortex input contains 99,997,497 rows and occupies 15,713,610,513
bytes. The host uses macOS 27.0, ten logical CPUs and 16 GiB physical RAM. The
declared policy is 24 GiB memory and parallelism 12, bounded by local capacity;
this is not a process RSS ceiling. Input prehashing, uncontrolled OS caches and
desktop activity are preserved in the diagnostic evidence. Only the engine
execution clock contributes to the tables. Preparation, serialization,
validation and supervision remain separately recorded.

The input representation and ingestion code are unchanged. No new full-size
ingestion is required for this read-side patch. The next ingestion work targets
material native-file size reduction without a fixed percentage target and must
check ingestion cost and query engine time together.

## Portable evidence

The [index](evidence/native-shared-execution-2026-10-17.json) and
[complete compressed manifest](evidence/native-shared-execution-2026-10-17.manifest.json.xz)
bind 28,621 members. The five archives contain 411,823,165 raw bytes in
31,642,988 compressed bytes:
[part 1](evidence/native-shared-execution-2026-10-17.tar.xz),
[part 2](evidence/native-shared-execution-2026-10-17.part-2.tar.xz),
[part 3](evidence/native-shared-execution-2026-10-17.part-3.tar.xz),
[part 4](evidence/native-shared-execution-2026-10-17.part-4.tar.xz), and
[part 5](evidence/native-shared-execution-2026-10-17.part-5.tar.xz).

Every archive member is reopened and hashed. The verifier independently checks
all 1,242 complete comparisons across 58 paired cohorts, including their raw
engine clocks, complete values and schemas, certificates, companion records
and timing CSVs. Both original Full43 threshold failures and the later explicit
acceptance of the 4.206790% result are recomputed separately. The 18 completed
calls from the interrupted screen retain integrity credit only.

The packet includes all 15 experiment protocols, exact designs and source
patches, the complete retained source inventory, nine diagnostic profiles,
dependency intake, local checks and executable reproduction helpers. Two
archive preflight naming collisions and their corrections are preserved;
measurements and runtime acceptance are unchanged. Large datasets, executables
and public output payloads are omitted with exact identities. Complete result
values and the separate format/oracle reports remain included. Replay requires
the declared resident inputs and new output paths; historical paths must not
be reused for a new run.

## Per-query engine means

Each cell is the mean of two complete executions. Every individual sample and
raw integer clock remains in the paired evidence.

| Query | Control engine seconds | Candidate engine seconds |
| --- | ---: | ---: |
| Q1 | 0.000046541 | 0.0000435205 |
| Q2 | 0.1367888335 | 0.1528195835 |
| Q3 | 0.879125605 | 0.91375075 |
| Q4 | 0.3867298535 | 0.391137062 |
| Q5 | 1.3470924585 | 1.3258801055 |
| Q6 | 1.1644284585 | 1.170675417 |
| Q7 | 0.0004731035 | 0.000468854 |
| Q8 | 0.1493254585 | 0.1562876865 |
| Q9 | 1.281130625 | 1.252533147 |
| Q10 | 2.456598417 | 1.837286834 |
| Q11 | 0.870439667 | 0.8647996875 |
| Q12 | 1.5029202085 | 1.4750246875 |
| Q13 | 0.601417751 | 0.6045747715 |
| Q14 | 1.6109093545 | 1.5860484795 |
| Q15 | 1.6226986245 | 1.5116338545 |
| Q16 | 2.5022100625 | 1.7296501665 |
| Q17 | 2.6423020835 | 2.598589167 |
| Q18 | 0.3320408335 | 0.363203478 |
| Q19 | 4.904113438 | 4.986915063 |
| Q20 | 0.194584791 | 0.207775354 |
| Q21 | 1.148836125 | 1.0475549375 |
| Q22 | 1.445046042 | 1.4149903545 |
| Q23 | 3.7908225405 | 3.8344230425 |
| Q24 | 1.274756876 | 1.360441583 |
| Q25 | 0.3343529375 | 0.3194720215 |
| Q26 | 1.3973955 | 1.3911777085 |
| Q27 | 1.4683130625 | 1.475055354 |
| Q28 | 2.335596687 | 2.272499688 |
| Q29 | 5.932256166 | 5.1684050205 |
| Q30 | 0.2575225 | 0.261654999 |
| Q31 | 1.3311760005 | 1.1611003955 |
| Q32 | 1.44205725 | 1.526184625 |
| Q33 | 2.4082024585 | 2.1963324595 |
| Q34 | 2.628657626 | 2.8776712295 |
| Q35 | 2.634694229 | 2.609844228 |
| Q36 | 0.4362559995 | 0.440476063 |
| Q37 | 0.3536659595 | 0.302640979 |
| Q38 | 0.2701908125 | 0.358529625 |
| Q39 | 0.218967208 | 0.2248376255 |
| Q40 | 0.8253675 | 0.8431786245 |
| Q41 | 0.4100912925 | 0.2923731465 |
| Q42 | 0.1847365205 | 0.1918259585 |
| Q43 | 0.195854042 | 0.1995048115 |
