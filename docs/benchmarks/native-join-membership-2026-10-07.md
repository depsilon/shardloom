# Conservative native join membership experiment

Disposition: **dropped** under PERF-02/03/10/12. The binary fuse candidate
reduces the mostly absent cohort's complete-operation score by 6.88%, but the
50% and 90% match controls regress by 4.17% and 3.28%. Both cross the frozen
regression gate. Exact results pass; the candidate is removed. No reversed-order
confirmation, production implementation, shipped speedup or version bump follows.

The [decision index](evidence/native-join-membership-2026-10-07.json),
[portable packet](evidence/native-join-membership-2026-10-07.tar.xz) and
[restoration proof](evidence/native-join-membership-restoration-2026-10-07.json)
preserve the experiment and prove all 941 runtime assets match accepted builder
runtime `53cd1582`. The [design](../architecture/native-join-membership-screen-2026-10-07.md)
describes the removed test candidate. Completion-aware input is the next
architectural capability; the other
[conditional-work tracks](../architecture/native-conditional-work-campaign-2026-10-07.md)
retain their prerequisites.

## Candidate and exactness

An original three-segment, eight-bit binary fuse filter is built after the
existing right-side join table is complete and before left-side probing. It
admits only inner equijoins without a residual condition and with 16,384 through
1,048,576 distinct exact-index entries. It uses the existing normalized key
hashes, deduplicates equal hashes, tries at most four fixed seeds, and verifies
every build hash before publication. Positive answers still use the unchanged
exact equality and duplicate chain. NULL keys retain their existing behavior.

The filter belongs to one sealed join and cannot receive later build rows or
outlive its source owners. Every construction vector and final fingerprint
buffer carries shared credits. The optional construction is skipped when its
conservative peak estimate exceeds a quarter of currently available credits.
Incomplete construction never supplies negative answers. Existing source
generation checks and cancellation remain authoritative. No dependency, external
engine, alternative index, source format or persisted filter is introduced.

The focused native run passes 31 test executions covering 29 distinct tests.
These include generated and duplicate hashes, exact build-key membership,
bounded construction failure and successful retry, memory release, headroom
refusal, cancellation, build-after-seal rejection, late source replacement,
NULLs, mixed numeric normalization and duplicate output order. Eighteen Python
tests check the independent oracle, scoring boundaries and evidence supervisor,
including failed-process and changed-identity receipt retention.

Seven real native fixture pairs pass full independent output comparison for
both strategies before timing. The timed cohort adds two full prechecks and two
full postchecks per cell. Each compares every little-endian `(left_id, right_id)`
pair in order, checks EOF and the nonnullable two-u64 output schema. Every timed
call also consumes and hashes every result, validates the complete report, and
checks that all query credits are released. A digest alone is not the independent
equality proof.

## Complete-operation result

Both strategies run in the same release test executable and process for each
cell, selected explicitly before each complete operation. Three warmups per
strategy precede 15 alternating pairs, with five complete calls per member.
There are 105 pairs and **1,050 timed complete calls**; no samples are discarded.

The clock starts before native session creation and plan binding. It includes
native reads, exact-index construction, filter construction and verification,
all probes, duplicate gathering, every output row, bounded native output
conversion and hashing, certificate checks, and destruction of all query owners.
The inert request declaration, fixture creation, file prehashing and independent
oracle generation are outside the clock. There is no reused build table or
query-answer cache. Mechanism counters are compiled out of timed calls.

| Workload | Distinct build keys / copies | Probe rows | Output rows | Candidate/control | Change |
| --- | ---: | ---: | ---: | ---: | ---: |
| U64, 1% match recipe | 262,144 / 1 | 1,048,576 | 10,375 | 0.912515 | −8.75% |
| Long UTF-8, 1% match recipe | 131,072 / 1 | 524,288 | 5,309 | 0.967743 | −3.23% |
| U64/string tuple, 1% match recipe | 131,072 / 1 | 524,288 | 5,309 | 0.945900 | −5.41% |
| Duplicate U64, 1% match recipe | 65,536 / 4 | 524,288 | 21,236 | 0.900173 | −9.98% |
| U64, 50% match control | 262,144 / 1 | 1,048,576 | 524,240 | 1.041701 | +4.17% |
| U64, 90% match control | 262,144 / 1 | 1,048,576 | 943,953 | 1.032800 | +3.28% |
| Small ineligible U64 control | 4,096 / 1 | 65,536 | 685 | 0.997150 | −0.28% |

Ratios are medians of 15 paired five-call sums. The first four rows define the
primary score: their geometric mean is **0.931200**, with a fixed-seed,
10,000-resample stratified paired-bootstrap 95% interval of
**[0.924896, 0.934047]**. All four primary cells exceed the required 3% gain.

Retention also forbids any cell with both a ratio above 1.03 and a median
increase above 100 microseconds per complete call. The 50% and 90% controls
increase by **12.934 ms** and **13.106 ms**. This fails that gate even though
the primary score and confidence interval pass. Eligibility and thresholds were
not retuned after seeing the controls. A different future placement would need
its own mechanism, admission proof and frozen experiment.

## Work avoided and resource cost

Untimed prechecks and postchecks agree on the mechanism. All eligible fixtures
construct a verified filter on their first attempt. The small control remains
unfiltered. Counts below describe one complete operation.

| Workload | Exact lookups, control → candidate | Rejected probes | Final filter bytes | Peak native reservation, control → candidate |
| --- | ---: | ---: | ---: | ---: |
| U64, mostly absent | 1,048,576 → 14,463 | 1,034,113 | 303,104 | 21,578,820 → 37,256,072 |
| UTF-8, mostly absent | 524,288 → 7,348 | 516,940 | 155,648 | 222,899,356 → 223,055,004 |
| Tuple, mostly absent | 524,288 → 7,410 | 516,878 | 155,648 | 48,395,388 → 49,229,512 |
| Duplicate U64, mostly absent | 524,288 → 7,336 | 516,952 | 77,824 | 13,717,484 → 17,468,104 |
| U64, 50% match | 1,048,576 → 526,319 | 522,257 | 303,104 | 21,622,212 → 37,256,040 |
| U64, 90% match | 1,048,576 → 944,359 | 104,217 | 303,104 | 21,654,132 → 37,256,040 |

The mostly absent cells avoid about 98.6% of exact index lookups, while still
reading and hashing every probe. This is work avoidance, not a corresponding
percentage reduction in query time, decoded bytes or source I/O. Construction
scratch is much larger than the final filter: the conservative estimate is
15,966,792 bytes for 262,144 hashes. Total reservation peaks occur at different
stages, so subtracting their maxima does not isolate construction memory.

Each strategy has the same 536,870,912-byte native grant and one-CPU policy.
Every call reports zero denied reservations and zero retained credits after
destruction. OS process RSS ranges from 20,283,392 to 441,171,968 bytes across
cells. Each process includes both strategies, so its CPU/RSS observations cannot
establish a per-strategy CPU or RSS saving. The measured supervised cohort lasts
181.839377 seconds, separate from paired call sums and archive/analysis work.
These observations do not establish a whole-process allocation ceiling.

Every measured Native I/O certificate is certified with
`fallback_attempted=false`. That schema does not expose `external_engine_invoked`;
the evidence records null and the limitation. The candidate source introduces
no external engine or execution delegation.

## Reproduction and limits

The host is Apple M5, arm64 macOS 27.0, ten logical CPUs and 17,179,869,184 bytes
of physical RAM. The release test executable uses Rust 1.99.0, Vortex 0.85.0,
`release-user-surfaces`, locked offline dependencies, two build jobs and disabled
incremental compilation. Each native input is a flat Vortex file with 8,192-row
physical chunks. The deterministic key, match and duplicate recipes and every
expected pair are preserved. Newly created files and prehashing warm caches;
OS cache state is uncontrolled. No cold-cache result is claimed.

- Executable SHA-256:
  `baa2e49f58b4ee0d2be27c2bc96ab0cb8fc9d4ce57d7752bb7c7d64fc08301ae`.
- Frozen screen manifest SHA-256:
  `1b1ec4a6d149b2add00a22e56f0bf2820841e09cdef56cbd32ee966c7a6f9139`.
- Portable archive SHA-256:
  `981973b93210f0e736d2e6f59acb2d206b7a1cc4b1e8982f07d6b4b75958000b`.

The 8,570,048-byte packet preserves 1,134 paths in 1,104 unique payloads. It
includes the complete 943-asset candidate snapshot, seven original files, full
reference pairs, all native receipts and outputs, scoring code, guard helpers,
host observations, build records and failed preflights. All source, protocol,
scoring and native supervision helpers, including the external workload and
process guards, were frozen before timing. Finalization/restoration tooling is
identified separately as post-measurement evidence work.

The measured executable and native fixture payloads remain local with exact
hashes and identities; they are omitted from the portable archive. Reconstruct
the recorded source overlay, build with its recorded command, regenerate native
fixtures with the frozen harness, and compare every expected pair. Rebind local
paths and use fresh run names. A rebuilt executable is a new artifact and must
not be described as the measured binary.

Three failed compiler/lint preflights remain in the packet. They were corrected
before source/fixture freeze and before any scored timing. Focused format,
all-target Vortex clippy, correctness and harness checks pass. The failed
performance gate ends the candidate; broad candidate workspace/public/golden,
Full43, reversed-order and production acceptance were not run. Exact restoration
is verified against all 941 accepted runtime assets and the prior builder
identity. The dropped experiment makes no ClickBench, spill-I/O, production
support or general engine claim.
