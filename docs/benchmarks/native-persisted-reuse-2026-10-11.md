# Native persisted output and later computation

Status: local source/package gates, lifecycle costs, complete public regression,
fresh ingest/query/modular UAT and independent evidence verification pass.
PR #1544 is merged after all 44 hosted checks passed; installed native wheels
and actual production deployment are verified below.

This finite unit implements the [persisted-output contract](../architecture/native-persisted-reuse-2026-10-11.md)
under PERF-02/07/09/10/11/12 and CG-19/20/21. Version 0.5.1 stays fixed. It does
not complete the six remaining areas, eight investigations, or CG-1 through CG-23.

## Behavior and provider decision

Ordinary durable native writers now derive useful file statistics from the
arrays they actually emit. A later process can open the file and use its exact
counts and supported bounds without retaining the producing session. Projection,
rename, filtering, limits and computed results describe the new output rather
than inherit the original input's statistics. Native payload schema, validity
and admitted serializable encodings retain their existing writer contract.

The shared durable writer covers source-backed, completed, produced, streamed
and final spill-produced results. Ingest/preparation uses the same bounded
provider accumulation and resource policy. Ephemeral query-run files retain
their existing no-file-statistics policy. Existing native-file pass-through
and byte copies preserve their original artifacts.

The provider decision is `use_vortex_native_provider`: adapt the pinned Vortex
0.85 file-statistics accumulator and its existing serialization. No new file
format, sidecar, query-answer table, engine, or user-facing execution mode is
introduced. The reviewed Apache-2.0 patch maintains bounded running state rather
than per-batch statistic builders. Unknown observations remain unknown;
overflowed sums cannot disappear into a later exact total. Truncated bounds stay
conservative, and compact string/binary bounds do not retain whole input buffers.

The writer reserves 64 KiB plus 8 KiB per top-level field for statistic state,
merge/finalization temporaries and footer validation. Its lease survives writer
completion through validation. The session owns native compute allocations;
schema, layout and output containers keep their separate reservations. This is
native resource admission, not a whole-process RSS limit.

Unsupported nested extrema remain absent, while nested values stay exact native
payload. Nullable root records retain empty field-statistic sets rather than
publish unmasked child bounds. Empty/all-null values, field changes, extrema,
overflow, conservative bounds, cancellation and publication failures have focused
coverage. No reusable sortedness, partitioning or custom derived-state metadata
is newly claimed. Foreign-owner encoding preservation and independent producer
interchange retain their separate PERF-07 work.

## Frozen build and comparison

Build 343 records all 2,202 selected source, Cargo, Python, provider and license
assets at parent commit `5d5f51947f5bd2e5092609b32956db9266ec7a5b`, including the
uncommitted implementation. Every native acceptance stage checks that source
map and both frozen release artifacts. Documentation is outside that runtime
source map.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Candidate release CLI | 74,965,328 | `28ee8b115d15720787fde3ceb035103091cec50351127cb231d5097bbe83478b` |
| Candidate native extension | 74,563,360 | `771c049e1341421009eff8330ef2171297c621626dd73a7241c2b5c36de038a1` |
| Installed local wheel | 48,706,149 | `c109015b97ec2d40f590988c6711bd9260dac8cea3d8732221bfc0e8613c594f` |
| Retained baseline CLI, build 320 | 74,968,192 | `2476d1edbdec5977b1d076d06bf317ffe345bb47a30af7b3c53b9164f62c334f` |

The baseline is the retained final timestamp runtime at `20c3960d`, not a new
build of current main. The contract records its unchanged computed-writer
algorithm and the separate later certificate/test correction. These results
compare the named artifacts only. Build 343's CLI and extension are byte-identical
to build 342; the four changed files add or correct verification. Both current
artifacts have fresh guarded build receipts. The obsolete identical copies were
removed after verification.

The local machine is an Apple M5 with 10 logical CPUs and 16 GiB physical RAM,
running macOS 27.0 arm64. The paired output cohort admits 1 GiB and two execution
lanes. Those declarations are permission ceilings, not measurements of memory
consumption or CPU utilization. OS cache and ordinary desktop activity are
uncontrolled.

## Complete write and reuse cohort

The frozen fixture has 40,000 rows with an exact integer domain, nullable
Unicode labels and renamed columns. Each baseline/candidate pair independently
writes 64, 10,003 or 40,000 projected rows. One warm-up pair precedes five
alternating measured pairs per size. Every output is newly created, durably
closed, and consumed by new processes running three different jobs, each three
times:

- Exact `COUNT`, `MIN`, `MAX` and non-null count.
- A disjoint range projection with a complete empty result.
- A payload-reading range/sort control returning the final seven rows.

The independent inspector reconstructs the fixture's values without using
candidate answers. It checks every process receipt, timing/PID file and raw
output hash, and recomputes all cost margins. The complete cohort contains 36
outputs, 324 follow-on jobs and 36 full readbacks: 397 distinct native processes
including fixture preparation. It verifies 600,804 full-readback rows and 864
follow-on result rows, including all warm-ups and controls.

All 54 candidate aggregate jobs report exact file-footer completion and zero
returned arrays; all 54 corresponding baseline jobs consume arrays. Each
payload-reading control delivers its entire source row count. The disjoint
public projection proves complete values and native execution but does not
publish an actual payload-request counter. A separate moved-file regression
uses the existing instrumented native segment source: the disjoint predicate
causes zero payload requests, while a surviving predicate causes positive
requests and returns all expected rows. Segment requests and returned-array
counts are not physical device-byte measurements.

All 15 frozen cost gates pass. The table shows medians in milliseconds; each
gate uses the median of paired margins, rather than comparing only these
displayed medians.

| Output rows | Write baseline / candidate | Write + first three jobs | Write + all nine jobs | Payload control | File bytes baseline / candidate |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 11.227 / 10.930 | 28.631 / 27.946 | 62.577 / 61.779 | 5.757 / 5.751 | 4,008 / 4,536 |
| 10,003 | 12.434 / 12.318 | 30.657 / 29.796 | 67.207 / 65.165 | 6.269 / 6.287 | 323,736 / 324,512 |
| 40,000 | 13.295 / 13.026 | 35.016 / 31.941 | 75.596 / 70.124 | 7.517 / 7.502 | 1,288,664 / 1,290,160 |

The write tolerance is the greater of 10% or 5 ms; first and repeated lifecycle
costs must be within 5%; the scan control allows the greater of 5% or 5 ms; the
file-size tolerance is the greater of 4 KiB or 1%. These thresholds, queries,
fixtures, sample count and pairing were unchanged after the initial harness
repair. All pairs are retained.

The measured cohort's actual supervised workflow wall time is 21.185816 s.
It includes preparation, native processes, complete validation, hashing and
archive work after lock acquisition, and excludes final receipt serialization
and lock release. The table's lifecycle entries sum native process clocks and
exclude that surrounding work. They are local retention observations, not
confidence intervals, cold-cache results or competitive performance claims.

## Verification and preserved failures

The required default workspace format/lint/test gates pass, including 3,217
tests, before the subsequent feature-only lint and verification changes. The
current feature lint passes. The complete native library suite passes 2,641
tests on the production sources shared by builds 342 and 343, with 24 existing
ignored tests; the additional instrumented projection regression passes
separately. Current CLI targets pass 1,400 tests, with three existing ignored
tests. Seven provider accumulator tests run independently and are now required
in hosted CI.

Write-only, no-write, all-feature and minimum-supported-Rust profiles pass.
Six lifecycle harness tests cover source binding, missing/malformed evidence
and frozen gates. All 747 Python tests pass without failures, errors or skips;
the isolated installed wheel independently passes its 76 native binding tests.
Those 76 methods overlap the source suite and are not added to its count.
Seven packaging and seven license-tool tests also pass. Dependency/license and
forbidden-fallback checks pass; advisory audits were not requested by this
dependency run and retain their separate release gate.

The original failed native lint/test attempt and its corrected successor remain
preserved. The first lifecycle attempt stopped during its initial baseline
warm-up because the harness expected an aggregate-specific counter from a
plain projection; no measured pair completed. The source-binding correction
uses the actual quoted output path for all later jobs. The projection's empty
answer is not relabeled as I/O proof, and no cost gate changed.

Two whole-Python attempts exposed incomplete test environments: optional
dependencies were missing from the first interpreter, and isolated subprocesses
ignored the second attempt's `PYTHONPATH`. The successful run uses the installed
wheel environment for isolated children and the required optional packages for
the parent suite. No runtime source change was made to obtain that pass. A
separate staged-marker integration command selected zero tests under this
feature set and supplies no test evidence.

The generated website build and type checks, eight link tests, public-status,
website-readiness, static-asset and CI-matrix checks also pass. Desktop/mobile
reading, navigation, search and both themes were inspected. The
[documentation receipt](evidence/native-persisted-reuse-docs-2026-10-11.json)
records those checks and confirms all 2,202 frozen runtime assets still match.
The release tracker continues to report 137 unchecked phase items; its
`--allow-blocked` check does not claim broader completion.

## Complete public and fresh workload acceptance

The complete public matrix passes 32,497 cases and 18,595,284 row comparisons.
Independent readback reopens all 64,741 original public envelopes from their
verified archives, checks complete results against the frozen expectations,
and validates the resource-report contract. The direct matrix separately passes
202 cases and 131,734 row comparisons. These overlapping suites are not added
together. Typed input (87 checks), streaming (442), growth (20), five pressure
controls, batch adapters (48), format adapters (19), admitted semantics (145
stages) and golden workflows (nine stages) also pass.

Fresh UAT rebuilds native preparation from the resident official hits Parquet,
containing 99,997,497 rows and 14,779,976,446 bytes. It produces a new
15,713,610,513-byte Vortex artifact. Source SHA-256 is
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`;
output SHA-256 is
`004f9639fa0aa527b70f326c366c37b1173dceeacb92b50b2d8fd02957155c18`.
The source is resident and hashed before execution. Final source/output hashes
are outside native timing; a newly written output can already be cached.

| Fresh workload clock | Seconds | Boundary |
| --- | ---: | --- |
| Native ingest | 53.389291 | Native process through completed output and exit |
| All 129 native query runs | 278.040783 | Sum of complete native query processes, three runs for each of 43 queries |
| Ingest plus query native work | 331.430074 | Sum of the preceding native clocks, not elapsed end-to-end time |
| Supervised ingest-plus-query workflow | 376.265890 | Actual elapsed workflow, including supervision and query validation; excludes initial source hash and final artifact hashes |

The query run-index groups total 95.724180, 92.227864 and 90.088739 seconds.
They group each query's first, second and third calls; they are not three
chronological complete passes. Every query's complete returned values match the
retained native regression reference. That reference is not a fresh independent
oracle. This UAT makes no comparative performance claim.

The fresh run admits 24 GiB and 12 execution lanes on the same 16-GiB machine.
The declaration is not physical RAM or an enforced RSS cap. The OS reports
2,436,923,392 bytes peak RSS for the ingest child; raw CPU/RSS observations and
timing/PID records remain in the packet. Workloads execute serially under the
existing storage, source-residency, concurrency and process-cleanup guards.
No storage ceiling is raised. Each supervised stage exits successfully, reaps
its direct child and proves its process group has drained.

The fresh modular matrix completes all 18 cohorts and 1,408 comparison records,
including 704 native executions across 22 workflows. It exercises all eight
inputs into collection, then CSV input into all eight outputs, with both raw
and prepared input states. This is not the full input/output Cartesian product.
Complete comparisons use independent pandas values solely as a testing oracle.
Native execution keeps `fallback_attempted=false` and
`external_engine_invoked=false`.

## Scoped artifact cleanup

After all local acceptance and archive checks passed, the superseded
`timestamp-native-20261011-a1.vortex` was retired. Its 15,713,610,545 bytes
matched the previously accepted hash before deletion; it is a superseded
generated output, not a byte-identical duplicate of the current artifact.
The original Parquet and new accepted Vortex output retain their identities.
Open-handle checks and the UAT lock protect this retirement. Observed free
space increases from 64,222,285,824 to 79,936,045,056 bytes.

The [cleanup evidence](evidence/native-persisted-reuse-cleanup-2026-10-11.json)
also records removal of the completed 9.84-GB logical debug cache, two identical
superseded executable copies and 21 byte-identical historical fixture copies.
Every unique fixture, summary and failure log remains. The fixture cleanup
recovers 24,469,504 accounted log bytes and restores fresh-UAT admission under
the unchanged storage ceiling. No compiler-cache backup or replacement bulk
archive is created. Free-space observations include other filesystem activity
and are not engine performance measurements.

## Evidence and reproduction

The [distribution manifest](evidence/native-persisted-reuse-2026-10-11.json)
binds every local result and the separately supervised readback. The portable
packet preserves full public/direct results, fresh query/workflow records,
cost samples, process receipts, verification drivers, source identities and
original failures. Its 5,455,444,702 uncompressed bytes are streamed and
reopened for complete byte verification; no expanded disk copy is required.

The 56,583,888-byte compressed packet is distributed as
[part 1](evidence/native-persisted-reuse-2026-10-11.json.xz.part-001),
[part 2](evidence/native-persisted-reuse-2026-10-11.json.xz.part-002),
[part 3](evidence/native-persisted-reuse-2026-10-11.json.xz.part-003) and
[part 4](evidence/native-persisted-reuse-2026-10-11.json.xz.part-004).
Concatenate those parts in order and verify SHA-256
`97ae0043c9e556cee6c43715e8146808376ee7f4181e9b874c345d0cc96cf5af`
before reading the compressed JSON. Its uncompressed SHA-256 is
`f01a264cef5b78abacc5a2301ac383afda14dee5fede043cc94142429e843f85`.
Path placeholders alter only path text; original hashes identify original bytes.

The [source archive](evidence/native-persisted-reuse-source-2026-10-11.tar.xz)
is 7,110,424 bytes, SHA-256
`a3dcf05f56eccdc9acc09c92d5b31bd71df42c2b54d254b4eaf62b82a83d2eae`.
All 2,484 members, including its index, were reopened and verified. It preserves
all 2,202 frozen build assets, supporting receipts, the failed source-packaging
preflight and its corrected driver. That preflight referenced a nonexistent
toolchain file and stopped before creating an archive; the existing Cargo,
CI and build-receipt toolchain records remain included. Executables, installed
environments and physical datasets are separate artifacts.

Reproduction requires the recorded Rust toolchain, the release user-surface
features, declared Python test dependencies, the retained original Parquet and
new unique local-only output paths. Build with the recorded source and feature
commands, verify the resulting identities, and replay the included guarded
drivers. The lifecycle comparison additionally needs the named baseline source
and executable; its source is retained in the timestamp evidence archive. Do
not replay a historical driver against its completed output directory or treat
later source changes as the original timed artifact.

## Hosted integration

[PR #1544](https://github.com/depsilon/shardloom/pull/1544) merged at
`249657ede5a3dafc6f59f9922e97457edd12a83f` after all 44 checks passed on
`be564ab81bacffb33fa42367874522a5dc55362a`. The reviewed and merged trees are
both `bcb8be27fae2cf314d18fb52724342228ec658b2`. All 2,202 frozen runtime assets
remain unchanged. Codex review completed on that head; no review threads or
submitted change requests remain unresolved.

Two CodeQL logging alerts were statically triaged as false positives in fixed
test fixtures and dismissed. The
[source-linked triage](https://github.com/depsilon/shardloom/pull/1544#issuecomment-6118178314)
preserves the exact reported source and the iterator-name heuristic responsible.
The rule remains enabled; no validation coverage was removed.

Ubuntu Python 3.10/3.13 and macOS Python 3.13 each pass the same 76 individually
recounted native integration methods with no failures, errors or skips. All 12
small proof files match their recorded hashes. Actual isolated native loads pass;
the separate static wheel-integrity field `load_verified=false` is preserved
and is not relabeled as the dynamic load result. These are hosted source-build
wheels, not republished v0.5.1 artifacts.

The exact merge deploys successfully to production. The actual guide, acceptance
link and search destination pass at 1280 pixels without horizontal overflow or
console warnings/errors. Both served guide pages preserve the merged HTML plus
one recorded 1,305-byte Cloudflare challenge/analytics insertion each; raw HTML
is not byte-identical. Preview pages were byte-identical, and 1280/390-pixel
layout checks pass. The mobile capture verifies layout width, while the desktop
capture displays the complete new paragraph.

The [hosted receipt](evidence/native-persisted-hosted-2026-10-12.json) and
[portable packet](evidence/native-persisted-hosted-2026-10-12.tar.xz) retain checks,
review/merge records, wheel evidence, source identities and production captures.
The archive is 323,696 bytes, SHA-256
`e06ee4b632e45bbc4b4ce2128987b04d3c0bdb86a65f7562c217acf26ce952a8`.
All 29 archive members, including its index, were reopened and verified against
the original and portable hashes. Local path substitutions do not change the
original evidence identities. Main-branch CI was not recounted; the 44-check
claim belongs to the reviewed PR head and its identical merged tree.

This completes the finite persisted-output unit. Cross-owner encoded transfer,
independent producer feasibility and the wider optimization/breadth queue remain
under their existing owners. Original timings stay bound to their original builds.
