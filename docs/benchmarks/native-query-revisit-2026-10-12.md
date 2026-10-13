# Native query optimization revisit — October 12, 2026

Status: all four admitted experiments are decided. Three changes pass their
individual retention gates; the empty-reservation candidate is dropped.
Cumulative native, Full43, installed-package and portable-evidence checks pass.
Hosted integration remains pending. This report accompanies the
[encoded interchange acceptance](native-encoded-interchange-2026-10-12.md)
in one combined implementation unit. Version 0.5.1 remains unchanged.

## Selection and decision contract

The historical review covers the 29 numbered September 26 intake experiments,
the September 19 ship/drop record, September 30 post-0.3.3 intake, October 6
native hardware acceptance, October 7 decimal and membership decisions, and
the September 6 continuation ledger with its four numeric, state, scheduling
and text-consumer design records. Two literal inventories contain 42 and 36
records. Repeated reports and superseded decisions remain distinct; these are
78 reviewed records, not 78 independent implementations or an exhaustive
inventory of every possible optimization.

A candidate was reopened only when a currently reachable execution path and
a changed mechanism, resource owner, workload or retention policy justified a
new experiment. Existing retained implementations received no duplicate credit.
Historical measurements and rejection gates remain unchanged. The current route
inventory independently reopened 129 saved Full43 outputs; overlapping internal
spans and unpaired historical times were not treated as isolated CPU costs or
causal performance evidence.

The finite queue was fixed before measurement: prepared sort provider drivers,
prepared sort scan granularity, empty reservation transitions, then adaptive
exact decimal accumulation. Each retained change became the next baseline;
the dropped reservation patch was removed before decimal work. The campaign
contains 880 measured processes, forming 440 per-case pairs. Every sample is
retained. No warm-up removal, replacement samples or post-measurement threshold
changes are used.

Each primary metric must improve by a positive amount, have a positive median
paired gain, and improve in at least nine of ten paired rounds. There is no
minimum percentage gain. This is the declared repeatability rule, not a general
statistical-confidence claim. Complete results, schemas, admitted resources and
no-fallback evidence must pass independently of timing.

## Decisions

The primary metrics below have different scopes; their percentages must not be
combined. C2's baseline already contains C1. C4's baseline also contains the
separately verified decimal provider-selection correction described below.

| Candidate | Primary metric | Baseline | Candidate | Change | Positive rounds | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| C1: prepared sort provider drivers | Q24, max parallelism 12; complete native process | 15.245003 s | 3.573122 s | 76.56% lower | 10/10 | Retain; local cumulative acceptance passes |
| C2: prepared sort scan granularity | Q24, max parallelism 12; complete native process | 3.571531 s | 1.464921 s | 58.98% lower | 10/10 | Retain; local cumulative acceptance passes |
| C3: empty reservation transitions | Sum of 20 target process durations per round | 10.815818 s | 10.659169 s | 1.45% lower | 6/10 | Drop: repeatability gate fails |
| C4: adaptive exact decimal accumulation | Sum of five complete public calls per round | 137.201938 ms | 136.036749 ms | 0.85% lower | 9/10 | Retain; local cumulative acceptance passes |

The C3 result illustrates the distinction between an incremental gain and a
repeatable gain: all 28 individual time/RSS limits pass, but the primary wins
only six of ten rounds. Its source and observations remain in the evidence;
its implementation is absent from the final candidate.

## Prepared sort: useful parallelism and fewer input splits

C1 reuses `with_admitted_native_execution_temporary_drivers` under the current
execution grant. Prepared sort owns no separate compute pool. Existing
persistent drivers are counted, temporary drivers finish before return, and
the report distinguishes admitted provider drivers from measured utilization
or I/O threads. Focused tests verify actual background progress while the
caller waits, changing per-operation grants, native result ownership,
cancellation, consumer failure and cleanup.

C2 separates input scan scheduling from output delivery. The Vortex provider's
natural layout splits are admitted when the current grant has at least four
times the estimated scalar scratch headroom. Small grants retain 2,048-row
input splits. Exact scratch reservations remain authoritative. Output batches
retain their independent 2,048-row bound, and the selected policy is reported.
Tests cover empty input, split boundaries, 150,001-row layouts, changing grants,
complete values and failed/cancelled consumption.

Both changes reuse existing Vortex-native runtime and scan providers. Neither
adds an execution engine, a query-answer cache or a second worker pool.

Each comparison has seven cases and 80 process samples. Q24 with a declared
maximum parallelism of 12 has ten alternating pairs. Its single-lane control
and Q25/Q26/Q27/Q1/Q14 at the same maximum of 12 have five pairs each.
The host exposes ten logical CPUs;
the prepared sort uses nine background provider drivers plus its caller under
the declared maximum of 12. Six control medians may rise by at most the greater
of 5% or 5 ms. Every case's peak-RSS median may rise by at most the greater of
25% or 256 MiB. All controls pass. Times below include native process creation,
complete public CLI output and process exit; they exclude parent validation,
hashing and log compression.

| Case | C1 baseline / candidate, s | C2 baseline / candidate, s |
| --- | ---: | ---: |
| Q24, max parallelism 12 | 15.245003 / 3.573122 | 3.571531 / 1.464921 |
| Q24, max parallelism 1 | 16.050619 / 15.998274 | 16.120149 / 5.557247 |
| Q25, max parallelism 12 | 1.237856 / 0.833170 | 0.831904 / 0.394182 |
| Q26, max parallelism 12 | 6.916227 / 6.579157 | 6.553419 / 1.945418 |
| Q27, max parallelism 12 | 7.425912 / 6.955943 | 6.795395 / 2.121486 |
| Q1, max parallelism 12 | 0.046271 / 0.046226 | 0.045715 / 0.046235 |
| Q14, max parallelism 12 | 2.023476 / 2.052316 | 2.035186 / 2.036217 |

The tradeoff is visible in Q24. C1 raises median child CPU time from 16.088642
to 27.392394 seconds and peak RSS from 1,037,279,232 to 1,243,275,264 bytes
(19.86%). C2 then reduces CPU time from its own 27.639648-second baseline to
8.110844 seconds, while peak RSS rises from 1,392,205,824 to 1,472,872,448 bytes
(5.79%). Parallelism and reduced scheduling work have separate measured effects.
The sequential cohorts do not supply a direct combined speedup ratio.

These CLI cases use the same retained 99,997,497-row, 15,713,610,513-byte Vortex
file, SHA-256 `004f9639fa0aa527b70f326c366c37b1173dceeacb92b50b2d8fd02957155c18`,
and a declared 24-GiB native memory budget. The grant is not a whole-process RSS
bound. Complete outputs match the frozen native regression reference, with
finite floats compared at `1e-12`; this reference is not an independent oracle.

## Empty reservation transitions: dropped

The historical implementation reduced its target sum by about 0.96% but missed
the old 2% threshold. The revisit checked the zero-byte accounting idea against
the current allocation locks and allocation-epoch peaks, preserving positive
admission, poisoned-lock diagnostics, identities and destruction order.

The new comparison contains 20 target and eight control cases, ten alternating
pairs per case and 560 processes. CLI cases perform one complete operation;
native Python cases perform two complete operations on one owner with successive
2-GiB/four-lane and 1-GiB/two-lane grants. The primary metric sums target process
durations within each paired round. Per-case limits are the greater of 5% or
5 ms for time and 10% or 16 MiB for peak RSS. All 840 complete outputs and
8,400 rows pass. The positive median paired gain is 9.598480 ms, but only six
rounds improve; the unchanged nine-of-ten rule rejects the candidate.

## Exact decimal accumulation: retained increment

Before measuring C4, the public single-file native Python path exposed a
correctness defect: inferred primitive aggregate, sort and filtered-count
declarations could select a provider that could not represent a referenced
decimal field. The corrected metadata selection uses the existing typed native
SQL binder before execution. It preserves the complete SQL, including grouping,
ordering and HAVING, and registers the already admitted native sources.
Explicit primitive payloads retain their declared operation. Compatibility
preparation keeps its existing report path. Unreferenced decimal columns do
not force an otherwise primitive query through typed execution.

The same public regression fails on the old single-file aggregate and passes
on the corrected file and directory paths, including grouped SUM/AVG, sort,
filtered COUNT, repeated collection and final resource release. Both arithmetic
implementations then pass all eight independent exact-value/schema workloads
before timing. The correction is present in both measured artifacts; its
benefit is not attributed to the arithmetic optimization.

C4 starts exact totals in checked signed 128-bit state. Overflow, a wide merge
operand or a wide restored total uses 256-bit state and never narrows again.
AVG widens the numerator before scaling even when its unscaled sum is narrow.
The existing 32-byte spill representation and restored wide state are unchanged.
This is a ShardLoom reduction-kernel change using the existing Vortex decimal
value provider, not a new encoded representation or a smaller-state-size claim.

Seven additional Rust tests cover both signs, cancellation after promotion,
removal-triggered promotion, all merge-width combinations, wide AVG numerators,
generated live multisets and native spill round-trips. Eight public cases
include dense SUM/AVG, many groups, rolling SUM/mean, wide AVG, promoted groups
and integer SUM. There are ten alternating pairs per case, one complete public
call per fresh interpreter and 160 processes. All 2,624,040 result rows and
their schemas are independently checked.

The primary clock includes collection, complete wire-row and schema/field
export, and native result release. Interpreter creation, declaration and process
exit are a separate clock. Each case must pass both clocks' greater-of-5%-or-5-ms
limit and a greater-of-10%-or-16-MiB peak-RSS limit. All eight cases pass.

| Case | Role | Public-call median, baseline / candidate ms | Process median, baseline / candidate ms |
| --- | --- | ---: | ---: |
| Dense SUM | Target | 17.713521 / 17.584708 | 111.049395 / 110.616209 |
| Dense AVG | Target | 17.621854 / 17.530042 | 110.722167 / 110.582396 |
| Many groups | Target | 82.331271 / 82.006667 | 185.591916 / 185.405292 |
| Rolling SUM | Target | 9.065979 / 8.858980 | 106.497604 / 106.218042 |
| Rolling mean | Target | 10.206250 / 10.045500 | 107.542417 / 107.474167 |
| Wide AVG | Control | 48.712250 / 49.190875 | 141.713583 / 142.054563 |
| Promoted groups | Control | 137.478750 / 138.113937 | 240.171187 / 240.764979 |
| Integer SUM | Control | 7.620542 / 7.750604 | 100.697355 / 101.343666 |

All controls remain visible, including their small increases. The median
paired target-sum gain is 1.191812 ms and nine of ten rounds improve. The
0.85% gain qualifies under the predeclared rule without a materiality floor.
The eight cases use a 512-MiB/one-lane grant on macOS arm64; prehashed inputs,
OS cache and ordinary host activity are not controlled benchmark variables.

## Cumulative acceptance before PR review

The measured cumulative release CLI has SHA-256
`7586c7b7e6050157bd1c8baade328bb7acc5342d1d096f1e32e5627d365e1c1e`
and the native extension
`da3906b8b650db1aefcf1ec7502b5d966207f0efc046795dd45efaf8544da595`.
Their build manifest freezes 2,219 selected source assets at parent
`249657ede5a3dafc6f59f9922e97457edd12a83f`, including the uncommitted changes.
The final validation overlay changes one test assertion, while preserving every
production source and the measured CLI and extension bytes. That assertion now
checks the two prepared-sort reporting fields explicitly and compares every
remaining field and complete result with the nonprepared path.
After the full native suite passed, those unchanged assertions were extracted
into a private test helper to satisfy the function-length lint. The eight
source-handoff tests and complete native Clippy gate pass on that final test
source. The source overlays retain both versions and the original lint failure;
no production source or measured executable changed, and the full native suite
and Full43 were not repeated for this test-only extraction.

The default workspace passes 3,219 tests with zero failures or ignored tests.
The complete native-feature workspace passes 5,444 tests, with 27 existing
opt-in tests ignored. The original failed assertion and its eight passing
source-handoff regressions remain recorded alongside the full passing rerun.
The October 13 cumulative Full43 run passes all 129 calls and independently
reopens all 1,143 result rows, schemas, resource reports and no-fallback fields.
The native input, reference, sources and release artifacts retain their frozen
identities before and after execution. Its 306.250408 seconds of complete
operation time and 366.005994-second supervised run are regression observations,
not a paired performance claim.

The fresh installed-wheel proof passes all 80 tests without failures, errors or
skips. Final Python source validation passes 751 tests; the native-package and
license-tool suites each pass seven. Formatting, default and native workspace
Clippy, no-write, write-only, all-features and Rust 1.96 compatibility checks
pass. Dependency evidence is reused only after verifying unchanged Cargo,
provider and license inputs. These overlapping suites are separate proofs, not
additive capability counts. Hosted acceptance remains pending.

The [portable evidence index](evidence/native-encoded-query-2026-10-13.json)
binds the selected source, candidate protocols,
complete result/timing evidence, historical intake, explicit drop and original
failed checks. The 9,093,284-byte query archive contains 2,683 content-addressed
objects representing 3,894 original paths. All 3,520 raw sample references resolve,
including the earlier losslessly archived C1 results. Every packet member was
reopened and checked against its byte count and SHA-256. Eleven source manifests
and 20 differing source versions join the base source archive; no executable,
installed environment or large dataset is bundled.

Full43 is a cumulative correctness regression on the retained
input, not another paired performance comparison or fresh-ingest measurement.
All six broader areas, eight investigations and CG-1 through CG-23 retain their
existing owners. Neither this finite campaign nor its local results establish
broad engine superiority or authorize package publication.

## PR review correction

Review of PR #1545 found that the IPC schema validator and physical registry
admitted Null fields, but recursive receiving-schema accounting rejected them
before reaching the Null leaf. The public receiving regression reproduces that
failure on the submitted implementation. Physical transfer now accounts for
Null leaves recursively at both IPC admission and encoded-copy planning; the
execution payload/key policy remains unchanged. Empty chunked and sliced record
views use the already admitted Vortex `Canonical::empty` provider, with temporary
construction credit retained until every copied node receives its own owner.

Four regressions cover complete values and schemas, empty and nonempty records,
list/fixed-size-list/struct Null children, escaped bufferless child ownership,
resource cleanup, strict execution rejection and retained schema bounds.
An expanded IPC fixture initially attempted to serialize `vortex.slice`, which
the pinned producer does not support. Its failure is preserved; IPC coverage
uses the admitted Struct and Chunked providers, while the in-process transfer
test covers empty slices. The corrected `null_` selection passes 110 tests;
the final broader `null` selection passes 226. Its first accounting script
incorrectly expected the earlier filter's count; that failed receipt and the
unchanged passing test output remain preserved.

The complete native-feature workspace passes 5,448 tests, with 27 existing
opt-in tests ignored. A no-write feature check then found two write-only schema
helpers compiled outside their production consumer's feature boundary. Two
`cfg` attributes correct that visibility without changing any function body or
test. Both conditions are true in the completed native suite; its enabled
behavior and the exact source overlay are verified. The final default workspace
passes 3,219 tests, and formatting, default/native Clippy, no-write, write-only,
all-features and Rust 1.96 compatibility checks pass on the corrected sources.

Fresh release artifacts have CLI SHA-256
`580ddc68476d9e9bf0ac4336f5c57156baa64f5317c4bdf2958af1ef516fa794`
and extension SHA-256
`2b595bb8f6015925b47bd1d378683f49d9df2699865dabb6ad00a9a6500ebc2d`.
The installed-wheel proof passes all 80 tests. Source Python validation passes
751 tests, with seven native-package and seven license-tool tests also passing.
The fresh Full43 check passes all 129 calls and reopens all 1,143 complete rows,
schemas, resource and no-fallback fields. Sources, input, reference, harness and
artifacts remain identical before and after execution. This run is correctness
regression evidence, not a replacement for any paired performance measurement.

Admission initially refused the run because retained history exceeded the
existing 252-MiB preflight limit. Two completed cohorts' 916 closed log members
were preserved and verified in compact archives, recovering 10,182,656 accounted
bytes. The first compaction attempt removed nothing because its extra headroom
check failed. The completed compaction reopened all members and retained the
original refusal, both attempts, summaries and failed/incomplete cohorts. This
Full43 run used the original storage ceilings throughout.

The [review acceptance index](evidence/native-encoded-review-2026-10-13.json)
binds the [separate review packet](evidence/native-encoded-review-2026-10-13.tar.xz),
including all six changed source files, full source manifests, test/build/package
receipts, original failures, complete Full43 outputs and storage history. Every
member was reopened and checked by size and SHA-256. Hosted acceptance of the
review correction remains pending.

The original paired measurements and three portable archives remain unchanged.
They describe their original source and executable identities. The review
correction does not change the retained sort or decimal mechanisms; new release
artifacts require their own validation and are not substituted into old samples.

### Run-end admission correction

Subsequent review identified a second intake gap: the pinned RunEnd provider's
empty-ends validation returns without checking the declared logical length.
A malformed IPC record could therefore publish a nonempty array with no runs.
ShardLoom now checks that invariant after each provider construction and before
returning the decoded child to its parent. This uses the existing Vortex RunEnd
provider and ShardLoom IPC validation boundary, requires no coordinate decode,
and preserves valid zero-row arrays.

The public-intake regression fails on the previous implementation. Its accepted
version covers zero, one and seventeen declared rows at two record nesting
depths, deterministic rejection and complete receiving-credit release. All 15
IPC tests and 3,219 default workspace tests pass, along with formatting and
default/native Clippy. The first compile attempt used the wrong generated slot
trait name; the first native lint attempt then required moving the new check
into the existing validation module to keep the decoder within its function
size limit. Both failures and the successful final checks remain preserved in
the [run-end correction packet](evidence/native-encoded-ipc-admission-2026-10-13.json.xz).

The packet records the three-file source overlay on `9185bc58`, complete test
receipts and original failure evidence. The earlier release binaries, installed
wheel and Full43 observations above retain their original identities. This
malformed-input correction makes no new performance claim; final hosted
acceptance remains pending.

## Inspecting and reproducing the evidence

The original index binds three immutable artifacts by complete size and SHA-256:

- [Base source, transfer and producer evidence](evidence/native-encoded-interchange-source-2026-10-12.tar.xz).
- [Independently inspected complete public results](evidence/native-encoded-interchange-public-2026-10-12.json.xz).
- [Query protocols, all samples and final acceptance](evidence/native-query-revisit-2026-10-12.tar.xz).

Start with `index.json` in the query archive. Its `files` map translates original
path identifiers to `files/<sha256>` members; those identifiers are provenance,
not extraction destinations. `selected_manifest` locates the final source and
artifact identity. `source_versions` provides each differing source member.
For a chosen manifest, resolve every `source_sha256` entry from
`frozen-source/<repository-relative path>` in the base archive or from a matching
path/hash in `source_versions`. The final manifest's `built_source_sha256` records
the measured build; its two validation overlays record the later test-only edits.

For the review correction, first reconstruct that prior snapshot, then apply
the six `new-source/` entries from the separate review packet's `index.json`
`references` map. Check all 2,219 paths against its `source_manifests.new` map.
The preserved pre-feature-guard schema source identifies the full native suite's
exact snapshot; its two later attributes are the only overlay for that suite.

The packet retains the executed drivers, build commands/features, protocols,
workloads, complete references, per-process results, timing/PID companions and
validation receipts. Use each protocol's paths through the index, verify member
hashes, then rebind inputs and outputs to a fresh local-only workspace and a new
run identity. Build each declared source snapshot and record the resulting
artifact identity; original binary hashes identify the measured artifacts and
are not a promise of cross-platform reproducible binaries. Require the declared
resident ClickBench input hash for C1/C2/C3 and Full43. C4's bounded input generator
and independent expected values are included. Preserve the declared alternating
order, grants, clocks and gates; run through the recorded storage/concurrency
guards. Do not replay historical output paths or infer a fresh-ingest comparison.

## October 13 filtered-count follow-up

This is a distinct follow-up to the completed four-candidate October 12
campaign, not a fifth October 12 candidate. The frozen protocol retains one
local filtered-count optimization for the native public SQL user surface in
version 0.5.1. It uses the same pinned 99,997,497-row ClickBench Vortex input
(`persisted-native-343-a1.vortex`, 15,713,610,513 bytes, SHA-256
`004f9639fa0aa527b70f326c366c37b1173dceeacb92b50b2d8fd02957155c18`), a
24-GiB memory grant, and requested maximum parallelism 12; the 10-logical-CPU
host admitted 10 lanes. The paired comparison includes 70 fresh CLI processes.
All 70 complete results match the retained references and schemas.

Configurable sessions had no persistent provider workers, and prepared filtered
counts did not start temporary workers. Counts now reuse the admitted Vortex
driver scope for the current operation's CPU grant, join the workers on every
exit, and preserve the actual worker count through JSON and native-result output.
The later review correction consults file statistics before creating temporary
drivers, retaining zero-worker execution for fully metadata-pruned counts.
It preserves the same source-generation and cancellation boundary. The timed
cohort below predates that correction; its measurements remain unchanged.

The sole target is Q21 `COUNT(*) WHERE URL LIKE '%google%'` at requested
parallelism 12. Its baseline/candidate median complete-process wall time is
4.105538333533332/1.037534291506745 seconds, about 74.7% lower, with all ten
paired rounds improving. Median child user CPU rises from 3.9713925 to
6.4348815 seconds, and median peak RSS rises from 2,969,509,888 to
3,140,009,984 bytes. RSS remains within its frozen gate; CPU was recorded but
was not a retention gate in this experiment.
This is a wall-time improvement with higher CPU and a modestly higher RSS, not
a reduction in total work.

The five controls also pass their predeclared time and RSS limits. Their exact
baseline/candidate median complete-process wall times and positive paired
rounds are:

| Case | Pairs | Median seconds, baseline / candidate | Positive pairs |
| --- | ---: | ---: | ---: |
| Q21, requested parallelism 1 | 5 | 4.2691852919524536 / 4.244411916995887 | 4/5 |
| Q2, requested parallelism 12 | 5 | 0.21806212502997369 / 0.20017291698604822 | 5/5 |
| Q20, requested parallelism 12 | 5 | 0.2614852499682456 / 0.2673157909885049 | 2/5 |
| Q1, requested parallelism 12 | 5 | 0.04684637498576194 / 0.04373112501343712 | 3/5 |
| Q29, requested parallelism 12 | 5 | 5.647706374991685 / 5.670675708039198 | 1/5 |

The target timing gate, all control timing gates and every case's RSS gate pass;
the retained decision follows the frozen repeatability rule with no percentage
minimum. The default workspace passes 3,219 tests. The native crate passes
2,708 tests with 24 existing ignored tests. The cumulative Full43 correctness
run passes all 129 complete calls and 1,143 complete result rows. These are
correctness and local acceptance results for this scoped change.

PR #1545's prerequisite integration correction merged at `3089c8aa`. The
measured count source commit `67f3296d` was rebased to local commit `eb1b5a0f`
with the accepted five-Rust-file tree unchanged. The filtered-count optimization
has not merged; its hosted CI is pending. The user-surface version remains
0.5.1, with no release or publication. This finite local experiment does not
establish whole-engine superiority or production readiness.

The [filtered-count evidence packet](evidence/native-filtered-count-2026-10-13.json.xz)
contains the frozen protocol, source and baseline files, receipts and logs,
all paired process outputs, and the cumulative Full43 evidence. Its failures
remain explicit, including the retained pre-fix regression and the earlier
Q21 scan compiler failure; neither is rewritten as a passing result. Archive
SHA-256: `7d4c62262d6bee2575e8089fb20693d1641ce2ffd66da94b047cedadbb3b5638`.

## October 13 sparse Zstd filter follow-up

This is a second, independent October 13 follow-up; it does not change the
filtered-count measurements above. The retained candidate is a bounded
metadata-only `FilterReduce` in the existing pinned `vortex-zstd` 0.85.0
provider. For eligible UTF-8/binary arrays without null-bearing validity, it
keeps compressed frames containing selected rows, remaps the selection mask,
and uses the unchanged native decoder. Admission is limited to 2–64 frames,
at most 4,096 selected rows and one eighth of logical rows, with at least one
touched frame skipped and retained complete-frame rows no greater than the
original mask length. Other arrays keep the existing native path. No payload
copy, new unsafe/C implementation, or dependency was added.

The frozen 24-GiB protocol requested maximum parallelism 12; the 10-logical-CPU
host admitted 10 lanes. The target is Q23 at requested parallelism 12. Across
ten alternating process pairs, median complete-process wall time fell from
4.652212062530452 to 3.2868943749926984 seconds (29.35%); all ten pairs
improved. Combined CPU fell from 13.6428525 to 12.1475845 seconds (10.96%),
while median peak RSS rose 2.22%, from 4,487,667,712 to 4,587,077,632 bytes.
The target CPU, timing, and RSS gates pass. Five controls also pass their
predeclared timing and RSS limits:

| Case | Pairs | Median seconds, baseline / candidate | Positive pairs |
| --- | ---: | ---: | ---: |
| Q23, requested parallelism 1 | 5 | 9.821828458982054 / 7.941711333056446 | 5/5 |
| Q22, requested parallelism 12 | 5 | 1.5668610410066321 / 1.1950804580119438 | 5/5 |
| Q21, requested parallelism 12 | 5 | 1.0878443749970756 / 1.0273072500131093 | 4/5 |
| Q20, requested parallelism 12 | 5 | 0.25757562497165054 / 0.25551716599147767 | 3/5 |
| Q29, requested parallelism 12 | 5 | 5.683524666994344 / 5.687517957994714 | 1/5 |

Local correctness checks pass: 29 codec tests (including five new regressions
and 200 selection-shape combinations), 15 IPC tests, 3,219 default workspace
tests, formatting, and default/native Clippy. The cumulative Full43 review
also passes all 129 calls and 1,143 rows; its exact schemas match the preceding
count cohort, native certificates and grants are present, no fallback or
external engine is reported, all native child PIDs are absent, and all 2,221
source hashes plus the full retained input hash are unchanged. Full43 evidence
is in `zstd-filter-full43-review-382.json` with summary identity
`10f39b1343889aaeec42d4bb23bb3888f9075efb0bace2e8a69d3b8cbebf1ec9`. The
measured source commit is `b635a708`; the CLI artifact SHA-256 is
`026a6ad9d430f586346a6a6ffa04d0beb476f7c903f4637a9baa3e643a7dc490`.
Integration also aligns the Python license manifest's description with this
vendored change; that later notice-text correction does not change runtime source.

The provider patch and optimization remain unmerged; hosted CI is pending.
Version 0.5.1 remains unchanged, with no release or publication. This scoped
local result does not establish whole-engine superiority or production
readiness. The [sparse-filter evidence packet](evidence/native-zstd-filter-2026-10-13.json.xz)
preserves its frozen protocol, source identity, complete pairs, controls, and
Full43 review. Archive SHA-256:
`969684043dc4eaac4120a307ddad19492eea33b6e1153dca5217c3cb88e9d499`.
