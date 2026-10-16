<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite streaming unary-operation acceptance

Finite streaming producers now compose through the nine existing statically
bound unary families: DISTINCT, duplicate removal, duplicate masks, tail,
sampling, scalar rewrites, melt, explode and rolling. Complete-plan admission
walks their inputs and records source reuse; the existing native binder,
operation state, credited owners and result consumers execute the work.
Dynamic pivot keeps its separate execution-time schema path.

Status: complete local acceptance and independent archive inspection pass.
Hosted integration remains pending. The
[portable index](evidence/native-stream-unary-2026-10-15.json) records **14,733
verified members**. This source change follows the immutable 0.6.0 packages.
The [implementation contract](../architecture/native-stream-unary-2026-10-15.md)
keeps unary state spill, other unadmitted families and native transport outside
this finite unit.

The [final review supplement](evidence/native-stream-unary-final-review-2026-10-15.json.xz)
retains the separate packet audit, exact source transition, Python core check,
19 documentation/build checks and rendered-browser observations. It preserves
both index states without rewriting the runtime archive. The primary source
review and separate evidence parser do not claim an independent hosted review.

## Shared behavior and scope

The frozen prior runtime passes eighteen controls: nine explicit streaming
refusals before producer demand and nine complete resident results. The existing
unary executor already accepts native batches of unknown total cardinality.
The change connects that executor to finite source admission without adding an
algorithm, representation, kernel, dependency or external execution provider.
Pinned Vortex 0.85 remains the native array and I/O provider.

Existing option and type rules remain: first/last/remove-all duplicate
selection, seeded sampling with admitted weights and replacement modes,
cross-batch rolling and forward-fill state, and melt/explode expansion order.
Each stage consumes the actual preceding schema and rows. Static pivot is not
admitted through ordinary unary counting. Unsupported kinds, excessive depth
and unsupported descendants still reject before producer demand.

Independent producers retain separate completion state. Repeated references
consume each original producer once and use declared native replay. Output
stop conditions and downstream limits, including zero, still drain and validate
the entire source; late errors cannot become successful partial results.
Incremental output, bounded collection, all eight existing local writers and
one-execution fanout retain their established format/type/fidelity restrictions.
Native Vortex remains the highest-fidelity output. Every producer, writer and
owned cleanup must finish before publication; multi-file publication remains
non-atomic.

## Correctness, ownership and pressure

| Check | Result |
| --- | --- |
| Workspace formatting | Passed |
| Workspace, native-read and release-user-surfaces Clippy | Passed with warnings denied |
| Default workspace tests | 3,219 passed; none ignored |
| Native CLI/Vortex tests | 4,249 passed; 26 manual and three platform exclusions |
| New native unary tests | 8 passed |
| Direct native Python binding tests | 91 methods passed; no failures, errors or skips |
| New direct unary coverage | Four methods with 468 declared subcases passed |
| Log-compression harness tests | 5 passed; no skips |
| Corrected focused public run | 1,564 passed across seven families |
| Complete public workflows | 2,601 passed across 27 families |
| New unary families | 494 semantic/writer, 348 reference, 261 composition, 331 fault and 46 pressure cases passed |
| Full43 regression | 43/43 complete queries; 381 result rows; one call per query |

Complete-value tests cover SQL and DataFrame declarations, independent and
repeated producers, mixed resident/file sources, empty/all-null input,
admitted scalar/recursive types, narrow and renamed schemas, multiple batch
sizes, surrounding relational stages and dynamic pivot composition.
Independent expectations for 54 sampling and 120 rolling fixtures are declared
before candidate execution and checked through both public frontends.
Writer checks reopen complete values and schemas, including typed empty output.

Native release witnesses prove that prior producer owners are gone before the
next demand. Returned native arrays remain valid after producer, plan and client
teardown; their credited owners survive until the final reference is released.
Fault cases cover producer and schema failure, cancellation before and during
input, early consumer closure, incomplete publication and clean reuse. Existing
and foreign files survive.

The bounded-state fixtures use 8,193 rows, 2,048-byte text values, 251-row
producer batches and a 16-MiB grant. Their cumulative input exceeds that grant
while admitted retained state fits. Wide high-cardinality membership and unknown
sampling populations fail deterministically with and without replay-spill
permission. Separate 262,145-row duplicate-mask fixtures distinguish streaming
first-keep state from future-dependent population identities: the latter fail
at an 8-MiB grant and complete with 32 MiB. These checks measure credited native
allocations, not whole-process RSS.

Repeated-source tail composition separately proves source replay spill.
A 16-MiB resident control refuses; a 128-MiB control completes without spill.
The original 16-MiB grant with an explicit 128-MiB disk quota completes two
executions with native runs, merge passes and cleanup. Producer failure, corrupt
runs and exhausted quota fail both result and writer routes. Source/result
replay spill does not spill unary membership or population state.

Independent public-evidence inspection checks 2,497 complete-value files with
29,553 expected rows, 4,029 native envelopes and 45 pressure files containing
1,578,741 rows. The exact ordered 2,601-case list matches the frozen protocol.
All 6,830 compressed JSON files reopen byte-for-byte: 304,011,169 original bytes
are retained in 32,355,345 stored bytes. The raw and compressed identities,
complete source snapshots and no-fallback fields are verified. Local direct
tests load the source package with an explicit frozen native library; installed
wheel and other-platform acceptance require the later hosted lanes.

The Python core shard also passes with 615 discovered cases: its console records
524 tests run and ten skip records, including unavailable native setup. Both new
modules load through the package entry point, and the five compression tests
execute there. These skips are separate from the zero-skip 91-method native run.

## Preserved failures and source identity

The packet retains the initial test-only Clippy finding and its correction,
the first declaration helper's missing-dependency failure and every subsequent
acceptance attempt. No failed observation is replaced by a successful one.

The first direct binding run has 182 subcase failures because its fixture
expects six native batches for both row sequences and Arrow tables. An empty
Arrow table emits no record batch. Six matched controls on each of the unchanged
prior and current bindings confirm the same values, producer events and counts:
row sequences produce six batches; Arrow mixed/empty/all-null inputs produce
three/zero/two. The corrected assertion uses those explicit counts and also
checks total input rows. Complete expected values are unchanged, and all 91
methods then pass, including retained-array checks after client teardown.

Before the expanded public run, the previous 1,121-case suite already occupies
192,524,528 bytes under the 192-MiB log ceiling. The runner now offers opt-in
`--compress-logs`: it verifies each newly written log's complete gzip roundtrip
and original identity before removing only its redundant raw copy. Plain mode
remains the default. Five tests cover exact content, existing-file refusal,
corrupt or mismatched compression and concurrent raw replacement. No ceiling
is raised and no assertion or case is removed.

The first focused run passes 1,079 cases, then stops at a rolling fixture that
reuses its input column name as the rolling output alias. The public declaration
correctly refuses that alias before execution. The corrected fixture uses a
distinct output name and projects it back to the expected downstream name;
the expected rolling values and pivot total remain unchanged. The complete
1,564-case focused and 2,601-case broad reruns then pass.

The first public-evidence verifier mistakes the predeclared 39-entry values
list for a result-comparison object because of its filename suffix. Its retained
failure is corrected only for that exact file, whose complete entries are
compared with the original pre-execution declaration. All ordinary comparison
objects retain their strict actual/expected checks. This correction reruns
evidence inspection without executing the engine.

The original build/native gates bind 2,282 selected source files. The corrected
verification snapshot binds 2,283: three test/harness files differ and one
compression-test file is added. All Rust and public Python runtime source
identities and both native artifact hashes remain unchanged across those
corrections. The separate reference fixture is explicitly hashed outside the
selected source map. The later documentation-validator transition is recorded
separately from runtime acceptance.

Independent archive inspection reopens all 14,733 members, complete public
comparisons, compressed logs and pressure rows, retained failed/focused runs,
all 43 query results and their original schemas, timing CSV and native clocks.
It checks the original preimplementation contract and every selected source.
This is a read-only evidence audit; it does not rerun native execution.

## Single-pass query observation

One sequential pass executes all 43 queries against the resident
99,997,497-row, 15,713,610,513-byte native Vortex input. Complete values and schemas
match the retained native regression reference, with its declared `1e-12`
finite-float tolerance. This is not a fresh independent correctness oracle.
No new ingest is needed for the admission change. Source/input generations,
complete hashes and native artifacts are checked before and after the run,
and owned children and the workload lock are released.

The host is macOS 27.0 on arm64 with ten logical CPUs and 16 GiB physical RAM.
The request uses parallelism 12 and a 24-GiB query allocation; physical RAM,
policy allocation and observed RSS are distinct. Input prehashing affects the
OS cache. Cache state and ordinary desktop activity remain uncontrolled.
There is no artificial pacing, battery/thermal admission, automatic retry,
replacement sample or matched performance claim.

Engine execution totals **93.002589 seconds**, serialization **0.010054
seconds**, other process work **3.805085 seconds**, and process wall time
**96.817728 seconds**. Summed supervised calls take **99.136703 seconds**,
including **2.318974 seconds** of supervision. Process user/system CPU totals
231.446545/56.274458 seconds; complete-value validation totals 0.048672 seconds.
The guarded query harness takes 134.190230 seconds including surrounding
identity/evidence work. These clocks are separate from ingest, build, input
prehashing, final input hashing and archive verification.

Engine time includes query I/O, decoding, native operators, source checks and
certification. Other process work includes startup, parsing/preparation,
control/report work and exit. CPU overlaps wall time and includes parallel
workers. This single observation does not replace prior paired decisions or
attribute a timing difference to unary admission.
The [complete CSV](evidence/native-stream-unary-2026-10-15.query-timings.csv)
retains supervision, validation and every query below.
Its checked-in line endings are normalized to LF; all 44 CSV records match the
original CRLF file byte-for-byte apart from that normalization. The original
remains in the runtime archive, and the [formatting receipt](evidence/native-stream-unary-formatting-2026-10-15.json)
records both identities and the final complete-diff check.

| Query | Engine (s) | Serialize (s) | Other process (s) | Process (s) | CPU (s) | Peak RSS (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 0.000079 | 0.000115 | 0.067685 | 0.067878 | 0.055653 | 147.375 |
| Q2 | 0.224670 | 0.000009 | 0.062580 | 0.287259 | 0.794071 | 562.250 |
| Q3 | 1.223865 | 0.000107 | 0.064871 | 1.288842 | 1.644097 | 473.750 |
| Q4 | 0.546378 | 0.000032 | 0.066079 | 0.612488 | 0.852386 | 375.078 |
| Q5 | 1.884448 | 0.000026 | 0.068884 | 1.953357 | 2.157204 | 1934.141 |
| Q6 | 2.062984 | 0.000032 | 0.066920 | 2.129936 | 5.945754 | 1264.797 |
| Q7 | 0.000695 | 0.000033 | 0.058415 | 0.059144 | 0.056785 | 150.672 |
| Q8 | 0.212139 | 0.000026 | 0.069670 | 0.281835 | 0.737232 | 570.500 |
| Q9 | 3.089033 | 0.000055 | 0.083596 | 3.172684 | 10.892590 | 3599.828 |
| Q10 | 3.849437 | 0.000064 | 0.133860 | 3.983361 | 6.957528 | 2092.375 |
| Q11 | 2.076391 | 0.000405 | 0.079088 | 2.155884 | 3.865059 | 497.531 |
| Q12 | 2.731498 | 0.000041 | 0.073502 | 2.805041 | 4.426193 | 540.984 |
| Q13 | 1.137709 | 0.000052 | 0.088896 | 1.226658 | 6.380903 | 1504.172 |
| Q14 | 2.867222 | 0.000068 | 0.169345 | 3.036635 | 15.147959 | 2381.391 |
| Q15 | 1.944291 | 0.000054 | 0.084087 | 2.028432 | 12.580829 | 1952.516 |
| Q16 | 3.330716 | 0.000033 | 0.070613 | 3.401362 | 5.457559 | 2180.797 |
| Q17 | 3.360091 | 0.000056 | 0.108924 | 3.469070 | 22.602930 | 3053.766 |
| Q18 | 0.402623 | 0.000038 | 0.064076 | 0.466738 | 0.844478 | 485.562 |
| Q19 | 7.898652 | 0.001705 | 0.082643 | 7.983001 | 14.043991 | 3063.328 |
| Q20 | 0.361291 | 0.000063 | 0.105011 | 0.466365 | 0.801067 | 1926.531 |
| Q21 | 1.872214 | 0.000013 | 0.090345 | 1.962572 | 11.698833 | 2253.031 |
| Q22 | 2.092184 | 0.000061 | 0.084239 | 2.176483 | 11.973016 | 1980.375 |
| Q23 | 5.278689 | 0.003636 | 0.110880 | 5.393205 | 18.083331 | 2730.359 |
| Q24 | 1.874944 | 0.000292 | 0.096788 | 1.972023 | 11.122010 | 2413.375 |
| Q25 | 0.475978 | 0.000040 | 0.092826 | 0.568844 | 1.585358 | 1964.141 |
| Q26 | 2.975669 | 0.000043 | 0.072023 | 3.047735 | 3.918627 | 610.406 |
| Q27 | 2.842310 | 0.000034 | 0.083613 | 2.925957 | 3.864038 | 2361.109 |
| Q28 | 3.281582 | 0.000374 | 0.093979 | 3.375935 | 4.215113 | 2673.031 |
| Q29 | 10.719909 | 0.000065 | 0.073332 | 10.793306 | 18.791814 | 1468.688 |
| Q30 | 0.372504 | 0.000091 | 0.069687 | 0.442283 | 0.690797 | 346.984 |
| Q31 | 2.097752 | 0.000043 | 0.129544 | 2.227339 | 5.540525 | 1885.031 |
| Q32 | 2.278552 | 0.000044 | 0.148888 | 2.427484 | 5.592740 | 3598.609 |
| Q33 | 3.284053 | 0.000064 | 0.085510 | 3.369627 | 10.690500 | 2858.797 |
| Q34 | 4.808611 | 0.000064 | 0.080140 | 4.888815 | 25.924115 | 3661.609 |
| Q35 | 4.933435 | 0.001778 | 0.090251 | 5.025464 | 25.303684 | 4689.000 |
| Q36 | 0.566240 | 0.000052 | 0.087712 | 0.654004 | 3.437703 | 859.125 |
| Q37 | 0.523110 | 0.000053 | 0.084793 | 0.607956 | 1.158213 | 1924.516 |
| Q38 | 0.573253 | 0.000061 | 0.097806 | 0.671120 | 1.402860 | 2956.625 |
| Q39 | 0.334960 | 0.000041 | 0.093651 | 0.428652 | 0.972702 | 1882.484 |
| Q40 | 1.174922 | 0.000077 | 0.098790 | 1.273789 | 1.997055 | 2499.906 |
| Q41 | 0.794366 | 0.000030 | 0.093544 | 0.887940 | 1.839498 | 2196.203 |
| Q42 | 0.282301 | 0.000053 | 0.083393 | 0.365747 | 0.769569 | 639.875 |
| Q43 | 0.360840 | 0.000030 | 0.094607 | 0.455478 | 0.904634 | 1432.359 |

## Portable provenance and reproduction

The [archive](evidence/native-stream-unary-2026-10-15.tar.xz) and
[compressed manifest](evidence/native-stream-unary-2026-10-15.manifest.json.xz)
retain 14,733 members totaling 90,887,120 raw bytes. They include seventeen
selected changed source files plus the explicit reference fixture, original and
corrected source manifests, build receipts, all required gate logs, predeclared
references, complete public/query observations, failure history and helper
dependencies. Every member is reopened and hash-checked. Bulk native binaries,
resident input and output payloads have explicit hashed exclusions.

The source base is `e859b3a8b9d323587cee9641f5ebacd21e7e799c`. The CLI SHA-256 is
`bc8e691f00dd053325a6b76f4144fb869c7640f9648733e2366c64377ab53766`;
the binding SHA-256 is
`4ba1f3d53e7d50bc103c0e8c00194d15a099906c00c1c227de0d56e0a2a69912`.
Reproduction uses the recorded release features, explicit native library,
all 27 public families with lossless log compression, and the complete retained
query reference with `--repeats 1`. Preserve the local-storage, workload,
deadline and cleanup guards. Historical absolute paths identify the original
observations; another host must supply or regenerate identical bulk fixtures
and use new run paths.

Hosted integration remains pending. This finite unit makes no package release,
speedup, zero-copy, process-wide memory ceiling or unary state-spill claim.
The six-area/eight-investigation completion contract, remaining allocation,
recovery, platform and stateful breadth obligations retain their owners.
Native transport follows the optimization and breadth work.
