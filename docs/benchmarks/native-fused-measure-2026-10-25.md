<!-- SPDX-License-Identifier: Apache-2.0 -->

# Computed integer measures consumed directly by native grouping

Status: finite mechanism initially passed; the maintainer subsequently drops
the whole item-15 batch after its combined Full43 checkpoint. The implementation
is removed. The [drop and restoration record](native-workflow-drop-2026-10-25.md)
supersedes the retention decision; all measurements below remain historical evidence.
The baseline is merged #1605 (`167ac248ba662ae7967a7801995e9b160b17bae5`)
plus the separately retained counted-join change. The original
[protocol](../architecture/native-fused-measure-work-elimination-2026-10-25.md)
and [encoding correction](../architecture/native-fused-measure-encoding-correction-2026-10-25.md)
remain frozen premeasurement records. This report records their decision.

The candidate's admitted scan → predicate → integer projection → grouped reduction
computed each selected integer directly into the existing aggregate states.
It avoided the intermediate computed column. Text and binary group keys resolved
native deferred views only far enough to expose a retained dictionary. The
ordinary grouping owner still owns exact value identity, full-domain validation,
nulls, row order and backing allocation credits. No new executor, dependency,
thread pool or external fallback is introduced. Explicit spill and unproved
expression shapes retain their existing native execution.

## Complete comparison

All 72 release calls pass independent complete ordered values, exact schemas,
source closure, resource limits and cleanup. Both paired cohorts pass every
frozen benefit and control gate. Each cell below is the median of three samples;
negative change means less engine time.

| Case | Cohort | Baseline engine ms | Candidate engine ms | Change |
| --- | ---: | ---: | ---: | ---: |
| Dictionary dense | 1 | 27.058041 | 20.078000 | -25.797% |
| Dictionary dense | 2 | 26.762500 | 20.069082 | -25.010% |
| Dictionary skew | 1 | 26.524292 | 19.619333 | -26.033% |
| Dictionary skew | 2 | 26.661042 | 19.811584 | -25.691% |
| Dictionary sparse | 1 | 9.661542 | 9.157375 | -5.218% |
| Dictionary sparse | 2 | 9.663542 | 9.032333 | -6.532% |
| Plain high cardinality | 1 | 23.926000 | 22.112709 | -7.579% |
| Plain high cardinality | 2 | 24.041585 | 22.297541 | -7.254% |
| Tiny | 1 | 0.440749 | 0.426125 | -3.318% |
| Tiny | 2 | 0.434583 | 0.442708 | +1.870% |
| Ineligible I64 | 1 | 7.330417 | 7.422083 | +1.250% |
| Ineligible I64 | 2 | 7.413459 | 7.508874 | +1.287% |

The three dictionary targets exceed the 2% minimum improvement in both cohorts.
All six cases pass the separate 5% engine and preparation-plus-engine controls,
including the original 50-microsecond allowance for tiny. Every clock and sample
remains in the packet; there is no repeat or replacement cohort.

The six immutable native files contain 262,144 rows for each target, 65,536 for
plain/high-cardinality and ineligible I64, and 17 for tiny. Both roles use the
same sequential flat Vortex files, one CPU allocation, a 128-MiB grant, no spill,
the same corrected harness, Rust 1.99.0 and the release profile with
`release-user-surfaces`. Files are hashed before and after execution. Cache state
is uncontrolled, and prehashing is explicit; this is not a storage-cold claim.
The skew source's selected rows contain one non-null key plus null: its other
keys occur on rows excluded by the fixed predicate. This observation therefore
does not establish a benefit for every skew distribution.

The accepted candidate release executable has SHA-256
`27923ab8a0ffa1ca301c6cfb5e435c9908c536935ebaf1d8b4a44cb708535850`.
Its manifest freezes all 2,134 Rust/Cargo source files and the 24-file source
overlay. Baseline identities, both build profiles and source closures remain
in the [portable packet](evidence/native-fused-measure-2026-10-25/README.md).

## Observed work and correctness

All twelve separate debug observations pass. Each eligible candidate constructs
zero computed-expression output rows, while the baseline constructs one per
selected row. Dense and skew each consume 157,287 selected rows; sparse consumes
7,866. Each of these targets observes 128 actual dictionary-backed key domains
and 8,320 domain entries. The plain and ineligible controls observe no dictionary
domains; tiny observes its single eight-entry domain. The ineligible I64 case
does not activate the optimization. Exact delivered-row and aggregate-update
counts prevent reduced input coverage from masquerading as work avoidance.

The final native suite passes 3,324 tests with 35 declared ignores, including
the text/binary deferred-dictionary regression. Coverage includes integer-domain
admission, both operand orders, nulls, empty input, duplicate dictionary values,
group order, aliases, non-admission, error validation, denial, cancellation,
explicit spill and released credits. Strict native lint and the workspace
formatting check pass:

```sh
cargo fmt --all -- --check
cargo clippy --offline --locked --profile ci-native -p shardloom-vortex \
  --features release-user-surfaces --all-targets -- -D warnings
```

The complete native suite runs from the separately frozen `ci-native` test
executable with `--test-threads=1`. It is not a claim of final default-workspace,
public SQL/DataFrame or installed-package acceptance for the combined package.

## Preserved failed attempts and portable proof

The first attempt stops on one baseline observation: the shared test JSON parser
changes selected F64 literals by one step. The exact-library reproduction and
unchanged production source prove the subsequent correction is in test parsing.
No candidate call or release measurement runs in that attempt.

The second attempt completes all twelve observation calls with correct values
and cleanup, but its dictionary-activation gate fails. Deferred projection views
hide the dictionary from the ordinary key preparation path. Both provider probes,
the failed gate and all observations remain preserved. No second-attempt release
measurement runs. The third revision resolves those native views, with changes
limited to the fused consumer and its regression test. An initially unsuitable
tiny-dictionary test assertion and its exact source are also retained; tiny
dictionaries may legitimately simplify to canonical or constant arrays.

The portable verifier independently replays all complete results and timing
decisions without starting an engine or opening original local files. An isolated
copy passes with original-file reads denied, and all 32 corruption controls are
rejected. The 488-member packet preserves the failed attempts, exact native
input witnesses, original JSON, source overlays, build/test receipts, supervision
and every successful call. Its archive SHA-256 is
`019822c727f37fbeb5de2f765cf12a024988f4dd9c8cd3c6d5fa96766b5a30f7`.

After portable proof, [exact retirement](evidence/native-fused-measure-2026-10-25/cleanup.json)
removes nine comparison executables, three diagnostic executables and six finite
native fixtures. The packet retains their identities and regeneration sources.
Current accepted data/runtime and the compiler cache needed by active work remain.

This decides one mechanism. Multi-output execution, shared branches, guarded
decorrelation and the remaining item 15 packages are still open. Final combined
native/default/public/installed-package checks and one isolated standard Full43
remain required before PR integration. No publication or CG-1 through CG-23
closure follows from these finite measurements.
