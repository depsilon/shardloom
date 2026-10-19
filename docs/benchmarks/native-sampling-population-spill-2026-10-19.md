<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native sampling population pressure acceptance

Local source acceptance passed on October 19, 2026. The shared relational route
now completes six sampling population variants under explicit native spill:
fractional sampling without replacement when the input row upper bound is
unknown, and fixed-count or fractional sampling with replacement for known or
unknown bounds, each weighted or unweighted. These are unreleased development
capabilities on the `vortex-write` Unix boundary. Hosted, installed-package and
other-platform acceptance remain separate; published v0.6.0 is unchanged.

The [implementation contract](../architecture/native-sampling-population-spill-2026-10-19.md),
[evidence index](evidence/native-sampling-population-spill-2026-10-19.json) and
[member manifest](evidence/native-sampling-population-spill-2026-10-19.manifest.json.xz)
bind the exact source, compiled artifacts, complete values and failed observations.
Every archived member is reopened against its size and SHA-256. Bulk output and
executable files remain local, with identities, complete result values and
regeneration sources retained in the packet.

Only this finite scope within `PERF-06.membership`, `PERF-06.recovery` and
`PERF-06.strategy` is accepted. Fixed-count and known-bound fractional sampling
without replacement retain their bounded resident top-k path. Subquery indexes,
legacy standalone `PreparedVortexUnary`, the older row-export sampler and other
state families retain their existing boundaries. All 14 broader owners and 58
substeps (37 required and 21 conditional) remain open. CG-1 through CG-23 retain
their independent evidence requirements.

## Execution and ownership

The implementation reuses pinned Vortex 0.85 providers and the existing native
query-run store and ordering machinery. It adds no dependency or external
execution provider. One producer pass supplies bounded private typed records;
the pending block contains at most 256 rows or the smaller requested batch
bound, with a 256-KiB shaping threshold. The threshold is not a record-size cap.
Records, metadata, ordering, merges, lookup blocks and output share the admitted
memory owner. Small populations can remain resident; actual run counters prove
temporary-file activity separately from strategy selection.

Predicates precede passing ordinals and weight observation. The existing seed,
score, target and draw functions remain authoritative. Fractional targets keep
their per-row carry and nonempty-minimum behavior. Without replacement, score
descending and ordinal ascending determine the selected population; another
native ordering stage restores source order. The private score representation
preserves negative infinity and normalizes signed-zero score ties without
changing the weighted formula.

Replacement retains population ordinal order. Unweighted draws use the existing
deterministic modulo index. Weighted draws preserve sequential floating addition
and select the first cumulative sum greater than or equal to the threshold,
including equal sums caused by rounding. Output retains draw order and repeated
rows. A single credited positional cache validates the run generation on every
access, including cache hits, and selected values are detached before borrowed
blocks are released.

Private movement preserves scalar floating payload bits; selected public output
retains its normal value checks. Nested payloads keep their existing eager
validation. Original payload-observation boundaries are why bounded top-k
nonreplacement branches are not replaced with full-population retention.
The legacy unary state-item count describes the logical retained population,
including native files, rather than resident heap entries. Sampling-stage,
population, output, draw and lookup counters remain separate from disk/merge
and reservation evidence.

## Pressure, output and failure boundaries

The original population fixture has 8,193 rows with 2,048-byte strings and nested
payloads. All six variants complete with exact independent rank/draw results at
the unchanged 16-MiB grant when spill is enabled. No-spill controls still deny
resident growth, and fixed-count nonreplacement retains its successful bounded
control. The store uses a 128-MiB quota and a 1-MiB buffer setting.

| Boundary | Grant |
| --- | --- |
| Original native and public sampling pressure | 16 MiB |
| Direct-binding producer and publication | 32 MiB |
| Public wide sampling through all eight destinations | 32 MiB |
| Independent direct native-file readback | 128 MiB |

Writer/readback grants do not replace the original pressure grant. Python
`batch_rows` limits delivered slices, not native file-reader or Arrow admission.
Managed reservations do not establish a whole-process RSS ceiling.

The native tests cover empty and singleton populations, independent seeds and
chunk sizes, known/unknown bounds, duplicates, predicates and hidden weights,
fractional carry edges, large draw counts, tiny weights, rounded cumulative ties,
invalid weights and total overflow. Typed, dictionary, nested, decimal, binary
and temporal payloads, signed zero, large records and selected/unselected
nonfinite values retain their declared behavior.

Generation tests corrupt, truncate and replace runs at open, cached access and
both merge inputs. Other controls cover quotas, grant denial, cancellation during
merge, late producer/schema/consumer faults and native-file generation changes.
Real child-process abandonment checks preserve live, foreign and unknown owners,
remove only owned abandoned state and complete a fresh execution. This is cleanup
and restart, not interrupted-query resume.

All eight local writers and fanout pass complete-value readback. Public failure
coverage contains 54 incomplete-execution faults and 12 existing-destination
admission refusals, for 66 cases total. Existing targets are rejected before
producer demand; those twelve cases are not in-flight publication failures or
proof of generation-conditional replacement. Every case checks a fresh successful
execution afterward. Early consumer closure remains provisional, and no final
success report is permitted before input, output and cleanup completion.
Repeated-source replay remains a different owner from sampling population state.

## Verification

| Check | Result |
| --- | --- |
| Required new native sampling selectors | 18 pass, including the process-death child entry |
| Retained unary membership regression selectors | 17 pass |
| Native Vortex library, release features | 3,010 pass; 29 opt-in screens ignored |
| Default workspace tests | 3,235 pass; zero ignored |
| Selected native CLI suites | 1,087 pass; zero ignored |
| Formatting and strict workspace/native-feature lint | Pass |
| Required direct native binding tests | 104 pass; zero skipped |
| New direct-binding sampling matrix | 48 subcases pass within one test |
| Public context tests | 17 pass; one installed-package check skipped |
| Public reference tests and independent population-fixture check | Pass |
| Public input/operator union | 5,492 cases across 38 families pass |
| Sampling complete-value archives | 406 archives; all 11,518 rows independently reopened |
| Full43 regression | 43 queries once each; all 381 result rows and schemas match |
| Modular workflows | 18 cohorts; 704 native and 704 fresh independent pandas oracle workflows match |

Suites overlap; their counts are not additive. The public union adds 358 sampling
cases and replaces four old sample-denial controls with twelve enabled/disabled
controls: 5,126 + 358 + 8 = 5,492. The focused public preflight passes 417 cases:
358 sampling and 59 unary-pressure cases. Independent full-run verification checks
all 14,921 compressed JSON members, 7,525 no-fallback envelopes and frozen source
snapshots. It also reopens the 274 prior unary membership and 74 distinct-set
value archives as regressions.

The modular matrix covers collection from eight input formats and eight output
formats from CSV, in raw and prepared states, over all 22 declared workflows.
It is not a complete input/output Cartesian product. Pandas runs solely as an
independent comparison oracle.

Full43 engine time is **50.415902622 seconds**, summed from
`engine_execution_seconds` for one execution of each query. This is one regression
observation against a retained native reference, not a fresh independent Full43
oracle or evidence of speed improvement or superiority. The unchanged input is
`fresh-roadmap-663-a1.vortex`, 15,713,610,513 bytes, SHA-256
`06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`.
It was not reingested. Full pre/post hashing and generation checks pass outside
query timing. Cache state is uncontrolled and preflight hashing may warm it.
All per-query engine clocks and supervision diagnostics remain in the packet.

The context skip is
`test_process_result_chaining_preserves_empty_nested_nullable_and_exact_types`.
It requires a separately selected clean installed package, which was not enabled.
Source acceptance does not replace that installed-package proof.

## Source identity and preserved corrections

Acceptance and both compiled artifacts use the same frozen source,
`f363f2890436b532469d9682f69457cee3ef7b2f`, following base
`a2728ac5e70773911e53eecdf6d110e23d905c5b`. The CLI SHA-256 is
`b1b5009ebcaeb92992de0bbc4cdc12c36d7e47d170f9532d509d185ea4fb06d9`;
the native binding SHA-256 is
`0e72ebe3ad5ddeb2cd9bde90caf29eccbe8d6e112aaab52fa1c9568119e9ba36`.
All 2,195 frozen source entries, the plan and its four verification helpers are
checked through acceptance. The later documentation closeout changes only
`scripts/check_user_surface_reference.py` among those entries so it can validate
the newly accepted scoped claim. Its explicit bridge records old/new hashes;
all 2,194 other entries, including runtime, Python and harness sources, remain
byte-identical.

The packet preserves the initial strict-lint failure and its formatting/style
corrections. It also preserves a focused recovery-test failure caused by comparing
the macOS `/var` and `/private/var` spellings of the same temporary path. The test
now canonicalizes that comparison. The corrected focused run passes all 24
selected tests, followed by the complete frozen native suite.

Before freezing acceptance, source review corrected a public failure expectation:
an existing destination must fail admission before the producer starts. The
twelve corresponding tests now require that exact refusal and unchanged target
bytes; the other 54 cases exercise incomplete execution. No runtime behavior or
grant was changed to satisfy this correction. The frozen build, binding, public,
Full43, modular and final acceptance gates all pass without an intervening source
or verifier repair.

Reproduction uses the archived source/build manifests, frozen plan, guarded
commands, independent oracles and verifiers in a fresh local-only directory.
Required broad gates are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, and
`cargo test --workspace --all-targets`. Native checks use the locked, offline
`release-user-surfaces` build; exact selectors and Python commands are retained.
Historical output paths are immutable and must be rebound for another run.
Storage and concurrency limits were not raised. No broader PERF owner, release,
publication or competitive gate is completed by this finite acceptance.
