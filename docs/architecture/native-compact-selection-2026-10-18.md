# Native relational selection experiment

Status: **dropped under the frozen throughput gate; production restored**.
The contiguous-range candidate reduced tracked memory but repeated its complete-
operation regression in both unfiltered and contiguous-band projection. The
required reversed-order confirmation therefore rejects this candidate. The
corrected measurement test and complete evidence remain; the runtime change does
not remain in the final tree.

This closes one experiment under RFC 0044 and
`PERF-10.compact_selections`, with PERF-03 ownership and PERF-01/12 attribution
and acceptance. The conditional substep stays open for a materially different
mechanism. The queue still has 58 substeps: 37 required and 21 conditional under
14 owners. No broader PERF owner or CG-1 through CG-23 is closed.

## Production opportunity and provider check

At production base `61afe0f5`, the native relational scan reserves
`array.len()` `usize` ordinals even without a residual predicate. Expression
filters also reserve a full-input ordinal vector. The existing `select_batch`
then rediscovers contiguous rows and uses a native slice; other selections use an
allocator-owned U64 take array. On this 64-bit target, an 8,192-row scan reserves
65,536 ordinal bytes. This is a real production owner, rather than a descriptive
selection object or a test-only expansion.

The candidate at `f0a67d00` kept increasing contiguous membership as a range
through scan/filter delivery and native count/projection. Its first gap admitted
one ordinal buffer no larger than the old full-input reservation, avoiding a
new growth-overlap denial. Explicit gathers retained order and duplicates such
as `17, 3, 17, 8`; global ordinals stayed checked `usize`/U64. It introduced
neither a bitmap nor narrower global indices.

Vortex-first classification: **use_vortex_native_provider**. Pinned Vortex 0.85
already supplies native slices, typed take indices and membership masks. The
experiment used existing slice/take providers behind `vortex-local-primitives`
and the native relational certificate. Its range was construction/admission
state, not a new array encoding. Generic mask consumers can create lazy indices
or slice Vec caches outside the existing allocator; admitting those providers
requires a separate resource audit.

The mechanism attempted to avoid ordinal construction and retain native views
through consumers. It added no pruning, decode shortcut, PulseWeave rule, worker
topology, adaptive policy or format change. Existing predicate evaluation and
resource admission remained in place. No dependency, public API, external
executor or Arrow execution substrate was added.

The package B source assessment found that the admitted Zstd compressor and
decoder already reuse mutable contexts across frames within an operation.
Dictionary preparation is also shared within that sequence. Reuse across slices
would extend the ownership boundary; its actual repeated cost is unmeasured.
Package B remains pending, with no claim that preparation is free.

## Frozen protocol and correction

The original protocol preceded the runtime candidate. It selected a useful
memory objective: both all-pass filter consumers must save at least 32 KiB and
10% of baseline peak shared-owner memory, and no cell may gain more than 32 KiB.
A median complete-lifecycle regression exceeding both 5% and 100 microseconds
requires one reversed-order paired cohort; the candidate is dropped if the same
regression repeats. Those thresholds were not changed.

Each screen runs fresh native-memory intake, binding, execution, complete output
validation and cleanup for 16,387 rows. Seven shapes—unfiltered, all, none,
contiguous band, sparse, dense and nullable membership—feed projection and count,
with three operations per cell. These diagnostic lifecycle clocks include
consumption and validation; they are not the engine-owned Full43 clock.

Review found that the original projection check verified every numeric value but
omitted the second projected Boolean field. Its 42 baseline and 42 candidate
observations remain **superseded preliminary evidence**. Before either corrected
build or measurement, a correction protocol froze the added field assertion and
kept all workloads, controls, resources and thresholds unchanged.

The corrected measurement source is byte-identical in both versions:
`ae65276f993f9dd8027f4b0c8454c919cd88a10ad7802db66f81afeacbf812a9`.
Baseline control `0b1d7f87` contains only that ignored test and its module
registration above production base `61afe0f5`. Candidate `e53e6068` changes
only the ignored test above the already tested runtime `f0a67d00`.
Release executables, source manifests and protocol identities were frozen before
execution, under serial workload/storage/process guards.

Both corrected first screens pass all value, count, certificate and cleanup
checks. The memory targets pass, including these complete-operation peaks:

| Operation | Baseline bytes | Rejected candidate bytes | Reduction |
| --- | ---: | ---: | ---: |
| Unfiltered projection | 326,797 | 261,261 | 20.05% |
| Unfiltered count | 364,813 | 299,277 | 17.96% |
| All-pass filter and projection | 359,565 | 278,287 | 22.60% |
| All-pass filter and count | 397,581 | 315,661 | 20.60% |

All fourteen cells reduce peak tracked memory. However, unfiltered projection
and contiguous-band projection both trigger the throughput gate. The one allowed
confirmation runs candidate first, then baseline, using the same frozen
executables. Both projection regressions repeat. All 168 corrected operations
preserve complete values and release every reservation, but the decision is
`drop_repeated_regression`. No additional repeats were used to seek a passing
result. The measurements establish this experiment's rejection; they do not
attribute the regression to a specific instruction or establish an engine-wide
slowdown. The memory reductions above are not delivered runtime improvements.

## Restoration and verification

Restoration commit `1acca085` removes the range implementation and its seven
candidate-specific tests, restoring production source exactly to `61afe0f5`.
The only Rust changes relative to that base are the corrected ignored cost test
and its two-line module registration. All 1,857 Rust/Cargo inputs match the
frozen corrected baseline executable, which is used for fresh native tests.
The portable source bridge verifies the restoration and full source identities.

| Final restored tree | Result |
| --- | --- |
| Required formatting and workspace lint | `cargo fmt --all -- --check` and strict workspace/all-target Clippy pass. |
| Required workspace tests | 3,235 pass, zero ignored. |
| Native provider tests | 2,969 pass; 29 opt-in screens remain ignored in the full suite. The corrected cost screen executes separately above. |
| Native feature lint | Strict CLI/Vortex all-target Clippy with `release-user-surfaces` passes. |
| Timing/scoring controls | Four tests pass for separate clocks, missing-clock refusal, one-call Full43 and rejection of partial-suite scoring. |

The rejected candidate also received broad correctness acceptance before the
corrected cost decision. These records are retained as evidence about that
candidate, not as performance acceptance of the restored runtime:

| Candidate boundary | Recorded result |
| --- | --- |
| Native semantics and ownership | 2,976 native tests pass, including sliced/nullable values, wide positions, duplicate/reordered gathers, composed filters/count/projection, allocation denial/retry, predicate failure, cancellation and last-owner release. |
| Native CLI and Python | 1,087 native CLI tests and 100 direct binding tests pass. Public context tests have 17 passes and one clean-installed-package opt-in skip; no new installed-wheel acceptance is claimed. |
| Public workflows | 4,827 cases across 36 families pass. Reopened evidence checks 6,330 no-fallback envelopes, including expected errors and unsupported diagnostics. |
| Format workflows | All 18 raw/prepared cohorts pass: 704 native workflows match 704 independent pandas-oracle workflows, including write/reopen results. This is the declared matrix, not a complete format Cartesian product. |
| Full43 | One execution of each query passes all 381 result rows and schemas. Candidate engine time totals 53.065528453 seconds. The immutable resident Vortex input is verified before and after; no ingestion is repeated. |

Full43 compares retained native regression values, rather than a newly generated
independent oracle. CG-5/6 remain open. Pandas is used only by the independent
format test oracle, never as runtime fallback. The first public evidence-checker
attempt incorrectly required success on expected-error envelopes; that verifier
was repaired and the same completed workflow evidence rechecked. Both attempts
and the original cost-check omission remain in the diagnostic history.

## Evidence and remaining work

The [portable evidence index](../benchmarks/evidence/native-compact-selection-2026-10-18.json)
identifies complete outputs, all cost samples, failed and superseded observations,
exact commands, source snapshots, restoration patches, frozen binary identities
and per-member verified archives. Its authoritative owner record is
`evidence/perf-10-acceptance.json`; the corrected decision is
`evidence/selection-complete-cost-confirmation-720.json`. The rejected source
is retained separately from the final source. Large binaries and generated
payloads stay in local storage with recorded hashes.

Do not rerun the unchanged range candidate. Further compact-selection work needs
a materially different mechanism and a new predeclared protocol that still
preserves membership/gather semantics, wide indices, resource ownership and
complete downstream costs. Required breadth and the other CPU investigations
retain their existing prerequisites. Local macOS acceptance establishes no
Linux/Windows runtime support or process-RSS ceiling. Vortex-native input/output,
no external-engine fallback, version 0.6.0 and the release hold remain unchanged.
