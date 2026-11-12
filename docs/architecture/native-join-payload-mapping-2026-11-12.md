<!-- SPDX-License-Identifier: Apache-2.0 -->

# Join payload mappings through bounded ranking

Status: first fixed candidate rejected, November 12, 2026. All 940 requests
pass, but complete engine time increases. Native implementation, candidate
tests and ignored-test registration are restored to the accepted base. An
independently verified CI cleanup correction remains. Portable packaging and
all 30 corruption controls pass. PR #1681 merged as
`3a0608b12e40435fab1adc4afefbaa4173932737`; all 58 PR and 55 exact-main checks
passed with review clear. Verified retirement and post-retirement replay are
complete; the compatible-window runtime remains accepted.

This records investigation A in the
[shared-pipeline program](native-shared-pipeline-investigations-2026-11-10.md),
inside item 20 and RFC 0044. Investigation B follows this closeout under the
[phase plan](phased-execution-plan.md#planned). Item 19, item 20, the broader
RFC and CG-1 through CG-23 remain open.

## Frozen cohort and decision

The accepted base is `e10fc31e4ada819038711dd3a4658c3ad10a91f0`, the merge of
rolling DISTINCT PR #1680. Its 58 PR checks, 55 exact-main checks, clear review,
verified retirement and post-retirement replay passed before this experiment.
The retained compatible-window runtime remains accepted.

The original 10,000 declarations contain 580 two-inner-join aggregate-barrier
controls, but no two-join ranked-output target. Those requests stay unchanged.
The separately versioned 360 compositions cross 24 scenarios with five public
interfaces and Vortex, Parquet and Arrow IPC inputs. They cover both join orders,
all INNER/LEFT pairs, wide text, nullable lists/structs, narrow controls,
K=1/31/1024, direct aliases, reordering, duplicate projections, empty/tiny
populations and bounded full-output controls. Four existing input profiles use
a fixed Latin rotation; shuffled holdout row IDs are identities, not physical
offsets. Independent references read actual IPC input generations and check
every retained value and schema.

An exact audit found no overlap with 1,648 compatible saved requests. Baseline
and candidate therefore each execute all 940 requests once. Requests, references,
clients, input generations, engine clock, deadlines, storage limits, result
checks and cleanup stay fixed. Other task work and agents remain idle during
each pass. No warmup, repeats, profiling, cache manipulation or post-score tuning
is performed.

| Family | Cases | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Original aggregate-barrier controls | 580 | 142.822532279 | 144.569862588 |
| Narrow join compositions | 45 | 45.637444496 | 45.821056331 |
| Rich join compositions | 120 | 99.471108086 | 115.983312083 |
| Wide join compositions | 195 | 365.126275040 | 433.553667217 |
| Complete cohort | 940 | 653.057359901 | 739.927898219 |

Independent saved-result replay verifies all 940 baseline and 940 candidate
results, complete schemas/values, requests, certificates and engine clocks.
The candidate total is **86.870538318 engine seconds higher (13.302129897%)**.
Every family remains in the denominator. The frozen strict gain/loss rule drops
the candidate; reduced payload-buffer construction cannot override it. These
single-pass observations do not establish statistical confidence or a causal
regression in unchanged controls.

The [portable packet](../benchmarks/evidence/native-join-payload-mapping-2026-11-12/README.md)
preserves protocols, complete results, references, exact source snapshots,
diagnostics and original failures. Replay executes no native query. This is
neither a new complete 10k nor a Full43 result. The original failed 10k and its
96 failures remain unchanged; results across different cohorts are not added.

The packet contains 6,002 logical members in ten identity-checked archives.
Complete source-bound replay and relocated source-only replay pass. All 30
corruption controls reject altered source, CI, installed-package, diagnostic,
result, reference, clock or decision evidence. The 17 diagnostic corruption
checks are separate. Portable verification uses standard-library Python and
executes no native query, compiler or external reference engine.

## Rejected representation and proof

The candidate reused ordinary join, gather, selection and ranked-output owners.
It admitted two resident, unconditioned INNER/LEFT equality joins through direct
projections into StreamingPrefix with prefix 1..16384. Stable output names and
backward alias mapping excluded later join/ranking keys. Computed projections,
filters, aggregates, windows, observations and other declared barriers stopped
admission. Each edge separately proved uniqueness from a complete ordinary
insert-built RowIndex with no duplicate links. Equal hashes still required
exact equality. Duplicate edges, ASOF/conditions and spill kept their established
behavior.

Pinned Vortex 0.93.0 `DictArray::try_new` retained native values and selection
codes for UTF8, Binary, List and Struct payloads. Gather retained metadata only
for touched chunks. One field-major loop validated every selected value before
publication; dropped fields and selected invalid tails beyond K kept their
existing checks. All-missing build sides retained typed defaults. Earlier
complete file-build validation remained unchanged. No public API, dependency,
external query engine or source-format execution family was added; Vortex
remained native input/output.

Payload and selection credits stayed with their allocations. New wrapper/schema
and replacement chunk/offset metadata received separate leases before allocation,
including temporary overlap. Root, offset child and raw offset buffer shared
the retained lease. Small selections could pin large backing buffers. Tests
covered next-probe release, build teardown, child/clone escape, cancellation,
denied grants, consumer failure, spill cleanup and final credit release.

The debug fixture contains six ordinary regressions and 96 matrix cases. Both
versions produce identical complete results and pass every ownership check.
Across this matrix, baseline construction of 1,214,090 compact payload cells
and 104,059,054 emitted payload-buffer bytes becomes mapped cells with zero
newly emitted compact payload buffers. Referenced backing remains retained and
credited. These counters exclude scratch, decode work and other provider writes;
they establish no whole-query or whole-process memory reduction.

## Validation and independent CI correction

The exact candidate passed formatting, strict default/native lint, 3,323 default
tests, 7,108 native tests and explicit attribution. The native workspace retained
40 intentionally ignored diagnostics. Actual release CI discovery found 4,151
tests with 36 declared ignored tests and exact ownership across four partitions;
all 17 required mapping/regression tests were present. The diagnostic verifier
rejected 17 corruptions. A fresh release build and isolated installed-package
proof passed all 441 integration tests with no skips and successful uninstall.
No package was published. Candidate source fingerprint:
`0304c24e95c4d1bc963a6ce1d16fff58b2bc9a7aa16890ef50483fd44c151024`.

Before candidate execution, launcher clarification established that both passes
use the same parent Python path without `-B`; the guard supplies
`PYTHONDONTWRITEBYTECODE=1`, while workers keep their original `-B` invocation.
The original verifier and clarification remain retained. No baseline query was
repeated to repair the verifier's command expectations.

Eleven original failed preparation/check observations remain intact, covering
temporary baseline/candidate fixture compilation, focused/prepared fixtures,
alias admission, strict lint and two actual CI cleanup failures. Corrections
passed before scoring. Pre-score source review covered eligibility/aliases,
validation/publication order and ownership/credit lifetime with no unresolved
in-scope finding.

The CI failures exposed a macOS cleanup signal error masking the runner's
original child-leak refusal and preventing its receipt. The two-script fix
preserves the original error, records cleanup errors, bounds both child waits,
and invalidates a passing partition when cleanup fails. Aggregation rejects
cleanup errors even after a status-only rewrite. Deterministic signal-denial
injection fails against the original runner; all 20 corrected runner tests pass.
Independent retention of this fix was declared before any candidate benchmark
query. It changes no native runtime or benchmark supervisor behavior.

## Restoration and retention

After the score, all 20 candidate runtime/test/registration paths were restored
or removed against exact accepted snapshots. Only the two verified CI scripts
remain as code changes. Candidate source bytes remain in evidence, and accepted
compatible-window binaries are unchanged. A future baseline must distinguish
this ancillary CI correction from the retained runtime's older build snapshot.

Verified preparation retirement removed 11,933 debug-cache entries, including
10,162 regular files and 7,005,916,644 logical bytes. Repeated identities and
closed-handle checks preserved all 220 input generations, accepted runtime and
release cache. Storage limits are unchanged and no payload backup was created.
Final retirement of rejected binaries and temporary package environments completed
after portable replay and hosted closeout. All 220 input generations and the
accepted runtime remained unchanged; portable replay passed after retirement.
The subsequent [B checkpoint](native-direct-count-bridge-2026-11-12.md) preserves
an invalid baseline and does not accept a COUNT runtime change.
This result authorizes neither an unchanged
retry nor a resource-only acceptance exception.
