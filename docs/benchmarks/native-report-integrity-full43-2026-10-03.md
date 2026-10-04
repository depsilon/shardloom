<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native execution report integrity

This is the reporting repair discovered while checking the
[dynamic-pivot acceptance](native-dynamic-pivot-full43-2026-10-03.md).
It belongs to that unit's correctness and PERF-12 evidence work. Runtime commit
`55b14d148f1d7dec8e49ee17be8293bb36cb01d8` gives each affected evidence field one
owner. Query algorithms, native input/output, resource policy and dependencies
are unchanged. Local public acceptance and all 258 paired Full43 complete-result
comparisons pass. Hosted completion remains pending.

## Defect and repair

The retained footer-count path emitted `local_primitive_no_query_answer_cache=true`
from its actual execution, then appended `false` from the generic renderer's
absent row-report default. Both frozen roles in the earlier Full43 observation
contained that contradiction in Q1. The values and timing results were checked
independently, and the conflicting field was explicitly excluded from proof.

The native input binding also emitted bare `fallback_attempted` and
`external_engine_invoked` flags already owned by execution renderers. An audit of
all 7,518 earlier public responses identified 37 aggregate execution responses
with identical repeated `false` values. Filtered-count rendering separately
repeated cache evidence supplied by its real primitive report.

The input binding now emits binding facts. Existing execution renderers remain
the sole owners of their fallback/external flags. The shared primitive renderer
emits the cache flag once, using the actual report when available. Exact native
footer counts use an explicit metadata-count wrapper: no row report is fabricated,
and the generation-validated footer execution supplies `true` cache evidence.
Unrelated absent-report rendering preserves its existing `false` value. There is
no first-value/last-value selection or duplicate-suppression pass.

The resident test transport now rejects every repeated field name before any
field accessor runs. Its footer, filtered-count, aggregate, collect, invalidation
and denial cases exercise the same public worker protocol. Both public acceptance
runners preserve the raw response and reject repeated names, including repeats
whose values agree. Unit checks also prove that generic absence does not fabricate
positive cache evidence and that real reports retain either Boolean value.

## Frozen build and local correctness

| Item | Evidence |
| --- | --- |
| Candidate runtime | `55b14d148f1d7dec8e49ee17be8293bb36cb01d8` |
| Candidate executable SHA-256 | `b7fc281612df107c61f5fc27eb9df84c1ea2dd90a3c8c9ee631b465f9f0abde0` |
| Paired control runtime | `50cc1e22c4df85883653ddba60783fa2e4108f3b` |
| Paired control executable SHA-256 | `a738248bcb8e8feaacc94854ff328f9d71cf47513fa92dfb9c6bfc2a694f0ed4` |
| Build | Rust 1.99.0, release profile, `release-user-surfaces`, identical Cargo lockfile |
| Source proof | Clean committed candidate, 800 source hashes; retained fixture executable and dependency sources reverified |
| Public workflow matrix | 3,305 checks; 7,687,525 complete row comparisons; native and compatibility inputs, all admitted local writers |
| Direct unary matrix | 202 checks; 131,734 complete row comparisons |
| Focused resident regression | New assertion fails before the repair; all 36 existing worker tests pass after it |
| Workspace tests | 3,447 passed |
| Native CLI tests | 1,582 passed |

Formatting, strict default-workspace and native-workspace Clippy, default workspace
tests, native CLI all-target tests, Rust 1.96 native all-target checks and UAT
consumer tests pass. The UAT consumer suite runs 30 tests with one existing
environment-dependent skip. Configuration totals overlap. The native Vortex
implementation, Python SDK and generated website are byte-identical to the
preceding accepted tree; their earlier full-suite evidence is retained. This
repair does not claim fresh runs of every unchanged suite.
The four affected support/status/productization/front-door documentation checks
also pass, for eleven selected local gates in total.

## Complete paired regression

The predeclared paired observation compares both frozen binaries on the same
resident 99,997,497-row Vortex artifact and all 43 retained references, with three
calls per query and role. The artifact SHA-256 is
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
Its generation, complete bytes and local residency are checked before timing.

The host has 16 GiB physical memory and 10 logical CPUs. Both roles request the
same 24 GiB policy and parallelism 12; the policy is not a process-RSS limit.
Execution is serial under the existing storage, concurrency, deadline and process
cleanup guards. Source prehashing and uncontrolled OS cache/ordinary host activity
are explicit. All raw runs, complete results, resource observations and archived
responses are retained.

Investigation thresholds are declared before execution: a per-query median time
change of at least 10% and 0.1 seconds, a median RSS change of at least 10% and
32 MiB, or an aggregate best/median-sum change of at least 5% and one second.
Flagged queries repeat with reversed role order; an aggregate flag repeats all
43 queries.

| Observation | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Sum of fastest valid query runs | 52.060873 s | 52.796376 s | +0.735503 s / +1.413% |
| Sum of per-query medians | 53.455496 s | 53.657423 s | +0.201927 s / +0.378% |
| Sum of every raw run | 162.300000 s | 161.998213 s | -0.301788 s / -0.186% |

All 258 complete results match the retained references. No per-query timing,
memory or aggregate threshold is crossed, so no follow-up repeat is required.
No performance improvement is claimed. These same-window local observations are
subject to the declared cache/host effects and do not establish causal differences.

## Historical correction and acceptance boundary

The original dynamic-pivot packet's `validation_scope` prose called the 37
matching duplicate-flag responses "preparation envelopes." They are aggregate
**run responses**, as shown by their retained command, summary and raw hashes.
The new audit records all 37 paths and values. This corrects the description;
the original packet, raw outputs, timings, counts and result proof remain intact.
Its unchanged compressed SHA-256 is
`f833b53f2062b86b0030a19faf20e7d13a42b291bb1aabafc376356b7ab27ec8`.

The repair's [portable addendum](evidence/native-report-integrity-2026-10-03.json.xz)
retains current raw public/Full43 envelopes,
source/binary identities, original duplicate observations, complete references,
frozen public oracles, check logs and verification tools. Its verifier checks field
uniqueness before accessors and preserves the old control's ambiguous fields as
historical observations. All 7,518 current public responses and all 129 candidate
Full43 responses have unique field names, including embedded certificate fields.
Every candidate Q1 response carries exactly one `true` cache flag, the exact
99,997,497 footer count, a certified metadata-only native path and explicit absence
of row execution. The verifier also checks direct-unary, inspection and denial
responses, persisted output hashes, source generations and released owned locks.

Hosted merge still depends on the nested-composition base and the website audit's
explicit advisory decision or remediation. This repair does not activate the
disabled exception, publish packages, close broader type/adapter/spill work, or
establish competitive or production certification.

## October 4 website dependency follow-up

The [dependency update](../dependencies/website-build-dependency-review.md#2026-10-04-registry-update)
selects `http-cache-semantics` 4.3.0 and passes the standard dependency audit.
The unused exception proposal is removed. Earlier website-blocker statements
in this report describe its original frozen revision; they no longer identify
the current dependency posture. Hosted runtime review and checks remain separate.
The recorded benchmark results, source identities and immutable packets are unchanged.
