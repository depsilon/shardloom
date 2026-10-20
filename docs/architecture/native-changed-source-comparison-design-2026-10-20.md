<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite whole-file refresh comparison — October 20, 2026

Status: prospective protocol, frozen before candidate comparison. This is the
finite mechanism under `PERF-02.changed_source`, conditional row 13 of the RFC
0044 program. Broader owners and CG-1 through CG-23 remain open. The
[provider/identity intake](native-changed-source-reuse-intake-2026-10-20.md) and
[region proof design](native-changed-source-region-proof-design-2026-10-20.md)
record the prerequisite failures and subsequent passing lifecycle.

## Mechanism and comparison boundary

The experimental adapter hashes a bounded current Parquet-file inventory and
authenticates a previous native artifact against its successful publication
digest. It validates the embedded complete file-to-row map, schema, build,
provider, recipe and transform. Exact same-name/content files reuse their whole
native arrays; changed and new files use ordinary conversion and the admitted
small-source writer child. Removed files contribute no rows. Every refresh
writes one complete new Vortex artifact and recomputes statistics and metadata.
Shared wrappers are explicitly removed; partial Slice wrappers are refused.

Compare this adapter with
`prepare_local_source_as_vortex_for_public_workflow_with_schema`, the current
public preparation helper, using a new target on each preparation. The baseline
retains its normal fingerprint policy, advisor, capillary, writer and post-write
identity checks. Do not charge it candidate-only content hashes. The candidate
uses the prerequisite's actual readers, native writer, shared credit owner and
validated publication helper; it is not exposed as a new public route yet.
Its measured cost includes its complete proof, read, conversion, serialization,
statistics, metadata, publication and ownership teardown. A positive screen
requires full public integration and renewed acceptance before retention.

This is a new-target changed-source workload. Existing same-source/same-target
reuse, native whole-file copy and generation-conditional replacement have
different contracts and receive no optimization claim from this comparison.
No external execution engine, new dependency or raw-segment-copy claim is used.

## Fixed cases

All cases use P1 and a 256-MiB native grant, two nullable fields (`number: i64`,
`text: utf8`) and the prerequisite's independent ordinal formula. Integers are
`row - 9000`, NULL every seventeenth row. The eleven-way text cycle includes
NULL, empty, Unicode/NUL, row-specific long text and repeated categories. Files
are lexically ordered and every file must supply one actual original source
batch. Source generation is outside preparation; source updates are outside both
routes' refresh clocks. Each new artifact is limited to 32 MiB, source inventory
to 64 MiB and the embedded binding to 64 KiB.

| Case | Initial shape | Source update | Expected reuse | Role |
| --- | --- | --- | --- | --- |
| `high-change1` | 16 files × 8,192 rows | Replace file 01 with the next ordinal range | 15 files | Target |
| `mixed` | 16 files × 8,192 rows | Replace 01, remove 02, add 16 with file 00's identical bytes | 14 files | Target |
| `moderate` | 4 files × 8,192 rows | Replace 01 with the next ordinal range | 3 files | Target |
| `all-changed` | 16 files × 8,192 rows | Replace every file with successive new ordinal ranges | 0 files | Control |
| `small` | 4 files × 32 rows | Replace 01 with the next ordinal range | 3 files | Control |
| `single` | 1 file × 8,192 rows | Replace 00 with the next ordinal range | 0 files | Control |

The candidate applies the same mechanism to every case. No case-specific bypass,
post-result size threshold, changed schema, smaller input, omitted control or
favorable-column promotion is allowed. An admission-policy redesign is a new
mechanism and requires a separately justified protocol.

## Complete workflow and clocks

Each workflow creates its fixtures, then performs an initial preparation,
producer-close/move, fresh native prepared filtered COUNT/SUM twice, a defined
source update, complete refresh or rebuild, another close/move and another fresh
prepared COUNT/SUM twice, followed by output retirement. The same prepared query
is reused only for the second execution of each artifact. Both roles use the
same native query construction and result serialization. There is no cold-cache
claim and no operating-system cache purge.

The primary complete-workflow cost is the sum of both complete preparation
calls, both file moves, both native consumer setup/execution/serialization/
teardown sequences and the two output unlinks. Initial content-map creation and
each old-artifact authentication are included. Keep query `timing.execution`
separate and report its four-call sum as the engine clock. Fixture generation,
intentional source updates, independent oracle checks, extra full-value
validation, harness record formatting, process launch and build are excluded
from both roles' compared operation costs and retained as distinct diagnostics
where observed. Do not substitute supervision time for either clock.

After the timed consumer calls, use a fresh native session to check every
ordered row and exact logical schema in bounded projections. Check every result
and certificate, source full hashes/generations, unchanged output hashes,
actual batch/reuse counts, peak native credits and zero final credits. Save
complete operation records before retiring only that workflow's generated
payloads. Preserve failures and partial results without treating them as a pass.
The independent verifier recomputes expected row digests and aggregate values
from the fixed formulas, validates the complete sample grid, and recomputes all
decision gates. The preceding separate-process proof remains a distinct
prerequisite; individual timed workflows do not claim separate-process handoff.

## Samples and immutable gates

Compile the ordinary CLI test executable and experimental native test executable
from the same frozen source manifest, locked dependency graph, optimized profile
and features needed by both paths. Bind each executable's complete digest.
Run serially through the existing storage/concurrency/process guard.

First run one unscored workflow per case and role (twelve). Require exact
semantics, normal public batch counts, actual candidate reuse counts, recipe
agreement, memory and cleanup before measurement. Then complete two cohorts,
each with three role pairs for every case (72 measured workflows). Case order
is the table order. Within cohort A, even `(case index + repeat)` pairs use
ordinary then candidate, odd pairs reverse it. Cohort B reverses those orders.
Complete both cohorts before evaluating comparison gates. Do not adapt samples
or rerun unchanged cohorts to obtain a passing result.

For each case/cohort/role use the median of its three observations. Both cohorts
must satisfy all of these conditions:

1. Every target improves both complete workflow and refresh preparation by at
   least 2%. An improvement in refresh alone cannot hide initial proof cost.
2. Every control's complete-workflow median and every case's initial-preparation
   median stay within 5% of ordinary preparation.
3. Every case's four-call engine median stays within 5% of ordinary execution.
   This is a downstream control, not the preparation benefit objective.
4. Both artifacts preserve exact values/types and all source/lifetime checks.
   Each candidate artifact uses at most 105% of its ordinary artifact's bytes
   plus the existing 64-KiB maximum binding allowance. Report the actual extra
   bytes; the allowance is a bounded provenance cost, not compression savings.
5. Every operation stays inside its 256-MiB native grant and releases all credits.
   Native peak is reported; it does not establish an RSS ceiling or complete
   allocator coverage inside the Parquet provider.

A failed gate rejects this finite unconditional adapter. Record target and
control outcomes separately, including whether the same failure repeats across
cohorts. A positive screen only admits integration work: it is not a shipped
optimization, production admission, generic partial-region reuse, safe
replacement, format breadth or performance-superiority claim.
