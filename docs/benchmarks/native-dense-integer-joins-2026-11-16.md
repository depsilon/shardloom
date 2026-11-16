<!-- SPDX-License-Identifier: Apache-2.0 -->

# Dense integer lookup in native resident joins

The bounded join directory is retained: all 630 declared requests pass, and
complete engine time decreases **102.926667930 → 94.178862655 seconds** (8.5%).
This is one scoped observation with a mixed reused/new baseline and uncontrolled
host/cache state, not a full-corpus or repeatability claim. PR #1695 is merged
as `90d036d6e3ce465e622f5fb330186a8c5e5db4d2`; exact-main validation is finishing.

## Implementation

Ordinary resident inner/left/right/full joins can map a single I64 key directly
to the existing completed index entry. Admission requires at least 128 distinct
keys, an inclusive span no larger than twice that count, and directory storage
within one sixteenth of the unreserved memory grant. The decision occurs once
after mandatory probe/pair reservations. Sparse, tiny, composite, conditioned,
semi/anti and explicit-spill joins retain their existing paths.

The directory reuses native typed owners, credited allocation, duplicate-row
order, pair gathering and payload validation. Signed extremes use widened
checked arithmetic; nulls and absent keys do not match. Optional budget refusal
keeps ordinary lookup; cancellation and invariant failures remain errors. No
dependency, public API, external executor or Vortex input/output contract changes.
Debug-only counters verify activation without adding production per-row tracing.

The pinned Vortex membership providers do not return the existing join entry
and ordered duplicate rows. The implementation therefore extends the existing
ShardLoom index owner rather than introducing another execution provider.

## Scope and result

The population was frozen before timing: 600 existing join and neighboring
requests plus 30 versioned negative/sparse-key controls, across five public
interfaces and Parquet, Arrow IPC and Vortex. Profiles comprise 448 primary,
141 holdout, 28 smoke and 13 scale requests. The saved source generations and
complete reference contracts are in the protocol below.

The baseline is the accepted variable-copy runtime at `ffe0d077a676fd8b`.
Its 148 reused observations have exact request, runtime, input, reference,
resource and clock correspondence; 482 baseline observations are fresh.
Every candidate request runs once on the release runtime built at
`b21494e08858d219`. There are no warmups, retries or competitor executions.
Both runtimes use two threads, a 2-GiB native grant and the same bounded
supervisor. Full input/source hashing occurs outside query execution.
The host is macOS 27 arm64 with Rust 1.99 and Python 3.13.13.

| Population | Requests | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Complete cohort | 630 | 102.926667930 | 94.178862655 |
| Fresh baseline observations | 482 | 79.787916354 | 72.405568442 |
| Reused baseline observations | 148 | 23.138751576 | 21.773294213 |

All format and interface totals decrease. There are 285 individual increases
totaling 0.928322258 seconds; semi/anti and sparse-key control totals increase.
Every result remains in the decision. The complete-cohort rule retains the
lower total after correctness, resources, ownership and no-fallback checks.
Earlier dropped join-mapping and propagation decisions remain unchanged.

## Validation and retained evidence

Formatting, strict default/native lint, default/native workspace tests,
all-feature/no-default checks and Rust 1.96 compatibility pass. The suites pass
3,344 default and 7,316 native tests, with 42 explicitly classified native
ignores. Eight focused native controls exercise semantics, non-admission,
cancellation, allocation refusal, duplicate order and final refunds. All 455
installed-wheel tests pass with zero skips. The initial installed activation
test incorrectly required Bloom probes on an admitted dense path; commit
`d5492454f18560ec3` separates sparse Bloom and dense-directory expectations.
Only that Python test changed after the release build; the source-correction
record proves unchanged Rust/package sources and binary bytes.

All 1,260 complete result packets pass independent-reference, schema,
certificate and resource replay. Separate debug observations validate 54
requests: 44 nodes admit the directory in 37 requests. Debug clocks are excluded
from the performance result. Original failed build, fixture and packaging
attempts are preserved; none supplies a scored result.

The compact checked-in records retain the exact frozen
[protocol](evidence/native-dense-integer-joins-2026-11-16/protocol.json.gz),
[complete paired replay](evidence/native-dense-integer-joins-2026-11-16/replay.json.gz),
[validation](evidence/native-dense-integer-joins-2026-11-16/validation.json.gz),
and [decision](evidence/native-dense-integer-joins-2026-11-16/decision.json.gz).
They include all per-request observations, regressions and original artifact
identities; they are not a standalone portable copy of the raw result store.

Raw results, exact source snapshots, frozen inputs/references, execution scripts
and both runtimes remain under
`/Users/dylan/LocalData/shardloom/dense-integer-joins-2026-11-16`, including the
already generated `dense-join-packet-r2`. With those retained dependencies,
`python3 -B dense_join_replay.py --help` describes offline replay; the frozen
protocol records the submitted jobs, input generations and execution settings
for reproduction. No additional portable-archive verification is claimed.
This concise closeout follows the maintainer's instruction to reduce optional
evidence ceremony and continue implementation. Item 19, item 20, RFC 0044 and
CG-1 through CG-23 remain open.
