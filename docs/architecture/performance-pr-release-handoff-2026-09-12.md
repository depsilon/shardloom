<!-- SPDX-License-Identifier: Apache-2.0 -->

# Performance PR and release handoff

Full UAT for the selected combined source `4f2c7b97007864d0396b10bdc5dc2bbfef52df38`
passed before PR submission. The [acceptance report](../benchmarks/combined-performance-uat-2026-09-12.md)
records the exact source/binary, commands, complete-value checks, timings and
preserved failed attempt. Source versions remain `0.2.3`; the next requested
step is the performance PR, followed by a separate stacked `0.2.4` version PR.

The branch `codex/performance-pr-20260912` starts at merged main
`5e8af695c02459be4fe7c6d3c49d3459d72a103f`. Accepted numeric ingest,
resident reuse, owned integer DISTINCT and existing ownership/spill work are
already inherited through #1440. The original local main checkout is untouched.
The [phase plan](phased-execution-plan.md) continues to own unfinished PERF/CG work.

## Selected scope

| Addition | Boundary |
|---|---|
| UTF8-grouped integer COUNT DISTINCT | Shared native workers, complete pair reduction and global count/UTF8-byte ordering; ordinary result path only. |
| Owned integer COUNT(*) | One nonnullable identity key; original integer width and U64 counts through existing prepared/owned APIs and sinks. |
| Prepared and owned UTF8 COUNT(*) | One nonnullable identity key; bounded exact selection, copied selected text bytes, source generation and final-owner reservations. |
| Exact scalar integer footer aggregates | Unfiltered global identity integer MIN/MAX/COUNT, optionally COUNT(*), when every measure has sufficient exact held-file evidence. |

Both owned COUNT families require a nonnullable parent Struct. Existing aliases,
HAVING and explicit compatibility sinks remain intact. Missing/inexact footer
facts retain the native scan; contradictory facts fail explicitly. No dependency,
external query-engine fallback, query-name switch or aggregate projection API is added.
See the [owned COUNT](../reference/owned-count-results.md),
[footer](../reference/exact-footer-aggregates.md) and
[DISTINCT worker](../reference/utf8-integer-distinct-workers.md) contracts.

Keep the regressing nullable-minute worker admission, failed compound-owned COUNT,
slower text-storage replacement, extra measures, nullable COUNT, new spill families,
broad join/window/projection work and speculative codec portfolios outside this PR.
Their experiments remain evidence for their own scopes, not release capabilities.

## Combined-source acceptance

- Formatting, workspace/native/minimal Clippy and focused checks passed.
  Workspace tests: 3,417 passed. Native feature tests: 3,321 passed, nine existing
  manual cases ignored. Counts overlap; they are not unique-test totals.
- 552 owned-result calls, 96 renamed DISTINCT calls, 460 independent held-out
  calls (440 values plus 20 expected overflow diagnostics), 64 paired query calls
  and nine public CLI/Python session calls passed.
- Fresh ingest: 99,997,497 rows in 95.923669 seconds, 18,591,586,804 native bytes.
  Every value across 112 columns and all 560 loaded footer-statistic slots matched.
- Fresh Full43: 129/129 exact results; sum of per-query best of three 91.825940 s,
  hot minimum sum 92.488320 s, all 129 native calls 281.793539 s.
- Q14 medians: 7.968275 → 1.554247 s. The declared paired gate passed all controls;
  Q35 still shows a +4.06% paired median and +16.39% worst pair. This is feasibility
  evidence, not general timing stability or a historical Full43 control promotion.
- Governance, versions, public docs, CI contracts and package catalog passed.
  The architecture tracker remains explicitly blocked by 116 open phase items.

The initial Full43 attempt stopped at its reserved-log limit after 86 validated
results. Its evidence is preserved and excluded from the new complete run.
Verified lossless archival restored headroom under unchanged guards; only the
identified reproducible slower text payload was retired without payload archival.
The full report preserves these facts, receipt identities and restoration limits.

Historical `4730003f`, `607db50a`, `b40fd02a` and `8acb1263` component
measurements remain scoped to their original sources. The historical
91.215296-second control is unchanged. Owned timings compare representations
within one binary. Neither these results nor static catalog checks certify
versioned packages, publication channels or production readiness.

## PR and version order

1. Open the cohesive performance PR with this completed implementation UAT.
2. Create the `0.2.4` branch and PR directly from the opened performance PR's
   exact tip. Record its dependency/base SHA and keep the direct diff limited
   to the five mechanical version sources plus release notes/evidence.
   This is a stacked PR; neither merge is assumed or authorized by preparation.
3. Change root `[workspace.package].version`, then use
   `scripts/sync_workspace_package_versions.py` for Cargo.lock, Python
   `_version.py` and the website package files. Run version contracts, relevant
   Python tests and required Rust checks on the versioned source.
4. Retain the experimental Rust
   `FlatLocalColumnarStreamSource::source_identities` struct-literal migration
   in the `v0.2.4` notes. Internal Rust crates remain unpublished.
5. Keep published-channel selectors, current installation guidance and `0.2.3`
   proofs unchanged until corresponding new channel proofs exist. Publication,
   if separately authorized, follows GitHub pre-release, TestPyPI, PyPI and
   Homebrew verification.

The train does not close unfinished PERF/CG gates or claim production readiness,
competitive superiority, general SQL/DataFrame completeness, distributed or
lakehouse support, public Rust crate availability or external-engine fallback.
