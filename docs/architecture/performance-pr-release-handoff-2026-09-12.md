<!-- SPDX-License-Identifier: Apache-2.0 -->

# Performance PR and release handoff

The selected performance and `0.2.4` publication train is complete. Performance
PR [#1443](https://github.com/depsilon/shardloom/pull/1443) merged at
`2c9b84b76757bf1fd3e1a2e71c76d692ab7b7afb`; version PR
[#1444](https://github.com/depsilon/shardloom/pull/1444) merged at
`8759b16e3421153302c9034e5a00c9d80b61d3d9`. All four selected channels have
passed proof recorded in [publication verification](../release/v0.2.4-publication-verification.md).

Full UAT for `4f2c7b97007864d0396b10bdc5dc2bbfef52df38` passed before PR
submission. The [acceptance report](../benchmarks/combined-performance-uat-2026-09-12.md)
retains its pre-bump source/binary, commands, complete-value checks, timings and
preserved failed attempt. Package source versions are now `0.2.4`; channel
proofs separately bind the published artifacts. Neither publication nor the
version bump reattributes those UAT measurements to a new binary.

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

## Completed PR and publication sequence

1. The performance PR merged only after the combined implementation UAT passed.
   Its merge is `2c9b84b76757bf1fd3e1a2e71c76d692ab7b7afb`.
2. The version PR began as a seven-file stack on the performance PR tip
   `818596c4d4e3375991d2c71685d8aaa6c08f7712`: five mechanical version sources,
   release notes and source-validation evidence. It was retargeted to main after
   the performance merge and merged as `8759b16e3421153302c9034e5a00c9d80b61d3d9`.
   The [source validation](../release/v0.2.4-source-validation.json) preserves
   the exact tested version checkpoint and command results.
3. The authorized selected-channel sequence completed: GitHub pre-release,
   TestPyPI, PyPI and Homebrew, each with its own installation, smoke, uninstall
   and artifact-identity proof. The [publication record](../release/v0.2.4-publication-verification.md)
   owns exact tag, build, workflow and tested-platform facts. Registry builds
   retain their own hashes; a shared version is not evidence of identical bytes.
4. The experimental Rust
   `FlatLocalColumnarStreamSource::source_identities` struct-literal migration
   remains in the [release notes](../release/v0.2.4-release-notes.md).
   Internal Rust crates remain unpublished.
5. Current install guidance and the selected-channel matrix follow the passed
   `0.2.4` proofs. Historical `0.2.3` transcripts and source/UAT evidence remain
   unchanged. Future channels and production gates are not promoted.

The train does not close unfinished PERF/CG gates or claim production readiness,
competitive superiority, general SQL/DataFrame completeness, distributed or
lakehouse support, public Rust crate availability or external-engine fallback.
