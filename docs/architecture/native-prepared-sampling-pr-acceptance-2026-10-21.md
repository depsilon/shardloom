<!-- SPDX-License-Identifier: Apache-2.0 -->

# Prepared sampling: current PR integration

Status: local integration accepted at
`9e51ffc1a490bc39d50d50acd53c56ec5d72e473` on October 21, 2026.

The [finite prepared-sampling decision](native-prepared-sampling-decision-2026-10-21.md)
is integrated with the existing investigation PR and its reviewed dependency
updates. The complete normal installed-package suite now uses the repository's
corrected output-window fixture directly. This integration adds no benchmark
measurement, new public capability or broader roadmap completion.

## Source bridge

The tested merge preserves both the previous PR source `e07bf341` and the
accepted sampling/fixture/documentation branch `23a78b10`. The
[integration packet](../benchmarks/evidence/prepared-sampling-2026-10-21/pr-integration.json)
verifies all 2,588 compiled/package/validation source inputs against Git. From
the original acceptance source `4cd3a9e`, exactly seven such paths differ:

- `Cargo.lock`: the existing PR updates `serde_json` from 1.0.151 to 1.0.154 and
  `sha2` from 0.11.0 to 0.11.1, preserving every other package field except the
  corresponding checksums.
- The Python dependency-license manifest, release-script tests and dependency
  freshness checker retain the existing PR's corresponding updates.
- The output-window binding fixture is byte-identical to the corrected fixture
  used by the original installed-package acceptance.
- The independent native-exchange fixture generator and its README retain the
  existing PR's quiet-output correction.

Every other native implementation input is unchanged. The original two scored
comparisons, public matrix, Full43 and format matrix retain their own source,
dependency and executable identities. Their measurements are not relabelled as
observations of this newer dependency graph.

## Fresh checks

All twelve validation phases and 23 supervised commands pass, with unchanged
source identities and drained child processes:

- Formatting and strict workspace/native Clippy pass.
- Workspace tests: 3,235 pass across 107 targets, with no ignored tests.
- Feature-enabled native tests: 3,107 pass across 13 targets, with 31 existing
  ignored tests. Native CLI tests: 1,091 pass across six targets.
- Minimal native feature compilation passes. Optional output-window, credit
  and encoded-Arrow controls pass 16, nine and eleven tests respectively.
- Python core discovers 700 cases; unittest reports 559 run and 23 skip
  entries. These are distinct counters. All 168 release-script and 18 focused
  Python checks pass.
- Fresh release CLI/library builds and all eight isolated package lifecycle
  steps pass. All 118 repository binding tests execute with no failures,
  errors or skips; no replacement test directory is used.
- Five documentation/provenance validators, golden workflows using the fresh
  CLI, and the complete original acceptance-packet replay pass.

An initial replay incorrectly equated Python discovery with unittest's run
count. The packet preserves that failed replay and its original helpers. The
corrected replay verifies both observed counters and the unchanged test
selection; no suite or engine workload is repeated for that correction.

The integration replay rejects eleven coherent evidence corruptions, including
omitted phases/source inputs, narrowed native/optional tests, unproved worker
drain, an installed-test overlay, skipped or omitted installed tests and a
changed installed library identity. Member references are rehashed in these
negative controls so they exercise semantic checks. The unchanged positive
packet passes again. This bounded check supplements the original packet's
34 corruption cases.

## Scope

The packet contains text evidence and build identities, not backup runtimes or
environments. Replay verifies archived evidence without running native queries
or asserting that old payloads remain resident. Fresh current-dependency
correctness and installed-package results are distinct from the original
complete-workflow and performance observations. Hosted/cross-platform checks,
release scope and the 37 required breadth substeps retain their separate
obligations. Vortex-native execution and explicit no-fallback policy remain
unchanged; no release or package publication occurs.
