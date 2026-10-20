<!-- SPDX-License-Identifier: Apache-2.0 -->

# Conditional investigation checkpoint — October 19, 2026

Status: current-host integration accepted; hosted PR checks remain separate.

This checkpoint includes the eight completed finite decisions in the
[conditional investigation sequence](native-conditional-investigations-2026-10-19.md):
one retained mechanism, six rejected mechanisms and one attribution without an
eligible signal. Thirteen investigations remain before the 37 required substeps.
The broader owners, all CG-1 through CG-23 gates and the release hold remain open.
The output-window investigation continues separately and is not included here.

## Retained behavior and evidence scope

The [compact local-index change](native-compact-local-indices-2026-10-19.md)
uses checked U16 positions for local extents of at most 65,536 rows. It preserves
native contiguous slices, wide extents/global gathers, preallocation credits and
retained native ownership. The frozen comparison saves 4.38–15.36% of peak tracked
reservations in its target workloads, with an explicit measured engine-time cost.
All frozen controls pass; this is not a CPU speed improvement or a general RSS claim.

The eight changed Rust sources at this checkpoint match the original accepted
source map. The merged base `ac08d39198cfe3922e0f5f560376edbb0e8517f8` has the same
tree as the completed-eight commit `f95490279ce06a91b27d0dc44a3b9ecf41b1d0d6`.
Original comparison, Public39, Full43 and modular-format measurements retain their
original source and compiled dependency identities. They are not relabeled as
measurements of the dependency patches below.

## Dependency intake

The [dependency review](../dependencies/json-digest-dependency-review.md) admits
`serde_json` 1.0.154 and `sha2` 0.11.1 from Dependabot #1570 and #1569. Only those
two versions and archive checksums change in the lock and notice manifest;
dependency edges and features are unchanged. Both remain MIT OR Apache-2.0,
downloaded crate hashes match the lock, and all four bundled license texts are
byte-identical to their originals. Their Rust floors fit the workspace floor.

The freshness gate is updated to the reviewed versions and superseding PR IDs;
stale or unreviewed versions remain blocked. Missing live checks still block
benchmark refresh. PyO3 0.30 (#1568) and the Arrow/Parquet/ORC major changes (#1513)
remain separate compatibility work. No external executor, public API, release,
package publication or performance claim is introduced by this intake.

## Current-host validation

| Boundary | Fresh result on the updated dependency graph |
| --- | --- |
| Formatting and strict workspace/native lint | Pass. |
| Workspace tests | 3,235 pass across 107 targets. |
| Native library, debug profile | 3,030 pass; 30 opt-in tests remain ignored. |
| Native CLI suites | 1,087 pass across six targets. |
| Release-script shard | 168 pass, no failures, errors or skips. |
| License bundle and license regression tests | Bundle validation and all seven tests pass. |
| User-surface reference, golden workflows, public status and contribution governance | Pass. |
| Rebuilt clean installed native package | 105 binding tests and 19 public-context tests pass, with no failures, errors or skips; installed CLI/extension hashes match the new builds and final uninstall is verified. |

Suites overlap. The debug library suite excludes the release-only UTF-8 guard
test under its existing `cfg(not(debug_assertions))`; the earlier compact-index
acceptance used a release test binary. No test annotation or selector was changed.

The first source freeze covers all 2,217 original accepted paths with only the
new lock and license manifest differing. The later freeze additionally changes
the dependency-admission script and its tests. Native Rust sources and the
resolved dependency graph remain identical across the two validation stages;
the Python shard and installed-package proof bind the later freeze.

The [checkpoint evidence](../benchmarks/evidence/conditional-checkpoint-2026-10-19.json)
contains both source freezes, all fresh validation logs and receipts, dependency
provenance, original bot-failure logs, the exact intake patch, current source
contents and rebuilt artifact identities. Binaries and the wheel remain outside
Git with their hashes and build commands recorded. The
[independent packet check](../benchmarks/evidence/conditional-checkpoint-packet-verification-2026-10-19.json)
reopens every archived member, checks the current source hashes and reconstructs
the five-file intake patch from its recorded base. Native runtime sources remain
identical to the original accepted candidate.
