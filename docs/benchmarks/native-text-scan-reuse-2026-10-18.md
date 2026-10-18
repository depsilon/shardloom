<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared native text filter/projection decoding

The retained candidate reduces the matched 43-query sum of per-query
two-sample means from **57.3990281195 to 55.3462438375 engine seconds**, a
**3.576340%** improvement. Both complete paired passes improve. All 172
complete results and schemas match, and no query crosses the frozen
material-regression rule.

The greater-than-3% whole-workload threshold was declared before this
comparison. The focused screen, original protocol, complete measurements and
decision are preserved. Each pass below sums one execution of each of the
43 queries; the mean combines the two matched passes.

Workspace/native gates, the direct Python binding, all public workflows and
the complete format/oracle matrix pass.
[PR #1573](https://github.com/depsilon/shardloom/pull/1573) is merged at
`9dd72db9` after all 44 final-PR-head checks, review and three installed-wheel
lanes pass; preview/production verification also passes. The separate
[hosted packet](evidence/native-text-scan-hosted-2026-10-18.json) records this
closure. The
[implementation contract](../architecture/native-shared-speed-2026-10-18.md)
retains both this mechanism and the rejected mixed-key experiment. Broader
PERF and CG-1 through CG-23 obligations remain open.

## Shared mechanism

An admitted aggregate can reuse the native text array that its filter has
already decoded. Admission uses the predicate, source type, projection and
memory policy. It contains no query numbers, source column names, aliases,
row counts or specific literal values.

A private adapter uses Vortex 0.85's existing `LayoutReader`, `ScanBuilder`,
native expressions and allocator. It delegates file/segment pruning and natural
splits unchanged. For an all-true incoming mask, it reads the text predicate's
source, decodes supported Zstd text once and evaluates the original native
predicate. The projection then consumes the retained filtered owner.

Other projected columns are read only after the final mask is known and
returned in their original declared order. Other encodings retain their native
provider representation. Sparse incoming masks retain the original selection
path. Residual predicates and winner-filter rewrites retain their existing
scan path.

The projection future owns each range entry; the registry keeps weak handles.
Metadata reservations precede allocation, and payloads keep the native
allocator's leases. Empty, failed, cancelled and unpolled tasks retire their
entries. This is query-local ownership, with no cross-query value cache.

The small persisted-file proof observes two native text-view allocations on
the original filtered path and one on the candidate path, with identical
complete values and all memory released. It is a decoder-invocation diagnostic.
The performance claim comes from the separate complete-query comparisons.

## Whole-workload evidence

| Aggregation | Control engine seconds | Candidate engine seconds | Candidate minus control |
| --- | ---: | ---: | ---: |
| Sum of all 43 per-query means | 57.3990281195 | 55.3462438375 | -2.0527842820 |
| Paired repetition 1 | 57.612304164 | 56.000667129 | -1.611637035 |
| Paired repetition 2 | 57.185752075 | 54.691820546 | -2.493931529 |

Both executables use the same resident input through the ordinary public native
CLI, with fresh processes and counterbalanced role order within each query.
The input is the existing 99,997,497-row ClickBench Vortex file; it is not
rewritten for this experiment. The frozen protocol records macOS 27 ARM64,
10 logical CPUs and 16 GiB of physical memory. The declared native policy is
24 GiB and parallelism 12; that admission policy is distinct from physical
RAM and from an enforced whole-process memory ceiling. Both release builds
use Rust 1.99.0, Vortex 0.85 and the same `release-user-surfaces` features.
Input identity checks prehash the resident file. OS cache state is uncontrolled,
so these observations do not establish cold-cache performance.
The score uses the native engine clock. Every query and every slower sample
remains included. A query flags when its mean increase exceeds the larger of
10% of its control mean and 0.15 engine seconds, and it is slower in both pairs.
No query flags.

The unchanged input, complete results and schemas, native certificates, resource
policy, executable identities and source hashes are checked before acceptance.
There is no cache purge, artificial delay, automatic retry or best-sample
replacement. These are matched local observations; variation across hosts and
other workloads remains unmeasured.

## Focused screen and experiment history

The focused comparison measures Q29 because the existing file has no embedded
length rewrite for that predicate source. Existing length-based rewrites keep
priority. The runtime admission rule remains independent of benchmark query
identities.

At parallelism 12, the five-pair Q29 mean improves from 4.4667727914 to
2.929149933 engine seconds, or 34.423574%, with all five pairs positive.
The two single-worker pairs also improve. All 74 focused calls match complete
reference values and schemas and pass the predefined resource and control
limits. Each candidate call records 3,120 native decoded chunks and 3,119
projection reuses, confirming that the intended path ran.

| Attempt | Scope | Complete paired calls | Decision |
| --- | --- | ---: | --- |
| 638 | Borrowed compound text and compact complete mixed keys | 78 | Drop: target gain/consistency and a control regression |
| 642 | Shared text filter/projection decoding, focused screen | 74 | Retain for whole-workload evaluation |
| 643 | Shared text filter/projection decoding, all 43 queries | 172 | Pass the prospective greater-than-3% material gate |

All 324 complete paired observations remain retained. The rejected mixed-key
runtime is absent from the accepted candidate. Twelve earlier profiles and the
small decoder-invocation fixture are separate diagnostics and receive no
benchmark timing credit.

The preceding
[native text-size screen](native-ingest-text-size-2026-10-18.md) rejects all five
alternatives under its frozen size/write/consumer limits. Its evidence is
retained separately; the current comparison uses the accepted input unchanged.

## Validation and reproduction

Seven new native tests cover the original duplicate decode and the shared
decode, complete projection values, nullable Unicode and embedded NULs,
slices and declared field order, partial input masks, preserved dictionary
encoding, unpolled cleanup, metadata denial, duplicate active ranges,
last-owner release, source errors and cancellation. Seventeen existing
UTF8 and parent-cancellation tests also pass. Strict native all-target Clippy
and formatting pass on the measured source.

Formatting and strict workspace Clippy pass. Workspace tests record 4,374
successful executions without exclusions. Native-feature tests record 4,046
successful executions and 28 explicitly ignored manual benchmark/fixture cases;
the native all-target Clippy pass uses identical measured source. These are
execution counts across configurations, not unique methods. The fresh release
binding passes all 100 direct methods without skips.

All 4,827 public cases across 36 families pass. Independent readback opens 13,004
compressed JSON records, 4,495 complete value files, 6,330 native envelopes and
45 pressure fixtures containing 1,578,741 rows. It verifies 39,979 explicitly
expected value rows against the preserved references.

All 18 format/workflow cohorts pass: 704 native workflow records agree with 704
independent pandas-oracle records. The matrix covers 22 workflows over raw and
prepared inputs, collecting from all eight input formats and writing all eight
output formats from CSV. Every cohort report is independently reopened. The
oracle remains a test-only comparison boundary.

The [portable evidence index](evidence/native-text-scan-reuse-2026-10-18.json)
records archive hashes and identifies the complete member manifest. Every archived member
is reopened: 27,981 members across five archives. All 324 paired calls are
rechecked for complete values, schemas,
native clocks, CSV parity and policy identity. The whole-workload decision is
recomputed from the archived observations. Large inputs and executables are
excluded with exact identities; their source and reproduction records remain.
Installed-wheel and final-head hosted checks remain separate from direct
source-package testing.

Download the [member manifest](evidence/native-text-scan-reuse-2026-10-18.manifest.json.xz)
and archives [1](evidence/native-text-scan-reuse-2026-10-18.tar.xz),
[2](evidence/native-text-scan-reuse-2026-10-18.part-2.tar.xz),
[3](evidence/native-text-scan-reuse-2026-10-18.part-3.tar.xz),
[4](evidence/native-text-scan-reuse-2026-10-18.part-4.tar.xz) and
[5](evidence/native-text-scan-reuse-2026-10-18.part-5.tar.xz).

To reproduce the measured cohort, build the frozen control and candidate
source snapshots in release mode, verify the recorded executable and input
identities, and use the retained paired-run command with all 43 queries,
`--repeats 2`, `--memory-gb 24` and `--max-parallelism 12`. Preserve the
predeclared role order, complete references, source-generation checks,
storage limits and process supervision. The packet retains the exact commands,
helpers, source snapshots, original contracts and every raw result archive.

The control is the accepted shared-execution runtime merged in
[PR #1571](https://github.com/depsilon/shardloom/pull/1571) at `d2e6867d`.
Its frozen executable is linked to that revision through the exact prior
comment bridge. The inherited ingestion changes are test-only. The candidate
is bound to 2,415 selected source files, with four changed or added Rust files.
Both release executables use `release-user-surfaces` and Vortex 0.85.

| Artifact | SHA-256 |
| --- | --- |
| Accepted control CLI | `a0b0b95c3440806d8445ca8b353e32a97906c65889273a0a8cf15c282a22406a` |
| Candidate CLI | `39348e1b49abfd08fc81a087a80b7c3312d7298dcae3658c3aff7fa129dd233f` |
| Native input | `06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22` |

No dependency, external execution engine, compatibility fallback, input rewrite,
package publication or release is introduced by this change.

## Hosted integration and merge

The final reviewed head is `f9f7a245cc1ba3f80284588a4cd05062435d83f5`;
the merge is `9dd72db9ea932c72de70c5790512082e0cd821ff`. All 44 checks pass
on that final PR head. Automated review completes on the same commit with no
findings, review threads or open PR code-scanning alerts. Reviewed, synthetic-test
and merged Git trees are identical. All 2,415 selected source files also match
the measured build and complete local acceptance byte-for-byte.

Eight complete hosted job logs are independently checked. They include 6,793
native/provider test executions with 28 explicitly ignored manual cases, all
seven new text-scan tests, and 1,079 public CLI tests. Each of the macOS Python
3.13, Ubuntu Python 3.13 and Ubuntu Python 3.10 wheels passes all 100 native
binding methods without skips after isolated installation; library/CLI identity,
load and uninstall checks also pass. These are CI-built wheels, not a new public
package release.

All eight preview and eight production responses preserve their committed
source, permitting only the recorded bounded Cloudflare insertion. Their
deployment checks pass on the reviewed and merged revisions respectively.
Desktop and verified 375-by-844 mobile layouts, navigation and search pass with
no console warnings or errors; temporary viewport overrides are restored.
The unapplied first production viewport attempt is retained and receives no
mobile acceptance credit. Browser coverage is limited to the current in-app
browser, not cross-browser certification.

The [hosted index](evidence/native-text-scan-hosted-2026-10-18.json) and
[archive](evidence/native-text-scan-hosted-2026-10-18.tar.xz) retain 186 reopened
members with exact hashes, including raw logs, review state, source identities,
served responses and browser observations. The archive SHA-256 is
`509543153dd3d43b0b658f52d5a739eaf934d570739191416f519acf4c1fc500`.
This post-merge record adds no timing samples and changes no original comparison
or acceptance rule. Broader PERF and CG obligations remain open.
