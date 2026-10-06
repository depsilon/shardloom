<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native nested pivot state: local core acceptance

The frozen implementation passes **27,373 public checks and 15,820,181 complete
row comparisons**, plus 202 direct-workflow checks and 131,734 rows. All 129
Full43 executions pass complete retained-result comparison through the shared
native engine. The new nested-pivot coverage contributes 3,587 checks: 2,871
complete value/resource proofs, 716 expected denials and 468,657 row comparisons.
These are correctness and availability results, with no comparative speed claim.

The [implementation contract](../architecture/native-nested-pivot-state-2026-10-06.md)
defines static List, FixedSizeList and Struct index, domain and selected-value
roles in the existing sparse pivot owner. Current source builds contain this
addition; published v0.4.0 packages predate it. Hosted integration is pending.

## Frozen identities

| Artifact | Identity |
| --- | --- |
| Runtime source commit | `750b3783005786851337d06364eb982c6bda5add` |
| Runtime source tree | `74db17fd3988c05be2bbd5f16cdf5c991cac3248` |
| Native executable SHA-256 | `2cde0abb05aeff2e6033365e8844f1be080d0df074489be38919a8f323fcaede` |
| Source-check manifest SHA-256 | `cd104591222bcb780e4ee4296f52661ab72e29ed7aeba6e7c6ce244b84e20703` |
| Acceptance packet | [native nested-pivot packet](evidence/native-nested-pivot-state-2026-10-06.json.xz) |
| Packet compressed size / SHA-256 | 44,500,632 bytes / `6d94bc2e072d97cd6e3f5cbdc9b296cee33960ccaf06d6f1c563f4a17bec4aa4` |
| Packet uncompressed size / SHA-256 | 3,357,227,528 bytes / `34e367cb90eec82ec5b005a8b1b535096535d50af04584e5168e546e43377b05` |

The release build uses Rust 1.99.0 and
`shardloom-cli/release-user-surfaces`. Its 910 frozen source assets include
Rust, Python, Cargo files and both compiled literal fixture files. The packet
retains source, executable and oracle identities, verification drivers, exact
commands, raw results and failed observations. Complete compressed/uncompressed
hash readback passes. The separate [streaming packet inspection](evidence/native-nested-pivot-state-inspection-2026-10-06.json)
also passes: it reopens all 54,733 public envelopes, 367 direct envelopes and
129 native Full43 logs, checks exact case/row/schema/resource counts and finds
no fallback or private-path violations. Its 86 parser-contract cases pass.

Twenty-seven source gates pass: formatting, strict default/native Clippy,
workspace/native/Python tests, feature and Rust 1.96 checks, harness/storage
tests, API and documentation contracts, and website audit/build/check/assets.
The optional-dependency Python run passes all 599 tests with no skips.
The 145 admitted-semantics stages and nine golden workflow stages also pass.
Configurations and repeated checks overlap; these counts are not distinct-test totals.

The final [documentation integration record](evidence/native-nested-pivot-state-integration-2026-10-06.json)
records twelve passing documentation/website gates on `3b86cc31`, unchanged
hashes for all 910 runtime assets, and desktop/mobile Field Guide review. The
mobile table scrolls within its container without widening the page. Hosted
integration remains a separate step.

## Complete public workflows

| Family | Checks |
| --- | ---: |
| Base relational workflows | 797 |
| Unary composition | 1,166 |
| Nested composition and retained state | 5,214 |
| Dynamic scalar and static nested pivot | 4,433 |
| Typed payloads, keys, expressions, unary state and reductions | 11,513 |
| Typed memory and source-free workflows | 909 |
| Analytic frames | 2,213 |
| Scalar-value subqueries | 1,128 |
| Total | 27,373 |

Before execution, independent expectations freeze five nested sources, 189
positive declarations, 62 negative declarations and two Python declaration
denials. SQL and DataFrame coverage includes repeated collection, five result
containers, correlated pivot discovery, downstream composition, names/collisions,
NULLs, empty schemas, fill and selected-scope margins. Every successful nested
terminal rechecks complete values and resource admission. The packet retains
1,892 exact native-schema proofs and 54,733 public raw envelopes; 367 separate
direct-workflow envelopes are also retained.

A 65,537-row nested result exceeds small collection admission and is completely
written and reopened through all seven representable outputs. Vortex and Arrow
IPC readback preserve the logical nested dtype and child nullability. CSV is
explicit JSON-text translation; nested ORC output is denied before publication.
NumPy, pandas and PyArrow serve input/result-container boundaries without
executing query residuals.

## Semantics, resources and failure evidence

Index/domain identity uses existing exact keys, including finite floating-bit
identity. Duplicate-cell equality and MIN/MAX use the existing recursive value
comparator. Native tests cover dictionary/chunked inputs, signed zero, hidden
NULL children, exact typed leaves and literal domain names. Python `pivot()`
and `pivot_table(aggfunc="first")` keep their `first_unique` alias; explicit
SQL `first` keeps the first complete value, including NULL.

Selected cells retain compact native payloads. Tests verify that unchanged cells
avoid retained copies, replacements charge overlapping owners, output credits
survive producer/session drop, and cancellation, changed sources, failed
consumers and failed publication release owned state. Public collection and
writers use a 1-GiB grant; conversion helpers use their explicit 4-GiB default.
These named reservations do not establish complete reader/codec accounting or
a process-RSS ceiling. Pivot spill, nested SUM/MEAN, non-NULL nested fill and
nested-index margins remain denied.

The first Full43 preflight stopped before queries at the unchanged log-admission
limit. Verified compaction of one completed historical cohort retained all 516
original JSON/companion members and recovered 3,362,816 accounted bytes. Failed
and incomplete observations remain intact. The continuation verified the frozen
build and all source hashes, retained the passed semantic/golden stages, and ran
the entire public, direct and Full43 matrices. Finalization reopened every
archived member. Storage ceilings were not raised.

## Full43 and claim boundaries

All 43 queries run three times through the public native SQL workflow. Every
complete result matches the retained native reference, which establishes
regression correctness; independently specified small and large nested fixtures
establish this feature's semantics. Q1's three footer-count proofs retain
99,997,497 rows with no scan, decode, row materialization or answer cache.

The retained 112-column Vortex input is 15,682,956,489 bytes, SHA-256
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
The query file SHA-256 is
`4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b`;
the reference packet SHA-256 is
`cb83c770674073f31ad0c4224f02faeb1ba557c512595a5c2d36f92bb2344b39`.
Source residency, generation and full hashes were verified. This cohort reuses
the retained native input and performs no fresh ingest.

Full43 uses a 24-GiB admission policy and maximum parallelism of 12 on the
local macOS arm64 host; the policy is distinct from physical memory or measured
RSS. Native builds, tests and queries ran serially under the existing guards.
Source prehashing, uncontrolled OS cache and ordinary desktop activity preclude
a cold-cache claim. There is no paired timing comparison or external-engine
execution.

Core-local status is `passed_core_local`; hosted review/integration remains
pending. Broader adapters, reader/codec accounting, state spill and recovery
remain open. This packet does not certify package publication, production
support, competitive superiority or the prior modular workload harness with
this binary. CG-1 through CG-23 retain their own obligations. Real Vortex
payload evidence remains distinct from placeholder artifact status.
