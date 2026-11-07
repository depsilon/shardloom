<!-- SPDX-License-Identifier: Apache-2.0 -->

# Compression and transport patch updates

Status: combined local acceptance passed; hosted review and integration pending.

This maintenance batch reconciles three existing dependency proposals before
the rich-data queue resumes. Preserve their original commits and validate one
combined graph with the managed binary-object implementation.

| Proposal | Previous version | Combined version |
| --- | --- | --- |
| [#1648](https://github.com/depsilon/shardloom/pull/1648) | flate2 1.1.9 | 1.1.10 |
| [#1647](https://github.com/depsilon/shardloom/pull/1647) | tonic 0.14.8 | 0.14.9 |
| [#1646](https://github.com/depsilon/shardloom/pull/1646) | tonic-prost 0.14.6 | 0.14.8 |

Flate2 selects miniz_oxide 0.9.1; the existing 0.8.9 remains for other format
consumers. Tonic's optional Hyper minimum becomes 1.10, which the existing
locked Hyper 1.12.0 satisfies. Tonic-prost's tonic minimum becomes 0.14.8,
which the combined 0.14.9 satisfies. Arrow, Parquet and Vortex remain unchanged.
These are existing compression and explicit transport boundaries; they add no
input format, execution provider, fallback engine or enabled remote effect.

## Source and compatibility review

The exact published archives and manifests are checked against the immutable
proposal lockfiles. Complete published-source comparisons cover every archive
member; a truncated GitHub comparison is retained as supplemental evidence only.
Flate2 adjusts feature routing, introduces runtime detection in its default
Rust backend and refuses incomplete deflate input. ShardLoom's gzip reader uses
the existing multi-member decoder. The affected tests must preserve complete
UTF-8/null values, member boundaries, checksum/truncation rejection, bounded
resources, cancellation and source-generation checks.

Tonic adds optional HTTP/2 reset limits and enforces configured HTTP/1 timeouts.
ShardLoom does not set the new reset limits. Tonic-prost implements codec
defaults without unnecessary generic bounds. These dependencies remain behind
the existing opt-in encoded-exchange prerequisite feature, outside every
default/release group. The rejected experimental transport stays removed;
this intake does not restore it or claim a public remote service. Compile the
complete optional graph and preserve the existing Flight codec controls.
No upstream implementation code is copied into ShardLoom, and no new adapter
abstraction is introduced.

The dependencies declare MIT, Apache-2.0 or Zlib alternatives compatible with
the existing intake policy. The regenerated notice bundle preserves exact
license and copyright text for all 435 registry and 13 vendored components.
Tonic-prost omits its root license from the archive; the bundle includes the
root license at its packaged VCS revision and its new source-level copyright
notice with explicit provenance. All four miniz_oxide legal files are included.

## Original failures and scope boundaries

The three patch proposals fail generated API identity and dependency-notice
checks. Regenerate both from the combined manifests and lockfile. Their Python
aggregate and package/readiness failures include dependent failures, not
independent runtime diagnoses.

The #1647 provider I/O job also fails before tests run because the hosted
filesystem runs out of space while compiling vortex-array. Its complete log
and downloaded artifact identity are preserved. The combined graph must pass
the provider checks; the earlier failure is not acceptance evidence.

The partial Arrow 60 proposal [#1642](https://github.com/depsilon/shardloom/pull/1642)
remains deferred under the [coordinated provider review](dependency-intake-2026-11-02.md#deferred-arrow-major-upgrade).
Do not introduce conversion bridges or a partial major-version upgrade to
make that proposal pass. Close it as deferred when the open-PR reconciliation
finishes.

## Acceptance

The [portable packet](../benchmarks/evidence/dependency-followup-2026-11-06/README.md)
binds the three changed Cargo files to every accepted validation stage and the
installed package. Runtime implementation, provider ownership and public API
behavior are unchanged. The source comparisons cover every member of each
published archive; the original failed proposal checks and intake corrections
remain available.

- All 15 focused streaming-text tests pass, including multi-member gzip,
  truncation/checksum rejection, resource limits and cancellation.
- Default workspace tests pass 3,319 cases. The native and optional prerequisite
  graph passes 6,947 cases, with 37 existing opt-in tests ignored. Strict default
  and native lint, format, seven feature-matrix stages and Rust 1.96 checks pass.
- The core Python shard passes with 833 reported test runs and 47 skip records;
  its 1,175 discovered cases are retained separately, not reported as executed
  passes. The installed macOS arm64 wheel passes all 319 integration tests
  without skips, plus clean installation, identity and removal checks.
- Offline replay validates all 441 archive members, complete published-source
  comparisons, the full notice bundle and the saved validation results. It does
  not rerun runtime tests or benchmarks.

The dependency audit passes its configured gate. It retains the same three
informational warnings and exact package versions as the accepted November 2
batch: paste 1.0.15 is unmaintained (RUSTSEC-2024-0436), and anyhow 1.0.102
(RUSTSEC-2026-0190) and event-listener 5.4.1 (RUSTSEC-2026-0221) carry unsoundness
advisories. This three-proposal intake does not remediate those unchanged
dependencies or establish that their affected APIs are unreachable. The
original and current audit logs remain in the evidence; no advisory-free
claim follows from a passing command.

- [x] Review immutable proposal heads, complete archive changes and manifests.
- [x] Reconcile exact license/copyright texts and generated API identities.
- [x] Pass focused compression controls and default/native/optional-graph checks.
- [x] Pass the supported Rust version, dependency audit and installed-wheel proof.
- [x] Preserve original failures and validate portable evidence.
- [ ] Pass final-head hosted checks and review, merge, then verify main.
- [ ] Reconcile incorporated proposals and retire superseded generated artifacts.

This maintenance work does not require a new benchmark or package publication.
