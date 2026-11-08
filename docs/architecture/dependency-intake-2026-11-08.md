<!-- SPDX-License-Identifier: Apache-2.0 -->

# Calendar, transport and CI patch intake

Status: all fifteen local validation stages passed after the completed
[sparse/quantized vector integration](native-vector-representations-2026-11-07.md).
The accepted starting commit is `a157a5ff86c50ff253300848950fed207ce3d72d`.
This maintenance unit prepares the existing small dependency proposals. During
its validation, the maintainer made coordinated Vortex/Arrow review and upgrade
the next active work, followed by the remaining Dependabot PRs and then item 19's
shared exact vector expressions. Preserve this in-flight validation as local
evidence; final integration follows that updated order. It does not close item 19, item 20,
RFC 0044 or CG-1 through CG-23.

| Proposal | Reviewed change | Boundary |
| --- | --- | --- |
| [#1665](https://github.com/depsilon/shardloom/pull/1665) | CodeQL action 4.39.1 to 4.39.3, pinned from `9b3254085494b55a0f7c4c2b3d2d52cc712ce370` to `33d7b85da1a04171e211befe2c739303b9a4c066` | Three action references; permissions, triggers, inputs and query configuration stay the same. |
| [#1667](https://github.com/depsilon/shardloom/pull/1667) | tonic-prost 0.14.8 to 0.14.9 | Existing optional encoded-exchange prerequisites; no remote service is enabled. |
| [#1668](https://github.com/depsilon/shardloom/pull/1668) | Jiff/jiff-static 0.2.38 to 0.2.39; jiff-core 0.1.0 to 0.1.3 | Existing calendar provider; the separately pinned timezone database and full native tick storage remain unchanged. |

## Source and compatibility decisions

The review compares all members of eight checksum-verified published crate
archives. Tonic-prost's Rust source is unchanged; its minimum tonic version
becomes 0.14.9, already selected here. Jiff-core fixes offset subtraction and
nanosecond-constructor bounds and changes clock decomposition. Jiff also changes
duration formatting, optional serde handling, borrowed timezone names and calendar
addition. ShardLoom retains checked seconds/fraction timestamp construction and
explicit embedded TZif data from jiff-tzdb 0.1.6 (2026a).

Clock decomposition reaches the existing `BoundTimeZone::civil` boundary. Add
independent regressions against ShardLoom's integer Gregorian conversion across
every second on both sides of the epoch, declared fixed offsets, all temporal
units, fractional boundaries and leap/non-leap-century dates. Preserve existing
range, gap/fold, half-hour transition, native persistence and installed-package
calendar tests. Direct ShardLoom call-site exclusions do not establish that an
affected Jiff API is unreachable through Vortex.

The CodeQL review binds 304 collected files to complete Git trees at the two
exact action commits. It covers selected runtime sources, manifests, action
metadata and the unchanged MIT license. Sixty generated upstream workflow
snapshots and the generated JavaScript execution bundle were excluded. This is
selected-source review, not a rebuilt-bundle proof or a security audit. The final
combined head must pass its own hosted CodeQL and CI checks.

The new crate versions retain the reviewed permissive license alternatives.
Regenerate the complete notice bundle from the combined graph. Preserve the
tonic-prost root license at its exact packaged VCS revision and its source-level
copyright notices. The prior and candidate legal text blobs are unchanged;
version/checksum records and two upstream provenance URLs need reconciliation.
The resolved graph, rather than that preparation alone, determines the final
notice inventory.

## Provider proposals promoted to next active work

[#1664](https://github.com/depsilon/shardloom/pull/1664) mixes Vortex 0.91 with
the current 0.90 zstd path and leaves six retained provider patches unused. Its
original builds also expose changed allocator and error-macro APIs. All nine
current Vortex patches require coordinated review before a provider upgrade.
[#1666](https://github.com/depsilon/shardloom/pull/1666) mixes four Arrow 60
crates with the retained Arrow/Parquet 59 graph. The Vortex 0.91 manifest itself
still requests Arrow/Parquet 59.2. Preserve the existing
[coordinated-major policy](dependency-intake-2026-11-02.md#deferred-arrow-major-upgrade);
neither partial proposal is incorporated, and no conversion bridge is introduced.
The November 8 maintainer instruction supersedes the earlier scheduling deferral:
review both updates for useful changes and carry out a coordinated upgrade before
resolving the remaining Dependabot work. It does not accept either original mixed
graph or waive the retained patch, provider, ownership and compatibility checks.

## Acceptance and evidence

Preserve all five original proposal heads and complete check/job observations,
the twelve selected failure logs, source comparisons and preparation corrections.
The patch proposals' generated API identities and dependency notices were stale;
regenerate both from the combined source. Existing aggregate failures are not
independent runtime diagnoses. Preserve every new failed attempt separately.

Preparation limitations remain explicit: the first archive inventory was
normalized after collection without retaining its original bytes; some original
reader-error output and per-file CodeQL connector envelopes were not retained.
The primary checks independently bind the retained archives, decoded source bytes,
complete Git trees and corrected comparisons. An initial notice draft truncated
one checksum; the corrected inventory and original failure record are retained.
These are dependency preparation observations, separate from the accepted vector
packet's ten original failures.

- [x] Preserve proposal ancestry and verify the complete combined dependency graph.
- [x] Add independent calendar regressions and reconcile notices/API identities.
- [x] Pass focused tests, formatting, strict lint, default/native/optional-graph
      tests, seven feature configurations and supported Rust 1.96 checks.
- [x] Run the unchanged audit policy and record every actual advisory result.
- [x] Verify the pre-upgrade native runtime, canonical Python core shard and ordinary
      installed-wheel proof with no installed-test skips.
- [ ] Bind accepted checks to the same complete source snapshot and replay a
      portable packet, including corruption and relocated controls.
- [ ] Pass final-head hosted checks and review, merge, verify exact main,
      reconcile proposals and retire superseded generated artifacts.

The implementation reuses the admitted calendar and optional transport providers;
it creates no execution abstraction, input format, fallback engine or remote
effect. Vortex-native input/output and all current provider patches remain intact.
PulseWeave, capillary scheduling, dynamic admission, metadata-first execution and
route timing do not change in this dependency unit. Existing ownership, storage
and process safeguards still govern validation. There is no new benchmark,
performance claim or package publication.

Preparation replay verifies eight archives and all 308 members, four complete
comparisons, five original proposals and 304 selected CodeQL source files. The
complete notice bundle covers 433 registry packages and 15 vendored packages,
with all 250 legal-text blobs unchanged. Independent transfer inventories agree
on all 426 preparation files. Two local selection attempts stopped before
transfer: one parsed a comment-bearing upstream configuration as JSON, and the
next correctly refused to overwrite a late-arriving inventory. Both observations
and the corrected transfer remain recorded; the first error has a tool-output
transcription rather than a separately retained complete command envelope.

Before validation, guarded retirement removed 11,688 completed debug compiler
files among 13,737 entries after repeated identity and closed-handle checks.
Current inputs, the accepted vector runtime/wheel and release cache remained
unchanged. No storage threshold changed and no payload backup was created.

All fifteen stages of the saved `validation-protocol-r1.json` completed with
passing outcomes. These observations bind the small dependency patch source,
before the Vortex/Arrow provider changes. They remain a distinct baseline; the
provider integration must repeat the required acceptance roles against its final
source and dependency graph. Hosted integration and proposal reconciliation are
still pending under the maintainer's updated order.
