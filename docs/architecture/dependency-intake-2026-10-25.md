<!-- SPDX-License-Identifier: Apache-2.0 -->

# Consolidated dependency follow-up after provider refresh

Status: accepted and merged in combined PR #1605 at
`167ac248ba662ae7967a7801995e9b160b17bae5` after all 57 checks passed.
The maintainer requests that all category and
dependency PRs be merged or closed, with needed dependency work retained in the
active queue. Provider PR #1601 is merged. The four new bot PRs below are closed
and consolidated here. Public naming PR #1604 is now merged; full-ingest
ownership/preparation is accepted in the same merge. The October 25
maintainer first places this maintenance immediately after ingestion, then
explicitly combines both in PR #1605. Complete the combined validation and merge
before [complete-workflow work elimination](native-workflow-work-elimination-2026-10-25.md),
then the [batch/window and shared CPU speed pass](native-ingest-batch-window-scheduling-2026-10-25.md)
and connected programming. The next-release deadline does not
defer this active maintenance.

| Closed proposal | Accepted baseline | Combined intake | Disposition |
| --- | --- | --- | --- |
| [#1598](https://github.com/depsilon/shardloom/pull/1598) | Tokio 1.53.3 | Tokio 1.54.1 | Source/license review and combined local validation complete |
| [#1599](https://github.com/depsilon/shardloom/pull/1599) | prost/prost-derive 0.14.3 | 0.14.4 | Source/license review complete; applied with the same existing prost 0.13.5 compatibility graph |
| [#1600](https://github.com/depsilon/shardloom/pull/1600) | Existing pinned workflow actions | rust-cache, setup-python, CodeQL and PyPI publishing action updates | All four move forward from verified pins; 29 uses and 16 workflow-contract lanes pass local verification |
| [#1602](https://github.com/depsilon/shardloom/pull/1602) | One Arrow/Parquet 59.3.1 graph | Selected Arrow packages at 60.0.0 | Blocked until the complete Vortex/Arrow/Parquet/Python graph is compatible |

PR #1600 is based on `d593da49`, an ancestor of current provider merge
`3c3e2f14`. Its CodeQL comparison starts at `6f2ac81f`. Direct source checks
at both that merge and the active branch confirm `6f2ac81f` for init, analyze
and Scorecard upload; the earlier intake text's `cdf488f5` claim was incorrect.
Assess proposed `9b325408` against the verified current pin. The remaining
proposed action pins are rust-cache
`c8037a86`, setup-python `6d4fd22b` (7.1.0), and PyPI publishing
`7245906f`. Exact full SHAs, current values, original diffs and closure receipts
are retained in the [warm closeout packet](../benchmarks/evidence/native-warm-source-reuse-2026-10-25/manifest.json).

The Arrow proposal updates array, Avro, Flight, PyArrow and schema dependencies
while IPC and the admitted provider graph remain on 59.3.1. Its lockfile adds
parallel Arrow major versions. This does not establish a compatible boundary
upgrade; do not introduce conversion bridges just to merge the bot proposal.
The earlier #1582 decision remains in force. A later coherent major intake
requires provider, capsule, schema, license/provenance and consumer validation.

## Reviewed dependency delta

The constrained lock update changes exactly three of 452 package identities:
Tokio, prost and prost-derive. It adds no package names and leaves Arrow/Parquet
59.3.1, Vortex 0.90.0, prost-types 0.14.3 and the existing ORC-side prost 0.13.5
graph unchanged. All unchanged package checksums match the preceding lockfile.

[Tokio 1.54.0](https://github.com/tokio-rs/tokio/releases/tag/tokio-1.54.0)
changes scheduling, readiness, cooperative I/O and several zero-capacity
preconditions; [1.54.1](https://github.com/tokio-rs/tokio/releases/tag/tokio-1.54.1)
reverts the timer-handle change. The local source review finds no calls to the
changed zero-capacity Tokio constructors. Existing Flight and Vortex I/O users
still require native integration coverage; absence of those constructors does
not prove the whole update. [Prost 0.14.4](https://github.com/tokio-rs/prost/releases/tag/v0.14.4)
includes generated enumeration validation/default fixes and a const validity
method. Both releases declare Rust 1.85, below ShardLoom's declared 1.96 minimum.

Exact registry archives match the three proposed lockfile checksums. Their
manifests and license texts are independently reopened from those archives:
Tokio is MIT; prost and prost-derive are Apache-2.0. Regenerated notices cover
all 437 locked registry packages and eight vendored components, with 827 file
records and 248 unique texts. Only the three component versions/checksums
change; the notice texts are unchanged.

## Reviewed workflow delta

| Action | Previous full pin | New full pin | Local impact |
| --- | --- | --- | --- |
| [rust-cache](https://github.com/Swatinem/rust-cache/compare/f0d9c3887740aee45f6153b24b3a6b815192ec16...c8037a86dde8214a8310e9f54a8fc61ab362aa31) | `f0d9c3887740aee45f6153b24b3a6b815192ec16` | `c8037a86dde8214a8310e9f54a8fc61ab362aa31` | Ten uses; preserves inputs and Node 24 entrypoints, includes the virtual-workspace root-manifest cache-key fix. |
| [setup-python](https://github.com/actions/setup-python/compare/5fda3b95a4ea91299a34e894583c3862153e4b97...6d4fd22b20465ccc0f7354476588e8df7c9ca0e0) | `5fda3b95a4ea91299a34e894583c3862153e4b97` | `6d4fd22b20465ccc0f7354476588e8df7c9ca0e0` | Fourteen uses; version 7.1.0 adds optional mirror inputs, unused here, while retaining the Node 24 entrypoints. |
| [CodeQL](https://github.com/github/codeql-action/compare/6f2ac81f5cd81fe351a197ba0b00d91d03cd2aee...9b3254085494b55a0f7c4c2b3d2d52cc712ce370) | `6f2ac81f5cd81fe351a197ba0b00d91d03cd2aee` | `9b3254085494b55a0f7c4c2b3d2d52cc712ce370` | Init, analyze and Scorecard SARIF upload remain aligned; existing inputs and Node 24 entrypoints are preserved. |
| [PyPI publish](https://github.com/pypa/gh-action-pypi-publish/compare/dc37677b2e1c63e2034f94d8a5b11f265b73ba33...7245906fb61fa33a13554f9d5fb856fe61758086) | `dc37677b2e1c63e2034f94d8a5b11f265b73ba33` | `7245906fb61fa33a13554f9d5fb856fe61758086` | Two protected publishing uses; action inputs and Docker entrypoint stay unchanged. Runtime dependency files change and the Dockerfile adds the pip-tools configuration. |

All four comparisons descend from the accepted pins, with no commits behind.
The fourteen exact endpoint action/Docker files are verified against their Git
blob identities. Review covers upstream change records, local input use and
entrypoint compatibility; it is not an exhaustive audit of bundled JavaScript
or third-party dependency implementations. Workflow triggers, permissions and
publication authorization gates are unchanged.

The ownership measurements at `20f03396` predate this dependency update. Their
source/build identity and failed speed screen remain intact; combined-build
validation must be recorded separately. This maintenance preserves native
ownership and no-fallback policy and does not publish a package.

Execution checklist:

- [x] Review Tokio/prost release notes, licenses and the exact lockfile changes;
  retain one compatible dependency graph and regenerate package-license data.
- [x] Compare the four proposed workflow pins with current main and upstream
  action changes, then update all accepted pins in one coherent batch.
- [x] Run formatting, strict workspace lint/tests, native provider and MSRV
  checks, installed-package proof and affected workflow validation.
- [x] Merge the combined PR #1605 after checks and review; record its outcome
  in the completed ledger. The [CI acceptance](../release/ci-performance-2026-10-25.md#completed-remote-acceptance)
  binds the final 57 passing checks and unchanged reviewed head.

Arrow 60 stays blocked outside the executable checklist until the coherent
provider/format/Python prerequisite is satisfied. The next outcome is a
source-reviewed maintenance batch, not a promise to accept every proposed pin.

## Combined local acceptance

At `23807cb7`, all nine selected Rust/default/native/MSRV gates pass, including
9,364 passing test executions and 35 declared platform/opt-in exclusions.
The installed Mac wheel passes 140 native integration tests without skips.
One isolated combined-runtime Full43 passes complete values and schemas at
46.973255495 engine seconds. The
[acceptance record](../benchmarks/native-ingest-ownership-2026-10-25.md#combined-runtime-acceptance)
and its portable packet bind source bytes, logs, frozen binaries, package
identities and both historical and combined observations.

Cargo deny's license/advisory/ban/source checks and Cargo audit pass. Existing
paste, anyhow and event-listener warnings remain in the raw logs; the intake
adds no waiver and does not claim an exhaustive dependency security audit.
The workflow diff changes only the four reviewed pins and setup-python's
version comment. Triggers, permissions, inputs and publication gates are
unchanged. Final GitHub checks and merge remain a separate integration step.
