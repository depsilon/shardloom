<!-- SPDX-License-Identifier: Apache-2.0 -->

# Consolidated dependency follow-up after provider refresh

Status: active maintenance queue. The maintainer requests that all category and
dependency PRs be merged or closed, with needed dependency work retained in the
active queue. Provider PR #1601 is merged. The four new bot PRs below are closed
and consolidated here; closure does not claim that their proposed updates have
been applied or validated. Public naming remains the next implementation item.

| Closed proposal | Current accepted pin | Proposed intake | Disposition |
| --- | --- | --- | --- |
| [#1598](https://github.com/depsilon/shardloom/pull/1598) | Tokio 1.53.3 | Tokio 1.54.1 | Review and test in the next cohesive dependency intake |
| [#1599](https://github.com/depsilon/shardloom/pull/1599) | prost/prost-derive 0.14.3 | 0.14.4 | Review and test with the Rust dependency batch |
| [#1600](https://github.com/depsilon/shardloom/pull/1600) | Existing pinned workflow actions | rust-cache, setup-python, CodeQL and PyPI publishing action updates | Reconcile against current main before applying; do not restore the older CodeQL base |
| [#1602](https://github.com/depsilon/shardloom/pull/1602) | One Arrow/Parquet 59.3.1 graph | Selected Arrow packages at 60.0.0 | Blocked until the complete Vortex/Arrow/Parquet/Python graph is compatible |

PR #1600 is based on `d593da49`, an ancestor of current provider merge
`3c3e2f14`. Its CodeQL comparison starts at `6f2ac81f`, while accepted main
already uses `cdf488f5`; its proposed `9b325408` must be assessed against that
actual current pin. The remaining proposed action pins are rust-cache
`c8037a86`, setup-python `6d4fd22b` (7.1.0), and PyPI publishing
`7245906f`. Exact full SHAs, current values, original diffs and closure receipts
are retained in the [warm closeout packet](../benchmarks/evidence/native-warm-source-reuse-2026-10-25/manifest.json).

The Arrow proposal updates array, Avro, Flight, PyArrow and schema dependencies
while IPC and the admitted provider graph remain on 59.3.1. Its lockfile adds
parallel Arrow major versions. This does not establish a compatible boundary
upgrade; do not introduce conversion bridges just to merge the bot proposal.
The earlier #1582 decision remains in force. A later coherent major intake
requires provider, capsule, schema, license/provenance and consumer validation.

The next dependency intake must review upstream changes and licenses, update
the graph and generated package-license evidence together, and run relevant
native/default, MSRV, installed-package and workflow checks. Keep action SHAs
pinned and preserve the current no-fallback and allocation contracts. Schedule
this maintenance before the next release; it neither publishes packages nor
asserts that any proposal is an urgent security fix. Do not mix these untested
updates into the frozen warm-source observations.

Execution checklist:

- [ ] Review Tokio/prost release notes, licenses and the exact lockfile changes;
  retain one compatible dependency graph and regenerate package-license data.
- [ ] Compare the four proposed workflow pins with current main and upstream
  action changes, then update all accepted pins in one coherent batch.
- [ ] Run formatting, strict workspace lint/tests, native provider and MSRV
  checks, installed-package proof and affected workflow validation.
- [ ] Open and merge the consolidated PR after checks and review; move its
  outcome to the completed ledger. Recheck for additional bot PRs at closeout.

Arrow 60 stays blocked outside the executable checklist until the coherent
provider/format/Python prerequisite is satisfied. The next outcome is a
source-reviewed maintenance batch, not a promise to accept every proposed pin.
