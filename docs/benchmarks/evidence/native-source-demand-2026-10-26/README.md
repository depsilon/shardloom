<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local Parquet source-demand evidence

The [acceptance report](../../native-source-demand-2026-10-26.md) explains the
implemented boundary, prospective thresholds, two native candidates, corrected
347-case result and limitations. The source change is locally accepted and has
not been merged or released.

- [manifest.json](manifest.json) maps original paths to bounded,
  content-addressed archive objects and records excluded large-artifact identities.
- [packet.tar.xz](packet.tar.xz) retains 6,667 compact files: exact source/harness
  snapshots, immutable inputs and decisions, worker requests/receipts, complete
  outputs, independent references, build/test/package records and failures.
- [replay.json](replay.json) verifies 873 distinct complete observations: 39
  original focused observations, then 21 focused, six seed-regression, 347 broad
  and 43 Full43 observations for each candidate.
- [clean-source-replay.json](clean-source-replay.json) records matching replay
  from a fresh source archive with the verifier overlay under isolated Python.
- [corruption-checks.json](corruption-checks.json) records 11 rejected changes,
  including a wrong complete result with updated output hashes.
- [cleanup.json](cleanup.json) records exact retirement of completed generated
  artifacts after replacement and portable verification.
- [post-retirement-replay.json](post-retirement-replay.json) confirms all 873
  results replay unchanged after the 54 retired paths are absent.

From the repository root with Python 3.10 or later:

```bash
python scripts/verify_native_source_demand_packet.py \
  docs/benchmarks/evidence/native-source-demand-2026-10-26
```

Replay executes no queries, imports no archived code and does not require the
original absolute paths. It checks complete typed values, duplicate/order/null
contracts, request/runtime identities, physical demand, frozen benefit rules,
installed-package proof and Full43 results. It is an integrity and correctness
replay, not independently signed execution provenance.

The slower first Parquet checkpoint and the prepared-input validation regression
remain visible. The original broad Polars pass was not repeated; its Arrow IPC
input and unavailable engine clock remain explicit. Large data, executables,
wheels, environments and compiler caches are excluded from the archive.
