<!-- SPDX-License-Identifier: Apache-2.0 -->

# Ingest ownership and combined dependency evidence

The retained implementation reduces copied buffer bytes by 61.236% in the
complete ingestion observation. Its frozen 3% speed gate **fails** and remains
failed. The maintainer explicitly retains the copying improvement. See the
[complete acceptance](../../native-ingest-ownership-2026-10-25.md#combined-runtime-acceptance).

From any checkout, using Python 3.13 or newer:

```sh
python3.13 -I -B docs/benchmarks/evidence/native-ingest-ownership-2026-10-25/verify.py
```

The standalone verifier reads only this packet. Historical absolute paths are
archive lookup keys; they are never opened or used as extraction destinations.
Replay needs no original dataset, native executable, installed package, network
access or repository helper imports. It does not rerun queries or ingestion.

[manifest.json](manifest.json) binds 735 original files to 592 unique objects
in `evidence.tar.xz`, with exact raw sizes and SHA-256 hashes. The verifier checks
archive membership and bounds before interpreting the retained evidence.
The original inventory plus explicit additions preserve failed and incomplete
observations. Native binaries, environments, compiler caches and datasets are
excluded; their source/build identities and acceptance records remain.

The [recorded replay](replay.json) verifies:

- Both complete ingests, byte-identical output, physical layout and independent
  readback of all 99,997,497 rows, 112 columns and 11,199,719,664 values.
- Copy/transfer conservation, backing-allocation accounting, cleanup and the
  unchanged failed ingestion-speed decision.
- The original ownership validation and final nine combined gates, including
  9,364 passing test executions with 35 declared exclusions.
- The three reviewed package identities, workflow scope, 16 workflow-contract
  lanes and dependency audits with existing warnings preserved.
- Final build/test source equality, installed-wheel identities and 140 passing
  native integration tests with no skips, plus the original receipt mismatch
  and its independently checked resolution.
- Both single-pass Full43s against complete independent values and schemas:
  47.281897502 engine seconds for ownership-only and 46.973255495 for the final
  combined runtime. The prior provider observation is 49.625514041 seconds.

All 21 semantic corruption controls reject forged coverage, failed tests,
fallback execution, altered clocks, changed source or package identities and
changed acceptance decisions. These checks go beyond archive digest rejection.
The earlier portable-verifier metadata corrections are retained as failed
assembly observations, without changing engine or test results.

Separate runs with uncontrolled host/cache state establish no causal speedup.
The Mac package proof does not establish Linux ABI compatibility or authorize
publication. Clean-checkout verification and generated-file retirement are
recorded separately at closeout.
