<!-- SPDX-License-Identifier: Apache-2.0 -->

# Use Case Reference Backlinks

This file is the central backlink ledger for source-of-truth docs referenced by the Use Case Atlas.
It avoids turning every reference document into a second active queue while still making the
relationship auditable.

| Reference file | Related use cases |
| --- | --- |
| `README.md` | `first-10-minutes-local-smoke`, `python-wrapper-client-smoke`, `python-local-csv-query-builder-smoke`, `sql-local-source-csv-smoke`, `table-lakehouse-boundary-report`, `package-channel-readiness-boundary` |
| `docs/getting-started/first-10-minutes.md` | `first-10-minutes-local-smoke`, `python-wrapper-client-smoke`, `package-channel-readiness-boundary` |
| `docs/getting-started/examples.md` | `first-10-minutes-local-smoke`, `local-file-etl-cleanup-smoke`, `python-local-csv-query-builder-smoke`, `sql-local-source-csv-smoke`, `messy-data-local-fixtures` |
| `docs/getting-started/certified-local-workload.md` | `local-file-etl-cleanup-smoke`, `compatibility-import-certified-local`, `evidence-audit-claim-gates` |
| `benchmarks/traditional_analytics/README.md` | `compatibility-import-certified-local`, `prepared-native-vortex-runtime-direction`, `messy-data-local-fixtures`, `query-scenario-cookbook-smoke`, `benchmark-interpretation-evidence-not-leaderboard` |
| `docs/architecture/benchmark-suite-catalog.md` | `prepared-native-vortex-runtime-direction`, `messy-data-local-fixtures`, `query-scenario-cookbook-smoke`, `benchmark-interpretation-evidence-not-leaderboard` |
| `docs/architecture/canonical-terminology.md` | `sql-dataframe-capability-posture` |
| `docs/architecture/compute-engine-flow-reference.md` | `local-file-etl-cleanup-smoke`, `compatibility-import-certified-local`, `prepared-native-vortex-runtime-direction`, `python-local-csv-query-builder-smoke`, `sql-local-source-csv-smoke`, `source-free-generated-output-boundary`, `object-store-boundary-report`, `object-store-local-emulator-write-smoke`, `table-lakehouse-boundary-report`, `foundry-local-proof-boundary`, `evidence-audit-claim-gates` |
| `docs/architecture/io-reuse-and-fanout-architecture.md` | `output-result-sink-and-fanout-boundary` |
| `docs/architecture/object-store-request-planner.md` | `object-store-boundary-report`, `object-store-local-emulator-read-smoke`, `object-store-local-emulator-write-smoke`, `local-table-append-commit-rehearsal-smoke` |
| `docs/architecture/operational-evidence-policy-hardening.md` | `evidence-audit-claim-gates` |
| `docs/architecture/phased-execution-completed-ledger.md` | `source-free-generated-output-boundary`, `object-store-local-emulator-write-smoke`, `local-table-append-commit-rehearsal-smoke` |
| `docs/architecture/table-intelligence-layer.md` | `local-table-append-commit-rehearsal-smoke`, `table-lakehouse-boundary-report` |
| `docs/architecture/universal-compatibility-coverage-scoreboard.md` | `object-store-boundary-report`, `table-lakehouse-boundary-report` |
| `docs/architecture/universal-ingress-route-taxonomy.md` | `vortex-ingest-prepare-once-local` |
| `docs/architecture/universal-input-contract.md` | `object-store-boundary-report`, `table-lakehouse-boundary-report` |
| `docs/architecture/vortex-public-api-inventory.md` | `object-store-local-emulator-read-smoke` |
| `docs/benchmarks/local-taxonomy-benchmark.md` | `local-file-etl-cleanup-smoke`, `compatibility-import-certified-local`, `query-scenario-cookbook-smoke`, `evidence-audit-claim-gates`, `benchmark-interpretation-evidence-not-leaderboard` |
| `docs/benchmarks/baseline-comparison-boundary.md` | `compatibility-import-certified-local`, `query-scenario-cookbook-smoke`, `benchmark-interpretation-evidence-not-leaderboard` |
| `docs/foundry/integration-pack-readiness.md` | `foundry-local-proof-boundary` |
| `docs/foundry/proof-of-use-certification.md` | `source-free-generated-output-boundary`, `foundry-local-proof-boundary` |
| `docs/getting-started/install.md` | `package-channel-readiness-boundary` |
| `docs/release/hard-release-readiness-gate.md` | `package-channel-readiness-boundary` |
| `docs/architecture/adoption-commercial-readiness-friction-reduction.md` | `package-channel-readiness-boundary` |
| `python/README.md` | `first-10-minutes-local-smoke`, `python-wrapper-client-smoke`, `python-local-csv-query-builder-smoke`, `sql-dataframe-capability-posture`, `source-free-generated-output-boundary`, `object-store-local-emulator-read-smoke`, `object-store-local-emulator-write-smoke`, `local-table-append-commit-rehearsal-smoke` |
| `examples/local-python-smoke/README.md` | `first-10-minutes-local-smoke`, `python-wrapper-client-smoke` |
| `examples/local-vortex-benchmark/README.md` | `local-file-etl-cleanup-smoke`, `output-result-sink-and-fanout-boundary` |
| `examples/foundry-lightweight-transform/README.md` | `foundry-local-proof-boundary` |
