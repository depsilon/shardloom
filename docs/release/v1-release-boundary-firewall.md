<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Release Boundary Firewall

`shardloom.v1_release_boundary_report.v1` is the fail-closed release-boundary
gate for the current finished-product v1 bundle. It does not publish packages,
create tags, upload artifacts, or authorize production/performance claims.

```text
python scripts/check_v1_release_boundary.py
target/v1-release-boundary-report.json
public_release_claim_allowed=false
public_package_claim_allowed=false
performance_claim_allowed=false
production_claim_allowed=false
spark_replacement_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

The gate validates the v1 support envelope, generated support page header,
package-channel matrix, local release dry-run transcript, package-channel
readiness report, package metadata, public claim-language scan, public status
docs, v1 docs productization, and v1 inclusion-scope matrix.

The current v1 support envelope is local and source-checkout first:

- local first-10-minutes smoke and Python examples
- scoped CLI and Python source/local Vortex front doors
- local CSV, JSON/JSONL/NDJSON, generated rows, local Vortex, and feature-gated
  flat scalar compatibility formats
- local inline JSONL/CSV, feature-gated local compatibility exports, and local
  Vortex writes

The gate keeps these families blocked or candidate-scoped until their owning
phase-plan item closes with real runtime, safety, release, and no-fallback
evidence: public package channels, production readiness, performance
superiority, Spark displacement, broad SQL/DataFrame parity, object-store,
lakehouse/table, Foundry, distributed, live/hybrid, and arbitrary
UDF/plugin/effect execution.
