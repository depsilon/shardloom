<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Local Source And Package Release Track

Status: selected local/source/package v1 release track, ready pending the final publication event.

Schema marker: `shardloom.v1_local_source_package_release.v1`.

Validate with:

```powershell
python scripts\check_v1_local_source_package_release.py
```

This page narrows the feasible v1 release after excluding real production environments. It does not
publish packages, create tags, create GitHub releases, upload artifacts, sign artifacts, add
secrets, run production services, or authorize fallback execution.

## Selected V1 Track

| Workstream | V1 decision | Evidence owner |
| --- | --- | --- |
| Source checkout install | Supported local path. | `docs/getting-started/source-checkout-install.md`, `scripts/release_dry_run_proof.py` |
| Local wheel/sdist proof | Required before publication. | `target/release-dry-run-proof/transcript.json`, `python/dist/*` |
| Python user-surface proof | Required local smoke and scenario proof. | `examples/local-python-smoke/run.py`, `examples/local-python-benchmark-scenarios/run.py`, `examples/local-python-benchmark-scenarios/timing_review.py` |
| API/schema stability | Stable local v1 machine-readable contract. | `docs/release/v1-api-schema-stability.md`, `docs/release/schemas/v1/*`, `scripts/check_v1_api_schema_stability.py` |
| Local benchmark publication | Scoped full-local evidence only. | `website/assets/benchmarks/latest/manifest.json` |
| Docs/website/readme | Claim-safe public interpretation layer. | `README.md`, `docs/release/public-status-matrix.md`, `website-src/` |
| GitHub pre-release | Selected package channel after approved tag/release object/assets. | `docs/release/package-channel-readiness-matrix.json`, `target/release-provenance-dry-run/github-prerelease-assets/asset-manifest.json` |
| TestPyPI | Selected rehearsal channel after Trusted Publisher and approval. | `.github/workflows/pypi-publish-draft.yml`, `scripts/python_registry_package_proof.py` |
| PyPI | Selected public Python package channel after TestPyPI proof and approval. | `.github/workflows/pypi-publish-draft.yml`, `scripts/python_registry_package_proof.py` |

## Final Publication Event Still Required

The final event is the only remaining non-environment release action. It requires maintainer
confirmation of:

- release version and tag
- selected channels: GitHub pre-release, TestPyPI, PyPI
- exact source revision
- release notes
- checksum, SBOM, provenance, and signing/attestation policy
- rollback, yank, delete, or advisory plan per channel
- clean release gate evidence at the selected revision

Until that final confirmation exists:

```text
publication_attempted=false
tag_created=false
package_upload_attempted=false
public_release_claim_allowed=false
public_package_claim_allowed=false
```

## Deferred Environment Gates

These gates are intentionally not part of the v1 public package release because the real service
environments are unavailable:

- production object-store claim
- production table/lakehouse claim
- production distributed claim
- production live/hybrid claim
- real Foundry integration claim

They remain explicit fail-closed claim gates. Local fixtures and reports may stay in the product as
evidence of scoped behavior, but they must not become production/platform claims.

## Claim Boundary

The release may be described as a source-checkout and package-installable technical preview only
after the selected package channels are actually published and verified. Local benchmark artifacts
may be published as scoped local evidence, but `performance_claim_allowed=false` remains in force
unless a separate benchmark claim gate is approved.

No selected channel may introduce Spark, DataFusion, DuckDB, Polars, pandas, Dask, Velox, Trino, or
another external query engine as a ShardLoom runtime fallback.
