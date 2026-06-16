<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Local Source And Package Release Track

Status: selected local/source/package v1 release track, approved pending channel publication proof
and post-release verification.

Schema marker: `shardloom.v1_local_source_package_release.v1`.

Validate with:

```powershell
python scripts\check_v1_local_source_package_release.py
```

This page narrows the feasible v1 release after excluding real production environments. Maintainer
approval exists for the v0.1.0 GitHub pre-release, TestPyPI, PyPI, and Homebrew sequence. This page
does not itself publish packages, create tags, create GitHub releases, upload artifacts, sign
artifacts, add secrets, run production services, or authorize fallback execution.

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
| Homebrew tap | Selected public CLI package channel after GitHub release assets and formula proof. | `docs/release/package-channel-readiness-matrix.json`, Homebrew tap formula proof |

## Publication Sequence Still Required

Publication approval is recorded, but channel proof still gates release claims. The logical channel
order is:

1. Merge the v0.1.0 release-prep source revision.
2. Create the GitHub v0.1.0 release and attach source, wheel, sdist, CLI, checksums, SBOM, and
   provenance assets.
3. Publish TestPyPI through Trusted Publisher/OIDC and run the clean registry install/uninstall
   smoke transcript.
4. Commit or otherwise attach the TestPyPI proof reference required by the PyPI workflow.
5. Publish PyPI through Trusted Publisher/OIDC and run the clean registry install/uninstall smoke
   transcript.
6. Publish the Homebrew tap formula against the immutable GitHub v0.1.0 source archive and run
   `brew install`, `shardloom status --format json`, and `brew uninstall` proof.

The remaining publication proof requires:

- release version and tag
- selected channels: GitHub pre-release, TestPyPI, PyPI, Homebrew
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
