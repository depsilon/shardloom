<!-- SPDX-License-Identifier: Apache-2.0 -->

# Publication API And Schema Stability Gate

Status: implemented fail-closed report contract for `GAR-0024-A`. This gate does not publish
packages, create tags, sign artifacts, use signing keys, upload artifacts, add secrets, execute
runtime workloads, invoke external engines, or authorize fallback execution.

Schema:

```text
shardloom.publication_api_schema_stability_gate.v1
```

Release-plan fields:

```text
publication_api_schema_gate_status=blocked
claim_gate_status=not_claim_grade
public_release_claim_allowed=false
public_package_claim_allowed=false
api_schema_stability_claim_allowed=false
package_publication_performed=false
tag_created=false
signing_key_used=false
checksum_manifest_publication_grade=false
sbom_publication_grade=false
runtime_execution=false
fallback_attempted=false
external_engine_invoked=false
v1_api_schema_stability_report=target/v1-api-schema-stability-report.json
v1_stable_schema_contract_status=passed
v1_stable_schema_surface_count=11
v1_schema_compatibility_window=v1_additive_compatibility
v1_diagnostic_code_stability_doc=docs/release/diagnostic-code-stability.md
v1_diagnostic_code_count=22
legacy_flat_field_policy=stable_aliases_for_v1_with_documented_deprecation_window
```

## Purpose

RFC 0024 requires public release, package publication, API compatibility, schema compatibility,
checksums, SBOM, signing, and publication approval to be explicit release gates. The current
technical-preview repository has useful local dry-run evidence and v0.1.0 publication approval, but
it does not yet have channel publication proof, stable API/schema compatibility windows,
publication-grade signing decisions, or publication-grade SBOM/checksum attachment.

This gate makes those missing release inputs visible through `release-plan` and through the hard
release-readiness validator so release/package claims fail closed.

`PROD-V1-2A` now adds a narrower local API/schema stability contract:

- `docs/release/v1-api-schema-stability-matrix.json`
- `docs/release/schemas/v1/*.schema.json`
- `docs/release/fixtures/v1-api-schema-stability/golden-fixtures.json`
- `scripts/check_v1_api_schema_stability.py`

That contract validates stable v1 machine-readable fields for local source-built workflows. It does
not unblock package identity, signing, checksum/SBOM publication grade, package-channel proof, tag
creation, or public release claims.

## Gate Rows

| Gate row | Status | Required publication evidence | Current blocker |
| --- | --- | --- | --- |
| `api_compatibility_window` | local_v1_contract_ready | Published API stability tiers, compatibility window, deprecation policy, breaking-change approval. | Local v1 stable-field aliases and deprecation policy are declared; public package/API claims remain blocked pending channel proof. |
| `schema_compatibility_window` | local_v1_contract_ready | Schema version registry, compatibility window, migration notes, golden fixtures. | Stable schema files and golden fixtures exist for 11 v1 machine-readable surfaces; public schema claims remain blocked pending channel proof. |
| `package_identity_approval` | approved_pending_channel_proof | Approved package identities, channel ownership, naming, install/uninstall/rollback proof. | GitHub/TestPyPI/PyPI/Homebrew are approved for v0.1.0 but still lack publication and install proof. |
| `signing_policy_decision` | blocked | Artifact signing policy, maintainer approval, key custody, signing workflow evidence. | No signing key may be used and no signing mechanism is approved before publication. |
| `checksum_manifest` | dry_run_only | Publication-grade checksum manifest tied to release artifacts and source revision. | Local dry-run checksum evidence exists, but publication-grade checksums are not attached. |
| `sbom_bundle` | dry_run_only | Publication-grade Rust, Python, CLI, and optional image SBOM bundle. | Local dry-run SBOM evidence exists, but publication-grade SBOM approval is missing. |
| `publication_approval` | approved_pending_channel_proof | Explicit maintainer approval, release notes, tag approval, package-channel gate pass. | Maintainer approval exists for the v0.1.0 GitHub, TestPyPI, PyPI, and Homebrew sequence; channel publication, install proof, and post-release verification remain pending. |

## Relationship To Existing Release Evidence

Existing dry-run and release-readiness surfaces remain valid inputs:

- `scripts/release_provenance_dry_run.py` can create local SBOM, checksum, provenance, and workflow
  policy snapshots.
- `scripts/release_dry_run_proof.py` can run the local install/smoke/provenance dry run.
- `scripts/check_package_channel_readiness.py` validates the package-channel matrix.
- `scripts/check_release_readiness.py` aggregates release readiness and now includes this gate.
- `docs/release/package-channel-readiness-matrix.json` names package channels and their blockers.
- `docs/architecture/workspace-feature-build-matrix.md` defines required feature/build evidence.

Those surfaces are necessary but not sufficient for public publication. The gate remains blocked
until the rows above are explicitly changed with maintainer-approved release evidence and
channel-specific proof.

## Hard Release Rule

`scripts/check_release_readiness.py` treats the current blocked gate as release-blocking:

```text
publication/API/schema stability gate remains blocked
```

That is the correct current state. The failure prevents a local dry-run transcript or generated SBOM
from being mistaken for a publication-ready release.

## Non-Goals

- No package publication.
- No release tags.
- No signing key use.
- No package-channel submission.
- No OCI image push.
- No public API/schema stability claim.
- No production/platform/performance/Spark-replacement claim.
- No ShardLoom runtime behavior change.
- No external engine fallback.
