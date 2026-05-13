<!-- SPDX-License-Identifier: Apache-2.0 -->

# Supply-Chain Response Plan

Status: P8.0B initial incident-response skeleton for RFC 0043. This document does not publish
packages, create tags, add secrets, or authorize any fallback engine.

## Response Principles

- Freeze publication before investigating release or dependency compromise.
- Preserve evidence without exposing secrets.
- Prefer private advisories while an exploit or package compromise is being triaged.
- Verify source, SBOM, checksums, provenance, and artifact contents before any replacement release.
- Keep external engines as baselines only. They cannot be introduced as emergency fallback
  execution.

## Incident Classes

| Incident | Immediate action | Verification | User-facing action |
| --- | --- | --- | --- |
| Compromised dependency | Freeze release branch and dependency updates | run `cargo deny`, `cargo audit`, and packaging/dev `pip-audit` where applicable | publish advisory or dependency pin guidance |
| Yanked crate or package | Block release gate | identify affected lockfile/artifacts | document upgrade or pin |
| Malicious package version | Disable publication and package use | compare package contents against reviewed source and checksums | yank/deprecate where supported and publish advisory |
| Compromised PyPI release | Freeze PyPI workflow and trusted publisher environment | verify wheel/sdist digest, SBOM, provenance, and source ref | yank affected versions where supported and notify users |
| Compromised Conda package | Freeze feedstock submission path | verify recipe, sources, hashes, artifacts, and maintainers | coordinate feedstock advisory/remediation |
| Compromised GitHub release | Disable release workflow | verify release assets, checksums, attestations, and tag source | delete or mark affected assets and publish advisory |
| Compromised CI workflow | Disable affected workflow | inspect workflow history, token permissions, and artifact chain | rotate credentials and document affected artifacts |
| Compromised maintainer account | Freeze all publication paths | audit pushes, tags, releases, workflows, and package uploads | rotate credentials and publish impact assessment |

## Standard Response Steps

1. Freeze publication, release tags, feedstock submission, package upload, and OCI publication.
2. Disable or restrict affected workflow/environment if CI or publishing is implicated.
3. Revoke or rotate credentials and invalidate long-lived tokens if any exist.
4. Identify affected commits, tags, packages, wheels, archives, binaries, SBOMs, and checksums.
5. Verify source, package contents, checksums, SBOMs, and provenance.
6. Compare artifacts against reviewed source and expected checksum/provenance evidence.
7. Yank, deprecate, delete, or mark affected package versions where the registry supports it.
8. Publish a private advisory while exploit details are sensitive.
9. Rebuild from a known-good source ref with a clean workflow.
10. Publish user remediation guidance and known affected versions.
11. Record the incident in release notes and security documentation.

## Release Re-Enablement Gate

Publication remains blocked until:

- maintainers approve release re-enablement
- dependency/advisory checks pass or carry explicit waivers
- SBOM and checksum manifests are regenerated
- provenance or attestations are regenerated where supported
- package contents match reviewed source
- release notes include the incident impact and remediation
- no runtime fallback dependencies were added

## No-Fallback Incident Rule

Incident response must not add Spark, DataFusion, DuckDB, Polars, pandas, Dask, Velox, Trino,
Snowflake, Databricks, BigQuery, Foundry compute, or any external engine as runtime fallback. If a
ShardLoom path is unsafe, it is blocked with deterministic diagnostics until native evidence exists.
