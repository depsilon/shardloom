<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Security And CI Hardening

Schema: `shardloom.v1_security_ci_hardening_report.v1`

Status: v1 local release-hardening evidence gate. This document and its validator do not publish
packages, create tags, sign artifacts, upload package artifacts, add secrets, execute ShardLoom
runtime workloads, invoke external engines, or authorize fallback execution.

```text
publication_attempted=false
tag_created=false
secrets_required=false
package_upload_attempted=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_claim_grade
```

## Scope

`scripts/check_v1_security_ci_hardening.py` closes the v1 control-plane hardening item by requiring
the existing release evidence to agree on:

- dependency audit, license classification, advisory/vulnerability scan, and forbidden-fallback dependency absence
- SBOM, checksum manifest, and local provenance dry-run evidence
- package artifact scan and local package smoke evidence through the package-channel readiness gate
- CodeQL, OpenSSF Scorecard, Dependabot, security posture, and PyPI Trusted Publisher/OIDC
  workflow boundaries
- no committed long-lived package upload tokens
- maintainer approval, release branch/tag protection, security advisory, incident-response,
  dependency-update, rollback, yank, delete, and deprecate policies
- Python 3.10 through 3.13 compatibility evidence
- OS matrix coverage across `ubuntu-latest`, `macos-latest`, and `windows-latest`
- Rust MSRV derived from root Cargo.toml validation with default features disabled
- CI artifact retention and release evidence bundle upload

The gate consumes these reports:

```text
target/dependency-audit-report.json
target/security-posture-report.json
target/release-security-gate-report.json
target/release-provenance-dry-run/supply-chain-release-evidence.json
target/package-channel-readiness-report.json
target/ci-gate-matrix-report.json
```

It writes:

```text
target/v1-security-ci-hardening-report.json
```

## Signing Policy

The v1 local gate uses an explicit no-signing rationale:

- local dry-run artifacts are not signed
- `signed_or_attested_status=not_signed_local_dry_run`
- no signing key is used in CI
- public signing or attestation requires maintainer approval for the selected release revision,
  artifact set, destination channels, key custody, and publication checklist

This is intentionally stricter than silently treating unsigned local artifacts as publication
grade. Local checksums and SBOMs are evidence inputs; they become publication-grade only after a
human-approved release process attaches them to the selected release artifacts.

## Package Credential Policy

PyPI and TestPyPI must prefer Trusted Publisher/OIDC. Long-lived package upload tokens are not
allowed in committed workflows, release evidence, or package-channel defaults. The draft PyPI
workflow keeps package upload authority behind a protected environment and manual approval.

Package publication remains blocked until the package-channel matrix has channel-specific install,
uninstall, smoke, SBOM/checksum/provenance, rollback, and maintainer approval evidence.

## CI Shape

The CI hardening design keeps expensive work parallel:

- Rust baseline and feature matrix remain separate.
- Rust MSRV derived from root Cargo.toml validation runs as its own small job.
- The workspace version-source report keeps Rust MSRV and upstream Vortex provider evidence
  manifest-derived: root `Cargo.toml` owns `[workspace.package].rust-version` and
  `[workspace.dependencies].vortex`; `scripts/release_report_utils.py`,
  `scripts/write_ci_version_env.py`, benchmark evidence, and `shardloom-vortex/build.rs` consume
  those sources rather than duplicating current-version strings.
- Python 3.10 through 3.13 plus OS matrix smoke/build checks run in a separate matrix job.
- Dependency/security, package smoke, runtime core, user-surface, benchmark, website/docs, and CI
  drift checks produce independent artifacts.
- `release-readiness` downloads existing producer artifacts and runs final aggregate gates,
  including `python scripts/check_v1_security_ci_hardening.py`.

This preserves the slow-tail optimization from the current CI design while making v1 release
hardening explicit and machine-readable.

## Claim Boundary

Passing this gate means ShardLoom has coherent local v1 security, supply-chain, compatibility, and
CI evidence for the checked-in release-hardening surface.

It does not allow:

- GitHub Release creation or release tags
- PyPI, TestPyPI, Homebrew, Scoop, winget, conda-forge, GHCR, or crates.io publication
- signing key use or public attestation publication
- production readiness claims
- performance, Spark-displacement, engine-replacement, Foundry, object-store, lakehouse, broad SQL,
  or broad DataFrame claims
- Spark, DataFusion, DuckDB, Polars, pandas, Dask, Ray, Trino, Velox, or another external engine as
  ShardLoom runtime fallback
