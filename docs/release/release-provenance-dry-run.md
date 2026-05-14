<!-- SPDX-License-Identifier: Apache-2.0 -->

# Release Provenance Dry Run

Status: P8.0E local evidence generator. This workflow does not publish packages, create tags, push
images, submit feedstocks, add secrets, or add runtime fallback dependencies.

## Command

```powershell
python scripts\release_provenance_dry_run.py --skip-build
```

For a full local proof that builds artifacts first:

```powershell
python scripts\release_provenance_dry_run.py
```

The script writes:

```text
target/release-provenance-dry-run/manifest.json
target/release-provenance-dry-run/supply-chain-release-evidence.json
target/release-provenance-dry-run/checksums.sha256
target/release-provenance-dry-run/workflow-policy-snapshot.json
target/release-provenance-dry-run/shardloom-rust-workspace.cdx.json
target/release-provenance-dry-run/shardloom-python-artifacts.cdx.json
target/release-provenance-dry-run/shardloom-cli-binary.cdx.json
```

## Evidence

The generated `SupplyChainReleaseEvidence` dry-run report records:

- source commit and dirty-state status
- local builder identity
- local CLI and Python artifact refs
- Rust workspace SBOM ref
- Python artifact SBOM ref
- CLI binary SBOM ref
- checksum manifest ref
- PyPI Trusted Publisher workflow policy snapshot
- `publication_attempted=false`
- `tag_created=false`
- `secrets_required=false`
- `fallback_engine_dependency_added=false`

The SBOM JSON files are local CycloneDX-style dry-run evidence produced from checked-in manifests,
`Cargo.lock`, Python package metadata, and local artifact digests. They are not a substitute for
maintainer-approved release SBOM tooling, but they make the release gate executable before public
publication is authorized.

## Workflow Hardening Snapshot

The workflow snapshot checks the draft PyPI workflow for:

- manual `workflow_dispatch`
- `publish_approved` acknowledgement input
- protected GitHub environment `pypi`
- OIDC `id-token: write`
- least-privilege `contents: read`
- absence of long-lived package tokens
- third-party publish action pin status

The current PyPI draft workflow is allowed to keep tagged third-party actions only as
`waived_until_real_publication`. Before real publication, third-party publish actions must be pinned
to commit SHAs or an explicit maintainer waiver must be recorded in the release evidence.

Release gate summary: third-party publish actions must be pinned to commit SHAs before public
publication unless a maintainer records an explicit waiver.

## Relationship To Release Dry Run

`scripts/release_dry_run_proof.py` invokes this provenance dry run after local wheel, CLI, smoke,
and benchmark checks complete. Its transcript records:

- `provenance_dry_run_performed`
- `sbom_checksum_manifest_generated`
- the `release_provenance_dry_run` step result

## Non-Goals

This dry run does not sign artifacts, create SLSA attestations, upload SBOMs, publish to PyPI,
publish to crates.io, submit Conda recipes, create release tags, or push OCI images.
