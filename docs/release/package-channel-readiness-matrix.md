<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package Channel Readiness Matrix

Status: blocked release-readiness evidence. This document does not publish packages, create tags,
push containers, submit package manifests, add secrets, or authorize fallback execution.

The machine-readable source of truth is
[`docs/release/package-channel-readiness-matrix.json`](package-channel-readiness-matrix.json) with
schema `shardloom.package_channel_readiness_matrix.v1`. Validate it with:

```powershell
python scripts\check_package_channel_readiness.py
```

The hard release-readiness gate also consumes this matrix:

```powershell
python scripts\check_release_readiness.py
```

Release-readiness and CI use the stricter local evidence mode:

```powershell
python scripts\check_package_channel_readiness.py --require-local-evidence
```

That mode consumes the dependency audit report, local package smoke transcript, and local
SBOM/checksum/provenance dry-run evidence before the package-channel report can pass. It still does
not mark any public channel ready; channel rows remain blocked until channel-specific install,
uninstall, clean-install, smoke, SBOM/checksum/provenance, rollback, authorization, and human
approval evidence is attached.

## Readiness Rules

- No channel may be marked ready without channel-specific install, uninstall, clean-install, smoke,
  SBOM/checksum/provenance, and rollback/yank/delete/deprecate evidence.
- The package gate requires dependency inventory, license classification, provenance status,
  forbidden-fallback dependency absence, package smoke transcript, SBOM refs, checksum refs,
  rollback policy refs, and publication authorization state.
- PyPI and TestPyPI require Trusted Publisher/OIDC posture. Long-lived upload tokens are not
  release-grade for the public package path.
- Internal Rust crates remain unpublished. crates.io is limited to future stable public API crates
  such as `shardloom-protocol` and `shardloom-client` after API/schema stability gates exist.
- Package access does not imply production readiness, performance or superiority, Spark
  replacement, production SQL/DataFrame support, production object-store/lakehouse support,
  production Foundry support, or public release readiness.
- Package channels cannot add Spark, DataFusion, DuckDB, Polars, pandas, Dask, Velox, Trino, or
  another external query engine as a ShardLoom runtime fallback dependency.

## Channel Matrix

| Channel | Target | Install command | Uninstall command | Required proof | Current status |
| --- | --- | --- | --- | --- | --- |
| GitHub pre-release | Reviewed source archive plus attached local artifacts | `gh release download <tag> --repo depsilon/shardloom --pattern shardloom-* --dir <install-dir>` | `Remove-Item -LiteralPath <install-dir> -Recurse -Force` | Approved tag/release, checksums, SBOM, provenance, clean install/smoke transcript, rollback/delete policy. | `blocked`: no approved release tag, channel install transcript, or attached artifacts. |
| TestPyPI | Python package `shardloom` | `python -m pip install --index-url https://test.pypi.org/simple/ --no-deps shardloom==<version>` | `python -m pip uninstall -y shardloom` | TestPyPI Trusted Publisher/OIDC or scoped human credential proof, clean install, uninstall, smoke, no committed token. | `blocked`: no TestPyPI Trusted Publisher proof, upload proof, or clean install/uninstall/smoke transcript. |
| PyPI | Python package `shardloom` | `python -m pip install shardloom==<version>` | `python -m pip uninstall -y shardloom` | PyPI Trusted Publisher/OIDC, maintainer approval, clean install, uninstall, smoke, SBOM/checksum/provenance, yank policy. | `blocked`: Trusted Publisher/OIDC, approval, channel smoke, and provenance evidence are missing. |
| Homebrew tap | CLI formula | `brew install depsilon/tap/shardloom` | `brew uninstall shardloom` | Tap/formula proof, versioned artifact checksum, install/uninstall, smoke, rollback/deprecate policy. | `blocked`: no tap formula, checksum proof, or smoke transcript. |
| Scoop | Windows CLI manifest | `scoop install shardloom` | `scoop uninstall shardloom` | Bucket manifest, checksum, install/uninstall, smoke, update/rollback policy. | `blocked`: no bucket manifest, checksum proof, or smoke transcript. |
| winget | Windows Package Manager manifest | `winget install depsilon.shardloom` | `winget uninstall depsilon.shardloom` | winget manifest, repository submission validation, install/uninstall, smoke, update/rollback policy. | `blocked`: no manifest submission, installer proof, or smoke transcript. |
| conda-forge | `shardloom-cli`, `shardloom-python`, and `shardloom` metapackage | `conda install -c conda-forge shardloom` | `conda remove shardloom shardloom-cli shardloom-python` | staged-recipes/feedstock proof, clean Conda install, smoke, no fallback dependencies, maintainer policy. | `blocked`: local clean Conda proof is not a conda-forge package proof, and no feedstock transcript exists. |
| GHCR container | OCI image `ghcr.io/depsilon/shardloom` | `docker pull ghcr.io/depsilon/shardloom:<tag>` | `docker rmi ghcr.io/depsilon/shardloom:<tag>` | Image build, SBOM, provenance, digest pin, vulnerability scan, smoke, pull/run docs. | `blocked`: no container build, image SBOM/provenance/vulnerability scan, or pull/run/smoke transcript. |
| crates.io future | Future `shardloom-protocol` and `shardloom-client` crates only | `cargo add shardloom-protocol@<version> shardloom-client@<version>` | `cargo remove shardloom-protocol shardloom-client` | Extracted stable public crates, API/schema stability gate, `cargo publish --dry-run`, maintainer approval, no internal crate publication. | `blocked`: candidate crates do not exist yet and current workspace crates remain `publish=false`. |

## Evidence Required Before A Channel Can Become Ready

Every ready row must attach:

- install command and transcript
- uninstall command and transcript
- clean environment proof
- smoke check proof
- SBOM, checksum, and provenance artifact refs
- rollback, yank, delete, deprecate, or advisory policy
- channel-specific authorization/provenance proof
- maintainer approval
- no-publication/no-tag/no-secret/no-fallback evidence until the approved publish step

The machine-readable matrix lists these gate references under `gate_evidence_refs`:

- `scripts/check_dependency_audit.py` and `target/dependency-audit-report.json`
- `scripts/release_dry_run_proof.py` and `target/release-dry-run-proof/transcript.json`
- `scripts/release_provenance_dry_run.py` and
  `target/release-provenance-dry-run/supply-chain-release-evidence.json`
- `docs/release/sbom-generation-plan.md`
- `docs/security/supply-chain-response.md`
- `scripts/check_package_channel_readiness.py`

The current matrix is intentionally blocked. It is valid because it lists blockers and prevents
package claims; it is not valid evidence for public package publication.
