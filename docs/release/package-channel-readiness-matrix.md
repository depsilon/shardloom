<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package Channel Readiness Matrix

Status: selected v0.1.9 release channels are published and proof-backed. This document does not
authorize production readiness, performance superiority, broad runtime support, future package
channels, container publication, crates.io publication, signing-key use, or fallback execution.

The machine-readable source of truth is
[`docs/release/package-channel-readiness-matrix.json`](package-channel-readiness-matrix.json) with
schema `shardloom.package_channel_readiness_matrix.v1`. Validate it with:

The selected v0.1.9 release-channel IDs and shared status vocabulary are centralized in
[`scripts/release_channel_contract.py`](../../scripts/release_channel_contract.py); validators and
tests must import that contract instead of duplicating the selected-channel list.

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

That mode consumes the dependency audit report, local package smoke transcript, local
SBOM/checksum/provenance dry-run evidence, and checked-in TestPyPI/PyPI registry proof transcripts.
Selected v0.1.9 rows are ready only when channel-specific install, uninstall, clean-install, smoke,
SBOM/checksum/provenance, rollback, and authorization evidence is attached.

After an approved registry upload exists, use `scripts/python_registry_package_proof.py` to produce
the clean install, smoke, and uninstall transcript for that channel. PyPI proof is not accepted
until the TestPyPI transcript exists and is referenced.

## Readiness Rules

- No selected channel may be marked ready without channel-specific install, uninstall,
  clean-install, smoke, SBOM/checksum/provenance, and rollback/yank/delete/deprecate evidence.
- The package gate requires dependency inventory, license classification, provenance status,
  forbidden-fallback dependency absence, package smoke transcript, SBOM refs, checksum refs,
  rollback policy refs, and publication authorization state.
- Package identity is machine-checked by the package-channel validator: the Python package identity
  remains `shardloom`, current workspace Rust crates must stay `publish = false`, and crates.io is
  limited to future stable public API crates until separate API/schema approval exists.
- PyPI and TestPyPI require Trusted Publisher/OIDC posture. Long-lived upload tokens are not
  release-grade for the public package path. The v0.1.9 Trusted Publisher uploads and
  registry-smoke proofs are complete for the selected package path.
- PyPI registry proof requires prior TestPyPI registry proof. Both proofs must install from the
  target registry into a clean environment, run the no-fallback Python client smoke against an
  explicit approved ShardLoom CLI binary, and uninstall the package.
- Internal Rust crates remain unpublished. crates.io is limited to future stable public API crates
  such as `shardloom-protocol` and `shardloom-client` after API/schema stability gates exist.
- Package access does not imply production readiness, performance or superiority, Spark
  replacement, production SQL/DataFrame support, production object-store/lakehouse support,
  production Foundry support, or public release readiness.
- Package channels cannot add Spark, DataFusion, DuckDB, Polars, pandas, Dask, Velox, Trino, or
  another external query engine as a ShardLoom runtime fallback dependency.
- Every package channel now carries a v1 feasibility decision in the JSON matrix. GitHub
  pre-release, TestPyPI, PyPI, and Homebrew are included and proof-backed in the v0.1.9 package
  sequence. Scoop, winget, and conda-forge remain v1-feasible later channels once
  their channel-specific install/uninstall/smoke and provenance evidence exists. GHCR and
  crates.io are explicitly not in v1 scope because the container image contract and future public
  Rust API crates are not real yet.

## Channel Matrix

| Channel | Target | v1 feasibility | Install command | Uninstall command | Required proof | Current status |
| --- | --- | --- | --- | --- | --- | --- |
| GitHub pre-release | GitHub v0.1.9 pre-release assets | included channel proof passed | `gh release download v0.1.9 --repo depsilon/shardloom --pattern '*' --dir <install-dir>` | `rm -rf <install-dir>` | Approved tag/release, attached checksums, SBOM, provenance, clean download/smoke transcript, rollback/delete policy. | `ready`: `docs/release/channel-proofs/github-prerelease-v0.1.9-transcript.json` verifies attached assets, checksums, and CLI smoke. |
| TestPyPI | Python package `shardloom` | included channel proof passed | `python -m pip install --index-url https://test.pypi.org/simple/ --no-deps shardloom==0.1.9` | `python -m pip uninstall -y shardloom` | TestPyPI Trusted Publisher/OIDC, clean install, uninstall, smoke, no committed token. | `ready`: `docs/release/channel-proofs/testpypi-v0.1.9-transcript.json` verifies registry install, Python client smoke, and uninstall. |
| PyPI | Python package `shardloom` | included channel proof passed | `python -m pip install shardloom==0.1.9` | `python -m pip uninstall -y shardloom` | PyPI Trusted Publisher/OIDC, prior TestPyPI proof, clean install, uninstall, smoke, SBOM/checksum/provenance, yank policy. | `ready`: `docs/release/channel-proofs/pypi-v0.1.9-transcript.json` verifies public registry install, Python client smoke, prior TestPyPI proof ref, and uninstall. |
| Homebrew tap | CLI formula | included channel proof passed | `brew install depsilon/tap/shardloom` | `brew uninstall shardloom` | Tap/formula proof, versioned artifact checksum, install/uninstall, smoke, rollback/deprecate policy. | `ready`: `docs/release/channel-proofs/homebrew-v0.1.9-transcript.json` verifies tap audit/style/test, source build install, CLI smoke, and uninstall. |
| Scoop | Windows CLI manifest | v1 feasible pending channel proof | `scoop install shardloom` | `scoop uninstall shardloom` | Bucket manifest, checksum, install/uninstall, smoke, update/rollback policy. | `blocked`: local CLI build, local smoke, checksum, and provenance refs are prepared; no bucket manifest, channel checksum, clean Scoop install/uninstall/smoke transcript, or maintainer approval exists. |
| winget | Windows Package Manager manifest | v1 feasible pending channel proof | `winget install depsilon.shardloom` | `winget uninstall depsilon.shardloom` | winget manifest, repository submission validation, install/uninstall, smoke, update/rollback policy. | `blocked`: local CLI build, local smoke, checksum, and provenance refs are prepared; no winget manifest/submission, installer proof, clean winget install/uninstall/smoke transcript, or maintainer approval exists. |
| conda-forge | `shardloom-cli`, `shardloom-python`, and `shardloom` metapackage | v1 feasible pending channel proof | `conda install -c conda-forge shardloom` | `conda remove shardloom shardloom-cli shardloom-python` | staged-recipes/feedstock proof, clean Conda install, smoke, no fallback dependencies, maintainer policy. | `blocked`: local Conda recipe scaffold tests and clean Conda source-local install proof pass; no staged-recipes/feedstock submission, feedstock install/uninstall/smoke transcript, or maintainer approval exists. |
| GHCR container | OCI image `ghcr.io/depsilon/shardloom` | not in v1 scope | `docker pull ghcr.io/depsilon/shardloom:<tag>` | `docker rmi ghcr.io/depsilon/shardloom:<tag>` | Image build, SBOM, provenance, digest pin, vulnerability scan, smoke, pull/run docs. | `blocked`: not included in the current release candidate; local non-container artifact/checksum/provenance refs exist, but Docker is unavailable locally and no image build, image SBOM/provenance/vulnerability scan, pull/run/smoke transcript, digest, or maintainer approval exists. |
| crates.io future | Future `shardloom-protocol` and `shardloom-client` crates only | not in v1 scope | `cargo add shardloom-protocol@<version> shardloom-client@<version>` | `cargo remove shardloom-protocol shardloom-client` | Extracted stable public crates, API/schema stability gate, `cargo publish --dry-run`, maintainer approval, no internal crate publication. | `blocked`: not included in the current release candidate; cargo metadata confirms current workspace crates remain unpublished, future public crates are not extracted, API/schema stability still blocks public crates, no `cargo publish --dry-run` applies, and maintainer approval is missing. |

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
- `scripts/python_registry_package_proof.py` and
  `target/python-registry-package-proof/<channel>-transcript.json`
- `docs/release/sbom-generation-plan.md`
- `docs/security/supply-chain-response.md`
- `scripts/check_package_channel_readiness.py`

The current matrix is ready for the selected v0.1.9 package channels only. It is not evidence for
future package channels, production readiness, performance superiority, or broad runtime claims.
