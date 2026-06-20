<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package Name Readiness

Status: v0.1.10 selected package identities are published and proof-backed for GitHub pre-release,
TestPyPI, PyPI, and Homebrew. Do not publish additional channels, create new tags, or add secrets
from this document.

Package-name posture is separate from package-channel readiness. The channel-by-channel release
gate lives in [`package-channel-readiness-matrix.md`](package-channel-readiness-matrix.md) and the
machine-readable matrix lives in
[`package-channel-readiness-matrix.json`](package-channel-readiness-matrix.json) with schema
`shardloom.package_channel_readiness_matrix.v1`.

## Targets

- PyPI: `shardloom`
- Conda-forge: `shardloom-cli`, `shardloom-python`, `shardloom`
- crates.io candidates: `shardloom-protocol`, `shardloom-client`

Internal crates remain unpublished. Public crates should be split out only when
their API contracts are stable enough for permanent publication.

## PyPI Trusted Publisher Workflow

The workflow lives at `.github/workflows/pypi-publish-draft.yml`.
It uses GitHub OIDC, the `testpypi` and `pypi` environments, and no token secrets. It is manual and
guarded by an explicit `publish-approved` input so it does not publish accidentally.

The v0.1.10 release sequence completed:

- GitHub release/tag/assets: `v0.1.10`
- TestPyPI package proof:
  `docs/release/channel-proofs/testpypi-v0.1.10-transcript.json`
- PyPI package proof:
  `docs/release/channel-proofs/pypi-v0.1.10-transcript.json`
- Homebrew tap proof:
  `docs/release/channel-proofs/homebrew-v0.1.10-transcript.json`
- no runtime fallback dependencies were added

## TestPyPI Dry Run

Use TestPyPI first through the Trusted Publisher workflow for future releases. The registry proof
must install from the target registry into a clean environment, smoke the package with an explicit
approved ShardLoom CLI binary via `--shardloom-bin` or `SHARDLOOM_BIN`, and uninstall the package.

```powershell
gh workflow run pypi-publish-draft.yml -f channel=testpypi -f publish_approved=publish-approved
python scripts\python_registry_package_proof.py --channel testpypi --version 0.1.10 --shardloom-bin target\release\shardloom --output target\python-registry-package-proof\testpypi-transcript.json
```

PyPI publication uses the same workflow with `channel=pypi`, but it must reference the passed
TestPyPI transcript. Do not commit tokens.

## Conda-Forge Staged-Recipes Readiness

Package split:

- `shardloom-cli`: compiled Rust CLI binary
- `shardloom-python`: pure Python wrapper
- `shardloom`: metapackage depending on both

Staged-recipes source placeholders:

```yaml
source:
  url: https://github.com/depsilon/shardloom/archive/refs/tags/v{{ version }}.tar.gz
  sha256: <release-archive-sha256>
```

Maintainers:

- `depsilon`

Conda recipes must not add Spark, DataFusion, DuckDB, Polars, pandas, Dask,
Velox, or another external query engine as runtime fallback dependencies.

## Crates.io Readiness

Candidate public crates:

- `shardloom-protocol`: future stable protocol types
- `shardloom-client`: future public Rust client

Do not publish current internal crates. Crates.io publication is permanent for
crate names and versions; mistakes cannot be fully deleted. Use dry runs only
until the maintainer explicitly approves publication:

```powershell
cargo publish --dry-run -p shardloom-protocol
cargo publish --dry-run -p shardloom-client
```

These commands are not currently applicable because the candidate crates have
not been extracted.

## Local Dry-Run Proof

The current package-name readiness proof is source-local:

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
```

It builds the local CLI, stages a bundled-wheel package tree, builds wheel/sdist artifacts, installs
the local wheel in a clean virtual environment, resolves the bundled CLI without `SHARDLOOM_BIN` or
`SHARDLOOM_REPO_ROOT`, runs the first-10-minutes smoke path, records
`wheel_client_resolved_bundled_cli` and `benchmark_smoke_required_for_package_release=false`, and
writes the transcript at `target/release-dry-run-proof/transcript.json`. Run the optional benchmark
smoke separately, or pass `--include-benchmark-smoke`, when benchmark evidence is the goal.

The Trusted Publisher draft workflow follows the same bundled-CLI staging contract for the selected
PyPI/TestPyPI channel build. It builds `shardloom-cli`, stages the CLI under
`shardloom/bin/<platform-tag>/`, builds wheel/sdist artifacts from the staged package tree, and
uploads the staged `dist` directory. The release workflow must not publish a direct
`python -m build python` wheel for bundled-CLI patch releases.

This proof is intentionally not a publish workflow. It does not create tags,
submit Conda feedstocks, upload to PyPI/TestPyPI, publish crates, push OCI
images, or add secrets.

Before any public package claim, maintainers must also run:

```powershell
python scripts\check_package_channel_readiness.py --require-local-evidence
python scripts\check_release_readiness.py
```

The current package-channel matrix is ready for the selected v0.1.10 channels and remains blocked for
future channels such as Scoop, winget, conda-forge, GHCR, and crates.io public API crates.
