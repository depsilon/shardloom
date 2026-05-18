<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package Name Readiness

Status: release-readiness scaffold. Do not publish packages, create tags, or
add secrets from this document.

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

## PyPI Trusted Publisher Draft

The draft workflow lives at `.github/workflows/pypi-publish-draft.yml`.
It uses GitHub OIDC, the `pypi` environment, and no token secrets. It is manual
and guarded by an explicit `publish-approved` input so it does not publish
accidentally.

Before enabling publication:

- configure PyPI Trusted Publisher for the repository, workflow, and `pypi`
  environment
- verify package metadata with `python -m build python`
- run `python scripts\release_dry_run_proof.py --rows 64 --iterations 1`
- verify `twine check python/dist/*`
- ensure the release has maintainer approval
- ensure no runtime fallback dependencies were added

## TestPyPI Dry Run

Use TestPyPI only from a release branch or throwaway package version. Do not
reuse a public version number.

```powershell
python -m pip install build twine
python -m build python
python -m twine check python/dist/*
python -m twine upload --repository testpypi python/dist/*
python -m pip install --index-url https://test.pypi.org/simple/ --no-deps shardloom
python -c "import shardloom; print(shardloom.__version__)"
```

This requires human-provided TestPyPI credentials or a future TestPyPI Trusted
Publisher configuration. Do not commit tokens.

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

It builds the local CLI, builds wheel/sdist artifacts, installs the local wheel
in a clean virtual environment, resolves the built CLI through `SHARDLOOM_BIN`,
runs the first-10-minutes smoke path, and executes the local benchmark smoke.
The transcript lives at `target/release-dry-run-proof/transcript.json`.

This proof is intentionally not a publish workflow. It does not create tags,
submit Conda feedstocks, upload to PyPI/TestPyPI, publish crates, push OCI
images, or add secrets.

Before any public package claim, maintainers must also run:

```powershell
python scripts\check_package_channel_readiness.py
python scripts\check_release_readiness.py --allow-blocked
```

The current package-channel matrix is valid but blocked: no channel has channel-specific install,
uninstall, clean-install, smoke, SBOM/checksum/provenance, rollback/yank, and authorization proof.
