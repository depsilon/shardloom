<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package Name Readiness

Status: release-readiness scaffold. Do not publish packages, create tags, or
add secrets from this document.

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
