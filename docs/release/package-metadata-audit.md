<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package Metadata Audit

Status: package-discoverability scaffold. No package publication is authorized.

## PyPI

Target package name: `shardloom`.

Current metadata expectations:

- Apache-2.0 license and license files
- homepage: `https://shardloom.io`
- repository and issue links on GitHub
- Python 3.10 through 3.13 classifiers
- topic classifiers for database, information analysis, Python modules, and utilities
- keywords: analytics, columnar, vortex, data-engine, ETL, benchmark, no-fallback, Rust, Python
- no runtime dependencies

The package uses the modern SPDX license expression `license = "Apache-2.0"`.
Do not add the legacy Apache license classifier while setuptools enforces
PEP 639; `python -m build python` rejects license classifiers when a license
expression is present.

Before publication, run:

```powershell
python -m build python
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
python -m twine check python/dist/*
```

## Cargo

Current workspace crates are internal and marked `publish = false`. Future
public candidates are `shardloom-protocol` and `shardloom-client`; those crates
do not exist yet and should be extracted only when their API contracts are
stable enough for permanent crates.io publication.

Cargo metadata expectations for public candidates:

- Apache-2.0 license
- homepage: `https://shardloom.io`
- repository and documentation URLs
- readme
- concise description
- keywords and crates.io categories
- no fallback-engine runtime dependencies

Use `cargo publish --dry-run` only for public candidate crates after
`publish = false` is not set on that crate and metadata is complete.

## Conda

Target recipe names:

- `shardloom-cli`
- `shardloom-python`
- `shardloom`

Metadata expectations:

- Apache-2.0 license
- `license_file` entries
- homepage and doc URL
- GitHub development URL
- package split preserving pure Python wrapper versus platform CLI binary
- no runtime fallback engines

Conda-forge submission requires tagged source archives, sha256 hashes,
maintainer approval, and feedstock review.
