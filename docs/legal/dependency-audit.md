<!-- SPDX-License-Identifier: Apache-2.0 -->

# Dependency Audit Policy

ShardLoom dependency review has two jobs: keep Apache-2.0-compatible packaging
clean, and preserve the no-fallback execution architecture.

## Runtime Versus Benchmark-Only Dependencies

Runtime dependencies are dependencies required by released ShardLoom packages:
the Rust CLI, future public protocol/client crates, and the pure Python wrapper.
They must be compatible with Apache-2.0 distribution goals and must not add
Spark, DataFusion, DuckDB, Polars, Velox, pandas, Dask, Ray, Trino, Snowflake,
Databricks, BigQuery, or another external query engine as a runtime fallback.

Benchmark-only dependencies live in benchmark/dev environments. They may include
external engines only as comparison baselines or correctness oracles. Benchmark
dependencies must stay out of runtime package metadata and runtime lockfile
claims unless separately approved.

Committed benchmark profile requirement files should pin direct dependencies to audited versions.
Those pins are reproducibility and repository-advisory hygiene for optional benchmark
environments; they do not add runtime dependencies or authorize fallback execution.

## Vortex Dependency Boundaries

Upstream Vortex crates are native provider candidates, not fallback engines.
Vortex array, file, scan, source, and sink APIs may be used only when admitted
through ShardLoom policy and reported through ShardLoom evidence. Vortex query
engine integrations, external SQL engines, and compatibility runtimes must not
execute unsupported ShardLoom work.

The optional `shardloom-vortex` feature tree is audited as part of release
readiness because Vortex-native support is central to the package identity.

## Approval Process

Every new dependency requires:

- package purpose and package scope
- license and Apache-2.0 compatibility review
- security/advisory posture review
- runtime, benchmark-only, test-only, or packaging-only classification
- no-fallback architecture review
- NOTICE, third-party license bundle, and SBOM impact review

Dependencies with GPL, LGPL, AGPL, SSPL, BUSL, proprietary, source-available,
unknown, or unclear licenses require explicit maintainer/RFC approval and should
normally be rejected.

## Audit Tooling

The root `deny.toml` configures cargo-deny for Rust license, advisory, ban, and
source checks:

```powershell
cargo install cargo-deny --locked
cargo deny check licenses advisories bans sources
```

The helper script runs installed audit tools and reports install commands for
missing tools:

```powershell
python scripts/check_dependency_audit.py
python scripts/check_dependency_audit.py --include-cargo-audit
python scripts/check_dependency_audit.py --include-python-packaging
python scripts/check_dependency_audit.py --release-gate
```

`--release-gate` is the hard P8.0C release gate. It implies strict missing-tool behavior,
`cargo-deny`, `cargo audit`, packaging/dev `pip-audit` against the ShardLoom Python project,
runtime no-fallback dependency checks, benchmark-only dependency classification checks across the
traditional analytics base, extended, Spark, and GPU-optional profile requirement files, and JSON
`DependencyAuditReport` emission under `target/dependency-audit-report.json`. Missing audit tools
are allowed only outside release-gate mode.

`cargo audit` is optional only outside the release gate:

```powershell
cargo install cargo-audit --locked
cargo audit
```

`pip-audit` is only for packaging/dev Python environments:

```powershell
python -m pip install pip-audit
python scripts\check_dependency_audit.py --include-python-packaging
```

The audit script first tries the invoking Python, then a `pip-audit` executable on `PATH`, then
known local packaging runtimes. Set `SHARDLOOM_PIP_AUDIT_PYTHON` to a Python executable that has
`pip-audit` installed when the release command is launched from a different interpreter.

The audit script writes `target/dependency-audit/python-runtime-requirements.txt` from
`python/pyproject.toml` and points `pip-audit` at that generated runtime requirements file. The
Python package currently has no runtime dependencies, so the generated file is empty and the script
uses `--disable-pip --no-deps` to avoid creating a temporary virtual environment. That output must
not be treated as evidence that ShardLoom has Python runtime dependency requirements.

See `docs/security/dependency-audit-release-gate.md` for the release-gate command, required report
fields, and no-fallback dependency rule.

The release dry-run proof installs only the local ShardLoom wheel into a clean
virtual environment and resolves a local CLI binary:

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
```

That proof must not install Spark, DataFusion, DuckDB, Polars, pandas, Dask,
Velox, or any other fallback execution engine as a runtime dependency.

## Current Duplicate-Crate Posture

The all-features Rust tree currently includes duplicate transitive versions
through optional Vortex/Arrow paths. `deny.toml` warns on multiple versions
rather than denying them. Duplicate-deny gates should be added only after a
dedicated dependency cleanup verifies that the all-features tree is clean.

## Current Release-Gate Exceptions

`deny.toml` includes two explicit permissive transitive licenses beyond the
base allowlist: `0BSD` for `enum-iterator`/`enum-iterator-derive` and `CC0-1.0`
for `tiny-keccak`. These are admitted because they are permissive/open licenses
pulled through Vortex/Arrow paths and do not introduce copied incompatible code
or fallback execution.

`RUSTSEC-2024-0436` for `paste 1.0.15` is waived in cargo-deny because it is an
unmaintained transitive dependency through upstream Vortex/Arrow/parquet paths,
`cargo audit` reports it as a warning, and no safe direct upgrade is available
inside ShardLoom. The waiver must be removed when an upstream-compatible Vortex
or transitive dependency update drops `paste`.
