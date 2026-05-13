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
```

`cargo audit` is optional until the maintainer adds it as a hard release gate:

```powershell
cargo install cargo-audit --locked
cargo audit
```

`pip-audit` is only for packaging/dev Python environments:

```powershell
python -m pip install pip-audit
python -m pip_audit
```

The Python package currently has no runtime dependencies, so pip-audit output
must not be treated as evidence that ShardLoom has Python runtime dependency
requirements.

## Current Duplicate-Crate Posture

The all-features Rust tree currently includes duplicate transitive versions
through optional Vortex/Arrow paths. `deny.toml` warns on multiple versions
rather than denying them. Duplicate-deny gates should be added only after a
dedicated dependency cleanup verifies that the all-features tree is clean.
