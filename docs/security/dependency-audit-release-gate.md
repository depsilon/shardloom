<!-- SPDX-License-Identifier: Apache-2.0 -->

# Dependency Audit Release Gate

Status: P8.0C release-gate contract. This document does not publish packages, create tags, add
secrets, add runtime dependencies, or authorize fallback execution.

## Purpose

ShardLoom's dependency audit gate has two jobs:

- block incompatible licenses, advisories, yanked crates, and unknown dependency sources
- prove that runtime packages do not include external fallback engines

Benchmark/dev environments may install Spark, DataFusion, DuckDB, Polars, pandas, Dask, PySpark, or
similar systems as comparison baselines. Runtime package metadata must not include them.

## Release-Gate Command

The hard gate is:

```powershell
python scripts\check_dependency_audit.py --release-gate
```

`--release-gate` implies:

- strict missing-tool behavior
- `cargo deny check licenses advisories bans sources`
- `cargo audit`
- packaging/dev `pip-audit` against the generated ShardLoom Python runtime requirements file
- runtime no-fallback dependency metadata check
- benchmark-only dependency classification check
- JSON `DependencyAuditReport` emission under `target/dependency-audit-report.json`

For local bring-up, the non-release command stays non-blocking for missing tools:

```powershell
python scripts\check_dependency_audit.py
```

Missing tools are allowed only outside the release gate.

## DependencyAuditReport

The script emits `schema_version = shardloom.dependency_audit_report.v1` with:

```text
cargo_deny_status
cargo_audit_status
pip_audit_status
license_policy_status
advisory_status
yanked_dependency_status
unknown_source_status
runtime_dependency_scope
benchmark_dependency_scope
fallback_dependency_absent
tool_results
diagnostics
```

The report is release-ready only when:

- `cargo_deny_status=passed`
- `cargo_audit_status=passed` or an explicit maintainer waiver is recorded outside the script
- `pip_audit_status=passed` for the packaging/dev environment
- `fallback_dependency_absent=true`
- external engine dependencies appear only in benchmark/dev scopes

## Package-Gate Integration

`scripts/check_package_channel_readiness.py --require-local-evidence` consumes
`target/dependency-audit-report.json` before the package-channel report can pass. The package gate
requires dependency inventory, license classification, advisory status, and
`fallback_dependency_absent=true` alongside package smoke and SBOM/checksum/provenance evidence.
This keeps package-channel rows blocked when any runtime dependency is unreviewed, incompatible,
or classified as a forbidden fallback engine.

## Tool Installation

```powershell
cargo install cargo-deny --locked
cargo install cargo-audit --locked
python -m pip install pip-audit
python scripts\check_dependency_audit.py --include-python-packaging
```

If `python scripts\check_dependency_audit.py --release-gate` is launched with a different Python
than the packaging/dev environment, set `SHARDLOOM_PIP_AUDIT_PYTHON` to the Python executable that
has `pip-audit` installed, or put a `pip-audit` executable on `PATH`.

These are developer/release tooling installs. They are not ShardLoom runtime dependencies.

## No-Fallback Dependency Rule

The runtime dependency scope must not include Spark, DataFusion, DuckDB, Polars, pandas, Dask,
PySpark, Ray, Velox, Trino, Snowflake, Databricks, BigQuery, or similar external engines as runtime
dependencies. If a dependency is present for benchmark comparison, it must remain benchmark-only and
must be reported as `external_baseline_only`, not ShardLoom execution.

## Current Waivers

The gate has one current advisory waiver: `RUSTSEC-2024-0436` for transitive
`paste 1.0.15` through upstream Vortex/Arrow/parquet paths. The waiver is not a
runtime fallback exception; it is a tracked unmaintained-dependency exception
with no safe direct ShardLoom upgrade. Remove it once upstream dependency paths
drop `paste`.

The license allowlist includes `0BSD` and `CC0-1.0` for current permissive
Vortex/Arrow transitive crates. GPL, LGPL, AGPL, SSPL, BUSL, proprietary,
source-available, unknown, and non-SPDX license declarations remain denied by
omission from the allowlist.
