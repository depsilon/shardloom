# Vortex Upstream Release Intake Runbook

This runbook defines how ShardLoom handles upstream Vortex releases and Dependabot Vortex pull
requests. It is a maintenance workflow, not a runtime-support grant.

## Trigger

Use this runbook when any of the following happens:

- Dependabot opens a Cargo pull request that changes `vortex` or `vortex-*`.
- A contributor proposes a Vortex version bump.
- Upstream Vortex publishes a new release that might affect ShardLoom runtime, I/O, encoding,
  scan, source, sink, dtype, Python, or benchmark behavior.

Vortex is pre-1.0, so minor releases are treated as compatibility-breaking until reviewed.

## What Dependabot Can Do

Dependabot can:

- detect available Cargo updates,
- open a manifest/lockfile pull request,
- group `vortex` and `vortex-*` updates under the `vortex-upstream` dependency group,
- run configured CI after the pull request is opened.

Dependabot cannot:

- interpret upstream release notes,
- classify new Vortex APIs as ShardLoom native-provider candidates,
- update ShardLoom architecture docs,
- decide whether runtime support should expand,
- prove no-fallback/no-external-engine invariants,
- update benchmark, certificate, Native I/O, materialization, or claim-gate evidence,
- auto-merge pre-1.0 Vortex minor releases.

## Required Intake Steps

1. Read the upstream release notes and record the release URL, release date, and version.
2. Run `cargo info vortex@<version>` and record the crate version/license metadata.
3. Update or add a release intake note under `docs/dependencies/`.
4. Update `docs/architecture/vortex-public-api-inventory.md` with every runtime-relevant API,
   dependency-only item, baseline-only item, not-applicable item, and blocked item.
5. If the release changes the dependency version, update `shardloom-vortex/Cargo.toml` and
   `Cargo.lock`.
6. Run feature-gated compile checks for every existing Vortex feature gate:
   - `cargo check -p shardloom-vortex`
   - `cargo check -p shardloom-vortex --features upstream-vortex`
   - `cargo check -p shardloom-vortex --features vortex-file-io`
   - `cargo check -p shardloom-vortex --features vortex-write`
   - `cargo check -p shardloom-vortex --features vortex-local-primitives`
   - `cargo check -p shardloom-vortex --features vortex-traditional-analytics-benchmark`
7. Run dependency-footprint checks and verify direct fallback dependencies remain absent:
   - DataFusion
   - Spark
   - DuckDB
   - Polars
   - Velox
   - `vortex-datafusion`
8. Map runtime opportunities into existing phase-plan items or add implementation-ready child
   slices before runtime work begins.
9. Keep unsupported APIs fail-closed until an owning runtime slice adds executable behavior,
   tests, certificates, materialization/decode evidence, no-fallback evidence, and claim gates.
10. Move completed intake work to `docs/architecture/phased-execution-completed-ledger.md`.

## Required Verification

For dependency bumps, run:

```powershell
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo fmt --all -- --check
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo clippy --workspace --all-targets -- -D warnings
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test --workspace --all-targets
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-contract-tests --test release_readiness_metadata
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-contract-tests --test no_fallback_invariants
git diff --check
```

For docs-only intake or mapping work, run at minimum:

```powershell
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-contract-tests --test release_readiness_metadata
git diff --check
```

## Claim Boundary

A Vortex release intake may prove only one of these states:

- release inventoried,
- dependency compatibility proven,
- opportunity mapped,
- unsupported behavior blocked,
- runtime support implemented by a later owning runtime slice.

No intake PR may claim performance improvement, object-store/table runtime, SQL/DataFrame runtime,
distributed runtime, GPU runtime, package publication readiness, production readiness, or Spark
replacement.

## Fallback Boundary

Vortex array, compute, scan, source, and sink APIs may become ShardLoom-native providers only when
feature-gated, version-recorded, policy-admitted, and certificate-backed.

Vortex query-engine integrations and external engines remain baselines/oracles only. They must not
execute unsupported ShardLoom residual work.
