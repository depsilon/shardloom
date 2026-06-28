<!-- SPDX-License-Identifier: Apache-2.0 -->

# Install ShardLoom

ShardLoom v0.2.1 is the current proof-backed technical-preview package release. You can use either
the source checkout path or the selected published package channels: GitHub pre-release assets,
TestPyPI, PyPI, and the `depsilon/tap` Homebrew formula.

Public status is owned by `docs/release/public-status-matrix.md`. This page routes install
questions; it is not a production, performance, broad SQL/DataFrame, or Spark-replacement claim.

## Choose The Path

| Need | Page | Current status |
| --- | --- | --- |
| Build and run from a clone | [Source Checkout Install](source-checkout-install.md) | Supported local proof path |
| Install from published packages | [Package User Install Status](package-user-install.md) | v0.2.1 selected package channels are published and proof-backed |
| Run first commands | [First 10 Minutes](first-10-minutes.md) | Supported local proof path |
| Inspect support state | [V1 Supported And Unsupported Surface](v1-supported-unsupported.md) | Generated from matrices |
| Diagnose failures | [Troubleshooting And Support Bundle](troubleshooting-support.md) | Local/redacted support only |

```text
package_install_commands_visible=true
public_package_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

## Package Quickstart

Python package:

```sh
python -m pip install shardloom==0.2.1
```

Homebrew CLI formula:

```sh
brew install depsilon/tap/shardloom
shardloom status
```

The PyPI package is a thin client over ShardLoom's CLI. Supported platform wheels include a bundled
CLI resource; explicit `SHARDLOOM_BIN`, `SHARDLOOM_REPO_ROOT`, source checkout binaries, and
`shardloom` on `PATH` remain supported when you want to pin or override the binary.

## Source Checkout Quickstart

```powershell
git clone https://github.com/depsilon/shardloom.git
cd shardloom
cargo build -p shardloom-cli --bin shardloom
target\debug\shardloom status --format json
```

On Unix-like shells, use `target/debug/shardloom`.

For Python examples from the source tree:

```powershell
$env:PYTHONPATH = "python\src"
python examples\local-python-smoke\run.py --repo-root .
```

Set `SHARDLOOM_BIN` when the CLI binary is not on `PATH`:

```powershell
$env:SHARDLOOM_BIN = "target\debug\shardloom.exe"
```

## Local Wheel Dry Run

Release-readiness proof uses a locally built wheel and a clean virtual environment before public
channel commands are advertised:

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
```

The proof builds the CLI, stages it inside a temporary platform wheel under
`shardloom/bin/<system-arch>/`, installs that exact wheel with `pip --no-index`, removes
`SHARDLOOM_BIN`/`SHARDLOOM_REPO_ROOT` from the clean environment, verifies bundled CLI resolution,
runs CLI/Python smoke checks, writes scoped generated-source local JSONL/CSV outputs, records that
benchmark smoke is not required for package-channel proof, and writes a transcript under `target/`.
Use `--include-benchmark-smoke` only when deliberately adding the benchmark-only feature lane to the
local proof.

## Package Boundary

GitHub release assets, PyPI, TestPyPI, and Homebrew are live for v0.2.1. That package access is not
a production, performance, broad SQL/DataFrame, or Spark-replacement claim.

The selected v0.2.1 package path is GitHub pre-release plus TestPyPI/PyPI and Homebrew. Scoop,
winget, and conda-forge remain later feasible channels; GHCR and crates.io are outside v1. The
canonical track is
[`docs/release/v1-local-source-package-release.md`](../release/v1-local-source-package-release.md).

After generating release dry-run, security, package-channel, website, and benchmark-completeness
evidence, the local usability aggregate is:

```powershell
python scripts\check_production_usability_gate.py
```

It writes `target/production-usability-gate.json` and keeps
`public_release_claim_allowed=false`, `public_package_claim_allowed=false`,
`fallback_attempted=false`, and `external_engine_invoked=false`.
