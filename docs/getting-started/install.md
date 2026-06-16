<!-- SPDX-License-Identifier: Apache-2.0 -->

# Install ShardLoom

ShardLoom is pre-release. Use source checkout workflows until release artifacts are explicitly
published and verified for the selected channel.

Public status is owned by `docs/release/public-status-matrix.md`. This page routes install
questions; it is not a package-publication, production, benchmark, or Spark-displacement claim.

## Choose The Path

| Need | Page | Current status |
| --- | --- | --- |
| Build and run from a clone | [Source Checkout Install](source-checkout-install.md) | Supported local proof path |
| Understand package availability | [Package User Install Status](package-user-install.md) | v0.1.0 publication approved; channel proof still required |
| Run first commands | [First 10 Minutes](first-10-minutes.md) | Supported local proof path |
| Inspect support state | [V1 Supported And Unsupported Surface](v1-supported-unsupported.md) | Generated from matrices |
| Diagnose failures | [Troubleshooting And Support Bundle](troubleshooting-support.md) | Local/redacted support only |

```text
package_install_commands_visible=false
public_package_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

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

The proof installs the exact wheel built during the dry run with `pip --no-index`, resolves the
local CLI through `SHARDLOOM_BIN`, runs CLI/Python smoke checks, writes scoped generated-source
local JSONL/CSV outputs, records that benchmark smoke is not required for package-channel proof,
and writes a transcript under `target/`. Use `--include-benchmark-smoke` only when deliberately
adding the benchmark-only feature lane to the local proof.

## Package Boundary

Do not assume PyPI, Conda-forge, Homebrew, GHCR, crates.io, or GitHub release packages are
available until a tagged release and channel proof says so. This local proof path is not a public
package path.
It is not a PyPI, Conda, Homebrew, GHCR, crates.io, production, or performance claim.

The selected v0.1.0 package path is GitHub pre-release plus TestPyPI/PyPI and Homebrew. Scoop,
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
