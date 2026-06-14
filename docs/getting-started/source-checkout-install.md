<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source Checkout Install

Status marker: `source_checkout_install_v1=true`.

Use this path when working from the repository before public package channels are live. This is the
active v1 local proof path; it is not a package publication, production, performance, Spark
replacement, or broad SQL/DataFrame claim.

## Build

```powershell
git clone https://github.com/depsilon/shardloom.git
cd shardloom
cargo build -p shardloom-cli --bin shardloom
```

Run the local CLI:

```powershell
target\debug\shardloom status --format json
target\debug\shardloom capabilities --format json
```

On Unix-like shells, use `target/debug/shardloom`.

## Python Source Checkout

For source-tree Python examples, prefer `PYTHONPATH` first so no environment is modified:

```powershell
$env:PYTHONPATH = "python\src"
python examples\local-python-smoke\run.py --repo-root .
```

Editable source installs are allowed for local development:

```powershell
python -m pip install -e python
```

Set `SHARDLOOM_BIN` only when the CLI binary is not on `PATH` or the Python wrapper cannot resolve
the source checkout binary:

```powershell
$env:SHARDLOOM_BIN = "target\debug\shardloom.exe"
```

## Single Local Proof

The source-checkout proof builds source artifacts, installs the locally built wheel in a clean
virtual environment, runs the local Python smoke, writes scoped local outputs, runs a tiny
ShardLoom-only benchmark smoke, and records a transcript:

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
```

Required execution evidence stays explicit:

```text
fallback_attempted=false
external_engine_invoked=false
public_package_claim_allowed=false
performance_claim_allowed=false
```

Next: [First 10 Minutes](first-10-minutes.md), [Examples](examples.md), and
[Troubleshooting And Support Bundle](troubleshooting-support.md).
