<!-- SPDX-License-Identifier: Apache-2.0 -->

# Install ShardLoom

ShardLoom is pre-release. Use source checkout workflows until release artifacts
are explicitly published.

## From Source

```powershell
git clone https://github.com/depsilon/shardloom.git
cd shardloom
cargo build -p shardloom-cli --bin shardloom
```

Run the local CLI:

```powershell
target\debug\shardloom status --format json
```

On Unix-like shells, use `target/debug/shardloom`.

## Python Source Package

The Python package is a pure wrapper over the CLI JSON protocol. It has no
runtime dependencies and does not execute ShardLoom at import time.

```powershell
python -m pip install -e python
python -c "from shardloom import ShardLoomClient; print(ShardLoomClient.from_env())"
```

Set `SHARDLOOM_BIN` when the CLI binary is not on `PATH`:

```powershell
$env:SHARDLOOM_BIN = "target\debug\shardloom.exe"
```

## Not Published Yet

Do not assume PyPI, Conda-forge, or crates.io packages are available until a
tagged release says so. Package-name readiness docs live in
`docs/release/package-name-readiness.md`.
