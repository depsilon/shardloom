<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source Checkout Install

Status marker: `source_checkout_install_v1=true`.

Use this path to run the current repository revision, including changes made
after the published v0.5.1 packages. Build the CLI and use the Python package
from the same checkout. This local proof does not establish production readiness,
performance superiority, Spark replacement, or broad SQL/DataFrame support.

## Build

```powershell
git clone https://github.com/depsilon/shardloom.git
cd shardloom
cargo build -p shardloom-cli --bin shardloom --features release-user-surfaces
```

Run the local CLI:

```powershell
target\debug\shardloom status --format json
target\debug\shardloom capabilities --format json
```

On Unix-like shells, use `target/debug/shardloom`.

These relative paths assume Cargo's default output directory. If Cargo is
configured to write elsewhere, resolve `target_directory` with
`cargo metadata --offline --no-deps --format-version 1` and use that directory.
On a developer Mac, follow the [local storage and sequential workload rules](../architecture/local-development-storage.md)
before building; generated artifacts must stay outside synced folders.

## Python Source Checkout

For source-tree Python examples, prefer `PYTHONPATH` first so no environment is modified.
The example supplies an illustrative allocation of 16 GiB and 8 execution lanes;
choose both values for your workload:

```powershell
$env:PYTHONPATH = "python\src"
python examples\local-python-smoke\run.py --repo-root . --memory-gb 16 --max-parallelism 8
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

## Optional Native Python Binding

Current source adds `shardloom.native` and the explicit client option
`transport="native"`. Published 0.5.1 wheels have not been replaced. An editable
Python install alone does not compile the extension. Build the extension, CLI
and Python package from one checkout, then verify the resulting wheel in an
isolated environment. The [acceptance report](../benchmarks/native-python-surface-2026-10-11.md)
records the tested host and complete workflow scope.

The commands below use a Unix shell on macOS or Linux. Use an interpreter with a
matching wheel in `.github/requirements/ci-native-python.txt`; the retained local
proof uses CPython 3.12 on macOS arm64. Run the substantial builds sequentially
under the storage/workload supervision described above. This example uses two
Cargo jobs and a fresh, local-only workspace; these are build settings, separate
from each query's required resource allocation.

```sh
mkdir -p "$HOME/LocalData/shardloom"
NATIVE_WORKSPACE="$(mktemp -d "$HOME/LocalData/shardloom/native-python-XXXXXX")"
export CARGO_TARGET_DIR="$HOME/.cache/shardloom/cargo-target"
export CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0
python3 -m venv "$NATIVE_WORKSPACE/build-env"
NATIVE_PYTHON="$NATIVE_WORKSPACE/build-env/bin/python"
"$NATIVE_PYTHON" -m pip install --require-hashes \
  -r .github/requirements/ci-native-python-build.txt
"$NATIVE_PYTHON" -m pip download --require-hashes --only-binary=:all: --no-deps \
  -r .github/requirements/ci-native-python.txt --dest "$NATIVE_WORKSPACE/wheelhouse"

cargo build --locked --release -p shardloom-cli --bin shardloom \
  --features release-user-surfaces
"$NATIVE_PYTHON" scripts/build_native_python.py \
  --output-dir "$NATIVE_WORKSPACE/extension" --release
NATIVE_TARGET="$(cargo metadata --offline --no-deps --format-version 1 | \
  "$NATIVE_PYTHON" -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"
"$NATIVE_PYTHON" scripts/prove_native_python_wheel.py \
  --output-dir "$NATIVE_WORKSPACE/proof" \
  --native-library "$NATIVE_WORKSPACE/extension/_native.abi3.so" \
  --cli-binary "$NATIVE_TARGET/release/shardloom" \
  --wheelhouse "$NATIVE_WORKSPACE/wheelhouse"
```

The proof retains the wheel under `proof/dist`, its report and logs under
`proof`, and a separate installed environment at `proof/venv`. It verifies wheel
RECORD hashes, legal notices, the actual extension and CLI imports, and every
native integration test. A skipped native test fails this proof. Use
`"$NATIVE_WORKSPACE/proof/venv/bin/python" -I` to try the
[native API example](../../python/README.md#source-only-native-python-api).
The installed optional PyArrow consumer supplies the Arrow interchange example;
ShardLoom still owns execution. Nothing is published, and the caller's existing
Python environment is not changed.

Windows extension execution, other unobserved platforms, Linux wheel portability
and public distribution retain separate acceptance requirements. CLI-backed
Python remains available through its existing supported-platform package path.

## Single Local Proof

The source-checkout proof builds source artifacts, installs the locally built wheel in a clean
virtual environment, runs the local Python smoke, writes scoped local outputs, runs a tiny
package-channel proof without requiring the benchmark-only feature lane, and records a transcript:

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
