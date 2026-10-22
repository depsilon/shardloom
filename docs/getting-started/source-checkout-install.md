<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source Checkout Install

Status marker: `source_checkout_install_v1=true`.

Use this path to run the current repository revision. For the published
v0.6.0 packages, see [Package User Install Status](package-user-install.md). Build the CLI and use the Python package
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

Published 0.6.0 Linux x86_64 and macOS arm64 wheels include `shardloom.native`
and the explicit client option `transport="native"`. This section builds the
matching extension from source. An editable Python install alone does not
compile the extension. Build the extension, CLI
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

The proof retains the wheel under `proof/dist` and its report and logs under
`proof`. It verifies wheel RECORD hashes, legal notices, the actual extension
and CLI imports, every native integration test, and complete package uninstall.
A skipped native test fails this proof. Its temporary `proof/venv` no longer
contains ShardLoom afterward. To try the maintained
[context/result example](../../examples/paid-orders/context-result.py), install the
retained wheel in a separate environment. This development example uses native
execution and retained ShardLoom results without optional Arrow dependencies.

The current source checkout adds an explicit native delivery option:
`query.iter_batches(output_window_bytes=4194304)`. Omission or zero keeps ordinary
demand. Eligible immutable memory scans and single projections can keep two
unacknowledged batches within a 4-MiB referenced-byte window. Admission is limited
to 65,536 rows and 1–64 Int64 output fields; files, producer inputs, filtering,
sorting, limits and spill keep ordinary delivery. Subprocess transport rejects a
nonzero request before work begins.

Admission checks the actual native plan. A composed DataFrame projection can
retain a nested SELECT and therefore keep ordinary delivery; the final policy
field records that decision.

Use a context manager when stopping early. Batches remain provisional until the
iterator's final report appears. Retained native buffers remain charged to the
original memory grant until their last owner is released. The final report's
`columnar_output_window_requested_bytes` and
`columnar_output_window_selected_policy` fields distinguish `two_batch_window`,
`ordinary_ineligible` and `ordinary_not_requested`. This source change is not a
newly published package; see the [integration and acceptance boundary](../architecture/native-output-window-integration-2026-10-19.md).
The [embedding guide](https://shardloom.io/field-guide/embedding/) covers direct
typed native integration. The proof publishes nothing
and does not change the caller's existing Python environment.

Local Linux wheels built by the command above retain their host-specific tag.
The publication workflow separately runs the compatibility audit and installed
proof for the published `manylinux_2_39_x86_64` wheel. Windows native extension
execution remains unsupported; its published Python wheel uses the bundled CLI.
See the [0.6.0 distribution proof](../release/v0.6.0-publication-verification.md)
for actual platform and interpreter coverage.

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
