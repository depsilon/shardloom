<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package User Install Status

ShardLoom v0.6.0 is published as a technical preview through GitHub pre-release
assets, TestPyPI, PyPI and the `depsilon/tap` Homebrew formula. The release includes
native Python bindings in the Linux x86_64 and macOS arm64 wheels, alongside the
bundled CLI. The support label remains a local engine with operational hardening
in progress; package access does not establish production readiness or general
SQL/DataFrame, platform or performance parity.

The repository source version may be ahead of the published package channels.
Install commands stay pinned to the latest version with matching channel proof.
Public status is owned by
[`docs/release/public-status-matrix.md`](../release/public-status-matrix.md).

```text
package_channel_status=published_v0.6.0_selected_channels
selected_publication_channels=github_prerelease,testpypi,pypi,homebrew_tap
package_install_commands_visible=true
public_package_release_claim_allowed=true
public_package_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

## Install

Python package:

```sh
python -m pip install shardloom==0.6.0
```

Homebrew CLI formula:

```sh
brew install depsilon/tap/shardloom
```

GitHub release assets:

```sh
gh release download v0.6.0 --repo depsilon/shardloom --pattern '*' --dir shardloom-v0.6.0
```

TestPyPI rehearsal package:

```sh
python -m pip install --index-url https://test.pypi.org/simple/ --no-deps shardloom==0.6.0
```

The default Python client uses its packaged CLI. Explicit binary, environment or
source configuration takes precedence; otherwise a supported wheel resolves its
`shardloom/bin/<system-arch>/shardloom` resource before checking `PATH`. Choose
`transport="native"` explicitly to use the in-process shared workflow surface,
or import the typed [native Python API](../../python/README.md#native-python-api).
PyArrow is optional and is needed only for examples that explicitly use Arrow
interchange.

The registries contain the following exact wheel filenames. Their interpreter,
ABI and platform tags determine whether pip can install them:

| Channel | Published wheel filename (interpreter, ABI and platform tags) |
| --- | --- |
| TestPyPI | `shardloom-0.6.0-cp310-abi3-macosx_26_0_arm64.whl` |
| TestPyPI | `shardloom-0.6.0-cp310-abi3-manylinux_2_39_x86_64.whl` |
| TestPyPI | `shardloom-0.6.0-cp313-cp313-win_amd64.whl` |
| PyPI | `shardloom-0.6.0-cp310-abi3-macosx_26_0_arm64.whl` |
| PyPI | `shardloom-0.6.0-cp310-abi3-manylinux_2_39_x86_64.whl` |
| PyPI | `shardloom-0.6.0-cp313-cp313-win_amd64.whl` |

The native wheels use `cp310-abi3`. Linux publication includes a compatibility
audit for `manylinux_2_39_x86_64` covering both the CLI and extension, with their
payload bytes preserved. Windows retains its CLI-backed Python wheel; the native
extension is unsupported there. The source distribution contains no compiled
binaries and needs a compatible CLI for the default transport. Use the
[source checkout guide](source-checkout-install.md) to build a native extension.

## Configure Execution Resources

Version 0.6.0 requires explicit memory and parallelism allocations before data
inspection, preparation or execution. Applications upgrading from 0.5.1 must
configure both on a client, context or session, or provide them to the terminal
operation. For example:

```python
import shardloom as sl

ctx = sl.context(resources=sl.ExecutionResources(256 << 20, 2))
result = ctx.sql("SELECT COUNT(*) AS n FROM range(0, 100)").collect()
print(result.output_row_count)
```

The 256 MiB and two lanes are illustrative caller choices. Lazy declarations and
inert capability queries remain resource-free. See the
[migration notes](../release/v0.6.0-release-notes.md#migration-declare-execution-resources)
for inheritance, overrides and administrator ceilings. A query allocation is
not a total-process RSS limit.

## Smoke Check

After installing the Homebrew formula:

```sh
shardloom status
```

Expected posture includes:

```text
fallback execution: disabled
```

For the Python package:

```sh
python - <<'PY'
import shardloom as sl

smoke = sl.context().smoke_check()
print(smoke.fallback_attempted)
print(smoke.external_engine_invoked)
PY
```

If installing a source distribution without a bundled CLI, select an approved
compatible binary with `SHARDLOOM_BIN=/path/to/shardloom` before CLI-backed work.

## Proof Refs

- [GitHub release proof](../release/channel-proofs/github-prerelease-v0.6.0-transcript.json).
- [TestPyPI proof](../release/channel-proofs/testpypi-v0.6.0-transcript.json).
- [PyPI proof](../release/channel-proofs/pypi-v0.6.0-transcript.json).
- [Homebrew proof](../release/channel-proofs/homebrew-v0.6.0-transcript.json).
- [Package-channel matrix](../release/package-channel-readiness-matrix.md).

## Uninstall And Upgrade

Python package:

```sh
python -m pip uninstall -y shardloom
python -m pip install --upgrade shardloom==0.6.0
```

Remove the Homebrew formula:

```sh
brew uninstall shardloom
```

To upgrade an installed formula:

```sh
brew upgrade depsilon/tap/shardloom
```

GitHub asset installs are downloaded files; remove their download directory when
no longer needed.

## Blocked Channels

Scoop, winget, conda-forge, GHCR containers and future crates.io public API crates
remain blocked until their separate channel proofs exist. Workspace Rust crates
remain unpublished. Unsupported execution fails explicitly without an external
query-engine fallback.

## v0.6.0 Distribution Proof

The [publication verification](../release/v0.6.0-publication-verification.md)
records artifact hashes, exact build sources and runtime scope. Each registry
publication workflow clean-installs its Linux and macOS native wheels, runs all
80 binding tests per wheel with no skips, and verifies uninstall. The matching
source CI also checks Linux Python 3.10/3.13 and macOS Python 3.13.

Separate complete-workflow proofs download, install and uninstall the following
registry wheels on macOS arm64:

- TestPyPI: `shardloom-0.6.0-cp310-abi3-macosx_26_0_arm64.whl`.
- PyPI: `shardloom-0.6.0-cp310-abi3-macosx_26_0_arm64.whl`.

The Windows distribution has build and artifact-inspection evidence; these checks
do not establish native extension or pressure-runtime parity. Full-size resource,
pressure and failure acceptance retains its recorded macOS arm64 scope. Homebrew
builds the CLI from source; the supplemental Python native check uses the verified
PyPI module and is not a Homebrew extension build.
