<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package User Install Status

ShardLoom v0.3.0 is published as a technical preview through the selected package channels:
GitHub pre-release assets, TestPyPI, PyPI, and the `depsilon/tap` Homebrew formula. These package
commands are install access only; they do not imply production readiness, performance superiority,
Spark replacement, broad SQL/DataFrame support, object-store/lakehouse production support, Foundry
production support, or fallback execution.

The repository source version may be ahead of the currently published selected channels during a
patch-release preparation window. Keep the install commands pinned to the latest proof-backed
published version until matching channel proofs are checked in.

```text
package_channel_status=published_v0.3.0_selected_channels
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
python -m pip install shardloom==0.3.0
```

Homebrew CLI formula:

```sh
brew install depsilon/tap/shardloom
```

GitHub release assets:

```sh
gh release download v0.3.0 --repo depsilon/shardloom --pattern '*' --dir shardloom-v0.3.0
```

TestPyPI rehearsal package:

```sh
python -m pip install --index-url https://test.pypi.org/simple/ --no-deps shardloom==0.3.0
```

The PyPI package is a Python client surface over the ShardLoom CLI. Explicit
binary/env/source configuration takes precedence. Otherwise, a supported wheel
resolves its packaged `shardloom/bin/<system-arch>/shardloom` resource before
checking `PATH`, so supported bundled-wheel installs do not need a separate CLI
or binary-path configuration.

The registry inventories record these exact wheel filenames. Interpreter, ABI and
platform tags constrain installation; they do not establish runtime UAT for every platform.

| Channel | Published wheel filename (interpreter, ABI and platform tags) |
| --- | --- |
| TestPyPI | `shardloom-0.3.0-cp313-cp313-macosx_26_0_arm64.whl` |
| TestPyPI | `shardloom-0.3.0-cp313-cp313-manylinux_2_39_x86_64.whl` |
| TestPyPI | `shardloom-0.3.0-cp313-cp313-win_amd64.whl` |
| PyPI | `shardloom-0.3.0-cp313-cp313-macosx_26_0_arm64.whl` |
| PyPI | `shardloom-0.3.0-cp313-cp313-manylinux_2_39_x86_64.whl` |
| PyPI | `shardloom-0.3.0-cp313-cp313-win_amd64.whl` |

Other supported Python/OS combinations may install the source package but need a
compatible CLI from Homebrew, a release asset, or a source build.

## Smoke Check

After installing the Homebrew formula:

```sh
shardloom status
```

Expected posture includes:

```text
fallback execution: disabled
```

For the Python package, use the normal context surface when the CLI is bundled in the installed
wheel or already on `PATH`:

```sh
python - <<'PY'
import shardloom as sl

ctx = sl.context()
smoke = ctx.smoke_check()
print(smoke.fallback_attempted)
print(smoke.external_engine_invoked)
PY
```

If the CLI is not on `PATH`, point the client at an approved CLI binary before running CLI-backed
smoke checks:

```sh
export SHARDLOOM_BIN=/path/to/shardloom
python - <<'PY'
import shardloom as sl

smoke = sl.context().smoke_check()
print(smoke.fallback_attempted)
print(smoke.external_engine_invoked)
PY
```

## Proof Refs

- GitHub release proof:
  [`docs/release/channel-proofs/github-prerelease-v0.3.0-transcript.json`](../release/channel-proofs/github-prerelease-v0.3.0-transcript.json)
- TestPyPI proof:
  [`docs/release/channel-proofs/testpypi-v0.3.0-transcript.json`](../release/channel-proofs/testpypi-v0.3.0-transcript.json)
- PyPI proof:
  [`docs/release/channel-proofs/pypi-v0.3.0-transcript.json`](../release/channel-proofs/pypi-v0.3.0-transcript.json)
- Homebrew proof:
  [`docs/release/channel-proofs/homebrew-v0.3.0-transcript.json`](../release/channel-proofs/homebrew-v0.3.0-transcript.json)
- Package-channel matrix:
  [`docs/release/package-channel-readiness-matrix.md`](../release/package-channel-readiness-matrix.md)

## Uninstall And Upgrade

Python package:

```sh
python -m pip uninstall -y shardloom
python -m pip install --upgrade shardloom==0.3.0
```

Homebrew formula:

```sh
brew uninstall shardloom
brew upgrade depsilon/tap/shardloom
```

GitHub release asset installs are ordinary downloaded files; remove the download directory when you
no longer need it.

## Blocked Channels

Scoop, winget, conda-forge, GHCR containers, and future crates.io public API crates remain blocked
until separate channel-specific proofs exist. Current workspace Rust crates remain unpublished.

## v0.3.0 Distribution Proof

See [publication verification](../release/v0.3.0-publication-verification.md) for
channel-specific artifact hashes, build sources and the scope of runtime checks.

- TestPyPI installed and smoked `shardloom-0.3.0-cp313-cp313-macosx_26_0_arm64.whl`.
- PyPI installed and smoked `shardloom-0.3.0-cp313-cp313-macosx_26_0_arm64.whl`.

The other published wheels have build and artifact-inspection evidence. Their
presence in a registry does not establish clean runtime installation proof.
