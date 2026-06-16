<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package User Install Status

ShardLoom package publication is approved for the v0.1.0 sequence, but channel install commands
become public only after each channel has been published and verified. The order is source checkout
proof, GitHub pre-release, TestPyPI, PyPI, then Homebrew tap.

```text
package_channel_status=blocked
selected_publication_channels=github_prerelease,testpypi,pypi,homebrew_tap
final_publication_event_required=true
package_install_commands_visible=false
public_package_claim_allowed=false
publication_attempted=false
tag_created=false
package_upload_attempted=false
fallback_attempted=false
external_engine_invoked=false
```

No package-user install command is active yet in this source revision. The source checkout path
remains the supported local proof path until the selected channel gates close and a tagged release
updates this page with verified install, smoke, uninstall, and rollback instructions.

## What Exists Today

- Local wheel and sdist build proof through `python scripts\release_dry_run_proof.py --rows 64 --iterations 1`.
- Local clean virtual-environment install proof from that local wheel.
- Publication approval for v0.1.0 GitHub pre-release, TestPyPI, PyPI, and Homebrew, recorded in
  [`docs/release/final-release-approval-post-release-verification.json`](../release/final-release-approval-post-release-verification.json).
- Package-channel readiness rows in
  [`docs/release/package-channel-readiness-matrix.md`](../release/package-channel-readiness-matrix.md).
- Package names and metadata checks in
  [`docs/release/package-name-readiness.md`](../release/package-name-readiness.md).
- The selected release track in
  [`docs/release/v1-local-source-package-release.md`](../release/v1-local-source-package-release.md).

## Uninstall And Upgrade While Local

For an editable source-tree Python install, uninstall the local package with:

```powershell
python -m pip uninstall -y shardloom
```

Then remove any local build outputs you no longer need:

```powershell
cargo clean
Remove-Item -LiteralPath python\dist -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath target\release-dry-run-proof -Recurse -Force -ErrorAction SilentlyContinue
```

To upgrade a source checkout, pull the repository and rerun the source proof:

```powershell
git pull --ff-only
cargo build -p shardloom-cli --bin shardloom
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
```

## Future Package Page Rule

When each publication step completes, this page must show the exact channel, version, install,
upgrade, uninstall, smoke-check, rollback/yank/deprecate, checksum/SBOM, and support-bundle
instructions for GitHub pre-release, TestPyPI, PyPI, and Homebrew as applicable. Until channel proof
exists, package commands stay withheld.
