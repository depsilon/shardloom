<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package User Install Status

ShardLoom package channels are not public install channels yet, but the v1 path is now selected:
source checkout first, then GitHub pre-release, TestPyPI, and PyPI after the final publication
event.

```text
package_channel_status=blocked
selected_publication_channels=github_prerelease,testpypi,pypi
final_publication_event_required=true
package_install_commands_visible=false
public_package_claim_allowed=false
publication_attempted=false
tag_created=false
package_upload_attempted=false
fallback_attempted=false
external_engine_invoked=false
```

No package-user install command is active yet. The source checkout path remains the supported local
proof path until the selected channel gates close and a tagged release updates this page.

## What Exists Today

- Local wheel and sdist build proof through `python scripts\release_dry_run_proof.py --rows 64 --iterations 1`.
- Local clean virtual-environment install proof from that local wheel.
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

When the final publication event completes, this page must show the exact channel, version, install,
upgrade, uninstall, smoke-check, rollback/yank, checksum/SBOM, and support-bundle instructions for
GitHub pre-release, TestPyPI, and PyPI as applicable. Until then, package commands stay withheld.
