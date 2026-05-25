<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package, release, and install channel boundary

## Quick Answer

- **Audience:** user asking how to install ShardLoom today
- **Status:** `report_only`
- **Execution mode:** `local_release_dry_run`
- **Engine mode:** `batch_status`
- **Claim boundary:** Local dry-run proof and channel readiness only; no PyPI, Homebrew, conda-forge, GHCR, crates.io, production, or package-publication claim.

## Can ShardLoom Do This?

Package, release, and install channel boundary is inspectable as posture or diagnostics, but it is not broad runtime support.

## Claim Boundary

Local dry-run proof and channel readiness only; no PyPI, Homebrew, conda-forge, GHCR, crates.io, production, or package-publication claim.

## How To Try It

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
```

## Blocker

Public channels need trusted publishing, provenance, clean install proof, security gates, release approval, and publication-specific evidence.

## Internal Flow

`source_checkout -> local_release_dry_run -> batch_status -> local_wheel_dry_run_report, install_channel_matrix -> evidence -> claim gate`

## Evidence You Should See

- `package_install_mode`
- `clean_env_install_status`
- `cli_binary_resolved`
- `smoke_status`
- `sbom_status`
- `provenance_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

target/release-dry-run-proof/transcript.json records local source artifacts and no-publication status.

## Common Mistakes

- `running_pip_install_shardloom_from_public_registry`
- `assuming_release_dry_run_published_anything`
- `installing_external_baselines_as_runtime_dependencies`

## Reference Files

- `docs/getting-started/first-10-minutes.md` - What this proves: Shortest local orientation path for smoke checks and evidence inspection.
- `docs/getting-started/install.md` - What this proves: Installation posture and package-channel caveats for technical-preview users.
- `docs/release/hard-release-readiness-gate.md` - What this proves: Release gate requirements for package publication and public claims.
- `docs/architecture/adoption-commercial-readiness-friction-reduction.md` - What this proves: Commercial-readiness friction plan and package/channel adoption boundaries.
- `README.md` - What this proves: Public technical-preview posture, Vortex-first/no-fallback positioning, and primary repo entrypoints.

## Related Use Cases

- `first-10-minutes-local-smoke`
- `foundry-local-proof-boundary`

## Related Field Guide Terms

- `website/field-guide/report-only.html` - report_only (`Unsupported Diagnostics` / `report_only`)
