<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package, release, and install channel boundary

## Quick Answer

- **Audience:** user asking how to install ShardLoom today
- **Status:** `report_only`
- **Execution mode:** `local_release_dry_run`
- **Engine mode:** `batch_status`
- **Claim boundary:** Local dry-run proof, production-usability rehearsal, and channel readiness only; no PyPI, Homebrew, conda-forge, GHCR, crates.io, production, performance, or package-publication claim.

## Can ShardLoom Do This?

Package, release, and install channel boundary is inspectable as posture or diagnostics, but it is not broad runtime support.

## Claim Boundary

Local dry-run proof, production-usability rehearsal, and channel readiness only; no PyPI, Homebrew, conda-forge, GHCR, crates.io, production, performance, or package-publication claim.

## How To Try It

```text
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
- `production_usability_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

target/release-dry-run-proof/transcript.json records local source artifacts and no-publication status; target/production-usability-gate.json aggregates local install, docs/website, benchmark-artifact, and release-gate posture.

## Common Mistakes

- `running_pip_install_shardloom_from_public_registry`
- `assuming_release_dry_run_published_anything`
- `treating_production_usability_as_public_release`
- `installing_external_baselines_as_runtime_dependencies`

## Reference Files

- `docs/getting-started/first-10-minutes.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/getting-started/install.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/release/production-usability-gate.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/release/hard-release-readiness-gate.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/architecture/adoption-commercial-readiness-friction-reduction.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `README.md` - What this proves: Public technical-preview posture, Vortex-first positioning, and no-fallback boundaries.

## Related Use Cases

- `first-10-minutes-local-smoke`
- `foundry-local-proof-boundary`

## Related Field Guide Terms

- [report_only](https://shardloom.io/field-guide/report-only) (`Unsupported Diagnostics` / `report_only`)
