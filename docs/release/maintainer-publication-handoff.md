<!-- SPDX-License-Identifier: Apache-2.0 -->

# Maintainer Publication Handoff

Status: release handoff packet after `RELEASE-PACKAGE-15`, amended through the proof-backed v0.1.8
GitHub/TestPyPI/PyPI/Homebrew selected-channel publication sequence. This document does not itself
create tags, publish packages, sign artifacts, upload SBOMs, submit package-channel manifests, add
secrets, or authorize fallback execution.

Date: 2026-06-13

Benchmark freshness addendum, 2026-06-13:

- The current website benchmark bundle was refreshed after PR #1206 from source revision
  `5743638a9225f479a0096f1c6db51a0068cac68f`, generated at
  `2026-06-13T11:33:10.063090+00:00`, and contains `1920` promoted normalized rows.
- The `RELEASE-PACKAGE-15` audit artifact paths and source revision below remain historical
  release-handoff evidence until the release gates are rerun for the selected release revision. Do
  not treat their `74a2e7d4f77eed0686971518e010463da26f2cdf` source revision or `1320` row count
  as the current public website benchmark freshness.
- The refreshed benchmark keeps `performance_claim_allowed=false`; it is evidence freshness and
  optimization direction, not a release, package, production, or performance claim.

V1 local/source/package track addendum, 2026-06-15:

- Real production object-store, table/lakehouse, distributed, live/hybrid, and Foundry environments
  are not available for v1 certification. Those claims remain fail-closed.
- The feasible v1 release path is narrowed to source checkout, GitHub pre-release, TestPyPI, PyPI,
  Homebrew, local API/schema stability, local Python user-surface proof, local benchmark evidence,
  and claim-safe docs/website/readme surfaces.
- `docs/release/v1-local-source-package-release.md` and
  `docs/release/v1-local-source-package-release.json` are the canonical selected-track contract.
- The selected-track validator is `scripts/check_v1_local_source_package_release.py`.
- Maintainer approval existed for v0.1.0 publication through GitHub pre-release, TestPyPI, PyPI,
  and Homebrew. At that point, publication still required the channel sequence, release notes,
  checksums, SBOM/provenance/signing policy, rollback/yank/deprecate policy, and post-release smoke
  transcripts; see the 2026-06-16 completion addendum below for current channel status.

Publication completion addendum, 2026-06-16:

- The selected v0.1.8 channels are published and proof-backed: GitHub pre-release assets,
  TestPyPI, PyPI, and Homebrew.
- Channel proofs are checked in under `docs/release/channel-proofs/`.
- Future channels remain blocked: Scoop, winget, conda-forge, GHCR, and crates.io public API
  crates.
- Package access remains technical-preview install access only. It is not production readiness,
  performance superiority, Spark replacement, broad SQL/DataFrame support, object-store/lakehouse
  support, Foundry support, or fallback execution.

Patch train addendum, 2026-06-16:

- v0.1.9 is prepared as the next compatible post-v0.1.8 patch train for PR #1319 Codex review
  closeout: expression-project null-scalar admission, fail-closed arithmetic/replace scalar
  parsing, Python-style regex replacement backreference lowering, and selected-channel proof
  cleanup. It must receive its own GitHub, TestPyPI, PyPI, and Homebrew channel proofs before
  package-channel matrices or public install docs mark v0.1.9 published.
- Patch release notes live in `docs/release/v0.1.9-release-notes.md`.

- v0.1.8 is published and proof-backed through GitHub pre-release, TestPyPI, PyPI, and Homebrew.
  It is the compatible post-v0.1.7 patch train for ClickBench runtime
  readiness fields, capillary/PulseWeave memory and spill diagnostics, DataFrame future-contract
  blocker IDs, and native Vortex explode companion-column preservation after PRs #1311 and #1312.
- Patch release notes live in `docs/release/v0.1.8-release-notes.md`.

- v0.1.7 is prepared as the next compatible post-v0.1.6 patch train for release-evidence
  hardening and native Vortex runtime review polish after PRs #1307 and #1308. It must receive its
  own GitHub, TestPyPI, PyPI, and Homebrew channel proofs before package-channel matrices or public
  install docs mark v0.1.7 published.
- Patch release notes live in `docs/release/v0.1.7-release-notes.md`.

- v0.1.6 is published as a GitHub pre-release at source revision
  `3d6dca279ca965c9d128e4963273590f584956df`. TestPyPI, PyPI, Homebrew, and checked-in
  channel-proof transcript status remain governed by the selected package-channel matrix until a
  later release pass advances them.
- Patch release notes live in `docs/release/v0.1.6-release-notes.md`.

- v0.1.5 is published as a GitHub pre-release at source revision
  `4d1bd2e4d9d16af300566a87de66b9a09da51eee`. TestPyPI, PyPI, Homebrew, and checked-in
  channel-proof transcript status remain governed by the selected package-channel matrix until a
  later release pass advances them.
- Patch release notes live in `docs/release/v0.1.5-release-notes.md`.

- v0.1.4 is published and proof-backed through GitHub pre-release assets, TestPyPI, PyPI, and
  Homebrew. It is the compatible post-v0.1.3 patch train for runtime activation summaries, bundled
  CLI package resolution, provider runtime feature wiring, and release-user surface documentation.
- Patch release notes live in `docs/release/v0.1.4-release-notes.md`.

- v0.1.3 is published and proof-backed through GitHub pre-release assets, TestPyPI, PyPI, and
  Homebrew. It is the compatible post-v0.1.2 patch train for native Vortex facade route hardening,
  structural SQL/Python provider inference, exact provider-shape gating, and the release workflow
  sdist/wheel split.
- Patch release notes live in `docs/release/v0.1.3-release-notes.md`.
- v0.1.2 is prepared as the next compatible post-v0.1.1 patch train for native Vortex route
  unification, local workflow cap removal, and bundled CLI package resolution. It must
  receive its own GitHub, TestPyPI, PyPI, and Homebrew channel proofs before public install docs or
  package-channel matrices mark v0.1.2 published.
- For v0.1.2 and later bundled-CLI wheel releases, the PyPI/TestPyPI Trusted Publisher workflow must
  build artifacts from the staged package tree that includes `shardloom/bin/<platform-tag>/`; direct
  `python -m build python` artifacts are not sufficient for managed-environment package proof.
- Patch release notes live in `docs/release/v0.1.2-release-notes.md`.

- v0.1.1 is prepared as the first compatible post-v0.1.0 patch train for documentation, website,
  user-surface, package metadata, and release-process cleanup.
- v0.1.0 channel proofs remain historical proof for the already-published selected channels.
- v0.1.1 must receive its own GitHub, TestPyPI, PyPI, and Homebrew channel proofs before public
  install docs or package-channel matrices mark v0.1.1 published.
- Patch release notes live in `docs/release/v0.1.1-release-notes.md`.
- Patch release version bumps now use root `Cargo.toml` `[workspace.package].version` as the
  source of truth; run `python3 scripts/sync_workspace_package_versions.py --check` before package
  build/publish and `python3 scripts/sync_workspace_package_versions.py` after changing the root
  version.

v0.1.4 publication completion addendum, 2026-06-17:

- v0.1.4 is published and proof-backed through the selected channels: GitHub pre-release assets,
  TestPyPI, PyPI, and Homebrew.
- The release tag is `v0.1.4` at source revision
  `184ce3161f3ae280e7b17bcfc5bf2d647d6fb8b8`.
- Channel proofs are checked in as
  `docs/release/channel-proofs/github-prerelease-v0.1.4-transcript.json`,
  `docs/release/channel-proofs/testpypi-v0.1.4-transcript.json`,
  `docs/release/channel-proofs/pypi-v0.1.4-transcript.json`, and
  `docs/release/channel-proofs/homebrew-v0.1.4-transcript.json`.
- The Homebrew tap formula update is published in `depsilon/homebrew-tap` at
  `8f2d8d20b1a7e5418435def540d7b29807af9002`.
- v0.1.4 remains technical-preview install access only. It does not authorize production,
  performance superiority, Spark replacement, broad runtime, object-store/lakehouse, Foundry,
  future package-channel, or fallback-execution claims.

v0.1.3 publication completion addendum, 2026-06-16:

- v0.1.3 is published and proof-backed through the selected channels: GitHub pre-release assets,
  TestPyPI, PyPI, and Homebrew.
- The release tag is `v0.1.3` at source revision
  `c44a0a9da3f6981518753ca53e5412708c2af03c`.
- Channel proofs are checked in as
  `docs/release/channel-proofs/github-prerelease-v0.1.3-transcript.json`,
  `docs/release/channel-proofs/testpypi-v0.1.3-transcript.json`,
  `docs/release/channel-proofs/pypi-v0.1.3-transcript.json`, and
  `docs/release/channel-proofs/homebrew-v0.1.3-transcript.json`.
- The Homebrew tap formula update is published in `depsilon/homebrew-tap` at
  `6f981df273770095bb2f7dccb3fd25c09a11f8b2`.
- v0.1.3 remains technical-preview install access only. It does not authorize production,
  performance superiority, Spark replacement, broad runtime, object-store/lakehouse, Foundry,
  future package-channel, or fallback-execution claims.

v0.1.1 publication completion addendum, 2026-06-16:

- v0.1.1 is published and proof-backed through the selected channels: GitHub pre-release assets,
  TestPyPI, PyPI, and Homebrew.
- The release tag is `v0.1.1` at source revision
  `99093904d923d275072456512627110b4c0862d2`.
- Channel proofs are checked in as
  `docs/release/channel-proofs/github-prerelease-v0.1.1-transcript.json`,
  `docs/release/channel-proofs/testpypi-v0.1.1-transcript.json`,
  `docs/release/channel-proofs/pypi-v0.1.1-transcript.json`, and
  `docs/release/channel-proofs/homebrew-v0.1.1-transcript.json`.
- The Homebrew tap formula update is published in `depsilon/homebrew-tap` at
  `3030e23afa866eea77543150753fef54f9b9e338`.
- v0.1.1 remains technical-preview install access only. It does not authorize production,
  performance superiority, Spark replacement, broad runtime, object-store/lakehouse, Foundry,
  future package-channel, or fallback-execution claims.

`RELEASE-PACKAGE-15` branch evidence was prepared from local branch
`codex/release-package-15-runtime-evidence`. The refreshed benchmark publication bundle records
clean benchmark source revision `74a2e7d4f77eed0686971518e010463da26f2cdf` for that historical
handoff packet.

`RELEASE-PACKAGE-15` local audit addendum, 2026-06-13:

- Required clean Conda local dry-run proof passed in
  `target/release-readiness-audit/release-dry-run-proof-conda/transcript.json` with
  `clean_conda_env_install_status=passed`, `clean_conda_env_install_required=true`,
  `proof_status=passed`, `publication_attempted=false`, `tag_created=false`,
  `secrets_required=false`, `fallback_attempted=false`, and `external_engine_invoked=false`.
- The refreshed local hard release aggregate
  `target/release-readiness-audit/hard-release-readiness-gate-release-package-15-final.json`
  remains blocked only for public release/package claims by package-channel proof/approval,
  publication/API/schema stability approval, and per-claim evidence promotion.
  Benchmark-publication currentness is refreshed locally for source revision
  `74a2e7d4f77eed0686971518e010463da26f2cdf`.
- Target-local dependency audit evidence now passes with `pip-audit` in
  `target/release-readiness-audit/pip-audit-venv/`; this is release/security tooling only and not a
  runtime dependency.
- Live pre-5J dependency freshness passes in `target/pre-5j-dependency-freshness-gate.json`.
  `RELEASE-PACKAGE-15` regenerated and promoted the full local benchmark artifact from clean
  source revision `74a2e7d4f77eed0686971518e010463da26f2cdf`.
- The strict benchmark-publication validator now permits a clean static-publication descendant of
  the benchmarked source revision when the only post-source changes are checked-in generated
  website/public static publication artifacts, benchmark data mirrors, or phase-plan
  ledger/handoff release bookkeeping. Code, tests, scripts, benchmark harness source,
  README/public docs, and website source changes after the manifest source SHA remain currentness
  blockers.
- The current artifact completeness report
  `target/release-readiness-audit/benchmark-completeness-release-package-15-final.json` passes.
  The final publish doctor report
  `target/release-readiness-audit/benchmark-publish-doctor-release-package-15-final.json` passes
  with 1320 published rows, 600 ShardLoom claim-grade rows, no mirror drift, and
  `fallback_attempted=false` / `external_engine_invoked=false`.
- Final local release validation evidence
  `target/release-readiness-audit/release-validation-evidence-release-package-15-final.json`
  passes with required validation, feature-build matrix, and supporting security/dependency
  evidence all passed.

## Decision Summary

Maintainer approval and proof are recorded for the v0.1.8 GitHub pre-release, TestPyPI, PyPI, and
Homebrew selected-channel publication sequence. The current repository has release-candidate
evidence for build, package smoke, SBOM/checksum/provenance dry run, package-channel readiness
classification, production-usability blocking, final no-publication rehearsal, and current
benchmark-publication artifacts. Public package install claims are allowed only for the selected
proof-backed technical-preview channels.

Allowed now:

- Local no-publication rehearsal evidence.
- Local package artifact, SBOM, checksum, and provenance inspection.
- Scoped local usability evidence with `public_release_claim_allowed=false`.
- Current scoped full-local benchmark-publication evidence with public performance claims still
  disallowed.
- Selected v0.1.8 package-channel install proof for GitHub pre-release, TestPyPI, PyPI, and
  Homebrew.

Not allowed now:

- conda-forge, Scoop, winget, GHCR, or crates.io publication.
- Signing key use or public attestation generation.
- Uploading unrelated feedstocks, manifests, images, or package artifacts outside a newly approved
  release sequence.
- Production, performance, Spark-replacement, Foundry/platform, broad SQL/DataFrame, or
  object-store/lakehouse claims.

## Evidence Packet

Primary release evidence:

- `target/release-readiness-audit/release-validation-evidence-release-package-15-final.json`
- `target/release-readiness-audit/hard-release-readiness-gate-release-package-15-final.json`
- `target/release-readiness-audit/benchmark-publication-claim-gate-release-package-15-final.json`
- `target/release-readiness-audit/benchmark-completeness-release-package-15-final.json`
- `target/release-readiness-audit/benchmark-publish-doctor-release-package-15-final.json`
- `target/release-readiness-audit/compute-engine-completion-gate-release-package-15-final.json`
- `target/release-readiness-audit/release-architecture-tracker-release-package-15-final.json`
- `target/release-readiness-audit/website-readiness-release-package-15-final.json`
- `target/release-readiness-audit/release-dry-run-proof-conda/transcript.json`
- `target/release-provenance-dry-run/supply-chain-release-evidence.json`
- `target/release-provenance-dry-run/checksums.sha256`

Source release references:

- `docs/release/package-channel-readiness-matrix.md`
- `docs/release/package-channel-readiness-matrix.json`
- `docs/release/publication-api-schema-stability-gate.md`
- `docs/release/per-claim-evidence-attachment-matrix.md`
- `docs/release/final-release-rehearsal.md`
- `docs/release/hard-release-readiness-gate.md`
- `docs/release/release-provenance-dry-run.md`
- `docs/security/supply-chain-response.md`

Prepared local artifact checksums recorded by the provenance dry run:

| Artifact | Local path | SHA-256 |
| --- | --- | --- |
| CLI binary | `target/debug/shardloom` | `968dc6d21e7c328e354909c07154f9256ac6a23cf1a9274115852ec3fc872d07` |
| Python wheel | `python/dist/shardloom-0.1.0-py3-none-any.whl` | `cef48a489b1b98115e4d78566113a504ca822c75dc21f9f36034b516b7cd418c` |
| Python sdist | `python/dist/shardloom-0.1.0.tar.gz` | `8a127f93913d65a23a2035ef62349250b7fc2c4bc1d2129fe06dd69cd57833c6` |

Prepared local SBOM/checksum refs:

- `target/release-provenance-dry-run/shardloom-rust-workspace.cdx.json`
- `target/release-provenance-dry-run/shardloom-python-artifacts.cdx.json`
- `target/release-provenance-dry-run/shardloom-cli-binary.cdx.json`
- `target/release-provenance-dry-run/checksums.sha256`

These are local dry-run refs only. Build outputs under `target/` and `python/dist/` may be
overwritten by later local builds; rerun the provenance dry run at the approved release source
revision before attaching checksums to a public release. They are not publication-grade
attachments until maintainers approve the release source revision, artifact set,
signing/attestation policy, and destination channels.

## Current Blockers

The hard release gate remains blocked by:

- Package-channel readiness is no longer blocked for the selected v0.1.8 channels; GitHub
  pre-release, TestPyPI, PyPI, and Homebrew proofs are attached.
- Publication/API/schema stability: functional v1 surfaces are approved as stable for v0.1.8, but
  production compatibility, signing, future package-channel, and broad runtime claims still require
  separate proof.
- Per-claim evidence: release, package, performance, Spark-displacement, production, platform, and
  broad runtime claims remain not claim-grade.
- Channel evidence is present for the selected channels and remains missing for future package
  channels.

Current local release evidence that is no longer a hard-gate blocker:

- Architecture tracker:
  `target/release-readiness-audit/release-architecture-tracker-release-package-15-final.json`
  passes.
- Benchmark publication currentness:
  `target/release-readiness-audit/benchmark-publication-claim-gate-release-package-15-final.json`
  passes for the static-publication descendant of source revision
  `74a2e7d4f77eed0686971518e010463da26f2cdf`.
- Required validation evidence:
  `target/release-readiness-audit/release-validation-evidence-release-package-15-final.json`
  passes.
- Clean Conda proof passes as local dry-run evidence. It is not a conda-forge
  feedstock/channel proof.
- Compute-engine completion:
  `target/release-readiness-audit/compute-engine-completion-gate-release-package-15-final.json`
  passes with no top-level benchmark blockers and no residual runtime-status blockers after
  timing-surface and optimization-only status classification. Optimization-only rows for
  encoded-native promotion, source-read scout split/reuse, and Vortex reopen/verify split
  attribution remain optimization-claim blockers, not route-support or fallback blockers.

## Channel Handoff

| Channel | Current status | Remaining action required before publication/proof |
| --- | --- | --- |
| GitHub pre-release | Published/proof-backed for v0.1.8 | Release/tag/assets/checksum/SBOM/provenance download proof: `docs/release/channel-proofs/github-prerelease-v0.1.8-transcript.json`. |
| TestPyPI | Published/proof-backed for v0.1.8 | Trusted Publisher upload and clean registry install/uninstall/smoke proof: `docs/release/channel-proofs/testpypi-v0.1.8-transcript.json`. |
| PyPI | Published/proof-backed for v0.1.8 | Trusted Publisher upload after TestPyPI proof and clean public install/uninstall/smoke proof: `docs/release/channel-proofs/pypi-v0.1.8-transcript.json`. |
| Homebrew tap | Published/proof-backed for v0.1.8 | Tap/formula audit/style/test plus source build install/uninstall/smoke proof: `docs/release/channel-proofs/homebrew-v0.1.8-transcript.json`. |
| Scoop | Blocked | Approve bucket manifest, checksums, install/uninstall/smoke transcript, update/rollback policy. |
| winget | Blocked | Approve manifest/submission, installer proof, install/uninstall/smoke transcript, update/rollback policy. |
| conda-forge | Blocked | Approve staged-recipes/feedstock submission, clean feedstock install/uninstall/smoke, maintainer policy. |
| GHCR | Blocked and not included | Approve container scope, build image, generate image SBOM/provenance/vulnerability evidence, run pull/run smoke. |
| crates.io future | Blocked and not included | Extract stable public crates, approve API/schema compatibility, run `cargo publish --dry-run`, approve publication. |

## Approval Record

Publication approval for v0.1.8 is recorded in
`docs/release/final-release-approval-post-release-verification.json`. The approved channels are
GitHub pre-release, TestPyPI, PyPI, and Homebrew. Channel proof now records:

- Approved source revision and release version/tag.
- Approved destination channels and package identities.
- Approved public claims and blocked-claim wording.
- Approved checksum, SBOM, provenance, signing, and attestation policy.
- Approved rollback, yank, delete, deprecate, and advisory plan per destination channel.
- Approved secrets/OIDC/environment setup for each selected channel.
- Passing strict hard release gate for the approved source revision.

Selected-channel package install claims are now allowed for v0.1.8. Production, performance,
Spark-replacement, platform, broad runtime, future package-channel, and fallback-execution claims
remain prohibited.

## Required Re-Run Before Approval

Run these from a clean worktree at the exact source revision being approved:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
PYTHONPATH=python/src python -m unittest discover python/tests
python -m build python
python scripts/release_dry_run_proof.py --rows 64 --iterations 1
python scripts/check_package_channel_readiness.py --require-local-evidence
python scripts/final_release_rehearsal.py
python scripts/check_release_readiness.py
```

For the current release candidate, also refresh or regenerate benchmark publication evidence before
any public benchmark claim:

```bash
python scripts/check_pre_5j_dependency_freshness.py --require-live-github
python scripts/check_benchmark_artifact_completeness.py --manifest website/assets/benchmarks/latest/manifest.json
python scripts/check_benchmark_publication_claim_gate.py --manifest website/assets/benchmarks/latest/manifest.json
python scripts/check_front_door_benchmark_publication.py
```

## No-Fallback Boundary

Release and package work must preserve:

```text
fallback_attempted=false
external_engine_invoked=false
publication_attempted=false until the approved publish step
tag_created=false until the approved tag step
secrets_required=false until an approved channel-specific secret/OIDC step
```

External engines may appear only as baselines, comparison rows, or test oracles. They must not
become ShardLoom runtime dependencies or fallback execution paths.
