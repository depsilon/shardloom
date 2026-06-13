<!-- SPDX-License-Identifier: Apache-2.0 -->

# Maintainer Publication Handoff

Status: release handoff packet for `RELEASE-SEQUENCE-14`. This document does not approve
publication, create tags, publish packages, sign artifacts, upload SBOMs, submit package-channel
manifests, add secrets, or authorize fallback execution.

Date: 2026-06-11

Current branch evidence was prepared from local branch
`codex/compute-engine-remaining-6d-closeout` at `a7176479`. The branch has local changes, so the
current checked-in benchmark publication bundle is not fresh for public benchmark claims.

Current local audit addendum, 2026-06-13:

- Required clean Conda local dry-run proof passed in
  `target/release-readiness-audit/release-dry-run-proof-conda/transcript.json` with
  `clean_conda_env_install_status=passed`, `clean_conda_env_install_required=true`,
  `proof_status=passed`, `publication_attempted=false`, `tag_created=false`,
  `secrets_required=false`, `fallback_attempted=false`, and `external_engine_invoked=false`.
- The refreshed local hard release aggregate
  `target/release-readiness-audit/hard-release-readiness-gate-current-final.json` remains blocked
  for public release/package claims by package-channel proof/approval, publication/API/schema
  stability approval, per-claim evidence promotion, and strict clean-source benchmark-publication
  validation for the exact source revision.
- Target-local dependency audit evidence now passes with `pip-audit` in
  `target/release-readiness-audit/pip-audit-venv/`; this is release/security tooling only and not a
  runtime dependency.
- Live pre-5J dependency freshness now passes in `target/pre-5j-dependency-freshness-gate.json`.
  The remaining strict benchmark-publication blocker is source currentness: the checked-in
  publication bundle was generated from `a693e299988830b0587d66df0f088a80b6038f75`, while the
  current local source revision is `173f88c25b36736aa51a6c50bafe0c6ec9bf5fed` with tracked local
  changes. This is tracked as `RELEASE-PACKAGE-15` in the phase plan.
- The strict benchmark-publication validator now permits a clean static-publication descendant of
  the benchmarked source revision when the only post-source changes are checked-in generated
  website/public static publication artifacts, benchmark data mirrors, or phase-plan
  ledger/handoff release bookkeeping. Code, tests, scripts, benchmark harness source,
  README/public docs, and website source changes after the manifest source SHA remain currentness
  blockers.
- The current strict report with that contract is
  `target/release-readiness-audit/benchmark-publication-claim-gate-strict-after-static-descendant-contract.json`;
  it remains blocked with `git_currentness_status=blocked_mismatched_source_revision` and
  `worktree_dirty=true`, while preserving `fallback_attempted=false` and
  `external_engine_invoked=false`.

## Decision Summary

Nothing is approved for public publication yet.

The current repository has local release-candidate evidence for build, package smoke,
SBOM/checksum/provenance dry run, package-channel readiness classification, production-usability
blocking, and final no-publication rehearsal. The hard release gate remains blocked, and all public
release/package/performance/production/platform claims remain disallowed.

Allowed now:

- Local no-publication rehearsal evidence.
- Local package artifact, SBOM, checksum, and provenance inspection.
- Scoped local usability evidence with `public_release_claim_allowed=false`.
- Package-channel planning and maintainer review.

Not allowed now:

- GitHub Release or release tag creation.
- PyPI, TestPyPI, conda-forge, Homebrew, Scoop, winget, GHCR, or crates.io publication.
- Signing key use or public attestation generation.
- Uploading SBOMs, checksums, assets, feedstocks, manifests, images, or package artifacts.
- Public API/schema stability, production, performance, Spark-replacement, Foundry/platform, broad
  SQL/DataFrame, object-store/lakehouse, or package-availability claims.

## Evidence Packet

Primary release evidence:

- `target/release-validation-evidence-rs13-configured.json`
- `target/hard-release-readiness-gate-rs13-configured.json`
- `target/final-release-rehearsal/final-release-rehearsal-report.json`
- `target/final-release-rehearsal/local-publication-attestation-plan.json`
- `target/release-dry-run-proof/transcript.json`
- `target/release-provenance-dry-run/supply-chain-release-evidence.json`
- `target/release-provenance-dry-run/checksums.sha256`
- `target/compute-engine-completion-gate-rs13.json`

Source release references:

- `docs/release/package-channel-readiness-matrix.md`
- `docs/release/package-channel-readiness-matrix.json`
- `docs/release/publication-api-schema-stability-gate.md`
- `docs/release/per-claim-evidence-attachment-matrix.md`
- `docs/release/final-release-rehearsal.md`
- `docs/release/hard-release-readiness-gate.md`
- `docs/release/release-provenance-dry-run.md`
- `docs/security/supply-chain-response.md`

Prepared local artifacts from the provenance dry run:

| Artifact | Local path | SHA-256 |
| --- | --- | --- |
| CLI binary | `target/debug/shardloom` | `b8e5df1ac6e3070dcc49cde2b66adb4fb40f4b7274125ef14badc039f2ce2269` |
| Python wheel | `python/dist/shardloom-0.1.0.dev0-py3-none-any.whl` | `e853340dfcb5801ccb83931cf7e70e8b05189f8f9bbdc20f91801c2b55bc2d2e` |
| Python sdist | `python/dist/shardloom-0.1.0.dev0.tar.gz` | `72b08b41a5ab750c5261a49b4e7774db6414ba81ea5e4a4bdff7cc2390b5cb85` |

Prepared local SBOM/checksum refs:

- `target/release-provenance-dry-run/shardloom-rust-workspace.cdx.json`
- `target/release-provenance-dry-run/shardloom-python-artifacts.cdx.json`
- `target/release-provenance-dry-run/shardloom-cli-binary.cdx.json`
- `target/release-provenance-dry-run/checksums.sha256`

These are local dry-run refs only. They are not publication-grade attachments until maintainers
approve the release source revision, artifact set, signing/attestation policy, and destination
channels.

## Current Blockers

The hard release gate remains blocked by:

- Package-channel readiness: every configured public channel remains blocked.
- Publication/API/schema stability: no public API/schema compatibility window is approved.
- Per-claim evidence: release, package, performance, Spark-displacement, production, platform, and
  broad runtime claims remain not claim-grade.
- Architecture tracker: currently passed for release tracking except for the explicitly open
  `RELEASE-PACKAGE-15` clean-source benchmark-publication item.
- Benchmark freshness: the promoted benchmark manifest records source revision
  `a693e299988830b0587d66df0f088a80b6038f75`, while current local source has moved to
  `173f88c25b36736aa51a6c50bafe0c6ec9bf5fed` with tracked local changes.
- Clean Conda proof: current local audit evidence now passes, but it is still local dry-run proof,
  not a conda-forge feedstock/channel proof.
- Required validation evidence: strict release validation has current local evidence in
  `target/release-readiness-audit/release-validation-evidence-conda-pip-audit-current.json`; the
  only release-blocking command there is the strict benchmark publication claim gate, which still
  needs a clean-worktree passing run at the exact source revision or a clean static-publication
  descendant containing only generated website/public static publication artifacts, benchmark data
  mirrors, and phase-plan ledger/handoff release bookkeeping.
- Benchmark publication currentness: `RELEASE-PACKAGE-15` must refresh/promote benchmark
  publication artifacts from the exact clean source revision before public benchmark/release
  claims. A separate static-publication commit is acceptable only if the strict report records
  `git_currentness_status=static_publication_descendant` and no non-publication delta paths.
- Human approval: no maintainer has approved publication, signing, tagging, package-channel upload,
  feedstock submission, release-asset upload, or public attestation.

The current compute-engine completion gate passes with no top-level benchmark blockers and no
residual runtime-status blockers after timing-surface and optimization-only status classification.
It still reports optimization-only rows for encoded-native promotion, source-read scout split/reuse,
and Vortex reopen/verify split attribution. Those rows are optimization claim blockers, not route
support or fallback blockers.

## Channel Handoff

| Channel | Current status | Maintainer action required before publication |
| --- | --- | --- |
| GitHub pre-release | Blocked | Approve tag/release, attach assets/checksums/SBOM/provenance, run channel download smoke, approve rollback/delete policy. |
| TestPyPI | Blocked | Configure Trusted Publisher or scoped credential proof, approve upload, run clean registry install/uninstall/smoke. |
| PyPI | Blocked | Configure Trusted Publisher/OIDC, approve package identity, approve upload, run clean public install/uninstall/smoke, approve yank policy. |
| Homebrew tap | Blocked | Approve tap/formula, versioned checksums, install/uninstall/smoke transcript, rollback/deprecate policy. |
| Scoop | Blocked | Approve bucket manifest, checksums, install/uninstall/smoke transcript, update/rollback policy. |
| winget | Blocked | Approve manifest/submission, installer proof, install/uninstall/smoke transcript, update/rollback policy. |
| conda-forge | Blocked | Approve staged-recipes/feedstock submission, clean feedstock install/uninstall/smoke, maintainer policy. |
| GHCR | Blocked and not included | Approve container scope, build image, generate image SBOM/provenance/vulnerability evidence, run pull/run smoke. |
| crates.io future | Blocked and not included | Extract stable public crates, approve API/schema compatibility, run `cargo publish --dry-run`, approve publication. |

## Approval Record

Publication is allowed only after maintainers explicitly record all selected approvals below in the
release issue, PR, or release checklist:

- Approved source revision and release version/tag.
- Approved destination channels and package identities.
- Approved public claims and blocked-claim wording.
- Approved checksum, SBOM, provenance, signing, and attestation policy.
- Approved rollback, yank, delete, deprecate, and advisory plan per destination channel.
- Approved secrets/OIDC/environment setup for each selected channel.
- Passing strict hard release gate for the approved source revision.

Until that approval exists, all publish commands remain prohibited.

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
