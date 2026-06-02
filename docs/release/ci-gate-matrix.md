# CI Gate Matrix

## Purpose

`shardloom.ci_gate_matrix_report.v1` records the release-grade CI gate matrix introduced for
`REVIEW-P0-2`. The matrix makes GitHub Actions fail closed across Rust, feature-gated Rust,
Python, package smoke, dependency/license/provenance, security, release-readiness, website/docs,
and CI drift checks.

This is a release and trust gate only. It does not publish packages, create a release tag, use
signing keys, upload artifacts to package channels, expand runtime support, or authorize production,
performance, Spark-replacement, object-store, lakehouse, Foundry, broad SQL, or broad DataFrame
claims.

```text
public_release_claim_allowed=false
public_package_claim_allowed=false
publication_attempted=false
tag_created=false
secrets_required=false
package_upload_attempted=false
fallback_attempted=false
external_engine_invoked=false
skipped_gate=clean_conda_release_environment
skipped_gate=real_publication
```

## Required Lanes

| Lane id | GitHub job | Commands | Artifacts | Release blocker refs |
| --- | --- | --- | --- | --- |
| `rust_baseline` | `rust-baseline` | `cargo fmt --all -- --check`<br>`cargo clippy --workspace --all-targets -- -D warnings`<br>`cargo test --workspace --all-targets` | none | default Rust formatting, linting, and tests |
| `rust_feature_matrix` | `rust-feature-matrix` | `cargo check --workspace`<br>`cargo check --workspace --all-features`<br>`cargo check --workspace --no-default-features`<br>`cargo check -p shardloom-vortex --features upstream-vortex`<br>`cargo check -p shardloom-vortex --features vortex-file-io`<br>`cargo check -p shardloom-vortex --features vortex-local-primitives`<br>`cargo check -p shardloom-vortex --features vortex-encoded-read-spike`<br>`cargo test -p shardloom-contract-tests --test conda_packaging_recipes`<br>`cargo check -p shardloom-vortex --features vortex-traditional-analytics-benchmark` | none | workspace feature/build matrix |
| `python_package_smoke` | `python-package` | `python -m unittest discover -s python/tests`<br>`python -m compileall -q python/src python/tests scripts examples benchmarks/traditional_analytics`<br>`python -m build python`<br>`python scripts/release_dry_run_proof.py --rows 8 --iterations 1 --skip-clean-conda` | `python/dist`<br>`target/release-dry-run-proof`<br>`target/release-provenance-dry-run` | Python tests; package/install smoke; local provenance dry run |
| `dependency_security` | `dependency-security` | `python scripts/check_dependency_audit.py --release-gate --json-output target/dependency-audit-report.json`<br>`python scripts/check_security_posture.py`<br>`python scripts/release_provenance_dry_run.py`<br>`python scripts/check_release_security_gate.py` | `target/dependency-audit-report.json`<br>`target/security-posture-report.json`<br>`target/release-provenance-dry-run`<br>`target/release-security-gate-report.json` | dependency/license audit; security posture; release security gate |
| `release_readiness_reports` | `release-readiness` | `python scripts/check_dependency_audit.py --release-gate --json-output target/dependency-audit-report.json`<br>`python scripts/check_security_posture.py`<br>`python scripts/release_dry_run_proof.py --rows 8 --iterations 1 --skip-clean-conda`<br>`python scripts/check_release_security_gate.py`<br>`python scripts/check_contribution_governance.py`<br>`python scripts/check_package_channel_readiness.py --require-local-evidence`<br>`python scripts/check_golden_workflows.py`<br>`python scripts/check_admitted_semantics_matrix.py`<br>`python scripts/check_release_architecture_tracker.py --allow-blocked`<br>`python scripts/final_release_rehearsal.py --allow-blocked`<br>`python scripts/check_website_readiness.py`<br>`python scripts/check_production_usability_gate.py`<br>`python scripts/check_python_user_surface_completion.py`<br>`python scripts/check_sql_python_dataframe_parity.py`<br>`python scripts/check_user_surface_runtime_gap_inventory.py`<br>`python scripts/check_user_surface_graduation_matrix.py`<br>`python scripts/check_runtime_gap_family_burn_down.py`<br>`python scripts/check_user_route_capability_report.py`<br>`python scripts/check_pre_5j_dependency_freshness.py`<br>`python scripts/check_benchmark_publication_claim_gate.py --manifest website/assets/benchmarks/latest/manifest.json`<br>`python scripts/check_ci_gate_matrix.py`<br>`python scripts/check_release_readiness.py` | `target/dependency-audit-report.json`<br>`target/security-posture-report.json`<br>`target/release-dry-run-proof`<br>`target/release-provenance-dry-run`<br>`target/release-security-gate-report.json`<br>`target/contribution-governance-report.json`<br>`target/package-channel-readiness-report.json`<br>`target/golden-workflow-report.json`<br>`target/golden-workflows`<br>`target/admitted-semantics-matrix-report.json`<br>`target/admitted-semantics-matrix`<br>`target/release-architecture-tracker-report.json`<br>`target/final-release-rehearsal`<br>`target/website-readiness-report.json`<br>`target/production-usability-gate.json`<br>`target/python-user-surface-completion-gate.json`<br>`target/sql-python-dataframe-parity-gate.json`<br>`target/user-surface-runtime-gap-inventory.json`<br>`target/user-surface-graduation-matrix.json`<br>`target/runtime-gap-family-burn-down.json`<br>`target/user-route-capability-report.json`<br>`target/pre-5j-dependency-freshness-gate.json`<br>`target/benchmark-publication-claim-gate-report.json`<br>`target/ci-gate-matrix-report.json`<br>`target/hard-release-readiness-gate.json` | release readiness scripts; contribution governance; package channel matrix; golden workflow validator; admitted semantics matrix; final rehearsal; production usability gate; Python user-surface completion gate; SQL/Python/DataFrame parity gate; user-surface runtime gap inventory; user-surface graduation matrix; runtime gap family burn-down; user route capability report; pre-5J dependency freshness gate; benchmark publication claim gate |
| `website_docs_validation` | `website-docs` | `npm run build`<br>`npm run check`<br>`python scripts/check_website_readiness.py`<br>`node website/validate_static_assets.js` | generated static website under `website/` | website build; docs/status generated assets |
| `ci_gate_matrix_contract` | `ci-gate-matrix` | `python scripts/check_ci_gate_matrix.py` | `target/ci-gate-matrix-report.json` | CI matrix drift contract |

## Failure Policy

Every lane above is release-blocking for the PR, except that the `release-readiness` job treats
`python scripts/check_release_readiness.py` as evidence collection while release blockers remain.
That step must run without `--allow-blocked`, emit `target/hard-release-readiness-gate.json`, and
use `continue-on-error: true` in CI so ordinary PRs can merge while public-release claims stay
blocked. The gate intentionally accepts the current blocked package/release posture when the
scripts report a coherent blocked state with
`publication_attempted=false`, `tag_created=false`, `secrets_required=false`,
`fallback_attempted=false`, and `external_engine_invoked=false`.

The following gates remain skipped by design until maintainers explicitly enter a real release
process:

- `clean_conda_release_environment`: the CI package smoke runs a clean virtual environment and
  records the Conda environment proof as skipped.
- `real_publication`: no GitHub release, TestPyPI/PyPI upload, Homebrew/Scoop/Winget/Conda/GHCR
  channel submission, crates.io publication, release tag, signing key, or package-channel secret is
  used by CI.

## Local Validator

Run:

```bash
python scripts/check_ci_gate_matrix.py
```

The validator writes `target/ci-gate-matrix-report.json`, checks that `.github/workflows/ci.yml`
contains each lane and command, checks this document for command/artifact/blocker drift, and keeps
public release/package claims blocked.
