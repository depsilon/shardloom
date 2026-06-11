# CI Gate Matrix

## Purpose

`shardloom.ci_gate_matrix_report.v1` records the release-grade CI gate matrix introduced for
`REVIEW-P0-2`. The matrix makes GitHub Actions fail closed across Rust, feature-gated Rust,
Python tests, package smoke, dependency/license/provenance, security, release-readiness,
website/docs, and CI drift checks.

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

The release evidence path is intentionally split. Producer jobs run independent checks in
parallel; `release-readiness` downloads their artifacts and runs only the final aggregate gates.
This keeps the hard gate strict without making every PR wait for a single serial evidence stack.
Python unit validation is split into parallel shards and then aggregated by the branch-protection
compatible `Python tests` check. `Python and package smoke` remains the local package/dry-run lane.
The workflow grants `pull-requests: read` and passes the scoped Actions token only to the live
pre-5J dependency freshness step so the Dependabot PR query is authenticated without write scope or
package/release authority.

| Lane id | GitHub job | Commands | Artifacts | Release blocker refs |
| --- | --- | --- | --- | --- |
| `rust_baseline` | `rust-baseline` | `cargo fmt --all -- --check`<br>`cargo clippy --workspace --all-targets -- -D warnings`<br>`cargo test --workspace --all-targets` | none | default Rust formatting, linting, and tests |
| `rust_feature_matrix` | `rust-feature-matrix` | `cargo check --workspace`<br>`cargo check --workspace --all-features`<br>`cargo check --workspace --no-default-features`<br>`cargo check -p shardloom-vortex --features upstream-vortex`<br>`cargo check -p shardloom-vortex --features vortex-file-io`<br>`cargo check -p shardloom-vortex --features vortex-local-primitives`<br>`cargo check -p shardloom-vortex --features vortex-encoded-read-spike`<br>`cargo test -p shardloom-contract-tests --test conda_packaging_recipes`<br>`cargo check -p shardloom-vortex --features vortex-traditional-analytics-benchmark` | none | workspace feature/build matrix |
| `python_test_shards` | `python-test-shards` | `python scripts/run_python_test_shard.py --shard ${{ matrix.shard }}` | `target/python-test-shards/${{ matrix.shard }}.json`<br>`python-test-shard-${{ matrix.shard }}` | Python test shards |
| `python_tests` | `python-tests` | `python -m compileall -q python/src python/tests scripts examples benchmarks/traditional_analytics`<br>`python scripts/merge_python_test_shard_evidence.py` | `python-test-shard-*`<br>`target/python-test-evidence.json`<br>`python-test-evidence` | Python tests; Python compile check |
| `python_package_smoke` | `python-package` | `python -m build python`<br>`python scripts/release_dry_run_proof.py --rows 8 --iterations 1 --skip-clean-conda` | `python/dist`<br>`target/debug/shardloom`<br>`target/release-dry-run-proof`<br>`target/release-provenance-dry-run`<br>`release-local-smoke-evidence` | package/install smoke; local provenance dry run |
| `dependency_security` | `dependency-security` | `python scripts/check_dependency_audit.py --release-gate --json-output target/dependency-audit-report.json`<br>`python scripts/check_security_posture.py`<br>`python scripts/release_provenance_dry_run.py`<br>`python scripts/check_release_security_gate.py` | `target/dependency-audit-report.json`<br>`target/security-posture-report.json`<br>`target/release-provenance-dry-run`<br>`target/release-security-gate-report.json` | dependency/license audit; security posture; release security gate |
| `release_runtime_core_evidence` | `release-runtime-core` | `python scripts/check_golden_workflows.py`<br>`python scripts/check_admitted_semantics_matrix.py`<br>`python scripts/check_release_architecture_tracker.py --allow-blocked` | `target/golden-workflow-report.json`<br>`target/golden-workflows`<br>`target/admitted-semantics-matrix-report.json`<br>`target/admitted-semantics-matrix`<br>`target/release-architecture-tracker-report.json` | golden workflow validator; admitted semantics matrix; release architecture tracker |
| `release_package_governance_evidence` | `release-package-governance` | `python scripts/merge_release_evidence_artifacts.py`<br>`python scripts/check_contribution_governance.py`<br>`python scripts/check_package_channel_readiness.py --require-local-evidence` | `target/contribution-governance-report.json`<br>`target/package-channel-readiness-report.json` | contribution governance; package channel matrix |
| `release_user_surface_evidence` | `release-user-surface` | `python scripts/check_python_user_surface_completion.py`<br>`python scripts/check_sql_python_dataframe_parity.py`<br>`python scripts/check_user_surface_runtime_gap_inventory.py`<br>`python scripts/check_user_surface_graduation_matrix.py`<br>`python scripts/check_runtime_gap_family_burn_down.py`<br>`python scripts/check_user_route_capability_report.py` | `target/python-user-surface-completion-gate.json`<br>`target/sql-python-dataframe-parity-gate.json`<br>`target/user-surface-runtime-gap-inventory.json`<br>`target/user-surface-graduation-matrix.json`<br>`target/runtime-gap-family-burn-down.json`<br>`target/user-route-capability-report.json` | Python user-surface completion gate; SQL/Python/DataFrame parity gate; user-surface runtime gap inventory; user-surface graduation matrix; runtime gap family burn-down; user route capability report |
| `release_benchmark_claim_evidence` | `release-benchmark-claim` | `python scripts/check_pre_5j_dependency_freshness.py`<br>`python scripts/check_benchmark_artifact_completeness.py --manifest website/assets/benchmarks/latest/manifest.json --output target/benchmark-artifact-completeness-report.json`<br>`python scripts/check_benchmark_publication_claim_gate.py --manifest website/assets/benchmarks/latest/manifest.json`<br>`python scripts/check_front_door_benchmark_publication.py --manifest website/assets/benchmarks/latest/manifest.json`<br>`python scripts/check_benchmark_optimization_targets.py --artifact website/assets/benchmarks/latest/benchmark-results.json` | `target/pre-5j-dependency-freshness-gate.json`<br>`target/benchmark-artifact-completeness-report.json`<br>`target/benchmark-publication-claim-gate-report.json`<br>`target/front-door-benchmark-publication-gate.json`<br>`target/benchmark-optimization-targets-report.json` | pre-5J dependency freshness gate; benchmark artifact completeness; benchmark publication claim gate; front-door benchmark publication gate; benchmark optimization targets |
| `release_readiness_reports` | `release-readiness` | `python scripts/merge_release_evidence_artifacts.py`<br>`python scripts/final_release_rehearsal.py --allow-blocked`<br>`python scripts/check_production_usability_gate.py`<br>`python scripts/check_release_readiness.py` | `target/dependency-audit-report.json`<br>`target/security-posture-report.json`<br>`target/release-dry-run-proof`<br>`target/release-provenance-dry-run`<br>`target/debug/shardloom`<br>`python/dist`<br>`target/python-test-evidence.json`<br>`target/release-security-gate-report.json`<br>`target/contribution-governance-report.json`<br>`target/package-channel-readiness-report.json`<br>`target/golden-workflow-report.json`<br>`target/golden-workflows`<br>`target/admitted-semantics-matrix-report.json`<br>`target/admitted-semantics-matrix`<br>`target/release-architecture-tracker-report.json`<br>`target/final-release-rehearsal`<br>`target/public-status-docs-report.json`<br>`target/website-readiness-report.json`<br>`target/production-usability-gate.json`<br>`target/python-user-surface-completion-gate.json`<br>`target/sql-python-dataframe-parity-gate.json`<br>`target/user-surface-runtime-gap-inventory.json`<br>`target/user-surface-graduation-matrix.json`<br>`target/runtime-gap-family-burn-down.json`<br>`target/user-route-capability-report.json`<br>`target/pre-5j-dependency-freshness-gate.json`<br>`target/benchmark-artifact-completeness-report.json`<br>`target/benchmark-publication-claim-gate-report.json`<br>`target/front-door-benchmark-publication-gate.json`<br>`target/benchmark-optimization-targets-report.json`<br>`target/ci-gate-matrix-report.json`<br>`target/hard-release-readiness-gate.json` | final rehearsal; production usability gate; hard release readiness gate; release readiness artifact aggregation |
| `website_docs_validation` | `website-docs` | `npm run build`<br>`npm run check`<br>`python scripts/check_public_status_docs.py`<br>`python scripts/check_website_readiness.py`<br>`node website/validate_static_assets.js` | `target/public-status-docs-report.json`<br>`target/website-readiness-report.json` | website build; public status docs; docs/status generated assets |
| `ci_gate_matrix_contract` | `ci-gate-matrix` | `python scripts/check_ci_gate_matrix.py` | `target/ci-gate-matrix-report.json` | CI matrix drift contract |

## Failure Policy

The CI workflow keeps a repository-wide `concurrency:` policy with `cancel-in-progress: true` so a
new push to the same pull request cancels stale runs instead of consuming the slow release-readiness
tail. Workflow permissions include `actions: read` so optimized jobs can download same-run evidence
artifacts while keeping `contents: read` as the only repository content permission.

Every lane above is release-blocking for the PR, except that the `release-readiness` job treats
`python scripts/check_release_readiness.py` as evidence collection while release blockers remain.
That step must run without `--allow-blocked`, emit `target/hard-release-readiness-gate.json`, and
use `continue-on-error: true` in CI so ordinary PRs can merge while public-release claims stay
blocked. The gate intentionally accepts the current blocked package/release posture when the
scripts report a coherent blocked state with
`publication_attempted=false`, `tag_created=false`, `secrets_required=false`,
`fallback_attempted=false`, and `external_engine_invoked=false`.

The release hard-gate stack is split into parallel producers and a short final aggregate:

- `python-test-shards` runs `core`, `front_door_benchmark_publication`, and `release_scripts`
  shards in parallel with `fail-fast: false`. The split isolates the two measured slow modules:
  `python.tests.test_release_scripts` and `python.tests.test_front_door_benchmark_publication`.
- `python-tests` depends on `python-test-shards`, uses `if: always()` so failed or missing shards
  still produce a stable aggregate check, downloads `python-test-shard-*` with
  `actions/download-artifact@v8` and `merge-multiple: true`, runs compileall, and emits the stable
  `python-test-evidence` artifact with `target/python-test-evidence.json` through
  `python scripts/merge_python_test_shard_evidence.py`. The merge script proves that discovered
  `python/tests/test_*.py` modules are covered exactly once, so the aggregate check remains
  equivalent to full discovery without requiring the slow modules to run serially.
- `python-package` keeps the GitHub check name `Python and package smoke` for branch-protection
  continuity and emits `release-local-smoke-evidence` with the dry-run transcript, local
  provenance, `target/debug/shardloom`, and `python/dist` so the release path does not rerun
  `release_dry_run_proof.py` and downstream provenance refs resolve in fresh jobs.
- `release-runtime-core` emits `release-runtime-core-evidence` for golden workflow, admitted
  semantics, and architecture tracker reports.
- `release-package-governance` has `needs:` entries for `dependency-security` and `python-package`,
  downloads `dependency-security-evidence` and `release-local-smoke-evidence` into
  `target/downloads`, runs `Merge package/governance input evidence` through
  `python scripts/merge_release_evidence_artifacts.py`, then emits
  `release-package-governance-evidence`.
- `release-user-surface` has a `needs:` entry for `python-package`, downloads
  `release-local-smoke-evidence` under `target/downloads`, runs
  `Merge local package smoke evidence`, then emits `release-user-surface-evidence` for Python,
  SQL/DataFrame, runtime gap, graduation, burn-down, and route-capability reports.
- `release-benchmark-claim` emits `release-benchmark-claim-evidence`; it precomputes
  `target/benchmark-artifact-completeness-report.json` once so downstream aggregate gates can
  consume the report instead of rescanning the large public benchmark bundle. The report carries
  manifest and benchmark JSON SHA-256 digests, and aggregate gates verify those digests before
  trusting the precomputed result. Its benchmark publication claim step stays
  `continue-on-error: true` while public performance claims remain gated. The front-door benchmark
  publication gate must still pass structurally and keeps SQL/Python/DataFrame performance
  equivalence blocked until measured equivalent front-door rows and rerun approval exist.
  The lane also emits `target/benchmark-optimization-targets-report.json`, which is diagnostic-only
  evidence for the current hot-runtime bottleneck queue and does not authorize a performance claim.
- `website-docs` emits `website-docs-evidence` with
  `target/public-status-docs-report.json` and `target/website-readiness-report.json`.
- `release-readiness` has `needs:` entries for `ci-gate-matrix`, `dependency-security`,
  `python-tests`, `python-package`, `release-runtime-core`, `release-package-governance`,
  `release-user-surface`, `release-benchmark-claim`, and `website-docs`. It downloads
  `ci-gate-matrix-report`, `dependency-security-evidence`, `release-local-smoke-evidence`,
  `python-test-evidence`, `release-runtime-core-evidence`, `release-package-governance-evidence`,
  `release-user-surface-evidence`, `release-benchmark-claim-evidence`, and
  `website-docs-evidence` with `actions/download-artifact@v8` under `target/downloads`, runs
  `Merge downloaded release evidence` through `python scripts/merge_release_evidence_artifacts.py`,
  then runs `Verify downloaded release evidence` before the aggregate gates. The production and
  hard-readiness aggregate scripts consume the precomputed benchmark completeness/publication
  reports when present and fall back to direct manifest scans only for local runs without the
  reports.

These explicit artifacts keep the final `release-readiness` job focused on final rehearsal,
production usability, hard release readiness, and release readiness artifact aggregation instead of
serially regenerating every upstream report. The strict existence check makes artifact wiring a
fast failure without adding another long producer command to the final job.

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
