# CI Gate Matrix

## Purpose

`shardloom.ci_gate_matrix_report.v1` records the release-grade CI gate matrix introduced for
`REVIEW-P0-2`. The matrix makes GitHub Actions fail closed across CI work shaping, Rust,
feature-gated Rust, Python tests, package smoke, dependency/license/provenance, security,
release-readiness, website/docs, and CI drift checks.

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

The v1 security/CI hardening layer adds two lightweight compatibility producers without moving them
into the serial release-readiness tail:

- `ci_work_shaping_contract` runs the Rust-backed metadata-first changed-file planner before the
  expensive release producers, writes `target/ci-work-shaping-plan.json`, and uploads
  `ci-work-shaping-evidence`. The report carries capillary family selection, pulseweave evidence
  fingerprints, source-aware benchmark rerun recommendations, and no-fallback/claim metadata gates
  without executing runtime, benchmark, package publication, or external engine work.
- `rust_baseline` keeps the required Rust hard gate but runs `fmt`, `clippy`, and full workspace
  tests as matrix capillary lanes under the stable `rust-baseline` job id. This preserves the same
  commands and gate semantics while avoiding the old serial `fmt -> clippy -> test` runner tail.
- `python_compatibility_matrix` checks Python 3.10 through 3.13 on `ubuntu-latest` and keeps
  `macos-latest` plus `windows-latest` smoke lanes for OS matrix coverage.
- `rust_msrv_validation` derives the Rust MSRV toolchain from root `Cargo.toml` and checks it with
  default features disabled, while the existing feature matrix continues to cover current stable
  Rust and optional Vortex feature sets.
- `release_readiness_reports` runs `python scripts/check_v1_security_ci_hardening.py` and uploads
  `target/v1-security-ci-hardening-report.json` as part of the release evidence bundle.
- `release_readiness_reports` runs `python scripts/check_v1_release_boundary.py` and uploads
  `target/v1-release-boundary-report.json` so public docs, package metadata, generated support
  surfaces, package dry-run evidence, and unsupported production-family boundaries remain
  fail-closed before finished-product readiness is evaluated.
- `release_readiness_reports` runs `python scripts/check_production_certification_gate.py` and
  uploads `target/production-certification-gate.json` so production workload declarations are
  schema-valid, ShardLoom-technique reviewed, and explicitly blocked until all production evidence
  keys pass.

Compatibility marker contract:

```text
python scripts/write_ci_version_env.py --github-env "$GITHUB_ENV"
SHARDLOOM_RUST_MSRV_TOOLCHAIN
SHARDLOOM_RUST_MSRV_LANE
rustup toolchain install "$SHARDLOOM_RUST_MSRV_TOOLCHAIN"
rustup default "$SHARDLOOM_RUST_MSRV_TOOLCHAIN"
key: rust-msrv-${{ hashFiles('Cargo.toml') }}
python-version: "3.10"
python-version: "3.11"
python-version: "3.12"
python-version: "3.13"
retention-days: 14
```

Workspace version source contract:

```text
python scripts/check_workspace_version_sources.py
target/workspace-version-source-report.json
rust_msrv_source=Cargo.toml#[workspace.package].rust-version
upstream_vortex_manifest_source=Cargo.toml#[workspace.dependencies].vortex
upstream_vortex_provider_source=Cargo.toml#[workspace.dependencies].vortex via shardloom-vortex/build.rs
```

CI work-shaping marker contract:

```text
fetch-depth: 0
Collect changed files
SHARDLOOM_CI_WORK_SHAPING_MODE=pull_request
SHARDLOOM_CI_WORK_SHAPING_MODE=merge
cargo run -q -p shardloom-cli -- ci-work-shaping-plan
--changed-paths-file target/ci-work-shaping-changed-files.txt
target/ci-work-shaping-plan.json
target/ci-work-shaping-changed-files.txt
ci-work-shaping-evidence
target/downloads/ci-work-shaping-evidence
metadata-first CI work shaping
capillary changed-file selection
pulseweave evidence fingerprint
source-aware benchmark rerun recommendations
retention-days: 14
```

Finished product readiness gate:

```text
python scripts/check_finished_product_readiness.py
target/finished-product-readiness-report.json
python scripts/check_v1_release_boundary.py
target/v1-release-boundary-report.json
python scripts/check_production_certification_gate.py
target/production-certification-gate.json
```

| Lane id | GitHub job | Commands | Artifacts | Release blocker refs |
| --- | --- | --- | --- | --- |
| `ci_work_shaping_contract` | `ci-work-shaping` | `cargo run -q -p shardloom-cli -- ci-work-shaping-plan` | `target/ci-work-shaping-plan.json`<br>`target/ci-work-shaping-changed-files.txt`<br>`ci-work-shaping-evidence` | metadata-first CI work shaping; capillary changed-file selection; pulseweave evidence fingerprint; source-aware benchmark rerun recommendations |
| `rust_baseline` | `rust-baseline` | `cargo fmt --all -- --check`<br>`cargo clippy --workspace --all-targets -- -D warnings`<br>`cargo test --workspace --all-targets` | none | default Rust formatting, linting, and tests; matrix capillary lanes for fmt, clippy, and test |
| `rust_feature_matrix` | `rust-feature-matrix` | `cargo check --workspace`<br>`cargo check --workspace --all-features`<br>`cargo check --workspace --no-default-features`<br>`cargo check -p shardloom-vortex --features upstream-vortex`<br>`cargo check -p shardloom-vortex --features vortex-file-io`<br>`cargo check -p shardloom-vortex --features vortex-local-primitives`<br>`cargo check -p shardloom-vortex --features vortex-encoded-read-spike`<br>`cargo check -p shardloom-vortex --features release-user-surfaces`<br>`cargo test -p shardloom-contract-tests --test conda_packaging_recipes`<br>`cargo check -p shardloom-vortex --features vortex-traditional-analytics-benchmark` | none | workspace feature/build matrix |
| `rust_msrv_validation` | `rust-msrv` | `cargo check --workspace --no-default-features`<br>`python scripts/write_release_compatibility_lane_report.py --lane "$SHARDLOOM_RUST_MSRV_LANE" --surface rust --rust-toolchain "$SHARDLOOM_RUST_MSRV_TOOLCHAIN" --os-name ubuntu-latest` | `target/release-compatibility/rust_msrv_*.json`<br>`release-compatibility-rust-msrv` | Rust MSRV derived from root Cargo.toml validation |
| `python_test_shards` | `python-test-shards` | `python scripts/run_python_test_shard.py --shard ${{ matrix.shard }}` | `target/python-test-shards/${{ matrix.shard }}.json`<br>`python-test-shard-${{ matrix.shard }}` | Python test shards |
| `python_tests` | `python-tests` | `python -m compileall -q python/src python/tests scripts examples benchmarks/traditional_analytics`<br>`python scripts/merge_python_test_shard_evidence.py` | `python-test-shard-*`<br>`target/python-test-evidence.json`<br>`python-test-evidence` | Python tests; Python compile check |
| `python_compatibility_matrix` | `python-compatibility-matrix` | `python -m compileall -q python/src scripts examples benchmarks/traditional_analytics`<br>`python -m build python`<br>`python scripts/write_release_compatibility_lane_report.py --lane ${{ matrix.lane }} --surface python --python-version ${{ matrix.python-version }} --os-name ${{ matrix.os }}` | `target/release-compatibility/${{ matrix.lane }}.json`<br>`release-compatibility-${{ matrix.lane }}` | Python 3.10 through 3.13 compatibility; OS matrix |
| `python_package_smoke` | `python-package` | `python -m build python`<br>`python scripts/release_dry_run_proof.py --rows 8 --iterations 1 --skip-clean-conda` | `target/release-dry-run-proof/transcript.json`<br>`target/debug/shardloom`<br>`python/dist/*.whl`<br>`python/dist/*.tar.gz`<br>`release-local-smoke-evidence` | package/install smoke; local provenance dry run |
| `dependency_security` | `dependency-security` | `python scripts/check_dependency_audit.py --release-gate --json-output target/dependency-audit-report.json`<br>`python scripts/check_security_posture.py`<br>`python scripts/release_provenance_dry_run.py`<br>`python scripts/check_release_security_gate.py` | `target/dependency-audit-report.json`<br>`target/security-posture-report.json`<br>`target/release-provenance-dry-run`<br>`target/release-security-gate-report.json` | dependency/license audit; security posture; release security gate |
| `release_runtime_core_evidence` | `release-runtime-core` | `python scripts/check_golden_workflows.py`<br>`python scripts/check_admitted_semantics_matrix.py`<br>`python scripts/check_release_architecture_tracker.py --allow-blocked` | `target/golden-workflow-report.json`<br>`target/golden-workflows`<br>`target/admitted-semantics-matrix-report.json`<br>`target/admitted-semantics-matrix`<br>`target/release-architecture-tracker-report.json` | golden workflow validator; admitted semantics matrix; release architecture tracker |
| `release_package_governance_evidence` | `release-package-governance` | `python scripts/merge_release_evidence_artifacts.py`<br>`python scripts/check_contribution_governance.py`<br>`python scripts/check_workspace_version_sources.py`<br>`python scripts/check_package_channel_readiness.py --require-local-evidence` | `target/contribution-governance-report.json`<br>`target/workspace-version-source-report.json`<br>`target/package-channel-readiness-report.json` | contribution governance; workspace Rust/Vortex version source contract; package channel matrix |
| `release_user_surface_evidence` | `release-user-surface` | `python scripts/check_python_user_surface_completion.py`<br>`python scripts/check_sql_python_dataframe_parity.py`<br>`python scripts/check_v1_front_door_runtime_scope.py`<br>`python scripts/check_v1_vortex_runtime_scope.py`<br>`python scripts/check_v1_source_prepared_state_scope.py`<br>`python scripts/check_v1_local_output_sink_scope.py`<br>`python scripts/check_v1_local_resource_safety.py --skip-build`<br>`python scripts/check_v1_observability_support.py --skip-build`<br>`python scripts/check_v1_example_replay.py --profile-order debug,release --skip-build`<br>`python scripts/check_user_surface_runtime_gap_inventory.py`<br>`python scripts/check_user_surface_graduation_matrix.py`<br>`python scripts/check_runtime_gap_family_burn_down.py`<br>`python scripts/check_user_route_capability_report.py` | `target/python-user-surface-completion-gate.json`<br>`target/sql-python-dataframe-parity-gate.json`<br>`target/v1-front-door-runtime-scope-report.json`<br>`target/v1-vortex-runtime-scope-report.json`<br>`target/v1-source-prepared-state-scope-report.json`<br>`target/v1-local-output-sink-scope-report.json`<br>`target/v1-local-resource-safety-report.json`<br>`target/v1-observability-support-report.json`<br>`target/v1-example-replay-report.json`<br>`target/v1-example-replay`<br>`target/user-surface-runtime-gap-inventory.json`<br>`target/user-surface-graduation-matrix.json`<br>`target/runtime-gap-family-burn-down.json`<br>`target/user-route-capability-report.json` | Python user-surface completion gate; SQL/Python/DataFrame parity gate; v1 front-door runtime scope gate; v1 Vortex runtime scope gate; v1 SourceState/prepared-state scope gate; v1 local output/sink scope gate; v1 local resource-safety gate; v1 observability/supportability gate; v1 example replay gate; user-surface runtime gap inventory; user-surface graduation matrix; runtime gap family burn-down; user route capability report |
| `release_benchmark_claim_evidence` | `release-benchmark-claim` | `python scripts/check_pre_5j_dependency_freshness.py`<br>`python scripts/check_benchmark_artifact_completeness.py --manifest website/assets/benchmarks/latest/manifest.json --output target/benchmark-artifact-completeness-report.json`<br>`python scripts/check_benchmark_publication_claim_gate.py --manifest website/assets/benchmarks/latest/manifest.json --allow-stale-git`<br>`python scripts/check_front_door_benchmark_publication.py --manifest website/assets/benchmarks/latest/manifest.json --allow-stale-git`<br>`python scripts/check_benchmark_optimization_targets.py --artifact website/assets/benchmarks/latest/benchmark-results.json` | `target/pre-5j-dependency-freshness-gate.json`<br>`target/benchmark-artifact-completeness-report.json`<br>`target/benchmark-publication-claim-gate-report.json`<br>`target/front-door-benchmark-publication-gate.json`<br>`target/benchmark-optimization-targets-report.json` | pre-5J dependency freshness gate; benchmark artifact completeness; benchmark publication claim gate; front-door benchmark publication gate; benchmark optimization targets |
| `release_readiness_reports` | `release-readiness` | `python scripts/merge_release_evidence_artifacts.py`<br>`python scripts/final_release_rehearsal.py --allow-blocked`<br>`python scripts/check_production_usability_gate.py`<br>`python scripts/check_v1_api_schema_stability.py`<br>`python scripts/check_v1_correctness_conformance.py`<br>`python scripts/check_v1_security_ci_hardening.py`<br>`python scripts/check_v1_release_boundary.py`<br>`python scripts/check_production_certification_gate.py`<br>`python scripts/check_release_readiness.py`<br>`python scripts/check_finished_product_readiness.py` | `target/dependency-audit-report.json`<br>`target/security-posture-report.json`<br>`target/release-dry-run-proof`<br>`target/release-provenance-dry-run`<br>`target/debug/shardloom`<br>`python/dist`<br>`target/python-test-evidence.json`<br>`target/release-security-gate-report.json`<br>`target/contribution-governance-report.json`<br>`target/workspace-version-source-report.json`<br>`target/package-channel-readiness-report.json`<br>`target/golden-workflow-report.json`<br>`target/golden-workflows`<br>`target/admitted-semantics-matrix-report.json`<br>`target/admitted-semantics-matrix`<br>`target/release-architecture-tracker-report.json`<br>`target/final-release-rehearsal`<br>`target/public-status-docs-report.json`<br>`target/website-readiness-report.json`<br>`target/production-usability-gate.json`<br>`target/v1-api-schema-stability-report.json`<br>`target/v1-example-replay-report.json`<br>`target/v1-example-replay`<br>`target/v1-correctness-conformance-report.json`<br>`target/v1-security-ci-hardening-report.json`<br>`target/v1-release-boundary-report.json`<br>`target/production-certification-gate.json`<br>`target/python-user-surface-completion-gate.json`<br>`target/sql-python-dataframe-parity-gate.json`<br>`target/v1-front-door-runtime-scope-report.json`<br>`target/v1-vortex-runtime-scope-report.json`<br>`target/v1-source-prepared-state-scope-report.json`<br>`target/v1-local-output-sink-scope-report.json`<br>`target/v1-local-resource-safety-report.json`<br>`target/v1-observability-support-report.json`<br>`target/user-surface-runtime-gap-inventory.json`<br>`target/user-surface-graduation-matrix.json`<br>`target/runtime-gap-family-burn-down.json`<br>`target/user-route-capability-report.json`<br>`target/pre-5j-dependency-freshness-gate.json`<br>`target/benchmark-artifact-completeness-report.json`<br>`target/benchmark-publication-claim-gate-report.json`<br>`target/front-door-benchmark-publication-gate.json`<br>`target/benchmark-optimization-targets-report.json`<br>`target/ci-gate-matrix-report.json`<br>`target/hard-release-readiness-gate.json`<br>`target/finished-product-readiness-report.json` | final rehearsal; production usability gate; v1 API/schema stability gate; v1 correctness/conformance gate; v1 security/CI hardening gate; v1 release boundary firewall; production certification gate; hard release readiness gate; finished product readiness gate; release readiness artifact aggregation |
| `website_docs_validation` | `website-docs` | `npm audit --audit-level=low`<br>`npm run build`<br>`npm run check`<br>`python scripts/check_public_status_docs.py`<br>`python scripts/check_website_readiness.py`<br>`node website/validate_static_assets.js` | `target/public-status-docs-report.json`<br>`target/website-readiness-report.json` | website dependency advisory gate; website build; public status docs; docs/status generated assets |
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

- `rust-baseline` keeps the branch-protection job id while using a `fail-fast: false` matrix with
  `fmt`, `clippy`, and `test` lanes. The commands are unchanged, but the workflow no longer waits
  for formatting before starting lint and full workspace tests.
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
  continuity and emits `release-local-smoke-evidence` with the compact dry-run transcript plus the
  provenance-referenced wheel, sdist, and local CLI binary. It does not upload the package-stage
  venv or full local provenance directories.
- `release-runtime-core` emits `release-runtime-core-evidence` for golden workflow, admitted
  semantics, and architecture tracker reports.
- `release-package-governance` has `needs:` entries for `dependency-security` and `python-package`,
  downloads `dependency-security-evidence` and `release-local-smoke-evidence` into
  `target/downloads`, runs `Merge package/governance input evidence` through
  `python scripts/merge_release_evidence_artifacts.py`, then emits
  `release-package-governance-evidence`.
- `release-user-surface` has `needs:` entries for `python-package` and `release-runtime-core`,
  downloads `release-local-smoke-evidence` and `release-runtime-core-evidence` under
  `target/downloads`, runs `Merge local package smoke evidence`, then emits
  `release-user-surface-evidence` for Python, SQL/DataFrame, v1 local resource safety, v1
  observability/supportability, v1 example replay, runtime gap, graduation, burn-down, and
  route-capability reports.
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
  Missing or zeroed optimization targets are reported as retired/diagnostic instead of release
  blockers; invalid ShardLoom rows, fallback attempts, and external-engine execution still fail the
  lane.
- `website-docs` emits `website-docs-evidence` with
  `target/public-status-docs-report.json` and `target/website-readiness-report.json`.
- `ci-work-shaping` emits `ci-work-shaping-evidence` with the Rust CLI envelope and changed-file
  manifest. The job is metadata-only and records `runtime_execution=false`,
  `benchmark_run_performed=false`, `fallback_attempted=false`, and
  `external_engine_invoked=false`. It does not make any check optional by itself.
- `release-readiness` has `needs:` entries for `ci-work-shaping`, `ci-gate-matrix`, `dependency-security`,
  `python-tests`, `python-package`, `release-runtime-core`, `release-package-governance`,
  `release-user-surface`, `release-benchmark-claim`, and `website-docs`. It downloads
  `ci-work-shaping-evidence`, `ci-gate-matrix-report`, `dependency-security-evidence`, `release-local-smoke-evidence`,
  `python-test-evidence`, `release-runtime-core-evidence`, `release-package-governance-evidence`,
  `release-user-surface-evidence`, `release-benchmark-claim-evidence`, and
  `website-docs-evidence` with `actions/download-artifact@v8` under `target/downloads`, runs
  `Merge downloaded release evidence` through `python scripts/merge_release_evidence_artifacts.py`,
  then runs `Verify downloaded release evidence` before the aggregate gates. The production,
  v1 API/schema stability, v1 correctness/conformance, and hard-readiness aggregate scripts consume
  precomputed evidence and reports when present and fall back to direct manifest scans only for
  local runs without the reports.

These explicit artifacts keep the final `release-readiness` job focused on final rehearsal,
production usability, v1 API/schema stability, v1 correctness/conformance, hard release readiness,
and release readiness artifact aggregation instead of serially regenerating every upstream report.
The strict existence check makes artifact wiring a fast failure without adding another long
producer command to the final job.

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
