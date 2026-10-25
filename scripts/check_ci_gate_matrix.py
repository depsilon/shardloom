#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the GitHub Actions release-grade CI gate matrix."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_report_utils import (
    fail_closed_fields,
    read_text,
    resolve_path,
    write_json,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.ci_gate_matrix_report.v1"


@dataclass(frozen=True)
class CiLane:
    lane_id: str
    job_id: str
    commands: tuple[str, ...]
    artifact_refs: tuple[str, ...]
    release_blocker_refs: tuple[str, ...]
    workflow_markers: tuple[str, ...] = ()
    no_fallback_required: bool = True


REQUIRED_LANES: tuple[CiLane, ...] = (
    CiLane(
        lane_id="ci_work_shaping_contract",
        job_id="ci-work-shaping",
        commands=(
            "cargo run -q -p shardloom-cli -- ci-work-shaping-plan",
            "python scripts/ci_execution_plan.py plan",
        ),
        artifact_refs=(
            "target/ci-work-shaping-plan.json",
            "target/ci-work-shaping-changed-files.txt",
            "ci-work-shaping-evidence",
            "target/ci-execution-plan.json",
        ),
        release_blocker_refs=(
            "metadata-first CI work shaping",
            "capillary changed-file selection",
            "pulseweave evidence fingerprint",
            "source-aware benchmark rerun recommendations",
        ),
        workflow_markers=(
            "fetch-depth: 0",
            "Collect changed files",
            "SHARDLOOM_CI_WORK_SHAPING_MODE=pull_request",
            "SHARDLOOM_CI_WORK_SHAPING_MODE=merge",
            "--changed-paths-file target/ci-work-shaping-changed-files.txt",
            "retention-days: 14",
            "selected_jobs:",
            "full_validation:",
            "git diff --no-renames --name-only",
        ),
    ),
    CiLane(
        lane_id="rust_baseline",
        job_id="rust-baseline",
        commands=(
            "cargo fmt --all -- --check",
            "cargo clippy --workspace --all-targets -- -D warnings",
            "cargo test --workspace --all-targets",
        ),
        artifact_refs=(
        ),
        release_blocker_refs=(
            "default Rust formatting, linting, and tests",
        ),
        workflow_markers=(
        ),
    ),
    CiLane(
        lane_id="rust_feature_matrix",
        job_id="rust-feature-matrix",
        commands=(
            "cargo check --workspace",
            "cargo check --workspace --all-features",
            "cargo check --workspace --no-default-features",
            "cargo check -p shardloom-vortex --features upstream-vortex",
            "cargo check -p shardloom-vortex --features vortex-file-io",
            "cargo check -p shardloom-vortex --features vortex-local-primitives",
            "cargo check -p shardloom-vortex --features vortex-encoded-read-spike",
            "cargo check -p shardloom-vortex --features release-user-surfaces",
            "cargo test --locked -p shardloom-vortex --lib --features release-user-surfaces,output-window-observation output_window_observation::tests",
            "cargo test --locked -p shardloom-vortex --lib --features release-user-surfaces,output-window-observation credit_window::tests",
            "cargo test -p shardloom-exec --lib --features spill-payload-fs spill_payload::tests -- --test-threads=1",
            "cargo test -p shardloom-vortex --lib --features shardloom-exec/spill-payload-fs bounded_execution::tests -- --test-threads=1",
            "cargo test -p shardloom-cli --test required_execution_resources --features shardloom-exec/spill-payload-fs -- --test-threads=1",
            "cargo test -p shardloom-cli --all-targets --no-default-features --features vortex-write",
            "cargo clippy -p shardloom-cli -p shardloom-vortex --all-targets --features release-user-surfaces -- -D warnings",
            "cargo test -p shardloom-contract-tests --test conda_packaging_recipes",
        ),
        artifact_refs=(
        ),
        release_blocker_refs=(
            "workspace feature/build matrix",
        ),
        workflow_markers=(
            "native_vortex_observation",
            "fail-fast: false",
        ),
    ),
    CiLane(
        lane_id="native_optimized_build",
        job_id="native-test-build",
        commands=(
            "python scripts/ci_build_artifact.py build --product native-tests --directory \"$RUNNER_TEMP/ci-native-tests\"",
            "python scripts/ci_native_tests.py prepare --bundle \"$RUNNER_TEMP/ci-native-tests\"",
        ),
        artifact_refs=(
            "native-test-executable",
            "${{ runner.temp }}/ci-native-tests",
        ),
        release_blocker_refs=(
            "checked optimized native build and complete inventory",
        ),
        workflow_markers=(
            "ubuntu-24.04",
            "if-no-files-found: error",
        ),
    ),
    CiLane(
        lane_id="native_test_partitions",
        job_id="native-test-partitions",
        commands=(
            "python scripts/ci_native_tests.py run --bundle target/ci-native-tests --partition ${{ matrix.partition }} --output target/ci-native-results/${{ matrix.partition }}",
        ),
        artifact_refs=(
            "native-test-executable",
            "native-test-partition-${{ matrix.partition }}",
            "target/ci-native-results/${{ matrix.partition }}",
        ),
        release_blocker_refs=(
            "duration-balanced complete native execution",
        ),
        workflow_markers=(
            "partition: [0, 1, 2, 3]",
            "fail-fast: false",
            "native-test-build",
        ),
    ),
    CiLane(
        lane_id="patched_provider_tests",
        job_id="patched-provider-tests",
        commands=(
            "python scripts/ci_provider_tests.py --group ${{ matrix.group }} --output target/provider-evidence/${{ matrix.group }}",
        ),
        artifact_refs=(
            "provider-evidence-${{ matrix.group }}",
            "target/provider-evidence/${{ matrix.group }}",
        ),
        release_blocker_refs=(
            "all eight patched-provider test and lint configurations",
        ),
        workflow_markers=(
            "group: [arrays, io, buffer]",
            "CARGO_TARGET_DIR: ${{ github.workspace }}/target/providers",
            ".github/provider-locks/*.lock",
        ),
    ),
    CiLane(
        lane_id="native_test_coverage",
        job_id="native-tests",
        commands=(
            "python scripts/ci_native_tests.py aggregate --bundle target/ci-native-tests --output target/ci-native-results",
        ),
        artifact_refs=(
            "native-test-executable",
            "native-test-partition-*",
            "native-test-coverage",
            "target/ci-native-results/coverage.json",
        ),
        release_blocker_refs=(
            "native test ownership and failure propagation",
        ),
        workflow_markers=(
            "Rust feature matrix (native_vortex_tests)",
            "always()",
            "native-test-build",
            "native-test-partitions",
            "patched-provider-tests",
            "rust-feature-matrix",
        ),
    ),
    CiLane(
        lane_id="native_cli_debug",
        job_id="native-cli-build-debug",
        commands=(
            "cargo test --locked -p shardloom-cli --lib --bin shardloom --test vortex_prepare --test public_memory_inputs --test public_workflow_route --test resident_worker --features release-user-surfaces",
            "python scripts/ci_build_artifact.py build --product cli-debug --directory \"$RUNNER_TEMP/ci-cli-debug\"",
        ),
        artifact_refs=(
            "native-cli-debug-linux",
            "${{ runner.temp }}/ci-cli-debug",
        ),
        release_blocker_refs=(
            "ordinary debug native public-surface coverage",
        ),
        workflow_markers=(
            "Rust feature matrix (native_public_surface_tests)",
            "ubuntu-24.04",
        ),
    ),
    CiLane(
        lane_id="native_cli_release",
        job_id="native-cli-build-release",
        commands=(
            "python scripts/ci_build_artifact.py build --product cli-release --directory \"$RUNNER_TEMP/ci-cli-release\"",
        ),
        artifact_refs=(
            "native-cli-release-linux",
            "${{ runner.temp }}/ci-cli-release",
        ),
        release_blocker_refs=(
            "exact release CLI artifact",
        ),
        workflow_markers=(
            "ubuntu-24.04",
        ),
    ),
    CiLane(
        lane_id="rust_msrv_validation",
        job_id="rust-msrv",
        commands=(
            "cargo check --workspace --no-default-features",
            "cargo check --workspace --all-targets --features release-user-surfaces",
            "python scripts/write_release_compatibility_lane_report.py",
            "--lane \"$SHARDLOOM_RUST_MSRV_LANE\"",
            "--rust-toolchain \"$SHARDLOOM_RUST_MSRV_TOOLCHAIN\"",
        ),
        artifact_refs=(
            "target/release-compatibility/rust_msrv_*.json",
            "release-compatibility-rust-msrv",
        ),
        release_blocker_refs=(
            "Rust MSRV derived from root Cargo.toml validation",
        ),
        workflow_markers=(
            "python scripts/write_ci_version_env.py --github-env \"$GITHUB_ENV\"",
            "rustup toolchain install \"$SHARDLOOM_RUST_MSRV_TOOLCHAIN\"",
            "rustup default \"$SHARDLOOM_RUST_MSRV_TOOLCHAIN\"",
            "key: rust-msrv-${{ hashFiles('Cargo.toml') }}",
            "SHARDLOOM_RUST_MSRV_LANE",
            "retention-days: 14",
        ),
    ),
    CiLane(
        lane_id="python_test_shards",
        job_id="python-test-shards",
        commands=(
            "python scripts/run_python_test_shard.py --shard ${{ matrix.shard }}",
        ),
        artifact_refs=(
            "target/python-test-shards/${{ matrix.shard }}.json",
            "python-test-shard-${{ matrix.shard }}",
        ),
        release_blocker_refs=(
            "Python test shards",
        ),
        workflow_markers=(
            "core",
            "front_door_benchmark_publication",
            "release_scripts",
            "fail-fast: false",
        ),
    ),
    CiLane(
        lane_id="python_tests",
        job_id="python-tests",
        commands=(
            "python -m compileall -q python/src python/tests scripts examples benchmarks/traditional_analytics",
            "python scripts/merge_python_test_shard_evidence.py",
        ),
        artifact_refs=(
            "python-test-shard-*",
            "target/python-test-evidence.json",
            "python-test-evidence",
        ),
        release_blocker_refs=(
            "Python tests",
            "Python compile check",
        ),
        workflow_markers=(
            "needs:",
            "python-test-shards",
            "if: always()",
            "actions/download-artifact@v8",
            "merge-multiple: true",
        ),
    ),
    CiLane(
        lane_id="python_compatibility_matrix",
        job_id="python-compatibility-matrix",
        commands=(
            "python -m compileall -q python/src scripts examples benchmarks/traditional_analytics",
            "python -m build python",
            "cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces",
            "./target/release/shardloom.exe --version",
            "cargo test --release -p shardloom-cli --test non_unix_resource_admission --features release-user-surfaces",
            "python scripts/write_release_compatibility_lane_report.py --lane ${{ matrix.lane }} --surface python --python-version ${{ matrix.python-version }} --os-name ${{ matrix.os }}",
        ),
        artifact_refs=(
            "target/release-compatibility/${{ matrix.lane }}.json",
            "release-compatibility-${{ matrix.lane }}",
        ),
        release_blocker_refs=(
            "Python 3.10 through 3.13 compatibility",
            "OS matrix",
            "Windows bundled native CLI compilation",
            "Windows native resource-admission refusal",
        ),
        workflow_markers=(
            "fail-fast: false",
            "python-version: \"3.10\"",
            "python-version: \"3.11\"",
            "python-version: \"3.12\"",
            "python-version: \"3.13\"",
            "ubuntu-latest",
            "macos-latest",
            "windows-latest",
            "if: ${{ matrix.os == 'windows-latest' }}",
            "key: windows-release-user-surfaces",
            "retention-days: 14",
        ),
    ),
    CiLane(
        lane_id="native_package_build_linux",
        job_id="native-package-build-linux",
        commands=(
            "python -m pip install --require-hashes -r .github/requirements/ci-native-python-build.txt",
            "python scripts/ci_native_package.py build --output \"$RUNNER_TEMP/native-package\" --cli-bundle target/ci-cli-release",
        ),
        artifact_refs=(
            "native-package-linux",
            "${{ runner.temp }}/native-package/manifest.json",
            "${{ runner.temp }}/native-package/wheel-build/report.json",
            "${{ runner.temp }}/native-package/wheel-build/dist/*.whl",
        ),
        release_blocker_refs=(
            "platform native package and distinct observation product",
        ),
        workflow_markers=(
            "ubuntu-24.04",
            "python-version: \"3.10\"",
            "if-no-files-found: error",
            "native-cli-build-release",
            "native-cli-release-linux",
        ),
    ),
    CiLane(
        lane_id="native_package_build_macos",
        job_id="native-package-build-macos",
        commands=(
            "python -m pip install --require-hashes -r .github/requirements/ci-native-python-build.txt",
            "python scripts/ci_native_package.py build --output \"$RUNNER_TEMP/native-package\"",
        ),
        artifact_refs=(
            "native-package-macos",
            "${{ runner.temp }}/native-package/manifest.json",
            "${{ runner.temp }}/native-package/wheel-build/report.json",
            "${{ runner.temp }}/native-package/wheel-build/dist/*.whl",
        ),
        release_blocker_refs=(
            "platform native package and distinct observation product",
        ),
        workflow_markers=(
            "macos-latest",
            "python-version: \"3.13\"",
            "if-no-files-found: error",
        ),
    ),
    CiLane(
        lane_id="native_package_proof_linux",
        job_id="python-native-wheel-linux",
        commands=(
            "python -m pip install --require-hashes -r .github/requirements/ci-native-python-build.txt",
            "python -m pip download --require-hashes --only-binary=:all: --no-deps -r .github/requirements/ci-native-python.txt --dest \"$RUNNER_TEMP/native-wheelhouse\"",
            "python -B -m unittest discover -s scripts -p 'test_*native_python*.py' -v",
            "python -B -m unittest discover -s scripts -p test_python_license_bundle.py -v",
            "python scripts/ci_native_package.py test --package \"$RUNNER_TEMP/native-package\" --output \"$RUNNER_TEMP/native-proof\" --wheelhouse \"$RUNNER_TEMP/native-wheelhouse\"",
        ),
        artifact_refs=(
            "native-package-linux",
            "${{ runner.temp }}/native-proof/ci-package-proof.json",
            "${{ runner.temp }}/native-proof/proof/report.json",
            "${{ runner.temp }}/native-proof/proof/integration.json",
            "${{ runner.temp }}/native-proof/output-window-tests.json",
            "native-python-wheel-${{ matrix.lane }}",
        ),
        release_blocker_refs=(
            "native Python wheel integrity and isolated installation",
        ),
        workflow_markers=(
            "ubuntu-24.04",
            "native-package-build-linux",
            "if-no-files-found: error",
            "python-version: \"3.10\"",
            "python-version: \"3.13\"",
        ),
    ),
    CiLane(
        lane_id="native_package_proof_macos",
        job_id="python-native-wheel-macos",
        commands=(
            "python -m pip install --require-hashes -r .github/requirements/ci-native-python-build.txt",
            "python -m pip download --require-hashes --only-binary=:all: --no-deps -r .github/requirements/ci-native-python.txt --dest \"$RUNNER_TEMP/native-wheelhouse\"",
            "python -B -m unittest discover -s scripts -p 'test_*native_python*.py' -v",
            "python -B -m unittest discover -s scripts -p test_python_license_bundle.py -v",
            "python scripts/ci_native_package.py test --package \"$RUNNER_TEMP/native-package\" --output \"$RUNNER_TEMP/native-proof\" --wheelhouse \"$RUNNER_TEMP/native-wheelhouse\"",
        ),
        artifact_refs=(
            "native-package-macos",
            "${{ runner.temp }}/native-proof/ci-package-proof.json",
            "${{ runner.temp }}/native-proof/proof/report.json",
            "${{ runner.temp }}/native-proof/proof/integration.json",
            "${{ runner.temp }}/native-proof/output-window-tests.json",
            "native-python-wheel-py313_macos",
        ),
        release_blocker_refs=(
            "native Python wheel integrity and isolated installation",
        ),
        workflow_markers=(
            "macos-latest",
            "native-package-build-macos",
            "if-no-files-found: error",
            "python-version: \"3.13\"",
        ),
    ),
    CiLane(
        lane_id="native_python_wheel",
        job_id="python-native-wheel",
        commands=(
            "python scripts/ci_native_package.py aggregate --reports target/native-package-results --output target/ci-native-package-coverage.json",
        ),
        artifact_refs=(
            "native-python-wheel-py*",
            "native-python-wheel-coverage",
            "target/ci-native-package-coverage.json",
        ),
        release_blocker_refs=(
            "all platform proofs and identical Linux wheel acceptance",
        ),
        workflow_markers=(
            "always()",
            "python-native-wheel-linux",
            "python-native-wheel-macos",
        ),
    ),
    CiLane(
        lane_id="python_package_smoke",
        job_id="python-package",
        commands=(
            "python -m build python",
            "python scripts/release_dry_run_proof.py --rows 8 --iterations 1 --skip-clean-conda",
        ),
        artifact_refs=(
            "target/release-dry-run-proof/transcript.json",
            "target/debug/shardloom",
            "python/dist/*.whl",
            "python/dist/*.tar.gz",
            "release-local-smoke-evidence",
        ),
        release_blocker_refs=(
            "package/install smoke",
            "local provenance dry run",
        ),
        workflow_markers=(
            "Python and package smoke",
        ),
    ),
    CiLane(
        lane_id="dependency_security",
        job_id="dependency-security",
        commands=(
            "python scripts/check_dependency_audit.py --release-gate --json-output target/dependency-audit-report.json",
            "python scripts/check_security_posture.py",
            "python scripts/release_provenance_dry_run.py",
            "python scripts/check_release_security_gate.py",
        ),
        artifact_refs=(
            "target/dependency-audit-report.json",
            "target/security-posture-report.json",
            "target/release-provenance-dry-run",
            "target/release-security-gate-report.json",
        ),
        release_blocker_refs=(
            "dependency/license audit",
            "security posture",
            "release security gate",
        ),
        workflow_markers=(
        ),
    ),
    CiLane(
        lane_id="release_runtime_core_evidence",
        job_id="release-runtime-core",
        commands=(
            "python scripts/check_golden_workflows.py",
            "python scripts/check_admitted_semantics_matrix.py",
            "python scripts/check_release_architecture_tracker.py --allow-blocked",
        ),
        artifact_refs=(
            "target/golden-workflow-report.json",
            "target/golden-workflows",
            "target/admitted-semantics-matrix-report.json",
            "target/admitted-semantics-matrix",
            "target/release-architecture-tracker-report.json",
        ),
        release_blocker_refs=(
            "golden workflow validator",
            "admitted semantics matrix",
            "release architecture tracker",
        ),
        workflow_markers=(
        ),
    ),
    CiLane(
        lane_id="release_package_governance_evidence",
        job_id="release-package-governance",
        commands=(
            "python scripts/merge_release_evidence_artifacts.py",
            "python scripts/check_contribution_governance.py",
            "python scripts/check_workspace_version_sources.py",
            "python scripts/check_package_channel_readiness.py --require-local-evidence",
        ),
        artifact_refs=(
            "target/contribution-governance-report.json",
            "target/workspace-version-source-report.json",
            "target/package-channel-readiness-report.json",
        ),
        release_blocker_refs=(
            "contribution governance",
            "workspace Rust/Vortex version source contract",
            "package channel matrix",
        ),
        workflow_markers=(
            "needs:",
            "dependency-security",
            "python-package",
            "actions/download-artifact@v8",
            "dependency-security-evidence",
            "release-local-smoke-evidence",
            "target/downloads",
            "Merge package/governance input evidence",
            "merge_release_evidence_artifacts.py",
            "Workspace version source contract",
        ),
    ),
    CiLane(
        lane_id="release_user_surface_evidence",
        job_id="release-user-surface",
        commands=(
            "python scripts/ci_build_artifact.py verify --product cli-debug --directory target/ci-cli-debug --restore target/debug/shardloom --replace-existing",
            "python scripts/ci_build_artifact.py verify --product cli-release --directory target/ci-cli-release --restore target/release/shardloom --replace-existing",
            "python scripts/check_python_user_surface_completion.py",
            "python scripts/check_sql_python_dataframe_parity.py",
            "python scripts/check_v1_front_door_runtime_scope.py",
            "python scripts/check_v1_vortex_runtime_scope.py",
            "python scripts/check_v1_source_prepared_state_scope.py",
            "python scripts/check_v1_local_output_sink_scope.py",
            "python scripts/check_v1_local_resource_safety.py --skip-build",
            "python scripts/check_v1_observability_support.py --skip-build",
            "python scripts/check_v1_example_replay.py --binary target/release/shardloom --profile-order release,debug --skip-build",
            "python scripts/check_user_surface_runtime_gap_inventory.py",
            "python scripts/check_user_surface_graduation_matrix.py",
            "python scripts/check_runtime_gap_family_burn_down.py",
            "python scripts/check_user_route_capability_report.py",
        ),
        artifact_refs=(
            "target/python-user-surface-completion-gate.json",
            "target/sql-python-dataframe-parity-gate.json",
            "target/v1-front-door-runtime-scope-report.json",
            "target/v1-vortex-runtime-scope-report.json",
            "target/v1-source-prepared-state-scope-report.json",
            "target/v1-local-output-sink-scope-report.json",
            "target/v1-local-resource-safety-report.json",
            "target/v1-observability-support-report.json",
            "target/v1-example-replay-report.json",
            "target/v1-example-replay",
            "target/user-surface-runtime-gap-inventory.json",
            "target/user-surface-graduation-matrix.json",
            "target/runtime-gap-family-burn-down.json",
            "target/user-route-capability-report.json",
            "native-cli-debug-linux",
            "native-cli-release-linux",
        ),
        release_blocker_refs=(
            "Python user-surface completion gate",
            "SQL/Python/DataFrame parity gate",
            "v1 front-door runtime scope gate",
            "v1 Vortex runtime scope gate",
            "v1 SourceState/prepared-state scope gate",
            "v1 local resource-safety gate",
            "v1 observability/supportability gate",
            "user-surface runtime gap inventory",
            "v1 example replay gate",
            "user-surface graduation matrix",
            "runtime gap family burn-down",
            "user route capability report",
        ),
        workflow_markers=(
            "needs:",
            "python-package",
            "release-runtime-core",
            "actions/download-artifact@v8",
            "release-local-smoke-evidence",
            "release-runtime-core-evidence",
            "target/downloads",
            "Merge local package smoke evidence",
            "merge_release_evidence_artifacts.py",
            "native-cli-build-debug",
            "native-cli-build-release",
        ),
    ),
    CiLane(
        lane_id="release_readiness_reports",
        job_id="release-readiness",
        commands=(
            "python scripts/ci_execution_plan.py summary --plan target/downloads/ci-execution-plan/ci-execution-plan.json --output target/ci-execution-summary.json",
            "python scripts/merge_release_evidence_artifacts.py",
            "python scripts/final_release_rehearsal.py --allow-blocked",
            "python scripts/check_production_usability_gate.py",
            "python scripts/check_v1_api_schema_stability.py",
            "python scripts/check_v1_correctness_conformance.py",
            "python scripts/check_v1_security_ci_hardening.py",
            "python scripts/check_v1_release_boundary.py",
            "python scripts/check_production_certification_gate.py",
            "python scripts/check_release_readiness.py",
            "python scripts/check_finished_product_readiness.py",
        ),
        artifact_refs=(
            "target/dependency-audit-report.json",
            "target/security-posture-report.json",
            "target/release-dry-run-proof",
            "target/release-provenance-dry-run",
            "target/debug/shardloom",
            "python/dist",
            "target/python-test-evidence.json",
            "target/release-security-gate-report.json",
            "target/contribution-governance-report.json",
            "target/package-channel-readiness-report.json",
            "target/golden-workflow-report.json",
            "target/golden-workflows",
            "target/admitted-semantics-matrix-report.json",
            "target/admitted-semantics-matrix",
            "target/release-architecture-tracker-report.json",
            "target/final-release-rehearsal",
            "target/public-status-docs-report.json",
            "target/website-readiness-report.json",
            "target/workspace-version-source-report.json",
            "target/production-usability-gate.json",
            "target/v1-api-schema-stability-report.json",
            "target/v1-example-replay-report.json",
            "target/v1-example-replay",
            "target/v1-correctness-conformance-report.json",
            "target/v1-security-ci-hardening-report.json",
            "target/v1-release-boundary-report.json",
            "target/production-certification-gate.json",
            "target/python-user-surface-completion-gate.json",
            "target/sql-python-dataframe-parity-gate.json",
            "target/v1-front-door-runtime-scope-report.json",
            "target/v1-vortex-runtime-scope-report.json",
            "target/v1-source-prepared-state-scope-report.json",
            "target/v1-local-output-sink-scope-report.json",
            "target/v1-local-resource-safety-report.json",
            "target/v1-observability-support-report.json",
            "target/user-surface-runtime-gap-inventory.json",
            "target/user-surface-graduation-matrix.json",
            "target/runtime-gap-family-burn-down.json",
            "target/user-route-capability-report.json",
            "target/ci-gate-matrix-report.json",
            "target/ci-work-shaping-plan.json",
            "target/ci-work-shaping-changed-files.txt",
            "target/hard-release-readiness-gate.json",
            "target/finished-product-readiness-report.json",
            "target/ci-execution-summary.json",
            "ci-execution-summary",
        ),
        release_blocker_refs=(
            "final rehearsal",
            "production usability gate",
            "v1 API/schema stability gate",
            "v1 correctness/conformance gate",
            "v1 security/CI hardening gate",
            "v1 release boundary firewall",
            "production certification gate",
            "hard release readiness gate",
            "finished product readiness gate",
            "release readiness artifact aggregation",
        ),
        workflow_markers=(
            "needs:",
            "ci-work-shaping",
            "ci-gate-matrix",
            "dependency-security",
            "python-tests",
            "python-package",
            "release-runtime-core",
            "release-package-governance",
            "release-user-surface",
            "website-docs",
            "actions/download-artifact@v8",
            "ci-work-shaping-evidence",
            "dependency-security-evidence",
            "release-local-smoke-evidence",
            "python-test-evidence",
            "target/downloads",
            "target/downloads/ci-work-shaping-evidence",
            "ci-gate-matrix-report",
            "release-runtime-core-evidence",
            "release-package-governance-evidence",
            "release-user-surface-evidence",
            "website-docs-evidence",
            "Merge downloaded release evidence",
            "Verify downloaded release evidence",
            "continue-on-error: true",
            "Finished product readiness gate",
            "always()",
            "needs.ci-work-shaping.outputs.full_validation == 'true'",
            "native-tests",
            "python-native-wheel",
            "native-cli-build-debug",
            "native-cli-build-release",
        ),
    ),
    CiLane(
        lane_id="website_docs_validation",
        job_id="website-docs",
        commands=(
            "npm audit --audit-level=low",
            "npm run build",
            "npm run check",
            "python scripts/check_public_status_docs.py",
            "python -m unittest discover -s scripts -p test_website_links.py",
            "python scripts/check_website_readiness.py",
            "node scripts/validate_static_assets.cjs",
        ),
        artifact_refs=(
            "target/public-status-docs-report.json",
            "target/website-readiness-report.json",
        ),
        release_blocker_refs=(
            "website build",
            "public status docs",
            "docs/status generated assets",
        ),
        workflow_markers=(
        ),
        no_fallback_required=False,
    ),
    CiLane(
        lane_id="ci_gate_matrix_contract",
        job_id="ci-gate-matrix",
        commands=(
            "python scripts/check_ci_gate_matrix.py",
            "python -B -m unittest discover -s scripts -p 'test_ci_*.py' -v",
        ),
        artifact_refs=(
            "target/ci-gate-matrix-report.json",
        ),
        release_blocker_refs=(
            "CI matrix drift contract",
        ),
        workflow_markers=(
        ),
    ),
)

WORKFLOW_POLICY_MARKERS = (
    "concurrency:",
    "cancel-in-progress: true",
    "actions: read",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--workflow",
        type=Path,
        default=Path(".github/workflows/ci.yml"),
    )
    parser.add_argument(
        "--matrix-doc",
        type=Path,
        default=Path("docs/release/ci-gate-matrix.md"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/ci-gate-matrix-report.json"),
    )
    return parser.parse_args()


def workflow_job_section(workflow: str, job_id: str) -> str:
    pattern = re.compile(
        rf"(?ms)^  {re.escape(job_id)}:\n(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)"
    )
    match = pattern.search(workflow)
    return match.group(0) if match else ""


def lane_status(lane: CiLane, workflow: str, doc: str) -> dict[str, Any]:
    blockers: list[str] = []
    job_section = workflow_job_section(workflow, lane.job_id)
    if not job_section:
        blockers.append(f"missing workflow job {lane.job_id}")
    for command in lane.commands:
        if command not in job_section:
            blockers.append(f"workflow job {lane.job_id} missing command: {command}")
        if command not in doc:
            blockers.append(f"doc missing command: {command}")
    for artifact in lane.artifact_refs:
        if artifact not in job_section:
            blockers.append(
                f"workflow job {lane.job_id} missing artifact ref: {artifact}"
            )
        if artifact not in doc:
            blockers.append(f"doc missing artifact ref: {artifact}")
    for blocker_ref in lane.release_blocker_refs:
        if blocker_ref not in doc:
            blockers.append(f"doc missing release blocker ref: {blocker_ref}")
    for marker in lane.workflow_markers:
        if marker not in job_section:
            blockers.append(f"workflow job {lane.job_id} missing marker: {marker}")
        if marker not in doc:
            blockers.append(f"doc missing marker: {marker}")
    if lane.lane_id not in doc:
        blockers.append(f"doc missing lane id {lane.lane_id}")
    return {
        "lane_id": lane.lane_id,
        "job_id": lane.job_id,
        "commands": list(lane.commands),
        "artifact_refs": list(lane.artifact_refs),
        "release_blocker_refs": list(lane.release_blocker_refs),
        "workflow_markers": list(lane.workflow_markers),
        "status": "passed" if not blockers else "failed",
        "blockers": blockers,
        "release_blocking": bool(blockers),
        "no_fallback_required": lane.no_fallback_required,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    workflow_path = resolve_path(repo_root, args.workflow)
    doc_path = resolve_path(repo_root, args.matrix_doc)
    output = resolve_path(repo_root, args.output)

    workflow = read_text(workflow_path)
    doc = read_text(doc_path)
    blockers: list[str] = []
    if not workflow:
        blockers.append("missing CI workflow")
    if not doc:
        blockers.append("missing CI gate matrix doc")

    for required in [
        SCHEMA_VERSION,
        "public_release_claim_allowed=false",
        "public_package_claim_allowed=false",
        "publication_attempted=false",
        "tag_created=false",
        "secrets_required=false",
        "package_upload_attempted=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "skipped_gate=clean_conda_release_environment",
        "skipped_gate=real_publication",
    ]:
        if required not in doc:
            blockers.append(f"doc missing marker: {required}")
    for marker in WORKFLOW_POLICY_MARKERS:
        if marker not in workflow:
            blockers.append(f"workflow missing policy marker: {marker}")
        if marker not in doc:
            blockers.append(f"doc missing policy marker: {marker}")

    lane_rows = [lane_status(lane, workflow, doc) for lane in REQUIRED_LANES]
    blockers.extend(
        f"{row['lane_id']}: {blocker}"
        for row in lane_rows
        for blocker in row["blockers"]
    )
    passed = not blockers
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "lane_count": len(REQUIRED_LANES),
        "workflow_ref": str(args.workflow).replace("\\", "/"),
        "matrix_doc_ref": str(args.matrix_doc).replace("\\", "/"),
        "lanes": lane_rows,
        "blockers": blockers,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "package_upload_attempted": False,
        "remaining_skipped_gates": [
            "clean_conda_release_environment",
            "real_publication",
            "release_tag_creation",
            "signing_key_use",
            "package_upload",
        ],
        **{
            key: value
            for key, value in fail_closed_fields().items()
            if key not in {"production_claim_allowed", "spark_replacement_claim_allowed"}
        },
    }
    write_json(output, report)
    print(output)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
