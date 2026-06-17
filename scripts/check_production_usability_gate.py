#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate local production-usability evidence without authorizing production claims.

This gate is intentionally narrower than the hard public-release gate. It proves that a local
local/source/package rehearsal gives a user a clean install path, admitted smokes,
docs/status/website learning path, benchmark evidence attachment, and explicit
unsupported production blockers. Selected GitHub/TestPyPI/PyPI/Homebrew
install access is allowed only as package access; it must not become a
production, performance, or Spark-replacement claim.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from check_benchmark_artifact_completeness import (
    REPORT_SCHEMA_VERSION as BENCHMARK_COMPLETENESS_REPORT_SCHEMA_VERSION,
)
from check_benchmark_artifact_completeness import validate_manifest as validate_benchmark_manifest
from release_channel_contract import (
    SELECTED_V0_1_0_INSTALL_ACCESS_BOUNDARY,
    SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.production_usability_gate.v1"

FALSE_SAFETY_FIELDS = [
    "publication_attempted",
    "tag_created",
    "secrets_required",
    "fallback_attempted",
    "external_engine_invoked",
]

DRY_RUN_REQUIRED_TRUE_FIELDS = [
    "wheel_import_and_client_smoke_performed",
    "cli_status_smoke_performed",
    "cli_capabilities_smoke_performed",
    "local_python_example_smoke_performed",
    "local_python_user_surface_quickstart_performed",
    "local_python_result_and_evidence_printed",
    "local_python_unsupported_path_evidence_printed",
    "generated_output_proof_distinct_from_no_dataset_smoke",
    "generated_source_user_rows_smoke_performed",
    "generated_source_range_smoke_performed",
    "provenance_dry_run_performed",
    "sbom_checksum_manifest_generated",
]

DRY_RUN_REQUIRED_STEPS = [
    "build_cli_binary",
    "build_python_artifacts",
    "create_clean_venv",
    "install_local_wheel_clean_venv",
    "wheel_import_and_client_smoke",
    "cli_status_json",
    "cli_capabilities_json",
    "example_local_python_smoke",
    "generated_source_user_rows_local_output_smoke",
    "generated_source_range_local_output_smoke",
    "release_provenance_dry_run",
]

REQUIRED_DOC_MARKERS = {
    "README.md": [
        "docs/getting-started/install.md",
        "docs/getting-started/first-10-minutes.md",
        "scripts\\release_dry_run_proof.py",
        "selected local/source/package v1 release track",
    ],
    "docs/getting-started/install.md": [
        "python scripts\\release_dry_run_proof.py --rows 64 --iterations 1",
        "pip --no-index",
        "SHARDLOOM_BIN",
    ],
    "docs/getting-started/first-10-minutes.md": [
        "python scripts\\release_dry_run_proof.py --rows 64 --iterations 1",
        "ctx.from_rows",
        "ctx.read",
        "quickstart_result_row_id",
        "ctx.range",
        "public package release",
    ],
    "docs/release/release-dry-run-proof.md": [
        "clean virtual environment",
        "local_python_user_surface_quickstart_performed=true",
        "generated_source_user_rows_smoke_performed=true",
        "benchmark_smoke_required_for_package_release=false",
    ],
    "docs/release/production-usability-gate.md": [
        SCHEMA_VERSION,
        "python scripts\\check_production_usability_gate.py",
        "public_release_claim_allowed=false",
    ],
    "docs/release/package-channel-readiness-matrix.md": [
        "Package Channel Readiness Matrix",
        "scripts/release_dry_run_proof.py",
    ],
    "docs/release/hard-release-readiness-gate.md": [
        "public_release_claim_allowed=false",
        "clean_conda_env_install_status=passed",
    ],
    "docs/release/known-unsupported-paths.md": [
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ],
    "website-src/src/pages/start.astro": [
        "release_dry_run_proof.py",
        "check_production_usability_gate.py",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--release-dry-run-transcript",
        type=Path,
        default=Path("target/release-dry-run-proof/transcript.json"),
    )
    parser.add_argument(
        "--package-channel-report",
        type=Path,
        default=Path("target/package-channel-readiness-report.json"),
    )
    parser.add_argument(
        "--release-security-report",
        type=Path,
        default=Path("target/release-security-gate-report.json"),
    )
    parser.add_argument(
        "--contribution-governance-report",
        type=Path,
        default=Path("target/contribution-governance-report.json"),
    )
    parser.add_argument(
        "--final-release-rehearsal-report",
        type=Path,
        default=Path("target/final-release-rehearsal/final-release-rehearsal-report.json"),
    )
    parser.add_argument(
        "--website-readiness-report",
        type=Path,
        default=Path("target/website-readiness-report.json"),
    )
    parser.add_argument(
        "--benchmark-manifest",
        type=Path,
        default=Path("website/assets/benchmarks/latest/manifest.json"),
    )
    parser.add_argument(
        "--benchmark-completeness-report",
        type=Path,
        default=Path("target/benchmark-artifact-completeness-report.json"),
        help=(
            "Optional precomputed benchmark completeness report. If the path exists, "
            "the gate consumes it instead of rescanning the published benchmark bundle."
        ),
    )
    parser.add_argument(
        "--runs-today-matrix",
        type=Path,
        default=Path("docs/status/runs-today-support-matrix.json"),
    )
    parser.add_argument("--output", type=Path, default=Path("target/production-usability-gate.json"))
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def rel(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def path_exists(repo_root: Path, path_text: str | None) -> bool:
    if not path_text:
        return False
    path = Path(path_text)
    return (path if path.is_absolute() else repo_root / path).exists()


def file_sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def benchmark_artifact_json_path(manifest: dict[str, Any] | None, repo_root: Path) -> Path | None:
    if not isinstance(manifest, dict):
        return None
    artifact_paths = manifest.get("artifact_paths")
    if not isinstance(artifact_paths, dict):
        return None
    path_text = artifact_paths.get("json")
    if not path_text:
        return None
    path = Path(str(path_text))
    return path if path.is_absolute() else repo_root / path


def false_field_blockers(payload: dict[str, Any] | None, label: str) -> list[str]:
    if payload is None:
        return []
    return [
        f"{label} {field} must be false"
        for field in FALSE_SAFETY_FIELDS
        if payload.get(field) is not False
    ]


def step_map(dry_run: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if dry_run is None:
        return {}
    rows = dry_run.get("steps")
    if not isinstance(rows, list):
        return {}
    return {
        str(row.get("name")): row
        for row in rows
        if isinstance(row, dict) and isinstance(row.get("name"), str)
    }


def step_passed(steps: dict[str, dict[str, Any]], name: str) -> bool:
    return steps.get(name, {}).get("returncode") == 0


def validate_release_dry_run(
    repo_root: Path,
    dry_run: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    steps = step_map(dry_run)
    if dry_run is None:
        return {"status": "missing", "passed_step_count": 0}, ["missing release dry-run transcript"]
    if dry_run.get("schema_version") != "shardloom.release_dry_run_proof.v1":
        blockers.append("release dry-run schema_version mismatch")
    if dry_run.get("proof_status") != "passed":
        blockers.append(f"release dry-run proof_status={dry_run.get('proof_status')}")
    if dry_run.get("clean_venv_install_status") != "passed":
        blockers.append(
            "release dry-run clean_venv_install_status="
            + str(dry_run.get("clean_venv_install_status", "missing"))
        )
    clean_conda_status = dry_run.get("clean_conda_env_install_status")
    if dry_run.get("clean_conda_env_install_required") is True and clean_conda_status != "passed":
        blockers.append(f"release dry-run required clean Conda proof did not pass: {clean_conda_status}")
    if clean_conda_status not in {"passed", "skipped_tool_missing", "skipped_by_request"}:
        blockers.append(f"release dry-run clean_conda_env_install_status={clean_conda_status}")
    for field in DRY_RUN_REQUIRED_TRUE_FIELDS:
        if dry_run.get(field) is not True:
            blockers.append(f"release dry-run {field} must be true")
    if dry_run.get("benchmark_smoke_required_for_package_release") is not False:
        blockers.append(
            "release dry-run benchmark_smoke_required_for_package_release must be false"
        )
    for field in [
        "publication_attempted",
        "tag_created",
        "secrets_required",
        "external_runtime_dependencies_added",
        "fallback_engine_dependency_added",
        "fallback_attempted",
        "external_engine_invoked",
        "public_package_release_claim_allowed",
    ]:
        if dry_run.get(field) is not False:
            blockers.append(f"release dry-run {field} must be false")
    for step_name in DRY_RUN_REQUIRED_STEPS:
        if not step_passed(steps, step_name):
            blockers.append(f"release dry-run step did not pass: {step_name}")
    for label, field in [
        ("local wheel", "local_wheel"),
        ("local CLI binary", "local_cli_binary"),
    ]:
        if not path_exists(repo_root, dry_run.get(field)):
            blockers.append(f"release dry-run missing {label} artifact: {dry_run.get(field)}")
    return (
        {
            "status": "passed" if not blockers else "blocked",
            "clean_venv_install_status": dry_run.get("clean_venv_install_status"),
            "clean_conda_env_install_status": clean_conda_status,
            "benchmark_smoke_status": dry_run.get("benchmark_smoke_status"),
            "benchmark_smoke_required_for_package_release": dry_run.get(
                "benchmark_smoke_required_for_package_release"
            ),
            "passed_step_count": sum(1 for step in DRY_RUN_REQUIRED_STEPS if step_passed(steps, step)),
            "required_step_count": len(DRY_RUN_REQUIRED_STEPS),
            "local_wheel": dry_run.get("local_wheel"),
            "local_cli_binary": dry_run.get("local_cli_binary"),
        },
        blockers,
    )


def validate_package_channel_report(package_report: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if package_report is None:
        return {"status": "missing"}, ["missing package-channel readiness report"]
    if package_report.get("schema_version") != "shardloom.package_channel_readiness_report.v1":
        blockers.append("package-channel report schema_version mismatch")
    if package_report.get("status") != "passed":
        blockers.append(f"package-channel report status={package_report.get('status')}")
    if package_report.get("local_gate_evidence_required") is not True:
        blockers.append("package-channel report must be generated with --require-local-evidence")
    if package_report.get("local_gate_evidence_status") != "passed":
        blockers.append(
            "package-channel local_gate_evidence_status="
            + str(package_report.get("local_gate_evidence_status"))
        )
    if package_report.get("public_package_release_claim_allowed") is not True:
        blockers.append(
            "package-channel public_package_release_claim_allowed must be true for "
            + SELECTED_V0_1_0_INSTALL_ACCESS_BOUNDARY
        )
    blockers.extend(false_field_blockers(package_report, "package-channel report"))
    return (
        {
            "status": "passed" if not blockers else "blocked",
            "ready_channel_count": package_report.get("ready_channel_count"),
            "expected_channel_count": package_report.get("expected_channel_count"),
            "local_gate_evidence_status": package_report.get("local_gate_evidence_status"),
        },
        blockers,
    )


def validate_status_report(
    payload: dict[str, Any] | None,
    *,
    label: str,
    schema_version: str,
    status_fields: tuple[str, ...] = ("status",),
    expected_statuses: tuple[str, ...] = ("passed",),
    allow_blocked: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if payload is None:
        return {"status": "missing"}, [f"missing {label} report"]
    if payload.get("schema_version") != schema_version:
        blockers.append(f"{label} schema_version mismatch")
    statuses = {field: payload.get(field) for field in status_fields if payload.get(field) is not None}
    if not statuses:
        blockers.append(f"{label} missing status")
    elif not any(value in expected_statuses for value in statuses.values()):
        if not (allow_blocked and any(value == "blocked" for value in statuses.values())):
            rendered = ",".join(f"{field}={value}" for field, value in statuses.items())
            blockers.append(f"{label} status not accepted: {rendered}")
    if not allow_blocked:
        upstream_blockers = payload.get("blockers")
        if isinstance(upstream_blockers, list) and upstream_blockers:
            blockers.append(f"{label} blockers present")
    blockers.extend(false_field_blockers(payload, label))
    return {"status": "passed" if not blockers else "blocked", "observed_statuses": statuses}, blockers


def validate_website_report(website_report: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if website_report is None:
        return {"status": "missing"}, ["missing website readiness report"]
    if website_report.get("schema_version") != "shardloom.website_readiness.v3":
        blockers.append("website readiness schema_version mismatch")
    report_blockers = website_report.get("blockers")
    if not isinstance(report_blockers, list):
        blockers.append("website readiness blockers must be a list")
    elif report_blockers:
        blockers.append("website readiness blockers present: " + ",".join(map(str, report_blockers)))
    return (
        {
            "status": "passed" if not blockers else "blocked",
            "checked_page_count": len(website_report.get("checked_pages") or []),
            "checked_asset_count": len(website_report.get("checked_assets") or []),
        },
        blockers,
    )


def validate_benchmark_completeness_report(
    report: dict[str, Any] | None,
    *,
    manifest_ref: str,
    manifest_path: Path | None = None,
    repo_root: Path | None = None,
) -> tuple[dict[str, Any], list[str]] | None:
    if report is None:
        return None
    blockers: list[str] = []
    if report.get("schema_version") != BENCHMARK_COMPLETENESS_REPORT_SCHEMA_VERSION:
        blockers.append("benchmark completeness report schema mismatch")
    if str(report.get("manifest") or "").replace("\\", "/") != manifest_ref:
        blockers.append(
            "benchmark completeness report manifest mismatch: "
            + str(report.get("manifest", "missing"))
        )
    if manifest_path is not None and manifest_path.exists():
        expected_manifest_digest = file_sha256(manifest_path)
        if report.get("manifest_sha256") != expected_manifest_digest:
            blockers.append("benchmark completeness report manifest digest mismatch")
        manifest = load_json(manifest_path)
        artifact_path = benchmark_artifact_json_path(
            manifest,
            repo_root or manifest_path.parent,
        )
        if artifact_path is not None and artifact_path.exists():
            expected_artifact_digest = file_sha256(artifact_path)
            if report.get("artifact_json_sha256") != expected_artifact_digest:
                blockers.append("benchmark completeness report artifact digest mismatch")
    report_blockers = report.get("blockers")
    if not isinstance(report_blockers, list):
        blockers.append("benchmark completeness report blockers must be a list")
    elif report_blockers:
        blockers.extend(
            f"benchmark artifact completeness: {blocker}"
            for blocker in report_blockers
        )
    if report.get("status") != "passed":
        blockers.append(
            "benchmark completeness report status="
            + str(report.get("status", "missing"))
        )
    if report.get("performance_claim_allowed") is not False:
        blockers.append("benchmark completeness performance_claim_allowed must be false")
    for field in [
        "benchmark_run_performed",
        "fallback_attempted",
        "external_engine_invoked",
    ]:
        if report.get(field) is not False:
            blockers.append(f"benchmark completeness {field} must be false")
    return (
        {
            "status": "passed" if not blockers else "blocked",
            "benchmark_profile": report.get("benchmark_profile"),
            "artifact_status": report.get("artifact_status"),
            "available_lane_count": report.get("available_lane_count"),
            "missing_lane_count": report.get("missing_lane_count"),
            "performance_claim_allowed": report.get("performance_claim_allowed"),
            "source": "precomputed_report",
        },
        blockers,
    )


def validate_benchmark(
    manifest_path: Path,
    *,
    manifest_ref: str,
    repo_root: Path,
    completeness_report: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    report_result = validate_benchmark_completeness_report(
        completeness_report,
        manifest_ref=manifest_ref,
        manifest_path=manifest_path,
        repo_root=repo_root,
    )
    if report_result is not None:
        return report_result
    try:
        blockers, manifest = validate_benchmark_manifest(manifest_path, allow_incomplete=False)
    except (FileNotFoundError, json.JSONDecodeError) as error:
        return {"status": "missing_or_invalid"}, [f"benchmark manifest invalid: {error}"]
    return (
        {
            "status": "passed" if not blockers else "blocked",
            "benchmark_profile": manifest.get("benchmark_profile"),
            "artifact_status": manifest.get("artifact_status"),
            "available_lane_count": len(manifest.get("available_lanes") or []),
            "missing_lane_count": len(manifest.get("missing_lanes") or []),
            "performance_claim_allowed": manifest.get("performance_claim_allowed"),
            "source": "direct_manifest_scan",
        },
        [f"benchmark artifact completeness: {blocker}" for blocker in blockers],
    )


def validate_runs_today(runs_today: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if runs_today is None:
        return {"status": "missing"}, ["missing runs-today support matrix"]
    if runs_today.get("schema_version") != "shardloom.runs_today_support_matrix.v1":
        blockers.append("runs-today support matrix schema mismatch")
    if runs_today.get("all_rows_no_fallback_no_external_engine") is not True:
        blockers.append("runs-today must prove all rows have no fallback/no external engine")
    if runs_today.get("performance_claim_allowed") is not False:
        blockers.append("runs-today performance_claim_allowed must be false")
    rows = runs_today.get("rows")
    if not isinstance(rows, list) or len(rows) < 20:
        blockers.append("runs-today must expose at least 20 support rows")
        rows = []
    by_id = {row.get("id"): row for row in rows if isinstance(row, dict)}
    for row_id in [
        "cli_status_capability_reports",
        "python_status_capabilities",
        "python_generated_source_helpers",
        "cli_prepared_vortex_batch_benchmark",
    ]:
        if row_id not in by_id:
            blockers.append(f"runs-today missing supported learning row: {row_id}")
    for row_id in ["claim_production_readiness", "claim_future_package_channels"]:
        row = by_id.get(row_id)
        if not isinstance(row, dict):
            blockers.append(f"runs-today missing blocked claim row: {row_id}")
            continue
        if row.get("support_state") != "blocked":
            blockers.append(f"runs-today {row_id} support_state must be blocked")
        if row.get("claim_gate_status") != "not_claim_grade":
            blockers.append(f"runs-today {row_id} claim_gate_status must be not_claim_grade")
        if row.get("fallback_attempted") is not False or row.get("external_engine_invoked") is not False:
            blockers.append(f"runs-today {row_id} must keep fallback/external false")
    package_row = by_id.get("claim_package_publication")
    if not isinstance(package_row, dict):
        blockers.append("runs-today missing selected package access row: claim_package_publication")
    else:
        if package_row.get("support_state") != "executable":
            blockers.append("runs-today claim_package_publication support_state must be executable")
        if package_row.get("claim_gate_status") != "package_access_only":
            blockers.append(
                "runs-today claim_package_publication claim_gate_status must be package_access_only"
            )
        if (
            package_row.get("fallback_attempted") is not False
            or package_row.get("external_engine_invoked") is not False
        ):
            blockers.append("runs-today claim_package_publication must keep fallback/external false")
    return (
        {
            "status": "passed" if not blockers else "blocked",
            "row_count": len(rows),
            "support_state_counts": runs_today.get("support_state_counts"),
        },
        blockers,
    )


def validate_docs(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    checked: list[str] = []
    for rel_path, markers in REQUIRED_DOC_MARKERS.items():
        path = repo_root / rel_path
        checked.append(rel_path)
        text = read_text(path)
        if not text:
            blockers.append(f"missing required usability doc: {rel_path}")
            continue
        for marker in markers:
            if marker not in text:
                blockers.append(f"{rel_path} missing marker: {marker}")
    for rel_path in ["SECURITY.md", "LICENSE", "NOTICE", "python/pyproject.toml"]:
        path = repo_root / rel_path
        checked.append(rel_path)
        text = read_text(path)
        if not text:
            blockers.append(f"missing security/legal/package file: {rel_path}")
    pyproject = read_text(repo_root / "python/pyproject.toml")
    if "license-files = [\"LICENSE\", \"NOTICE\"]" not in pyproject:
        blockers.append("python/pyproject.toml must include LICENSE and NOTICE files")
    return {"status": "passed" if not blockers else "blocked", "checked_refs": checked}, blockers


def usability_matrix(
    dry_run_summary: dict[str, Any],
    package_summary: dict[str, Any],
    website_summary: dict[str, Any],
    benchmark_summary: dict[str, Any],
    runs_today_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "id": "clean_venv_local_wheel_install",
            "status": dry_run_summary.get("clean_venv_install_status", "missing"),
            "evidence_ref": "target/release-dry-run-proof/transcript.json",
            "claim_boundary": "local wheel artifact only; no public package publication",
        },
        {
            "id": "cli_python_generated_output_smokes",
            "status": dry_run_summary.get("status"),
            "evidence_ref": "target/release-dry-run-proof/transcript.json",
            "claim_boundary": "admitted local smokes only; not broad production runtime",
        },
        {
            "id": "package_channel_local_gate",
            "status": package_summary.get("local_gate_evidence_status", package_summary.get("status")),
            "evidence_ref": "target/package-channel-readiness-report.json",
            "claim_boundary": "channel rows remain blocked until channel-specific evidence and approval",
        },
        {
            "id": "website_learning_path",
            "status": website_summary.get("status"),
            "evidence_ref": "target/website-readiness-report.json",
            "claim_boundary": "website explains current runtime state without overclaiming",
        },
        {
            "id": "benchmark_artifact_completeness",
            "status": benchmark_summary.get("status"),
            "evidence_ref": "website/assets/benchmarks/latest/manifest.json",
            "claim_boundary": "workload-scoped evidence only; no performance/superiority claim",
        },
        {
            "id": "public_production_and_package_claims",
            "status": "blocked_not_claim_grade"
            if runs_today_summary.get("status") == "passed"
            else "blocked_missing_status_matrix",
            "evidence_ref": "docs/status/runs-today-support-matrix.json",
            "claim_boundary": "public production and package-publication claims remain false",
        },
    ]


def build_report(
    *,
    repo_root: Path,
    release_dry_run_ref: str,
    package_channel_report_ref: str,
    release_security_report_ref: str,
    contribution_governance_report_ref: str,
    final_release_rehearsal_report_ref: str,
    website_readiness_report_ref: str,
    benchmark_manifest_ref: str,
    benchmark_completeness_report_ref: str,
    runs_today_matrix_ref: str,
    dry_run: dict[str, Any] | None,
    package_report: dict[str, Any] | None,
    release_security: dict[str, Any] | None,
    contribution_governance: dict[str, Any] | None,
    final_rehearsal: dict[str, Any] | None,
    website_report: dict[str, Any] | None,
    benchmark_manifest_path: Path,
    benchmark_completeness_report: dict[str, Any] | None,
    runs_today: dict[str, Any] | None,
) -> dict[str, Any]:
    dry_run_summary, dry_run_blockers = validate_release_dry_run(repo_root, dry_run)
    package_summary, package_blockers = validate_package_channel_report(package_report)
    security_summary, security_blockers = validate_status_report(
        release_security,
        label="release security",
        schema_version="shardloom.release_security_gate_report.v1",
    )
    contribution_summary, contribution_blockers = validate_status_report(
        contribution_governance,
        label="contribution governance",
        schema_version="shardloom.contribution_governance_report.v1",
    )
    rehearsal_summary, rehearsal_blockers = validate_status_report(
        final_rehearsal,
        label="final release rehearsal",
        schema_version="shardloom.final_release_rehearsal_report.v1",
        status_fields=("status", "rehearsal_status"),
        expected_statuses=("passed",),
        allow_blocked=True,
    )
    if final_rehearsal is not None:
        for field, expected in [
            ("claim_gate_status", "not_claim_grade"),
            ("local_artifacts_only", True),
            ("public_release_claim_allowed", False),
            ("public_package_claim_allowed", False),
            (
                "publication_authorization_status",
                SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS,
            ),
            ("publication_human_approved", True),
            ("signing_key_used", False),
        ]:
            if final_rehearsal.get(field) != expected:
                rehearsal_blockers.append(f"final release rehearsal {field} must be {expected}")
    website_summary, website_blockers = validate_website_report(website_report)
    benchmark_summary, benchmark_blockers = validate_benchmark(
        benchmark_manifest_path,
        manifest_ref=benchmark_manifest_ref,
        repo_root=repo_root,
        completeness_report=benchmark_completeness_report,
    )
    runs_today_summary, runs_today_blockers = validate_runs_today(runs_today)
    docs_summary, docs_blockers = validate_docs(repo_root)

    blockers = [
        *dry_run_blockers,
        *package_blockers,
        *security_blockers,
        *contribution_blockers,
        *rehearsal_blockers,
        *website_blockers,
        *benchmark_blockers,
        *runs_today_blockers,
        *docs_blockers,
    ]
    passed = not blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "blocked",
        "production_usability_gate_status": "passed" if passed else "blocked",
        "covered_phase_items": ["GAR-RUNTIME-IMPL-4S", "GAR-RUNTIME-IMPL-5Q"],
        "claim_gate_status": "not_claim_grade",
        "claim_scope": "local_no_publication_production_usability_rehearsal",
        "production_claim_allowed": False,
        "performance_claim_allowed": False,
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
        "local_artifacts_only": True,
        "release_dry_run_transcript_ref": release_dry_run_ref,
        "package_channel_report_ref": package_channel_report_ref,
        "release_security_report_ref": release_security_report_ref,
        "contribution_governance_report_ref": contribution_governance_report_ref,
        "final_release_rehearsal_report_ref": final_release_rehearsal_report_ref,
        "website_readiness_report_ref": website_readiness_report_ref,
        "benchmark_manifest_ref": benchmark_manifest_ref,
        "benchmark_completeness_report_ref": benchmark_completeness_report_ref,
        "runs_today_matrix_ref": runs_today_matrix_ref,
        "release_dry_run": dry_run_summary,
        "package_channel": package_summary,
        "release_security": security_summary,
        "contribution_governance": contribution_summary,
        "final_release_rehearsal": rehearsal_summary,
        "website_readiness": website_summary,
        "benchmark_artifact_completeness": benchmark_summary,
        "runs_today_support_matrix": runs_today_summary,
        "docs_security_legal_learning_path": docs_summary,
        "usability_matrix": usability_matrix(
            dry_run_summary,
            package_summary,
            website_summary,
            benchmark_summary,
            runs_today_summary,
        ),
        "remaining_unsupported_paths": [
            "public package publication",
            "production-readiness claim",
            "performance/superiority/Spark-replacement claim",
            "live/authenticated object-store providers",
            "production Foundry deployment",
        ],
        "blockers": blockers,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "package_upload_attempted": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    release_dry_run_path = resolve(repo_root, args.release_dry_run_transcript)
    package_channel_report_path = resolve(repo_root, args.package_channel_report)
    release_security_report_path = resolve(repo_root, args.release_security_report)
    contribution_governance_report_path = resolve(repo_root, args.contribution_governance_report)
    final_release_rehearsal_report_path = resolve(repo_root, args.final_release_rehearsal_report)
    website_readiness_report_path = resolve(repo_root, args.website_readiness_report)
    benchmark_manifest_path = resolve(repo_root, args.benchmark_manifest)
    benchmark_completeness_report_path = resolve(
        repo_root,
        args.benchmark_completeness_report,
    )
    runs_today_matrix_path = resolve(repo_root, args.runs_today_matrix)

    report = build_report(
        repo_root=repo_root,
        release_dry_run_ref=rel(repo_root, release_dry_run_path),
        package_channel_report_ref=rel(repo_root, package_channel_report_path),
        release_security_report_ref=rel(repo_root, release_security_report_path),
        contribution_governance_report_ref=rel(repo_root, contribution_governance_report_path),
        final_release_rehearsal_report_ref=rel(repo_root, final_release_rehearsal_report_path),
        website_readiness_report_ref=rel(repo_root, website_readiness_report_path),
        benchmark_manifest_ref=rel(repo_root, benchmark_manifest_path),
        benchmark_completeness_report_ref=rel(
            repo_root,
            benchmark_completeness_report_path,
        ),
        runs_today_matrix_ref=rel(repo_root, runs_today_matrix_path),
        dry_run=load_json(release_dry_run_path),
        package_report=load_json(package_channel_report_path),
        release_security=load_json(release_security_report_path),
        contribution_governance=load_json(contribution_governance_report_path),
        final_rehearsal=load_json(final_release_rehearsal_report_path),
        website_report=load_json(website_readiness_report_path),
        benchmark_manifest_path=benchmark_manifest_path,
        benchmark_completeness_report=load_json(benchmark_completeness_report_path),
        runs_today=load_json(runs_today_matrix_path),
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if report["blockers"]:
        for blocker in report["blockers"]:
            print(f"production usability blocker: {blocker}")
        return 1
    print(f"production usability gate passed: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
