#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the finished-product v1 release boundary firewall.

This gate is intentionally claim-safe: it proves the current support envelope,
package dry-run evidence, public docs, and package metadata do not imply public
package publication, production platform support, performance superiority, or
external-engine fallback. It does not publish packages or create releases.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

from check_package_channel_readiness import validate_matrix as validate_package_channel_matrix
from check_public_claim_language import (
    SCHEMA_VERSION as PUBLIC_CLAIM_LANGUAGE_SCHEMA_VERSION,
)
from check_public_claim_language import build_report as build_public_claim_language_report
from check_public_status_docs import SCHEMA_VERSION as PUBLIC_STATUS_DOCS_SCHEMA_VERSION
from check_public_status_docs import build_report as build_public_status_docs_report
from check_v1_docs_productization import SCHEMA_VERSION as V1_DOCS_PRODUCTIZATION_SCHEMA_VERSION
from check_v1_docs_productization import build_report as build_v1_docs_productization_report
from check_v1_inclusion_scope import SCHEMA_VERSION as V1_INCLUSION_SCOPE_SCHEMA_VERSION
from check_v1_inclusion_scope import build_report as build_v1_inclusion_scope_report
from release_report_utils import fail_closed_fields, load_json, read_text, resolve_path, write_json


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_release_boundary_report.v1"

RUNS_TODAY_MATRIX = Path("docs/status/runs-today-support-matrix.json")
PACKAGE_CHANNEL_MATRIX = Path("docs/release/package-channel-readiness-matrix.json")
V1_SUPPORTED_DOC = Path("docs/getting-started/v1-supported-unsupported.md")
PACKAGE_USER_INSTALL_DOC = Path("docs/getting-started/package-user-install.md")
PYPROJECT = Path("python/pyproject.toml")
PKG_INFO = Path("python/src/shardloom.egg-info/PKG-INFO")
RELEASE_DRY_RUN_TRANSCRIPT = Path("target/release-dry-run-proof/transcript.json")
PACKAGE_CHANNEL_REPORT = Path("target/package-channel-readiness-report.json")

V1_SUPPORT_ENVELOPE = {
    "local_first_10_minutes": "source-checkout dry run and local Python examples",
    "front_doors": "scoped CLI and Python source/local Vortex front doors",
    "input_formats": "local CSV, JSON/JSONL/NDJSON, generated rows, local Vortex, and feature-gated flat scalar compatibility formats",
    "output_targets": "local inline JSONL/CSV, feature-gated local compatibility exports, and local Vortex writes",
    "evidence_boundary": "no public package, production, superiority, broad SQL/DataFrame, object-store, lakehouse, Foundry, live/hybrid, distributed, or fallback claim",
}

UNSUPPORTED_PRODUCTION_FAMILIES = (
    "public package channels",
    "production readiness",
    "performance superiority",
    "Spark displacement or broad engine replacement",
    "broad SQL/DataFrame parity",
    "object-store runtime",
    "lakehouse/table runtime",
    "Foundry production integration",
    "distributed runtime",
    "live/hybrid runtime",
    "arbitrary UDF/plugin/effect execution",
)

FALSE_SAFETY_FIELDS = (
    "publication_attempted",
    "tag_created",
    "secrets_required",
    "package_upload_attempted",
    "package_channel_submission_attempted",
    "oci_push_attempted",
    "signing_key_used",
    "fallback_attempted",
    "external_engine_invoked",
    "public_package_release_claim_allowed",
    "public_package_claim_allowed",
)

REQUIRED_DRY_RUN_TRUE_FIELDS = (
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
)

SUPPORT_DOC_HEADER_EXPECTED = {
    "runs_today_schema_version": "shardloom.runs_today_support_matrix.v1",
    "package_channel_schema_version": "shardloom.package_channel_readiness_matrix.v1",
    "fallback_attempted": "false",
    "external_engine_invoked": "false",
    "performance_claim_allowed": "false",
    "package_publication_allowed": "false",
}

PACKAGE_CHANNEL_BLOCK_EXPECTED = {
    "package_install_commands_visible": "false",
    "public_package_release_claim_allowed": "false",
    "publication_attempted": "false",
    "tag_created": "false",
    "package_upload_attempted": "false",
}

PYPROJECT_REQUIRED_MARKERS = (
    'name = "shardloom"',
    "Pre-release Python client",
    '"Development Status :: 2 - Pre-Alpha"',
    'license = "Apache-2.0"',
    'requires-python = ">=3.10"',
)

PKG_INFO_REQUIRED_MARKERS = (
    "Name: shardloom",
    "Summary: Pre-release Python client",
    "Classifier: Development Status :: 2 - Pre-Alpha",
    "License-Expression: Apache-2.0",
    "Public status is owned by `docs/release/public-status-matrix.md`",
)

FORBIDDEN_PACKAGE_METADATA_MARKERS = (
    "Development Status :: 5 - Production/Stable",
    "Development Status :: 4 - Beta",
    "Production/Stable",
    "production-ready",
    "Spark replacement",
    "drop-in replacement",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-release-boundary-report.json"),
    )
    parser.add_argument(
        "--release-dry-run-transcript",
        type=Path,
        default=RELEASE_DRY_RUN_TRANSCRIPT,
    )
    parser.add_argument(
        "--package-channel-report",
        type=Path,
        default=PACKAGE_CHANNEL_REPORT,
    )
    return parser.parse_args()


def first_text_block_kv(text: str) -> dict[str, str]:
    match = re.search(r"```text\n(?P<body>.*?)\n```", text, re.DOTALL)
    if match is None:
        return {}
    values: dict[str, str] = {}
    for raw in match.group("body").splitlines():
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def text_block_after_heading(text: str, heading: str) -> dict[str, str]:
    marker = f"## {heading}"
    _, _, tail = text.partition(marker)
    return first_text_block_kv(tail)


def require_report_passed(
    label: str,
    report: dict[str, Any],
    expected_schema: str,
) -> list[str]:
    blockers: list[str] = []
    if report.get("schema_version") != expected_schema:
        blockers.append(f"{label}: schema_version={report.get('schema_version', 'missing')}")
    if report.get("status") != "passed":
        blockers.append(f"{label}: status={report.get('status', 'missing')}")
    upstream = report.get("blockers")
    if isinstance(upstream, list) and upstream:
        blockers.extend(f"{label}: {blocker}" for blocker in upstream[:20])
    for field in ("fallback_attempted", "external_engine_invoked"):
        if report.get(field) is not False:
            blockers.append(f"{label}: {field} must be false")
    return blockers


def validate_runs_today(matrix: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if matrix is None:
        return {"status": "missing"}, ["runs-today matrix missing"]
    if matrix.get("schema_version") != "shardloom.runs_today_support_matrix.v1":
        blockers.append(
            "runs-today schema_version=" + str(matrix.get("schema_version", "missing"))
        )
    for field in [
        "all_rows_fallback_attempted_false",
        "all_rows_external_engine_invoked_false",
        "all_rows_no_fallback_no_external_engine",
    ]:
        if matrix.get(field) is not True:
            blockers.append(f"runs-today {field} must be true")
    for field in [
        "runtime_expansion_allowed",
        "package_publication_allowed",
        "performance_claim_allowed",
    ]:
        if matrix.get(field) is not False:
            blockers.append(f"runs-today {field} must be false")
    blocked_rows = [
        str(row.get("id", "unknown"))
        for row in matrix.get("rows", [])
        if isinstance(row, dict)
        and row.get("support_state") in {"blocked", "future", "report_only"}
    ]
    summary = {
        "schema_version": matrix.get("schema_version"),
        "status": "passed" if not blockers else "blocked",
        "row_count": matrix.get("row_count"),
        "support_state_counts": matrix.get("support_state_counts", {}),
        "blocked_or_report_only_rows": blocked_rows,
    }
    return summary, blockers


def validate_support_doc(text: str) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    header = first_text_block_kv(text)
    package_block = text_block_after_heading(text, "Package Channels")
    for key, expected in SUPPORT_DOC_HEADER_EXPECTED.items():
        if header.get(key) != expected:
            blockers.append(f"v1 supported doc header {key}={header.get(key, 'missing')}")
    for key, expected in PACKAGE_CHANNEL_BLOCK_EXPECTED.items():
        if package_block.get(key) != expected:
            blockers.append(
                f"v1 supported doc package block {key}={package_block.get(key, 'missing')}"
            )
    for marker in [
        "unsupported_example_broad_sql",
        "unsupported_example_object_store",
        "unsupported_example_foundry",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ]:
        if marker not in text:
            blockers.append(f"v1 supported doc missing marker {marker}")
    return {
        "status": "passed" if not blockers else "blocked",
        "header": header,
        "package_channels": package_block,
    }, blockers


def validate_package_metadata(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    checked = [PYPROJECT.as_posix()]
    pyproject = read_text(resolve_path(repo_root, PYPROJECT))
    for marker in PYPROJECT_REQUIRED_MARKERS:
        if marker not in pyproject:
            blockers.append(f"{PYPROJECT.as_posix()}: missing marker {marker!r}")
    for marker in FORBIDDEN_PACKAGE_METADATA_MARKERS:
        if marker in pyproject:
            blockers.append(f"{PYPROJECT.as_posix()}: forbidden marker {marker!r}")

    pkg_info_path = resolve_path(repo_root, PKG_INFO)
    if pkg_info_path.exists():
        checked.append(PKG_INFO.as_posix())
        pkg_info = read_text(pkg_info_path)
        for marker in PKG_INFO_REQUIRED_MARKERS:
            if marker not in pkg_info:
                blockers.append(f"{PKG_INFO.as_posix()}: missing marker {marker!r}")
        for marker in FORBIDDEN_PACKAGE_METADATA_MARKERS:
            if marker in pkg_info:
                blockers.append(f"{PKG_INFO.as_posix()}: forbidden marker {marker!r}")
    return {
        "status": "passed" if not blockers else "blocked",
        "checked_files": checked,
    }, blockers


def validate_package_matrix(matrix: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    blockers = validate_package_channel_matrix(matrix)
    channels = matrix.get("channels", []) if isinstance(matrix, dict) else []
    ready_channels = [
        row.get("channel_id")
        for row in channels
        if isinstance(row, dict) and row.get("ready") is True
    ]
    blocked_channels = [
        row.get("channel_id")
        for row in channels
        if isinstance(row, dict) and row.get("ready") is not True
    ]
    if isinstance(matrix, dict):
        for field in [
            "publication_attempted",
            "tag_created",
            "package_channel_submission_attempted",
            "fallback_engine_dependency_added",
            "external_engine_runtime_dependency_added",
            "package_access_implies_production_readiness",
        ]:
            if matrix.get(field) is not False:
                blockers.append(f"package matrix {field} must be false")
        if matrix.get("public_package_release_claim_allowed") is not False:
            blockers.append("package matrix public_package_release_claim_allowed must be false")
    return {
        "status": "passed" if not blockers else "blocked",
        "schema_version": matrix.get("schema_version") if isinstance(matrix, dict) else None,
        "matrix_status": matrix.get("status") if isinstance(matrix, dict) else None,
        "ready_channel_count": len(ready_channels),
        "blocked_channel_count": len(blocked_channels),
        "blocked_channels": blocked_channels,
    }, blockers


def validate_release_dry_run(payload: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if payload is None:
        return {"status": "missing"}, ["release dry-run transcript missing"]
    if payload.get("schema_version") != "shardloom.release_dry_run_proof.v1":
        blockers.append(
            "release dry-run schema_version=" + str(payload.get("schema_version", "missing"))
        )
    if payload.get("proof_status") != "passed":
        blockers.append("release dry-run proof_status=" + str(payload.get("proof_status")))
    if payload.get("clean_venv_install_status") != "passed":
        blockers.append(
            "release dry-run clean_venv_install_status="
            + str(payload.get("clean_venv_install_status"))
        )
    for field in REQUIRED_DRY_RUN_TRUE_FIELDS:
        if payload.get(field) is not True:
            blockers.append(f"release dry-run {field} must be true")
    if payload.get("benchmark_smoke_required_for_package_release") is not False:
        blockers.append(
            "release dry-run benchmark_smoke_required_for_package_release must be false"
        )
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            blockers.append(f"release dry-run {field} must be false")
    for field in [
        "external_runtime_dependencies_added",
        "fallback_engine_dependency_added",
    ]:
        if payload.get(field) is not False:
            blockers.append(f"release dry-run {field} must be false")
    return {
        "status": "passed" if not blockers else "blocked",
        "proof_status": payload.get("proof_status"),
        "clean_venv_install_status": payload.get("clean_venv_install_status"),
        "benchmark_smoke_status": payload.get("benchmark_smoke_status"),
        "benchmark_smoke_required_for_package_release": payload.get(
            "benchmark_smoke_required_for_package_release"
        ),
        "local_wheel": payload.get("local_wheel"),
        "local_cli_binary": payload.get("local_cli_binary"),
    }, blockers


def validate_package_channel_report(
    payload: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if payload is None:
        return {"status": "missing"}, ["package-channel readiness report missing"]
    if payload.get("schema_version") != "shardloom.package_channel_readiness_report.v1":
        blockers.append(
            "package-channel report schema_version="
            + str(payload.get("schema_version", "missing"))
        )
    if payload.get("status") != "passed":
        blockers.append("package-channel report status=" + str(payload.get("status")))
    if payload.get("local_gate_evidence_status") != "passed":
        blockers.append(
            "package-channel local_gate_evidence_status="
            + str(payload.get("local_gate_evidence_status"))
        )
    if payload.get("package_identity_contract_status") != "passed":
        blockers.append(
            "package-channel package_identity_contract_status="
            + str(payload.get("package_identity_contract_status", "missing"))
        )
    if payload.get("ready_channel_count") != 0:
        blockers.append("package-channel ready_channel_count must remain 0 before approval")
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            blockers.append(f"package-channel report {field} must be false")
    return {
        "status": "passed" if not blockers else "blocked",
        "report_status": payload.get("status"),
        "local_gate_evidence_status": payload.get("local_gate_evidence_status"),
        "package_identity_contract_status": payload.get("package_identity_contract_status"),
        "ready_channel_count": payload.get("ready_channel_count"),
    }, blockers


def build_report(
    repo_root: Path,
    *,
    release_dry_run_transcript: Path = RELEASE_DRY_RUN_TRANSCRIPT,
    package_channel_report: Path = PACKAGE_CHANNEL_REPORT,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    blockers: list[str] = []

    runs_today = load_json(resolve_path(repo_root, RUNS_TODAY_MATRIX), missing_ok=True)
    package_matrix = load_json(resolve_path(repo_root, PACKAGE_CHANNEL_MATRIX), missing_ok=True)
    dry_run = load_json(resolve_path(repo_root, release_dry_run_transcript), missing_ok=True)
    package_report = load_json(resolve_path(repo_root, package_channel_report), missing_ok=True)

    runs_today_summary, runs_today_blockers = validate_runs_today(runs_today)
    support_doc_summary, support_doc_blockers = validate_support_doc(
        read_text(resolve_path(repo_root, V1_SUPPORTED_DOC))
    )
    package_metadata_summary, package_metadata_blockers = validate_package_metadata(repo_root)
    package_matrix_summary, package_matrix_blockers = validate_package_matrix(package_matrix)
    release_dry_run_summary, release_dry_run_blockers = validate_release_dry_run(dry_run)
    package_report_summary, package_report_blockers = validate_package_channel_report(
        package_report
    )

    public_claim_language_report = build_public_claim_language_report(repo_root)
    public_status_docs_report = build_public_status_docs_report(repo_root)
    v1_docs_productization_report = build_v1_docs_productization_report(repo_root)
    v1_inclusion_scope_report = build_v1_inclusion_scope_report(repo_root)

    subreport_blockers = []
    subreport_blockers.extend(
        require_report_passed(
            "public claim language",
            public_claim_language_report,
            PUBLIC_CLAIM_LANGUAGE_SCHEMA_VERSION,
        )
    )
    subreport_blockers.extend(
        require_report_passed(
            "public status docs",
            public_status_docs_report,
            PUBLIC_STATUS_DOCS_SCHEMA_VERSION,
        )
    )
    subreport_blockers.extend(
        require_report_passed(
            "v1 docs productization",
            v1_docs_productization_report,
            V1_DOCS_PRODUCTIZATION_SCHEMA_VERSION,
        )
    )
    subreport_blockers.extend(
        require_report_passed(
            "v1 inclusion scope",
            v1_inclusion_scope_report,
            V1_INCLUSION_SCOPE_SCHEMA_VERSION,
        )
    )

    for section_blockers in [
        runs_today_blockers,
        support_doc_blockers,
        package_metadata_blockers,
        package_matrix_blockers,
        release_dry_run_blockers,
        package_report_blockers,
        subreport_blockers,
    ]:
        blockers.extend(section_blockers)

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "failed",
        "v1_support_envelope": V1_SUPPORT_ENVELOPE,
        "unsupported_production_families": list(UNSUPPORTED_PRODUCTION_FAMILIES),
        "runs_today_matrix": runs_today_summary,
        "support_doc": support_doc_summary,
        "package_metadata": package_metadata_summary,
        "package_channel_matrix": package_matrix_summary,
        "release_dry_run": release_dry_run_summary,
        "package_channel_report": package_report_summary,
        "public_claim_language_status": public_claim_language_report.get("status", "missing"),
        "public_status_docs_status": public_status_docs_report.get("status", "missing"),
        "v1_docs_productization_status": v1_docs_productization_report.get("status", "missing"),
        "v1_inclusion_scope_status": v1_inclusion_scope_report.get("status", "missing"),
        "package_publication_allowed": False,
        "claim_gate_status": "not_claim_grade",
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    report = build_report(
        repo_root,
        release_dry_run_transcript=args.release_dry_run_transcript,
        package_channel_report=args.package_channel_report,
    )
    output = resolve_path(repo_root, args.output)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
