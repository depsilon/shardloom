#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate v1 security, supply-chain, and CI hardening evidence.

The gate is intentionally control-plane only. It consumes existing release
evidence reports and workflow/docs contracts, then emits a compact v1 closeout
report. Passing this gate does not approve public package publication, signing,
release tags, production readiness, performance claims, or fallback execution.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from release_report_utils import (
    fail_closed_fields,
    load_json,
    read_text,
    require_markers,
    resolve_path,
    write_json,
)
from release_channel_contract import SELECTED_V0_1_0_INSTALL_ACCESS_BOUNDARY


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_security_ci_hardening_report.v1"
SCOPE_DOC = Path("docs/architecture/v1-security-ci-hardening.md")

FALSE_FIELDS = (
    "publication_attempted",
    "tag_created",
    "secrets_required",
    "fallback_attempted",
    "external_engine_invoked",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--dependency-audit-report",
        type=Path,
        default=Path("target/dependency-audit-report.json"),
    )
    parser.add_argument(
        "--security-posture-report",
        type=Path,
        default=Path("target/security-posture-report.json"),
    )
    parser.add_argument(
        "--release-security-gate-report",
        type=Path,
        default=Path("target/release-security-gate-report.json"),
    )
    parser.add_argument(
        "--provenance-report",
        type=Path,
        default=Path("target/release-provenance-dry-run/supply-chain-release-evidence.json"),
    )
    parser.add_argument(
        "--package-channel-report",
        type=Path,
        default=Path("target/package-channel-readiness-report.json"),
    )
    parser.add_argument(
        "--ci-gate-matrix-report",
        type=Path,
        default=Path("target/ci-gate-matrix-report.json"),
    )
    parser.add_argument("--workflow", type=Path, default=Path(".github/workflows/ci.yml"))
    parser.add_argument("--matrix-doc", type=Path, default=Path("docs/release/ci-gate-matrix.md"))
    parser.add_argument("--scope-doc", type=Path, default=SCOPE_DOC)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-security-ci-hardening-report.json"),
    )
    return parser.parse_args()


def false_field_blockers(label: str, payload: dict[str, Any] | None) -> list[str]:
    if payload is None:
        return []
    return [
        f"{label} {field} must be false"
        for field in FALSE_FIELDS
        if payload.get(field) is not False
    ]


def check_dependency_audit(payload: dict[str, Any] | None) -> dict[str, Any]:
    blockers: list[str] = []
    if payload is None:
        blockers.append("missing dependency audit report")
    else:
        if payload.get("schema_version") != "shardloom.dependency_audit_report.v1":
            blockers.append("dependency audit schema_version mismatch")
        for field in [
            "cargo_deny_status",
            "cargo_audit_status",
            "pip_audit_status",
            "license_policy_status",
            "advisory_status",
        ]:
            if payload.get(field) != "passed":
                blockers.append(f"dependency audit {field}={payload.get(field, 'missing')}")
        if payload.get("fallback_dependency_absent") is not True:
            blockers.append("dependency audit fallback_dependency_absent must be true")
    return {
        "name": "dependency_audit_license_advisory_and_forbidden_fallbacks",
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
    }


def check_security_posture(payload: dict[str, Any] | None) -> dict[str, Any]:
    blockers: list[str] = []
    required_checks = (
        "codeql_workflow",
        "scorecard_workflow",
        "pypi_trusted_publisher_boundary",
        "pypi_trusted_publisher_oidc_boundary",
        "dependabot_config",
        "posture_doc",
    )
    if payload is None:
        blockers.append("missing security posture report")
    else:
        if payload.get("schema_version") != "shardloom.open_source_security_posture_report.v1":
            blockers.append("security posture schema_version mismatch")
        if payload.get("status") != "passed":
            blockers.append(f"security posture status={payload.get('status', 'missing')}")
        checks = payload.get("checks")
        if not isinstance(checks, dict):
            blockers.append("security posture checks missing")
        else:
            for check_id in required_checks:
                row = checks.get(check_id)
                if not isinstance(row, dict) or row.get("status") != "passed":
                    blockers.append(f"security posture {check_id} must pass")
        blockers.extend(false_field_blockers("security posture", payload))
    return {
        "name": "open_source_security_posture",
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
    }


def check_release_security(payload: dict[str, Any] | None) -> dict[str, Any]:
    blockers: list[str] = []
    if payload is None:
        blockers.append("missing release security gate report")
    else:
        if payload.get("schema_version") != "shardloom.release_security_gate_report.v1":
            blockers.append("release security schema_version mismatch")
        if payload.get("status") != "passed":
            blockers.extend(payload.get("blockers", ["release security gate blocked"]))
        blockers.extend(false_field_blockers("release security", payload))
    return {
        "name": "release_security_gate",
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
    }


def ref_count(payload: dict[str, Any] | None, key: str) -> int:
    rows = (payload or {}).get(key, [])
    return len(rows) if isinstance(rows, list) else 0


def check_provenance(payload: dict[str, Any] | None) -> dict[str, Any]:
    blockers: list[str] = []
    if payload is None:
        blockers.append("missing supply-chain release evidence report")
    else:
        if payload.get("schema_version") != "shardloom.supply_chain_release_evidence.v1":
            blockers.append("provenance schema_version mismatch")
        if payload.get("provenance_status") != "dry_run_unsigned_local_evidence":
            blockers.append(
                "provenance_status="
                + str(payload.get("provenance_status", "missing"))
            )
        if payload.get("signed_or_attested_status") != "not_signed_local_dry_run":
            blockers.append(
                "signed_or_attested_status="
                + str(payload.get("signed_or_attested_status", "missing"))
            )
        if payload.get("fallback_dependency_absent") is not True:
            blockers.append("provenance fallback_dependency_absent must be true")
        for key in ["artifact_refs", "sbom_refs", "checksum_refs"]:
            if ref_count(payload, key) <= 0:
                blockers.append(f"provenance missing {key}")
        blockers.extend(false_field_blockers("provenance", payload))
        for field in [
            "external_runtime_dependencies_added",
            "fallback_engine_dependency_added",
        ]:
            if payload.get(field) is not False:
                blockers.append(f"provenance {field} must be false")
    return {
        "name": "sbom_checksum_and_provenance_dry_run",
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
    }


def check_package_channel(payload: dict[str, Any] | None) -> dict[str, Any]:
    blockers: list[str] = []
    if payload is None:
        blockers.append("missing package-channel readiness report")
    else:
        if payload.get("schema_version") != "shardloom.package_channel_readiness_report.v1":
            blockers.append("package-channel schema_version mismatch")
        if payload.get("status") != "passed":
            blockers.extend(payload.get("blockers", ["package-channel readiness blocked"]))
        if payload.get("local_gate_evidence_status") != "passed":
            blockers.append(
                "local_gate_evidence_status="
                + str(payload.get("local_gate_evidence_status", "missing"))
            )
        if payload.get("package_identity_contract_status") != "passed":
            blockers.append(
                "package_identity_contract_status="
                + str(payload.get("package_identity_contract_status", "missing"))
            )
        if payload.get("claim_gate_status") != "not_claim_grade":
            blockers.append(
                "claim_gate_status=" + str(payload.get("claim_gate_status", "missing"))
            )
        if payload.get("public_package_release_claim_allowed") is not True:
            blockers.append(
                "public_package_release_claim_allowed must be true for "
                + SELECTED_V0_1_0_INSTALL_ACCESS_BOUNDARY
            )
        blockers.extend(false_field_blockers("package-channel", payload))
    return {
        "name": "package_artifact_scan_and_blocked_publication_channels",
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
    }


def check_ci_matrix(payload: dict[str, Any] | None) -> dict[str, Any]:
    blockers: list[str] = []
    required_lanes = {
        "rust_baseline",
        "rust_feature_matrix",
        "rust_msrv_validation",
        "python_compatibility_matrix",
        "python_test_shards",
        "python_tests",
        "python_package_smoke",
        "dependency_security",
        "release_package_governance_evidence",
        "release_readiness_reports",
        "website_docs_validation",
        "ci_gate_matrix_contract",
    }
    if payload is None:
        blockers.append("missing CI gate matrix report")
    else:
        if payload.get("schema_version") != "shardloom.ci_gate_matrix_report.v1":
            blockers.append("CI gate matrix schema_version mismatch")
        if payload.get("status") != "passed":
            blockers.extend(payload.get("blockers", ["CI gate matrix blocked"]))
        lanes = payload.get("lanes")
        lane_ids: set[str] = set()
        if isinstance(lanes, list):
            lane_ids = {
                str(row.get("lane_id"))
                for row in lanes
                if isinstance(row, dict)
            }
        else:
            blockers.append("CI gate matrix lanes must be a list")
        missing = sorted(required_lanes.difference(lane_ids))
        if missing:
            blockers.append("CI matrix missing lanes: " + ", ".join(missing))
        blockers.extend(false_field_blockers("CI gate matrix", payload))
    return {
        "name": "ci_gate_matrix_release_contract",
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
    }


def check_docs_and_workflow(
    *,
    workflow_text: str,
    matrix_doc_text: str,
    scope_doc_text: str,
) -> dict[str, Any]:
    blockers: list[str] = []
    workflow_markers = (
        "python-compatibility-matrix:",
        "rust-msrv:",
        'python-version: "3.10"',
        'python-version: "3.11"',
        'python-version: "3.12"',
        'python-version: "3.13"',
        "ubuntu-latest",
        "macos-latest",
        "windows-latest",
        'python scripts/write_ci_version_env.py --github-env "$GITHUB_ENV"',
        'rustup toolchain install "$SHARDLOOM_RUST_MSRV_TOOLCHAIN"',
        'rustup default "$SHARDLOOM_RUST_MSRV_TOOLCHAIN"',
        "python scripts/write_release_compatibility_lane_report.py",
        "--rust-toolchain \"$SHARDLOOM_RUST_MSRV_TOOLCHAIN\"",
        "python scripts/check_v1_security_ci_hardening.py",
        "target/v1-security-ci-hardening-report.json",
        "retention-days: 14",
    )
    matrix_doc_markers = (
        "python_compatibility_matrix",
        "rust_msrv_validation",
        "Python 3.10 through 3.13",
        "ubuntu-latest",
        "macos-latest",
        "windows-latest",
        "Rust MSRV derived from root Cargo.toml",
        "python scripts/check_v1_security_ci_hardening.py",
        "target/v1-security-ci-hardening-report.json",
    )
    scope_doc_markers = (
        SCHEMA_VERSION,
        "dependency audit",
        "license classification",
        "forbidden-fallback dependency absence",
        "SBOM",
        "checksum manifest",
        "provenance",
        "vulnerability scan",
        "package artifact scan",
        "no-signing rationale",
        "Trusted Publisher/OIDC",
        "long-lived package upload tokens",
        "Python 3.10 through 3.13",
        "Rust MSRV derived from root Cargo.toml",
        "OS matrix",
        "publication_attempted=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    )
    blockers.extend(require_markers("CI workflow", workflow_text, workflow_markers))
    blockers.extend(require_markers("CI gate matrix doc", matrix_doc_text, matrix_doc_markers))
    blockers.extend(require_markers("v1 security/CI hardening doc", scope_doc_text, scope_doc_markers))
    return {
        "name": "docs_workflow_and_policy_contracts",
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    dependency_audit = load_json(
        resolve_path(repo_root, args.dependency_audit_report),
        missing_ok=True,
    )
    security_posture = load_json(
        resolve_path(repo_root, args.security_posture_report),
        missing_ok=True,
    )
    release_security = load_json(
        resolve_path(repo_root, args.release_security_gate_report),
        missing_ok=True,
    )
    provenance = load_json(resolve_path(repo_root, args.provenance_report), missing_ok=True)
    package_channel = load_json(
        resolve_path(repo_root, args.package_channel_report),
        missing_ok=True,
    )
    ci_matrix = load_json(resolve_path(repo_root, args.ci_gate_matrix_report), missing_ok=True)
    workflow_text = read_text(resolve_path(repo_root, args.workflow))
    matrix_doc_text = read_text(resolve_path(repo_root, args.matrix_doc))
    scope_doc_text = read_text(resolve_path(repo_root, args.scope_doc))

    checks = [
        check_dependency_audit(dependency_audit),
        check_security_posture(security_posture),
        check_release_security(release_security),
        check_provenance(provenance),
        check_package_channel(package_channel),
        check_ci_matrix(ci_matrix),
        check_docs_and_workflow(
            workflow_text=workflow_text,
            matrix_doc_text=matrix_doc_text,
            scope_doc_text=scope_doc_text,
        ),
    ]
    blockers = [
        f"{check['name']}: {blocker}"
        for check in checks
        for blocker in check["blockers"]
    ]
    passed = not blockers
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "blocked",
        "v1_scope_ready": passed,
        "security_ci_hardening_evidence_ready": passed,
        "checks": checks,
        "blockers": blockers,
        "dependency_audit_report_ref": str(args.dependency_audit_report).replace("\\", "/"),
        "security_posture_report_ref": str(args.security_posture_report).replace("\\", "/"),
        "release_security_gate_report_ref": str(args.release_security_gate_report).replace("\\", "/"),
        "provenance_report_ref": str(args.provenance_report).replace("\\", "/"),
        "package_channel_report_ref": str(args.package_channel_report).replace("\\", "/"),
        "ci_gate_matrix_report_ref": str(args.ci_gate_matrix_report).replace("\\", "/"),
        "scope_doc_ref": str(args.scope_doc).replace("\\", "/"),
        "signing_policy": "not_signed_local_dry_run_until_maintainer_approval",
        "trusted_publisher_oidc_required": True,
        "long_lived_package_upload_tokens_allowed": False,
        "package_publication_requires_human_approval": True,
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "spark_replacement_claim_allowed": False,
        "package_upload_attempted": False,
        "signing_key_used": False,
        "secrets_required": False,
        "claim_gate_status": "not_claim_grade",
        **fail_closed_fields(),
    }
    output = resolve_path(repo_root, args.output)
    write_json(output, report)
    print(output)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
