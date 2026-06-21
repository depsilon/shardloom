#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the common production certification gate without claiming production readiness.

The default mode proves that production workload declarations are schema-valid, claim-safe, and
fail closed when production evidence is missing. Future release commands may use
``--require-production-ready-workload`` to require at least one fully certified workload.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from release_report_utils import fail_closed_fields, load_json, read_text, resolve_path, write_json


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.production_workload_declarations.v1"
REPORT_SCHEMA_VERSION = "shardloom.production_certification_gate.v1"
DEFAULT_MATRIX = Path("docs/release/production-certification-workloads.json")
DEFAULT_OUTPUT = Path("target/production-certification-gate.json")

REQUIRED_EVIDENCE_KEYS = (
    "runtime_execution",
    "correctness",
    "native_io_certificate",
    "execution_certificate",
    "fault_tolerance",
    "memory_backpressure",
    "benchmark",
    "security_governance",
    "release_api_stability",
    "unsupported_diagnostics",
)

TECHNIQUE_REVIEW_KEYS = (
    "pulseweave",
    "capillary_work_units",
    "dynamic_admission_work_shaping",
    "metadata_first_execution",
    "timing_surface_separation",
    "evidence_tier_controls",
)

FALSE_SAFETY_FIELDS = (
    "production_claim_allowed",
    "performance_claim_allowed",
    "public_release_claim_allowed",
    "public_package_claim_allowed",
    "publication_attempted",
    "tag_created",
    "secrets_required",
    "package_upload_attempted",
    "package_channel_submission_attempted",
    "oci_push_attempted",
    "fallback_attempted",
    "external_engine_invoked",
    "fallback_engine_dependency_added",
    "external_engine_runtime_dependency_added",
)

READY_STATUS = "production_ready"
BLOCKED_STATUS_PREFIXES = ("blocked_", "not_ready_", "v1_candidate_")
EVIDENCE_PASS_PREFIXES = ("passed", "certified")
UNSUPPORTED_STATUS = "deterministic_unsupported_diagnostic"
OBJECT_STORE_LOCAL_EMULATOR_WORKLOAD_ID = "object_store_local_emulator_runtime_v1_candidate"
OBJECT_STORE_REQUIRED_EFFECT_PERMISSIONS = {
    "explicit_local_file_read",
    "explicit_local_file_write",
    "local_emulator_object_store_io",
    "no_network",
    "no_live_credentials",
    "no_provider_probe",
    "no_request_signing",
    "no_table_commit",
}
OBJECT_STORE_REQUIRED_SECURITY_EVIDENCE_REFS = {
    "provider_admission_report_id=shardloom.object_store_provider_admission.v1",
    "provider_admission_status=admitted_local_emulator",
    "provider_admission_status=admitted_public_no_credential_fixture",
    "provider_admission_status=blocked_live_provider_no_probe",
    "credential_policy_status=credential_policy_required_not_admitted",
    "request_signing_performed=false",
    "request_signing_status=blocked_not_invoked",
    "uri_redaction_policy_status",
    "explain_estimate_doctor_probe_policy=static_no_provider_probe_default",
    "capability_discovery_probe_policy=static_capability_report_no_provider_probe",
}
OBJECT_STORE_REQUIRED_UNSUPPORTED_OPERATIONS = {
    "live_s3_provider_runtime",
    "live_gcs_provider_runtime",
    "live_adls_provider_runtime",
    "credentialed_object_store_read",
    "object_store_table_commit",
    "object_store_distributed_runtime",
}
LOCAL_TABLE_MANIFEST_WORKLOAD_ID = "local_manifest_table_runtime_v1_candidate"
LOCAL_TABLE_REQUIRED_EFFECT_PERMISSIONS = {
    "explicit_local_file_read",
    "explicit_local_file_write",
    "local_manifest_table_metadata_io",
    "local_manifest_write_io",
    "no_network",
    "no_live_credentials",
    "no_provider_probe",
    "no_external_catalog",
    "no_object_store_io",
    "no_external_table_format_dependency",
}
LOCAL_TABLE_REQUIRED_UNSUPPORTED_OPERATIONS = {
    "external_iceberg_runtime",
    "external_delta_runtime",
    "external_hudi_runtime",
    "external_catalog_resolution",
    "object_store_table_commit",
    "merge_update_delete_runtime",
}
LOCAL_TABLE_REQUIRED_SECURITY_EVIDENCE_REFS = {
    "credential_policy_status=not_required_local_manifest_fixture",
    "credential_resolution_performed=false",
    "network_probe_performed=false",
    "provider_probe_performed=false",
    "external_catalog_resolution_performed=false",
    "external_table_format_dependency_invoked=false",
    "catalog_io_performed=false",
    "object_store_io=false",
}

CLAIM_SURFACE_REFS = {
    "README.md": (
        "production_claim_allowed",
        "Must remain false unless a later production gate authorizes the specific workload.",
    ),
    "docs/release/public-status-matrix.md": (
        "production_claim_allowed=false",
        "performance_claim_allowed=false",
    ),
    "docs/status/runs-today-support-matrix.json": (
        '"performance_claim_allowed": false',
        '"package_publication_allowed": true',
        '"claim_production_readiness"',
    ),
    "python/pyproject.toml": (
        '"Development Status :: 2 - Pre-Alpha"',
        'name = "shardloom"',
    ),
    "website-src/src/pages/benchmarks.astro": (
        "https://benchmark.clickhouse.com/",
        "stale local artifacts do not read as a",
        "current public leaderboard",
        "Performance claims stay explicit.",
    ),
}

FORBIDDEN_CLAIM_RE = re.compile(
    r"Development Status :: 5 - Production/Stable|"
    r"\bproduction-ready\b|"
    r"\bproduction ready\b|"
    r"\bSpark replacement\b|"
    r"\bdrop-in replacement\b",
    re.IGNORECASE,
)

FORBIDDEN_CLAIM_ALLOWLIST = {
    "README.md",
    "docs/release/public-status-matrix.md",
    "docs/status/runs-today-support-matrix.json",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--require-production-ready-workload",
        action="store_true",
        help="Fail unless at least one declared workload is fully production-ready.",
    )
    return parser.parse_args()


def evidence_passed(status: object) -> bool:
    if not isinstance(status, str):
        return False
    return status == READY_STATUS or any(status.startswith(prefix) for prefix in EVIDENCE_PASS_PREFIXES)


def is_blocked_status(status: object) -> bool:
    return isinstance(status, str) and any(status.startswith(prefix) for prefix in BLOCKED_STATUS_PREFIXES)


def require_false_fields(payload: dict[str, Any], label: str, fields: tuple[str, ...]) -> list[str]:
    return [
        f"{label}: {field} must be false"
        for field in fields
        if field in payload and payload.get(field) is not False
    ]


def validate_claim_surfaces(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    rows: list[dict[str, Any]] = []
    for rel_path, markers in CLAIM_SURFACE_REFS.items():
        path = resolve_path(repo_root, Path(rel_path))
        text = read_text(path)
        row_blockers: list[str] = []
        if not text:
            row_blockers.append(f"missing claim surface {rel_path}")
        else:
            for marker in markers:
                if marker not in text:
                    row_blockers.append(f"{rel_path} missing marker {marker!r}")
            if rel_path not in FORBIDDEN_CLAIM_ALLOWLIST:
                match = FORBIDDEN_CLAIM_RE.search(text)
                if match is not None:
                    row_blockers.append(
                        f"{rel_path} contains forbidden production claim marker {match.group(0)!r}"
                    )
        rows.append(
            {
                "path": rel_path,
                "status": "passed" if not row_blockers else "blocked",
                "markers_checked": list(markers),
                "blockers": row_blockers,
            }
        )
        blockers.extend(row_blockers)
    return {"status": "passed" if not blockers else "blocked", "surfaces": rows}, blockers


def validate_required_reports(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    report_specs = (
        (
            "user_route_capability",
            Path("target/user-route-capability-report.json"),
            "shardloom.user_route_capability_report.v1",
        ),
        (
            "runtime_gap_family_burn_down",
            Path("target/runtime-gap-family-burn-down.json"),
            "shardloom.runtime_gap_family_burn_down.v1",
        ),
        (
            "v1_correctness_conformance",
            Path("target/v1-correctness-conformance-report.json"),
            "shardloom.v1_correctness_conformance_report.v1",
        ),
        (
            "v1_local_resource_safety",
            Path("target/v1-local-resource-safety-report.json"),
            "shardloom.v1_local_resource_safety_report.v1",
        ),
        (
            "v1_release_boundary",
            Path("target/v1-release-boundary-report.json"),
            "shardloom.v1_release_boundary_report.v1",
        ),
    )
    blockers: list[str] = []
    rows: list[dict[str, Any]] = []
    for name, rel_path, schema_version in report_specs:
        payload = load_json(resolve_path(repo_root, rel_path), missing_ok=True)
        row_blockers: list[str] = []
        if payload is None:
            row_blockers.append(f"{name}: missing {rel_path.as_posix()}")
        else:
            if payload.get("schema_version") != schema_version:
                row_blockers.append(
                    f"{name}: schema_version={payload.get('schema_version', 'missing')}"
                )
            if payload.get("status", "passed") not in {"passed", None}:
                row_blockers.append(f"{name}: status={payload.get('status')}")
            row_blockers.extend(require_false_fields(payload, name, ("fallback_attempted", "external_engine_invoked")))
        rows.append(
            {
                "name": name,
                "path": rel_path.as_posix(),
                "schema_version": payload.get("schema_version") if payload else None,
                "status": "passed" if not row_blockers else "blocked",
                "blockers": row_blockers,
            }
        )
        blockers.extend(row_blockers)
    return {"status": "passed" if not blockers else "blocked", "reports": rows}, blockers


def validate_unsupported_rows(
    workload_id: str,
    rows: object,
) -> tuple[list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    if not isinstance(rows, list) or not rows:
        return [], [f"{workload_id}: unsupported_diagnostics must be a non-empty list"]
    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            blockers.append(f"{workload_id}: unsupported_diagnostics[{index}] must be an object")
            continue
        operation = row.get("operation")
        code = row.get("diagnostic_code")
        status = row.get("status")
        row_blockers: list[str] = []
        if not isinstance(operation, str) or not operation.strip():
            row_blockers.append("operation missing")
        if not isinstance(code, str) or not code.startswith("SL_PROD_UNSUPPORTED_"):
            row_blockers.append("diagnostic_code must start with SL_PROD_UNSUPPORTED_")
        if status != UNSUPPORTED_STATUS:
            row_blockers.append(f"status must be {UNSUPPORTED_STATUS}")
        if row.get("fallback_attempted") is not False:
            row_blockers.append("fallback_attempted must be false")
        if row.get("external_engine_invoked") is not False:
            row_blockers.append("external_engine_invoked must be false")
        normalized.append(
            {
                "operation": operation,
                "diagnostic_code": code,
                "status": status,
                "blockers": row_blockers,
            }
        )
        blockers.extend(f"{workload_id}: {operation or index}: {item}" for item in row_blockers)
    return normalized, blockers


def validate_workload(row: object) -> tuple[dict[str, Any], list[str], list[str]]:
    validation_blockers: list[str] = []
    production_blockers: list[str] = []
    if not isinstance(row, dict):
        return {"status": "blocked", "workload_id": "unknown"}, ["workload row must be an object"], []

    workload_id = str(row.get("workload_id", "unknown"))
    for field in (
        "workload_id",
        "workload_name",
        "v1_scope_classification",
        "readiness_status",
        "environment",
        "data_scale",
        "statefulness",
        "security_posture",
        "unsupported_edge_boundary",
    ):
        if not isinstance(row.get(field), str) or not row[field].strip():
            validation_blockers.append(f"{workload_id}: {field} must be a non-empty string")

    for field in ("input_formats", "output_formats", "effect_permissions"):
        value = row.get(field)
        if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
            validation_blockers.append(f"{workload_id}: {field} must be a non-empty string list")

    validation_blockers.extend(
        require_false_fields(
            row,
            workload_id,
            (
                "production_claim_allowed",
                "performance_claim_allowed",
                "fallback_attempted",
                "external_engine_invoked",
            ),
        )
    )

    technique_review = row.get("technique_review")
    if not isinstance(technique_review, dict):
        validation_blockers.append(f"{workload_id}: technique_review must be an object")
    else:
        for key in TECHNIQUE_REVIEW_KEYS:
            decision = technique_review.get(key, {})
            if not isinstance(decision, dict):
                validation_blockers.append(f"{workload_id}: technique_review.{key} must be an object")
                continue
            if not isinstance(decision.get("decision"), str) or not decision["decision"].strip():
                validation_blockers.append(f"{workload_id}: technique_review.{key}.decision missing")
            if not isinstance(decision.get("reason"), str) or not decision["reason"].strip():
                validation_blockers.append(f"{workload_id}: technique_review.{key}.reason missing")

    evidence = row.get("evidence")
    evidence_statuses: dict[str, str | None] = {}
    if not isinstance(evidence, dict):
        validation_blockers.append(f"{workload_id}: evidence must be an object")
    else:
        if set(evidence) != set(REQUIRED_EVIDENCE_KEYS):
            validation_blockers.append(
                f"{workload_id}: evidence keys must match the production certification contract"
            )
        for key in REQUIRED_EVIDENCE_KEYS:
            evidence_row = evidence.get(key)
            if not isinstance(evidence_row, dict):
                validation_blockers.append(f"{workload_id}: evidence.{key} must be an object")
                evidence_statuses[key] = None
                continue
            status = evidence_row.get("status")
            evidence_statuses[key] = status if isinstance(status, str) else None
            if not isinstance(status, str) or not status.strip():
                validation_blockers.append(f"{workload_id}: evidence.{key}.status missing")
            refs = evidence_row.get("evidence_refs")
            if not isinstance(refs, list) or not refs or not all(isinstance(item, str) for item in refs):
                validation_blockers.append(
                    f"{workload_id}: evidence.{key}.evidence_refs must be a non-empty string list"
                )

    unsupported_rows, unsupported_blockers = validate_unsupported_rows(
        workload_id,
        row.get("unsupported_diagnostics"),
    )
    validation_blockers.extend(unsupported_blockers)

    production_ready = row.get("production_ready") is True
    readiness_status = row.get("readiness_status")
    nonpassing = [
        f"{key}={status}"
        for key, status in evidence_statuses.items()
        if not evidence_passed(status)
    ]
    if production_ready:
        if readiness_status != READY_STATUS:
            validation_blockers.append(
                f"{workload_id}: production_ready rows require readiness_status={READY_STATUS}"
            )
        if row.get("claim_gate_status") != "claim_grade":
            validation_blockers.append(f"{workload_id}: production_ready rows require claim_grade")
        production_blockers.extend(nonpassing)
        if production_blockers:
            validation_blockers.append(
                f"{workload_id}: production_ready cannot have missing evidence: "
                + "; ".join(production_blockers)
            )
    else:
        if row.get("claim_gate_status") != "not_claim_grade":
            validation_blockers.append(
                f"{workload_id}: non-production rows require claim_gate_status=not_claim_grade"
            )
        if not is_blocked_status(readiness_status):
            validation_blockers.append(
                f"{workload_id}: non-production rows require blocked readiness_status"
            )
        declared_blockers = row.get("production_blockers")
        if not isinstance(declared_blockers, list) or not declared_blockers:
            validation_blockers.append(
                f"{workload_id}: blocked workloads must list production_blockers"
            )
        production_blockers.extend(nonpassing)
        if not production_blockers:
            validation_blockers.append(
                f"{workload_id}: blocked workload must have at least one non-passing evidence key"
            )

    summary = {
        "workload_id": workload_id,
        "workload_name": row.get("workload_name"),
        "v1_scope_classification": row.get("v1_scope_classification"),
        "readiness_status": readiness_status,
        "production_ready": production_ready,
        "claim_gate_status": row.get("claim_gate_status"),
        "environment": row.get("environment"),
        "data_scale": row.get("data_scale"),
        "evidence_statuses": evidence_statuses,
        "unsupported_diagnostic_count": len(unsupported_rows),
        "unsupported_diagnostics": unsupported_rows,
        "production_blockers": production_blockers,
        "validation_blockers": validation_blockers,
        "status": "passed" if not validation_blockers else "blocked",
    }
    return summary, validation_blockers, production_blockers


def validate_object_store_local_emulator_workload(row: object) -> tuple[dict[str, Any], list[str]]:
    if not isinstance(row, dict):
        return {"present": False, "status": "not_applicable"}, []
    if row.get("workload_id") != OBJECT_STORE_LOCAL_EMULATOR_WORKLOAD_ID:
        return {"present": False, "status": "not_applicable"}, []

    blockers: list[str] = []
    evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
    unsupported_rows_value = row.get("unsupported_diagnostics")
    unsupported_rows = unsupported_rows_value if isinstance(unsupported_rows_value, list) else []
    unsupported_operations = {
        item.get("operation")
        for item in unsupported_rows
        if isinstance(item, dict) and isinstance(item.get("operation"), str)
    }
    effect_permissions_value = row.get("effect_permissions")
    effect_permissions = (
        set(effect_permissions_value)
        if isinstance(effect_permissions_value, list)
        else set()
    )
    missing_effects = sorted(OBJECT_STORE_REQUIRED_EFFECT_PERMISSIONS - effect_permissions)
    missing_unsupported = sorted(
        OBJECT_STORE_REQUIRED_UNSUPPORTED_OPERATIONS - unsupported_operations
    )

    if row.get("v1_scope_classification") != "v1_candidate_pending_feasibility":
        blockers.append("object-store local-emulator workload must remain a v1 feasibility candidate")
    if row.get("production_ready") is not False:
        blockers.append("object-store local-emulator workload must not be production_ready")
    if row.get("claim_gate_status") != "not_claim_grade":
        blockers.append("object-store local-emulator workload must remain not_claim_grade")
    if row.get("production_claim_allowed") is not False:
        blockers.append("object-store local-emulator production_claim_allowed must be false")
    if row.get("performance_claim_allowed") is not False:
        blockers.append("object-store local-emulator performance_claim_allowed must be false")
    if row.get("fallback_attempted") is not False or row.get("external_engine_invoked") is not False:
        blockers.append("object-store local-emulator no-fallback fields must be false")
    if missing_effects:
        blockers.append(
            "object-store local-emulator effect_permissions missing "
            + ",".join(missing_effects)
        )
    if missing_unsupported:
        blockers.append(
            "object-store local-emulator unsupported_diagnostics missing "
            + ",".join(missing_unsupported)
        )

    expected_evidence_prefixes = {
        "runtime_execution": "passed_scoped_local_emulator",
        "correctness": "passed_scoped_local_emulator",
        "native_io_certificate": "passed_fixture_smoke_only",
        "execution_certificate": "passed_fixture_smoke_only",
        "fault_tolerance": "passed_scoped_local_recovery_smoke",
        "security_governance": "passed_scoped_local_emulator",
        "release_api_stability": "passed_scoped_local_emulator",
        "unsupported_diagnostics": "passed_scoped_local_emulator",
    }
    for key, expected in expected_evidence_prefixes.items():
        evidence_row = evidence.get(key) if isinstance(evidence, dict) else None
        status = evidence_row.get("status") if isinstance(evidence_row, dict) else None
        if status != expected:
            blockers.append(
                f"object-store local-emulator evidence.{key}.status must be {expected}"
            )
    security_evidence = (
        evidence.get("security_governance") if isinstance(evidence, dict) else None
    )
    security_refs_value = (
        security_evidence.get("evidence_refs")
        if isinstance(security_evidence, dict)
        else None
    )
    security_refs = (
        set(security_refs_value) if isinstance(security_refs_value, list) else set()
    )
    missing_security_refs = sorted(
        OBJECT_STORE_REQUIRED_SECURITY_EVIDENCE_REFS - security_refs
    )
    if missing_security_refs:
        blockers.append(
            "object-store local-emulator security_governance.evidence_refs missing "
            + ",".join(missing_security_refs)
        )
    for key in ("memory_backpressure", "benchmark"):
        evidence_row = evidence.get(key) if isinstance(evidence, dict) else None
        status = evidence_row.get("status") if isinstance(evidence_row, dict) else None
        if not isinstance(status, str) or not status.startswith("blocked_"):
            blockers.append(
                f"object-store local-emulator evidence.{key}.status must remain blocked"
            )

    summary = {
        "present": True,
        "status": "passed" if not blockers else "blocked",
        "workload_id": OBJECT_STORE_LOCAL_EMULATOR_WORKLOAD_ID,
        "scope": row.get("environment"),
        "claim_gate_status": row.get("claim_gate_status"),
        "production_claim_allowed": row.get("production_claim_allowed"),
        "performance_claim_allowed": row.get("performance_claim_allowed"),
        "effect_permissions_checked": sorted(OBJECT_STORE_REQUIRED_EFFECT_PERMISSIONS),
        "unsupported_operations_checked": sorted(OBJECT_STORE_REQUIRED_UNSUPPORTED_OPERATIONS),
        "blockers": blockers,
    }
    return summary, blockers


def validate_local_table_manifest_workload(row: object) -> tuple[dict[str, Any], list[str]]:
    if not isinstance(row, dict):
        return {"present": False, "status": "not_applicable"}, []
    if row.get("workload_id") != LOCAL_TABLE_MANIFEST_WORKLOAD_ID:
        return {"present": False, "status": "not_applicable"}, []

    blockers: list[str] = []
    evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
    unsupported_rows_value = row.get("unsupported_diagnostics")
    unsupported_rows = unsupported_rows_value if isinstance(unsupported_rows_value, list) else []
    unsupported_operations = {
        item.get("operation")
        for item in unsupported_rows
        if isinstance(item, dict) and isinstance(item.get("operation"), str)
    }
    effect_permissions_value = row.get("effect_permissions")
    effect_permissions = (
        set(effect_permissions_value)
        if isinstance(effect_permissions_value, list)
        else set()
    )
    missing_effects = sorted(LOCAL_TABLE_REQUIRED_EFFECT_PERMISSIONS - effect_permissions)
    missing_unsupported = sorted(
        LOCAL_TABLE_REQUIRED_UNSUPPORTED_OPERATIONS - unsupported_operations
    )

    if row.get("v1_scope_classification") != "v1_candidate_pending_feasibility":
        blockers.append("local table manifest workload must remain a v1 feasibility candidate")
    if row.get("production_ready") is not False:
        blockers.append("local table manifest workload must not be production_ready")
    if row.get("claim_gate_status") != "not_claim_grade":
        blockers.append("local table manifest workload must remain not_claim_grade")
    if row.get("production_claim_allowed") is not False:
        blockers.append("local table manifest production_claim_allowed must be false")
    if row.get("performance_claim_allowed") is not False:
        blockers.append("local table manifest performance_claim_allowed must be false")
    if row.get("fallback_attempted") is not False or row.get("external_engine_invoked") is not False:
        blockers.append("local table manifest no-fallback fields must be false")
    if missing_effects:
        blockers.append(
            "local table manifest effect_permissions missing " + ",".join(missing_effects)
        )
    if missing_unsupported:
        blockers.append(
            "local table manifest unsupported_diagnostics missing "
            + ",".join(missing_unsupported)
        )

    expected_evidence_statuses = {
        "runtime_execution": "passed_scoped_local_manifest",
        "correctness": "passed_scoped_local_manifest",
        "native_io_certificate": "passed_fixture_smoke_only",
        "execution_certificate": "passed_fixture_smoke_only",
        "fault_tolerance": "passed_scoped_local_recovery_smoke",
        "security_governance": "passed_scoped_local_manifest",
        "release_api_stability": "passed_scoped_local_manifest",
        "unsupported_diagnostics": "passed_scoped_local_manifest",
    }
    for key, expected in expected_evidence_statuses.items():
        evidence_row = evidence.get(key) if isinstance(evidence, dict) else None
        status = evidence_row.get("status") if isinstance(evidence_row, dict) else None
        if status != expected:
            blockers.append(f"local table manifest evidence.{key}.status must be {expected}")

    security_evidence = (
        evidence.get("security_governance") if isinstance(evidence, dict) else None
    )
    security_refs_value = (
        security_evidence.get("evidence_refs")
        if isinstance(security_evidence, dict)
        else None
    )
    security_refs = (
        set(security_refs_value) if isinstance(security_refs_value, list) else set()
    )
    missing_security_refs = sorted(
        LOCAL_TABLE_REQUIRED_SECURITY_EVIDENCE_REFS - security_refs
    )
    if missing_security_refs:
        blockers.append(
            "local table manifest security_governance.evidence_refs missing "
            + ",".join(missing_security_refs)
        )

    for key in ("memory_backpressure", "benchmark"):
        evidence_row = evidence.get(key) if isinstance(evidence, dict) else None
        status = evidence_row.get("status") if isinstance(evidence_row, dict) else None
        if not isinstance(status, str) or not status.startswith("blocked_"):
            blockers.append(f"local table manifest evidence.{key}.status must remain blocked")

    summary = {
        "present": True,
        "status": "passed" if not blockers else "blocked",
        "workload_id": LOCAL_TABLE_MANIFEST_WORKLOAD_ID,
        "scope": row.get("environment"),
        "claim_gate_status": row.get("claim_gate_status"),
        "production_claim_allowed": row.get("production_claim_allowed"),
        "performance_claim_allowed": row.get("performance_claim_allowed"),
        "effect_permissions_checked": sorted(LOCAL_TABLE_REQUIRED_EFFECT_PERMISSIONS),
        "unsupported_operations_checked": sorted(LOCAL_TABLE_REQUIRED_UNSUPPORTED_OPERATIONS),
        "blockers": blockers,
    }
    return summary, blockers


def build_report(
    repo_root: Path,
    *,
    matrix: Path = DEFAULT_MATRIX,
    require_production_ready_workload: bool = False,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    matrix_path = resolve_path(repo_root, matrix)
    matrix_payload = load_json(matrix_path, missing_ok=True)
    validation_blockers: list[str] = []
    production_evidence_blockers: list[str] = []

    if matrix_payload is None:
        validation_blockers.append(f"missing production certification matrix: {matrix.as_posix()}")
        workload_rows: list[dict[str, Any]] = []
    else:
        if matrix_payload.get("schema_version") != SCHEMA_VERSION:
            validation_blockers.append(
                "matrix schema_version="
                + str(matrix_payload.get("schema_version", "missing"))
            )
        if matrix_payload.get("claim_gate_status") != "not_claim_grade":
            validation_blockers.append("matrix claim_gate_status must be not_claim_grade")
        validation_blockers.extend(
            require_false_fields(matrix_payload, "matrix", FALSE_SAFETY_FIELDS)
        )
        if matrix_payload.get("required_evidence_keys") != list(REQUIRED_EVIDENCE_KEYS):
            validation_blockers.append("matrix required_evidence_keys drifted")
        if matrix_payload.get("technique_review_keys") != list(TECHNIQUE_REVIEW_KEYS):
            validation_blockers.append("matrix technique_review_keys drifted")
        workloads = matrix_payload.get("workloads")
        if not isinstance(workloads, list) or not workloads:
            validation_blockers.append("matrix workloads must be a non-empty list")
            workloads = []
        workload_rows = []
        for workload in workloads:
            row, row_blockers, row_production_blockers = validate_workload(workload)
            object_store_summary, object_store_blockers = (
                validate_object_store_local_emulator_workload(workload)
            )
            if object_store_summary.get("present"):
                row["object_store_local_emulator_profile"] = object_store_summary
            row_blockers.extend(object_store_blockers)
            if object_store_blockers:
                row["validation_blockers"].extend(object_store_blockers)
                row["status"] = "blocked"
            local_table_summary, local_table_blockers = (
                validate_local_table_manifest_workload(workload)
            )
            if local_table_summary.get("present"):
                row["local_table_manifest_profile"] = local_table_summary
            row_blockers.extend(local_table_blockers)
            if local_table_blockers:
                row["validation_blockers"].extend(local_table_blockers)
                row["status"] = "blocked"
            workload_rows.append(row)
            validation_blockers.extend(row_blockers)
            production_evidence_blockers.extend(
                f"{row.get('workload_id', 'unknown')}: {blocker}"
                for blocker in row_production_blockers
            )

    claim_surface_report, claim_surface_blockers = validate_claim_surfaces(repo_root)
    report_evidence, report_evidence_blockers = validate_required_reports(repo_root)
    validation_blockers.extend(claim_surface_blockers)
    validation_blockers.extend(report_evidence_blockers)

    production_ready_workload_count = sum(
        1 for row in workload_rows if row.get("production_ready") is True
    )
    blocked_workload_count = len(workload_rows) - production_ready_workload_count
    if require_production_ready_workload and production_ready_workload_count == 0:
        validation_blockers.append("strict production mode requires a production-ready workload")

    status = "passed" if not validation_blockers else "blocked"
    certification_status = (
        "production_ready"
        if production_ready_workload_count > 0 and not production_evidence_blockers
        else "blocked_not_production_ready"
    )
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "production_certification_status": certification_status,
        "require_production_ready_workload": require_production_ready_workload,
        "matrix_ref": matrix.as_posix(),
        "matrix_schema_version": matrix_payload.get("schema_version") if matrix_payload else None,
        "workload_count": len(workload_rows),
        "production_ready_workload_count": production_ready_workload_count,
        "blocked_workload_count": blocked_workload_count,
        "workloads": workload_rows,
        "claim_surfaces": claim_surface_report,
        "required_report_evidence": report_evidence,
        "validation_blockers": validation_blockers,
        "production_evidence_blockers": production_evidence_blockers,
        "blockers": validation_blockers,
        "claim_gate_status": "claim_grade"
        if certification_status == "production_ready"
        else "not_claim_grade",
        "production_claim_allowed": certification_status == "production_ready",
        "performance_claim_allowed": False,
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "package_upload_attempted": False,
        "package_channel_submission_attempted": False,
        "oci_push_attempted": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "fallback_engine_dependency_added": False,
        "external_engine_runtime_dependency_added": False,
    }
    if certification_status != "production_ready":
        report.update(
            {
                "production_claim_allowed": False,
                "spark_replacement_claim_allowed": False,
                "object_store_production_claim_allowed": False,
                "lakehouse_production_claim_allowed": False,
                "foundry_production_claim_allowed": False,
                "distributed_production_claim_allowed": False,
                "live_hybrid_production_claim_allowed": False,
            }
        )
    report.update(
        {
            key: value
            for key, value in fail_closed_fields().items()
            if key
            not in {
                "claim_gate_status",
                "production_claim_allowed",
                "performance_claim_allowed",
                "public_release_claim_allowed",
                "public_package_claim_allowed",
            }
        }
    )
    return report


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    report = build_report(
        repo_root,
        matrix=args.matrix,
        require_production_ready_workload=args.require_production_ready_workload,
    )
    output = resolve_path(repo_root, args.output)
    write_json(output, report)
    print(f"{report['status']}: {output}")
    if report["blockers"]:
        for blocker in report["blockers"][:50]:
            print(f"- {blocker}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
