#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate ShardLoom v1 API/schema stability contracts and golden fixtures.

The validator is local and side-effect-free. It reads checked-in JSON contracts and fixtures,
does not import jsonschema, does not execute ShardLoom runtime paths, and does not authorize
package publication or fallback execution.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_api_schema_stability_report.v1"
MATRIX_SCHEMA_VERSION = "shardloom.v1_api_schema_stability_matrix.v1"
CONTRACT_SCHEMA_VERSION = "shardloom.stable_schema_contract.v1"
FIXTURE_SCHEMA_VERSION = "shardloom.v1_api_schema_stability_fixtures.v1"
REQUIRED_SURFACES = (
    "output_envelope",
    "diagnostic",
    "fallback_status",
    "route_fields",
    "evidence_summary",
    "claim_summary",
    "execution_certificate",
    "native_io_certificate",
    "capability_report",
    "package_release_report",
    "support_bundle",
)
DIAGNOSTIC_CODE_DOC = Path("docs/release/diagnostic-code-stability.md")
DIAGNOSTIC_SOURCE = Path("shardloom-core/src/diagnostics.rs")
TYPE_CHECKS = {
    "array": list,
    "boolean": bool,
    "object": dict,
    "string": str,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--matrix",
        type=Path,
        default=Path("docs/release/v1-api-schema-stability-matrix.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-api-schema-stability-report.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: str | Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else repo_root / path


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def display_path(repo_root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def value_at_path(payload: dict[str, Any], path: str) -> tuple[bool, Any]:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    return True, current


def type_matches(value: Any, expected: str) -> bool:
    if expected == "nullable_string":
        return value is None or isinstance(value, str)
    expected_type = TYPE_CHECKS.get(expected)
    return expected_type is not None and isinstance(value, expected_type)


def validate_contract_fixture(
    surface_id: str,
    contract: dict[str, Any],
    fixture: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if contract.get("schema_version") != CONTRACT_SCHEMA_VERSION:
        blockers.append(f"{surface_id}: schema_version mismatch")
    if contract.get("surface_id") != surface_id:
        blockers.append(f"{surface_id}: surface_id mismatch")
    if contract.get("stability_tier") != "stable_v1":
        blockers.append(f"{surface_id}: stability_tier is not stable_v1")
    if contract.get("compatibility_window") != "v1_additive_compatibility":
        blockers.append(f"{surface_id}: compatibility_window is not v1_additive_compatibility")
    if not contract.get("legacy_flat_field_policy"):
        blockers.append(f"{surface_id}: missing legacy flat-field policy")
    required_fields = contract.get("required_fields")
    if not isinstance(required_fields, list) or not required_fields:
        blockers.append(f"{surface_id}: missing required_fields")
        required_fields = []
    for field in required_fields:
        if not isinstance(field, dict):
            blockers.append(f"{surface_id}: malformed required field row")
            continue
        field_path = str(field.get("path") or "")
        expected_type = str(field.get("type") or "")
        present, value = value_at_path(fixture, field_path)
        if not present:
            blockers.append(f"{surface_id}: fixture missing required field {field_path}")
            continue
        if not type_matches(value, expected_type):
            blockers.append(
                f"{surface_id}: fixture field {field_path} has {type(value).__name__}, expected {expected_type}"
            )
    no_fallback_fields = contract.get("no_fallback_fields")
    if not isinstance(no_fallback_fields, list) or not no_fallback_fields:
        blockers.append(f"{surface_id}: missing no_fallback_fields")
        no_fallback_fields = []
    for field_path in no_fallback_fields:
        present, value = value_at_path(fixture, str(field_path))
        if not present:
            blockers.append(f"{surface_id}: fixture missing no-fallback field {field_path}")
        elif value is not False:
            blockers.append(f"{surface_id}: no-fallback field {field_path} must be false")
    if not contract.get("claim_boundary"):
        blockers.append(f"{surface_id}: missing claim boundary")
    return blockers


def validate_matrix(repo_root: Path, matrix_path: Path) -> dict[str, Any]:
    matrix = load_json(matrix_path)
    blockers: list[str] = []
    if matrix.get("schema_version") != MATRIX_SCHEMA_VERSION:
        blockers.append("matrix schema_version mismatch")
    if matrix.get("compatibility_window") != "v1_additive_compatibility":
        blockers.append("matrix compatibility_window is not v1_additive_compatibility")
    for field in [
        "publication_approval_required",
        "runtime_execution",
        "fallback_attempted",
        "external_engine_invoked",
        "public_release_claim_allowed",
        "public_package_claim_allowed",
        "package_publication_performed",
        "tag_created",
        "signing_key_used",
    ]:
        expected = field == "publication_approval_required"
        if matrix.get(field) is not expected:
            blockers.append(f"matrix {field} must be {str(expected).lower()}")
    surfaces = matrix.get("surfaces")
    if not isinstance(surfaces, list):
        blockers.append("matrix surfaces must be a list")
        surfaces = []
    surface_ids = [str(row.get("surface_id")) for row in surfaces if isinstance(row, dict)]
    if tuple(surface_ids) != REQUIRED_SURFACES:
        blockers.append("surface order mismatch: " + ",".join(surface_ids))
    fixture_path = resolve(repo_root, matrix.get("fixture_path", ""))
    fixtures = load_json(fixture_path) if fixture_path.exists() else {}
    if fixtures.get("schema_version") != FIXTURE_SCHEMA_VERSION:
        blockers.append("fixture schema_version mismatch")
    fixture_map = fixtures.get("fixtures")
    if not isinstance(fixture_map, dict):
        blockers.append("fixture file missing fixtures object")
        fixture_map = {}
    validated_surfaces: list[str] = []
    for row in surfaces:
        if not isinstance(row, dict):
            blockers.append("malformed surface row")
            continue
        surface_id = str(row.get("surface_id") or "")
        schema_path = resolve(repo_root, row.get("schema_path", ""))
        fixture_key = str(row.get("fixture_key") or "")
        if not schema_path.exists():
            blockers.append(f"{surface_id}: missing schema file {schema_path}")
            continue
        if fixture_key not in fixture_map:
            blockers.append(f"{surface_id}: missing fixture key {fixture_key}")
            continue
        contract = load_json(schema_path)
        fixture = fixture_map[fixture_key]
        if not isinstance(fixture, dict):
            blockers.append(f"{surface_id}: fixture must be an object")
            continue
        blockers.extend(validate_contract_fixture(surface_id, contract, fixture))
        validated_surfaces.append(surface_id)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "blocked" if blockers else "passed",
        "matrix_schema_version": MATRIX_SCHEMA_VERSION,
        "matrix_ref": display_path(repo_root, matrix_path),
        "fixture_ref": display_path(repo_root, fixture_path),
        "stable_surface_count": len(validated_surfaces),
        "stable_surfaces": validated_surfaces,
        "compatibility_window": matrix.get("compatibility_window"),
        "legacy_flat_field_policy": matrix.get("legacy_flat_field_policy"),
        "publication_approval_required": matrix.get("publication_approval_required"),
        "public_release_claim_allowed": matrix.get("public_release_claim_allowed"),
        "public_package_claim_allowed": matrix.get("public_package_claim_allowed"),
        "package_publication_performed": matrix.get("package_publication_performed"),
        "tag_created": matrix.get("tag_created"),
        "signing_key_used": matrix.get("signing_key_used"),
        "runtime_execution": matrix.get("runtime_execution"),
        "fallback_attempted": matrix.get("fallback_attempted"),
        "external_engine_invoked": matrix.get("external_engine_invoked"),
        "blockers": blockers,
    }


def diagnostic_codes_from_source(source: str) -> list[str]:
    return re.findall(r'Self::[A-Za-z0-9_]+ => "(SL_[A-Z0-9_]+)"', source)


def validate_diagnostic_code_policy(repo_root: Path) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    source_path = repo_root / DIAGNOSTIC_SOURCE
    doc_path = repo_root / DIAGNOSTIC_CODE_DOC
    if not source_path.exists():
        return [], [f"missing diagnostic source: {DIAGNOSTIC_SOURCE}"]
    if not doc_path.exists():
        return [], [f"missing diagnostic-code stability doc: {DIAGNOSTIC_CODE_DOC}"]
    source = source_path.read_text(encoding="utf-8")
    doc = doc_path.read_text(encoding="utf-8")
    codes = diagnostic_codes_from_source(source)
    if len(codes) != len(set(codes)):
        blockers.append("diagnostic code enum contains duplicate stable code strings")
    if not codes:
        blockers.append("no stable diagnostic codes found in diagnostic source")
    for required in [
        "Compatibility window: additive v1",
        "Migration policy:",
        "fallback_attempted=false",
        "breaking-change approval",
    ]:
        if required not in doc:
            blockers.append(f"diagnostic-code stability doc missing {required}")
    for code in codes:
        if code not in doc:
            blockers.append(f"diagnostic-code stability doc missing {code}")
    return codes, blockers


def build_report(repo_root: Path, matrix_path: Path) -> dict[str, Any]:
    report = validate_matrix(repo_root, matrix_path)
    diagnostic_codes, diagnostic_blockers = validate_diagnostic_code_policy(repo_root)
    report["diagnostic_code_doc_ref"] = str(DIAGNOSTIC_CODE_DOC)
    report["diagnostic_code_source_ref"] = str(DIAGNOSTIC_SOURCE)
    report["diagnostic_code_count"] = len(diagnostic_codes)
    report["diagnostic_code_order"] = diagnostic_codes
    report["blockers"].extend(diagnostic_blockers)
    report["status"] = "blocked" if report["blockers"] else "passed"
    return report


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    matrix_path = resolve(repo_root, args.matrix)
    report = build_report(repo_root, matrix_path)
    output = resolve(repo_root, args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
