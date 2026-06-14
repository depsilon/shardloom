#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate representative local-format edge-case fixture traceability."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from release_report_utils import fail_closed_fields, load_json, write_json


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.local_format_edge_case_fixture_validation.v1"
REPORT_SCHEMA_VERSION = "shardloom.local_format_edge_case_fixture_matrix.v1"
CORRECTNESS_SCHEMA_VERSION = "shardloom.v1_correctness_conformance_matrix.v1"
DEFAULT_REPORT = Path("docs/release/local-format-edge-case-fixtures.json")
DEFAULT_CORRECTNESS_MATRIX = Path("docs/release/v1-correctness-conformance-matrix.json")

REQUIRED_ROW_IDS = (
    "vortex_native_prepared_edge_replay",
    "csv_dirty_timestamp_and_numeric_cast",
    "csv_null_heavy_aggregate",
    "jsonl_nested_payload_scan",
    "parquet_arrow_ipc_columnar_roundtrip",
    "avro_orc_compatibility_roundtrip",
    "compatibility_output_metadata_loss_policy",
)

REQUIRED_PROFILE_REFS = (
    "vortex_native_input_output",
    "csv_jsonl_text_source",
    "parquet_arrow_ipc_columnar_source",
    "avro_orc_compatibility_source",
    "compatibility_output_exports",
)

REQUIRED_EDGE_FAMILIES = (
    "malformed_rows",
    "null_coercion",
    "nested_json_payload",
    "columnar_projection",
    "metadata_loss",
)

REQUIRED_ROW_FIELDS = (
    "profile_ref",
    "formats",
    "edge_case_families",
    "fixture_status",
    "fixture_refs",
    "correctness_case_refs",
    "property_or_fuzz_case_refs",
    "expected_diagnostic_or_policy",
    "coverage_boundary",
    "claim_gate_status",
    "fallback_attempted",
    "external_engine_invoked",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument(
        "--correctness-matrix",
        type=Path,
        default=DEFAULT_CORRECTNESS_MATRIX,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/local-format-edge-case-fixture-validation.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _fixture_path(ref: str) -> str:
    return ref.split("::", maxsplit=1)[0]


def _correctness_case_ids(correctness_matrix: dict[str, Any]) -> set[str]:
    case_ids: set[str] = set()
    for field in (
        "required_semantic_case_ids",
        "required_unsupported_case_ids",
        "front_door_example_scenario_ids",
        "front_door_expected_error_scenario_ids",
        "golden_workflow_ids",
    ):
        case_ids.update(str(value) for value in _as_list(correctness_matrix.get(field)))
    return case_ids


def validate_row(
    repo_root: Path,
    row: dict[str, Any],
    correctness_case_ids: set[str],
) -> list[str]:
    blockers: list[str] = []
    row_id = str(row.get("row_id", "unknown"))
    for field in REQUIRED_ROW_FIELDS:
        if field not in row:
            blockers.append(f"{row_id}: missing field {field}")

    formats = _as_list(row.get("formats"))
    edge_case_families = _as_list(row.get("edge_case_families"))
    fixture_refs = _as_list(row.get("fixture_refs"))
    correctness_refs = _as_list(row.get("correctness_case_refs"))
    property_or_fuzz_refs = _as_list(row.get("property_or_fuzz_case_refs"))

    if not formats:
        blockers.append(f"{row_id}: formats must be non-empty")
    if not edge_case_families:
        blockers.append(f"{row_id}: edge_case_families must be non-empty")
    if len(fixture_refs) < 2:
        blockers.append(f"{row_id}: fixture_refs must include at least two refs")
    if not correctness_refs:
        blockers.append(f"{row_id}: correctness_case_refs must be non-empty")
    if len(property_or_fuzz_refs) < 2:
        blockers.append(f"{row_id}: property_or_fuzz_case_refs must include at least two refs")

    for ref in fixture_refs:
        if not isinstance(ref, str):
            blockers.append(f"{row_id}: fixture ref must be a string")
            continue
        path = resolve(repo_root, Path(_fixture_path(ref)))
        if not path.exists():
            blockers.append(f"{row_id}: fixture ref path missing {path}")

    unknown_correctness_refs = [
        str(ref) for ref in correctness_refs if str(ref) not in correctness_case_ids
    ]
    unknown_property_or_fuzz_refs = [
        str(ref) for ref in property_or_fuzz_refs if str(ref) not in correctness_case_ids
    ]
    for ref in unknown_correctness_refs:
        blockers.append(f"{row_id}: unknown correctness_case_ref {ref}")
    for ref in unknown_property_or_fuzz_refs:
        blockers.append(f"{row_id}: unknown property_or_fuzz_case_ref {ref}")

    if row.get("profile_ref") not in REQUIRED_PROFILE_REFS:
        blockers.append(f"{row_id}: profile_ref must be a declared local-format profile")
    if row.get("fixture_status") != "covered_by_existing_focused_tests":
        blockers.append(f"{row_id}: fixture_status must be covered_by_existing_focused_tests")
    if not _non_empty_string(row.get("expected_diagnostic_or_policy")):
        blockers.append(f"{row_id}: expected_diagnostic_or_policy must be non-empty")
    if not _non_empty_string(row.get("coverage_boundary")):
        blockers.append(f"{row_id}: coverage_boundary must be non-empty")
    if row.get("claim_gate_status") != "not_claim_grade":
        blockers.append(f"{row_id}: claim_gate_status must be not_claim_grade")
    if row.get("fallback_attempted") is not False:
        blockers.append(f"{row_id}: fallback_attempted must be false")
    if row.get("external_engine_invoked") is not False:
        blockers.append(f"{row_id}: external_engine_invoked must be false")
    return blockers


def build_report(
    repo_root: Path,
    report_path: Path = DEFAULT_REPORT,
    correctness_matrix_path: Path = DEFAULT_CORRECTNESS_MATRIX,
) -> dict[str, Any]:
    report_ref = report_path.as_posix()
    correctness_ref = correctness_matrix_path.as_posix()
    payload = load_json(resolve(repo_root, report_path), missing_ok=True)
    correctness_matrix = load_json(
        resolve(repo_root, correctness_matrix_path),
        missing_ok=True,
    )
    blockers: list[str] = []
    if not isinstance(payload, dict):
        blockers.append(f"{report_ref}: missing or invalid JSON object")
        payload = {}
    if not isinstance(correctness_matrix, dict):
        blockers.append(f"{correctness_ref}: missing or invalid JSON object")
        correctness_matrix = {}
    if payload.get("schema_version") != REPORT_SCHEMA_VERSION:
        blockers.append(
            f"{report_ref}: schema_version={payload.get('schema_version', 'missing')}"
        )
    if correctness_matrix.get("schema_version") != CORRECTNESS_SCHEMA_VERSION:
        blockers.append(
            f"{correctness_ref}: schema_version={correctness_matrix.get('schema_version', 'missing')}"
        )
    for field in (
        "fallback_attempted",
        "external_engine_invoked",
        "production_claim_allowed",
        "performance_claim_allowed",
    ):
        if payload.get(field) is not False:
            blockers.append(f"{report_ref}: {field} must be false")
    if payload.get("claim_gate_status") != "not_claim_grade":
        blockers.append(f"{report_ref}: claim_gate_status must be not_claim_grade")

    rows = _as_list(payload.get("rows"))
    by_id = {
        str(row.get("row_id")): row
        for row in rows
        if isinstance(row, dict) and row.get("row_id")
    }
    missing_row_ids = [row_id for row_id in REQUIRED_ROW_IDS if row_id not in by_id]
    for row_id in missing_row_ids:
        blockers.append(f"{report_ref}: missing row {row_id}")
    present_profile_refs = {
        str(row.get("profile_ref")) for row in rows if isinstance(row, dict)
    }
    missing_profile_refs = [
        profile_ref
        for profile_ref in REQUIRED_PROFILE_REFS
        if profile_ref not in present_profile_refs
    ]
    for profile_ref in missing_profile_refs:
        blockers.append(f"{report_ref}: missing profile_ref {profile_ref}")
    present_edge_families = {
        str(edge_family)
        for row in rows
        if isinstance(row, dict)
        for edge_family in _as_list(row.get("edge_case_families"))
    }
    missing_edge_families = [
        edge_family
        for edge_family in REQUIRED_EDGE_FAMILIES
        if edge_family not in present_edge_families
    ]
    for edge_family in missing_edge_families:
        blockers.append(f"{report_ref}: missing edge_case_family {edge_family}")

    known_correctness_case_ids = _correctness_case_ids(correctness_matrix)
    for row in rows:
        if isinstance(row, dict):
            blockers.extend(validate_row(repo_root, row, known_correctness_case_ids))
        else:
            blockers.append(f"{report_ref}: row must be an object")

    return {
        "schema_version": SCHEMA_VERSION,
        "report_schema_version": REPORT_SCHEMA_VERSION,
        "correctness_schema_version": CORRECTNESS_SCHEMA_VERSION,
        "report_ref": report_ref,
        "correctness_matrix_ref": correctness_ref,
        "status": "passed" if not blockers else "failed",
        "required_row_count": len(REQUIRED_ROW_IDS),
        "row_count": len(rows),
        "covered_row_count": len(REQUIRED_ROW_IDS) - len(missing_row_ids),
        "missing_row_ids": missing_row_ids,
        "missing_profile_refs": missing_profile_refs,
        "missing_edge_families": missing_edge_families,
        "claim_gate_status": "not_claim_grade",
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(repo_root, args.report, args.correctness_matrix)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
