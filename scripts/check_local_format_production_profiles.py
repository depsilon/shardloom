#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate local format production-profile declarations."""

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
SCHEMA_VERSION = "shardloom.local_format_production_profiles_report.v1"
MATRIX_SCHEMA_VERSION = "shardloom.local_format_production_profiles.v1"
DEFAULT_MATRIX = Path("docs/release/local-format-production-profiles.json")

REQUIRED_PROFILE_IDS = (
    "vortex_native_input_output",
    "csv_jsonl_text_source",
    "parquet_arrow_ipc_columnar_source",
    "avro_orc_compatibility_source",
    "compatibility_output_exports",
)

REQUIRED_FORMATS = {
    "vortex_native_input_output": {"vortex"},
    "csv_jsonl_text_source": {"csv", "json", "jsonl", "ndjson"},
    "parquet_arrow_ipc_columnar_source": {"parquet", "arrow-ipc"},
    "avro_orc_compatibility_source": {"avro", "orc"},
    "compatibility_output_exports": {
        "jsonl",
        "csv",
        "parquet",
        "arrow-ipc",
        "avro",
        "orc",
    },
}

TECHNIQUE_TOKENS = (
    "dynamic",
    "capillary",
    "PulseWeave",
    "metadata-first",
    "timing-surface",
    "evidence-tier",
)

REQUIRED_PROFILE_FIELDS = (
    "profile_kind",
    "formats",
    "default_build_status",
    "feature_gate",
    "current_runtime_status",
    "production_certification_status",
    "pushdown_contract",
    "fidelity_contract",
    "malformed_policy",
    "parser_reader_contract",
    "nested_complex_dtype_policy",
    "deterministic_blocker_id",
    "vortex_provider_review",
    "technique_review",
    "required_certification_evidence",
    "claim_gate_status",
    "fallback_attempted",
    "external_engine_invoked",
)

REQUIRED_PARSER_READER_CONTRACT_FIELDS = (
    "malformed_rows",
    "encoding",
    "null_coercion",
    "projection_aware_typed_builders",
    "nested_complex_dtype",
    "deterministic_blocker",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/local-format-production-profiles-report.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _technique_review_complete(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return all(token in value for token in TECHNIQUE_TOKENS)


def validate_profile(row: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    profile_id = str(row.get("profile_id", "unknown"))
    for field in REQUIRED_PROFILE_FIELDS:
        if field not in row:
            blockers.append(f"{profile_id}: missing field {field}")
    required_formats = REQUIRED_FORMATS.get(profile_id)
    formats = {str(value) for value in _as_list(row.get("formats"))}
    if required_formats is None:
        blockers.append(f"{profile_id}: unexpected profile id")
    elif not required_formats.issubset(formats):
        blockers.append(
            f"{profile_id}: missing formats {sorted(required_formats - formats)}"
        )
    for field in (
        "pushdown_contract",
        "fidelity_contract",
        "malformed_policy",
        "nested_complex_dtype_policy",
        "deterministic_blocker_id",
        "vortex_provider_review",
    ):
        if not _non_empty_string(row.get(field)):
            blockers.append(f"{profile_id}: {field} must be non-empty")
    parser_reader_contract = row.get("parser_reader_contract")
    if not isinstance(parser_reader_contract, dict):
        blockers.append(f"{profile_id}: parser_reader_contract must be an object")
    else:
        for field in REQUIRED_PARSER_READER_CONTRACT_FIELDS:
            if not _non_empty_string(parser_reader_contract.get(field)):
                blockers.append(
                    f"{profile_id}: parser_reader_contract.{field} must be non-empty"
                )
        if parser_reader_contract.get("deterministic_blocker") != row.get(
            "deterministic_blocker_id"
        ):
            blockers.append(
                f"{profile_id}: parser_reader_contract deterministic blocker mismatch"
            )
    if "vortex" not in str(row.get("vortex_provider_review", "")).lower():
        blockers.append(f"{profile_id}: vortex_provider_review must mention Vortex")
    if not _technique_review_complete(row.get("technique_review")):
        blockers.append(f"{profile_id}: technique_review missing required tokens")
    evidence = _as_list(row.get("required_certification_evidence"))
    if len(evidence) < 4:
        blockers.append(f"{profile_id}: required_certification_evidence is too thin")
    if row.get("claim_gate_status") != "not_claim_grade":
        blockers.append(f"{profile_id}: claim_gate_status must be not_claim_grade")
    if row.get("fallback_attempted") is not False:
        blockers.append(f"{profile_id}: fallback_attempted must be false")
    if row.get("external_engine_invoked") is not False:
        blockers.append(f"{profile_id}: external_engine_invoked must be false")
    if row.get("production_certification_status") != "incomplete":
        blockers.append(f"{profile_id}: production_certification_status must be incomplete")
    return blockers


def build_report(repo_root: Path, matrix_path: Path = DEFAULT_MATRIX) -> dict[str, Any]:
    matrix_ref = matrix_path.as_posix()
    matrix = load_json(resolve(repo_root, matrix_path), missing_ok=True)
    blockers: list[str] = []
    if not isinstance(matrix, dict):
        blockers.append(f"{matrix_ref}: missing or invalid JSON object")
        matrix = {}
    if matrix.get("schema_version") != MATRIX_SCHEMA_VERSION:
        blockers.append(
            f"{matrix_ref}: schema_version={matrix.get('schema_version', 'missing')}"
        )
    for field in (
        "fallback_attempted",
        "external_engine_invoked",
        "production_claim_allowed",
        "performance_claim_allowed",
    ):
        if matrix.get(field) is not False:
            blockers.append(f"{matrix_ref}: {field} must be false")
    if matrix.get("claim_gate_status") != "not_claim_grade":
        blockers.append(f"{matrix_ref}: claim_gate_status must be not_claim_grade")
    profiles = _as_list(matrix.get("profiles"))
    by_id = {
        str(row.get("profile_id")): row
        for row in profiles
        if isinstance(row, dict) and row.get("profile_id")
    }
    missing_profile_ids = [
        profile_id for profile_id in REQUIRED_PROFILE_IDS if profile_id not in by_id
    ]
    for profile_id in missing_profile_ids:
        blockers.append(f"{matrix_ref}: missing profile {profile_id}")
    for row in profiles:
        if isinstance(row, dict):
            blockers.extend(validate_profile(row))
        else:
            blockers.append(f"{matrix_ref}: profile row must be an object")

    return {
        "schema_version": SCHEMA_VERSION,
        "matrix_schema_version": MATRIX_SCHEMA_VERSION,
        "matrix_ref": matrix_ref,
        "status": "passed" if not blockers else "failed",
        "required_profile_count": len(REQUIRED_PROFILE_IDS),
        "profile_count": len(profiles),
        "covered_profile_count": len(REQUIRED_PROFILE_IDS) - len(missing_profile_ids),
        "missing_profile_ids": missing_profile_ids,
        "claim_gate_status": "not_claim_grade",
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(repo_root, args.matrix)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
