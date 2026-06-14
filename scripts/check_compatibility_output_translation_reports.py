#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate compatibility-output TranslationReport coverage declarations."""

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
SCHEMA_VERSION = "shardloom.compatibility_output_translation_report_validation.v1"
REPORT_SCHEMA_VERSION = "shardloom.compatibility_output_translation_report_coverage.v1"
DEFAULT_REPORT = Path("docs/release/compatibility-output-translation-report-coverage.json")

REQUIRED_ROW_IDS = (
    "vortex_native_output",
    "csv_compatibility_output",
    "jsonl_compatibility_output",
    "parquet_compatibility_output",
    "arrow_ipc_compatibility_output",
    "avro_compatibility_output",
    "orc_compatibility_output",
    "iceberg_compatible_table_output",
    "delta_compatible_table_output",
)

REQUIRED_ROW_FIELDS = (
    "target_kind",
    "translation_status",
    "fidelity_level",
    "metadata_preserved",
    "metadata_lost_or_partial",
    "materialization_requirement",
    "unsupported_schema_diagnostic",
    "writer_support_status",
    "current_evidence_refs",
    "required_next_evidence",
    "claim_gate_status",
    "fallback_attempted",
    "external_engine_invoked",
)

COMPATIBILITY_ROW_IDS = tuple(
    row_id for row_id in REQUIRED_ROW_IDS if row_id != "vortex_native_output"
)

BLOCKED_TABLE_ROW_IDS = (
    "iceberg_compatible_table_output",
    "delta_compatible_table_output",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/compatibility-output-translation-report-validation.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def validate_row(row: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    row_id = str(row.get("row_id", "unknown"))
    for field in REQUIRED_ROW_FIELDS:
        if field not in row:
            blockers.append(f"{row_id}: missing field {field}")

    metadata_preserved = _as_list(row.get("metadata_preserved"))
    metadata_lost_or_partial = _as_list(row.get("metadata_lost_or_partial"))
    current_evidence_refs = _as_list(row.get("current_evidence_refs"))
    required_next_evidence = _as_list(row.get("required_next_evidence"))

    if not metadata_preserved:
        blockers.append(f"{row_id}: metadata_preserved must be non-empty")
    if row_id in COMPATIBILITY_ROW_IDS and not metadata_lost_or_partial:
        blockers.append(
            f"{row_id}: compatibility outputs must list metadata_lost_or_partial"
        )
    if row_id == "vortex_native_output" and metadata_lost_or_partial:
        blockers.append(
            "vortex_native_output: native output must not list metadata_lost_or_partial"
        )

    if row_id == "vortex_native_output":
        if row.get("translation_status") != "native_output_planned":
            blockers.append("vortex_native_output: translation_status must be native")
        if row.get("fidelity_level") != "native_full_fidelity":
            blockers.append("vortex_native_output: fidelity_level must be native_full_fidelity")
        if row.get("writer_support_status") != "native_vortex_reference":
            blockers.append(
                "vortex_native_output: writer_support_status must be native_vortex_reference"
            )
    else:
        if row.get("translation_status") != "compatibility_output_planned":
            blockers.append(
                f"{row_id}: translation_status must be compatibility_output_planned"
            )
        if row.get("fidelity_level") != "compatibility_lossy_physical":
            blockers.append(
                f"{row_id}: fidelity_level must be compatibility_lossy_physical"
            )
        if row_id in BLOCKED_TABLE_ROW_IDS:
            if row.get("writer_support_status") != "report_only_blocked":
                blockers.append(f"{row_id}: table targets must be report_only_blocked")
        elif row.get("writer_support_status") != "local_fixture_smoke":
            blockers.append(f"{row_id}: writer_support_status must be local_fixture_smoke")

    if not _non_empty_string(row.get("unsupported_schema_diagnostic")):
        blockers.append(f"{row_id}: unsupported_schema_diagnostic must be non-empty")
    if len(current_evidence_refs) < 2:
        blockers.append(f"{row_id}: current_evidence_refs is too thin")
    if len(required_next_evidence) < 2:
        blockers.append(f"{row_id}: required_next_evidence is too thin")
    if row.get("claim_gate_status") != "not_claim_grade":
        blockers.append(f"{row_id}: claim_gate_status must be not_claim_grade")
    if row.get("fallback_attempted") is not False:
        blockers.append(f"{row_id}: fallback_attempted must be false")
    if row.get("external_engine_invoked") is not False:
        blockers.append(f"{row_id}: external_engine_invoked must be false")
    return blockers


def build_report(repo_root: Path, report_path: Path = DEFAULT_REPORT) -> dict[str, Any]:
    report_ref = report_path.as_posix()
    payload = load_json(resolve(repo_root, report_path), missing_ok=True)
    blockers: list[str] = []
    if not isinstance(payload, dict):
        blockers.append(f"{report_ref}: missing or invalid JSON object")
        payload = {}
    if payload.get("schema_version") != REPORT_SCHEMA_VERSION:
        blockers.append(
            f"{report_ref}: schema_version={payload.get('schema_version', 'missing')}"
        )
    for field in (
        "fallback_attempted",
        "external_engine_invoked",
        "production_claim_allowed",
        "performance_claim_allowed",
        "table_commit_claim_allowed",
        "object_store_output_claim_allowed",
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
    for row in rows:
        if isinstance(row, dict):
            blockers.extend(validate_row(row))
        else:
            blockers.append(f"{report_ref}: row must be an object")

    return {
        "schema_version": SCHEMA_VERSION,
        "report_schema_version": REPORT_SCHEMA_VERSION,
        "report_ref": report_ref,
        "status": "passed" if not blockers else "failed",
        "required_row_count": len(REQUIRED_ROW_IDS),
        "row_count": len(rows),
        "covered_row_count": len(REQUIRED_ROW_IDS) - len(missing_row_ids),
        "missing_row_ids": missing_row_ids,
        "claim_gate_status": "not_claim_grade",
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(repo_root, args.report)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
