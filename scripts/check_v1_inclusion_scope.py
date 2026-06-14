#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate v1 inclusion-scope classification and unsupported-surface firewall rows."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from release_report_utils import fail_closed_fields, read_text, require_markers, write_json


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_inclusion_scope_report.v1"
MATRIX_SCHEMA_VERSION = "shardloom.v1_inclusion_scope_matrix.v1"
PRODUCTION_UNSUPPORTED_DIAGNOSTIC_SCHEMA_VERSION = (
    "shardloom.production_unsupported_diagnostics.v1"
)

PHASE_PLAN = Path("docs/architecture/phased-execution-plan.md")
MATRIX_DOC = Path("docs/release/v1-inclusion-scope-matrix.md")
KNOWN_UNSUPPORTED_PATHS = Path("docs/release/known-unsupported-paths.md")

ALLOWED_CLASSIFICATIONS = {
    "required_for_v1",
    "v1_candidate_pending_feasibility",
    "deferred_out_of_v1",
    "documentation_only",
    "unsupported_boundary",
}

FORBIDDEN_REQUIRED_POSTURES = {
    "report_only",
    "blocked",
    "unsupported",
    "not_claim_grade",
}

TECHNIQUE_TOKENS = (
    "dynamic",
    "capillary",
    "PulseWeave",
    "metadata-first",
    "timing-surface",
    "evidence-tier",
)

MATRIX_MARKERS = (
    MATRIX_SCHEMA_VERSION,
    "v1_inclusion_scope_allowed_classifications=required_for_v1,v1_candidate_pending_feasibility,deferred_out_of_v1,documentation_only,unsupported_boundary",
    "v1_inclusion_scope_required_rows_cannot_be_report_only=true",
    "v1_inclusion_scope_deferred_rows_require_unsupported_diagnostics=true",
    "v1_inclusion_scope_external_engine_fallback_allowed=false",
)

KNOWN_UNSUPPORTED_MARKERS = (
    MATRIX_DOC.as_posix(),
    "v1 candidates pending feasibility are not outside v1 by default",
    "deferred rows require deterministic unsupported diagnostics",
    PRODUCTION_UNSUPPORTED_DIAGNOSTIC_SCHEMA_VERSION,
)

REQUIRED_PRODUCTION_UNSUPPORTED_DIAGNOSTIC_ROWS = (
    "broad_sql_dataframe_runtime",
    "object_store_runtime",
    "lakehouse_table_runtime",
    "foundry_integration_pack",
    "live_hybrid_remote_distributed_runtime",
    "rest_event_remote_api_runtime",
    "arbitrary_extension_effect_runtime",
    "public_package_publication",
    "performance_superiority_replacement_claim",
    "production_readiness_claim",
)

PRODUCTION_UNSUPPORTED_ROW_REQUIRED_MARKERS = (
    "fallback_attempted=false",
    "external_engine_invoked=false",
    "side_effects_performed=false",
    "claim_gate_status=not_claim_grade",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-inclusion-scope-report.json"),
    )
    return parser.parse_args()


def open_phase_items(text: str) -> dict[str, dict[str, str | None]]:
    matches = list(re.finditer(r"^- \[ \] `([^`]+)` ([^\n]+)", text, re.MULTILINE))
    items: dict[str, dict[str, str | None]] = {}
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        block = text[start:end]
        classification = re.search(r"V1 scope classification: `([^`]+)`", block)
        items[match.group(1)] = {
            "title": match.group(2).strip(),
            "classification": classification.group(1) if classification else None,
        }
    return items


def parse_matrix_rows(text: str) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    headers: list[str] | None = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if not cells:
            continue
        if cells[0] == "Phase item":
            headers = cells
            continue
        if cells[0].startswith("---") or headers is None:
            continue
        row_id = cells[0].strip("`")
        if not row_id:
            continue
        padded = cells + [""] * (len(headers) - len(cells))
        rows[row_id] = {headers[i]: padded[i] for i in range(len(headers))}
    return rows


def technique_review_complete(value: str) -> bool:
    return all(token in value for token in TECHNIQUE_TOKENS)


def build_report(repo_root: Path) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    blockers: list[str] = []

    phase_text = read_text(repo_root / PHASE_PLAN)
    matrix_text = read_text(repo_root / MATRIX_DOC)
    unsupported_text = read_text(repo_root / KNOWN_UNSUPPORTED_PATHS)

    blockers.extend(require_markers(MATRIX_DOC.as_posix(), matrix_text, MATRIX_MARKERS))
    blockers.extend(
        require_markers(KNOWN_UNSUPPORTED_PATHS.as_posix(), unsupported_text, KNOWN_UNSUPPORTED_MARKERS)
    )
    missing_production_unsupported_diagnostic_rows = [
        row_id
        for row_id in REQUIRED_PRODUCTION_UNSUPPORTED_DIAGNOSTIC_ROWS
        if f"diagnostic_row_id={row_id}" not in unsupported_text
    ]
    for row_id in missing_production_unsupported_diagnostic_rows:
        blockers.append(f"production unsupported diagnostic row missing: {row_id}")
    for marker in PRODUCTION_UNSUPPORTED_ROW_REQUIRED_MARKERS:
        if marker not in unsupported_text:
            blockers.append(f"production unsupported diagnostic catalog missing {marker}")

    phase_items = open_phase_items(phase_text)
    matrix_rows = parse_matrix_rows(matrix_text)

    for item_id, item in phase_items.items():
        classification = item["classification"]
        if classification is None:
            blockers.append(f"{item_id}: missing V1 scope classification")
            continue
        if classification not in ALLOWED_CLASSIFICATIONS:
            blockers.append(f"{item_id}: unknown V1 scope classification {classification}")
        row = matrix_rows.get(item_id)
        if row is None:
            blockers.append(f"{item_id}: missing v1 inclusion matrix row")
            continue
        row_classification = row.get("Classification", "").strip("`")
        if row_classification != classification:
            blockers.append(
                f"{item_id}: matrix classification mismatch {row_classification} != {classification}"
            )

    for item_id, row in matrix_rows.items():
        classification = row.get("Classification", "").strip("`")
        support_gate_posture = row.get("Support gate posture", "").strip("`")
        feasibility_status = row.get("Feasibility status", "").strip("`")
        unsupported_boundary = row.get("Unsupported boundary", "")
        technique_review = row.get("Technique review", "")

        if classification and classification not in ALLOWED_CLASSIFICATIONS:
            blockers.append(f"{item_id}: unknown matrix classification {classification}")
        if classification == "required_for_v1":
            if support_gate_posture in FORBIDDEN_REQUIRED_POSTURES:
                blockers.append(
                    f"{item_id}: v1-required row has forbidden support gate posture {support_gate_posture}"
                )
            if support_gate_posture in {"", "missing"}:
                blockers.append(f"{item_id}: v1-required row missing support gate posture")
        if classification == "v1_candidate_pending_feasibility":
            if "pending" not in feasibility_status:
                blockers.append(f"{item_id}: v1 candidate missing pending feasibility status")
        if classification in {"deferred_out_of_v1", "unsupported_boundary"}:
            boundary = unsupported_boundary.lower()
            if "diagnostic" not in boundary:
                blockers.append(f"{item_id}: deferred/unsupported row missing diagnostic boundary")
            for marker in ["fallback_attempted=false", "external_engine_invoked=false"]:
                if marker not in unsupported_boundary:
                    blockers.append(f"{item_id}: deferred/unsupported row missing {marker}")
        if classification in {"required_for_v1", "v1_candidate_pending_feasibility"}:
            if not technique_review_complete(technique_review):
                blockers.append(f"{item_id}: technique review missing required ShardLoom tokens")

    classification_counts: dict[str, int] = {}
    for row in matrix_rows.values():
        classification = row.get("Classification", "").strip("`")
        classification_counts[classification] = classification_counts.get(classification, 0) + 1

    missing_phase_classification_count = sum(
        1 for item in phase_items.values() if item["classification"] is None
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "matrix_schema_version": MATRIX_SCHEMA_VERSION,
        "status": "passed" if not blockers else "failed",
        "phase_plan": PHASE_PLAN.as_posix(),
        "matrix_doc": MATRIX_DOC.as_posix(),
        "known_unsupported_paths": KNOWN_UNSUPPORTED_PATHS.as_posix(),
        "open_phase_item_count": len(phase_items),
        "matrix_row_count": len(matrix_rows),
        "production_unsupported_diagnostic_schema_version": (
            PRODUCTION_UNSUPPORTED_DIAGNOSTIC_SCHEMA_VERSION
        ),
        "production_unsupported_diagnostic_required_row_count": len(
            REQUIRED_PRODUCTION_UNSUPPORTED_DIAGNOSTIC_ROWS
        ),
        "production_unsupported_diagnostic_covered_row_count": len(
            REQUIRED_PRODUCTION_UNSUPPORTED_DIAGNOSTIC_ROWS
        )
        - len(missing_production_unsupported_diagnostic_rows),
        "production_unsupported_diagnostic_missing_rows": (
            missing_production_unsupported_diagnostic_rows
        ),
        "missing_phase_classification_count": missing_phase_classification_count,
        "classification_counts": classification_counts,
        "claim_gate_status": "not_claim_grade",
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = args.output if args.output.is_absolute() else repo_root / args.output
    report = build_report(repo_root)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
