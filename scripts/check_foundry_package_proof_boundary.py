#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the GAR-0036-A Foundry package/proof boundary matrix."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.foundry_package_proof_boundary_matrix.v1"
REQUIRED_ROWS = [
    "local_style_transform_fixture",
    "local_certificate_metrics_output",
    "shardloom_foundry_package",
    "artifact_repository_publication",
    "foundry_service_invocation",
    "compute_module_surface",
    "virtual_table_native_execution",
    "dataset_transaction_runtime",
    "f10_workload_certified_deployment",
]
REQUIRED_FALSE_FIELDS = [
    "foundry_runtime_invoked",
    "foundry_compute_invoked",
    "foundry_spark_invoked",
    "fallback_attempted",
    "external_engine_invoked",
]
REQUIRED_DOC_SNIPPETS = [
    "shardloom.foundry_package_proof_boundary_matrix.v1",
    "GAR-0036-A",
    "foundry_runtime_invoked=false",
    "foundry_compute_invoked=false",
    "foundry_spark_invoked=false",
    "fallback_attempted=false",
    "external_engine_invoked=false",
    "no `shardloom-foundry` package claim",
    "no dataset transaction runtime claim",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--matrix",
        type=Path,
        default=Path("docs/foundry/package-proof-boundary-matrix.json"),
    )
    parser.add_argument(
        "--doc",
        type=Path,
        default=Path("docs/foundry/package-proof-boundary-matrix.md"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_matrix(matrix: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if matrix.get("schema_version") != SCHEMA_VERSION:
        blockers.append(f"schema_version={matrix.get('schema_version')}")
    if matrix.get("gar_id") != "GAR-0036-A":
        blockers.append(f"gar_id={matrix.get('gar_id')}")
    if matrix.get("support_status") != "report_only":
        blockers.append(f"support_status={matrix.get('support_status')}")
    if matrix.get("claim_gate_status") != "not_claim_grade":
        blockers.append(f"claim_gate_status={matrix.get('claim_gate_status')}")
    if matrix.get("public_foundry_claim_allowed") is not False:
        blockers.append("public_foundry_claim_allowed must be false")
    for field in REQUIRED_FALSE_FIELDS:
        if matrix.get(field) is not False:
            blockers.append(f"{field} must be false")

    rows = matrix.get("rows")
    if not isinstance(rows, list):
        return blockers + ["rows must be a list"]
    row_ids = [row.get("row_id") for row in rows if isinstance(row, dict)]
    if row_ids != REQUIRED_ROWS:
        blockers.append(f"row_order mismatch: {row_ids}")

    for row in rows:
        if not isinstance(row, dict):
            blockers.append("row entries must be objects")
            continue
        row_id = row.get("row_id", "<missing>")
        for field in REQUIRED_FALSE_FIELDS:
            if row.get(field) is not False:
                blockers.append(f"{row_id}: {field} must be false")
        if row.get("public_foundry_claim_allowed") is not False:
            blockers.append(f"{row_id}: public_foundry_claim_allowed must be false")
        if row_id.startswith("local_"):
            if row.get("local_style_claim_allowed") is not True:
                blockers.append(f"{row_id}: local_style_claim_allowed must be true")
            if row.get("claim_gate_status") != "fixture_smoke_only":
                blockers.append(f"{row_id}: expected fixture_smoke_only")
        else:
            if row.get("local_style_claim_allowed") is not False:
                blockers.append(f"{row_id}: local_style_claim_allowed must be false")
            if row.get("claim_gate_status") != "not_claim_grade":
                blockers.append(f"{row_id}: expected not_claim_grade")
            if row.get("support_status") != "blocked":
                blockers.append(f"{row_id}: expected blocked support status")
    return blockers


def validate_doc(doc_text: str) -> list[str]:
    return [
        f"doc missing {snippet}"
        for snippet in REQUIRED_DOC_SNIPPETS
        if snippet not in doc_text
    ]


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    matrix_path = resolve(repo_root, args.matrix)
    doc_path = resolve(repo_root, args.doc)
    blockers = validate_matrix(load_json(matrix_path))
    blockers.extend(validate_doc(doc_path.read_text(encoding="utf-8")))
    if blockers:
        print(json.dumps({"status": "blocked", "blockers": blockers}, indent=2))
        return 1
    print(
        json.dumps(
            {
                "schema_version": "shardloom.foundry_package_proof_boundary_matrix_report.v1",
                "status": "passed",
                "matrix": str(matrix_path),
                "doc": str(doc_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
