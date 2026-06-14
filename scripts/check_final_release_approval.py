#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate final release approval and post-release verification contract.

Default mode validates the no-publication contract artifact and writes a report that is safe for
local release-readiness aggregation. Use ``--require-public-release-ready`` only from a future
maintainer-approved release command; that mode fails unless the approval artifact and all
post-release verification rows have passed.
"""

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
SCHEMA_VERSION = "shardloom.final_release_approval_report.v1"
CONTRACT_SCHEMA_VERSION = "shardloom.final_release_approval_post_release_verification.v1"
DEFAULT_CONTRACT = Path("docs/release/final-release-approval-post-release-verification.json")
DEFAULT_OUTPUT = Path("target/final-release-approval-post-release-verification-report.json")

REQUIRED_ROW_IDS = (
    "package_install_uninstall_smoke",
    "first_10_minutes_public_smoke",
    "golden_workflow_public_smoke",
    "no_fallback_runtime_smoke",
    "docs_links_public_smoke",
    "website_support_matrix_public_smoke",
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
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--require-public-release-ready",
        action="store_true",
        help="Fail unless maintainer approval and all post-release verification rows passed.",
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def validate_contract(contract: dict[str, Any] | None) -> tuple[list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    if not isinstance(contract, dict):
        return [], ["missing final release approval contract"]
    if contract.get("schema_version") != CONTRACT_SCHEMA_VERSION:
        blockers.append(
            "contract schema_version="
            + str(contract.get("schema_version", "missing"))
        )
    for field in (
        "status",
        "publication_authorization_state",
        "approval_artifact_ref",
        "claim_boundary",
        "fallback_boundary",
    ):
        if not _non_empty_string(contract.get(field)):
            blockers.append(f"contract missing {field}")
    for field in FALSE_SAFETY_FIELDS:
        if contract.get(field) is not False:
            blockers.append(f"contract {field} must be false")
    if contract.get("public_release_ready") is not False:
        blockers.append("contract public_release_ready must be false until approval evidence exists")
    if contract.get("post_release_verification_ready") is not False:
        blockers.append(
            "contract post_release_verification_ready must be false until public verification passes"
        )

    rows = [row for row in _as_list(contract.get("verification_rows")) if isinstance(row, dict)]
    by_id = {str(row.get("row_id")): row for row in rows if row.get("row_id")}
    for row_id in REQUIRED_ROW_IDS:
        if row_id not in by_id:
            blockers.append(f"missing verification row {row_id}")
    for row in rows:
        row_id = str(row.get("row_id", "unknown"))
        for field in ("verification_status", "required_evidence", "evidence_ref"):
            if not _non_empty_string(row.get(field)):
                blockers.append(f"{row_id}: missing {field}")
        for field in ("fallback_attempted", "external_engine_invoked"):
            if row.get(field) is not False:
                blockers.append(f"{row_id}: {field} must be false")
    return rows, blockers


def public_release_blockers(contract: dict[str, Any] | None, rows: list[dict[str, Any]]) -> list[str]:
    blockers: list[str] = []
    if not isinstance(contract, dict):
        return ["missing final release approval contract"]
    if contract.get("publication_authorization_state") != "approved":
        blockers.append(
            "publication_authorization_state="
            + str(contract.get("publication_authorization_state", "missing"))
        )
    if contract.get("public_release_ready") is not True:
        blockers.append("public_release_ready must be true")
    if contract.get("post_release_verification_ready") is not True:
        blockers.append("post_release_verification_ready must be true")
    if not contract.get("approved_release_tag"):
        blockers.append("approved_release_tag missing")
    if not contract.get("approved_release_commit"):
        blockers.append("approved_release_commit missing")
    if not contract.get("approved_package_channels"):
        blockers.append("approved_package_channels missing")
    for row in rows:
        if row.get("verification_status") != "passed":
            blockers.append(
                f"{row.get('row_id', 'unknown')}: verification_status="
                + str(row.get("verification_status", "missing"))
            )
    return blockers


def build_report(
    repo_root: Path,
    *,
    contract_path: Path = DEFAULT_CONTRACT,
    output_path: Path = DEFAULT_OUTPUT,
    require_public_release_ready: bool = False,
) -> dict[str, Any]:
    contract = load_json(resolve(repo_root, contract_path), missing_ok=True)
    rows, contract_blockers = validate_contract(contract)
    release_blockers = public_release_blockers(contract, rows)
    blockers = list(contract_blockers)
    if require_public_release_ready:
        blockers.extend(release_blockers)
    contract_valid = not contract_blockers
    public_release_ready = contract_valid and not release_blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_schema_version": CONTRACT_SCHEMA_VERSION,
        "contract_ref": contract_path.as_posix(),
        "output_ref": output_path.as_posix(),
        "status": "passed" if not blockers else "failed",
        "contract_validation_status": "passed" if contract_valid else "failed",
        "require_public_release_ready": require_public_release_ready,
        "public_release_ready": public_release_ready,
        "post_release_verification_ready": bool(
            isinstance(contract, dict)
            and contract.get("post_release_verification_ready") is True
        ),
        "publication_authorization_state": (
            contract or {}
        ).get("publication_authorization_state", "missing"),
        "required_row_count": len(REQUIRED_ROW_IDS),
        "verification_row_count": len(rows),
        "verification_rows": rows,
        "public_release_blockers": release_blockers,
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(
        repo_root,
        contract_path=args.contract,
        output_path=args.output,
        require_public_release_ready=args.require_public_release_ready,
    )
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
