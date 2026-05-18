#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate ShardLoom package-channel readiness evidence.

This validator accepts the current blocked/report-only matrix as valid. It fails only when the
matrix is missing, malformed, overclaims readiness, or allows publication/fallback behavior without
the required evidence.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.package_channel_readiness_matrix.v1"
REPORT_SCHEMA_VERSION = "shardloom.package_channel_readiness_report.v1"

EXPECTED_CHANNEL_IDS = [
    "github_prerelease",
    "testpypi",
    "pypi",
    "homebrew_tap",
    "scoop",
    "winget",
    "conda_forge",
    "ghcr_container",
    "crates_io_future",
]

FORBIDDEN_TRUE_FIELDS = [
    "publication_attempted",
    "tag_created",
    "secrets_required",
    "runtime_fallback_dependency_allowed",
    "external_engine_runtime_dependency_allowed",
    "internal_crates_publish_allowed",
]

TOP_LEVEL_FALSE_FIELDS = [
    "public_package_release_claim_allowed",
    "publication_attempted",
    "tag_created",
    "secrets_required",
    "oci_push_attempted",
    "package_channel_submission_attempted",
    "fallback_engine_dependency_added",
    "external_engine_runtime_dependency_added",
    "package_access_implies_production_readiness",
]

READY_PROOF_FIELDS = [
    "clean_install_proof_status",
    "smoke_check_status",
    "sbom_checksum_provenance_status",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--matrix",
        type=Path,
        default=Path("docs/release/package-channel-readiness-matrix.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/package-channel-readiness-report.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _non_empty_string(row: dict[str, Any], field: str) -> bool:
    return isinstance(row.get(field), str) and bool(row[field].strip())


def validate_matrix(matrix: dict[str, Any] | None) -> list[str]:
    blockers: list[str] = []
    if matrix is None:
        return ["missing package-channel readiness matrix"]

    if matrix.get("schema_version") != SCHEMA_VERSION:
        blockers.append(f"schema_version={matrix.get('schema_version')}")
    if matrix.get("status") not in {"blocked", "ready"}:
        blockers.append(f"status={matrix.get('status')}")
    if matrix.get("claim_gate_status") != "not_claim_grade":
        blockers.append(f"claim_gate_status={matrix.get('claim_gate_status')}")
    for field in TOP_LEVEL_FALSE_FIELDS:
        if matrix.get(field) is not False:
            blockers.append(f"{field} must be false")
    if matrix.get("channel_count") != len(EXPECTED_CHANNEL_IDS):
        blockers.append(f"channel_count={matrix.get('channel_count')}")
    if matrix.get("required_channel_ids") != EXPECTED_CHANNEL_IDS:
        blockers.append("required_channel_ids must match the expected release-channel list")
    for field in ["claim_boundary", "fallback_boundary"]:
        if not _non_empty_string(matrix, field):
            blockers.append(f"missing top-level {field}")

    channels = matrix.get("channels")
    if not isinstance(channels, list):
        return blockers + ["channels must be a list"]
    seen_ids = [row.get("channel_id") for row in channels if isinstance(row, dict)]
    if seen_ids != EXPECTED_CHANNEL_IDS:
        blockers.append(f"channel order/ids mismatch: {seen_ids}")

    for row in channels:
        if not isinstance(row, dict):
            blockers.append("channel rows must be objects")
            continue
        channel_id = str(row.get("channel_id", "<missing>"))
        prefix = f"{channel_id}: "
        for field in [
            "display_name",
            "target_artifact",
            "status",
            "install_command",
            "uninstall_command",
            "clean_install_proof_status",
            "smoke_check_status",
            "sbom_checksum_provenance_status",
            "rollback_yank_policy",
            "auth_provenance_requirement",
            "trusted_publisher_status",
            "claim_boundary",
        ]:
            if not _non_empty_string(row, field):
                blockers.append(prefix + f"missing {field}")
        for field in [
            "ready",
            "trusted_publisher_required",
            "human_approval_required",
            *FORBIDDEN_TRUE_FIELDS,
        ]:
            if not isinstance(row.get(field), bool):
                blockers.append(prefix + f"{field} must be boolean")
        for field in FORBIDDEN_TRUE_FIELDS:
            if row.get(field) is not False:
                blockers.append(prefix + f"{field} must be false")
        if row.get("human_approval_required") is not True:
            blockers.append(prefix + "human_approval_required must be true")
        if not isinstance(row.get("current_blockers"), list) or not row.get("current_blockers"):
            blockers.append(prefix + "current_blockers must be a non-empty list until ready")

        is_ready = row.get("ready") is True
        if is_ready:
            if row.get("status") != "ready":
                blockers.append(prefix + "ready=true requires status=ready")
            for field in READY_PROOF_FIELDS:
                if row.get(field) != "passed":
                    blockers.append(prefix + f"ready=true requires {field}=passed")
            if row.get("current_blockers"):
                blockers.append(prefix + "ready=true requires no current_blockers")
        elif row.get("status") == "ready":
            blockers.append(prefix + "status=ready requires ready=true")

        if channel_id in {"testpypi", "pypi"}:
            requirement = row.get("auth_provenance_requirement", "")
            if row.get("trusted_publisher_required") is not True:
                blockers.append(prefix + "trusted_publisher_required must be true")
            if "Trusted Publisher" not in requirement or "OIDC" not in requirement:
                blockers.append(prefix + "auth_provenance_requirement must mention Trusted Publisher/OIDC")
            if row.get("trusted_publisher_status") not in {"not_configured", "configured", "passed"}:
                blockers.append(prefix + "trusted_publisher_status is invalid")

        if channel_id == "crates_io_future":
            claim_boundary = row.get("claim_boundary", "")
            requirement = row.get("auth_provenance_requirement", "")
            if "future stable public" not in claim_boundary:
                blockers.append(prefix + "claim boundary must limit crates.io to future stable public crates")
            if "no internal crate publication" not in requirement:
                blockers.append(prefix + "auth requirement must forbid internal crate publication")

    return blockers


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    matrix_path = resolve(repo_root, args.matrix)
    output_path = resolve(repo_root, args.output)
    matrix = load_json(matrix_path)
    blockers = validate_matrix(matrix)
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "matrix_ref": str(args.matrix).replace("\\", "/"),
        "status": "passed" if not blockers else "failed",
        "claim_gate_status": (matrix or {}).get("claim_gate_status", "missing"),
        "public_package_release_claim_allowed": (matrix or {}).get(
            "public_package_release_claim_allowed", False
        ),
        "ready_channel_count": sum(
            1
            for row in (matrix or {}).get("channels", [])
            if isinstance(row, dict) and row.get("ready") is True
        ),
        "expected_channel_count": len(EXPECTED_CHANNEL_IDS),
        "blockers": blockers,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output_path)
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
