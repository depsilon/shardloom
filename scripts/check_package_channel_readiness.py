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
PYTHON_REGISTRY_PROOF_SCHEMA_VERSION = "shardloom.python_registry_package_proof.v1"

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

EXPECTED_V1_FEASIBILITY_REVIEWED_CHANNEL_IDS = EXPECTED_CHANNEL_IDS
V1_FEASIBILITY_STATUSES = {
    "included_pending_channel_proof",
    "feasible_pending_channel_proof",
    "not_in_v1_scope_recorded",
}

PYPROJECT = Path("python/pyproject.toml")
PACKAGE_NAME_READINESS_DOC = Path("docs/release/package-name-readiness.md")

EXPECTED_PYTHON_PACKAGE_NAME = "shardloom"
EXPECTED_PYTHON_REQUIRES = ">=3.10"
EXPECTED_PUBLIC_CRATE_CANDIDATES = ["shardloom-protocol", "shardloom-client"]
GITHUB_PRERELEASE_BUNDLE_SCHEMA_VERSION = "shardloom.github_prerelease_asset_bundle.v1"
GITHUB_PRERELEASE_REQUIRED_ASSET_KINDS = [
    "source_archive",
    "release_notes",
    "release_binary",
    "python_wheel",
    "python_sdist",
    "checksum_manifest",
    "rust_workspace_sbom",
    "python_artifact_sbom",
    "cli_binary_sbom",
    "supply_chain_provenance",
]
INTERNAL_CRATE_MANIFESTS = [
    "shardloom-core/Cargo.toml",
    "shardloom-plan/Cargo.toml",
    "shardloom-exec/Cargo.toml",
    "shardloom-vortex/Cargo.toml",
    "shardloom-cli/Cargo.toml",
    "shardloom-contract-tests/Cargo.toml",
]
PACKAGE_WORKSPACE_REF_MANIFESTS = ["Cargo.toml", *INTERNAL_CRATE_MANIFESTS]

FORBIDDEN_TRUE_FIELDS = [
    "publication_attempted",
    "tag_created",
    "secrets_required",
    "runtime_fallback_dependency_allowed",
    "external_engine_runtime_dependency_allowed",
    "internal_crates_publish_allowed",
]

TOP_LEVEL_FALSE_FIELDS = [
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

PACKAGE_GATE_REQUIRED_EVIDENCE = [
    "dependency_inventory",
    "license_classification",
    "provenance_status",
    "forbidden_fallback_dependency_check",
    "package_smoke_transcript",
    "sbom_refs",
    "checksum_refs",
    "rollback_policy_ref",
    "publication_authorization_state",
]

GATE_EVIDENCE_REF_FIELDS = [
    "dependency_audit_script",
    "dependency_audit_report",
    "release_dry_run_script",
    "release_dry_run_transcript",
    "release_provenance_script",
    "release_provenance_report",
    "python_registry_package_proof_script",
    "sbom_generation_plan",
    "rollback_policy_ref",
    "package_channel_validator",
]

READY_REFERENCE_FIELDS = [
    "install_transcript_ref",
    "uninstall_transcript_ref",
    "clean_install_transcript_ref",
    "smoke_transcript_ref",
    "sbom_ref",
    "checksum_ref",
    "provenance_ref",
    "authorization_ref",
]

FALSE_SAFETY_FIELDS = [
    "publication_attempted",
    "tag_created",
    "secrets_required",
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
    parser.add_argument(
        "--dependency-audit-report",
        type=Path,
        default=Path("target/dependency-audit-report.json"),
    )
    parser.add_argument(
        "--release-dry-run-transcript",
        type=Path,
        default=Path("target/release-dry-run-proof/transcript.json"),
    )
    parser.add_argument(
        "--provenance-report",
        type=Path,
        default=Path("target/release-provenance-dry-run/supply-chain-release-evidence.json"),
    )
    parser.add_argument(
        "--testpypi-proof",
        type=Path,
        default=Path("target/python-registry-package-proof/testpypi-transcript.json"),
    )
    parser.add_argument(
        "--pypi-proof",
        type=Path,
        default=Path("target/python-registry-package-proof/pypi-transcript.json"),
    )
    parser.add_argument(
        "--require-local-evidence",
        action="store_true",
        help=(
            "Fail when local dependency audit, package smoke, SBOM/checksum, or provenance "
            "reports are missing or incomplete."
        ),
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run synthetic regression checks for package-gate failure cases.",
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


def read_text(path: Path) -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def require_marker(
    blockers: list[str], label: str, text: str | None, marker: str
) -> None:
    if text is None:
        blockers.append(f"{label} missing")
    elif marker not in text:
        blockers.append(f"{label} missing marker {marker!r}")


def find_channel(matrix: dict[str, Any] | None, channel_id: str) -> dict[str, Any] | None:
    if matrix is None:
        return None
    channels = matrix.get("channels", [])
    if not isinstance(channels, list):
        return None
    for row in channels:
        if isinstance(row, dict) and row.get("channel_id") == channel_id:
            return row
    return None


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
    if matrix.get("channel_v1_feasibility_review_status") != "reviewed":
        blockers.append("channel_v1_feasibility_review_status must be reviewed")
    if (
        matrix.get("v1_feasibility_reviewed_channel_ids")
        != EXPECTED_V1_FEASIBILITY_REVIEWED_CHANNEL_IDS
    ):
        blockers.append(
            "v1_feasibility_reviewed_channel_ids must match the expected release-channel list"
        )
    if matrix.get("package_gate_required_evidence") != PACKAGE_GATE_REQUIRED_EVIDENCE:
        blockers.append("package_gate_required_evidence must match the package-gate evidence list")
    if (
        matrix.get("package_identity_contract_status")
        != "local_contract_recorded_publication_approval_blocked"
    ):
        blockers.append(
            "package_identity_contract_status must record local contract with publication approval blocked"
        )
    if matrix.get("python_package_identity") != EXPECTED_PYTHON_PACKAGE_NAME:
        blockers.append(
            f"python_package_identity must be {EXPECTED_PYTHON_PACKAGE_NAME!r}"
        )
    if matrix.get("internal_workspace_crates_publish_allowed") is not False:
        blockers.append("internal_workspace_crates_publish_allowed must be false")
    if matrix.get("future_public_crate_candidates") != EXPECTED_PUBLIC_CRATE_CANDIDATES:
        blockers.append(
            "future_public_crate_candidates must match the approved future public crate names"
        )
    gate_refs = matrix.get("gate_evidence_refs")
    if not isinstance(gate_refs, dict):
        blockers.append("gate_evidence_refs must be an object")
    else:
        for field in GATE_EVIDENCE_REF_FIELDS:
            if not _non_empty_string(gate_refs, field):
                blockers.append(f"gate_evidence_refs missing {field}")
    if matrix.get("publication_authorization_state") not in {
        "human_approval_required",
        "approved",
    }:
        blockers.append(
            f"publication_authorization_state={matrix.get('publication_authorization_state')}"
        )
    for field in ["claim_boundary", "fallback_boundary"]:
        if not _non_empty_string(matrix, field):
            blockers.append(f"missing top-level {field}")

    channels = matrix.get("channels")
    if not isinstance(channels, list):
        return blockers + ["channels must be a list"]
    seen_ids = [row.get("channel_id") for row in channels if isinstance(row, dict)]
    if seen_ids != EXPECTED_CHANNEL_IDS:
        blockers.append(f"channel order/ids mismatch: {seen_ids}")

    ready_rows: list[dict[str, Any]] = []
    in_scope_channel_rows: list[dict[str, Any]] = []
    for row in channels:
        if not isinstance(row, dict):
            blockers.append("channel rows must be objects")
            continue
        channel_id = str(row.get("channel_id", "<missing>"))
        prefix = f"{channel_id}: "
        row_is_in_v1_scope = row.get("v1_feasibility_status") != "not_in_v1_scope_recorded"
        if row_is_in_v1_scope:
            in_scope_channel_rows.append(row)
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
            "v1_feasibility_status",
            "v1_scope_decision",
            "v1_feasibility_reason",
            "claim_boundary",
        ]:
            if not _non_empty_string(row, field):
                blockers.append(prefix + f"missing {field}")
        if row.get("v1_feasibility_status") not in V1_FEASIBILITY_STATUSES:
            blockers.append(prefix + "v1_feasibility_status is invalid")
        if row.get("v1_feasibility_status") == "not_in_v1_scope_recorded":
            if row.get("ready") is True:
                blockers.append(prefix + "not_in_v1_scope rows cannot be ready")
            if "not in v1" not in str(row.get("v1_scope_decision", "")).lower():
                blockers.append(prefix + "v1_scope_decision must record not-in-v1 scope")
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
        is_ready = row.get("ready") is True
        if is_ready:
            ready_rows.append(row)
            if row.get("status") != "ready":
                blockers.append(prefix + "ready=true requires status=ready")
            for field in READY_PROOF_FIELDS:
                if row.get(field) != "passed":
                    blockers.append(prefix + f"ready=true requires {field}=passed")
            for field in READY_REFERENCE_FIELDS:
                if not _non_empty_string(row, field):
                    blockers.append(prefix + f"ready=true requires {field}")
            if row.get("current_blockers"):
                blockers.append(prefix + "ready=true requires no current_blockers")
        elif row.get("status") == "ready":
            blockers.append(prefix + "status=ready requires ready=true")
        elif not isinstance(row.get("current_blockers"), list) or not row.get("current_blockers"):
            blockers.append(prefix + "current_blockers must be a non-empty list until ready")

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

    all_v1_scope_channels_ready = len(ready_rows) == len(in_scope_channel_rows)
    public_claim_allowed = matrix.get("public_package_release_claim_allowed")
    if public_claim_allowed is True:
        if matrix.get("status") != "ready":
            blockers.append(
                "public_package_release_claim_allowed=true requires top-level status=ready"
            )
        if not all_v1_scope_channels_ready:
            blockers.append(
                "public_package_release_claim_allowed=true requires every v1-scope channel ready"
            )
    elif public_claim_allowed is not False:
        blockers.append("public_package_release_claim_allowed must be boolean")
    if matrix.get("status") == "ready" and not all_v1_scope_channels_ready:
        blockers.append("top-level status=ready requires every v1-scope channel ready")

    return blockers


def validate_package_identity_contract(
    repo_root: Path, matrix: dict[str, Any] | None
) -> dict[str, Any]:
    blockers: list[str] = []

    pyproject = read_text(repo_root / PYPROJECT)
    require_marker(
        blockers,
        PYPROJECT.as_posix(),
        pyproject,
        f'name = "{EXPECTED_PYTHON_PACKAGE_NAME}"',
    )
    require_marker(
        blockers,
        PYPROJECT.as_posix(),
        pyproject,
        f'requires-python = "{EXPECTED_PYTHON_REQUIRES}"',
    )
    require_marker(blockers, PYPROJECT.as_posix(), pyproject, 'license = "Apache-2.0"')
    require_marker(blockers, PYPROJECT.as_posix(), pyproject, "dependencies = []")
    if pyproject is not None:
        for forbidden in [
            "Development Status :: 5 - Production/Stable",
            "Development Status :: 6 - Mature",
            "Development Status :: 7 - Inactive",
        ]:
            if forbidden in pyproject:
                blockers.append(
                    f"{PYPROJECT.as_posix()} contains forbidden classifier {forbidden!r}"
                )

    unpublished_crates: list[str] = []
    for manifest in INTERNAL_CRATE_MANIFESTS:
        text = read_text(repo_root / manifest)
        require_marker(blockers, manifest, text, "publish = false")
        if text is not None and "publish = false" in text:
            unpublished_crates.append(manifest)

    doc = read_text(repo_root / PACKAGE_NAME_READINESS_DOC)
    require_marker(
        blockers,
        PACKAGE_NAME_READINESS_DOC.as_posix(),
        doc,
        f"PyPI: `{EXPECTED_PYTHON_PACKAGE_NAME}`",
    )
    require_marker(
        blockers,
        PACKAGE_NAME_READINESS_DOC.as_posix(),
        doc,
        "Internal crates remain unpublished.",
    )
    for candidate in EXPECTED_PUBLIC_CRATE_CANDIDATES:
        require_marker(
            blockers,
            PACKAGE_NAME_READINESS_DOC.as_posix(),
            doc,
            f"`{candidate}`",
        )

    crates_row = find_channel(matrix, "crates_io_future")
    if crates_row is None:
        blockers.append("missing crates_io_future package identity row")
    else:
        if (
            crates_row.get("workspace_crate_publish_status")
            != "all_current_workspace_crates_publish_false"
        ):
            blockers.append(
                "crates_io_future workspace_crate_publish_status must prove publish=false"
            )
        if crates_row.get("internal_crates_publish_allowed") is not False:
            blockers.append("crates_io_future internal_crates_publish_allowed must be false")
        refs = crates_row.get("prepared_local_workspace_refs")
        if refs != PACKAGE_WORKSPACE_REF_MANIFESTS:
            blockers.append(
                "crates_io_future prepared_local_workspace_refs must match current workspace package refs"
            )

    channels = matrix.get("channels", []) if isinstance(matrix, dict) else []
    if isinstance(channels, list):
        for row in channels:
            if isinstance(row, dict) and row.get("internal_crates_publish_allowed") is not False:
                blockers.append(
                    f"{row.get('channel_id', '<missing>')}: "
                    "internal_crates_publish_allowed must be false"
                )

    return {
        "status": "passed" if not blockers else "blocked",
        "python_package_identity": EXPECTED_PYTHON_PACKAGE_NAME,
        "python_requires": EXPECTED_PYTHON_REQUIRES,
        "internal_crate_publish_status": "all_publish_false"
        if len(unpublished_crates) == len(INTERNAL_CRATE_MANIFESTS)
        else "blocked",
        "internal_crate_manifest_count": len(INTERNAL_CRATE_MANIFESTS),
        "internal_crate_manifests": INTERNAL_CRATE_MANIFESTS,
        "future_public_crate_candidates": EXPECTED_PUBLIC_CRATE_CANDIDATES,
        "publication_authorization_state": (matrix or {}).get(
            "publication_authorization_state", "missing"
        ),
        "blockers": blockers,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def v1_feasibility_summary(matrix: dict[str, Any] | None) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    status_counts = {status: 0 for status in sorted(V1_FEASIBILITY_STATUSES)}
    if isinstance(matrix, dict):
        for row in matrix.get("channels", []):
            if not isinstance(row, dict):
                continue
            status = str(row.get("v1_feasibility_status", "missing"))
            if status in status_counts:
                status_counts[status] += 1
            rows.append(
                {
                    "channel_id": row.get("channel_id"),
                    "v1_feasibility_status": row.get("v1_feasibility_status"),
                    "v1_scope_decision": row.get("v1_scope_decision"),
                    "v1_feasibility_reason": row.get("v1_feasibility_reason"),
                    "ready": row.get("ready"),
                    "status": row.get("status"),
                }
            )
    reviewed_ids = (matrix or {}).get("v1_feasibility_reviewed_channel_ids", [])
    return {
        "status": "passed"
        if reviewed_ids == EXPECTED_V1_FEASIBILITY_REVIEWED_CHANNEL_IDS
        and len(rows) == len(EXPECTED_CHANNEL_IDS)
        else "blocked",
        "review_status": (matrix or {}).get("channel_v1_feasibility_review_status"),
        "reviewed_channel_ids": reviewed_ids,
        "expected_reviewed_channel_ids": EXPECTED_V1_FEASIBILITY_REVIEWED_CHANNEL_IDS,
        "status_counts": status_counts,
        "rows": rows,
    }


def false_field_blockers(payload: dict[str, Any] | None, label: str, fields: list[str]) -> list[str]:
    if payload is None:
        return []
    return [
        f"{label} {field} must be false"
        for field in fields
        if payload.get(field) is not False
    ]


def ref_rows(payload: dict[str, Any] | None, key: str) -> list[dict[str, Any]]:
    if payload is None:
        return []
    rows = payload.get(key, [])
    return [row for row in rows if isinstance(row, dict)]


def ref_paths_exist(repo_root: Path, rows: list[dict[str, Any]]) -> list[str]:
    missing: list[str] = []
    for row in rows:
        path = row.get("path")
        if not isinstance(path, str) or not path.strip():
            missing.append("<missing path>")
            continue
        if not resolve(repo_root, Path(path)).exists():
            missing.append(path)
    return missing


def validate_github_prerelease_asset_bundle(
    repo_root: Path,
    provenance_report: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    summary: dict[str, Any] = {
        "status": None,
        "asset_manifest_ref": None,
        "required_asset_kinds": GITHUB_PRERELEASE_REQUIRED_ASSET_KINDS,
        "present_asset_kinds": [],
        "missing_asset_kinds": GITHUB_PRERELEASE_REQUIRED_ASSET_KINDS,
        "staged_asset_count": 0,
    }
    if provenance_report is None:
        blockers.append("missing GitHub prerelease provenance bundle evidence")
        return summary, blockers

    status = provenance_report.get("github_prerelease_asset_bundle_status")
    summary["status"] = status
    if status != "prepared_local_no_publication":
        blockers.append(
            "github prerelease asset bundle status must be prepared_local_no_publication"
        )
    manifest_ref = provenance_report.get("github_prerelease_asset_manifest_ref")
    summary["asset_manifest_ref"] = manifest_ref
    if not isinstance(manifest_ref, str) or not manifest_ref.strip():
        blockers.append("github prerelease asset manifest ref missing")
        return summary, blockers

    manifest_path = resolve(repo_root, Path(manifest_ref))
    manifest = load_json(manifest_path)
    if manifest is None:
        blockers.append(f"github prerelease asset manifest missing: {manifest_ref}")
        return summary, blockers
    if manifest.get("schema_version") != GITHUB_PRERELEASE_BUNDLE_SCHEMA_VERSION:
        blockers.append("github prerelease asset manifest schema_version mismatch")
    if manifest.get("status") != "prepared_local_no_publication":
        blockers.append(
            "github prerelease asset manifest status must be prepared_local_no_publication"
        )
    for field in ["publication_attempted", "tag_created", "secrets_required"]:
        if manifest.get(field) is not False:
            blockers.append(f"github prerelease asset manifest {field} must be false")
    present_kinds = manifest.get("present_asset_kinds")
    if not isinstance(present_kinds, list):
        blockers.append("github prerelease asset manifest present_asset_kinds must be a list")
        present_kinds = []
    summary["present_asset_kinds"] = present_kinds
    missing_kinds = [
        kind for kind in GITHUB_PRERELEASE_REQUIRED_ASSET_KINDS if kind not in present_kinds
    ]
    summary["missing_asset_kinds"] = missing_kinds
    if missing_kinds:
        blockers.append(
            "github prerelease asset manifest missing asset kinds: "
            + ",".join(missing_kinds)
        )
    staged_refs = ref_rows(manifest, "staged_asset_refs")
    summary["staged_asset_count"] = len(staged_refs)
    if len(staged_refs) < len(GITHUB_PRERELEASE_REQUIRED_ASSET_KINDS):
        blockers.append("github prerelease asset manifest has too few staged assets")
    missing_paths = ref_paths_exist(repo_root, staged_refs)
    if missing_paths:
        blockers.append(
            "github prerelease asset manifest missing files: " + ",".join(missing_paths)
        )
    return summary, blockers


def python_registry_proof_blockers(
    proof: dict[str, Any] | None,
    *,
    channel_id: str,
    require_prior_testpypi_ref: bool = False,
) -> list[str]:
    if proof is None:
        return [f"{channel_id}: missing Python registry package proof transcript"]
    blockers: list[str] = []
    if proof.get("schema_version") != PYTHON_REGISTRY_PROOF_SCHEMA_VERSION:
        blockers.append(f"{channel_id}: registry proof schema_version mismatch")
    if proof.get("channel_id") != channel_id:
        blockers.append(
            f"{channel_id}: registry proof channel_id={proof.get('channel_id')}"
        )
    if proof.get("package_name") != EXPECTED_PYTHON_PACKAGE_NAME:
        blockers.append(f"{channel_id}: registry proof package_name must be shardloom")
    for field in [
        "proof_status",
        "install_transcript_status",
        "smoke_check_status",
        "uninstall_transcript_status",
    ]:
        if proof.get(field) != "passed":
            blockers.append(f"{channel_id}: registry proof {field}={proof.get(field)}")
    for field in [
        "fallback_attempted",
        "external_engine_invoked",
        "tag_created",
        "secrets_required",
    ]:
        if proof.get(field) is not False:
            blockers.append(f"{channel_id}: registry proof {field} must be false")
    for field in [
        "registry_upload_attempted_by_this_tool",
        "publication_attempted_by_this_tool",
        "package_channel_submission_attempted_by_this_tool",
    ]:
        if proof.get(field) is not False:
            blockers.append(f"{channel_id}: registry proof {field} must be false")
    if require_prior_testpypi_ref and not proof.get("testpypi_proof_ref"):
        blockers.append(f"{channel_id}: registry proof requires testpypi_proof_ref")
    return blockers


def python_registry_proof_summary(proof: dict[str, Any] | None) -> dict[str, Any]:
    if proof is None:
        return {
            "present": False,
            "proof_status": "missing",
            "channel_id": None,
            "package_name": None,
            "package_version": None,
        }
    return {
        "present": True,
        "proof_status": proof.get("proof_status"),
        "channel_id": proof.get("channel_id"),
        "package_name": proof.get("package_name"),
        "package_version": proof.get("package_version"),
        "install_transcript_status": proof.get("install_transcript_status"),
        "smoke_check_status": proof.get("smoke_check_status"),
        "uninstall_transcript_status": proof.get("uninstall_transcript_status"),
        "testpypi_proof_ref": proof.get("testpypi_proof_ref"),
    }


def validate_python_registry_package_proofs(
    matrix: dict[str, Any] | None,
    *,
    testpypi_proof: dict[str, Any] | None,
    pypi_proof: dict[str, Any] | None,
) -> dict[str, Any]:
    blockers: list[str] = []
    testpypi_row = find_channel(matrix, "testpypi")
    pypi_row = find_channel(matrix, "pypi")
    testpypi_ready = bool(testpypi_row and testpypi_row.get("ready") is True)
    pypi_ready = bool(pypi_row and pypi_row.get("ready") is True)

    if testpypi_proof is not None:
        blockers.extend(
            python_registry_proof_blockers(testpypi_proof, channel_id="testpypi")
        )
    if pypi_proof is not None:
        blockers.extend(
            python_registry_proof_blockers(
                pypi_proof,
                channel_id="pypi",
                require_prior_testpypi_ref=True,
            )
        )
    if testpypi_ready and testpypi_proof is None:
        blockers.append("testpypi: ready channel requires Python registry package proof")
    if pypi_ready:
        if pypi_proof is None:
            blockers.append("pypi: ready channel requires Python registry package proof")
        if testpypi_proof is None:
            blockers.append("pypi: ready channel requires prior TestPyPI proof")
        if not testpypi_ready:
            blockers.append("pypi: ready channel requires testpypi ready first")

    return {
        "status": "passed" if not blockers else "blocked",
        "testpypi": python_registry_proof_summary(testpypi_proof),
        "pypi": python_registry_proof_summary(pypi_proof),
        "pypi_requires_prior_testpypi": True,
        "blockers": blockers,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def validate_local_gate_evidence(
    *,
    repo_root: Path,
    dependency_audit_report: dict[str, Any] | None,
    release_dry_run_transcript: dict[str, Any] | None,
    provenance_report: dict[str, Any] | None,
    python_registry_package_proofs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    blockers: list[str] = []
    dependency_fields = {
        "cargo_deny_status": None,
        "cargo_audit_status": None,
        "pip_audit_status": None,
        "license_policy_status": None,
        "advisory_status": None,
        "fallback_dependency_absent": None,
    }
    if dependency_audit_report is None:
        blockers.append("missing dependency audit report")
    else:
        if dependency_audit_report.get("schema_version") != "shardloom.dependency_audit_report.v1":
            blockers.append("dependency audit schema_version mismatch")
        for field in [
            "cargo_deny_status",
            "cargo_audit_status",
            "pip_audit_status",
            "license_policy_status",
        ]:
            dependency_fields[field] = dependency_audit_report.get(field)
            if dependency_audit_report.get(field) != "passed":
                blockers.append(f"dependency audit {field}={dependency_audit_report.get(field)}")
        dependency_fields["advisory_status"] = dependency_audit_report.get("advisory_status")
        if dependency_audit_report.get("advisory_status") != "passed":
            blockers.append(
                f"dependency audit advisory_status={dependency_audit_report.get('advisory_status')}"
            )
        dependency_fields["fallback_dependency_absent"] = dependency_audit_report.get(
            "fallback_dependency_absent"
        )
        if dependency_audit_report.get("fallback_dependency_absent") is not True:
            blockers.append("dependency audit fallback_dependency_absent must be true")

    smoke_fields = {
        "proof_status": None,
        "clean_venv_install_status": None,
        "wheel_import_and_client_smoke_performed": None,
        "cli_status_smoke_performed": None,
        "cli_capabilities_smoke_performed": None,
        "local_python_example_smoke_performed": None,
        "local_python_user_surface_quickstart_performed": None,
        "local_python_result_and_evidence_printed": None,
        "local_python_unsupported_path_evidence_printed": None,
        "generated_source_user_rows_smoke_performed": None,
        "generated_source_range_smoke_performed": None,
        "prepared_native_benchmark_smoke_performed": None,
        "provenance_dry_run_performed": None,
        "sbom_checksum_manifest_generated": None,
    }
    if release_dry_run_transcript is None:
        blockers.append("missing release dry-run package smoke transcript")
    else:
        if release_dry_run_transcript.get("schema_version") != "shardloom.release_dry_run_proof.v1":
            blockers.append("release dry-run transcript schema_version mismatch")
        if release_dry_run_transcript.get("proof_status") != "passed":
            blockers.append(
                f"release dry-run proof_status={release_dry_run_transcript.get('proof_status')}"
            )
        smoke_fields["proof_status"] = release_dry_run_transcript.get("proof_status")
        smoke_fields["clean_venv_install_status"] = release_dry_run_transcript.get(
            "clean_venv_install_status"
        )
        if release_dry_run_transcript.get("clean_venv_install_status") != "passed":
            blockers.append(
                "release dry-run clean_venv_install_status="
                + str(release_dry_run_transcript.get("clean_venv_install_status", "missing"))
            )
        for field in [
            "wheel_import_and_client_smoke_performed",
            "cli_status_smoke_performed",
            "cli_capabilities_smoke_performed",
            "local_python_example_smoke_performed",
            "local_python_user_surface_quickstart_performed",
            "local_python_result_and_evidence_printed",
            "local_python_unsupported_path_evidence_printed",
            "generated_output_proof_distinct_from_no_dataset_smoke",
            "generated_source_user_rows_smoke_performed",
            "generated_source_range_smoke_performed",
            "prepared_native_benchmark_smoke_performed",
            "provenance_dry_run_performed",
            "sbom_checksum_manifest_generated",
        ]:
            smoke_fields[field] = release_dry_run_transcript.get(field)
            if release_dry_run_transcript.get(field) is not True:
                blockers.append(f"release dry-run {field} must be true")
        blockers.extend(
            false_field_blockers(
                release_dry_run_transcript,
                "release dry-run",
                [
                    *FALSE_SAFETY_FIELDS,
                    "external_runtime_dependencies_added",
                    "fallback_engine_dependency_added",
                    "fallback_attempted",
                    "external_engine_invoked",
                    "public_package_release_claim_allowed",
                ],
            )
        )

    artifact_rows = ref_rows(provenance_report, "artifact_refs")
    sbom_rows = ref_rows(provenance_report, "sbom_refs")
    checksum_rows = ref_rows(provenance_report, "checksum_refs")
    github_prerelease_bundle, github_prerelease_bundle_blockers = (
        validate_github_prerelease_asset_bundle(repo_root, provenance_report)
    )
    provenance_fields = {
        "provenance_status": None,
        "artifact_ref_count": len(artifact_rows),
        "sbom_ref_count": len(sbom_rows),
        "checksum_ref_count": len(checksum_rows),
        "fallback_dependency_absent": None,
        "github_prerelease_asset_bundle_status": github_prerelease_bundle["status"],
        "github_prerelease_asset_manifest_ref": github_prerelease_bundle[
            "asset_manifest_ref"
        ],
    }
    if provenance_report is None:
        blockers.append("missing supply-chain release evidence report")
    else:
        if provenance_report.get("schema_version") != "shardloom.supply_chain_release_evidence.v1":
            blockers.append("provenance report schema_version mismatch")
        provenance_fields["provenance_status"] = provenance_report.get("provenance_status")
        if provenance_report.get("provenance_status") != "dry_run_unsigned_local_evidence":
            blockers.append(
                "provenance status must be dry_run_unsigned_local_evidence: "
                + str(provenance_report.get("provenance_status"))
            )
        provenance_fields["fallback_dependency_absent"] = provenance_report.get(
            "fallback_dependency_absent"
        )
        if provenance_report.get("fallback_dependency_absent") is not True:
            blockers.append("provenance fallback_dependency_absent must be true")
        if not artifact_rows:
            blockers.append("provenance report missing artifact_refs")
        if not sbom_rows:
            blockers.append("provenance report missing sbom_refs")
        if not checksum_rows:
            blockers.append("provenance report missing checksum_refs")
        for label, rows in [
            ("artifact_refs", artifact_rows),
            ("sbom_refs", sbom_rows),
            ("checksum_refs", checksum_rows),
        ]:
            missing = ref_paths_exist(repo_root, rows)
            if missing:
                blockers.append(f"provenance {label} missing files: {','.join(missing)}")
        blockers.extend(
            false_field_blockers(
                provenance_report,
                "provenance",
                [
                    *FALSE_SAFETY_FIELDS,
                    "external_runtime_dependencies_added",
                    "fallback_engine_dependency_added",
                ],
            )
        )
    blockers.extend(github_prerelease_bundle_blockers)

    return {
        "status": "passed" if not blockers else "blocked",
        "required_evidence": PACKAGE_GATE_REQUIRED_EVIDENCE,
        "dependency_audit": dependency_fields,
        "package_smoke": smoke_fields,
        "provenance": provenance_fields,
        "github_prerelease_asset_bundle": github_prerelease_bundle,
        "python_registry_package_proofs": python_registry_package_proofs,
        "blockers": blockers,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def self_test(matrix: dict[str, Any] | None) -> list[str]:
    blockers: list[str] = []
    if matrix is None:
        return ["self-test requires a matrix fixture"]
    synthetic = json.loads(json.dumps(matrix))
    channels = synthetic.get("channels", [])
    if not isinstance(channels, list) or not channels:
        return ["self-test requires at least one channel row"]
    first = channels[0]
    first["ready"] = True
    first["status"] = "ready"
    first["clean_install_proof_status"] = "passed"
    first["smoke_check_status"] = "passed"
    first["sbom_checksum_provenance_status"] = "passed"
    first["current_blockers"] = []
    ready_blockers = validate_matrix(synthetic)
    expected = f"{first['channel_id']}: ready=true requires install_transcript_ref"
    if expected not in ready_blockers:
        blockers.append("self-test did not reject a ready package channel without evidence refs")
    missing_feasibility = json.loads(json.dumps(matrix))
    missing_feasibility["channel_v1_feasibility_review_status"] = "missing"
    feasibility_blockers = validate_matrix(missing_feasibility)
    expected_feasibility = "channel_v1_feasibility_review_status must be reviewed"
    if expected_feasibility not in feasibility_blockers:
        blockers.append("self-test did not reject missing channel feasibility review status")

    missing_local = validate_local_gate_evidence(
        repo_root=ROOT,
        dependency_audit_report=None,
        release_dry_run_transcript=None,
        provenance_report=None,
    )
    for expected_missing in [
        "missing dependency audit report",
        "missing release dry-run package smoke transcript",
        "missing supply-chain release evidence report",
    ]:
        if expected_missing not in missing_local["blockers"]:
            blockers.append(f"self-test did not reject {expected_missing}")
    return blockers


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    matrix_path = resolve(repo_root, args.matrix)
    output_path = resolve(repo_root, args.output)
    matrix = load_json(matrix_path)
    matrix_blockers = validate_matrix(matrix)
    package_identity_contract = validate_package_identity_contract(repo_root, matrix)
    dependency_audit = load_json(resolve(repo_root, args.dependency_audit_report))
    release_dry_run = load_json(resolve(repo_root, args.release_dry_run_transcript))
    provenance = load_json(resolve(repo_root, args.provenance_report))
    testpypi_proof = load_json(resolve(repo_root, args.testpypi_proof))
    pypi_proof = load_json(resolve(repo_root, args.pypi_proof))
    python_registry_package_proofs = validate_python_registry_package_proofs(
        matrix,
        testpypi_proof=testpypi_proof,
        pypi_proof=pypi_proof,
    )
    local_gate_evidence = validate_local_gate_evidence(
        repo_root=repo_root,
        dependency_audit_report=dependency_audit,
        release_dry_run_transcript=release_dry_run,
        provenance_report=provenance,
        python_registry_package_proofs=python_registry_package_proofs,
    )
    blockers = list(matrix_blockers)
    blockers.extend(package_identity_contract["blockers"])
    blockers.extend(python_registry_package_proofs["blockers"])
    if args.require_local_evidence:
        blockers.extend(local_gate_evidence["blockers"])
    if args.self_test:
        blockers.extend(self_test(matrix))
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "matrix_ref": str(args.matrix).replace("\\", "/"),
        "dependency_audit_report_ref": str(args.dependency_audit_report).replace("\\", "/"),
        "release_dry_run_transcript_ref": str(args.release_dry_run_transcript).replace("\\", "/"),
        "provenance_report_ref": str(args.provenance_report).replace("\\", "/"),
        "testpypi_proof_ref": str(args.testpypi_proof).replace("\\", "/"),
        "pypi_proof_ref": str(args.pypi_proof).replace("\\", "/"),
        "status": "passed" if not blockers else "failed",
        "matrix_validation_status": "passed" if not matrix_blockers else "failed",
        "package_identity_contract_status": package_identity_contract["status"],
        "package_identity_contract": package_identity_contract,
        "python_registry_package_proof_status": python_registry_package_proofs["status"],
        "python_registry_package_proofs": python_registry_package_proofs,
        "local_gate_evidence_required": args.require_local_evidence,
        "local_gate_evidence_status": local_gate_evidence["status"],
        "local_gate_evidence": local_gate_evidence,
        "channel_v1_feasibility": v1_feasibility_summary(matrix),
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
