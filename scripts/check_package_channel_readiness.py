#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate ShardLoom package-channel readiness evidence.

This validator accepts the current blocked/report-only matrix as valid. It fails only when the
matrix is missing, malformed, overclaims readiness, or allows publication/fallback behavior without
the required evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from check_registry_bundled_proof import bundled_registry_proof_blockers
from check_published_channel_proofs import validate_published_channel_proofs

from release_channel_contract import (
    PUBLISHED_REGISTRY_BUILD_IDENTITIES,
    PUBLISHED_REGISTRY_PROVENANCE_SHA256,
    PUBLISHED_REGISTRY_DISTRIBUTIONS,
    SELECTED_PACKAGE_RELEASE_VERSION,
    SELECTED_V0_1_0_FEASIBILITY_STATUS,
    SELECTED_V0_1_0_RELEASE_CHANNEL_IDS,
    SELECTED_PACKAGE_RELEASE_TAG,
    selected_channels_ready,
)


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
    SELECTED_V0_1_0_FEASIBILITY_STATUS,
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
        default=Path(
            f"docs/release/channel-proofs/testpypi-{SELECTED_PACKAGE_RELEASE_TAG}-transcript.json"
        ),
    )
    parser.add_argument(
        "--pypi-proof",
        type=Path,
        default=Path(
            f"docs/release/channel-proofs/pypi-{SELECTED_PACKAGE_RELEASE_TAG}-transcript.json"
        ),
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
    selected_release_channel_ids = matrix.get("selected_v0_1_0_release_channel_ids")
    if selected_release_channel_ids != SELECTED_V0_1_0_RELEASE_CHANNEL_IDS:
        blockers.append(
            "selected_v0_1_0_release_channel_ids must match the approved "
            f"{SELECTED_PACKAGE_RELEASE_TAG} channel list"
        )
    if matrix.get("package_gate_required_evidence") != PACKAGE_GATE_REQUIRED_EVIDENCE:
        blockers.append("package_gate_required_evidence must match the package-gate evidence list")
    if matrix.get("package_identity_contract_status") not in {
        "local_contract_recorded_publication_approval_blocked",
        "local_contract_recorded_publication_approved_pending_channel_proof",
        "public_package_release_selected_channels_ready",
    }:
        blockers.append(
            "package_identity_contract_status must record local contract with publication approval state"
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
            if is_ready:
                if row.get("registry_artifact_digest_binding_status") != "passed":
                    blockers.append(
                        prefix
                        + "ready=true requires registry_artifact_digest_binding_status=passed"
                    )
                if row.get("registry_install_cache_disabled") is not True:
                    blockers.append(
                        prefix + "ready=true requires registry_install_cache_disabled=true"
                    )
                if row.get("registry_download_isolated") is not True:
                    blockers.append(
                        prefix + "ready=true requires registry_download_isolated=true"
                    )
                if row.get("registry_download_cache_disabled") is not True:
                    blockers.append(
                        prefix + "ready=true requires registry_download_cache_disabled=true"
                    )
                if row.get("registry_install_from_downloaded_artifact") is not True:
                    blockers.append(
                        prefix
                        + "ready=true requires registry_install_from_downloaded_artifact=true"
                    )
                if row.get("registry_install_cache_hit_detected") is not False:
                    blockers.append(
                        prefix + "ready=true requires registry_install_cache_hit_detected=false"
                    )
                for field in [
                    "downloaded_registry_artifact_ref",
                    "downloaded_registry_artifact_filename",
                    "downloaded_registry_artifact_sha256",
                    "installed_registry_artifact_ref",
                    "installed_registry_artifact_filename",
                    "installed_registry_artifact_sha256",
                    "registry_release_artifacts_ref",
                ]:
                    if not _non_empty_string(row, field):
                        blockers.append(prefix + f"ready=true requires {field}")

        if channel_id == "crates_io_future":
            claim_boundary = row.get("claim_boundary", "")
            requirement = row.get("auth_provenance_requirement", "")
            if "future stable public" not in claim_boundary:
                blockers.append(prefix + "claim boundary must limit crates.io to future stable public crates")
            if "no internal crate publication" not in requirement:
                blockers.append(prefix + "auth requirement must forbid internal crate publication")

    all_selected_release_channels_ready = selected_channels_ready(matrix)
    public_claim_allowed = matrix.get("public_package_release_claim_allowed")
    if public_claim_allowed is True:
        if matrix.get("status") != "ready":
            blockers.append(
                "public_package_release_claim_allowed=true requires top-level status=ready"
            )
        if not all_selected_release_channels_ready:
            blockers.append(
                "public_package_release_claim_allowed=true requires every selected "
                f"{SELECTED_PACKAGE_RELEASE_TAG} release channel ready"
            )
    elif public_claim_allowed is not False:
        blockers.append("public_package_release_claim_allowed must be boolean")
    if matrix.get("status") == "ready" and not all_selected_release_channels_ready:
        blockers.append(
            "top-level status=ready requires every selected "
            f"{SELECTED_PACKAGE_RELEASE_TAG} release channel ready"
        )

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
        "download_transcript_status",
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
    if require_prior_testpypi_ref:
        expected_testpypi_ref = (
            f"docs/release/channel-proofs/testpypi-"
            f"{SELECTED_PACKAGE_RELEASE_TAG}-transcript.json"
        )
        if not proof.get("testpypi_proof_ref"):
            blockers.append(f"{channel_id}: registry proof requires testpypi_proof_ref")
        elif proof.get("testpypi_proof_ref") != expected_testpypi_ref:
            blockers.append(
                f"{channel_id}: registry proof testpypi_proof_ref must be "
                f"{expected_testpypi_ref}"
            )
    if proof.get("cli_binary_required_for_clean_registry_smoke") is not True:
        blockers.append(
            f"{channel_id}: registry proof must require an explicit ShardLoom CLI binary"
        )
    if proof.get("cli_binary_available") is not True:
        blockers.append(f"{channel_id}: registry proof cli_binary_available must be true")
    if proof.get("cli_binary_smoke_source") != "approved_release_or_local_artifact":
        blockers.append(
            f"{channel_id}: registry proof cli_binary_smoke_source="
            + str(proof.get("cli_binary_smoke_source", "missing"))
        )
    if not isinstance(proof.get("cli_binary_ref"), str) or not proof.get("cli_binary_ref"):
        blockers.append(f"{channel_id}: registry proof cli_binary_ref missing")
    if proof.get("registry_artifact_digest_binding_status") != "passed":
        blockers.append(
            f"{channel_id}: registry proof registry_artifact_digest_binding_status="
            + str(proof.get("registry_artifact_digest_binding_status", "missing"))
        )
    if proof.get("package_version") != SELECTED_PACKAGE_RELEASE_VERSION:
        blockers.append(
            f"{channel_id}: registry proof package_version must be "
            f"{SELECTED_PACKAGE_RELEASE_VERSION}"
        )
    if proof.get("registry_install_cache_disabled") is not True:
        blockers.append(f"{channel_id}: registry proof must disable pip cache")
    if proof.get("registry_download_isolated") is not True:
        blockers.append(f"{channel_id}: registry proof must use pip --isolated download")
    if proof.get("registry_download_cache_disabled") is not True:
        blockers.append(f"{channel_id}: registry proof download must disable pip cache")
    if proof.get("registry_install_from_downloaded_artifact") is not True:
        blockers.append(
            f"{channel_id}: registry proof must install the downloaded registry artifact"
        )
    if proof.get("registry_install_cache_hit_detected") is True:
        blockers.append(f"{channel_id}: registry proof must not use pip cache")
    downloaded_filename = proof.get("downloaded_registry_artifact_filename")
    if not isinstance(downloaded_filename, str):
        blockers.append(f"{channel_id}: registry proof downloaded registry artifact missing")
    elif f"-{SELECTED_PACKAGE_RELEASE_VERSION}-" not in downloaded_filename:
        blockers.append(
            f"{channel_id}: registry proof downloaded registry artifact must match "
            f"{SELECTED_PACKAGE_RELEASE_VERSION}"
        )
    downloaded_sha256 = proof.get("downloaded_registry_artifact_sha256")
    if not isinstance(downloaded_sha256, str):
        blockers.append(
            f"{channel_id}: registry proof downloaded registry artifact SHA256 missing"
        )
    artifact_filename = proof.get("installed_registry_artifact_filename")
    if not isinstance(artifact_filename, str):
        blockers.append(f"{channel_id}: registry proof installed registry artifact missing")
    elif f"-{SELECTED_PACKAGE_RELEASE_VERSION}-" not in artifact_filename:
        blockers.append(
            f"{channel_id}: registry proof installed registry artifact must match "
            f"{SELECTED_PACKAGE_RELEASE_VERSION}"
        )
    installed_sha256 = proof.get("installed_registry_artifact_sha256")
    if not isinstance(installed_sha256, str):
        blockers.append(f"{channel_id}: registry proof installed registry artifact SHA256 missing")
    elif isinstance(downloaded_sha256, str) and installed_sha256 != downloaded_sha256:
        blockers.append(
            f"{channel_id}: registry proof installed SHA256 must match downloaded SHA256"
        )
    registry_artifacts = proof.get("registry_release_artifacts")
    if not isinstance(registry_artifacts, list) or not registry_artifacts:
        blockers.append(f"{channel_id}: registry proof registry_release_artifacts missing")
    elif isinstance(downloaded_filename, str) and isinstance(downloaded_sha256, str):
        registry_row = next(
            (
                row
                for row in registry_artifacts
                if isinstance(row, dict) and row.get("filename") == downloaded_filename
            ),
            None,
        )
        if registry_row is None:
            blockers.append(
                f"{channel_id}: downloaded registry artifact missing from registry JSON"
            )
        elif registry_row.get("sha256") != downloaded_sha256:
            blockers.append(
                f"{channel_id}: downloaded registry artifact SHA256 must match registry JSON"
            )
    return blockers


def matrix_registry_artifact_blockers(
    row: dict[str, Any] | None,
    proof: dict[str, Any] | None,
    *,
    channel_id: str,
) -> list[str]:
    if row is None or proof is None or row.get("ready") is not True:
        return []

    installed_artifact = proof.get("installed_registry_artifact")
    expected_values = {
        "downloaded_registry_artifact_ref": proof.get("downloaded_registry_artifact_ref"),
        "downloaded_registry_artifact_filename": proof.get(
            "downloaded_registry_artifact_filename"
        ),
        "downloaded_registry_artifact_sha256": proof.get(
            "downloaded_registry_artifact_sha256"
        ),
        "installed_registry_artifact_filename": proof.get(
            "installed_registry_artifact_filename"
        ),
        "installed_registry_artifact_sha256": proof.get(
            "installed_registry_artifact_sha256"
        ),
        "installed_registry_artifact_ref": installed_artifact.get("url")
        if isinstance(installed_artifact, dict)
        else None,
    }

    blockers: list[str] = []
    for field, expected in expected_values.items():
        if row.get(field) != expected:
            blockers.append(
                f"{channel_id}: matrix {field} must match registry proof transcript"
            )
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
        "download_transcript_status": proof.get("download_transcript_status"),
        "install_transcript_status": proof.get("install_transcript_status"),
        "smoke_check_status": proof.get("smoke_check_status"),
        "uninstall_transcript_status": proof.get("uninstall_transcript_status"),
        "testpypi_proof_ref": proof.get("testpypi_proof_ref"),
        "cli_binary_required_for_clean_registry_smoke": proof.get(
            "cli_binary_required_for_clean_registry_smoke"
        ),
        "cli_binary_available": proof.get("cli_binary_available"),
        "cli_binary_ref": proof.get("cli_binary_ref"),
        "cli_binary_smoke_source": proof.get("cli_binary_smoke_source"),
        "registry_artifact_digest_binding_status": proof.get(
            "registry_artifact_digest_binding_status"
        ),
        "registry_download_isolated": proof.get("registry_download_isolated"),
        "registry_download_cache_disabled": proof.get("registry_download_cache_disabled"),
        "registry_install_from_downloaded_artifact": proof.get(
            "registry_install_from_downloaded_artifact"
        ),
        "registry_install_cache_disabled": proof.get("registry_install_cache_disabled"),
        "registry_install_cache_hit_detected": proof.get("registry_install_cache_hit_detected"),
        "downloaded_registry_artifact_filename": proof.get(
            "downloaded_registry_artifact_filename"
        ),
        "downloaded_registry_artifact_sha256": proof.get(
            "downloaded_registry_artifact_sha256"
        ),
        "installed_registry_artifact_filename": proof.get(
            "installed_registry_artifact_filename"
        ),
        "installed_registry_artifact_sha256": proof.get(
            "installed_registry_artifact_sha256"
        ),
        "registry_release_artifact_count": proof.get("registry_release_artifact_count"),
    }


def registry_bundled_cli_inventory_blockers(channel_id, artifact_rows, sbom):
    """Require the observed platform executables and their complete SBOM graph."""
    prefix = f"{channel_id}: registry bundled CLI inventory "
    blockers = []
    expected_components, expected_edges = {}, {}
    layouts = {
        "cp313-cp313-macosx_26_0_arm64": ("macos-aarch64", "shardloom"),
        "cp313-cp313-manylinux_2_39_x86_64": ("linux-x86_64", "shardloom"),
        "cp313-cp313-win_amd64": ("windows-x86_64", "shardloom.exe"),
    }
    for artifact in artifact_rows if isinstance(artifact_rows, list) else []:
        if not isinstance(artifact, dict) or not isinstance(artifact.get("filename"), str):
            continue  # The enclosing distribution validator rejects this row.
        filename, digest = artifact["filename"], artifact.get("sha256")
        parent = f"sha256:{digest}"
        expected_components[parent] = {
            "type": "file", "name": filename, "hashes": [{"alg": "SHA-256", "content": digest}],
            "externalReferences": [{"type": "distribution", "url": artifact.get("url")}],
        }
        if filename.endswith(".tar.gz"):
            if "bundled_cli" in artifact or artifact.get("clean_sdist_no_bundled_cli") is not True:
                blockers.append(prefix + "source distribution must record no bundled CLI")
            continue
        tag = filename.removeprefix(f"shardloom-{SELECTED_PACKAGE_RELEASE_VERSION}-").removesuffix(".whl")
        layout = layouts.get(tag)
        binary = artifact.get("bundled_cli")
        if layout is None or not isinstance(binary, dict):
            blockers.append(prefix + f"requires its approved platform CLI record: {filename}")
            continue
        platform, executable = layout
        member = f"shardloom/bin/{platform}/{executable}"
        allowed_members = {member, f"shardloom-{SELECTED_PACKAGE_RELEASE_VERSION}.data/purelib/{member}"}
        binary_digest, size = binary.get("sha256"), binary.get("size_bytes")
        if (binary.get("platform") != platform or not isinstance(binary.get("member"), str)
                or binary.get("member") not in allowed_members
                or artifact.get("wheel_tag") != tag
                or not isinstance(binary_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", binary_digest)
                or type(size) is not int or not 0 < size <= 128 << 20):
            blockers.append(prefix + f"requires a valid member, platform, digest and size: {filename}")
            continue
        child = parent + ":bundled-cli"
        expected_components[child] = {
            "type": "file", "name": filename + "!/" + binary["member"],
            "hashes": [{"alg": "SHA-256", "content": binary_digest}], "externalReferences": [],
        }
        expected_edges[parent] = [child]
    actual_components = {}
    for component in sbom.get("components", []) if isinstance(sbom.get("components"), list) else []:
        ref = component.get("bom-ref") if isinstance(component, dict) else None
        if not isinstance(ref, str) or ref in actual_components:
            blockers.append(prefix + "SBOM requires unique component references")
            continue
        actual_components[ref] = {field: component.get(field, [] if field == "externalReferences" else None)
                                  for field in ("type", "name", "hashes", "externalReferences")}
    if actual_components != expected_components:
        blockers.append(prefix + "SBOM must contain the exact distribution and bundled CLI components")
    actual_edges = {}
    for edge in sbom.get("dependencies", []) if isinstance(sbom.get("dependencies"), list) else []:
        ref = edge.get("ref") if isinstance(edge, dict) else None
        if not isinstance(ref, str) or ref in actual_edges:
            blockers.append(prefix + "SBOM requires unique dependency references")
            continue
        actual_edges[ref] = edge.get("dependsOn")
    if actual_edges != expected_edges:
        blockers.append(prefix + "SBOM must bind each wheel to its bundled CLI dependency")
    return blockers


def validate_registry_supply_chain_evidence(
    repo_root: Path,
    matrix: dict[str, Any] | None,
    proofs: dict[str, dict[str, Any] | None],
) -> dict[str, Any]:
    """Bind every ready registry channel to its own checked-in artifact evidence.

    Registry uploads rebuild the distributions. A source-equivalent GitHub wheel's
    checksum or SBOM cannot certify the bytes distributed by either registry.
    This check is offline and does not assert signed build provenance.
    """
    blockers: list[str] = []
    verified_channels: list[str] = []
    for channel_id in ("testpypi", "pypi"):
        row = find_channel(matrix, channel_id)
        if not row or row.get("ready") is not True:
            continue
        start = len(blockers)
        prefix = f"{channel_id}: "

        def read_evidence(field: str) -> bytes | None:
            ref = row.get(field)
            if not isinstance(ref, str) or not ref:
                blockers.append(prefix + f"missing registry {field}")
                return None
            path = Path(ref)
            if path.is_absolute() or ":" in ref or ".." in path.parts:
                blockers.append(prefix + f"registry {field} must be a checked-in relative path")
                return None
            try:
                evidence_path = repo_root / path
                if not evidence_path.resolve().is_relative_to(repo_root.resolve()):
                    raise ValueError("evidence path escapes repository")
                return evidence_path.read_bytes()
            except (OSError, ValueError):
                blockers.append(prefix + f"registry {field} is not readable: {ref}")
                return None

        def object_evidence(data: bytes | None, field: str) -> dict[str, Any]:
            try:
                value = json.loads(data) if data is not None else None
            except (ValueError, UnicodeError):
                value = None
            if not isinstance(value, dict):
                blockers.append(prefix + f"registry {field} must contain a JSON object")
                return {}
            return value

        provenance_bytes = read_evidence("provenance_ref")
        provenance = object_evidence(provenance_bytes, "provenance_ref")
        approved_provenance_sha = PUBLISHED_REGISTRY_PROVENANCE_SHA256.get(
            SELECTED_PACKAGE_RELEASE_VERSION, {}).get(channel_id)
        if (approved_provenance_sha is None or provenance_bytes is None
                or hashlib.sha256(provenance_bytes).hexdigest() != approved_provenance_sha):
            blockers.append(prefix + "registry provenance SHA256 must match the approved observation")
        if row.get("provenance_ref") != (
            f"docs/release/channel-proofs/{channel_id}-v{SELECTED_PACKAGE_RELEASE_VERSION}-provenance.json"
        ):
            blockers.append(prefix + "registry provenance must reference the approved observation path")
        sbom_bytes = read_evidence("sbom_ref")
        sbom = object_evidence(sbom_bytes, "sbom_ref")
        checksum_bytes = read_evidence("checksum_ref")
        for field, expected in {
            "schema_version": "shardloom.registry_release_evidence.v1",
            "channel_id": channel_id,
            "package_version": SELECTED_PACKAGE_RELEASE_VERSION,
            "proof_status": "passed",
            "provenance_status": "unsigned_post_publication_observation",
        }.items():
            if provenance.get(field) != expected:
                blockers.append(prefix + f"registry provenance {field} must be {expected}")
        for field in ("publication_attempted", "package_upload_attempted", "fallback_attempted",
                      "external_engine_invoked", "crypto_attestation_verification_performed",
                      "complete_compiled_dependency_inventory_claimed", "local_build_or_package_execution_performed"):
            if provenance.get(field) is not False:
                blockers.append(prefix + f"registry provenance {field} must be false")
        if not re.fullmatch(r"[0-9a-f]{40}", str(provenance.get("source_commit", ""))):
            blockers.append(prefix + "registry provenance requires the actual build source commit")
        run_id = provenance.get("workflow_run_id")
        if type(run_id) is not int or run_id <= 0 or provenance.get("workflow_url") != (
            f"https://github.com/depsilon/shardloom/actions/runs/{run_id}"
        ):
            blockers.append(prefix + "registry provenance requires its publishing workflow run")
        expected_identity = PUBLISHED_REGISTRY_BUILD_IDENTITIES.get(
            SELECTED_PACKAGE_RELEASE_VERSION, {}
        ).get(channel_id)
        if expected_identity is None:
            blockers.append(prefix + "selected release has no approved registry build identity")
        else:
            for field, expected in expected_identity.items():
                if provenance.get(field) != expected:
                    blockers.append(prefix + f"registry provenance {field} must match the approved channel build")
        for field, data in (("sbom_ref", sbom_bytes), ("checksum_ref", checksum_bytes)):
            binding = provenance.get(field)
            if not isinstance(binding, dict) or binding.get("path") != row.get(field) or (
                data is not None and binding.get("sha256") != hashlib.sha256(data).hexdigest()
            ):
                blockers.append(prefix + f"registry provenance must bind {field} path and SHA256")

        proof = proofs.get(channel_id) or {}
        proof_bytes = read_evidence("registry_release_artifacts_ref")
        canonical_proof = object_evidence(proof_bytes, "registry_release_artifacts_ref")
        binding = provenance.get("channel_proof_ref")
        if (canonical_proof != proof or not isinstance(binding, dict)
                or binding.get("path") != row.get("registry_release_artifacts_ref")
                or proof_bytes is None
                or binding.get("sha256") != hashlib.sha256(proof_bytes).hexdigest()):
            blockers.append(prefix + "registry provenance must bind the complete channel proof path and SHA256")
        for field in ("install_transcript_ref", "uninstall_transcript_ref", "clean_install_transcript_ref", "smoke_transcript_ref"):
            if row.get(field) != row.get("registry_release_artifacts_ref"):
                blockers.append(prefix + f"{field} must reference the bound channel proof")
        runtime_identity = PUBLISHED_REGISTRY_BUILD_IDENTITIES.get(SELECTED_PACKAGE_RELEASE_VERSION, {}).get("testpypi", {})
        stdout_path = repo_root / (f"docs/release/channel-proofs/{channel_id}-v"
                                  f"{SELECTED_PACKAGE_RELEASE_VERSION}-bundled-smoke.stdout.json")
        try:
            if not stdout_path.resolve().is_relative_to(repo_root.resolve()):
                raise ValueError("smoke stdout escapes repository")
            with stdout_path.open("rb") as capture:
                smoke_stdout = capture.read(65537)
        except (OSError, ValueError):
            smoke_stdout = None
        blockers.extend(bundled_registry_proof_blockers(
            proof, channel_id=channel_id, package_version=SELECTED_PACKAGE_RELEASE_VERSION,
            runtime_source_commit=runtime_identity.get("source_commit"),
            smoke_stdout=smoke_stdout,
        ))
        registry_rows = proof.get("registry_release_artifacts")
        artifact_rows = provenance.get("artifact_refs")
        expected_artifacts: dict[str, tuple[Any, Any, Any]] = {}
        actual_artifacts: dict[str, tuple[Any, Any, Any]] = {}
        for rows, destination, label, size_field in (
            (registry_rows, expected_artifacts, "registry proof", "size"),
            (artifact_rows, actual_artifacts, "registry provenance", "size_bytes"),
        ):
            if not isinstance(rows, list) or not rows:
                blockers.append(prefix + f"{label} requires a complete artifact inventory")
                continue
            for artifact in rows:
                if not isinstance(artifact, dict) or not isinstance(artifact.get("filename"), str):
                    blockers.append(prefix + f"{label} contains an invalid artifact")
                    continue
                filename = artifact["filename"]
                if filename in destination:
                    blockers.append(prefix + f"{label} duplicates {filename}")
                digest = artifact.get("sha256")
                if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                    blockers.append(prefix + f"{label} requires SHA256 for {filename}")
                size = artifact.get(size_field)
                url = artifact.get("url")
                if type(size) is not int or size <= 0:
                    blockers.append(prefix + f"{label} requires a positive byte size for {filename}")
                expected_host = "test-files.pythonhosted.org" if channel_id == "testpypi" else "files.pythonhosted.org"
                try:
                    parsed_url = urlsplit(url) if isinstance(url, str) else None
                    valid_url = (parsed_url is not None and parsed_url.scheme == "https"
                                 and url == parsed_url.geturl()
                                 and parsed_url.netloc == expected_host
                                 and parsed_url.path.rsplit("/", 1)[-1] == filename
                                 and not parsed_url.query and not parsed_url.fragment)
                except ValueError:
                    valid_url = False
                if not valid_url:
                    blockers.append(prefix + f"{label} URL must identify {filename} on {expected_host}")
                if label == "registry provenance" and artifact.get("registry_digest_match") is not True:
                    blockers.append(prefix + f"registry provenance must record a passed digest match for {filename}")
                destination[filename] = (digest, size, url)
        if actual_artifacts != expected_artifacts:
            blockers.append(prefix + "registry provenance must match every registry artifact digest, size and URL")
        installed = proof.get("installed_registry_artifact")
        installed_name = proof.get("installed_registry_artifact_filename")
        installed_expected = expected_artifacts.get(installed_name) if isinstance(installed_name, str) else None
        if (not isinstance(installed, dict) or installed_expected is None
                or installed.get("filename") != installed_name
                or installed.get("sha256") != installed_expected[0]
                or installed.get("url") != installed_expected[2]
                or row.get("installed_registry_artifact_ref") != installed_expected[2]):
            blockers.append(prefix + "installed artifact and matrix URL must match the registry inventory")
        approved_filenames = PUBLISHED_REGISTRY_DISTRIBUTIONS.get(SELECTED_PACKAGE_RELEASE_VERSION)
        if not approved_filenames:
            blockers.append(prefix + "selected release has no approved registry distribution inventory")
        else:
            if set(expected_artifacts) != set(approved_filenames) or set(actual_artifacts) != set(approved_filenames):
                blockers.append(prefix + "registry proof and provenance must cover the exact approved distribution filenames")
            for label, evidence in (("proof", proof), ("matrix", row)):
                count = evidence.get("registry_release_artifact_count")
                if type(count) is not int or count != len(approved_filenames):
                    blockers.append(prefix + f"registry {label} artifact count must match the approved distribution inventory")
        expected_hashes = {name: values[0] for name, values in expected_artifacts.items()}
        checksums: dict[str, str] = {}
        try:
            for line in (checksum_bytes or b"").decode("utf-8").splitlines():
                match = re.fullmatch(r"([0-9a-f]{64})  ([^/\\]+)", line)
                if match is None or match[2] in checksums:
                    raise ValueError("invalid or duplicate checksum row")
                checksums[match[2]] = match[1]
        except (ValueError, UnicodeError):
            blockers.append(prefix + "registry checksum manifest is malformed")
        if checksums != expected_hashes:
            blockers.append(prefix + "registry checksum manifest must cover every registry artifact")
        if sbom.get("bomFormat") != "CycloneDX" or sbom.get("specVersion") != "1.5":
            blockers.append(prefix + "registry SBOM must be CycloneDX 1.5")
        components = sbom.get("components")
        components = components if isinstance(components, list) else []
        for filename, digest in expected_hashes.items():
            matches = [item for item in components if isinstance(item, dict)
                       and item.get("type") == "file" and item.get("name") == filename]
            hashes = matches[0].get("hashes") if len(matches) == 1 else None
            if not isinstance(hashes, list) or {"alg": "SHA-256", "content": digest} not in hashes:
                blockers.append(prefix + f"registry SBOM must bind artifact SHA256: {filename}")
        blockers.extend(registry_bundled_cli_inventory_blockers(channel_id, artifact_rows, sbom))
        if len(blockers) == start:
            verified_channels.append(channel_id)
    return {
        "status": "passed" if not blockers else "blocked",
        "verified_channels": verified_channels,
        "blockers": blockers,
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
        blockers.extend(
            matrix_registry_artifact_blockers(
                testpypi_row,
                testpypi_proof,
                channel_id="testpypi",
            )
        )
    if pypi_proof is not None:
        blockers.extend(
            python_registry_proof_blockers(
                pypi_proof,
                channel_id="pypi",
                require_prior_testpypi_ref=True,
            )
        )
        blockers.extend(
            matrix_registry_artifact_blockers(
                pypi_row,
                pypi_proof,
                channel_id="pypi",
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
        "generated_source_user_rows_runtime_performed": None,
        "generated_source_range_runtime_performed": None,
        "benchmark_smoke_required_for_package_release": None,
        "benchmark_smoke_status": None,
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
            "generated_source_user_rows_runtime_performed",
            "generated_source_range_runtime_performed",
            "provenance_dry_run_performed",
            "sbom_checksum_manifest_generated",
        ]:
            smoke_fields[field] = release_dry_run_transcript.get(field)
            if release_dry_run_transcript.get(field) is not True:
                blockers.append(f"release dry-run {field} must be true")
        smoke_fields["benchmark_smoke_required_for_package_release"] = (
            release_dry_run_transcript.get("benchmark_smoke_required_for_package_release")
        )
        smoke_fields["benchmark_smoke_status"] = release_dry_run_transcript.get(
            "benchmark_smoke_status"
        )
        if release_dry_run_transcript.get("benchmark_smoke_required_for_package_release") is not False:
            blockers.append(
                "release dry-run benchmark_smoke_required_for_package_release must be false"
            )
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
    for field in READY_REFERENCE_FIELDS:
        first.pop(field, None)
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
    registry_supply_chain_evidence = validate_registry_supply_chain_evidence(
        repo_root, matrix, {"testpypi": testpypi_proof, "pypi": pypi_proof}
    )
    published_channel_proofs = validate_published_channel_proofs(repo_root, matrix)
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
    blockers.extend(registry_supply_chain_evidence["blockers"])
    blockers.extend(published_channel_proofs["blockers"])
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
        "registry_supply_chain_evidence": registry_supply_chain_evidence,
        "published_channel_proofs": published_channel_proofs,
        "local_gate_evidence_required": args.require_local_evidence,
        "local_gate_evidence_status": local_gate_evidence["status"],
        "local_gate_evidence": local_gate_evidence,
        "channel_v1_feasibility": v1_feasibility_summary(matrix),
        "claim_gate_status": (matrix or {}).get("claim_gate_status", "missing"),
        "public_package_release_claim_allowed": (matrix or {}).get(
            "public_package_release_claim_allowed", False
        ),
        "selected_v0_1_0_release_channel_ids": (matrix or {}).get(
            "selected_v0_1_0_release_channel_ids", []
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
