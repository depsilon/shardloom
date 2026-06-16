#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the feasible v1 local/source/package release track.

This gate is intentionally local and side-effect-free. It does not publish
packages, create tags, create GitHub releases, upload artifacts, use secrets,
sign artifacts, or run production platform environments.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from release_channel_contract import (
    SELECTED_V0_1_0_FEASIBILITY_STATUS,
    SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS,
    SELECTED_V0_1_0_RELEASE_CHANNEL_IDS,
)
from release_report_utils import (
    fail_closed_fields,
    load_json,
    python_package_version,
    read_text,
    resolve_path,
    write_json,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_local_source_package_release_report.v1"
CONTRACT_SCHEMA_VERSION = "shardloom.v1_local_source_package_release.v1"
DEFAULT_CONTRACT = Path("docs/release/v1-local-source-package-release.json")
DEFAULT_OUTPUT = Path("target/v1-local-source-package-release-report.json")
DEFERRED_ENVIRONMENT_GATES = [
    "production_object_store_claim",
    "production_table_lakehouse_claim",
    "production_distributed_claim",
    "production_live_hybrid_claim",
    "real_foundry_integration_claim",
]
LOCAL_SOURCE_PACKAGE_FAIL_CLOSED_FIELDS = {
    **fail_closed_fields(),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _channel_map(matrix: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(matrix, dict):
        return {}
    rows = matrix.get("channels", [])
    if not isinstance(rows, list):
        return {}
    return {
        str(row.get("channel_id")): row
        for row in rows
        if isinstance(row, dict) and row.get("channel_id")
    }


def _require_markers(blockers: list[str], label: str, text: str, markers: list[str]) -> None:
    if not text:
        blockers.append(f"{label} missing or empty")
        return
    for marker in markers:
        if marker not in text:
            blockers.append(f"{label} missing marker {marker!r}")


def validate_contract(contract: dict[str, Any] | None) -> list[str]:
    blockers: list[str] = []
    if not isinstance(contract, dict):
        return ["missing v1 local/source/package release contract"]
    if contract.get("schema_version") != CONTRACT_SCHEMA_VERSION:
        blockers.append(f"contract schema_version={contract.get('schema_version')}")
    expected = {
        "status": "published_selected_channels_ready_production_claims_blocked",
        "release_track_status": "local_source_package_v1_selected_channels_published",
        "v1_scope_classification": "required_for_v1_publication_prep",
        "source_checkout_install_status": "supported_local_source_checkout",
        "local_source_install_proof_status": "validated_by_release_dry_run",
        "python_user_surface_proof_status": "validated_by_local_smoke_and_scenarios",
        "api_schema_stability_status": "stable_v1_local_contract",
        "benchmark_publication_scope": "full_local_evidence_only_claim_gated",
        "docs_website_status": "claim_safe_current_source",
        "publication_authorization_state": SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS,
    }
    for field, value in expected.items():
        if contract.get(field) != value:
            blockers.append(f"contract {field}={contract.get(field)!r}")
    if contract.get("selected_publication_channels") != SELECTED_V0_1_0_RELEASE_CHANNEL_IDS:
        blockers.append(
            "selected_publication_channels must be GitHub prerelease, TestPyPI, PyPI, and Homebrew tap"
        )
    if contract.get("deferred_environment_gate_ids") != DEFERRED_ENVIRONMENT_GATES:
        blockers.append("deferred_environment_gate_ids must match the production environment gates")
    for field, value in LOCAL_SOURCE_PACKAGE_FAIL_CLOSED_FIELDS.items():
        if contract.get(field) is not value:
            blockers.append(f"contract {field} must be {value}")
    if contract.get("public_package_release_claim_allowed") is not True:
        blockers.append("contract public_package_release_claim_allowed must be True")
    for field in (
        "package_channel_matrix_ref",
        "api_schema_stability_matrix_ref",
        "benchmark_manifest_ref",
        "release_dry_run_command",
        "python_scenario_command",
        "timing_review_command",
        "claim_boundary",
        "fallback_boundary",
    ):
        if not _non_empty_string(contract.get(field)):
            blockers.append(f"contract missing {field}")
    return blockers


def build_report(
    repo_root: Path,
    *,
    contract_ref: Path = DEFAULT_CONTRACT,
    output_ref: Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    contract_path = resolve_path(repo_root, contract_ref)
    contract = load_json(contract_path, missing_ok=True)
    blockers = validate_contract(contract)

    package_matrix_ref = Path(
        (contract or {}).get(
            "package_channel_matrix_ref",
            "docs/release/package-channel-readiness-matrix.json",
        )
    )
    api_matrix_ref = Path(
        (contract or {}).get(
            "api_schema_stability_matrix_ref",
            "docs/release/v1-api-schema-stability-matrix.json",
        )
    )
    benchmark_manifest_ref = Path(
        (contract or {}).get(
            "benchmark_manifest_ref",
            "website/assets/benchmarks/latest/manifest.json",
        )
    )
    package_matrix = load_json(resolve_path(repo_root, package_matrix_ref), missing_ok=True)
    api_matrix = load_json(resolve_path(repo_root, api_matrix_ref), missing_ok=True)
    benchmark_manifest = load_json(resolve_path(repo_root, benchmark_manifest_ref), missing_ok=True)

    channels = _channel_map(package_matrix)
    selected_channel_status: dict[str, dict[str, Any]] = {}
    for channel_id in SELECTED_V0_1_0_RELEASE_CHANNEL_IDS:
        row = channels.get(channel_id)
        if row is None:
            blockers.append(f"package matrix missing selected channel {channel_id}")
            selected_channel_status[channel_id] = {"status": "missing"}
            continue
        selected_channel_status[channel_id] = {
            "status": row.get("status"),
            "ready": row.get("ready"),
            "v1_feasibility_status": row.get("v1_feasibility_status"),
            "human_approval_required": row.get("human_approval_required"),
        }
        if row.get("v1_feasibility_status") != SELECTED_V0_1_0_FEASIBILITY_STATUS:
            blockers.append(f"{channel_id}: must be {SELECTED_V0_1_0_FEASIBILITY_STATUS}")
        if row.get("ready") is not True or row.get("status") != "ready":
            blockers.append(f"{channel_id}: must be ready after channel proof exists")
        if row.get("human_approval_required") is not True:
            blockers.append(f"{channel_id}: human_approval_required must be true")

    if isinstance(api_matrix, dict):
        if api_matrix.get("status") != "stable_schema_fixtures_declared":
            blockers.append("api schema stability matrix status must be stable_schema_fixtures_declared")
        if api_matrix.get("public_release_claim_allowed") is not False:
            blockers.append("api schema matrix public_release_claim_allowed must be false")
        if api_matrix.get("fallback_attempted") is not False:
            blockers.append("api schema matrix fallback_attempted must be false")
        if len(api_matrix.get("surfaces", [])) < 12:
            blockers.append("api schema matrix must expose at least 12 stable surfaces")
    else:
        blockers.append("missing API/schema stability matrix")

    if isinstance(benchmark_manifest, dict):
        if benchmark_manifest.get("benchmark_profile") != "full_local":
            blockers.append("benchmark manifest must be the full_local profile")
        if benchmark_manifest.get("performance_claim_allowed") is not False:
            blockers.append("benchmark manifest performance_claim_allowed must be false")
        if int(benchmark_manifest.get("published_benchmark_row_count") or 0) <= 0:
            blockers.append("benchmark manifest must include published rows")
    else:
        blockers.append("missing benchmark manifest")

    pypi_workflow = read_text(repo_root / ".github/workflows/pypi-publish-draft.yml")
    _require_markers(
        blockers,
        ".github/workflows/pypi-publish-draft.yml",
        pypi_workflow,
        [
            "python/src/shardloom/_version.py",
            "expected_version = resolve_python_package_version()",
            "validate-pypi-prior-proof",
            "inputs.testpypi_proof_ref != ''",
        ],
    )
    if 'pyproject["project"]["version"]' in pypi_workflow:
        blockers.append("PyPI workflow must not read project.version for the dynamic package")

    docs_to_markers = {
        "README.md": [
            "selected local/source/package v1 release track",
            "GitHub pre-release, TestPyPI, PyPI, and Homebrew are published for",
        ],
        "docs/getting-started/package-user-install.md": [
            "selected_publication_channels=github_prerelease,testpypi,pypi,homebrew_tap",
            "package_channel_status=published_v0.1.0_selected_channels",
            "brew install depsilon/tap/shardloom",
        ],
        "docs/release/public-status-matrix.md": [
            "selected local/source/package v1 release track",
            "real production environment gates remain",
        ],
        "website-src/src/pages/start.astro": [
            "selected local/source/package v1 release track",
            "GitHub prerelease + TestPyPI/PyPI + Homebrew",
        ],
    }
    for relative, markers in docs_to_markers.items():
        _require_markers(blockers, relative, read_text(repo_root / relative), markers)

    output_path = resolve_path(repo_root, output_ref)
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_schema_version": CONTRACT_SCHEMA_VERSION,
        "contract_ref": contract_ref.as_posix(),
        "output_ref": output_ref.as_posix(),
        "status": "passed" if not blockers else "failed",
        "release_track_status": (contract or {}).get("release_track_status", "missing"),
        "selected_publication_channels": SELECTED_V0_1_0_RELEASE_CHANNEL_IDS,
        "selected_channel_status": selected_channel_status,
        "deferred_environment_gate_ids": DEFERRED_ENVIRONMENT_GATES,
        "python_package_version": python_package_version(repo_root),
        "benchmark_manifest_ref": benchmark_manifest_ref.as_posix(),
        "benchmark_git_sha": (benchmark_manifest or {}).get("benchmark_git_sha"),
        "published_benchmark_row_count": (benchmark_manifest or {}).get(
            "published_benchmark_row_count"
        ),
        "blockers": blockers,
        **LOCAL_SOURCE_PACKAGE_FAIL_CLOSED_FIELDS,
        "public_package_release_claim_allowed": bool(
            (contract or {}).get("public_package_release_claim_allowed")
        ),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    report = build_report(
        repo_root,
        contract_ref=args.contract,
        output_ref=args.output,
    )
    output = resolve_path(repo_root, args.output)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
