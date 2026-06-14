#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Aggregate finished-product readiness without publishing anything.

Default mode proves that the local v1 product evidence bundle is coherent while public package and
release publication remain blocked. Use `--require-public-release-ready` only from a future
maintainer-approved release command; in that mode any package channel, benchmark publication,
human-approval, or hard-release blocker fails the gate.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_report_utils import fail_closed_fields, load_json


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.finished_product_readiness_report.v1"
DEFAULT_OUTPUT = Path("target/finished-product-readiness-report.json")

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


@dataclass(frozen=True)
class ReportRequirement:
    name: str
    path: Path
    schema_version: str
    status_field: str | None = "status"
    expected_status: str = "passed"


LOCAL_PRODUCT_REPORTS: tuple[ReportRequirement, ...] = (
    ReportRequirement(
        "production_usability",
        Path("target/production-usability-gate.json"),
        "shardloom.production_usability_gate.v1",
    ),
    ReportRequirement(
        "package_channel_readiness",
        Path("target/package-channel-readiness-report.json"),
        "shardloom.package_channel_readiness_report.v1",
    ),
    ReportRequirement(
        "workspace_version_source",
        Path("target/workspace-version-source-report.json"),
        "shardloom.workspace_version_source_report.v1",
    ),
    ReportRequirement(
        "python_user_surface",
        Path("target/python-user-surface-completion-gate.json"),
        "shardloom.python_user_surface_completion_gate.v1",
    ),
    ReportRequirement(
        "sql_python_dataframe_parity",
        Path("target/sql-python-dataframe-parity-gate.json"),
        "shardloom.sql_python_dataframe_parity_gate.v1",
    ),
    ReportRequirement(
        "v1_front_door_runtime_scope",
        Path("target/v1-front-door-runtime-scope-report.json"),
        "shardloom.v1_front_door_runtime_scope_report.v1",
    ),
    ReportRequirement(
        "v1_vortex_runtime_scope",
        Path("target/v1-vortex-runtime-scope-report.json"),
        "shardloom.v1_vortex_runtime_scope_report.v1",
    ),
    ReportRequirement(
        "v1_source_prepared_state_scope",
        Path("target/v1-source-prepared-state-scope-report.json"),
        "shardloom.v1_source_prepared_state_scope_report.v1",
    ),
    ReportRequirement(
        "v1_local_output_sink_scope",
        Path("target/v1-local-output-sink-scope-report.json"),
        "shardloom.v1_local_output_sink_scope_report.v1",
    ),
    ReportRequirement(
        "v1_local_resource_safety",
        Path("target/v1-local-resource-safety-report.json"),
        "shardloom.v1_local_resource_safety_report.v1",
    ),
    ReportRequirement(
        "v1_observability_support",
        Path("target/v1-observability-support-report.json"),
        "shardloom.v1_observability_support_report.v1",
    ),
    ReportRequirement(
        "v1_api_schema_stability",
        Path("target/v1-api-schema-stability-report.json"),
        "shardloom.v1_api_schema_stability_report.v1",
    ),
    ReportRequirement(
        "v1_example_replay",
        Path("target/v1-example-replay-report.json"),
        "shardloom.v1_example_replay_report.v1",
    ),
    ReportRequirement(
        "v1_correctness_conformance",
        Path("target/v1-correctness-conformance-report.json"),
        "shardloom.v1_correctness_conformance_report.v1",
    ),
    ReportRequirement(
        "v1_security_ci_hardening",
        Path("target/v1-security-ci-hardening-report.json"),
        "shardloom.v1_security_ci_hardening_report.v1",
    ),
    ReportRequirement(
        "v1_release_boundary",
        Path("target/v1-release-boundary-report.json"),
        "shardloom.v1_release_boundary_report.v1",
    ),
    ReportRequirement(
        "user_surface_runtime_gap_inventory",
        Path("target/user-surface-runtime-gap-inventory.json"),
        "shardloom.user_surface_runtime_gap_inventory.v1",
    ),
    ReportRequirement(
        "user_surface_graduation_matrix",
        Path("target/user-surface-graduation-matrix.json"),
        "shardloom.user_surface_graduation_matrix.v1",
    ),
    ReportRequirement(
        "runtime_gap_family_burn_down",
        Path("target/runtime-gap-family-burn-down.json"),
        "shardloom.runtime_gap_family_burn_down.v1",
    ),
    ReportRequirement(
        "user_route_capability",
        Path("target/user-route-capability-report.json"),
        "shardloom.user_route_capability_report.v1",
    ),
    ReportRequirement(
        "public_status_docs",
        Path("target/public-status-docs-report.json"),
        "shardloom.public_status_docs_report.v1",
    ),
    ReportRequirement(
        "website_readiness",
        Path("target/website-readiness-report.json"),
        "shardloom.website_readiness.v3",
        status_field=None,
    ),
    ReportRequirement(
        "benchmark_artifact_completeness",
        Path("target/benchmark-artifact-completeness-report.json"),
        "shardloom.benchmark_artifact_completeness_report.v1",
    ),
    ReportRequirement(
        "front_door_benchmark_publication",
        Path("target/front-door-benchmark-publication-gate.json"),
        "shardloom.front_door_benchmark_publication_gate.v1",
    ),
    ReportRequirement(
        "ci_gate_matrix",
        Path("target/ci-gate-matrix-report.json"),
        "shardloom.ci_gate_matrix_report.v1",
    ),
)

PUBLICATION_REPORTS: tuple[ReportRequirement, ...] = (
    ReportRequirement(
        "benchmark_publication_claim",
        Path("target/benchmark-publication-claim-gate-report.json"),
        "shardloom.benchmark_publication_claim_gate.v1",
    ),
    ReportRequirement(
        "final_release_rehearsal",
        Path("target/final-release-rehearsal/final-release-rehearsal-report.json"),
        "shardloom.final_release_rehearsal_report.v1",
    ),
    ReportRequirement(
        "hard_release_readiness",
        Path("target/hard-release-readiness-gate.json"),
        "shardloom.hard_release_readiness_gate.v1",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--package-channel-matrix",
        type=Path,
        default=Path("docs/release/package-channel-readiness-matrix.json"),
    )
    parser.add_argument(
        "--require-public-release-ready",
        action="store_true",
        help="Fail unless package/publication/human-approval evidence is public-release ready.",
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def report_blockers(
    requirement: ReportRequirement,
    payload: dict[str, Any] | None,
) -> list[str]:
    blockers: list[str] = []
    if payload is None:
        return [f"{requirement.name}: missing {requirement.path.as_posix()}"]
    if payload.get("schema_version") != requirement.schema_version:
        blockers.append(
            f"{requirement.name}: schema_version="
            + str(payload.get("schema_version", "missing"))
        )
    if requirement.status_field is not None:
        observed = payload.get(requirement.status_field)
        if observed != requirement.expected_status:
            blockers.append(
                f"{requirement.name}: {requirement.status_field}={observed!s}"
            )
    upstream_blockers = payload.get("blockers")
    if isinstance(upstream_blockers, list) and upstream_blockers:
        blockers.append(
            f"{requirement.name}: blockers present: "
            + "; ".join(str(item) for item in upstream_blockers[:20])
        )
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            blockers.append(f"{requirement.name}: {field} must be false")
    return blockers


def local_report_summary(
    repo_root: Path,
    requirement: ReportRequirement,
) -> tuple[dict[str, Any], list[str]]:
    payload = load_json(resolve(repo_root, requirement.path), missing_ok=True)
    blockers = report_blockers(requirement, payload)
    if requirement.name == "package_channel_readiness" and payload is not None:
        if payload.get("local_gate_evidence_status") != "passed":
            blockers.append(
                "package_channel_readiness: local_gate_evidence_status="
                + str(payload.get("local_gate_evidence_status", "missing"))
            )
        if payload.get("package_identity_contract_status") != "passed":
            blockers.append(
                "package_channel_readiness: package_identity_contract_status="
                + str(payload.get("package_identity_contract_status", "missing"))
            )
    summary = {
        "name": requirement.name,
        "path": requirement.path.as_posix(),
        "schema_version": payload.get("schema_version") if payload else None,
        "status": "passed" if not blockers else "blocked",
        "report_status": payload.get("status") if payload else None,
        "blockers": blockers,
    }
    return summary, blockers


def publication_report_summary(
    repo_root: Path,
    requirement: ReportRequirement,
) -> tuple[dict[str, Any], list[str], bool]:
    payload = load_json(resolve(repo_root, requirement.path), missing_ok=True)
    blockers = report_blockers(requirement, payload)
    public_ready = payload is not None and not blockers
    public_blockers: list[str] = []
    if payload is None:
        public_blockers.extend(blockers)
    elif blockers:
        public_blockers.extend(blockers)
    summary = {
        "name": requirement.name,
        "path": requirement.path.as_posix(),
        "schema_version": payload.get("schema_version") if payload else None,
        "status": "passed" if not blockers else "blocked",
        "report_status": payload.get("status") if payload else None,
        "public_release_ready": public_ready,
        "blockers": blockers,
    }
    return summary, public_blockers, public_ready


def package_matrix_summary(
    repo_root: Path,
    matrix_path: Path,
) -> tuple[dict[str, Any], list[str], bool]:
    payload = load_json(resolve(repo_root, matrix_path), missing_ok=True)
    blockers: list[str] = []
    if payload is None:
        blockers.append(f"package_channel_matrix: missing {matrix_path.as_posix()}")
        return (
            {
                "path": matrix_path.as_posix(),
                "status": "blocked",
                "blockers": blockers,
                "ready_channel_count": 0,
                "expected_channel_count": 0,
                "blocked_channel_ids": [],
            },
            blockers,
            False,
        )
    if payload.get("schema_version") != "shardloom.package_channel_readiness_matrix.v1":
        blockers.append(
            "package_channel_matrix: schema_version="
            + str(payload.get("schema_version", "missing"))
        )
    channels = [row for row in payload.get("channels", []) if isinstance(row, dict)]
    blocked_channels = [
        str(row.get("channel_id", "unknown"))
        for row in channels
        if row.get("ready") is not True
    ]
    if payload.get("public_package_release_claim_allowed") is not True:
        blockers.append("package_channel_matrix: public_package_release_claim_allowed=false")
    if blocked_channels:
        blockers.append(
            "package_channel_matrix: package channels not ready: "
            + ", ".join(blocked_channels)
        )
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            blockers.append(f"package_channel_matrix: {field} must be false")
    human_approved = payload.get("publication_authorization_state") == "approved"
    if not human_approved:
        blockers.append(
            "package_channel_matrix: publication_authorization_state="
            + str(payload.get("publication_authorization_state", "missing"))
        )
    public_ready = not blockers
    summary = {
        "path": matrix_path.as_posix(),
        "schema_version": payload.get("schema_version"),
        "status": "passed" if public_ready else "blocked",
        "matrix_status": payload.get("status"),
        "ready_channel_count": len(channels) - len(blocked_channels),
        "expected_channel_count": len(channels),
        "blocked_channel_ids": blocked_channels,
        "publication_authorization_state": payload.get("publication_authorization_state"),
        "public_package_release_claim_allowed": payload.get(
            "public_package_release_claim_allowed"
        ),
        "blockers": blockers,
    }
    return summary, blockers, public_ready


def build_report(
    repo_root: Path,
    *,
    output: Path = DEFAULT_OUTPUT,
    package_channel_matrix: Path = Path("docs/release/package-channel-readiness-matrix.json"),
    require_public_release_ready: bool = False,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    local_report_rows: list[dict[str, Any]] = []
    local_blockers: list[str] = []
    for requirement in LOCAL_PRODUCT_REPORTS:
        row, blockers = local_report_summary(repo_root, requirement)
        local_report_rows.append(row)
        local_blockers.extend(blockers)

    publication_rows: list[dict[str, Any]] = []
    public_release_blockers: list[str] = []
    for requirement in PUBLICATION_REPORTS:
        row, blockers, _ready = publication_report_summary(repo_root, requirement)
        publication_rows.append(row)
        public_release_blockers.extend(blockers)

    matrix_row, matrix_blockers, _matrix_ready = package_matrix_summary(
        repo_root,
        package_channel_matrix,
    )
    public_release_blockers.extend(matrix_blockers)

    local_evidence_ready = not local_blockers
    public_release_ready = local_evidence_ready and not public_release_blockers
    status = (
        "passed"
        if local_evidence_ready
        and (public_release_ready or not require_public_release_ready)
        else "blocked"
    )
    readiness_status = (
        "public_release_ready"
        if public_release_ready
        else "local_v1_ready_publication_blocked"
        if local_evidence_ready
        else "blocked_local_v1_evidence"
    )
    blockers = list(local_blockers)
    if require_public_release_ready:
        blockers.extend(public_release_blockers)

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "finished_product_readiness_status": readiness_status,
        "require_public_release_ready": require_public_release_ready,
        "local_evidence_ready": local_evidence_ready,
        "public_release_ready": public_release_ready,
        "public_release_claim_allowed": public_release_ready,
        "public_package_claim_allowed": public_release_ready,
        "publication_authorization_status": matrix_row.get(
            "publication_authorization_state", "missing"
        ),
        "local_product_report_count": len(local_report_rows),
        "local_product_reports": local_report_rows,
        "publication_report_count": len(publication_rows),
        "publication_reports": publication_rows,
        "package_channel_matrix": matrix_row,
        "local_evidence_blockers": local_blockers,
        "public_release_blockers": public_release_blockers,
        "blockers": blockers,
        "output_ref": output.as_posix(),
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "package_upload_attempted": False,
        "package_channel_submission_attempted": False,
        "oci_push_attempted": False,
        "signing_key_used": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_gate_status": "not_claim_grade" if not public_release_ready else "claim_grade",
    }
    if not public_release_ready:
        report.update(
            {
                "production_claim_allowed": False,
                "performance_claim_allowed": False,
                "spark_replacement_claim_allowed": False,
            }
        )
    report.update(
        {
            key: value
            for key, value in fail_closed_fields().items()
            if key not in {"public_release_claim_allowed", "public_package_claim_allowed"}
        }
    )
    return report


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(
        repo_root,
        output=args.output,
        package_channel_matrix=args.package_channel_matrix,
        require_public_release_ready=args.require_public_release_ready,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"{report['status']}: {output}")
    if report["blockers"]:
        for blocker in report["blockers"][:50]:
            print(f"- {blocker}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
