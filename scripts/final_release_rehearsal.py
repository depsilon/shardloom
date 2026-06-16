#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Aggregate final no-publication release rehearsal evidence for ShardLoom.

This rehearsal is intentionally local-only. It inspects existing dry-run artifacts, writes a local
attestation plan, and fails closed until approved package channels have real publication proof.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.final_release_rehearsal_report.v1"
ATTESTATION_PLAN_SCHEMA_VERSION = "shardloom.local_publication_attestation_plan.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("target/final-release-rehearsal"),
    )
    parser.add_argument(
        "--provenance-manifest",
        type=Path,
        default=Path("target/release-provenance-dry-run/manifest.json"),
    )
    parser.add_argument(
        "--provenance-report",
        type=Path,
        default=Path("target/release-provenance-dry-run/supply-chain-release-evidence.json"),
    )
    parser.add_argument(
        "--release-security-report",
        type=Path,
        default=Path("target/release-security-gate-report.json"),
    )
    parser.add_argument(
        "--contribution-governance-report",
        type=Path,
        default=Path("target/contribution-governance-report.json"),
    )
    parser.add_argument(
        "--golden-workflow-report",
        type=Path,
        default=Path("target/golden-workflow-report.json"),
    )
    parser.add_argument(
        "--admitted-semantics-report",
        type=Path,
        default=Path("target/admitted-semantics-matrix-report.json"),
    )
    parser.add_argument(
        "--architecture-tracker-report",
        type=Path,
        default=Path("target/release-architecture-tracker-report.json"),
    )
    parser.add_argument(
        "--package-channel-report",
        type=Path,
        default=Path("target/package-channel-readiness-report.json"),
    )
    parser.add_argument("--allow-blocked", action="store_true")
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def rel(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def artifact_refs(provenance: dict[str, Any] | None, key: str) -> list[dict[str, Any]]:
    if provenance is None:
        return []
    rows = provenance.get(key, [])
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


def false_field_blockers(payload: dict[str, Any] | None, label: str, fields: list[str]) -> list[str]:
    if payload is None:
        return []
    blockers = []
    for field in fields:
        if payload.get(field) is not False:
            blockers.append(f"{label} {field} must be false")
    return blockers


def upstream_report_blockers(
    payload: dict[str, Any] | None,
    label: str,
    *,
    status_fields: tuple[str, ...] = ("status",),
    expected_status: str = "passed",
) -> list[str]:
    if payload is None:
        return []
    blockers: list[str] = []
    upstream_blockers = payload.get("blockers")
    if isinstance(upstream_blockers, list) and upstream_blockers:
        blockers.append(f"{label} blockers: " + ",".join(str(item) for item in upstream_blockers))
    observed_statuses = {
        field: payload.get(field)
        for field in status_fields
        if payload.get(field) is not None
    }
    if not observed_statuses:
        blockers.append(f"{label} missing status field: {'/'.join(status_fields)}")
    elif not any(status == expected_status for status in observed_statuses.values()):
        rendered = ",".join(f"{field}={value}" for field, value in observed_statuses.items())
        blockers.append(f"{label} status must include {expected_status}: {rendered}")
    return blockers


def provenance_status_blockers(payload: dict[str, Any] | None) -> list[str]:
    if payload is None:
        return []
    blockers: list[str] = []
    if payload.get("provenance_status") != "dry_run_unsigned_local_evidence":
        blockers.append(
            f"provenance status must be dry_run_unsigned_local_evidence: {payload.get('provenance_status')}"
        )
    upstream_blockers = payload.get("blockers")
    if isinstance(upstream_blockers, list) and upstream_blockers:
        blockers.append("provenance blockers: " + ",".join(str(item) for item in upstream_blockers))
    return blockers


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output_dir = resolve(repo_root, args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / "final-release-rehearsal-report.json"
    attestation_plan_path = output_dir / "local-publication-attestation-plan.json"

    provenance_manifest_path = resolve(repo_root, args.provenance_manifest)
    provenance_report_path = resolve(repo_root, args.provenance_report)
    security_report_path = resolve(repo_root, args.release_security_report)
    contribution_report_path = resolve(repo_root, args.contribution_governance_report)
    golden_workflow_report_path = resolve(repo_root, args.golden_workflow_report)
    admitted_semantics_report_path = resolve(repo_root, args.admitted_semantics_report)
    architecture_report_path = resolve(repo_root, args.architecture_tracker_report)
    package_report_path = resolve(repo_root, args.package_channel_report)

    provenance_manifest = load_json(provenance_manifest_path)
    provenance = load_json(provenance_report_path)
    security_report = load_json(security_report_path)
    contribution_report = load_json(contribution_report_path)
    golden_workflow_report = load_json(golden_workflow_report_path)
    admitted_semantics_report = load_json(admitted_semantics_report_path)
    architecture_report = load_json(architecture_report_path)
    package_report = load_json(package_report_path)
    package_matrix = load_json(repo_root / "docs/release/package-channel-readiness-matrix.json")
    per_claim_matrix_text = read_text(repo_root / "docs/release/per-claim-evidence-attachment-matrix.md")
    unsupported_text = read_text(repo_root / "docs/release/known-unsupported-paths.md")
    publication_gate_text = read_text(repo_root / "docs/release/publication-api-schema-stability-gate.md")

    artifact_rows = artifact_refs(provenance, "artifact_refs")
    sbom_rows = artifact_refs(provenance, "sbom_refs")
    checksum_rows = artifact_refs(provenance, "checksum_refs")
    attestation_rows = artifact_refs(provenance, "attestation_refs")

    blockers: list[str] = []
    if provenance_manifest is None:
        blockers.append("missing release provenance dry-run manifest")
    elif provenance_manifest.get("schema_version") != "shardloom.release_provenance_dry_run_manifest.v1":
        blockers.append(f"provenance manifest schema_version={provenance_manifest.get('schema_version')}")
    if provenance is None:
        blockers.append("missing supply-chain release evidence")
    elif provenance.get("schema_version") != "shardloom.supply_chain_release_evidence.v1":
        blockers.append(f"provenance schema_version={provenance.get('schema_version')}")
    if security_report is None:
        blockers.append("missing release security gate report")
    if contribution_report is None:
        blockers.append("missing contribution governance report")
    if golden_workflow_report is None:
        blockers.append("missing golden workflow report")
    if admitted_semantics_report is None:
        blockers.append("missing admitted semantics matrix report")
    if architecture_report is None:
        blockers.append("missing release architecture tracker report")
    if package_report is None:
        blockers.append("missing package-channel readiness report")
    blockers.extend(provenance_status_blockers(provenance))
    blockers.extend(upstream_report_blockers(security_report, "release security"))
    blockers.extend(upstream_report_blockers(contribution_report, "contribution governance"))
    blockers.extend(
        upstream_report_blockers(
            golden_workflow_report,
            "golden workflow",
            status_fields=("status", "golden_workflow_validator_status"),
        )
    )
    if golden_workflow_report is not None:
        if golden_workflow_report.get("schema_version") != "shardloom.golden_workflow_validation_report.v1":
            blockers.append(
                "golden workflow schema_version="
                + str(golden_workflow_report.get("schema_version"))
            )
        if golden_workflow_report.get("workflow_count") != 3:
            blockers.append(
                "golden workflow workflow_count="
                + str(golden_workflow_report.get("workflow_count"))
            )
        stage_count = golden_workflow_report.get("stage_count")
        if not isinstance(stage_count, int) or stage_count < 9:
            blockers.append(
                "golden workflow stage_count="
                + str(golden_workflow_report.get("stage_count"))
            )
        if golden_workflow_report.get("support_matrix_status") != "passed":
            blockers.append(
                "golden workflow support_matrix_status="
                + str(golden_workflow_report.get("support_matrix_status"))
            )
    blockers.extend(
        upstream_report_blockers(
            admitted_semantics_report,
            "admitted semantics",
            status_fields=("status", "admitted_semantics_validator_status"),
        )
    )
    if admitted_semantics_report is not None:
        if admitted_semantics_report.get("schema_version") != "shardloom.admitted_semantics_matrix_report.v1":
            blockers.append(
                "admitted semantics schema_version="
                + str(admitted_semantics_report.get("schema_version"))
            )
        if admitted_semantics_report.get("matrix_status") != "passed":
            blockers.append(
                "admitted semantics matrix_status="
                + str(admitted_semantics_report.get("matrix_status"))
            )
        if admitted_semantics_report.get("property_execution_performed") is not True:
            blockers.append("admitted semantics property_execution_performed missing")
        if admitted_semantics_report.get("decoded_reference_differential_execution_performed") is not True:
            blockers.append(
                "admitted semantics decoded_reference_differential_execution_performed missing"
            )
    blockers.extend(
        upstream_report_blockers(
            architecture_report,
            "architecture tracker",
            status_fields=("status", "architecture_tracker_status"),
        )
    )
    blockers.extend(upstream_report_blockers(package_report, "package channel report"))
    if package_report is not None:
        if package_report.get("local_gate_evidence_required") is not True:
            blockers.append("package channel report must be generated with --require-local-evidence")
        if package_report.get("local_gate_evidence_status") != "passed":
            blockers.append(
                "package channel local_gate_evidence_status="
                + str(package_report.get("local_gate_evidence_status"))
            )

    artifact_missing = ref_paths_exist(repo_root, artifact_rows)
    sbom_missing = ref_paths_exist(repo_root, sbom_rows)
    checksum_missing = ref_paths_exist(repo_root, checksum_rows)
    if artifact_missing:
        blockers.append("missing local package/binary artifacts: " + ",".join(artifact_missing))
    if sbom_missing:
        blockers.append("missing local SBOM artifacts: " + ",".join(sbom_missing))
    if checksum_missing:
        blockers.append("missing checksum artifacts: " + ",".join(checksum_missing))
    if not sbom_rows:
        blockers.append("no SBOM refs attached")
    if not checksum_rows:
        blockers.append("no checksum refs attached")

    blockers.extend(
        false_field_blockers(
            provenance,
            "provenance",
            [
                "publication_attempted",
                "tag_created",
                "secrets_required",
                "external_runtime_dependencies_added",
                "fallback_engine_dependency_added",
            ],
        )
    )
    blockers.extend(
        false_field_blockers(
            security_report,
            "release security",
            ["publication_attempted", "tag_created", "secrets_required", "fallback_attempted", "external_engine_invoked"],
        )
    )
    blockers.extend(
        false_field_blockers(
            contribution_report,
            "contribution governance",
            [
                "public_release_claim_allowed",
                "public_package_claim_allowed",
                "publication_attempted",
                "tag_created",
                "secrets_required",
                "fallback_attempted",
                "external_engine_invoked",
            ],
        )
    )
    blockers.extend(
        false_field_blockers(
            golden_workflow_report,
            "golden workflow",
            [
                "production_claim_allowed",
                "performance_claim_allowed",
                "public_release_claim_allowed",
                "public_package_claim_allowed",
                "package_publication_performed",
                "publication_attempted",
                "tag_created",
                "secrets_required",
                "fallback_attempted",
                "external_engine_invoked",
            ],
        )
    )
    blockers.extend(
        false_field_blockers(
            admitted_semantics_report,
            "admitted semantics",
            [
                "production_claim_allowed",
                "ansi_sql_claim_allowed",
                "performance_claim_allowed",
                "public_release_claim_allowed",
                "public_package_claim_allowed",
                "package_publication_performed",
                "publication_attempted",
                "tag_created",
                "secrets_required",
                "fallback_attempted",
                "external_engine_invoked",
            ],
        )
    )
    blockers.extend(
        false_field_blockers(
            architecture_report,
            "architecture tracker",
            ["publication_attempted", "tag_created", "secrets_required", "fallback_attempted", "external_engine_invoked"],
        )
    )
    blockers.extend(
        false_field_blockers(
            package_report,
            "package channel report",
            ["publication_attempted", "tag_created", "secrets_required", "fallback_attempted", "external_engine_invoked"],
        )
    )

    if package_matrix is None:
        blockers.append("missing package-channel readiness matrix")
    elif package_matrix.get("public_package_release_claim_allowed") is not False:
        blockers.append(
            "package matrix public_package_release_claim_allowed must be false until channel proof"
        )
    if "per_claim_evidence_attachment_matrix_claim_gate_status=not_claim_grade" not in per_claim_matrix_text:
        blockers.append("per-claim matrix not-claim-grade marker missing")
    if "fallback_attempted=false" not in unsupported_text or "external_engine_invoked=false" not in unsupported_text:
        blockers.append("known unsupported no-fallback markers missing")
    if "publication_api_schema_gate_status=blocked" not in publication_gate_text:
        blockers.append("publication/API/schema gate is not visibly blocked")

    attestation_plan = {
        "schema_version": ATTESTATION_PLAN_SCHEMA_VERSION,
        "attestation_plan_status": "local_rehearsal_only",
        "artifact_attestation_refs": attestation_rows,
        "artifact_attestation_count": len(attestation_rows),
        "attestation_generation_status": "not_signed_local_rehearsal",
        "slsa_attestation_status": "not_generated_until_channel_publication_proof",
        "signing_policy_decision": "approved_pending_channel_publication_proof",
        "signing_key_used": False,
        "publication_authorization_status": "approved_pending_channel_proof",
        "publication_human_approval_required": False,
        "publication_human_approved": True,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    write_json(attestation_plan_path, attestation_plan)

    local_rehearsal_complete = not blockers
    status = "passed" if local_rehearsal_complete else "blocked"
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "rehearsal_status": status,
        "claim_gate_status": "not_claim_grade",
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
        "publication_authorization_status": "approved_pending_channel_proof",
        "publication_human_approval_required": False,
        "publication_human_approved": True,
        "local_artifacts_only": True,
        "package_artifact_ref_count": len(artifact_rows),
        "sbom_ref_count": len(sbom_rows),
        "checksum_ref_count": len(checksum_rows),
        "attestation_ref_count": len(attestation_rows),
        "final_attestation_status": "not_signed_local_rehearsal",
        "attestation_plan_ref": rel(repo_root, attestation_plan_path),
        "release_provenance_manifest_ref": rel(repo_root, provenance_manifest_path),
        "release_provenance_report_ref": rel(repo_root, provenance_report_path),
        "release_security_report_ref": rel(repo_root, security_report_path),
        "contribution_governance_report_ref": rel(repo_root, contribution_report_path),
        "golden_workflow_report_ref": rel(repo_root, golden_workflow_report_path),
        "admitted_semantics_report_ref": rel(repo_root, admitted_semantics_report_path),
        "release_architecture_tracker_report_ref": rel(repo_root, architecture_report_path),
        "package_channel_report_ref": rel(repo_root, package_report_path),
        "known_unsupported_paths_ref": "docs/release/known-unsupported-paths.md",
        "per_claim_evidence_matrix_ref": "docs/release/per-claim-evidence-attachment-matrix.md",
        "publication_api_schema_stability_gate_ref": "docs/release/publication-api-schema-stability-gate.md",
        "blockers": blockers,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "package_upload_attempted": False,
        "feedstock_submission_attempted": False,
        "marketplace_submission_attempted": False,
        "signing_key_used": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    write_json(output, report)
    print(output)
    return 0 if local_rehearsal_complete or args.allow_blocked else 1


if __name__ == "__main__":
    raise SystemExit(main())
