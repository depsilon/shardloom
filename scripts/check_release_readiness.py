#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Aggregate hard release-readiness evidence for ShardLoom.

The gate requires feature/build matrix execution evidence, not only matrix documentation.
The gate also consumes the package-channel readiness matrix so public package claims cannot pass
without channel-specific install, smoke, provenance, and rollback evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from check_benchmark_artifact_completeness import (
    REPORT_SCHEMA_VERSION as BENCHMARK_COMPLETENESS_REPORT_SCHEMA_VERSION,
)
from check_benchmark_artifact_completeness import (
    validate_manifest as validate_benchmark_artifact_completeness,
)
from check_benchmark_publication_claim_gate import (
    SCHEMA_VERSION as BENCHMARK_PUBLICATION_CLAIM_REPORT_SCHEMA_VERSION,
)
from check_benchmark_publication_claim_gate import (
    validate_publication_claim_gate as validate_benchmark_publication_claim_gate,
)
from check_front_door_benchmark_publication import (
    SCHEMA_VERSION as FRONT_DOOR_BENCHMARK_PUBLICATION_SCHEMA_VERSION,
)
from check_runtime_execution_envelopes import (
    validate_repo as validate_runtime_execution_envelope_surfaces,
)
from check_package_channel_readiness import (
    validate_local_gate_evidence as validate_package_local_gate_evidence,
)
from check_package_channel_readiness import validate_matrix as validate_package_channel_matrix


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.hard_release_readiness_gate.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--release-dry-run-transcript",
        type=Path,
        default=Path("target/release-dry-run-proof/transcript.json"),
    )
    parser.add_argument(
        "--security-gate-report",
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
        "--validation-evidence",
        type=Path,
        default=Path("target/release-validation-evidence.json"),
    )
    parser.add_argument(
        "--package-channel-matrix",
        type=Path,
        default=Path("docs/release/package-channel-readiness-matrix.json"),
    )
    parser.add_argument(
        "--package-channel-report",
        type=Path,
        default=Path("target/package-channel-readiness-report.json"),
    )
    parser.add_argument(
        "--per-claim-evidence-matrix",
        type=Path,
        default=Path("docs/release/per-claim-evidence-attachment-matrix.md"),
    )
    parser.add_argument(
        "--architecture-tracker-report",
        type=Path,
        default=Path("target/release-architecture-tracker-report.json"),
    )
    parser.add_argument(
        "--final-release-rehearsal-report",
        type=Path,
        default=Path("target/final-release-rehearsal/final-release-rehearsal-report.json"),
    )
    parser.add_argument(
        "--production-usability-report",
        type=Path,
        default=Path("target/production-usability-gate.json"),
    )
    parser.add_argument(
        "--python-user-surface-report",
        type=Path,
        default=Path("target/python-user-surface-completion-gate.json"),
    )
    parser.add_argument(
        "--sql-python-dataframe-parity-report",
        type=Path,
        default=Path("target/sql-python-dataframe-parity-gate.json"),
    )
    parser.add_argument(
        "--user-surface-runtime-gap-inventory-report",
        type=Path,
        default=Path("target/user-surface-runtime-gap-inventory.json"),
    )
    parser.add_argument(
        "--user-surface-graduation-matrix-report",
        type=Path,
        default=Path("target/user-surface-graduation-matrix.json"),
    )
    parser.add_argument(
        "--runtime-gap-family-burn-down-report",
        type=Path,
        default=Path("target/runtime-gap-family-burn-down.json"),
    )
    parser.add_argument(
        "--user-route-capability-report",
        type=Path,
        default=Path("target/user-route-capability-report.json"),
    )
    parser.add_argument(
        "--pre-5j-dependency-report",
        type=Path,
        default=Path("target/pre-5j-dependency-freshness-gate.json"),
    )
    parser.add_argument(
        "--benchmark-completeness-report",
        type=Path,
        default=Path("target/benchmark-artifact-completeness-report.json"),
    )
    parser.add_argument(
        "--benchmark-publication-claim-report",
        type=Path,
        default=Path("target/benchmark-publication-claim-gate-report.json"),
    )
    parser.add_argument(
        "--front-door-benchmark-publication-report",
        type=Path,
        default=Path("target/front-door-benchmark-publication-gate.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/hard-release-readiness-gate.json"),
    )
    parser.add_argument("--allow-blocked", action="store_true")
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def file_sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def benchmark_artifact_json_path(manifest: dict[str, Any] | None, repo_root: Path) -> Path | None:
    if not isinstance(manifest, dict):
        return None
    artifact_paths = manifest.get("artifact_paths")
    if not isinstance(artifact_paths, dict):
        return None
    path_text = artifact_paths.get("json")
    if not path_text:
        return None
    path = Path(str(path_text))
    return path if path.is_absolute() else repo_root / path


def check(name: str, ref: str, blockers: list[str]) -> dict[str, Any]:
    return {"name": name, "ref": ref, "status": "passed" if not blockers else "blocked", "blockers": blockers}


def validation_command_passed(command_status: dict[Any, Any], expected: str) -> bool:
    if command_status.get(expected) == "passed":
        return True
    prefix_allowed = {
        "python scripts/release_dry_run_proof.py --rows 64 --iterations 1",
        "python scripts/check_pre_5j_dependency_freshness.py",
    }
    if expected in prefix_allowed:
        return any(
            isinstance(command, str)
            and (command == expected or command.startswith(expected + " "))
            and status == "passed"
            for command, status in command_status.items()
        )
    return False


def runtime_gap_family_burn_down_blockers(
    runtime_gap_family_burn_down: dict[str, Any] | None,
) -> list[str]:
    blockers: list[str] = []
    if runtime_gap_family_burn_down is None:
        blockers.append("missing runtime gap family burn-down report")
        return blockers
    if (
        runtime_gap_family_burn_down.get("schema_version")
        != "shardloom.runtime_gap_family_burn_down.v1"
    ):
        blockers.append(
            "runtime gap family burn-down schema_version="
            + str(runtime_gap_family_burn_down.get("schema_version", "missing"))
        )
    if runtime_gap_family_burn_down.get("status") != "passed":
        blockers.extend(
            runtime_gap_family_burn_down.get(
                "blockers", ["runtime gap family burn-down blocked"]
            )
        )
    global_review_unchecked_count = runtime_gap_family_burn_down.get(
        "global_review_unchecked_count"
    )
    mapped_gap_count = runtime_gap_family_burn_down.get("mapped_gap_count")
    if not isinstance(global_review_unchecked_count, int):
        blockers.append(
            "runtime gap family burn-down global_review_unchecked_count="
            + str(
                global_review_unchecked_count
                if global_review_unchecked_count is not None
                else "missing"
            )
        )
    if not isinstance(mapped_gap_count, int):
        blockers.append(
            "runtime gap family burn-down mapped_gap_count="
            + str(mapped_gap_count if mapped_gap_count is not None else "missing")
        )
    if (
        isinstance(global_review_unchecked_count, int)
        and isinstance(mapped_gap_count, int)
        and mapped_gap_count != global_review_unchecked_count
    ):
        blockers.append(
            "runtime gap family burn-down mapped_gap_count does not match global_review_unchecked_count: "
            + f"{mapped_gap_count} != {global_review_unchecked_count}"
        )
    acceptance = runtime_gap_family_burn_down.get("acceptance_summary")
    if not isinstance(acceptance, dict):
        blockers.append("runtime gap family burn-down missing acceptance_summary")
    else:
        for field in [
            "all_unchecked_global_review_rows_mapped",
            "all_families_have_phase_items",
            "all_families_have_evidence_and_validators",
            "all_no_fallback_invariants_named",
            "all_claim_boundaries_named",
        ]:
            if acceptance.get(field) is not True:
                blockers.append(f"runtime gap family burn-down {field} must be true")
    for field in [
        "fallback_attempted",
        "external_engine_invoked",
        "runtime_support_claim_allowed",
        "performance_claim_allowed",
        "production_claim_allowed",
    ]:
        if runtime_gap_family_burn_down.get(field) is not False:
            blockers.append(f"runtime gap family burn-down {field} must be false")
    if runtime_gap_family_burn_down.get("claim_gate_status") != "not_claim_grade":
        blockers.append(
            "runtime gap family burn-down claim_gate_status="
            + str(runtime_gap_family_burn_down.get("claim_gate_status", "missing"))
        )
    return blockers


def benchmark_completeness_report_blockers(
    report: dict[str, Any] | None,
    *,
    manifest_ref: str,
    manifest_path: Path | None = None,
    repo_root: Path | None = None,
) -> list[str]:
    blockers: list[str] = []
    if report is None:
        return blockers
    if report.get("schema_version") != BENCHMARK_COMPLETENESS_REPORT_SCHEMA_VERSION:
        blockers.append("benchmark completeness report schema mismatch")
    if str(report.get("manifest") or "").replace("\\", "/") != manifest_ref:
        blockers.append(
            "benchmark completeness report manifest mismatch: "
            + str(report.get("manifest", "missing"))
        )
    if manifest_path is not None and manifest_path.exists():
        expected_manifest_digest = file_sha256(manifest_path)
        if report.get("manifest_sha256") != expected_manifest_digest:
            blockers.append("benchmark completeness report manifest digest mismatch")
        manifest = load_json(manifest_path)
        artifact_path = benchmark_artifact_json_path(
            manifest,
            repo_root or manifest_path.parent,
        )
        if artifact_path is not None and artifact_path.exists():
            expected_artifact_digest = file_sha256(artifact_path)
            if report.get("artifact_json_sha256") != expected_artifact_digest:
                blockers.append("benchmark completeness report artifact digest mismatch")
    report_blockers = report.get("blockers")
    if not isinstance(report_blockers, list):
        blockers.append("benchmark completeness report blockers must be a list")
    else:
        blockers.extend(f"benchmark artifact completeness: {item}" for item in report_blockers)
    if report.get("status") != "passed":
        blockers.append(
            "benchmark completeness report status="
            + str(report.get("status", "missing"))
        )
    if report.get("performance_claim_allowed") is not False:
        blockers.append("benchmark completeness performance_claim_allowed must be false")
    for field in [
        "benchmark_run_performed",
        "fallback_attempted",
        "external_engine_invoked",
    ]:
        if report.get(field) is not False:
            blockers.append(f"benchmark completeness {field} must be false")
    return blockers


def benchmark_publication_claim_report_blockers(
    report: dict[str, Any] | None,
    *,
    manifest_ref: str,
) -> list[str]:
    blockers: list[str] = []
    if report is None:
        return blockers
    if report.get("schema_version") != BENCHMARK_PUBLICATION_CLAIM_REPORT_SCHEMA_VERSION:
        blockers.append("benchmark publication claim report schema mismatch")
    if str(report.get("manifest") or "").replace("\\", "/") != manifest_ref:
        blockers.append(
            "benchmark publication claim report manifest mismatch: "
            + str(report.get("manifest", "missing"))
        )
    report_blockers = report.get("blockers")
    if not isinstance(report_blockers, list):
        blockers.append("benchmark publication claim report blockers must be a list")
    else:
        blockers.extend(f"benchmark publication claim gate: {item}" for item in report_blockers)
    if report.get("status") != "passed":
        blockers.append(
            "benchmark publication claim report status="
            + str(report.get("status", "missing"))
        )
    for field in ["benchmark_run_performed", "fallback_attempted", "external_engine_invoked"]:
        if report.get(field) is not False:
            blockers.append(f"benchmark publication claim {field} must be false")
    return blockers


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    dry_run_path = resolve(repo_root, args.release_dry_run_transcript)
    security_gate_path = resolve(repo_root, args.security_gate_report)
    contribution_governance_path = resolve(repo_root, args.contribution_governance_report)
    golden_workflow_path = resolve(repo_root, args.golden_workflow_report)
    admitted_semantics_path = resolve(repo_root, args.admitted_semantics_report)
    validation_evidence_path = resolve(repo_root, args.validation_evidence)
    package_channel_matrix_path = resolve(repo_root, args.package_channel_matrix)
    package_channel_report_path = resolve(repo_root, args.package_channel_report)
    per_claim_evidence_matrix_path = resolve(repo_root, args.per_claim_evidence_matrix)
    architecture_tracker_report_path = resolve(repo_root, args.architecture_tracker_report)
    final_release_rehearsal_report_path = resolve(repo_root, args.final_release_rehearsal_report)
    production_usability_report_path = resolve(repo_root, args.production_usability_report)
    python_user_surface_report_path = resolve(repo_root, args.python_user_surface_report)
    sql_python_dataframe_parity_report_path = resolve(
        repo_root,
        args.sql_python_dataframe_parity_report,
    )
    user_surface_runtime_gap_inventory_path = resolve(
        repo_root,
        args.user_surface_runtime_gap_inventory_report,
    )
    user_surface_graduation_matrix_path = resolve(
        repo_root,
        args.user_surface_graduation_matrix_report,
    )
    runtime_gap_family_burn_down_path = resolve(
        repo_root,
        args.runtime_gap_family_burn_down_report,
    )
    user_route_capability_report_path = resolve(repo_root, args.user_route_capability_report)
    pre_5j_dependency_report_path = resolve(repo_root, args.pre_5j_dependency_report)
    benchmark_completeness_report_path = resolve(
        repo_root,
        args.benchmark_completeness_report,
    )
    benchmark_publication_claim_report_path = resolve(
        repo_root,
        args.benchmark_publication_claim_report,
    )
    front_door_benchmark_publication_report_path = resolve(
        repo_root,
        args.front_door_benchmark_publication_report,
    )

    checks: list[dict[str, Any]] = []

    dry_run = load_json(dry_run_path)
    dry_run_blockers: list[str] = []
    if dry_run is None:
        dry_run_blockers.append("missing release dry-run transcript")
    else:
        if dry_run.get("proof_status") != "passed":
            dry_run_blockers.append(f"proof_status={dry_run.get('proof_status')}")
        for field in [
            "publication_attempted",
            "tag_created",
            "secrets_required",
            "external_runtime_dependencies_added",
            "fallback_engine_dependency_added",
        ]:
            if dry_run.get(field) is not False:
                dry_run_blockers.append(f"{field} must be false")
        if dry_run.get("provenance_dry_run_performed") is not True:
            dry_run_blockers.append("provenance_dry_run_performed missing")
        if dry_run.get("sbom_checksum_manifest_generated") is not True:
            dry_run_blockers.append("sbom_checksum_manifest_generated missing")
        if dry_run.get("clean_conda_env_install_status") != "passed":
            dry_run_blockers.append(
                f"clean_conda_env_install_status={dry_run.get('clean_conda_env_install_status', 'missing')}"
            )
    checks.append(
        check(
            "clean_install_first_10_minutes_and_benchmark_smoke",
            str(args.release_dry_run_transcript).replace("\\", "/"),
            dry_run_blockers,
        )
    )

    security_gate = load_json(security_gate_path)
    security_blockers: list[str] = []
    if security_gate is None:
        security_blockers.append("missing release security gate report")
    else:
        if security_gate.get("status") != "passed":
            security_blockers.extend(security_gate.get("blockers", ["security gate blocked"]))
        if security_gate.get("fallback_attempted") is not False:
            security_blockers.append("security gate fallback_attempted must be false")
        if security_gate.get("external_engine_invoked") is not False:
            security_blockers.append("security gate external_engine_invoked must be false")
    checks.append(
        check(
            "security_dependency_provenance_and_known_unsupported_paths",
            str(args.security_gate_report).replace("\\", "/"),
            security_blockers,
        )
    )

    contribution_governance = load_json(contribution_governance_path)
    contribution_blockers: list[str] = []
    if contribution_governance is None:
        contribution_blockers.append("missing contribution governance report")
    else:
        if (
            contribution_governance.get("schema_version")
            != "shardloom.contribution_governance_report.v1"
        ):
            contribution_blockers.append(
                "contribution governance schema_version="
                + str(contribution_governance.get("schema_version", "missing"))
            )
        if contribution_governance.get("status") != "passed":
            contribution_blockers.extend(
                contribution_governance.get("blockers", ["contribution governance blocked"])
            )
        for field in [
            "public_release_claim_allowed",
            "public_package_claim_allowed",
            "publication_attempted",
            "tag_created",
            "secrets_required",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if contribution_governance.get(field) is not False:
                contribution_blockers.append(f"contribution governance {field} must be false")
        for field, expected in [
            ("external_contribution_acceptance_status", "maintainer_approval_required"),
            ("cla_assistant_status", "not_active"),
            ("dco_policy_status", "not_active"),
            ("legal_claim_status", "documented_policy_only"),
        ]:
            if contribution_governance.get(field) != expected:
                contribution_blockers.append(
                    f"contribution governance {field}="
                    + str(contribution_governance.get(field, "missing"))
                )
    checks.append(
        check(
            "contribution_governance_intake_gate",
            str(args.contribution_governance_report).replace("\\", "/"),
            contribution_blockers,
        )
    )

    golden_workflow = load_json(golden_workflow_path)
    golden_workflow_blockers: list[str] = []
    if golden_workflow is None:
        golden_workflow_blockers.append("missing golden workflow report")
    else:
        if (
            golden_workflow.get("schema_version")
            != "shardloom.golden_workflow_validation_report.v1"
        ):
            golden_workflow_blockers.append(
                "golden workflow schema_version="
                + str(golden_workflow.get("schema_version", "missing"))
            )
        if golden_workflow.get("status") != "passed":
            golden_workflow_blockers.extend(
                golden_workflow.get("blockers", ["golden workflow validator failed"])
            )
        if golden_workflow.get("workflow_count") != 3:
            golden_workflow_blockers.append(
                "golden workflow workflow_count="
                + str(golden_workflow.get("workflow_count", "missing"))
            )
        stage_count = golden_workflow.get("stage_count")
        if not isinstance(stage_count, int) or stage_count < 9:
            golden_workflow_blockers.append(
                "golden workflow stage_count="
                + str(golden_workflow.get("stage_count", "missing"))
            )
        if golden_workflow.get("support_matrix_status") != "passed":
            golden_workflow_blockers.append(
                "golden workflow support_matrix_status="
                + str(golden_workflow.get("support_matrix_status", "missing"))
            )
        required_workflows = {
            "local_csv_jsonl_to_vortex_ingest_prepared_query_jsonl_csv_output",
            "generated_source_to_local_vortex_output_replay_fidelity",
            "prepared_native_vortex_count_filter_project_execution_certificates",
        }
        observed_workflows = set(golden_workflow.get("workflow_ids", []))
        missing_workflows = sorted(required_workflows - observed_workflows)
        if missing_workflows:
            golden_workflow_blockers.append(
                "golden workflow missing workflow ids: " + ",".join(missing_workflows)
            )
        for field in [
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
        ]:
            if golden_workflow.get(field) is not False:
                golden_workflow_blockers.append(f"golden workflow {field} must be false")
    checks.append(
        check(
            "golden_workflow_validator",
            str(args.golden_workflow_report).replace("\\", "/"),
            golden_workflow_blockers,
        )
    )

    admitted_semantics = load_json(admitted_semantics_path)
    admitted_semantics_blockers: list[str] = []
    if admitted_semantics is None:
        admitted_semantics_blockers.append("missing admitted semantics matrix report")
    else:
        if (
            admitted_semantics.get("schema_version")
            != "shardloom.admitted_semantics_matrix_report.v1"
        ):
            admitted_semantics_blockers.append(
                "admitted semantics schema_version="
                + str(admitted_semantics.get("schema_version", "missing"))
            )
        if admitted_semantics.get("status") != "passed":
            admitted_semantics_blockers.extend(
                admitted_semantics.get("blockers", ["admitted semantics validator failed"])
            )
        if admitted_semantics.get("matrix_status") != "passed":
            admitted_semantics_blockers.append(
                "admitted semantics matrix_status="
                + str(admitted_semantics.get("matrix_status", "missing"))
            )
        if admitted_semantics.get("property_execution_performed") is not True:
            admitted_semantics_blockers.append("admitted semantics property_execution_performed missing")
        if admitted_semantics.get("decoded_reference_differential_execution_performed") is not True:
            admitted_semantics_blockers.append(
                "admitted semantics decoded_reference_differential_execution_performed missing"
            )
        if admitted_semantics.get("executable_fixture_count") != 103:
            admitted_semantics_blockers.append(
                "admitted semantics executable_fixture_count="
                + str(admitted_semantics.get("executable_fixture_count", "missing"))
            )
        if admitted_semantics.get("diagnostic_case_count") != 24:
            admitted_semantics_blockers.append(
                "admitted semantics diagnostic_case_count="
                + str(admitted_semantics.get("diagnostic_case_count", "missing"))
            )
        if admitted_semantics.get("unsupported_diagnostic_count") != 22:
            admitted_semantics_blockers.append(
                "admitted semantics unsupported_diagnostic_count="
                + str(admitted_semantics.get("unsupported_diagnostic_count", "missing"))
            )
        if admitted_semantics.get("runtime_error_diagnostic_count") != 1:
            admitted_semantics_blockers.append(
                "admitted semantics runtime_error_diagnostic_count="
                + str(admitted_semantics.get("runtime_error_diagnostic_count", "missing"))
            )
        if admitted_semantics.get("invalid_shape_diagnostic_count") != 1:
            admitted_semantics_blockers.append(
                "admitted semantics invalid_shape_diagnostic_count="
                + str(admitted_semantics.get("invalid_shape_diagnostic_count", "missing"))
            )
        if admitted_semantics.get("semantic_conformance_suite_status") != "passed":
            admitted_semantics_blockers.append(
                "admitted semantics semantic_conformance_suite_status="
                + str(admitted_semantics.get("semantic_conformance_suite_status", "missing"))
            )
        if admitted_semantics.get("correctness_harness_boundary_status") != "passed":
            admitted_semantics_blockers.append(
                "admitted semantics correctness_harness_boundary_status="
                + str(admitted_semantics.get("correctness_harness_boundary_status", "missing"))
            )
        for field in [
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
        ]:
            if admitted_semantics.get(field) is not False:
                admitted_semantics_blockers.append(f"admitted semantics {field} must be false")
    checks.append(
        check(
            "admitted_semantics_matrix_validator",
            str(args.admitted_semantics_report).replace("\\", "/"),
            admitted_semantics_blockers,
        )
    )

    pyproject = read_text(repo_root / "python/pyproject.toml")
    metadata_blockers = []
    for required in [
        "name = \"shardloom\"",
        "license = \"Apache-2.0\"",
        "Homepage = \"https://shardloom.io\"",
        "Repository = \"https://github.com/depsilon/shardloom\"",
    ]:
        if required not in pyproject:
            metadata_blockers.append(f"missing Python metadata: {required}")
    cargo = read_text(repo_root / "Cargo.toml")
    for required in [
        "license = \"Apache-2.0\"",
        "homepage = \"https://shardloom.io\"",
        "repository = \"https://github.com/depsilon/shardloom\"",
    ]:
        if required not in cargo:
            metadata_blockers.append(f"missing Cargo metadata: {required}")
    checks.append(check("package_metadata_license_and_discoverability", "Cargo.toml python/pyproject.toml", metadata_blockers))

    package_channel_matrix = load_json(package_channel_matrix_path)
    package_channel_blockers = validate_package_channel_matrix(package_channel_matrix)
    package_local_gate_evidence = validate_package_local_gate_evidence(
        repo_root=repo_root,
        dependency_audit_report=load_json(repo_root / "target/dependency-audit-report.json"),
        release_dry_run_transcript=dry_run,
        provenance_report=load_json(
            repo_root / "target/release-provenance-dry-run/supply-chain-release-evidence.json"
        ),
    )
    package_channel_blockers.extend(package_local_gate_evidence["blockers"])
    package_channel_report = load_json(package_channel_report_path)
    if package_channel_report is None:
        package_channel_blockers.append("missing package-channel readiness report")
    else:
        if package_channel_report.get("schema_version") != "shardloom.package_channel_readiness_report.v1":
            package_channel_blockers.append("package-channel report schema_version mismatch")
        if package_channel_report.get("local_gate_evidence_status") != "passed":
            package_channel_blockers.append(
                "package-channel local_gate_evidence_status="
                + str(package_channel_report.get("local_gate_evidence_status"))
            )
        for field in [
            "publication_attempted",
            "tag_created",
            "secrets_required",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if package_channel_report.get(field) is not False:
                package_channel_blockers.append(f"package-channel report {field} must be false")
    if package_channel_matrix is not None:
        if package_channel_matrix.get("public_package_release_claim_allowed") is not True:
            package_channel_blockers.append("public_package_release_claim_allowed=false")
        channels = package_channel_matrix.get("channels", [])
        blocked_channels = [
            row.get("channel_id", "unknown")
            for row in channels
            if isinstance(row, dict) and row.get("ready") is not True
        ]
        if blocked_channels:
            package_channel_blockers.append(
                "package channels not ready: "
                + ", ".join(str(channel) for channel in blocked_channels)
            )
    checks.append(
        check(
            "package_channel_readiness_matrix",
            str(args.package_channel_matrix).replace("\\", "/"),
            package_channel_blockers,
        )
    )

    api_schema_gate_doc = repo_root / "docs/release/publication-api-schema-stability-gate.md"
    api_schema_gate_blockers: list[str] = []
    api_schema_gate_text = read_text(api_schema_gate_doc)
    for required in [
        "shardloom.publication_api_schema_stability_gate.v1",
        "publication_api_schema_gate_status=blocked",
        "claim_gate_status=not_claim_grade",
        "api_compatibility_window",
        "schema_compatibility_window",
        "package_identity_approval",
        "signing_policy_decision",
        "checksum_manifest",
        "sbom_bundle",
        "publication_approval",
        "public_release_claim_allowed=false",
        "public_package_claim_allowed=false",
        "package_publication_performed=false",
        "tag_created=false",
        "signing_key_used=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ]:
        if required not in api_schema_gate_text:
            api_schema_gate_blockers.append(f"missing publication/API/schema gate field: {required}")
    if "publication_api_schema_gate_status=blocked" in api_schema_gate_text:
        api_schema_gate_blockers.append("publication/API/schema stability gate remains blocked")
    checks.append(
        check(
            "publication_api_schema_stability_gate",
            "docs/release/publication-api-schema-stability-gate.md",
            api_schema_gate_blockers,
        )
    )

    per_claim_matrix_text = read_text(per_claim_evidence_matrix_path)
    per_claim_matrix_blockers: list[str] = []
    for required in [
        "shardloom.per_claim_evidence_attachment_matrix.v1",
        "per_claim_evidence_attachment_matrix_support_status=blocked",
        "per_claim_evidence_attachment_matrix_claim_gate_status=not_claim_grade",
        "per_claim_evidence_attachment_matrix_all_claims_blocked=true",
        "per_claim_evidence_attachment_matrix_public_release_claim_allowed=false",
        "per_claim_evidence_attachment_matrix_public_package_claim_allowed=false",
        "per_claim_evidence_attachment_matrix_performance_claim_allowed=false",
        "per_claim_evidence_attachment_matrix_performance_superiority_claim_allowed=false",
        "per_claim_evidence_attachment_matrix_spark_displacement_claim_allowed=false",
        "per_claim_evidence_attachment_matrix_engine_replacement_claim_allowed=false",
        "per_claim_evidence_attachment_matrix_required_v1_row_count=7",
        "per_claim_evidence_attachment_matrix_out_of_v1_row_count=6",
        "per_claim_evidence_attachment_matrix_external_baseline_context_allowed=true",
        "per_claim_evidence_attachment_matrix_fallback_attempted=false",
        "per_claim_evidence_attachment_matrix_external_engine_invoked=false",
        "public_release_claim",
        "public_package_claim",
        "local_runtime_product_claim",
        "api_schema_stability_claim",
        "supported_front_door_scope_claim",
        "supported_vortex_route_claim",
        "supported_output_sink_claim",
        "security_supply_chain_claim",
        "external_baseline_comparison_claim",
        "performance_superiority_claim",
        "spark_displacement_claim",
        "engine_replacement_claim",
        "production_sql_dataframe_claim",
        "object_store_lakehouse_claim",
        "foundry_platform_claim",
        "required_test_evidence",
        "required_benchmark_evidence",
        "required_certificate_evidence",
        "required_native_io_evidence",
        "required_security_evidence",
        "required_provenance_evidence",
        "required_unsupported_path_evidence",
        "required_no_fallback_evidence",
        "required_release_approval",
    ]:
        if required not in per_claim_matrix_text:
            per_claim_matrix_blockers.append(f"missing per-claim evidence matrix field: {required}")
    if "per_claim_evidence_attachment_matrix_claim_gate_status=not_claim_grade" in per_claim_matrix_text:
        per_claim_matrix_blockers.append("per-claim evidence attachment matrix remains not claim-grade")
    checks.append(
        check(
            "per_claim_evidence_attachment_matrix",
            str(args.per_claim_evidence_matrix).replace("\\", "/"),
            per_claim_matrix_blockers,
        )
    )

    architecture_tracker = load_json(architecture_tracker_report_path)
    architecture_tracker_blockers: list[str] = []
    if architecture_tracker is None:
        architecture_tracker_blockers.append("missing release architecture tracker report")
    else:
        if architecture_tracker.get("schema_version") != "shardloom.release_architecture_tracker_report.v1":
            architecture_tracker_blockers.append(
                f"schema_version={architecture_tracker.get('schema_version', 'missing')}"
            )
        if architecture_tracker.get("architecture_tracker_status") != "passed":
            architecture_tracker_blockers.append(
                f"architecture_tracker_status={architecture_tracker.get('architecture_tracker_status', 'missing')}"
            )
        if architecture_tracker.get("status") != "passed":
            architecture_tracker_blockers.extend(
                architecture_tracker.get("blockers", ["release architecture tracker blocked"])
            )
            for field in ["public_release_claim_allowed", "public_package_claim_allowed"]:
                if architecture_tracker.get(field) is not False:
                    architecture_tracker_blockers.append(f"blocked architecture tracker {field} must be false")
        for field in [
            "publication_attempted",
            "tag_created",
            "secrets_required",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            expected = False
            if architecture_tracker.get(field) is not expected:
                architecture_tracker_blockers.append(f"architecture tracker {field} must be false")
    checks.append(
        check(
            "architecture_tracker_validation",
            str(args.architecture_tracker_report).replace("\\", "/"),
            architecture_tracker_blockers,
        )
    )

    final_release_rehearsal = load_json(final_release_rehearsal_report_path)
    final_rehearsal_blockers: list[str] = []
    if final_release_rehearsal is None:
        final_rehearsal_blockers.append("missing final release rehearsal report")
    else:
        if final_release_rehearsal.get("schema_version") != "shardloom.final_release_rehearsal_report.v1":
            final_rehearsal_blockers.append(
                f"schema_version={final_release_rehearsal.get('schema_version', 'missing')}"
            )
        if final_release_rehearsal.get("rehearsal_status") != "passed":
            final_rehearsal_blockers.append(
                f"rehearsal_status={final_release_rehearsal.get('rehearsal_status', 'missing')}"
            )
            final_rehearsal_blockers.extend(
                final_release_rehearsal.get("blockers", ["final release rehearsal blocked"])
            )
        for field in [
            "public_release_claim_allowed",
            "public_package_claim_allowed",
            "publication_human_approved",
            "publication_attempted",
            "tag_created",
            "secrets_required",
            "package_upload_attempted",
            "feedstock_submission_attempted",
            "marketplace_submission_attempted",
            "signing_key_used",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if final_release_rehearsal.get(field) is not False:
                final_rehearsal_blockers.append(f"final rehearsal {field} must be false")
    checks.append(
        check(
            "final_release_rehearsal",
            str(args.final_release_rehearsal_report).replace("\\", "/"),
            final_rehearsal_blockers,
        )
    )

    feature_matrix_doc = repo_root / "docs/architecture/workspace-feature-build-matrix.md"
    feature_blockers = []
    matrix_text = read_text(feature_matrix_doc)
    for required in [
        "WorkspaceFeatureBuildMatrixReport",
        "default_features",
        "all_features",
        "no_default_features",
        "vortex_local_primitives",
        "benchmark_extras",
    ]:
        if required not in matrix_text:
            feature_blockers.append(f"missing feature matrix field: {required}")
    validation_evidence = load_json(validation_evidence_path)
    if validation_evidence is None:
        feature_blockers.append("missing release validation evidence")
    elif validation_evidence.get("feature_build_matrix_status") != "passed":
        feature_blockers.append(
            f"feature_build_matrix_status={validation_evidence.get('feature_build_matrix_status')}"
        )
        for row in validation_evidence.get("feature_build_matrix_rows", []):
            if row.get("release_blocking"):
                feature_blockers.append(f"{row.get('feature_set')}: {row.get('status')}")
    checks.append(
        check(
            "feature_build_matrix",
            str(args.validation_evidence).replace("\\", "/"),
            feature_blockers,
        )
    )

    typed_contracts = repo_root / "shardloom-cli/tests/typed_envelope_contract_snapshots.rs"
    typed_blockers = []
    if not typed_contracts.exists():
        typed_blockers.append("missing typed envelope contract snapshots")
    typed_envelope_doc = read_text(repo_root / "docs/architecture/typed-command-result-envelope.md").lower()
    if "typed envelope" not in typed_envelope_doc and "typed-envelope" not in typed_envelope_doc:
        typed_blockers.append("missing typed envelope architecture doc")
    evidence_schema_registry = repo_root / "shardloom-cli/src/evidence_schema_registry.rs"
    evidence_schema_doc = repo_root / "docs/status/evidence-field-schema-registry.md"
    evidence_schema_validator = repo_root / "scripts/check_evidence_schema_registry.py"
    runtime_envelope_status_doc = repo_root / "docs/status/runtime-execution-envelope-validation.md"
    runtime_envelope_status_json = repo_root / "docs/status/runtime-execution-envelope-validation.json"
    runtime_envelope_validator = repo_root / "scripts/check_runtime_execution_envelopes.py"
    if not evidence_schema_registry.exists():
        typed_blockers.append("missing evidence schema registry source")
    if "shardloom.evidence_field_schema_registry.v1" not in read_text(evidence_schema_doc):
        typed_blockers.append("missing evidence schema registry status doc")
    if not evidence_schema_validator.exists():
        typed_blockers.append("missing evidence schema registry validator script")
    if "shardloom.runtime_execution_envelope_validation.v1" not in read_text(runtime_envelope_status_doc):
        typed_blockers.append("missing runtime execution envelope validation status doc")
    runtime_envelope_status = load_json(runtime_envelope_status_json)
    if runtime_envelope_status is None:
        typed_blockers.append("missing runtime execution envelope validation status json")
    elif (
        runtime_envelope_status.get("validator_schema_version")
        != "shardloom.runtime_execution_envelope_validation.v1"
    ):
        typed_blockers.append("runtime execution envelope validator schema mismatch")
    else:
        for surface in [
            "runtime_envelope_fixtures",
            "website_published_benchmark_rows",
            "runs_today_support_matrix",
        ]:
            if surface not in (runtime_envelope_status.get("validated_surfaces") or []):
                typed_blockers.append(
                    f"runtime execution envelope status missing surface: {surface}"
                )
    if not runtime_envelope_validator.exists():
        typed_blockers.append("missing runtime execution envelope validator script")
    runtime_envelope_report = validate_runtime_execution_envelope_surfaces(repo_root)
    if runtime_envelope_report.get("status") != "passed":
        for blocker in runtime_envelope_report.get("blockers", []):
            typed_blockers.append(f"runtime execution envelope: {blocker}")
    checks.append(check("typed_envelope_compatibility", "shardloom-cli/tests/typed_envelope_contract_snapshots.rs", typed_blockers))

    benchmark_constitution_script = repo_root / "scripts/check_benchmark_constitution.py"
    benchmark_constitution_doc = repo_root / "docs/architecture/benchmark-constitution.md"
    benchmark_constitution_source = repo_root / "shardloom-core/src/benchmark.rs"
    benchmark_manifest_path = repo_root / "website/assets/benchmarks/latest/manifest.json"
    benchmark_manifest_ref = "website/assets/benchmarks/latest/manifest.json"
    benchmark_manifest = load_json(benchmark_manifest_path)
    benchmark_completeness_report = load_json(benchmark_completeness_report_path)
    benchmark_publication_claim_report = load_json(
        benchmark_publication_claim_report_path
    )
    benchmark_constitution_blockers: list[str] = []
    if not benchmark_constitution_script.exists():
        benchmark_constitution_blockers.append("missing benchmark constitution validator script")
    if "shardloom.benchmark_constitution_validation.v1" not in read_text(benchmark_constitution_doc):
        benchmark_constitution_blockers.append("missing benchmark constitution architecture doc")
    for required in [
        "BenchmarkConstitutionValidationReport",
        "BenchmarkConstitutionValidationRow",
        "plan_benchmark_constitution_validation",
        "benchmark_constitution_validation_from_parts",
    ]:
        if required not in read_text(benchmark_constitution_source):
            benchmark_constitution_blockers.append(f"missing benchmark constitution source marker: {required}")
    if benchmark_manifest is None:
        benchmark_constitution_blockers.append("missing website benchmark manifest")
    else:
        precomputed_completeness_blockers = benchmark_completeness_report_blockers(
            benchmark_completeness_report,
            manifest_ref=benchmark_manifest_ref,
            manifest_path=benchmark_manifest_path,
            repo_root=repo_root,
        )
        if benchmark_completeness_report is not None:
            benchmark_constitution_blockers.extend(precomputed_completeness_blockers)
        else:
            completeness_blockers, _ = validate_benchmark_artifact_completeness(
                benchmark_manifest_path,
                allow_incomplete=False,
            )
            for blocker in completeness_blockers:
                benchmark_constitution_blockers.append(
                    f"benchmark artifact completeness: {blocker}"
                )
        precomputed_publication_blockers = benchmark_publication_claim_report_blockers(
            benchmark_publication_claim_report,
            manifest_ref=benchmark_manifest_ref,
        )
        if benchmark_publication_claim_report is not None:
            benchmark_constitution_blockers.extend(precomputed_publication_blockers)
        else:
            publication_claim_gate = validate_benchmark_publication_claim_gate(
                benchmark_manifest_path,
                repo_root=repo_root,
                require_current_git=False,
            )
            for blocker in publication_claim_gate.get("blockers", []):
                benchmark_constitution_blockers.append(
                    f"benchmark publication claim gate: {blocker}"
                )
        for required in [
            "benchmark_constitution_schema_version",
            "benchmark_constitution_validator",
            "benchmark_constitution_required_field_order",
            "benchmark_constitution_claim_gate_status",
            "benchmark_constitution_performance_claim_allowed",
        ]:
            if required not in benchmark_manifest:
                benchmark_constitution_blockers.append(f"benchmark manifest missing {required}")
        if benchmark_manifest.get("benchmark_constitution_schema_version") != "shardloom.benchmark_constitution_validation.v1":
            benchmark_constitution_blockers.append("benchmark manifest constitution schema mismatch")
        if benchmark_manifest.get("benchmark_constitution_performance_claim_allowed") is not False:
            benchmark_constitution_blockers.append("benchmark constitution performance claim must be false")
        if benchmark_manifest.get("artifact_status") != "complete":
            benchmark_constitution_blockers.append(
                "benchmark manifest artifact_status="
                + str(benchmark_manifest.get("artifact_status", "missing"))
            )
        if benchmark_manifest.get("missing_required_lanes"):
            benchmark_constitution_blockers.append(
                "benchmark manifest missing_required_lanes="
                + ",".join(str(lane) for lane in benchmark_manifest.get("missing_required_lanes", []))
            )
        benchmark_profile = str(benchmark_manifest.get("benchmark_profile", ""))
        if benchmark_profile in {"full_local", "full_local_plus_spark", "extended_local"}:
            expected_lanes = set(benchmark_manifest.get("expected_lanes") or [])
            available_lanes = set(benchmark_manifest.get("available_lanes") or [])
            for lane in [
                "shardloom",
                "shardloom-prepared-vortex",
                "shardloom-prepare-batch",
                "shardloom-vortex",
            ]:
                if lane not in expected_lanes:
                    benchmark_constitution_blockers.append(
                        f"benchmark manifest expected_lanes missing {lane}"
                    )
                if lane not in available_lanes:
                    benchmark_constitution_blockers.append(
                        f"benchmark manifest available_lanes missing {lane}"
                    )
    checks.append(
        check(
            "benchmark_constitution_validator",
            "scripts/check_benchmark_constitution.py",
            benchmark_constitution_blockers,
        )
    )

    front_door_benchmark_publication = load_json(
        front_door_benchmark_publication_report_path
    )
    front_door_publication_blockers: list[str] = []
    if front_door_benchmark_publication is None:
        front_door_publication_blockers.append(
            "missing front-door benchmark publication gate report"
        )
    else:
        if (
            front_door_benchmark_publication.get("schema_version")
            != FRONT_DOOR_BENCHMARK_PUBLICATION_SCHEMA_VERSION
        ):
            front_door_publication_blockers.append(
                "front-door benchmark publication schema_version="
                + str(front_door_benchmark_publication.get("schema_version", "missing"))
            )
        if front_door_benchmark_publication.get("status") != "passed":
            front_door_publication_blockers.extend(
                front_door_benchmark_publication.get(
                    "blockers", ["front-door benchmark publication gate blocked"]
                )
            )
        if front_door_benchmark_publication.get("claim_gate_status") != "not_claim_grade":
            front_door_publication_blockers.append(
                "front-door benchmark publication claim_gate_status="
                + str(front_door_benchmark_publication.get("claim_gate_status", "missing"))
            )
        if (
            front_door_benchmark_publication.get(
                "front_door_performance_publication_status"
            )
            != "blocked_pending_measured_equivalence_artifact"
        ):
            front_door_publication_blockers.append(
                "front-door benchmark publication status must stay blocked pending measured equivalence artifact"
            )
        if (
            front_door_benchmark_publication.get(
                "front_door_performance_equivalence_claim_allowed"
            )
            is not False
        ):
            front_door_publication_blockers.append(
                "front-door performance equivalence claim must be false"
            )
        for field in [
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
            "benchmark_run_performed",
            "benchmark_rerun_approved",
            "publication_attempted",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if front_door_benchmark_publication.get(field) is not False:
                front_door_publication_blockers.append(
                    f"front-door benchmark publication {field} must be false"
                )
        if (
            int(
                front_door_benchmark_publication.get(
                    "public_front_door_benchmark_row_count", 0
                )
                or 0
            )
            < 2
        ):
            front_door_publication_blockers.append(
                "front-door benchmark publication must expose public front-door rows"
            )
        if not front_door_benchmark_publication.get("publication_admission_blockers"):
            front_door_publication_blockers.append(
                "front-door benchmark publication must list admission blockers"
            )
    checks.append(
        check(
            "front_door_benchmark_publication_gate",
            str(args.front_door_benchmark_publication_report).replace("\\", "/"),
            front_door_publication_blockers,
        )
    )

    production_usability = load_json(production_usability_report_path)
    production_usability_blockers: list[str] = []
    if production_usability is None:
        production_usability_blockers.append("missing production usability gate report")
    else:
        if production_usability.get("schema_version") != "shardloom.production_usability_gate.v1":
            production_usability_blockers.append(
                "production usability schema_version="
                + str(production_usability.get("schema_version", "missing"))
            )
        if production_usability.get("status") != "passed":
            production_usability_blockers.extend(
                production_usability.get("blockers", ["production usability gate blocked"])
            )
        for field in [
            "production_claim_allowed",
            "performance_claim_allowed",
            "public_release_claim_allowed",
            "public_package_claim_allowed",
            "publication_attempted",
            "tag_created",
            "secrets_required",
            "package_upload_attempted",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if production_usability.get(field) is not False:
                production_usability_blockers.append(
                    f"production usability {field} must be false"
                )
        if production_usability.get("claim_gate_status") != "not_claim_grade":
            production_usability_blockers.append(
                "production usability claim_gate_status="
                + str(production_usability.get("claim_gate_status", "missing"))
            )
    checks.append(
        check(
            "production_usability_gate",
            str(args.production_usability_report).replace("\\", "/"),
            production_usability_blockers,
        )
    )

    python_user_surface = load_json(python_user_surface_report_path)
    python_user_surface_blockers: list[str] = []
    if python_user_surface is None:
        python_user_surface_blockers.append("missing Python user-surface completion gate report")
    else:
        if (
            python_user_surface.get("schema_version")
            != "shardloom.python_user_surface_completion_gate.v1"
        ):
            python_user_surface_blockers.append(
                "Python user-surface schema_version="
                + str(python_user_surface.get("schema_version", "missing"))
            )
        if python_user_surface.get("status") != "passed":
            python_user_surface_blockers.extend(
                python_user_surface.get(
                    "blockers", ["Python user-surface completion gate blocked"]
                )
            )
        if python_user_surface.get("scoped_python_front_door_claim_allowed") is not True:
            python_user_surface_blockers.append(
                "Python user-surface scoped_python_front_door_claim_allowed must be true"
            )
        for field in [
            "production_sql_dataframe_claim_allowed",
            "spark_compatibility_claim_allowed",
            "package_publication_claim_allowed",
            "performance_claim_allowed",
            "publication_attempted",
            "tag_created",
            "secrets_required",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if python_user_surface.get(field) is not False:
                python_user_surface_blockers.append(
                    f"Python user-surface {field} must be false"
                )
        if python_user_surface.get("claim_gate_status") != "not_claim_grade":
            python_user_surface_blockers.append(
                "Python user-surface claim_gate_status="
                + str(python_user_surface.get("claim_gate_status", "missing"))
            )
    checks.append(
        check(
            "python_user_surface_completion_gate",
            str(args.python_user_surface_report).replace("\\", "/"),
            python_user_surface_blockers,
        )
    )

    sql_python_dataframe_parity = load_json(sql_python_dataframe_parity_report_path)
    parity_blockers: list[str] = []
    if sql_python_dataframe_parity is None:
        parity_blockers.append("missing SQL/Python/DataFrame parity gate report")
    else:
        if (
            sql_python_dataframe_parity.get("schema_version")
            != "shardloom.sql_python_dataframe_parity_gate.v1"
        ):
            parity_blockers.append(
                "SQL/Python/DataFrame parity schema_version="
                + str(sql_python_dataframe_parity.get("schema_version", "missing"))
            )
        if sql_python_dataframe_parity.get("status") != "passed":
            parity_blockers.extend(
                sql_python_dataframe_parity.get(
                    "blockers", ["SQL/Python/DataFrame parity gate blocked"]
                )
            )
        if (
            sql_python_dataframe_parity.get("scoped_local_front_door_parity_supported")
            is not True
        ):
            parity_blockers.append(
                "SQL/Python/DataFrame scoped_local_front_door_parity_supported must be true"
            )
        if sql_python_dataframe_parity.get("all_no_fallback_no_external_engine") is not True:
            parity_blockers.append(
                "SQL/Python/DataFrame all_no_fallback_no_external_engine must be true"
            )
        for field in [
            "flexible_anything_claim_allowed",
            "performance_equivalence_claim_allowed",
        ]:
            if sql_python_dataframe_parity.get(field) is not False:
                parity_blockers.append(f"SQL/Python/DataFrame {field} must be false")
        if sql_python_dataframe_parity.get("claim_gate_status") != "not_claim_grade":
            parity_blockers.append(
                "SQL/Python/DataFrame claim_gate_status="
                + str(sql_python_dataframe_parity.get("claim_gate_status", "missing"))
            )
        if int(sql_python_dataframe_parity.get("remaining_gap_count", 0) or 0) < 1:
            parity_blockers.append(
                "SQL/Python/DataFrame parity report must keep remaining broad gaps visible"
            )
    checks.append(
        check(
            "sql_python_dataframe_parity_gate",
            str(args.sql_python_dataframe_parity_report).replace("\\", "/"),
            parity_blockers,
        )
    )

    user_surface_runtime_gap_inventory = load_json(user_surface_runtime_gap_inventory_path)
    user_surface_runtime_gap_blockers: list[str] = []
    if user_surface_runtime_gap_inventory is None:
        user_surface_runtime_gap_blockers.append("missing user-surface runtime gap inventory report")
    else:
        if (
            user_surface_runtime_gap_inventory.get("schema_version")
            != "shardloom.user_surface_runtime_gap_inventory.v1"
        ):
            user_surface_runtime_gap_blockers.append(
                "user-surface runtime gap inventory schema_version="
                + str(user_surface_runtime_gap_inventory.get("schema_version", "missing"))
            )
        if user_surface_runtime_gap_inventory.get("status") != "passed":
            user_surface_runtime_gap_blockers.extend(
                user_surface_runtime_gap_inventory.get(
                    "blockers", ["user-surface runtime gap inventory blocked"]
                )
            )
        acceptance = user_surface_runtime_gap_inventory.get("acceptance_summary")
        if not isinstance(acceptance, dict):
            user_surface_runtime_gap_blockers.append(
                "user-surface runtime gap inventory missing acceptance_summary"
            )
        else:
            if acceptance.get("shardloom_benchmark_unsupported_rows") != 0:
                user_surface_runtime_gap_blockers.append(
                    "user-surface runtime gap inventory must report zero ShardLoom benchmark "
                    "unsupported rows"
                )
            if acceptance.get("all_inventory_rows_classified") is not True:
                user_surface_runtime_gap_blockers.append(
                    "user-surface runtime gap inventory all_inventory_rows_classified must be true"
                )
            if acceptance.get("all_inventory_rows_no_fallback_no_external_engine") is not True:
                user_surface_runtime_gap_blockers.append(
                    "user-surface runtime gap inventory all_inventory_rows_no_fallback_no_external_engine must be true"
                )
            if acceptance.get("claim_gate_status") != "not_claim_grade":
                user_surface_runtime_gap_blockers.append(
                    "user-surface runtime gap inventory claim_gate_status="
                    + str(acceptance.get("claim_gate_status", "missing"))
                )
            for field in ["fallback_attempted", "external_engine_invoked"]:
                if acceptance.get(field) is not False:
                    user_surface_runtime_gap_blockers.append(
                        f"user-surface runtime gap inventory {field} must be false"
                    )
        benchmark_summary = user_surface_runtime_gap_inventory.get("benchmark_support_summary")
        if not isinstance(benchmark_summary, dict):
            user_surface_runtime_gap_blockers.append(
                "user-surface runtime gap inventory missing benchmark_support_summary"
            )
        elif benchmark_summary.get("external_baseline_classification_blockers"):
            user_surface_runtime_gap_blockers.append(
                "user-surface runtime gap inventory external unsupported rows must remain "
                "external-baseline-only"
            )
    checks.append(
        check(
            "user_surface_runtime_gap_inventory",
            str(args.user_surface_runtime_gap_inventory_report).replace("\\", "/"),
            user_surface_runtime_gap_blockers,
        )
    )

    user_surface_graduation_matrix = load_json(user_surface_graduation_matrix_path)
    user_surface_graduation_blockers: list[str] = []
    if user_surface_graduation_matrix is None:
        user_surface_graduation_blockers.append("missing user-surface graduation matrix report")
    else:
        if (
            user_surface_graduation_matrix.get("schema_version")
            != "shardloom.user_surface_graduation_matrix.v1"
        ):
            user_surface_graduation_blockers.append(
                "user-surface graduation matrix schema_version="
                + str(user_surface_graduation_matrix.get("schema_version", "missing"))
            )
        if user_surface_graduation_matrix.get("status") != "passed":
            user_surface_graduation_blockers.extend(
                user_surface_graduation_matrix.get(
                    "blockers", ["user-surface graduation matrix blocked"]
                )
            )
        acceptance = user_surface_graduation_matrix.get("acceptance_summary")
        if not isinstance(acceptance, dict):
            user_surface_graduation_blockers.append(
                "user-surface graduation matrix missing acceptance_summary"
            )
        else:
            for field in [
                "all_python_context_methods_classified",
                "all_python_client_methods_classified",
                "all_cli_commands_classified",
                "all_high_level_cli_commands_have_context_matrix_refs",
                "all_no_fallback_no_external_engine",
            ]:
                if acceptance.get(field) is not True:
                    user_surface_graduation_blockers.append(
                        f"user-surface graduation matrix {field} must be true"
                    )
        for field in [
            "fallback_attempted",
            "external_engine_invoked",
            "runtime_support_claim_allowed",
            "performance_claim_allowed",
            "production_claim_allowed",
        ]:
            if user_surface_graduation_matrix.get(field) is not False:
                user_surface_graduation_blockers.append(
                    f"user-surface graduation matrix {field} must be false"
                )
        if user_surface_graduation_matrix.get("claim_gate_status") != "not_claim_grade":
            user_surface_graduation_blockers.append(
                "user-surface graduation matrix claim_gate_status="
                + str(user_surface_graduation_matrix.get("claim_gate_status", "missing"))
            )
    checks.append(
        check(
            "user_surface_graduation_matrix",
            str(args.user_surface_graduation_matrix_report).replace("\\", "/"),
            user_surface_graduation_blockers,
        )
    )

    runtime_gap_family_burn_down = load_json(runtime_gap_family_burn_down_path)
    runtime_gap_family_blockers = runtime_gap_family_burn_down_blockers(
        runtime_gap_family_burn_down
    )
    checks.append(
        check(
            "runtime_gap_family_burn_down",
            str(args.runtime_gap_family_burn_down_report).replace("\\", "/"),
            runtime_gap_family_blockers,
        )
    )

    user_route_capability = load_json(user_route_capability_report_path)
    user_route_capability_blockers: list[str] = []
    if user_route_capability is None:
        user_route_capability_blockers.append("missing user route capability report")
    else:
        if (
            user_route_capability.get("schema_version")
            != "shardloom.user_route_capability_report.v1"
        ):
            user_route_capability_blockers.append(
                "user route capability schema_version="
                + str(user_route_capability.get("schema_version", "missing"))
            )
        if user_route_capability.get("status") != "passed":
            user_route_capability_blockers.extend(
                user_route_capability.get(
                    "blockers", ["user route capability report blocked"]
                )
            )
        if user_route_capability.get("all_no_fallback_no_external_engine") is not True:
            user_route_capability_blockers.append(
                "user route capability all_no_fallback_no_external_engine must be true"
            )
        if user_route_capability.get("unsupported_local_benchmark_route_ids"):
            user_route_capability_blockers.append(
                "user route capability must report zero unsupported local benchmark-range routes"
            )
        if user_route_capability.get("local_vortex_primitive_all_runtime_supported") is not True:
            user_route_capability_blockers.append(
                "user route capability local Vortex primitive routes must all be runtime-supported"
            )
        if (
            user_route_capability.get(
                "local_vortex_primitive_all_no_fallback_no_external_engine"
            )
            is not True
        ):
            user_route_capability_blockers.append(
                "user route capability local Vortex primitive routes must preserve no fallback"
            )
        required_primitive_commands = {
            "vortex-run",
            "vortex-count-where",
            "vortex-filter",
            "vortex-project",
            "vortex-filter-project",
        }
        primitive_commands = set(
            user_route_capability.get("local_vortex_primitive_command_coverage", [])
        )
        if primitive_commands != required_primitive_commands:
            user_route_capability_blockers.append(
                "user route capability local Vortex primitive command coverage mismatch"
            )
        required_local_file_benchmark_scenarios = {
            "selective_filter",
            "filter_projection_limit",
            "group_by_aggregation",
            "multi_key_group_by",
            "join_aggregate",
            "sort_top_k",
            "row_number_window",
            "top_n_per_group",
            "clean_cast_filter_write",
            "partition_pruning",
            "many_small_files_scan",
            "null_heavy_aggregate",
            "high_cardinality_string_group_distinct",
            "nested_json_field_scan",
            "small_change_over_large_base",
        }
        local_file_scenarios = set(
            user_route_capability.get("local_file_benchmark_scenario_ids", [])
        )
        if local_file_scenarios != required_local_file_benchmark_scenarios:
            user_route_capability_blockers.append(
                "user route capability local file benchmark scenario coverage mismatch"
            )
        if user_route_capability.get("local_file_benchmark_unsupported_scenario_ids"):
            user_route_capability_blockers.append(
                "user route capability local file benchmark scenarios must not be unsupported"
            )
        if (
            user_route_capability.get(
                "local_file_benchmark_all_no_fallback_no_external_engine"
            )
            is not True
        ):
            user_route_capability_blockers.append(
                "user route capability local file benchmark routes must preserve no fallback"
            )
        if (
            user_route_capability.get(
                "local_file_benchmark_all_mapped_without_generic_unsupported"
            )
            is not True
        ):
            user_route_capability_blockers.append(
                "user route capability local file benchmark routes must avoid generic unsupported"
            )
        for field in [
            "flexible_anything_claim_allowed",
            "performance_equivalence_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
        ]:
            if user_route_capability.get(field) is not False:
                user_route_capability_blockers.append(
                    f"user route capability {field} must be false"
                )
        if user_route_capability.get("claim_gate_status") != "not_claim_grade":
            user_route_capability_blockers.append(
                "user route capability claim_gate_status="
                + str(user_route_capability.get("claim_gate_status", "missing"))
            )
        acceptance = user_route_capability.get("acceptance_summary")
        if not isinstance(acceptance, dict):
            user_route_capability_blockers.append(
                "user route capability missing acceptance_summary"
            )
        else:
            for field in [
                "all_routes_have_vortex_normalization",
                "all_routes_have_output_and_evidence",
                "all_routes_have_materialization_decode_boundary",
                "no_generic_unsupported_local_benchmark_route",
                "all_local_vortex_primitive_routes_supported",
                "all_local_vortex_primitive_routes_start_at_native_boundary",
                "all_local_vortex_primitive_commands_covered",
                "all_required_local_file_benchmark_scenarios_mapped",
                "no_generic_unsupported_local_file_benchmark_scenario",
                "all_local_file_benchmark_routes_have_vortex_normalization",
                "all_local_file_benchmark_routes_have_output_and_evidence",
                "all_prepared_routes_expose_workspace_manifest_reuse_contract",
                "generated_source_route_exposes_artifact_adjacent_manifest_reuse_contract",
                "public_front_door_routes_expose_auto_and_generated_prepared_surfaces",
                "public_front_door_routes_expose_prepared_state_reuse_contracts",
                "public_front_door_routes_preserve_no_fallback",
                "all_prepared_local_file_benchmark_routes_expose_workspace_manifest_reuse_contract",
                "all_local_file_benchmark_routes_preserve_no_fallback",
                "all_no_fallback_no_external_engine",
            ]:
                if acceptance.get(field) is not True:
                    user_route_capability_blockers.append(
                        f"user route capability {field} must be true"
                    )
            for field in [
                "performance_claim_allowed",
                "production_claim_allowed",
                "spark_replacement_claim_allowed",
                "fallback_attempted",
                "external_engine_invoked",
            ]:
                if acceptance.get(field) is not False:
                    user_route_capability_blockers.append(
                        f"user route capability {field} must be false"
                    )
            if acceptance.get("claim_gate_status") != "not_claim_grade":
                user_route_capability_blockers.append(
                    "user route capability acceptance claim_gate_status="
                    + str(acceptance.get("claim_gate_status", "missing"))
                )
    checks.append(
        check(
            "user_route_capability_report",
            str(args.user_route_capability_report).replace("\\", "/"),
            user_route_capability_blockers,
        )
    )

    pre_5j_dependency = load_json(pre_5j_dependency_report_path)
    pre_5j_dependency_blockers: list[str] = []
    if pre_5j_dependency is None:
        pre_5j_dependency_blockers.append("missing pre-5J dependency freshness gate report")
    else:
        if (
            pre_5j_dependency.get("schema_version")
            != "shardloom.pre_5j_dependency_freshness_gate.v1"
        ):
            pre_5j_dependency_blockers.append(
                "pre-5J dependency schema_version="
                + str(pre_5j_dependency.get("schema_version", "missing"))
            )
        if pre_5j_dependency.get("status") != "passed":
            pre_5j_dependency_blockers.extend(
                pre_5j_dependency.get("blockers", ["pre-5J dependency freshness gate blocked"])
            )
        if pre_5j_dependency.get("benchmark_run_performed") is not False:
            pre_5j_dependency_blockers.append(
                "pre-5J dependency benchmark_run_performed must be false"
            )
        for field in [
            "publication_attempted",
            "tag_created",
            "secrets_required",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if pre_5j_dependency.get(field) is not False:
                pre_5j_dependency_blockers.append(f"pre-5J dependency {field} must be false")
    checks.append(
        check(
            "pre_5j_dependency_freshness_gate",
            str(args.pre_5j_dependency_report).replace("\\", "/"),
            pre_5j_dependency_blockers,
        )
    )

    validation_commands = [
        "cargo fmt --all -- --check",
        "cargo clippy --workspace --all-targets -- -D warnings",
        "cargo test --workspace --all-targets",
        "python -m unittest discover python/tests",
        "python -m build python",
        "python scripts/release_dry_run_proof.py --rows 64 --iterations 1",
        "cargo run -q -p shardloom-cli -- global-architecture-gate --format json",
        "python scripts/check_contribution_governance.py",
        "python scripts/check_ci_gate_matrix.py",
        "python scripts/check_release_security_gate.py",
        "python scripts/check_release_architecture_tracker.py --allow-blocked",
        "python scripts/check_package_channel_readiness.py --require-local-evidence",
        "python scripts/check_golden_workflows.py",
        "python scripts/check_admitted_semantics_matrix.py",
        "python scripts/check_runtime_execution_envelopes.py",
        "python scripts/check_website_readiness.py",
        "python scripts/check_benchmark_constitution.py",
        "python scripts/check_benchmark_artifact_completeness.py --manifest website/assets/benchmarks/latest/manifest.json --output target/benchmark-artifact-completeness-report.json",
        "python scripts/check_pre_5j_dependency_freshness.py",
        "python scripts/check_benchmark_publication_claim_gate.py --manifest website/assets/benchmarks/latest/manifest.json",
        "python scripts/check_front_door_benchmark_publication.py --manifest website/assets/benchmarks/latest/manifest.json",
        "python scripts/final_release_rehearsal.py --allow-blocked",
        "python scripts/check_production_usability_gate.py",
        "python scripts/check_python_user_surface_completion.py",
        "python scripts/check_sql_python_dataframe_parity.py",
        "python scripts/check_user_surface_runtime_gap_inventory.py",
        "python scripts/check_user_surface_graduation_matrix.py",
        "python scripts/check_runtime_gap_family_burn_down.py",
        "python scripts/check_user_route_capability_report.py",
    ]
    validation_blockers = []
    if validation_evidence is None:
        validation_blockers.append("missing release validation evidence")
    elif validation_evidence.get("required_validation_status") != "passed":
        validation_blockers.append(
            f"required_validation_status={validation_evidence.get('required_validation_status')}"
        )
    command_status = {
        row.get("command"): row.get("status")
        for row in (validation_evidence or {}).get("required_validation_commands", [])
    }

    for command in validation_commands:
        if not validation_command_passed(command_status, command):
            validation_blockers.append(f"attach current run evidence for: {command}")
    checks.append(
        check(
            "required_validation_commands",
            str(args.validation_evidence).replace("\\", "/"),
            validation_blockers,
        )
    )

    blockers = [f"{item['name']}: {blocker}" for item in checks for blocker in item["blockers"]]
    passed = not blockers
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "blocked",
        "public_release_claim_allowed": passed,
        "public_package_claim_allowed": passed,
        "package_channel_matrix_ref": str(args.package_channel_matrix).replace("\\", "/"),
        "package_channel_report_ref": str(args.package_channel_report).replace("\\", "/"),
        "contribution_governance_report_ref": str(args.contribution_governance_report).replace(
            "\\", "/"
        ),
        "golden_workflow_report_ref": str(args.golden_workflow_report).replace("\\", "/"),
        "admitted_semantics_report_ref": str(args.admitted_semantics_report).replace("\\", "/"),
        "per_claim_evidence_matrix_ref": str(args.per_claim_evidence_matrix).replace("\\", "/"),
        "architecture_tracker_report_ref": str(args.architecture_tracker_report).replace("\\", "/"),
        "final_release_rehearsal_report_ref": str(args.final_release_rehearsal_report).replace("\\", "/"),
        "production_usability_report_ref": str(args.production_usability_report).replace("\\", "/"),
        "benchmark_completeness_report_ref": str(args.benchmark_completeness_report).replace(
            "\\", "/"
        ),
        "benchmark_publication_claim_report_ref": str(
            args.benchmark_publication_claim_report
        ).replace("\\", "/"),
        "front_door_benchmark_publication_report_ref": str(
            args.front_door_benchmark_publication_report
        ).replace("\\", "/"),
        "user_surface_runtime_gap_inventory_ref": str(
            args.user_surface_runtime_gap_inventory_report
        ).replace("\\", "/"),
        "checks": checks,
        "blockers": blockers,
        "required_validation_commands": validation_commands,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0 if passed or args.allow_blocked else 1


if __name__ == "__main__":
    raise SystemExit(main())
