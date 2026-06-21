#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Aggregate hard release-readiness evidence for ShardLoom.

The gate requires feature/build matrix execution evidence, not only matrix documentation.
The gate also consumes the package-channel readiness matrix so public package claims cannot pass
without channel-specific install, smoke, provenance, and rollback evidence.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from check_runtime_execution_envelopes import (
    validate_repo as validate_runtime_execution_envelope_surfaces,
)
from check_package_channel_readiness import (
    validate_local_gate_evidence as validate_package_local_gate_evidence,
)
from check_package_channel_readiness import validate_matrix as validate_package_channel_matrix
from release_channel_contract import (
    SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS,
    selected_channel_ids,
)


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
        "--workspace-version-source-report",
        type=Path,
        default=Path("target/workspace-version-source-report.json"),
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
        "--v1-front-door-runtime-scope-report",
        type=Path,
        default=Path("target/v1-front-door-runtime-scope-report.json"),
    )
    parser.add_argument(
        "--v1-vortex-runtime-scope-report",
        type=Path,
        default=Path("target/v1-vortex-runtime-scope-report.json"),
    )
    parser.add_argument(
        "--v1-source-prepared-state-scope-report",
        type=Path,
        default=Path("target/v1-source-prepared-state-scope-report.json"),
    )
    parser.add_argument(
        "--v1-local-output-sink-scope-report",
        type=Path,
        default=Path("target/v1-local-output-sink-scope-report.json"),
    )
    parser.add_argument(
        "--v1-local-resource-safety-report",
        type=Path,
        default=Path("target/v1-local-resource-safety-report.json"),
    )
    parser.add_argument(
        "--v1-observability-support-report",
        type=Path,
        default=Path("target/v1-observability-support-report.json"),
    )
    parser.add_argument(
        "--v1-api-schema-stability-report",
        type=Path,
        default=Path("target/v1-api-schema-stability-report.json"),
    )
    parser.add_argument(
        "--v1-example-replay-report",
        type=Path,
        default=Path("target/v1-example-replay-report.json"),
    )
    parser.add_argument(
        "--v1-correctness-conformance-report",
        type=Path,
        default=Path("target/v1-correctness-conformance-report.json"),
    )
    parser.add_argument(
        "--v1-security-ci-hardening-report",
        type=Path,
        default=Path("target/v1-security-ci-hardening-report.json"),
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


def check(name: str, ref: str, blockers: list[str]) -> dict[str, Any]:
    return {"name": name, "ref": ref, "status": "passed" if not blockers else "blocked", "blockers": blockers}


def validation_command_passed(command_status: dict[Any, Any], expected: str) -> bool:
    if command_status.get(expected) == "passed":
        return True
    prefix_allowed = {
        "python scripts/release_dry_run_proof.py --rows 64 --iterations 1",
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
    workspace_version_source_report_path = resolve(
        repo_root,
        args.workspace_version_source_report,
    )
    per_claim_evidence_matrix_path = resolve(repo_root, args.per_claim_evidence_matrix)
    architecture_tracker_report_path = resolve(repo_root, args.architecture_tracker_report)
    final_release_rehearsal_report_path = resolve(repo_root, args.final_release_rehearsal_report)
    production_usability_report_path = resolve(repo_root, args.production_usability_report)
    python_user_surface_report_path = resolve(repo_root, args.python_user_surface_report)
    sql_python_dataframe_parity_report_path = resolve(
        repo_root,
        args.sql_python_dataframe_parity_report,
    )
    v1_front_door_runtime_scope_report_path = resolve(
        repo_root,
        args.v1_front_door_runtime_scope_report,
    )
    v1_vortex_runtime_scope_report_path = resolve(
        repo_root,
        args.v1_vortex_runtime_scope_report,
    )
    v1_source_prepared_state_scope_report_path = resolve(
        repo_root,
        args.v1_source_prepared_state_scope_report,
    )
    v1_local_output_sink_scope_report_path = resolve(
        repo_root,
        args.v1_local_output_sink_scope_report,
    )
    v1_local_resource_safety_report_path = resolve(
        repo_root,
        args.v1_local_resource_safety_report,
    )
    v1_observability_support_report_path = resolve(
        repo_root,
        args.v1_observability_support_report,
    )
    v1_api_schema_stability_report_path = resolve(
        repo_root,
        args.v1_api_schema_stability_report,
    )
    v1_example_replay_report_path = resolve(
        repo_root,
        args.v1_example_replay_report,
    )
    v1_correctness_conformance_report_path = resolve(
        repo_root,
        args.v1_correctness_conformance_report,
    )
    v1_security_ci_hardening_report_path = resolve(
        repo_root,
        args.v1_security_ci_hardening_report,
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
        if admitted_semantics.get("executable_fixture_count") != 117:
            admitted_semantics_blockers.append(
                "admitted semantics executable_fixture_count="
                + str(admitted_semantics.get("executable_fixture_count", "missing"))
            )
        if admitted_semantics.get("diagnostic_case_count") != 25:
            admitted_semantics_blockers.append(
                "admitted semantics diagnostic_case_count="
                + str(admitted_semantics.get("diagnostic_case_count", "missing"))
            )
        if admitted_semantics.get("unsupported_diagnostic_count") != 23:
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
        if admitted_semantics.get("property_lane_count") != 10:
            admitted_semantics_blockers.append(
                "admitted semantics property_lane_count="
                + str(admitted_semantics.get("property_lane_count", "missing"))
            )
        if admitted_semantics.get("deterministic_fuzz_execution_performed") is not True:
            admitted_semantics_blockers.append(
                "admitted semantics deterministic_fuzz_execution_performed missing"
            )
        if admitted_semantics.get("deterministic_fuzz_case_count") != 5:
            admitted_semantics_blockers.append(
                "admitted semantics deterministic_fuzz_case_count="
                + str(admitted_semantics.get("deterministic_fuzz_case_count", "missing"))
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

    workspace_version_source = load_json(workspace_version_source_report_path)
    workspace_version_blockers: list[str] = []
    if workspace_version_source is None:
        workspace_version_blockers.append("missing workspace version source report")
    else:
        if (
            workspace_version_source.get("schema_version")
            != "shardloom.workspace_version_source_report.v1"
        ):
            workspace_version_blockers.append(
                "workspace version source schema_version="
                + str(workspace_version_source.get("schema_version", "missing"))
            )
        if workspace_version_source.get("status") != "passed":
            workspace_version_blockers.extend(
                workspace_version_source.get(
                    "blockers", ["workspace version source contract blocked"]
                )
            )
        version_env = workspace_version_source.get("version_env")
        if not isinstance(version_env, dict):
            workspace_version_blockers.append("workspace version source missing version_env")
        else:
            for key in [
                "SHARDLOOM_RUST_MSRV_TOOLCHAIN",
                "SHARDLOOM_RUST_MSRV_LANE",
                "SHARDLOOM_UPSTREAM_VORTEX_MANIFEST_VERSION",
                "SHARDLOOM_UPSTREAM_VORTEX_LOCK_VERSION",
                "SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION",
            ]:
                if not version_env.get(key):
                    workspace_version_blockers.append(
                        f"workspace version source missing version_env {key}"
                    )
        for field in [
            "public_release_claim_allowed",
            "public_package_claim_allowed",
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
            "publication_attempted",
            "tag_created",
            "secrets_required",
            "package_upload_attempted",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if workspace_version_source.get(field) is not False:
                workspace_version_blockers.append(f"workspace version source {field} must be false")
        if workspace_version_source.get("claim_gate_status") != "not_claim_grade":
            workspace_version_blockers.append(
                "workspace version source claim_gate_status="
                + str(workspace_version_source.get("claim_gate_status", "missing"))
            )
    checks.append(
        check(
            "workspace_rust_vortex_version_source_contract",
            str(args.workspace_version_source_report).replace("\\", "/"),
            workspace_version_blockers,
        )
    )

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
        if package_channel_report.get("package_identity_contract_status") != "passed":
            package_channel_blockers.append(
                "package-channel package_identity_contract_status="
                + str(package_channel_report.get("package_identity_contract_status", "missing"))
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
        selected_ids = selected_channel_ids(package_channel_matrix)
        blocked_channels = [
            row.get("channel_id", "unknown")
            for row in channels
            if (
                isinstance(row, dict)
                and row.get("channel_id") in selected_ids
                and row.get("ready") is not True
            )
        ]
        if blocked_channels:
            package_channel_blockers.append(
                "selected package channels not ready: "
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

    v1_api_schema_stability = load_json(v1_api_schema_stability_report_path)
    v1_api_schema_stability_blockers: list[str] = []
    if v1_api_schema_stability is None:
        v1_api_schema_stability_blockers.append("missing v1 API/schema stability report")
    else:
        if (
            v1_api_schema_stability.get("schema_version")
            != "shardloom.v1_api_schema_stability_report.v1"
        ):
            v1_api_schema_stability_blockers.append("v1 API/schema stability schema_version mismatch")
        if v1_api_schema_stability.get("status") != "passed":
            v1_api_schema_stability_blockers.extend(
                v1_api_schema_stability.get(
                    "blockers",
                    ["v1 API/schema stability report blocked"],
                )
            )
        if v1_api_schema_stability.get("stable_surface_count") != 12:
            v1_api_schema_stability_blockers.append(
                "v1 API/schema stable_surface_count="
                + str(v1_api_schema_stability.get("stable_surface_count"))
            )
        if v1_api_schema_stability.get("compatibility_window") != "v1_additive_compatibility":
            v1_api_schema_stability_blockers.append(
                "v1 API/schema compatibility_window must be v1_additive_compatibility"
            )
        if not v1_api_schema_stability.get("legacy_flat_field_policy"):
            v1_api_schema_stability_blockers.append("missing v1 API/schema legacy flat-field policy")
        for field in [
            "public_release_claim_allowed",
            "public_package_claim_allowed",
            "package_publication_performed",
            "tag_created",
            "signing_key_used",
            "runtime_execution",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if v1_api_schema_stability.get(field) is not False:
                v1_api_schema_stability_blockers.append(f"v1 API/schema {field} must be false")
        if v1_api_schema_stability.get("publication_approval_required") is not True:
            v1_api_schema_stability_blockers.append(
                "v1 API/schema publication_approval_required must be true"
            )
    checks.append(
        check(
            "v1_api_schema_stability",
            str(args.v1_api_schema_stability_report).replace("\\", "/"),
            v1_api_schema_stability_blockers,
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
        if final_release_rehearsal.get(
            "publication_authorization_status"
        ) != SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS:
            final_rehearsal_blockers.append(
                "final rehearsal publication_authorization_status must be "
                + SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS
            )
        if final_release_rehearsal.get("publication_human_approved") is not True:
            final_rehearsal_blockers.append(
                "final rehearsal publication_human_approved must be true"
            )
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

    v1_front_door_runtime_scope = load_json(v1_front_door_runtime_scope_report_path)
    v1_front_door_blockers: list[str] = []
    if v1_front_door_runtime_scope is None:
        v1_front_door_blockers.append("missing v1 front-door runtime scope report")
    else:
        if (
            v1_front_door_runtime_scope.get("schema_version")
            != "shardloom.v1_front_door_runtime_scope_report.v1"
        ):
            v1_front_door_blockers.append(
                "v1 front-door runtime scope schema_version="
                + str(v1_front_door_runtime_scope.get("schema_version", "missing"))
            )
        if v1_front_door_runtime_scope.get("status") != "passed":
            v1_front_door_blockers.extend(
                v1_front_door_runtime_scope.get(
                    "blockers", ["v1 front-door runtime scope gate blocked"]
                )
            )
        if (
            v1_front_door_runtime_scope.get("scoped_local_front_door_parity_supported")
            is not True
        ):
            v1_front_door_blockers.append(
                "v1 front-door scoped_local_front_door_parity_supported must be true"
            )
        if v1_front_door_runtime_scope.get("all_no_fallback_no_external_engine") is not True:
            v1_front_door_blockers.append(
                "v1 front-door all_no_fallback_no_external_engine must be true"
            )
        if set(v1_front_door_runtime_scope.get("example_scenario_ids", [])) != {
            "selective_filter",
            "filter_projection_limit",
            "group_by_aggregation",
            "hash_join",
            "global_top_n",
            "clean_cast_filter_write",
            "malformed_timestamp_cast",
            "null_heavy_aggregate",
            "nested_json_field_scan",
        }:
            v1_front_door_blockers.append("v1 front-door example scenario coverage mismatch")
        for field in [
            "flexible_anything_claim_allowed",
            "performance_equivalence_claim_allowed",
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
        ]:
            if v1_front_door_runtime_scope.get(field) is not False:
                v1_front_door_blockers.append(f"v1 front-door {field} must be false")
        if v1_front_door_runtime_scope.get("claim_gate_status") != "not_claim_grade":
            v1_front_door_blockers.append(
                "v1 front-door claim_gate_status="
                + str(v1_front_door_runtime_scope.get("claim_gate_status", "missing"))
            )
    checks.append(
        check(
            "v1_front_door_runtime_scope_gate",
            str(args.v1_front_door_runtime_scope_report).replace("\\", "/"),
            v1_front_door_blockers,
        )
    )

    v1_vortex_runtime_scope = load_json(v1_vortex_runtime_scope_report_path)
    v1_vortex_blockers: list[str] = []
    if v1_vortex_runtime_scope is None:
        v1_vortex_blockers.append("missing v1 Vortex runtime scope report")
    else:
        if (
            v1_vortex_runtime_scope.get("schema_version")
            != "shardloom.v1_vortex_runtime_scope_report.v1"
        ):
            v1_vortex_blockers.append(
                "v1 Vortex runtime scope schema_version="
                + str(v1_vortex_runtime_scope.get("schema_version", "missing"))
            )
        if v1_vortex_runtime_scope.get("status") != "passed":
            v1_vortex_blockers.extend(
                v1_vortex_runtime_scope.get(
                    "blockers", ["v1 Vortex runtime scope gate blocked"]
                )
            )
        if v1_vortex_runtime_scope.get("local_vortex_primitive_v1_scope_ready") is not True:
            v1_vortex_blockers.append(
                "v1 Vortex local_vortex_primitive_v1_scope_ready must be true"
            )
        if v1_vortex_runtime_scope.get("user_route_v1_vortex_scope_ready") is not True:
            v1_vortex_blockers.append(
                "v1 Vortex user_route_v1_vortex_scope_ready must be true"
            )
        if v1_vortex_runtime_scope.get("all_no_fallback_no_external_engine") is not True:
            v1_vortex_blockers.append(
                "v1 Vortex all_no_fallback_no_external_engine must be true"
            )
        if len(v1_vortex_runtime_scope.get("supported_primitive_route_ids", [])) != 11:
            v1_vortex_blockers.append("v1 Vortex primitive route coverage must contain 11 rows")
        if len(v1_vortex_runtime_scope.get("supported_benchmark_scenario_ids", [])) != 16:
            v1_vortex_blockers.append(
                "v1 Vortex benchmark scenario coverage must contain 16 rows"
            )
        if "object_store_vortex_io" not in set(
            v1_vortex_runtime_scope.get("unsupported_boundary_ids", [])
        ):
            v1_vortex_blockers.append(
                "v1 Vortex unsupported boundaries must include object_store_vortex_io"
            )
        for field in [
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
        ]:
            if v1_vortex_runtime_scope.get(field) is not False:
                v1_vortex_blockers.append(f"v1 Vortex {field} must be false")
        if v1_vortex_runtime_scope.get("claim_gate_status") != "not_claim_grade":
            v1_vortex_blockers.append(
                "v1 Vortex claim_gate_status="
                + str(v1_vortex_runtime_scope.get("claim_gate_status", "missing"))
            )
    checks.append(
        check(
            "v1_vortex_runtime_scope_gate",
            str(args.v1_vortex_runtime_scope_report).replace("\\", "/"),
            v1_vortex_blockers,
        )
    )

    v1_source_prepared_state_scope = load_json(v1_source_prepared_state_scope_report_path)
    v1_source_prepared_blockers: list[str] = []
    if v1_source_prepared_state_scope is None:
        v1_source_prepared_blockers.append("missing v1 SourceState/prepared-state scope report")
    else:
        if (
            v1_source_prepared_state_scope.get("schema_version")
            != "shardloom.v1_source_prepared_state_scope_report.v1"
        ):
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state scope schema_version="
                + str(v1_source_prepared_state_scope.get("schema_version", "missing"))
            )
        if v1_source_prepared_state_scope.get("status") != "passed":
            v1_source_prepared_blockers.extend(
                v1_source_prepared_state_scope.get(
                    "blockers",
                    ["v1 SourceState/prepared-state scope gate blocked"],
                )
            )
        if v1_source_prepared_state_scope.get("v1_scope_ready") is not True:
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state v1_scope_ready must be true"
            )
        if (
            v1_source_prepared_state_scope.get("all_no_fallback_no_external_engine")
            is not True
        ):
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state all_no_fallback_no_external_engine must be true"
            )
        if (
            v1_source_prepared_state_scope.get(
                "all_prepared_routes_expose_reuse_contract"
            )
            is not True
        ):
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state prepared routes must expose reuse contracts"
            )
        if (
            v1_source_prepared_state_scope.get(
                "all_internal_source_smoke_routes_are_labeled_non_persistent"
            )
            is not True
        ):
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state internal source smoke routes must be non-persistent"
            )
        if len(v1_source_prepared_state_scope.get("supported_input_formats", [])) != 6:
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state format coverage must contain 6 formats"
            )
        if len(v1_source_prepared_state_scope.get("invalidation_case_ids", [])) != 9:
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state invalidation coverage must contain 9 cases"
            )
        if len(v1_source_prepared_state_scope.get("golden_fixture_paths", [])) != 3:
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state fixture coverage must contain 3 fixtures"
            )
        if (
            v1_source_prepared_state_scope.get(
                "source_prepared_benchmark_required_fields_ready"
            )
            is not True
        ):
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state benchmark rows must expose required fields"
            )
        if (
            v1_source_prepared_state_scope.get(
                "source_prepared_benchmark_rows_with_required_fields",
                0,
            )
            <= 0
        ):
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state benchmark row evidence must be non-empty"
            )
        if "global_hidden_cache" not in set(
            v1_source_prepared_state_scope.get("unsupported_boundary_ids", [])
        ):
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state unsupported boundaries must include global_hidden_cache"
            )
        for field in [
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
        ]:
            if v1_source_prepared_state_scope.get(field) is not False:
                v1_source_prepared_blockers.append(
                    f"v1 SourceState/prepared-state {field} must be false"
                )
        if v1_source_prepared_state_scope.get("claim_gate_status") != "not_claim_grade":
            v1_source_prepared_blockers.append(
                "v1 SourceState/prepared-state claim_gate_status="
                + str(v1_source_prepared_state_scope.get("claim_gate_status", "missing"))
            )
    checks.append(
        check(
            "v1_source_prepared_state_scope_gate",
            str(args.v1_source_prepared_state_scope_report).replace("\\", "/"),
            v1_source_prepared_blockers,
        )
    )

    v1_local_output_sink_scope = load_json(v1_local_output_sink_scope_report_path)
    v1_local_output_sink_blockers: list[str] = []
    if v1_local_output_sink_scope is None:
        v1_local_output_sink_blockers.append("missing v1 local output/sink scope report")
    else:
        if (
            v1_local_output_sink_scope.get("schema_version")
            != "shardloom.v1_local_output_sink_scope_report.v1"
        ):
            v1_local_output_sink_blockers.append(
                "v1 local output/sink scope schema_version="
                + str(v1_local_output_sink_scope.get("schema_version", "missing"))
            )
        if v1_local_output_sink_scope.get("status") != "passed":
            v1_local_output_sink_blockers.extend(
                v1_local_output_sink_scope.get(
                    "blockers",
                    ["v1 local output/sink scope gate blocked"],
                )
            )
        if v1_local_output_sink_scope.get("v1_scope_ready") is not True:
            v1_local_output_sink_blockers.append(
                "v1 local output/sink v1_scope_ready must be true"
            )
        if (
            v1_local_output_sink_scope.get("all_no_fallback_no_external_engine")
            is not True
        ):
            v1_local_output_sink_blockers.append(
                "v1 local output/sink all_no_fallback_no_external_engine must be true"
            )
        if v1_local_output_sink_scope.get("all_write_methods_registered") is not True:
            v1_local_output_sink_blockers.append(
                "v1 local output/sink write methods must be registered"
            )
        if v1_local_output_sink_scope.get("write_policy_contract_ready") is not True:
            v1_local_output_sink_blockers.append(
                "v1 local output/sink write policy contract must be ready"
            )
        if len(v1_local_output_sink_scope.get("supported_output_formats", [])) != 7:
            v1_local_output_sink_blockers.append(
                "v1 local output/sink format coverage must contain 7 formats"
            )
        if len(v1_local_output_sink_scope.get("user_write_methods", [])) != 9:
            v1_local_output_sink_blockers.append(
                "v1 local output/sink method coverage must contain 9 methods"
            )
        if len(v1_local_output_sink_scope.get("golden_fixture_paths", [])) != 3:
            v1_local_output_sink_blockers.append(
                "v1 local output/sink fixture coverage must contain 3 fixtures"
            )
        if (
            v1_local_output_sink_scope.get(
                "local_output_sink_benchmark_required_fields_ready"
            )
            is not True
        ):
            v1_local_output_sink_blockers.append(
                "v1 local output/sink benchmark rows must expose required fields"
            )
        if (
            v1_local_output_sink_scope.get(
                "local_output_sink_benchmark_replay_ready"
            )
            is not True
        ):
            v1_local_output_sink_blockers.append(
                "v1 local output/sink benchmark rows must expose replay verification"
            )
        if "append_mode" not in set(
            v1_local_output_sink_scope.get("unsupported_boundary_ids", [])
        ):
            v1_local_output_sink_blockers.append(
                "v1 local output/sink unsupported boundaries must include append_mode"
            )
        for field in [
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
        ]:
            if v1_local_output_sink_scope.get(field) is not False:
                v1_local_output_sink_blockers.append(
                    f"v1 local output/sink {field} must be false"
                )
        if v1_local_output_sink_scope.get("claim_gate_status") != "not_claim_grade":
            v1_local_output_sink_blockers.append(
                "v1 local output/sink claim_gate_status="
                + str(v1_local_output_sink_scope.get("claim_gate_status", "missing"))
            )
    checks.append(
        check(
            "v1_local_output_sink_scope_gate",
            str(args.v1_local_output_sink_scope_report).replace("\\", "/"),
            v1_local_output_sink_blockers,
        )
    )

    v1_local_resource_safety = load_json(v1_local_resource_safety_report_path)
    v1_local_resource_safety_blockers: list[str] = []
    if v1_local_resource_safety is None:
        v1_local_resource_safety_blockers.append("missing v1 local resource-safety report")
    else:
        if (
            v1_local_resource_safety.get("schema_version")
            != "shardloom.v1_local_resource_safety_report.v1"
        ):
            v1_local_resource_safety_blockers.append(
                "v1 local resource safety schema_version="
                + str(v1_local_resource_safety.get("schema_version", "missing"))
            )
        if v1_local_resource_safety.get("status") != "passed":
            v1_local_resource_safety_blockers.extend(
                v1_local_resource_safety.get(
                    "blockers",
                    ["v1 local resource-safety gate blocked"],
                )
            )
        for field, expected in [
            ("runtime_command_count", 5),
            ("runtime_command_pass_count", 5),
            ("prerequisite_report_count", 2),
            ("memory_budget_config_status", "passed"),
            ("pre_oom_guard_status", "passed"),
            ("retry_gate_status", "passed"),
            ("cancellation_cleanup_status", "passed"),
            ("memory_runtime_hardening_status", "passed"),
            ("fault_tolerance_gate_status", "passed"),
            ("prepared_state_cleanup_status", "passed"),
            ("local_output_cleanup_status", "passed"),
            ("claim_gate_status", "not_claim_grade"),
        ]:
            if v1_local_resource_safety.get(field) != expected:
                v1_local_resource_safety_blockers.append(
                    f"v1 local resource safety {field}="
                    + str(v1_local_resource_safety.get(field, "missing"))
                )
        for field in [
            "v1_scope_ready",
            "local_resource_safety_evidence_ready",
            "unsupported_paths_blocked_without_writes",
            "all_no_fallback_no_external_engine",
        ]:
            if v1_local_resource_safety.get(field) is not True:
                v1_local_resource_safety_blockers.append(
                    f"v1 local resource safety {field} must be true"
                )
        for field in [
            "larger_than_memory_claim_allowed",
            "native_spill_runtime_claim_allowed",
            "distributed_resource_claim_allowed",
            "spill_io_performed",
            "object_store_io",
            "output_dataset_write_by_resource_gate",
            "public_release_claim_allowed",
            "public_package_claim_allowed",
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
            "publication_attempted",
            "tag_created",
            "package_upload_attempted",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if v1_local_resource_safety.get(field) is not False:
                v1_local_resource_safety_blockers.append(
                    f"v1 local resource safety {field} must be false"
                )
    checks.append(
        check(
            "v1_local_resource_safety_gate",
            str(args.v1_local_resource_safety_report).replace("\\", "/"),
            v1_local_resource_safety_blockers,
        )
    )

    v1_observability_support = load_json(v1_observability_support_report_path)
    v1_observability_support_blockers: list[str] = []
    if v1_observability_support is None:
        v1_observability_support_blockers.append(
            "missing v1 observability/supportability report"
        )
    else:
        if (
            v1_observability_support.get("schema_version")
            != "shardloom.v1_observability_support_report.v1"
        ):
            v1_observability_support_blockers.append(
                "v1 observability/supportability schema_version="
                + str(v1_observability_support.get("schema_version", "missing"))
            )
        if v1_observability_support.get("status") != "passed":
            v1_observability_support_blockers.extend(
                v1_observability_support.get(
                    "blockers",
                    ["v1 observability/supportability gate blocked"],
                )
            )
        for field, expected in [
            ("runtime_command_count", 8),
            ("runtime_command_pass_count", 8),
            ("doctor_status", "passed"),
            ("support_bundle_status", "passed"),
            ("agent_contract_status", "passed"),
            ("capability_discovery_status", "passed"),
            ("runtime_observability_status", "passed"),
            ("observability_schema_status", "passed"),
            ("explain_plan_only_status", "passed"),
            ("estimate_plan_only_status", "passed"),
            ("route_capability_status", "passed"),
            ("api_schema_stability_status", "passed"),
            ("docs_status", "passed"),
            ("issue_template_status", "passed"),
            ("benchmark_observability_status", "passed"),
            ("claim_gate_status", "not_claim_grade"),
        ]:
            if v1_observability_support.get(field) != expected:
                v1_observability_support_blockers.append(
                    f"v1 observability/supportability {field}="
                    + str(v1_observability_support.get(field, "missing"))
                )
        for field in [
            "v1_scope_ready",
            "observability_support_evidence_ready",
            "side_effect_free_support_surfaces",
            "support_bundle_redaction_ready",
            "all_no_fallback_no_external_engine",
        ]:
            if v1_observability_support.get(field) is not True:
                v1_observability_support_blockers.append(
                    f"v1 observability/supportability {field} must be true"
                )
        for field in [
            "telemetry_exporter_enabled",
            "remote_support_upload_enabled",
            "runtime_profile_collection_enabled",
            "public_release_claim_allowed",
            "public_package_claim_allowed",
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
            "publication_attempted",
            "tag_created",
            "package_upload_attempted",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if v1_observability_support.get(field) is not False:
                v1_observability_support_blockers.append(
                    f"v1 observability/supportability {field} must be false"
                )
    checks.append(
        check(
            "v1_observability_support_gate",
            str(args.v1_observability_support_report).replace("\\", "/"),
            v1_observability_support_blockers,
        )
    )

    v1_example_replay = load_json(v1_example_replay_report_path)
    v1_example_replay_blockers: list[str] = []
    if v1_example_replay is None:
        v1_example_replay_blockers.append("missing v1 example replay report")
    else:
        if (
            v1_example_replay.get("schema_version")
            != "shardloom.v1_example_replay_report.v1"
        ):
            v1_example_replay_blockers.append(
                "v1 example replay schema_version="
                + str(v1_example_replay.get("schema_version", "missing"))
            )
        if v1_example_replay.get("status") != "passed":
            v1_example_replay_blockers.extend(
                v1_example_replay.get("blockers", ["v1 example replay report blocked"])
            )
        for field, expected in [
            ("docs_marker_source_count", 6),
            ("runtime_command_count", 3),
            ("golden_workflow_replay_verified_count", 3),
            ("benchmark_scenario_count", 9),
            ("benchmark_expected_error_scenario_count", 0),
            ("unsupported_failure_fixture_count", 1),
            ("docs_marker_status", "passed"),
            ("runtime_command_status", "passed"),
            ("golden_workflow_replay_status", "passed"),
            ("docs_example_execution_status", "passed"),
            ("python_readme_example_execution_status", "passed"),
            ("website_example_execution_status", "passed"),
            ("quickstart_smoke_status", "passed"),
            ("benchmark_scenario_execution_status", "passed"),
            ("timing_review_status", "passed"),
            ("unsupported_failure_fixture_status", "passed"),
            ("claim_gate_status", "not_claim_grade"),
        ]:
            if v1_example_replay.get(field) != expected:
                v1_example_replay_blockers.append(
                    f"v1 example replay {field}="
                    + str(v1_example_replay.get(field, "missing"))
                )
        for field in [
            "all_no_fallback_no_external_engine",
            "correctness_claim_allowed",
        ]:
            if v1_example_replay.get(field) is not True:
                v1_example_replay_blockers.append(
                    f"v1 example replay {field} must be true"
                )
        for field in [
            "runtime_support_claim_allowed",
            "public_release_claim_allowed",
            "public_package_claim_allowed",
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
            "publication_attempted",
            "tag_created",
            "package_upload_attempted",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if v1_example_replay.get(field) is not False:
                v1_example_replay_blockers.append(
                    f"v1 example replay {field} must be false"
                )
    checks.append(
        check(
            "v1_example_replay_gate",
            str(args.v1_example_replay_report).replace("\\", "/"),
            v1_example_replay_blockers,
        )
    )

    v1_correctness_conformance = load_json(v1_correctness_conformance_report_path)
    v1_correctness_conformance_blockers: list[str] = []
    if v1_correctness_conformance is None:
        v1_correctness_conformance_blockers.append(
            "missing v1 correctness/conformance report"
        )
    else:
        if (
            v1_correctness_conformance.get("schema_version")
            != "shardloom.v1_correctness_conformance_report.v1"
        ):
            v1_correctness_conformance_blockers.append(
                "v1 correctness/conformance schema_version="
                + str(v1_correctness_conformance.get("schema_version", "missing"))
            )
        if v1_correctness_conformance.get("status") != "passed":
            v1_correctness_conformance_blockers.extend(
                v1_correctness_conformance.get(
                    "blockers",
                    ["v1 correctness/conformance report blocked"],
                )
            )
        for field, expected in [
            ("input_report_count", 8),
            ("matrix_status", "passed"),
            ("v1_correctness_matrix_status", "passed"),
            ("scope_report_status", "passed"),
            ("golden_workflow_validator_status", "passed"),
            ("admitted_semantics_validator_status", "passed"),
            ("example_replay_validator_status", "passed"),
            ("docs_example_execution_status", "passed"),
            ("unsupported_path_test_status", "passed"),
        ]:
            if v1_correctness_conformance.get(field) != expected:
                v1_correctness_conformance_blockers.append(
                    f"v1 correctness/conformance {field}="
                    + str(v1_correctness_conformance.get(field, "missing"))
                )
        for field in [
            "decoded_reference_differential_execution_performed",
            "property_execution_performed",
            "deterministic_fuzz_execution_performed",
            "correctness_claim_allowed",
            "external_engines_allowed_as_oracles_only",
        ]:
            if v1_correctness_conformance.get(field) is not True:
                v1_correctness_conformance_blockers.append(
                    f"v1 correctness/conformance {field} must be true"
                )
        if v1_correctness_conformance.get("external_oracle_used") is not False:
            v1_correctness_conformance_blockers.append(
                "v1 correctness/conformance external_oracle_used must be false"
            )
        if v1_correctness_conformance.get("claim_gate_status") != "not_claim_grade":
            v1_correctness_conformance_blockers.append(
                "v1 correctness/conformance claim_gate_status="
                + str(v1_correctness_conformance.get("claim_gate_status", "missing"))
            )
        matrix_summary = v1_correctness_conformance.get("matrix_summary", {})
        if not isinstance(matrix_summary, dict):
            v1_correctness_conformance_blockers.append(
                "v1 correctness/conformance matrix_summary must be an object"
            )
        else:
            expected_matrix_values = {
                "schema_version": "shardloom.v1_correctness_conformance_matrix.v1",
                "matrix_id": "prod-v1-2b.correctness_conformance",
                "expected_count_field_count": 34,
                "required_semantic_case_count": 47,
                "required_unsupported_case_count": 11,
                "report_input_count": 8,
                "residual_gap_count": 3,
            }
            for field, expected in expected_matrix_values.items():
                if matrix_summary.get(field) != expected:
                    v1_correctness_conformance_blockers.append(
                        f"v1 correctness/conformance matrix_summary.{field}="
                        + str(matrix_summary.get(field, "missing"))
                    )
        summaries = v1_correctness_conformance.get("summaries", {})
        if not isinstance(summaries, dict):
            v1_correctness_conformance_blockers.append(
                "v1 correctness/conformance summaries must be an object"
            )
        else:
            expected_summary_values = {
                ("admitted_semantics", "executable_fixture_count"): 117,
                ("admitted_semantics", "diagnostic_case_count"): 25,
                ("admitted_semantics", "unsupported_diagnostic_count"): 23,
                ("admitted_semantics", "property_lane_count"): 10,
                ("admitted_semantics", "deterministic_fuzz_execution_performed"): True,
                ("admitted_semantics", "deterministic_fuzz_case_count"): 5,
                ("admitted_semantics", "required_semantic_case_count"): 47,
                ("admitted_semantics", "required_unsupported_case_count"): 11,
                ("admitted_semantics", "remaining_matrix_gap_status"): "passed",
                ("admitted_semantics", "v1_runtime_scope_status"): "passed",
                ("admitted_semantics", "v1_expected_validator_case_count"): 142,
                ("admitted_semantics", "v1_required_runtime_row_count"): 142,
                ("admitted_semantics", "v1_missing_validator_case_count"): 0,
                (
                    "admitted_semantics",
                    "v1_unexpected_required_runtime_row_count",
                ): 0,
                ("admitted_semantics", "v1_support_report_row_count"): 2,
                (
                    "admitted_semantics",
                    "deterministic_unsupported_scope_status",
                ): "passed",
                ("admitted_semantics", "deterministic_unsupported_row_count"): 25,
                (
                    "admitted_semantics",
                    "deterministic_unsupported_oracle_row_count",
                ): 25,
                ("admitted_semantics", "semantic_fixture_evidence_status"): "passed",
                ("admitted_semantics", "required_executable_stage_evidence_count"): 47,
                ("admitted_semantics", "required_unsupported_stage_evidence_count"): 11,
                ("admitted_semantics", "required_stage_artifact_ref_count"): 58,
                (
                    "admitted_semantics",
                    "required_stage_decoded_reference_digest_count",
                ): 47,
                (
                    "admitted_semantics",
                    "required_stage_expected_output_digest_count",
                ): 47,
                (
                    "admitted_semantics",
                    "required_stage_observed_output_digest_count",
                ): 47,
                ("admitted_semantics", "required_stage_output_digest_match_count"): 47,
                (
                    "admitted_semantics",
                    "required_stage_expected_output_digest_source_count",
                ): 47,
                (
                    "admitted_semantics",
                    "required_stage_observed_output_digest_source_count",
                ): 47,
                ("admitted_semantics", "required_stage_correctness_digest_count"): 47,
                ("admitted_semantics", "required_stage_result_digest_count"): 47,
                (
                    "admitted_semantics",
                    "required_unsupported_stage_diagnostic_field_count",
                ): 11,
                ("admitted_semantics", "required_stage_no_fallback_count"): 58,
                ("admitted_semantics", "required_stage_no_external_engine_count"): 58,
                ("front_door", "supported_parity_row_count"): 6,
                ("front_door", "broad_pending_parity_row_count"): 4,
                ("front_door", "example_scenario_count"): 9,
                ("front_door", "expected_error_scenario_count"): 0,
                ("golden_workflow", "workflow_count"): 3,
                ("golden_workflow", "stage_count"): 9,
                ("source_prepared_state", "supported_input_format_count"): 6,
                ("source_prepared_state", "prepared_route_count"): 4,
                ("source_prepared_state", "invalidation_case_count"): 9,
                ("vortex_runtime", "primitive_route_count"): 11,
                ("vortex_runtime", "local_file_benchmark_route_count"): 15,
                ("local_output_sink", "supported_output_format_count"): 7,
                ("local_output_sink", "write_method_count"): 9,
                ("local_output_sink", "output_route_count"): 7,
                ("python_user_surface", "method_matrix_row_count"): 113,
                ("python_user_surface", "method_matrix_row_list_count"): 113,
                ("python_user_surface", "required_operation_method_count"): 13,
                (
                    "python_user_surface",
                    "required_operation_method_rows_present",
                ): 13,
                ("example_replay", "docs_marker_source_count"): 6,
                ("example_replay", "runtime_command_count"): 3,
                ("example_replay", "golden_workflow_replay_verified_count"): 3,
                ("example_replay", "benchmark_scenario_count"): 9,
                ("example_replay", "benchmark_expected_error_scenario_count"): 0,
                ("example_replay", "unsupported_failure_fixture_count"): 1,
                (
                    "example_replay",
                    "all_no_fallback_no_external_engine",
                ): True,
                ("operation_coverage", "operation_coverage_status"): "passed",
                ("operation_coverage", "operation_coverage_row_count"): 9,
                ("operation_coverage", "operation_coverage_semantic_link_count"): 28,
                ("operation_coverage", "operation_coverage_unsupported_link_count"): 2,
                (
                    "operation_coverage",
                    "operation_coverage_python_method_link_count",
                ): 45,
                (
                    "operation_coverage",
                    "operation_coverage_unique_python_method_count",
                ): 13,
                ("operation_coverage", "operation_coverage_output_digest_row_count"): 9,
                ("operation_coverage", "operation_coverage_diagnostic_row_count"): 1,
                (
                    "operation_coverage",
                    "operation_coverage_python_method_rows_present",
                ): 45,
                ("operation_coverage", "operation_coverage_no_fallback_row_count"): 9,
            }
            for (section, field), expected in expected_summary_values.items():
                section_value = summaries.get(section, {})
                value = section_value.get(field) if isinstance(section_value, dict) else None
                if value != expected:
                    v1_correctness_conformance_blockers.append(
                        f"v1 correctness/conformance {section}.{field}={value}"
                    )
        for field in [
            "runtime_support_claim_allowed",
            "public_release_claim_allowed",
            "public_package_claim_allowed",
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
            "publication_attempted",
            "tag_created",
            "package_upload_attempted",
            "fallback_attempted",
            "external_engine_invoked",
        ]:
            if v1_correctness_conformance.get(field) is not False:
                v1_correctness_conformance_blockers.append(
                    f"v1 correctness/conformance {field} must be false"
                )
    checks.append(
        check(
            "v1_correctness_conformance_gate",
            str(args.v1_correctness_conformance_report).replace("\\", "/"),
            v1_correctness_conformance_blockers,
        )
    )

    v1_security_ci_hardening = load_json(v1_security_ci_hardening_report_path)
    v1_security_ci_hardening_blockers: list[str] = []
    if v1_security_ci_hardening is None:
        v1_security_ci_hardening_blockers.append(
            "missing v1 security/CI hardening report"
        )
    else:
        if (
            v1_security_ci_hardening.get("schema_version")
            != "shardloom.v1_security_ci_hardening_report.v1"
        ):
            v1_security_ci_hardening_blockers.append(
                "v1 security/CI hardening schema_version="
                + str(v1_security_ci_hardening.get("schema_version", "missing"))
            )
        if v1_security_ci_hardening.get("status") != "passed":
            v1_security_ci_hardening_blockers.extend(
                v1_security_ci_hardening.get(
                    "blockers",
                    ["v1 security/CI hardening blocked"],
                )
            )
        for field in [
            "v1_scope_ready",
            "security_ci_hardening_evidence_ready",
            "trusted_publisher_oidc_required",
            "package_publication_requires_human_approval",
        ]:
            if v1_security_ci_hardening.get(field) is not True:
                v1_security_ci_hardening_blockers.append(
                    f"v1 security/CI hardening {field} must be true"
                )
        for field in [
            "public_release_claim_allowed",
            "public_package_claim_allowed",
            "performance_claim_allowed",
            "production_claim_allowed",
            "spark_replacement_claim_allowed",
            "publication_attempted",
            "tag_created",
            "secrets_required",
            "package_upload_attempted",
            "signing_key_used",
            "fallback_attempted",
            "external_engine_invoked",
            "long_lived_package_upload_tokens_allowed",
        ]:
            if v1_security_ci_hardening.get(field) is not False:
                v1_security_ci_hardening_blockers.append(
                    f"v1 security/CI hardening {field} must be false"
                )
        if v1_security_ci_hardening.get("claim_gate_status") != "not_claim_grade":
            v1_security_ci_hardening_blockers.append(
                "v1 security/CI hardening claim_gate_status="
                + str(v1_security_ci_hardening.get("claim_gate_status", "missing"))
            )
    checks.append(
        check(
            "v1_security_ci_hardening_gate",
            str(args.v1_security_ci_hardening_report).replace("\\", "/"),
            v1_security_ci_hardening_blockers,
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
            "public-workflow run",
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
        "python scripts/check_workspace_version_sources.py",
        "python scripts/check_release_security_gate.py",
        "python scripts/check_release_architecture_tracker.py --allow-blocked",
        "python scripts/check_package_channel_readiness.py --require-local-evidence",
        "python scripts/check_golden_workflows.py",
        "python scripts/check_admitted_semantics_matrix.py",
        "python scripts/check_runtime_execution_envelopes.py",
        "python scripts/check_website_readiness.py",
        "python scripts/final_release_rehearsal.py --allow-blocked",
        "python scripts/check_production_usability_gate.py",
        "python scripts/check_python_user_surface_completion.py",
        "python scripts/check_sql_python_dataframe_parity.py",
        "python scripts/check_v1_front_door_runtime_scope.py",
        "python scripts/check_v1_vortex_runtime_scope.py",
        "python scripts/check_v1_source_prepared_state_scope.py",
        "python scripts/check_v1_local_output_sink_scope.py",
        "python scripts/check_local_format_production_profiles.py",
        "python scripts/check_local_format_pushdown_fidelity.py",
        "python scripts/check_compatibility_output_translation_reports.py",
        "python scripts/check_local_format_edge_case_fixtures.py",
        "python scripts/check_v1_local_resource_safety.py",
        "python scripts/check_v1_observability_support.py",
        "python scripts/check_v1_api_schema_stability.py",
        "python scripts/check_v1_example_replay.py",
        "python scripts/check_v1_correctness_conformance.py",
        "python scripts/check_v1_security_ci_hardening.py",
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
        "workspace_version_source_report_ref": str(
            args.workspace_version_source_report
        ).replace("\\", "/"),
        "contribution_governance_report_ref": str(args.contribution_governance_report).replace(
            "\\", "/"
        ),
        "golden_workflow_report_ref": str(args.golden_workflow_report).replace("\\", "/"),
        "admitted_semantics_report_ref": str(args.admitted_semantics_report).replace("\\", "/"),
        "per_claim_evidence_matrix_ref": str(args.per_claim_evidence_matrix).replace("\\", "/"),
        "architecture_tracker_report_ref": str(args.architecture_tracker_report).replace("\\", "/"),
        "final_release_rehearsal_report_ref": str(args.final_release_rehearsal_report).replace("\\", "/"),
        "production_usability_report_ref": str(args.production_usability_report).replace("\\", "/"),
        "v1_front_door_runtime_scope_report_ref": str(
            args.v1_front_door_runtime_scope_report
        ).replace("\\", "/"),
        "v1_vortex_runtime_scope_report_ref": str(
            args.v1_vortex_runtime_scope_report
        ).replace("\\", "/"),
        "v1_source_prepared_state_scope_report_ref": str(
            args.v1_source_prepared_state_scope_report
        ).replace("\\", "/"),
        "v1_local_output_sink_scope_report_ref": str(
            args.v1_local_output_sink_scope_report
        ).replace("\\", "/"),
        "v1_local_resource_safety_report_ref": str(
            args.v1_local_resource_safety_report
        ).replace("\\", "/"),
        "v1_observability_support_report_ref": str(
            args.v1_observability_support_report
        ).replace("\\", "/"),
        "v1_api_schema_stability_report_ref": str(
            args.v1_api_schema_stability_report
        ).replace("\\", "/"),
        "v1_example_replay_report_ref": str(
            args.v1_example_replay_report
        ).replace("\\", "/"),
        "v1_correctness_conformance_report_ref": str(
            args.v1_correctness_conformance_report
        ).replace("\\", "/"),
        "v1_security_ci_hardening_report_ref": str(
            args.v1_security_ci_hardening_report
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
