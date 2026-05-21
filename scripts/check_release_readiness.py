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
from pathlib import Path
from typing import Any

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


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    dry_run_path = resolve(repo_root, args.release_dry_run_transcript)
    security_gate_path = resolve(repo_root, args.security_gate_report)
    validation_evidence_path = resolve(repo_root, args.validation_evidence)
    package_channel_matrix_path = resolve(repo_root, args.package_channel_matrix)
    per_claim_evidence_matrix_path = resolve(repo_root, args.per_claim_evidence_matrix)
    architecture_tracker_report_path = resolve(repo_root, args.architecture_tracker_report)
    final_release_rehearsal_report_path = resolve(repo_root, args.final_release_rehearsal_report)

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
        "per_claim_evidence_attachment_matrix_spark_displacement_claim_allowed=false",
        "per_claim_evidence_attachment_matrix_fallback_attempted=false",
        "per_claim_evidence_attachment_matrix_external_engine_invoked=false",
        "public_release_claim",
        "public_package_claim",
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
    if not evidence_schema_registry.exists():
        typed_blockers.append("missing evidence schema registry source")
    if "shardloom.evidence_field_schema_registry.v1" not in read_text(evidence_schema_doc):
        typed_blockers.append("missing evidence schema registry status doc")
    if not evidence_schema_validator.exists():
        typed_blockers.append("missing evidence schema registry validator script")
    checks.append(check("typed_envelope_compatibility", "shardloom-cli/tests/typed_envelope_contract_snapshots.rs", typed_blockers))

    validation_commands = [
        "cargo fmt --all -- --check",
        "cargo clippy --workspace --all-targets -- -D warnings",
        "cargo test --workspace --all-targets",
        "python -m unittest discover python/tests",
        "python -m build python",
        "python scripts/release_dry_run_proof.py --rows 64 --iterations 1",
        "cargo run -q -p shardloom-cli -- global-architecture-gate --format json",
        "python scripts/check_ci_gate_matrix.py",
        "python scripts/check_release_security_gate.py",
        "python scripts/check_release_architecture_tracker.py --allow-blocked",
        "python scripts/check_package_channel_readiness.py",
        "python scripts/final_release_rehearsal.py --allow-blocked",
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
        if command_status.get(command) != "passed":
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
        "per_claim_evidence_matrix_ref": str(args.per_claim_evidence_matrix).replace("\\", "/"),
        "architecture_tracker_report_ref": str(args.architecture_tracker_report).replace("\\", "/"),
        "final_release_rehearsal_report_ref": str(args.final_release_rehearsal_report).replace("\\", "/"),
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
