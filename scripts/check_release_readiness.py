#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Aggregate hard release-readiness evidence for ShardLoom.

The gate requires feature/build matrix execution evidence, not only matrix documentation.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


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
    checks.append(check("typed_envelope_compatibility", "shardloom-cli/tests/typed_envelope_contract_snapshots.rs", typed_blockers))

    validation_commands = [
        "cargo fmt --all -- --check",
        "cargo clippy --workspace --all-targets -- -D warnings",
        "cargo test --workspace --all-targets",
        "python -m unittest discover python/tests",
        "python -m build python",
        "python scripts/release_dry_run_proof.py --rows 64 --iterations 1",
        "cargo run -q -p shardloom-cli -- global-architecture-gate --format json",
        "python scripts/check_release_security_gate.py",
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
