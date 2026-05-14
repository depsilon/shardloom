#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Aggregate ShardLoom security evidence for the hard release gate."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.release_security_gate_report.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--dependency-audit-report",
        type=Path,
        default=Path("target/dependency-audit-report.json"),
    )
    parser.add_argument(
        "--provenance-report",
        type=Path,
        default=Path("target/release-provenance-dry-run/supply-chain-release-evidence.json"),
    )
    parser.add_argument(
        "--security-posture-report",
        type=Path,
        default=Path("target/security-posture-report.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/release-security-gate-report.json"),
    )
    parser.add_argument(
        "--allow-blocked",
        action="store_true",
        help="Write the report and exit 0 even when the gate is blocked.",
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def status(passed: bool, ref: str, blockers: list[str]) -> dict[str, Any]:
    return {
        "status": "passed" if passed else "blocked",
        "ref": ref,
        "blockers": blockers,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    dep_report_path = resolve(repo_root, args.dependency_audit_report)
    provenance_path = resolve(repo_root, args.provenance_report)
    posture_path = resolve(repo_root, args.security_posture_report)

    checks: dict[str, dict[str, Any]] = {}

    threat_model = repo_root / "docs/security/threat-model.md"
    threat_text = read_text(threat_model) if threat_model.exists() else ""
    threat_blockers = [
        item
        for item in [
            "SecurityThreatModelReport",
            "RuntimeInputSafetyReport",
            "WorkspacePathSafetyReport",
            "EvidenceArtifactSafetyReport",
            "SEC-4 deterministic regression",
        ]
        if item not in threat_text
    ]
    checks["SecurityThreatModelReport"] = status(
        threat_model.exists() and not threat_blockers,
        "docs/security/threat-model.md",
        threat_blockers or ([] if threat_model.exists() else ["missing threat model"]),
    )

    security_policy = repo_root / "SECURITY.md"
    security_text = read_text(security_policy) if security_policy.exists() else ""
    security_blockers = [
        item
        for item in [
            "Reporting A Vulnerability",
            "Security Release Policy",
            "Compromised Package Or Dependency Response",
            "No-Fallback Security Invariant",
        ]
        if item not in security_text
    ]
    checks["VulnerabilityResponseReport"] = status(
        security_policy.exists() and not security_blockers,
        "SECURITY.md",
        security_blockers or ([] if security_policy.exists() else ["missing SECURITY.md"]),
    )

    dep_report = load_json(dep_report_path)
    dep_blockers = []
    if dep_report is None:
        dep_blockers.append("missing DependencyAuditReport JSON")
    else:
        for field in ["cargo_deny_status", "cargo_audit_status", "pip_audit_status"]:
            if dep_report.get(field) != "passed":
                dep_blockers.append(f"{field}={dep_report.get(field)}")
        if dep_report.get("fallback_dependency_absent") is not True:
            dep_blockers.append("fallback_dependency_absent is not true")
    checks["DependencyAuditReport"] = status(
        dep_report is not None and not dep_blockers,
        str(args.dependency_audit_report).replace("\\", "/"),
        dep_blockers,
    )

    provenance = load_json(provenance_path)
    provenance_blockers = []
    if provenance is None:
        provenance_blockers.append("missing SupplyChainReleaseEvidence JSON")
    else:
        if provenance.get("schema_version") != "shardloom.supply_chain_release_evidence.v1":
            provenance_blockers.append("unexpected provenance schema")
        if not provenance.get("sbom_refs"):
            provenance_blockers.append("missing SBOM refs")
        if not provenance.get("checksum_refs"):
            provenance_blockers.append("missing checksum refs")
        if provenance.get("publication_attempted") is not False:
            provenance_blockers.append("publication_attempted must be false")
        if provenance.get("tag_created") is not False:
            provenance_blockers.append("tag_created must be false")
        if provenance.get("secrets_required") is not False:
            provenance_blockers.append("secrets_required must be false")
        if provenance.get("fallback_dependency_absent") is not True:
            provenance_blockers.append("fallback_dependency_absent is not true")
    checks["SupplyChainReleaseEvidence"] = status(
        provenance is not None and not provenance_blockers,
        str(args.provenance_report).replace("\\", "/"),
        provenance_blockers,
    )

    security_rs = repo_root / "shardloom-core/src/security.rs"
    runtime_text = read_text(security_rs) if security_rs.exists() else ""
    runtime_blockers = [
        item
        for item in [
            "RuntimeInputSafetyReport",
            "WorkspacePathSafetyReport",
            "EvidenceArtifactSafetyReport",
            "runtime_input_safety_report_blocks_malformed_inputs_without_fallback",
            "workspace_path_safety_rejects_parent_traversal_and_external_outputs",
            "evidence_artifact_safety_redacts_credential_like_values",
        ]
        if item not in runtime_text
    ]
    checks["RuntimeInputSafetyReport"] = status(
        security_rs.exists() and not runtime_blockers,
        "shardloom-core/src/security.rs",
        runtime_blockers,
    )

    posture = load_json(posture_path)
    posture_blockers = []
    if posture is None:
        posture_blockers.append("missing open-source security posture JSON")
    elif posture.get("status") != "passed":
        posture_blockers.append(f"security posture status={posture.get('status')}")
    checks["OpenSourceSecurityPostureReport"] = status(
        posture is not None and not posture_blockers,
        str(args.security_posture_report).replace("\\", "/"),
        posture_blockers,
    )

    unsupported = repo_root / "docs/release/known-unsupported-paths.md"
    unsupported_text = read_text(unsupported) if unsupported.exists() else ""
    unsupported_blockers = [
        item
        for item in [
            "broad SQL/DataFrame execution",
            "live/hybrid production behavior",
            "object-store runtime",
            "Foundry proof-of-use",
            "fallback_attempted=false",
        ]
        if item not in unsupported_text
    ]
    checks["KnownUnsupportedPathsReport"] = status(
        unsupported.exists() and not unsupported_blockers,
        "docs/release/known-unsupported-paths.md",
        unsupported_blockers or ([] if unsupported.exists() else ["missing known unsupported paths"]),
    )

    blockers = [
        f"{name}: {blocker}"
        for name, check in checks.items()
        for blocker in check["blockers"]
    ]
    passed = not blockers
    report = {
        "schema_version": SCHEMA_VERSION,
        "release_ref": "local-security-gate",
        "status": "passed" if passed else "blocked",
        "public_release_claim_allowed": passed,
        "security_evidence_required": True,
        "checks": checks,
        "blockers": blockers,
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
