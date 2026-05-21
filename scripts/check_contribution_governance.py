#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate ShardLoom contribution intake governance posture.

This script checks documentation and PR-template controls only. It does not activate a CLA
assistant, add a DCO route, publish packages, create tags, add secrets, or authorize fallback
execution.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.contribution_governance_report.v1"


@dataclass(frozen=True)
class TextCheck:
    check_id: str
    path: Path
    required_markers: tuple[str, ...]


REQUIRED_CHECKS: tuple[TextCheck, ...] = (
    TextCheck(
        check_id="contributing_entrypoint",
        path=Path("CONTRIBUTING.md"),
        required_markers=(
            "Outside contributions are not automatically accepted",
            "Contribution Governance Controls",
            "required signoff/CLA/DCO state",
            "Maintainer Roles And Review States",
            "Decision Escalation",
            "No external CLA Assistant",
            "DCO remains inactive",
            "docs/legal/contribution-intake-readiness.md",
            "docs/legal/contributor-policy.md",
            "docs/legal/license-provenance.md",
            "SECURITY.md",
            "AGENTS.md",
            "docs/skills/license-provenance.md",
            "docs/skills/release-engineering-packaging.md",
            "docs/architecture/phased-execution-plan.md",
            "no Spark, DataFusion, DuckDB, Polars, Velox",
        ),
    ),
    TextCheck(
        check_id="contributor_policy",
        path=Path("docs/legal/contributor-policy.md"),
        required_markers=(
            "Contribution Intake Governance Gate",
            "shardloom.contribution_governance_report.v1",
            "required signoff/CLA/DCO state",
            "review-state reporting",
            "decision escalation",
            "documented_policy_only",
            "No external CLA Assistant is active",
            "DCO remains inactive",
            "External contribution acceptance remains blocked until maintainer approval",
            "fallback_attempted=false",
            "external_engine_invoked=false",
        ),
    ),
    TextCheck(
        check_id="contribution_intake_readiness_doc",
        path=Path("docs/legal/contribution-intake-readiness.md"),
        required_markers=(
            "shardloom.contribution_governance_report.v1",
            "contribution_intake_status=documented_and_ci_checked",
            "external_contribution_acceptance_status=maintainer_approval_required",
            "cla_assistant_status=not_active",
            "dco_policy_status=not_active",
            "legal_claim_status=documented_policy_only",
            "automated_control=ci_contribution_governance_validator",
            "documented_control=reviewer_roles_and_decision_escalation",
            "blocked_control=external_cla_assistant",
            "blocked_control=dco_signoff_route",
            "fallback_attempted=false",
            "external_engine_invoked=false",
        ),
    ),
    TextCheck(
        check_id="pull_request_template",
        path=Path(".github/PULL_REQUEST_TEMPLATE.md"),
        required_markers=(
            "Contribution Route",
            "required signoff/CLA/DCO state",
            "I understand outside contributions require maintainer approval",
            "DCO remains inactive",
            "No external CLA Assistant is active",
            "Claim Boundary",
            "No-Fallback And Dependency Check",
            "Security, Release, And RFC Impact",
            "Reviewer State",
            "runtime fallback dependency",
        ),
    ),
    TextCheck(
        check_id="ci_workflow",
        path=Path(".github/workflows/ci.yml"),
        required_markers=(
            "Contribution governance",
            "python scripts/check_contribution_governance.py",
            "target/contribution-governance-report.json",
        ),
    ),
    TextCheck(
        check_id="ci_gate_matrix_doc",
        path=Path("docs/release/ci-gate-matrix.md"),
        required_markers=(
            "python scripts/check_contribution_governance.py",
            "target/contribution-governance-report.json",
            "contribution governance",
            "contribution_governance",
        ),
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/contribution-governance-report.json"),
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Print the report instead of writing the default JSON artifact.",
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def has_marker(text: str, marker: str) -> bool:
    if marker in text:
        return True
    return " ".join(marker.split()) in " ".join(text.split())


def run_check(repo_root: Path, check: TextCheck) -> dict[str, Any]:
    target = repo_root / check.path
    text = read_text(target)
    blockers: list[str] = []
    if not target.exists():
        blockers.append(f"missing {check.path.as_posix()}")
    for marker in check.required_markers:
        if not has_marker(text, marker):
            blockers.append(f"missing marker: {marker}")
    return {
        "check_id": check.check_id,
        "path": check.path.as_posix(),
        "status": "passed" if not blockers else "failed",
        "required_marker_count": len(check.required_markers),
        "blockers": blockers,
    }


def build_report(repo_root: Path) -> dict[str, Any]:
    rows = [run_check(repo_root, check) for check in REQUIRED_CHECKS]
    blockers = [
        f"{row['check_id']}: {blocker}"
        for row in rows
        for blocker in row["blockers"]
    ]
    passed = not blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "contribution_intake_status": "documented_and_ci_checked" if passed else "incomplete",
        "external_contribution_acceptance_status": "maintainer_approval_required",
        "cla_assistant_status": "not_active",
        "dco_policy_status": "not_active",
        "legal_claim_status": "documented_policy_only",
        "review_state_reporting_status": "pr_template_and_ci_checked" if passed else "incomplete",
        "decision_escalation_status": "documented" if passed else "incomplete",
        "automated_controls": [
            "ci_contribution_governance_validator",
            "pr_template_marker_check",
            "release_readiness_report_integration",
        ],
        "documented_controls": [
            "required_signoff_cla_dco_state",
            "reviewer_roles_and_decision_escalation",
            "dependency_license_provenance_checklist",
            "security_release_rfc_checklist",
            "claim_boundary_checklist",
            "no_fallback_dependency_policy",
        ],
        "blocked_controls": [
            "external_cla_assistant",
            "dco_signoff_route",
            "broad_governance_transfer",
            "package_publication_from_contribution_gate",
        ],
        "checks": rows,
        "blockers": blockers,
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    report = build_report(repo_root)
    if args.no_json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        output = resolve(repo_root, args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
