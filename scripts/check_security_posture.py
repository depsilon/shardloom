#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Check ShardLoom open-source security posture configuration."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "target" / "security-posture-report.json"
SCHEMA_VERSION = "shardloom.open_source_security_posture_report.v1"
ACTION_USE_RE = re.compile(r"^\s*(?:-\s*)?uses:\s*([^#\s]+)", re.MULTILINE)
FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--json-output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-json", action="store_true")
    return parser.parse_args()


def read(repo_root: Path, path: str) -> str:
    return (repo_root / path).read_text(encoding="utf-8")


def check_contains(text: str, required: list[str]) -> dict[str, Any]:
    missing = [item for item in required if item not in text]
    return {"status": "passed" if not missing else "failed", "missing": missing}


def action_pin_check(text: str) -> dict[str, Any]:
    mutable_refs: list[str] = []
    pinned_refs: list[str] = []
    for match in ACTION_USE_RE.finditer(text):
        action_ref = match.group(1)
        _, sep, ref = action_ref.rpartition("@")
        if sep != "@" or not FULL_SHA_RE.fullmatch(ref):
            mutable_refs.append(action_ref)
        else:
            pinned_refs.append(action_ref)
    return {
        "status": "passed" if not mutable_refs else "failed",
        "mutable_refs": mutable_refs,
        "pinned_ref_count": len(pinned_refs),
    }


def pypi_trusted_publisher_boundary_check(text: str) -> dict[str, Any]:
    required = [
        "channel:",
        "testpypi_proof_ref:",
        "build:",
        "permissions:\n      contents: read",
        "Upload Python dist artifact",
        "publish-testpypi:",
        "environment: testpypi",
        "Publish to TestPyPI with Trusted Publisher",
        "repository-url: https://test.pypi.org/legacy/",
        "publish:",
        "needs: build",
        "environment: pypi",
        "id-token: write",
        "Download Python dist artifact",
        "Publish to PyPI with Trusted Publisher",
    ]
    missing = [item for item in required if item not in text]
    build_section = text.split("\n  publish-testpypi:", 1)[0]
    testpypi_section = (
        text.split("\n  publish-testpypi:", 1)[1].split("\n  publish:", 1)[0]
        if "\n  publish-testpypi:" in text
        else ""
    )
    publish_section = text.split("\n  publish:", 1)[1] if "\n  publish:" in text else ""
    if "id-token: write" in build_section:
        missing.append("build job must not grant id-token: write")
    if "python -m build python" in testpypi_section:
        missing.append("publish-testpypi job must not build the package")
    if "python -m build python" in publish_section:
        missing.append("publish job must not build the package")
    if "inputs.testpypi_proof_ref != ''" not in publish_section:
        missing.append("publish job must require prior TestPyPI proof ref")
    return {"status": "passed" if not missing else "failed", "missing": missing}


def build_report(repo_root: Path) -> dict[str, Any]:
    checks: dict[str, dict[str, Any]] = {}

    codeql = read(repo_root, ".github/workflows/codeql-analysis.yml")
    checks["codeql_workflow"] = check_contains(
        codeql,
        [
            "workflow_dispatch:",
            "pull_request:",
            "security-events: write",
            "github/codeql-action/init@v4",
            "github/codeql-action/analyze@v4",
            "language: rust",
            "language: python",
            "build-mode: none",
        ],
    )
    checks["codeql_action_pinning"] = action_pin_check(codeql)

    scorecard = read(repo_root, ".github/workflows/scorecard.yml")
    checks["scorecard_workflow"] = check_contains(
        scorecard,
        [
            "workflow_dispatch:",
            "ossf/scorecard-action@v2.4.3",
            "publish_results: false",
            "github/codeql-action/upload-sarif@v4",
            "security-events: write",
            "persist-credentials: false",
        ],
    )
    checks["scorecard_action_pinning"] = action_pin_check(scorecard)

    pypi_publish = read(repo_root, ".github/workflows/pypi-publish-draft.yml")
    checks["pypi_trusted_publisher_boundary"] = check_contains(
        pypi_publish,
        [
            "workflow_dispatch:",
            "publish-approved",
            "environment: testpypi",
            "environment: pypi",
            "id-token: write",
            "repository-url: https://test.pypi.org/legacy/",
            "pypa/gh-action-pypi-publish@release/v1",
        ],
    )
    checks["pypi_trusted_publisher_action_pinning"] = action_pin_check(pypi_publish)
    checks["pypi_trusted_publisher_oidc_boundary"] = pypi_trusted_publisher_boundary_check(
        pypi_publish
    )

    dependabot = read(repo_root, ".github/dependabot.yml")
    checks["dependabot_config"] = check_contains(
        dependabot,
        [
            'package-ecosystem: "cargo"',
            'package-ecosystem: "pip"',
            'package-ecosystem: "github-actions"',
            "directory: \"/\"",
            "directory: \"/python\"",
            'interval: "weekly"',
        ],
    )

    doc = read(repo_root, "docs/security/open-source-security-posture.md")
    checks["posture_doc"] = check_contains(
        doc,
        [
            "CodeQL",
            "OpenSSF Scorecard",
            "Dependabot",
            "secret scanning",
            "push protection",
            "branch protection",
            "required checks",
            "no-fallback",
        ],
    )

    passed = all(check["status"] == "passed" for check in checks.values())
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "checks": checks,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    return report


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    report = build_report(repo_root)
    passed = report["status"] == "passed"

    if not args.no_json:
        output = args.json_output if args.json_output.is_absolute() else repo_root / args.json_output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(output)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
