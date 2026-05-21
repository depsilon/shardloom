#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Check ShardLoom open-source security posture configuration."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "target" / "security-posture-report.json"
SCHEMA_VERSION = "shardloom.open_source_security_posture_report.v1"


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


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
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
