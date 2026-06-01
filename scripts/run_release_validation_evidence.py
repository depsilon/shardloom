#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Run and record local hard release-readiness validation evidence.

This script is intentionally local-only. It does not publish packages, create tags, add secrets,
or invoke external fallback engines.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.release_validation_evidence.v1"

SUPPORTING_COMMANDS = [
    (
        "dependency_audit_release_gate",
        [
            sys.executable,
            "scripts/check_dependency_audit.py",
            "--release-gate",
            "--json-output",
            "target/dependency-audit-report.json",
        ],
        "security_dependency_provenance",
    ),
]

FEATURE_MATRIX_COMMANDS = [
    ("default_features", ["cargo", "check", "--workspace"], False),
    ("all_features", ["cargo", "check", "--workspace", "--all-features"], False),
    ("no_default_features", ["cargo", "check", "--workspace", "--no-default-features"], False),
    ("upstream_vortex", ["cargo", "check", "-p", "shardloom-vortex", "--features", "upstream-vortex"], False),
    ("vortex_file_io", ["cargo", "check", "-p", "shardloom-vortex", "--features", "vortex-file-io"], False),
    (
        "vortex_local_primitives",
        ["cargo", "check", "-p", "shardloom-vortex", "--features", "vortex-local-primitives"],
        False,
    ),
    (
        "vortex_encoded_read_spike",
        ["cargo", "check", "-p", "shardloom-vortex", "--features", "vortex-encoded-read-spike"],
        False,
    ),
    (
        "packaging_deployment",
        ["cargo", "test", "-p", "shardloom-contract-tests", "--test", "conda_packaging_recipes"],
        False,
    ),
    (
        "benchmark_extras",
        ["cargo", "check", "-p", "shardloom-vortex", "--features", "vortex-traditional-analytics-benchmark"],
        False,
    ),
    ("future_foundry_optional", [], True),
]

REQUIRED_VALIDATION_COMMANDS = [
    ("cargo_fmt", ["cargo", "fmt", "--all", "--", "--check"]),
    ("cargo_clippy_workspace", ["cargo", "clippy", "--workspace", "--all-targets", "--", "-D", "warnings"]),
    ("cargo_test_workspace", ["cargo", "test", "--workspace", "--all-targets"]),
    ("python_unittest", [sys.executable, "-m", "unittest", "discover", "python/tests"]),
    ("python_build", [sys.executable, "-m", "build", "python"]),
    (
        "release_dry_run_proof",
        [sys.executable, "scripts/release_dry_run_proof.py", "--rows", "64", "--iterations", "1"],
    ),
    (
        "global_architecture_gate",
        ["cargo", "run", "-q", "-p", "shardloom-cli", "--", "global-architecture-gate", "--format", "json"],
    ),
    ("contribution_governance", [sys.executable, "scripts/check_contribution_governance.py"]),
    ("ci_gate_matrix_contract", [sys.executable, "scripts/check_ci_gate_matrix.py"]),
    ("release_security_gate", [sys.executable, "scripts/check_release_security_gate.py"]),
    (
        "release_architecture_tracker",
        [sys.executable, "scripts/check_release_architecture_tracker.py", "--allow-blocked"],
    ),
    (
        "package_channel_readiness",
        [sys.executable, "scripts/check_package_channel_readiness.py", "--require-local-evidence"],
    ),
    ("golden_workflow_validator", [sys.executable, "scripts/check_golden_workflows.py"]),
    ("admitted_semantics_matrix", [sys.executable, "scripts/check_admitted_semantics_matrix.py"]),
    ("runtime_execution_envelopes", [sys.executable, "scripts/check_runtime_execution_envelopes.py"]),
    ("website_readiness", [sys.executable, "scripts/check_website_readiness.py"]),
    (
        "benchmark_artifact_completeness",
        [
            sys.executable,
            "scripts/check_benchmark_artifact_completeness.py",
            "--manifest",
            "website/assets/benchmarks/latest/manifest.json",
        ],
    ),
    (
        "pre_5j_dependency_freshness_gate",
        [sys.executable, "scripts/check_pre_5j_dependency_freshness.py"],
    ),
    (
        "benchmark_publication_claim_gate",
        [
            sys.executable,
            "scripts/check_benchmark_publication_claim_gate.py",
            "--manifest",
            "website/assets/benchmarks/latest/manifest.json",
        ],
    ),
    (
        "final_release_rehearsal",
        [sys.executable, "scripts/final_release_rehearsal.py", "--allow-blocked"],
    ),
    ("production_usability_gate", [sys.executable, "scripts/check_production_usability_gate.py"]),
    (
        "python_user_surface_completion_gate",
        [sys.executable, "scripts/check_python_user_surface_completion.py"],
    ),
    (
        "sql_python_dataframe_parity_gate",
        [sys.executable, "scripts/check_sql_python_dataframe_parity.py"],
    ),
    (
        "user_surface_runtime_gap_inventory",
        [sys.executable, "scripts/check_user_surface_runtime_gap_inventory.py"],
    ),
    (
        "user_route_capability_report",
        [sys.executable, "scripts/check_user_route_capability_report.py"],
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=Path("target/release-validation-evidence.json"))
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--skip-slow", action="store_true", help="Record only fast metadata for local inspection.")
    return parser.parse_args()


def command_text(command: list[str]) -> str:
    return " ".join(command).replace(sys.executable, "python")


def tail(text: str, limit: int = 4000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


def run_command(repo_root: Path, name: str, command: list[str], group: str) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(command, cwd=repo_root, text=True, capture_output=True, check=False)
    elapsed = (time.perf_counter() - started) * 1000.0
    return {
        "name": name,
        "group": group,
        "command": command_text(command),
        "argv": command,
        "returncode": completed.returncode,
        "status": "passed" if completed.returncode == 0 else "failed",
        "elapsed_millis": round(elapsed, 4),
        "stdout_tail": tail(completed.stdout),
        "stderr_tail": tail(completed.stderr),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = args.output if args.output.is_absolute() else repo_root / args.output
    results: list[dict[str, Any]] = []

    planned: list[tuple[str, list[str], str]] = []
    planned.extend((name, command, group) for name, command, group in SUPPORTING_COMMANDS)
    planned.extend(
        (name, command, "feature_build_matrix")
        for name, command, skip in FEATURE_MATRIX_COMMANDS
        if not skip
    )
    if not args.skip_slow:
        planned.extend((name, command, "required_validation") for name, command in REQUIRED_VALIDATION_COMMANDS)

    for name, command, group in planned:
        result = run_command(repo_root, name, command, group)
        results.append(result)
        if result["returncode"] != 0 and not args.continue_on_failure:
            break

    command_status = {result["command"]: result["status"] for result in results}
    feature_rows = []
    for name, command, skip in FEATURE_MATRIX_COMMANDS:
        if skip:
            feature_rows.append(
                {
                    "feature_set": name,
                    "command": "not applicable yet",
                    "status": "not_applicable_yet",
                    "release_blocking": False,
                }
            )
            continue
        text = command_text(command)
        feature_rows.append(
            {
                "feature_set": name,
                "command": text,
                "status": command_status.get(text, "not_run"),
                "release_blocking": command_status.get(text) != "passed",
            }
        )

    required_rows = []
    for name, command in REQUIRED_VALIDATION_COMMANDS:
        text = command_text(command)
        required_rows.append(
            {
                "name": name,
                "command": text,
                "status": command_status.get(text, "not_run"),
                "release_blocking": command_status.get(text) != "passed",
            }
        )

    feature_matrix_passed = all(not row["release_blocking"] for row in feature_rows)
    required_validation_passed = args.skip_slow or all(
        not row["release_blocking"] for row in required_rows
    )
    supporting_passed = all(
        result["status"] == "passed" for result in results if result["group"] == "security_dependency_provenance"
    )
    passed = feature_matrix_passed and required_validation_passed and supporting_passed

    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "feature_build_matrix_status": "passed" if feature_matrix_passed else "failed",
        "required_validation_status": "skipped_slow"
        if args.skip_slow
        else ("passed" if required_validation_passed else "failed"),
        "supporting_security_dependency_status": "passed" if supporting_passed else "failed",
        "feature_build_matrix_rows": feature_rows,
        "required_validation_commands": required_rows,
        "command_results": results,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(output)
    return 0 if passed or args.continue_on_failure else 1


if __name__ == "__main__":
    raise SystemExit(main())
