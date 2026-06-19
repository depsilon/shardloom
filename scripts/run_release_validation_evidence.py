#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Run and record local hard release-readiness validation evidence.

This script is intentionally local-only. It does not publish packages, create tags, add secrets,
or invoke external fallback engines.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.release_validation_evidence.v1"

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
        "release_user_surfaces",
        ["cargo", "check", "-p", "shardloom-vortex", "--features", "release-user-surfaces"],
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
SKIPPED_SLOW_STATUS = "skipped_slow"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=Path("target/release-validation-evidence.json"))
    parser.add_argument(
        "--python-executable",
        type=Path,
        help=(
            "Python >=3.10 executable to use for package/Python release checks. "
            "Defaults to the interpreter running this script."
        ),
    )
    parser.add_argument(
        "--pip-audit-python",
        type=Path,
        help="Python executable with pip-audit installed for the dependency audit gate.",
    )
    parser.add_argument(
        "--require-clean-conda",
        action="store_true",
        help="Require clean Conda/mamba/micromamba install proof in release_dry_run_proof.",
    )
    parser.add_argument(
        "--conda-executable",
        type=Path,
        help="Explicit conda, mamba, or micromamba executable for release_dry_run_proof.",
    )
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--skip-slow", action="store_true", help="Record only fast metadata for local inspection.")
    return parser.parse_args()


def release_python(args: argparse.Namespace) -> str:
    return str(args.python_executable) if args.python_executable else sys.executable


def command_text(command: list[str], python_executable: str | None = None) -> str:
    text = " ".join(command)
    if python_executable:
        text = text.replace(python_executable, "python")
    return text.replace(sys.executable, "python")


def dependency_audit_env(pip_audit_python: Path | None) -> dict[str, str]:
    if pip_audit_python is None:
        return {}
    return {"SHARDLOOM_PIP_AUDIT_PYTHON": str(pip_audit_python)}


def supporting_commands(
    python_executable: str,
    pip_audit_python: Path | None,
) -> list[tuple[str, list[str], str, dict[str, str]]]:
    return [
        (
            "dependency_audit_release_gate",
            [
                python_executable,
                "scripts/check_dependency_audit.py",
                "--release-gate",
                "--json-output",
                "target/dependency-audit-report.json",
            ],
            "security_dependency_provenance",
            dependency_audit_env(pip_audit_python),
        ),
        (
            "security_posture",
            [
                python_executable,
                "scripts/check_security_posture.py",
                "--json-output",
                "target/security-posture-report.json",
            ],
            "security_dependency_provenance",
            {},
        ),
    ]


def release_dry_run_command(python_executable: str, args: argparse.Namespace) -> list[str]:
    command = [
        python_executable,
        "scripts/release_dry_run_proof.py",
        "--rows",
        "64",
        "--iterations",
        "1",
    ]
    if args.require_clean_conda:
        command.append("--require-clean-conda")
    if args.conda_executable:
        command.extend(["--conda-executable", str(args.conda_executable)])
    return command


def required_validation_commands(
    python_executable: str,
    args: argparse.Namespace,
) -> list[tuple[str, list[str]]]:
    return [
        ("cargo_fmt", ["cargo", "fmt", "--all", "--", "--check"]),
        (
            "cargo_clippy_workspace",
            ["cargo", "clippy", "--workspace", "--all-targets", "--", "-D", "warnings"],
        ),
        ("cargo_test_workspace", ["cargo", "test", "--workspace", "--all-targets"]),
        ("python_unittest", [python_executable, "-m", "unittest", "discover", "python/tests"]),
        ("python_build", [python_executable, "-m", "build", "python"]),
        ("release_dry_run_proof", release_dry_run_command(python_executable, args)),
        (
            "global_architecture_gate",
            [
                "cargo",
                "run",
                "-q",
                "-p",
                "shardloom-cli",
                "--",
                "global-architecture-gate",
                "--format",
                "json",
            ],
        ),
        ("contribution_governance", [python_executable, "scripts/check_contribution_governance.py"]),
        ("ci_gate_matrix_contract", [python_executable, "scripts/check_ci_gate_matrix.py"]),
        (
            "workspace_version_source_contract",
            [python_executable, "scripts/check_workspace_version_sources.py"],
        ),
        ("release_security_gate", [python_executable, "scripts/check_release_security_gate.py"]),
        (
            "release_architecture_tracker",
            [python_executable, "scripts/check_release_architecture_tracker.py", "--allow-blocked"],
        ),
        (
            "package_channel_readiness",
            [
                python_executable,
                "scripts/check_package_channel_readiness.py",
                "--require-local-evidence",
            ],
        ),
        (
            "v1_security_ci_hardening_gate",
            [python_executable, "scripts/check_v1_security_ci_hardening.py"],
        ),
        (
            "v1_release_boundary_firewall",
            [python_executable, "scripts/check_v1_release_boundary.py"],
        ),
        (
            "final_release_approval_post_release_verification",
            [python_executable, "scripts/check_final_release_approval.py"],
        ),
        ("golden_workflow_validator", [python_executable, "scripts/check_golden_workflows.py"]),
        ("admitted_semantics_matrix", [python_executable, "scripts/check_admitted_semantics_matrix.py"]),
        ("runtime_execution_envelopes", [python_executable, "scripts/check_runtime_execution_envelopes.py"]),
        ("website_readiness", [python_executable, "scripts/check_website_readiness.py"]),
        ("benchmark_constitution", [python_executable, "scripts/check_benchmark_constitution.py"]),
        (
            "benchmark_artifact_completeness",
            [
                python_executable,
                "scripts/check_benchmark_artifact_completeness.py",
                "--manifest",
                "website/assets/benchmarks/latest/manifest.json",
                "--output",
                "target/benchmark-artifact-completeness-report.json",
            ],
        ),
        (
            "pre_5j_dependency_freshness_gate",
            [
                python_executable,
                "scripts/check_pre_5j_dependency_freshness.py",
                "--require-live-github",
                "--output",
                "target/pre-5j-dependency-freshness-gate.json",
            ],
        ),
        (
            "benchmark_publication_claim_gate",
            [
                python_executable,
                "scripts/check_benchmark_publication_claim_gate.py",
                "--manifest",
                "website/assets/benchmarks/latest/manifest.json",
                "--allow-stale-git",
            ],
        ),
        (
            "front_door_benchmark_publication_gate",
            [
                python_executable,
                "scripts/check_front_door_benchmark_publication.py",
                "--manifest",
                "website/assets/benchmarks/latest/manifest.json",
                "--allow-stale-git",
            ],
        ),
        (
            "final_release_rehearsal",
            [python_executable, "scripts/final_release_rehearsal.py", "--allow-blocked"],
        ),
        ("production_usability_gate", [python_executable, "scripts/check_production_usability_gate.py"]),
        (
            "python_user_surface_completion_gate",
            [python_executable, "scripts/check_python_user_surface_completion.py"],
        ),
        (
            "sql_python_dataframe_parity_gate",
            [python_executable, "scripts/check_sql_python_dataframe_parity.py"],
        ),
        (
            "v1_front_door_runtime_scope_gate",
            [python_executable, "scripts/check_v1_front_door_runtime_scope.py"],
        ),
        (
            "v1_vortex_runtime_scope_gate",
            [python_executable, "scripts/check_v1_vortex_runtime_scope.py"],
        ),
        (
            "v1_source_prepared_state_scope_gate",
            [python_executable, "scripts/check_v1_source_prepared_state_scope.py"],
        ),
        (
            "v1_local_output_sink_scope_gate",
            [python_executable, "scripts/check_v1_local_output_sink_scope.py"],
        ),
        (
            "local_format_production_profiles_gate",
            [python_executable, "scripts/check_local_format_production_profiles.py"],
        ),
        (
            "local_format_pushdown_fidelity_gate",
            [python_executable, "scripts/check_local_format_pushdown_fidelity.py"],
        ),
        (
            "compatibility_output_translation_report_gate",
            [python_executable, "scripts/check_compatibility_output_translation_reports.py"],
        ),
        (
            "local_format_edge_case_fixture_gate",
            [python_executable, "scripts/check_local_format_edge_case_fixtures.py"],
        ),
        (
            "v1_local_resource_safety_gate",
            [python_executable, "scripts/check_v1_local_resource_safety.py"],
        ),
        (
            "v1_observability_support_gate",
            [python_executable, "scripts/check_v1_observability_support.py"],
        ),
        (
            "v1_api_schema_stability_gate",
            [python_executable, "scripts/check_v1_api_schema_stability.py"],
        ),
        (
            "v1_example_replay_gate",
            [python_executable, "scripts/check_v1_example_replay.py"],
        ),
        (
            "v1_correctness_conformance_gate",
            [python_executable, "scripts/check_v1_correctness_conformance.py"],
        ),
        (
            "user_surface_runtime_gap_inventory",
            [python_executable, "scripts/check_user_surface_runtime_gap_inventory.py"],
        ),
        (
            "user_surface_graduation_matrix",
            [python_executable, "scripts/check_user_surface_graduation_matrix.py"],
        ),
        (
            "runtime_gap_family_burn_down",
            [python_executable, "scripts/check_runtime_gap_family_burn_down.py"],
        ),
        (
            "user_route_capability_report",
            [python_executable, "scripts/check_user_route_capability_report.py"],
        ),
        (
            "production_certification_gate",
            [python_executable, "scripts/check_production_certification_gate.py"],
        ),
    ]


def planned_release_validation_commands(
    python_executable: str,
    args: argparse.Namespace,
) -> tuple[
    list[tuple[str, list[str], str, dict[str, str]]],
    list[tuple[str, list[str]]],
]:
    required_commands = required_validation_commands(python_executable, args)
    if args.skip_slow:
        return [], required_commands

    planned: list[tuple[str, list[str], str, dict[str, str]]] = []
    planned.extend(supporting_commands(python_executable, args.pip_audit_python))
    planned.extend(
        (name, command, "feature_build_matrix", {})
        for name, command, skip in FEATURE_MATRIX_COMMANDS
        if not skip
    )
    planned.extend(
        (name, command, "required_validation", {})
        for name, command in required_commands
    )
    return planned, required_commands


def tail(text: str, limit: int = 4000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


def run_command(
    repo_root: Path,
    name: str,
    command: list[str],
    group: str,
    python_executable: str,
    env_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    completed = subprocess.run(
        command,
        cwd=repo_root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    elapsed = (time.perf_counter() - started) * 1000.0
    return {
        "name": name,
        "group": group,
        "command": command_text(command, python_executable),
        "argv": command,
        "env_overrides": sorted((env_overrides or {}).keys()),
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
    python_executable = release_python(args)
    results: list[dict[str, Any]] = []

    planned, required_commands = planned_release_validation_commands(python_executable, args)

    for name, command, group, env_overrides in planned:
        result = run_command(
            repo_root,
            name,
            command,
            group,
            python_executable,
            env_overrides,
        )
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
        text = command_text(command, python_executable)
        status = SKIPPED_SLOW_STATUS if args.skip_slow else command_status.get(text, "not_run")
        feature_rows.append(
            {
                "feature_set": name,
                "command": text,
                "status": status,
                "release_blocking": False if args.skip_slow else status != "passed",
            }
        )

    required_rows = []
    for name, command in required_commands:
        text = command_text(command, python_executable)
        status = SKIPPED_SLOW_STATUS if args.skip_slow else command_status.get(text, "not_run")
        required_rows.append(
            {
                "name": name,
                "command": text,
                "status": status,
                "release_blocking": False if args.skip_slow else status != "passed",
            }
        )

    feature_matrix_passed = (not args.skip_slow) and all(
        not row["release_blocking"] for row in feature_rows
    )
    required_validation_passed = (not args.skip_slow) and all(
        not row["release_blocking"] for row in required_rows
    )
    supporting_passed = all(
        result["status"] == "passed"
        for result in results
        if result["group"] == "security_dependency_provenance"
    )
    passed = feature_matrix_passed and required_validation_passed and supporting_passed

    report = {
        "schema_version": SCHEMA_VERSION,
        "status": SKIPPED_SLOW_STATUS if args.skip_slow else ("passed" if passed else "failed"),
        "python_executable": python_executable,
        "pip_audit_python": str(args.pip_audit_python) if args.pip_audit_python else None,
        "clean_conda_required": args.require_clean_conda,
        "conda_executable": str(args.conda_executable) if args.conda_executable else None,
        "feature_build_matrix_status": SKIPPED_SLOW_STATUS
        if args.skip_slow
        else ("passed" if feature_matrix_passed else "failed"),
        "required_validation_status": SKIPPED_SLOW_STATUS
        if args.skip_slow
        else ("passed" if required_validation_passed else "failed"),
        "supporting_security_dependency_status": SKIPPED_SLOW_STATUS
        if args.skip_slow
        else ("passed" if supporting_passed else "failed"),
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
    return 0 if passed or args.skip_slow or args.continue_on_failure else 1


if __name__ == "__main__":
    raise SystemExit(main())
