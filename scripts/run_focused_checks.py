#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Run focused local validation commands without broad Cargo target drain.

The script is intentionally a local developer/agent helper. It does not replace
the full workspace gates; it records the faster exact checks that should run
before broad validation.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.focused_check_evidence.v1"


@dataclass(frozen=True)
class FocusedCommand:
    command: tuple[str, ...]
    env: tuple[tuple[str, str], ...] = ()


PROFILE_DESCRIPTIONS: dict[str, str] = {
    "fmt": "Rust formatting check.",
    "rust-cli-bin": "ShardLoom CLI Rust unit tests; uses --bin shardloom to avoid integration-target enumeration.",
    "rust-cli-test": "One ShardLoom CLI Rust integration test target; uses --test <target>.",
    "rust-vortex-lib": "ShardLoom Vortex Rust unit tests; uses --lib to avoid integration-target enumeration.",
    "python-unittest": "One Python unittest module/class/test with PYTHONPATH=python/src.",
    "current-native-vortex": "Focused checks for native Vortex manifest and partitioned primitive work.",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--profile",
        choices=tuple(PROFILE_DESCRIPTIONS),
        help="Focused validation profile to run.",
    )
    parser.add_argument(
        "--filter",
        help="Rust test filter for Rust profiles, or unittest name for python-unittest.",
    )
    parser.add_argument(
        "--target",
        help="Integration test target name for rust-cli-test.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/focused-check-evidence.json"),
    )
    parser.add_argument("--list", action="store_true", help="List profiles and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing.")
    parser.add_argument(
        "--nocapture",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Pass -- --nocapture to Rust test commands.",
    )
    return parser.parse_args()


def command_text(command: tuple[str, ...]) -> str:
    return " ".join(command)


def rust_test_command(
    *,
    package: str,
    features: str,
    target_flag: str,
    target: str | None = None,
    test_filter: str | None = None,
    nocapture: bool = True,
) -> FocusedCommand:
    command: list[str] = ["cargo", "test", "-p", package, "--features", features]
    if target_flag == "--lib":
        command.append("--lib")
    elif target_flag == "--bin":
        if not target:
            raise ValueError("Rust binary test targets require --target")
        command.extend(["--bin", target])
    elif target_flag == "--test":
        if not target:
            raise ValueError("rust-cli-test requires --target")
        command.extend(["--test", target])
    else:
        raise ValueError(f"unknown Rust test target flag: {target_flag}")
    if test_filter:
        command.append(test_filter)
    if nocapture:
        command.extend(["--", "--nocapture"])
    return FocusedCommand(tuple(command))


def python_unittest_command(repo_root: Path, test_name: str | None) -> FocusedCommand:
    if not test_name:
        raise ValueError("python-unittest requires --filter with a module/class/test name")
    env = (("PYTHONPATH", str(repo_root / "python" / "src")),)
    return FocusedCommand((sys.executable, "-m", "unittest", test_name), env)


def commands_for_profile(args: argparse.Namespace) -> list[FocusedCommand]:
    repo_root = args.repo_root.resolve()
    profile = args.profile
    if profile == "fmt":
        return [FocusedCommand(("cargo", "fmt", "--all", "--", "--check"))]
    if profile == "rust-cli-bin":
        return [
            rust_test_command(
                package="shardloom-cli",
                features="release-user-surfaces",
                target_flag="--bin",
                target="shardloom",
                test_filter=args.filter,
                nocapture=args.nocapture,
            )
        ]
    if profile == "rust-cli-test":
        return [
            rust_test_command(
                package="shardloom-cli",
                features="release-user-surfaces",
                target_flag="--test",
                target=args.target,
                test_filter=args.filter,
                nocapture=args.nocapture,
            )
        ]
    if profile == "rust-vortex-lib":
        return [
            rust_test_command(
                package="shardloom-vortex",
                features="vortex-local-primitives",
                target_flag="--lib",
                test_filter=args.filter,
                nocapture=args.nocapture,
            )
        ]
    if profile == "python-unittest":
        return [python_unittest_command(repo_root, args.filter)]
    if profile == "current-native-vortex":
        return [
            FocusedCommand(("cargo", "fmt", "--all", "--", "--check")),
            rust_test_command(
                package="shardloom-cli",
                features="release-user-surfaces",
                target_flag="--bin",
                target="shardloom",
                test_filter="route_infers_vortex_manifest_as_native_vortex_input",
                nocapture=args.nocapture,
            ),
            rust_test_command(
                package="shardloom-vortex",
                features="vortex-local-primitives",
                target_flag="--lib",
                test_filter="partitioned_local_primitive",
                nocapture=args.nocapture,
            ),
            rust_test_command(
                package="shardloom-cli",
                features="release-user-surfaces",
                target_flag="--test",
                target="public_workflow_route",
                test_filter="partitioned",
                nocapture=args.nocapture,
            ),
            python_unittest_command(
                repo_root,
                "python.tests.test_query_builder.LazyWorkflowBuilderTests."
                "test_context_sql_vortex_manifest_source_binds_native_vortex_collect",
            ),
            python_unittest_command(
                repo_root,
                "python.tests.test_query_builder.LazyWorkflowBuilderTests."
                "test_context_sql_embedded_vortex_manifest_broad_query_uses_native_input_binding",
            ),
        ]
    raise ValueError(f"unknown focused profile: {profile}")


def env_for_command(base: dict[str, str], focused_command: FocusedCommand) -> dict[str, str]:
    env = dict(base)
    for key, value in focused_command.env:
        if key == "PYTHONPATH" and env.get(key):
            env[key] = value + os.pathsep + env[key]
        else:
            env[key] = value
    return env


def run_command(repo_root: Path, focused_command: FocusedCommand) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(
        list(focused_command.command),
        cwd=repo_root,
        env=env_for_command(os.environ, focused_command),
        check=False,
    )
    elapsed = time.perf_counter() - started
    return {
        "command": command_text(focused_command.command),
        "status": "passed" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "elapsed_seconds": round(elapsed, 3),
    }


def write_evidence(output: Path, payload: dict[str, Any]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def list_profiles() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "profiles": [
            {"profile": profile, "description": description}
            for profile, description in PROFILE_DESCRIPTIONS.items()
        ],
    }


def main() -> int:
    args = parse_args()
    if args.list:
        print(json.dumps(list_profiles(), indent=2, sort_keys=True))
        return 0
    if not args.profile:
        print("--profile is required unless --list is used", file=sys.stderr)
        return 2

    repo_root = args.repo_root.resolve()
    try:
        commands = commands_for_profile(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    command_rows = [{"command": command_text(command.command)} for command in commands]
    if args.dry_run:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "status": "dry_run",
            "profile": args.profile,
            "commands": command_rows,
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    results: list[dict[str, Any]] = []
    status = "passed"
    for focused_command in commands:
        print(f"+ {command_text(focused_command.command)}", flush=True)
        result = run_command(repo_root, focused_command)
        results.append(result)
        if result["returncode"] != 0:
            status = "failed"
            break

    payload = {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "profile": args.profile,
        "command_count": len(commands),
        "commands": results,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    output_path = args.output if args.output.is_absolute() else repo_root / args.output
    write_evidence(output_path, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
