#!/usr/bin/env python
"""Documented helper for ShardLoom PGO benchmark builds.

The script defaults to print-only mode. Passing --run executes an instrumented
build, a representative training workload, llvm-profdata merge, and a
profile-use rebuild. It is benchmark tooling only; it does not publish packages,
create release artifacts, or upgrade benchmark claims.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Any

from release_report_utils import rust_toolchain_version


DEFAULT_TRAINING_COMMAND = (
    "python benchmarks/traditional_analytics/run.py "
    "--engines shardloom-prepared-vortex "
    "--formats csv,parquet "
    "--include-taxonomy-extra "
    "--shardloom-build-profile release-pgo "
    "--rows 10000 "
    "--iterations 1 "
    "--no-markdown"
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build ShardLoom CLI with a reproducible PGO benchmark workflow.",
        allow_abbrev=False,
    )
    parser.add_argument("--repo-root", type=Path, default=repo_root())
    parser.add_argument(
        "--profile-dir",
        type=Path,
        default=Path("target/pgo/shardloom-profraw"),
        help="Directory for generated .profraw files.",
    )
    parser.add_argument(
        "--merged-profile",
        type=Path,
        default=Path("target/pgo/shardloom.profdata"),
        help="Merged llvm-profdata output path.",
    )
    parser.add_argument(
        "--training-command",
        default=DEFAULT_TRAINING_COMMAND,
        help="Representative command to run under the instrumented binary.",
    )
    parser.add_argument(
        "--features",
        default="vortex-traditional-analytics-benchmark",
        help="ShardLoom CLI features for the PGO benchmark build.",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Execute commands. Omitted by default so the workflow is reportable without side effects.",
    )
    return parser.parse_args()


def resolve_under(root: Path, path: Path) -> Path:
    resolved = path if path.is_absolute() else root / path
    return resolved.resolve()


def run_command(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    execute: bool,
) -> dict[str, Any]:
    if not execute:
        return {"command": command, "returncode": None, "status": "print_only"}
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    return {
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "status": "passed" if completed.returncode == 0 else "failed",
    }


def llvm_profdata_path(root: Path, env: dict[str, str]) -> str:
    rustup = shutil.which("rustup")
    if rustup is not None:
        completed = subprocess.run(
            [rustup, "which", "llvm-profdata"],
            cwd=root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode == 0 and completed.stdout.strip():
            return completed.stdout.strip()
    direct = shutil.which("llvm-profdata")
    return direct or "llvm-profdata"


def main() -> int:
    args = parse_args()
    root = args.repo_root.resolve()
    cargo = shutil.which("cargo")
    if cargo is None:
        raise SystemExit("cargo was not found on PATH")

    profile_dir = resolve_under(root, args.profile_dir)
    merged_profile = resolve_under(root, args.merged_profile)
    base_env = os.environ.copy()
    base_env["RUSTUP_TOOLCHAIN"] = base_env.get(
        "RUSTUP_TOOLCHAIN",
        rust_toolchain_version(root),
    )

    generate_env = dict(base_env)
    generate_env["RUSTFLAGS"] = (
        f"{generate_env.get('RUSTFLAGS', '')} -Cprofile-generate={profile_dir}"
    ).strip()
    generate_env["LLVM_PROFILE_FILE"] = str(profile_dir / "shardloom-%p-%m.profraw")

    profile_use_env = dict(base_env)
    profile_use_env["RUSTFLAGS"] = (
        f"{profile_use_env.get('RUSTFLAGS', '')} -Cprofile-use={merged_profile}"
    ).strip()
    profile_use_env["SHARDLOOM_PGO_PROFILE"] = str(merged_profile)

    build_command = [
        cargo,
        "build",
        "-p",
        "shardloom-cli",
        "--features",
        args.features,
        "--profile",
        "release-pgo",
    ]
    training_command = shlex.split(args.training_command)
    profraw_files = sorted(profile_dir.glob("*.profraw"))
    profraw_inputs = [str(path) for path in profraw_files] or [
        str(profile_dir / "*.profraw")
    ]
    merge_command = [
        llvm_profdata_path(root, base_env),
        "merge",
        "-o",
        str(merged_profile),
        *profraw_inputs,
    ]

    if args.run:
        profile_dir.mkdir(parents=True, exist_ok=True)
        merged_profile.parent.mkdir(parents=True, exist_ok=True)

    results = [
        run_command(build_command, cwd=root, env=generate_env, execute=args.run),
        run_command(training_command, cwd=root, env=generate_env, execute=args.run),
    ]
    if args.run:
        profraw_files = sorted(profile_dir.glob("*.profraw"))
        if not profraw_files:
            raise SystemExit(f"no profraw files were produced under {profile_dir}")
        merge_command = [
            llvm_profdata_path(root, base_env),
            "merge",
            "-o",
            str(merged_profile),
            *[str(path) for path in profraw_files],
        ]
    results.extend(
        [
            run_command(merge_command, cwd=root, env=base_env, execute=args.run),
            run_command(build_command, cwd=root, env=profile_use_env, execute=args.run),
        ]
    )

    report = {
        "schema_version": "shardloom.pgo_build_helper.v1",
        "status": "executed" if args.run else "print_only",
        "profile_dir": str(profile_dir),
        "merged_profile": str(merged_profile),
        "training_command": args.training_command,
        "target_cpu_native_enabled": False,
        "benchmark_only_build": True,
        "portable_release_artifact": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "commands": results,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if all(result["returncode"] in (None, 0) for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
