#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Build and inspect local ShardLoom release artifacts without publishing.

This script is release proof tooling only. It creates local build artifacts,
installs the local wheel in a clean virtual environment, resolves a locally
built ShardLoom CLI, runs smoke commands, and writes a transcript under target/.
It does not create tags, publish packages, add secrets, or install fallback
runtime engines.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--venv-dir",
        type=Path,
        default=Path("target/release-dry-run-proof/venv"),
        help="Clean virtual environment path, relative to the repo root by default.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/release-dry-run-proof/transcript.json"),
        help="Transcript path, relative to the repo root by default.",
    )
    parser.add_argument("--rows", type=int, default=64)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument(
        "--skip-benchmark-smoke",
        action="store_true",
        help="Skip the local benchmark smoke. Intended only for focused packaging troubleshooting.",
    )
    return parser.parse_args()


def resolve_under_repo(repo_root: Path, path: Path) -> Path:
    resolved = path if path.is_absolute() else repo_root / path
    return resolved.resolve()


def venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def shardloom_binary(repo_root: Path) -> Path:
    binary = repo_root / "target" / "debug" / "shardloom"
    if os.name == "nt":
        binary = binary.with_suffix(".exe")
    return binary


def run_step(
    *,
    name: str,
    command: list[str],
    cwd: Path,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    elapsed_ms = round((time.perf_counter() - started) * 1000.0, 4)
    return {
        "name": name,
        "command": command,
        "returncode": completed.returncode,
        "elapsed_millis": elapsed_ms,
        "stdout": completed.stdout[-4000:],
        "stderr": completed.stderr[-4000:],
    }


def newest_wheel(dist_dir: Path) -> Path:
    wheels = sorted(dist_dir.glob("shardloom-*.whl"), key=lambda path: path.stat().st_mtime)
    if not wheels:
        raise FileNotFoundError(f"no shardloom wheel found in {dist_dir}")
    return wheels[-1]


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    venv_dir = resolve_under_repo(repo_root, args.venv_dir)
    output = resolve_under_repo(repo_root, args.output)
    dist_dir = repo_root / "python" / "dist"
    binary = shardloom_binary(repo_root)

    steps: list[dict[str, Any]] = []

    if venv_dir.exists():
        shutil.rmtree(venv_dir)
    output.parent.mkdir(parents=True, exist_ok=True)

    steps.append(
        run_step(
            name="build_cli_binary",
            command=["cargo", "build", "-p", "shardloom-cli", "--bin", "shardloom"],
            cwd=repo_root,
        )
    )
    steps.append(
        run_step(
            name="build_python_artifacts",
            command=[sys.executable, "-m", "build", "python"],
            cwd=repo_root,
        )
    )
    steps.append(
        run_step(
            name="create_clean_venv",
            command=[sys.executable, "-m", "venv", str(venv_dir)],
            cwd=repo_root,
        )
    )

    if any(step["returncode"] != 0 for step in steps):
        return write_transcript(repo_root, output, venv_dir, binary, None, steps, False)

    wheel = newest_wheel(dist_dir)
    clean_python = venv_python(venv_dir)
    smoke_env = os.environ.copy()
    smoke_env["SHARDLOOM_BIN"] = str(binary)

    steps.append(
        run_step(
            name="install_local_wheel_clean_venv",
            command=[
                str(clean_python),
                "-m",
                "pip",
                "install",
                "--no-index",
                "--find-links",
                str(dist_dir),
                "shardloom",
            ],
            cwd=repo_root,
        )
    )
    steps.append(
        run_step(
            name="wheel_import_and_client_smoke",
            command=[
                str(clean_python),
                "-c",
                (
                    "from shardloom import ShardLoomClient; "
                    "client=ShardLoomClient.from_env(); "
                    "smoke=client.smoke_check(); "
                    "caps=client.capabilities(); "
                    "print('fallback_attempted=' + str(smoke.fallback_attempted)); "
                    "print('capabilities_command=' + caps.command)"
                ),
            ],
            cwd=repo_root,
            env=smoke_env,
        )
    )
    steps.append(
        run_step(
            name="cli_status_json",
            command=[str(binary), "status", "--format", "json"],
            cwd=repo_root,
        )
    )
    steps.append(
        run_step(
            name="cli_capabilities_json",
            command=[str(binary), "capabilities", "--format", "json"],
            cwd=repo_root,
        )
    )
    steps.append(
        run_step(
            name="example_local_python_smoke",
            command=[
                str(clean_python),
                "examples/local-python-smoke/run.py",
                "--repo-root",
                str(repo_root),
                "--shardloom-bin",
                str(binary),
            ],
            cwd=repo_root,
        )
    )
    if not args.skip_benchmark_smoke:
        steps.append(
            run_step(
                name="example_local_vortex_benchmark_smoke",
                command=[
                    sys.executable,
                    "examples/local-vortex-benchmark/run.py",
                    "--repo-root",
                    str(repo_root),
                    "--rows",
                    str(args.rows),
                    "--iterations",
                    str(args.iterations),
                ],
                cwd=repo_root,
            )
        )

    passed = all(step["returncode"] == 0 for step in steps)
    return write_transcript(repo_root, output, venv_dir, binary, wheel, steps, passed)


def write_transcript(
    repo_root: Path,
    output: Path,
    venv_dir: Path,
    binary: Path,
    wheel: Path | None,
    steps: list[dict[str, Any]],
    passed: bool,
) -> int:
    transcript = {
        "schema_version": "shardloom.release_dry_run_proof.v1",
        "proof_status": "passed" if passed else "failed",
        "repo_root": str(repo_root),
        "clean_venv": str(venv_dir),
        "local_wheel": str(wheel) if wheel is not None else None,
        "local_cli_binary": str(binary),
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "external_runtime_dependencies_added": False,
        "fallback_engine_dependency_added": False,
        "steps": steps,
    }
    output.write_text(json.dumps(transcript, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
