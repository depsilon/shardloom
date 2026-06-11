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
PROTECTED_CLEANUP_ROOTS = {
    ".git",
    "benchmarks",
    "docs",
    "examples",
    "python",
    "scripts",
    "shardloom-cli",
    "shardloom-core",
    "shardloom-exec",
    "shardloom-python-ffi",
    "shardloom-vortex",
    "target",
    "website",
    "website-public",
    "website-src",
}


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
        "--conda-env-dir",
        type=Path,
        default=Path("target/release-dry-run-proof/conda-env"),
        help="Clean Conda-style environment prefix, relative to the repo root by default.",
    )
    parser.add_argument(
        "--conda-executable",
        type=Path,
        help="Explicit conda, mamba, or micromamba executable for clean Conda proof.",
    )
    parser.add_argument(
        "--conda-python-version",
        default="3.11",
        help="Python version requested for the clean Conda proof environment.",
    )
    parser.add_argument(
        "--skip-clean-conda",
        action="store_true",
        help="Record clean Conda proof as skipped. The hard release gate will remain blocked.",
    )
    parser.add_argument(
        "--require-clean-conda",
        action="store_true",
        help="Fail this dry run when clean Conda proof cannot pass.",
    )
    parser.add_argument(
        "--skip-benchmark-smoke",
        action="store_true",
        help="Skip the local benchmark smoke. Intended only for focused packaging troubleshooting.",
    )
    return parser.parse_args()


def resolve_under_repo(repo_root: Path, path: Path) -> Path:
    resolved = path if path.is_absolute() else repo_root / path
    return resolved.resolve()


def transcript_path_ref(repo_root: Path, path: Path | None) -> str | None:
    if path is None:
        return None
    resolved_root = repo_root.resolve()
    resolved_path = path.resolve()
    try:
        return resolved_path.relative_to(resolved_root).as_posix()
    except ValueError:
        return f"external-path:{resolved_path.name}"


def redact_command_for_transcript(repo_root: Path, command: list[str]) -> list[str]:
    repo_prefix = str(repo_root.resolve())
    redacted: list[str] = []
    for part in command:
        path = Path(part)
        if path.is_absolute():
            redacted.append(transcript_path_ref(repo_root, path) or "not_available")
        else:
            redacted.append(part.replace(repo_prefix, "<repo>"))
    return redacted


def redact_text_for_transcript(repo_root: Path, text: str) -> str:
    return text.replace(str(repo_root.resolve()), "<repo>")


def venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def conda_env_python(env_dir: Path) -> Path:
    if os.name == "nt":
        return env_dir / "python.exe"
    return env_dir / "bin" / "python"


def shardloom_binary(repo_root: Path) -> Path:
    binary = repo_root / "target" / "debug" / "shardloom"
    if os.name == "nt":
        binary = binary.with_suffix(".exe")
    return binary


def find_conda_tool(explicit: Path | None) -> Path | None:
    if explicit is not None:
        found = shutil.which(str(explicit))
        resolved = Path(found).resolve() if found else explicit.resolve()
        return resolved if resolved.exists() else None
    for candidate in ["mamba", "conda", "micromamba"]:
        found = shutil.which(candidate)
        if found:
            return Path(found)
    return None


def conda_create_command(tool: Path, env_dir: Path, python_version: str) -> list[str]:
    command = [
        str(tool),
        "create",
        "-y",
        "-p",
        str(env_dir),
        f"python={python_version}",
        "pip",
    ]
    if "micromamba" in tool.name.lower():
        command.extend(["-c", "conda-forge"])
    return command


def env_with_path_prepend(directory: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["PATH"] = str(directory) + os.pathsep + env.get("PATH", "")
    return env


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
        "command": redact_command_for_transcript(cwd, command),
        "returncode": completed.returncode,
        "elapsed_millis": elapsed_ms,
        "stdout": redact_text_for_transcript(cwd, completed.stdout[-4000:]),
        "stderr": redact_text_for_transcript(cwd, completed.stderr[-4000:]),
    }


def newest_wheel(dist_dir: Path) -> Path:
    wheels = sorted(dist_dir.glob("shardloom-*.whl"), key=lambda path: path.stat().st_mtime)
    if not wheels:
        raise FileNotFoundError(f"no shardloom wheel found in {dist_dir}")
    return wheels[-1]


def clean_python_dist(dist_dir: Path) -> None:
    dist_dir.mkdir(parents=True, exist_ok=True)
    for pattern in ("shardloom-*.whl", "shardloom-*.tar.gz"):
        for artifact in dist_dir.glob(pattern):
            artifact.unlink()


def build_python_artifacts(repo_root: Path, dist_dir: Path) -> dict[str, Any]:
    clean_python_dist(dist_dir)
    build_step = run_step(
        name="build_python_artifacts",
        command=[sys.executable, "-m", "build", "python"],
        cwd=repo_root,
    )
    if build_step["returncode"] == 0:
        build_step["build_backend"] = "python_build_frontend"
        return build_step
    if "No module named build" not in build_step.get("stderr", ""):
        build_step["build_backend"] = "python_build_frontend"
        return build_step

    fallback_step = run_step(
        name="build_python_artifacts",
        command=[
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-build-isolation",
            "--no-deps",
            "--wheel-dir",
            str(dist_dir),
            str(repo_root / "python"),
        ],
        cwd=repo_root,
    )
    fallback_step["build_backend"] = "pip_wheel_no_build_isolation"
    fallback_step["fallback_reason"] = "python_build_frontend_missing"
    fallback_step["frontend_stderr"] = build_step.get("stderr", "")
    return fallback_step


def generated_user_rows_smoke_script(output_path: Path) -> str:
    output_arg = json.dumps(str(output_path))
    return (
        "from shardloom import context; "
        "ctx=context(); "
        "report=ctx.from_rows([{'id': 1, 'label': 'alpha'}, {'id': 2, 'label': 'beta'}]).write("
        f"{output_arg}, allow_overwrite=True); "
        "print('generated_source_kind=' + report.generated_source_kind); "
        "print('generated_source_row_count=' + str(report.generated_source_row_count)); "
        "print('output_io_performed=' + str(report.envelope.field('output_io_performed'))); "
        "print('generated_source_certificate_status=' + report.generated_source_certificate_status); "
        "print('output_native_io_certificate_status=' + report.output_native_io_certificate_status); "
        "print('fallback_attempted=' + str(report.fallback_attempted)); "
        "print('external_engine_invoked=' + str(report.external_engine_invoked)); "
        "print('claim_gate_status=' + report.claim_gate_status)"
    )


def generated_range_smoke_script(output_path: Path) -> str:
    output_arg = json.dumps(str(output_path))
    return (
        "from shardloom import context; "
        "ctx=context(); "
        f"report=ctx.range(0, 8, column='id').write({output_arg}, allow_overwrite=True); "
        "print('generated_source_kind=' + report.generated_source_kind); "
        "print('generated_source_row_count=' + str(report.generated_source_row_count)); "
        "print('generated_source_range_start=' + str(report.generated_source_range_start)); "
        "print('generated_source_range_end=' + str(report.generated_source_range_end)); "
        "print('output_io_performed=' + str(report.envelope.field('output_io_performed'))); "
        "print('generated_source_certificate_status=' + report.generated_source_certificate_status); "
        "print('output_native_io_certificate_status=' + report.output_native_io_certificate_status); "
        "print('fallback_attempted=' + str(report.fallback_attempted)); "
        "print('external_engine_invoked=' + str(report.external_engine_invoked)); "
        "print('claim_gate_status=' + report.claim_gate_status)"
    )


def remove_tree_under_repo(repo_root: Path, path: Path) -> None:
    repo_root = repo_root.resolve()
    resolved = path.resolve()
    if resolved != repo_root and repo_root not in resolved.parents:
        raise ValueError(f"refusing to remove path outside repo: {resolved}")
    if resolved == repo_root:
        raise ValueError(f"refusing to remove repository root: {resolved}")
    try:
        relative = resolved.relative_to(repo_root)
    except ValueError:
        raise ValueError(f"refusing to remove path outside repo: {resolved}") from None
    if len(relative.parts) == 1 and relative.parts[0] in PROTECTED_CLEANUP_ROOTS:
        raise ValueError(f"refusing to remove protected repository directory: {resolved}")
    shutil.rmtree(resolved)


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    venv_dir = resolve_under_repo(repo_root, args.venv_dir)
    conda_env_dir = resolve_under_repo(repo_root, args.conda_env_dir)
    output = resolve_under_repo(repo_root, args.output)
    dist_dir = repo_root / "python" / "dist"
    binary = shardloom_binary(repo_root)
    proof_artifact_dir = repo_root / "target" / "release-dry-run-proof"
    generated_user_rows_output = proof_artifact_dir / "generated-user-rows.jsonl"
    generated_range_output = proof_artifact_dir / "generated-range.jsonl"
    clean_conda_status = "not_run_prerequisite_failed"
    clean_conda_tool: Path | None = None

    steps: list[dict[str, Any]] = []

    if venv_dir.exists():
        remove_tree_under_repo(repo_root, venv_dir)
    output.parent.mkdir(parents=True, exist_ok=True)

    steps.append(
        run_step(
            name="build_cli_binary",
            command=["cargo", "build", "-p", "shardloom-cli", "--bin", "shardloom"],
            cwd=repo_root,
        )
    )
    steps.append(build_python_artifacts(repo_root, dist_dir))
    steps.append(
        run_step(
            name="create_clean_venv",
            command=[sys.executable, "-m", "venv", str(venv_dir)],
            cwd=repo_root,
        )
    )

    if any(step["returncode"] != 0 for step in steps):
        return write_transcript(
            repo_root,
            output,
            venv_dir,
            conda_env_dir,
            binary,
            None,
            steps,
            False,
            clean_conda_status,
            clean_conda_tool,
            args.require_clean_conda,
        )

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
                str(wheel),
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
    if args.skip_clean_conda:
        clean_conda_status = "skipped_by_request"
    else:
        clean_conda_tool = find_conda_tool(args.conda_executable)
        if clean_conda_tool is None:
            clean_conda_status = "skipped_tool_missing"
        else:
            if conda_env_dir.exists():
                remove_tree_under_repo(repo_root, conda_env_dir)
            before = len(steps)
            steps.append(
                run_step(
                    name="create_clean_conda_env",
                    command=conda_create_command(
                        clean_conda_tool,
                        conda_env_dir,
                        args.conda_python_version,
                    ),
                    cwd=repo_root,
                    env=env_with_path_prepend(clean_conda_tool.parent),
                )
            )
            clean_conda_python = conda_env_python(conda_env_dir)
            if steps[-1]["returncode"] == 0:
                steps.append(
                    run_step(
                        name="install_local_wheel_clean_conda",
                        command=[
                            str(clean_conda_python),
                            "-m",
                            "pip",
                            "install",
                            "--no-index",
                            str(wheel),
                        ],
                        cwd=repo_root,
                    )
                )
            if steps[-1]["returncode"] == 0:
                steps.append(
                    run_step(
                        name="conda_wheel_import_and_client_smoke",
                        command=[
                            str(clean_conda_python),
                            "-c",
                            (
                                "from shardloom import ShardLoomClient; "
                                "client=ShardLoomClient.from_env(); "
                                "smoke=client.smoke_check(); "
                                "print('fallback_attempted=' + str(smoke.fallback_attempted))"
                            ),
                        ],
                        cwd=repo_root,
                        env=smoke_env,
                    )
                )
            conda_steps = steps[before:]
            clean_conda_status = (
                "passed"
                if conda_steps and all(step["returncode"] == 0 for step in conda_steps)
                else "failed"
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
    steps.append(
        run_step(
            name="generated_source_user_rows_local_output_smoke",
            command=[
                str(clean_python),
                "-c",
                generated_user_rows_smoke_script(generated_user_rows_output),
            ],
            cwd=repo_root,
            env=smoke_env,
        )
    )
    steps.append(
        run_step(
            name="generated_source_range_local_output_smoke",
            command=[
                str(clean_python),
                "-c",
                generated_range_smoke_script(generated_range_output),
            ],
            cwd=repo_root,
            env=smoke_env,
        )
    )
    if not args.skip_benchmark_smoke:
        steps.append(
            run_step(
                name="example_local_vortex_benchmark_smoke",
                command=[
                    str(clean_python),
                    "examples/local-vortex-benchmark/run.py",
                    "--repo-root",
                    str(repo_root),
                    "--run-root",
                    "target/release-dry-run-proof/local-vortex-benchmark",
                    "--rows",
                    str(args.rows),
                    "--iterations",
                    str(args.iterations),
                ],
                cwd=repo_root,
            )
        )
    steps.append(
        run_step(
            name="release_provenance_dry_run",
            command=[
                sys.executable,
                "scripts/release_provenance_dry_run.py",
                "--repo-root",
                str(repo_root),
                "--skip-build",
            ],
            cwd=repo_root,
        )
    )

    passed = all(step["returncode"] == 0 for step in steps) and (
        clean_conda_status == "passed" or not args.require_clean_conda
    )
    return write_transcript(
        repo_root,
        output,
        venv_dir,
        conda_env_dir,
        binary,
        wheel,
        steps,
        passed,
        clean_conda_status,
        clean_conda_tool,
        args.require_clean_conda,
    )


def write_transcript(
    repo_root: Path,
    output: Path,
    venv_dir: Path,
    conda_env_dir: Path,
    binary: Path,
    wheel: Path | None,
    steps: list[dict[str, Any]],
    passed: bool,
    clean_conda_status: str,
    clean_conda_tool: Path | None,
    clean_conda_required: bool,
) -> int:
    steps_by_name = {step["name"]: step for step in steps}

    def step_attempted(name: str) -> bool:
        return name in steps_by_name

    def step_passed(name: str) -> bool:
        return steps_by_name.get(name, {}).get("returncode") == 0

    def step_status(name: str) -> str:
        if not step_attempted(name):
            return "not_run"
        return "passed" if step_passed(name) else "failed"

    def step_stdout_contains(name: str, marker: str) -> bool:
        return marker in steps_by_name.get(name, {}).get("stdout", "")

    local_python_user_surface_quickstart_performed = step_passed(
        "example_local_python_smoke"
    ) and step_stdout_contains(
        "example_local_python_smoke",
        "quickstart_user_surface_status=passed",
    )
    local_python_result_and_evidence_printed = all(
        step_stdout_contains("example_local_python_smoke", marker)
        for marker in [
            "quickstart_result_row_id=",
            "quickstart_output_row_count=",
            "quickstart_evidence_fallback_attempted=false",
            "quickstart_claim_gate_status=",
            "quickstart_generated_source_row_count=",
            "quickstart_generated_claim_gate_status=",
        ]
    )
    local_python_unsupported_path_evidence_printed = all(
        step_stdout_contains("example_local_python_smoke", marker)
        for marker in [
            "quickstart_unsupported_blocker_id=",
            "quickstart_unsupported_runtime_execution=false",
            "quickstart_unsupported_fallback_attempted=false",
            "quickstart_unsupported_external_engine_invoked=false",
        ]
    )

    transcript = {
        "schema_version": "shardloom.release_dry_run_proof.v1",
        "proof_status": "passed" if passed else "failed",
        "repo_root": "repo",
        "clean_venv": transcript_path_ref(repo_root, venv_dir),
        "clean_venv_install_status": step_status("install_local_wheel_clean_venv"),
        "clean_conda_env": transcript_path_ref(repo_root, conda_env_dir),
        "clean_conda_env_install_status": clean_conda_status,
        "clean_conda_env_install_tool": transcript_path_ref(repo_root, clean_conda_tool),
        "clean_conda_env_install_required": clean_conda_required,
        "local_wheel": transcript_path_ref(repo_root, wheel),
        "local_cli_binary": transcript_path_ref(repo_root, binary),
        "cli_binary_build_status": step_status("build_cli_binary"),
        "python_artifact_build_status": step_status("build_python_artifacts"),
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "external_runtime_dependencies_added": False,
        "fallback_engine_dependency_added": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "public_package_release_claim_allowed": False,
        "wheel_import_and_client_smoke_performed": step_passed("wheel_import_and_client_smoke"),
        "cli_status_smoke_performed": step_passed("cli_status_json"),
        "cli_capabilities_smoke_performed": step_passed("cli_capabilities_json"),
        "local_python_example_smoke_performed": step_passed("example_local_python_smoke"),
        "local_python_user_surface_quickstart_performed": local_python_user_surface_quickstart_performed,
        "local_python_result_and_evidence_printed": local_python_result_and_evidence_printed,
        "local_python_unsupported_path_evidence_printed": local_python_unsupported_path_evidence_printed,
        "generated_output_proof_distinct_from_no_dataset_smoke": True,
        "generated_source_user_rows_smoke_performed": step_passed(
            "generated_source_user_rows_local_output_smoke"
        ),
        "generated_source_range_smoke_performed": step_passed(
            "generated_source_range_local_output_smoke"
        ),
        "prepared_native_benchmark_smoke_performed": step_passed(
            "example_local_vortex_benchmark_smoke"
        ),
        "provenance_dry_run_performed": step_passed("release_provenance_dry_run"),
        "sbom_checksum_manifest_generated": any(
            step["name"] == "release_provenance_dry_run" and step["returncode"] == 0
            for step in steps
        ),
        "steps": steps,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(transcript, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
