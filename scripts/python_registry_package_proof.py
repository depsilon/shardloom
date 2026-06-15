#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Verify an already-published ShardLoom Python package from a registry.

This tool never uploads packages, creates tags, creates GitHub releases, writes
secrets, or authorizes public release claims. It creates a clean virtual
environment, installs a specific package version from TestPyPI or PyPI, runs a
minimal no-fallback client smoke, uninstalls the package, and writes a
machine-readable transcript.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.python_registry_package_proof.v1"
PACKAGE_NAME = "shardloom"


@dataclass(frozen=True)
class RegistryChannel:
    channel_id: str
    display_name: str
    index_url: str | None
    install_source: str


REGISTRY_CHANNELS = {
    "testpypi": RegistryChannel(
        channel_id="testpypi",
        display_name="TestPyPI",
        index_url="https://test.pypi.org/simple/",
        install_source="testpypi_registry",
    ),
    "pypi": RegistryChannel(
        channel_id="pypi",
        display_name="PyPI",
        index_url=None,
        install_source="pypi_registry",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--channel", choices=sorted(REGISTRY_CHANNELS), required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument(
        "--python",
        type=Path,
        default=Path(sys.executable),
        help="Python executable used to create the clean virtual environment.",
    )
    parser.add_argument(
        "--venv-dir",
        type=Path,
        default=Path("target/python-registry-package-proof/venv"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/python-registry-package-proof/transcript.json"),
    )
    parser.add_argument(
        "--testpypi-proof-ref",
        help="Required when --channel=pypi; records the prior TestPyPI proof reference.",
    )
    return parser.parse_args()


def resolve_under_repo(repo_root: Path, path: Path) -> Path:
    resolved = path if path.is_absolute() else repo_root / path
    return resolved.resolve()


def rel(repo_root: Path, path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return f"external-path:{path.name}"


def venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def remove_tree_under_repo(repo_root: Path, path: Path) -> None:
    repo_root = repo_root.resolve()
    resolved = path.resolve()
    if resolved == repo_root or repo_root not in resolved.parents:
        raise ValueError(f"refusing to remove unsafe path: {resolved}")
    shutil.rmtree(resolved)


def redact_command(repo_root: Path, command: list[str]) -> list[str]:
    redacted: list[str] = []
    for part in command:
        if Path(part).is_absolute():
            redacted.append(rel(repo_root, Path(part)) or Path(part).name)
        else:
            redacted.append(part)
    return redacted


def run_step(
    *,
    repo_root: Path,
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
    return {
        "name": name,
        "command": redact_command(repo_root, command),
        "returncode": completed.returncode,
        "elapsed_millis": round((time.perf_counter() - started) * 1000.0, 4),
        "stdout": completed.stdout[-4000:],
        "stderr": completed.stderr[-4000:],
    }


def install_command(python: Path, channel: RegistryChannel, version: str) -> list[str]:
    command = [
        str(python),
        "-m",
        "pip",
        "install",
        "--no-deps",
    ]
    if channel.index_url is not None:
        command.extend(["--index-url", channel.index_url])
    command.append(f"{PACKAGE_NAME}=={version}")
    return command


def smoke_command(python: Path) -> list[str]:
    return [
        str(python),
        "-c",
        (
            "from shardloom import ShardLoomClient; "
            "client=ShardLoomClient.from_env(); "
            "smoke=client.smoke_check(); "
            "caps=client.capabilities(); "
            "print('fallback_attempted=' + str(smoke.fallback_attempted)); "
            "print('external_engine_invoked=' + str(getattr(smoke, 'external_engine_invoked', False))); "
            "print('capabilities_command=' + caps.command)"
        ),
    ]


def uninstall_command(python: Path) -> list[str]:
    return [str(python), "-m", "pip", "uninstall", "-y", PACKAGE_NAME]


def step_status(steps: list[dict[str, Any]], name: str) -> str:
    for step in steps:
        if step["name"] == name:
            return "passed" if step["returncode"] == 0 else "failed"
    return "not_run"


def write_transcript(
    *,
    repo_root: Path,
    output: Path,
    channel: RegistryChannel,
    version: str,
    venv_dir: Path,
    steps: list[dict[str, Any]],
    testpypi_proof_ref: str | None,
) -> int:
    installed = step_status(steps, "install_registry_package") == "passed"
    smoked = step_status(steps, "registry_package_client_smoke") == "passed"
    uninstalled = step_status(steps, "uninstall_registry_package") == "passed"
    smoke_stdout = "\n".join(
        step.get("stdout", "") for step in steps if step["name"] == "registry_package_client_smoke"
    )
    fallback_attempted = "fallback_attempted=True" in smoke_stdout
    external_engine_invoked = "external_engine_invoked=True" in smoke_stdout
    proof_passed = (
        installed
        and smoked
        and uninstalled
        and not fallback_attempted
        and not external_engine_invoked
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "proof_status": "passed" if proof_passed else "failed",
        "channel_id": channel.channel_id,
        "display_name": channel.display_name,
        "package_name": PACKAGE_NAME,
        "package_version": version,
        "install_source": channel.install_source,
        "index_url": channel.index_url,
        "clean_env": rel(repo_root, venv_dir),
        "install_transcript_status": step_status(steps, "install_registry_package"),
        "smoke_check_status": step_status(steps, "registry_package_client_smoke"),
        "uninstall_transcript_status": step_status(steps, "uninstall_registry_package"),
        "fallback_attempted": fallback_attempted,
        "external_engine_invoked": external_engine_invoked,
        "testpypi_proof_ref": testpypi_proof_ref,
        "testpypi_proof_required": channel.channel_id == "pypi",
        "registry_upload_attempted_by_this_tool": False,
        "publication_attempted_by_this_tool": False,
        "tag_created": False,
        "secrets_required": False,
        "package_channel_submission_attempted_by_this_tool": False,
        "steps": steps,
    }
    if channel.channel_id == "pypi" and not testpypi_proof_ref:
        report["proof_status"] = "failed"
        report["blockers"] = ["pypi proof requires a prior TestPyPI proof reference"]
    else:
        blockers = []
        if report["proof_status"] != "passed":
            blockers.append("registry proof step failed")
        if fallback_attempted:
            blockers.append("registry proof smoke reported fallback_attempted=True")
        if external_engine_invoked:
            blockers.append("registry proof smoke reported external_engine_invoked=True")
        report["blockers"] = blockers
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0 if report["proof_status"] == "passed" else 1


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    channel = REGISTRY_CHANNELS[args.channel]
    venv_dir = resolve_under_repo(repo_root, args.venv_dir)
    output = resolve_under_repo(repo_root, args.output)
    if channel.channel_id == "pypi" and not args.testpypi_proof_ref:
        return write_transcript(
            repo_root=repo_root,
            output=output,
            channel=channel,
            version=args.version,
            venv_dir=venv_dir,
            steps=[],
            testpypi_proof_ref=None,
        )

    if venv_dir.exists():
        remove_tree_under_repo(repo_root, venv_dir)
    steps: list[dict[str, Any]] = []
    steps.append(
        run_step(
            repo_root=repo_root,
            name="create_clean_venv",
            command=[str(args.python), "-m", "venv", str(venv_dir)],
            cwd=repo_root,
        )
    )
    clean_python = venv_python(venv_dir)
    if step_status(steps, "create_clean_venv") == "passed":
        steps.append(
            run_step(
                repo_root=repo_root,
                name="install_registry_package",
                command=install_command(clean_python, channel, args.version),
                cwd=repo_root,
            )
        )
    if step_status(steps, "install_registry_package") == "passed":
        steps.append(
            run_step(
                repo_root=repo_root,
                name="registry_package_client_smoke",
                command=smoke_command(clean_python),
                cwd=repo_root,
                env=os.environ.copy(),
            )
        )
    if step_status(steps, "install_registry_package") == "passed":
        steps.append(
            run_step(
                repo_root=repo_root,
                name="uninstall_registry_package",
                command=uninstall_command(clean_python),
                cwd=repo_root,
            )
        )

    return write_transcript(
        repo_root=repo_root,
        output=output,
        channel=channel,
        version=args.version,
        venv_dir=venv_dir,
        steps=steps,
        testpypi_proof_ref=args.testpypi_proof_ref,
    )


if __name__ == "__main__":
    raise SystemExit(main())
