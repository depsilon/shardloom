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
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.python_registry_package_proof.v1"
PACKAGE_NAME = "shardloom"
ENV_BINARY = "SHARDLOOM_BIN"


@dataclass(frozen=True)
class RegistryChannel:
    channel_id: str
    display_name: str
    index_url: str | None
    install_source: str
    release_json_base_url: str


REGISTRY_CHANNELS = {
    "testpypi": RegistryChannel(
        channel_id="testpypi",
        display_name="TestPyPI",
        index_url="https://test.pypi.org/simple/",
        install_source="testpypi_registry",
        release_json_base_url="https://test.pypi.org/pypi",
    ),
    "pypi": RegistryChannel(
        channel_id="pypi",
        display_name="PyPI",
        index_url=None,
        install_source="pypi_registry",
        release_json_base_url="https://pypi.org/pypi",
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
        "--download-dir",
        type=Path,
        default=Path("target/python-registry-package-proof/downloads"),
        help="Repo-local directory used for the isolated registry artifact download.",
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
    parser.add_argument(
        "--shardloom-bin",
        type=Path,
        help=(
            "Approved local ShardLoom CLI binary used for the registry package smoke. "
            "If omitted, SHARDLOOM_BIN must already point at an existing binary."
        ),
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


def registry_index_url(channel: RegistryChannel) -> str:
    return channel.index_url or "https://pypi.org/simple/"


def download_command(
    python: Path,
    channel: RegistryChannel,
    version: str,
    download_dir: Path,
) -> list[str]:
    command = [
        str(python),
        "-m",
        "pip",
        "--isolated",
        "download",
        "--no-deps",
        "--no-cache-dir",
        "--only-binary",
        ":all:",
        "--dest",
        str(download_dir),
        "--index-url",
        registry_index_url(channel),
    ]
    command.append(f"{PACKAGE_NAME}=={version}")
    return command


def install_downloaded_artifact_command(python: Path, artifact: Path) -> list[str]:
    return [
        str(python),
        "-m",
        "pip",
        "install",
        "--no-deps",
        "--no-index",
        str(artifact),
    ]


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


def registry_release_json_url(channel: RegistryChannel, version: str) -> str:
    return f"{channel.release_json_base_url}/{PACKAGE_NAME}/{version}/json"


def registry_release_artifacts(channel: RegistryChannel, version: str) -> tuple[list[dict[str, Any]], list[str]]:
    url = registry_release_json_url(channel, version)
    try:
        with urlopen(url, timeout=30) as response:
            payload = json.load(response)
    except (OSError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        return [], [f"{channel.channel_id}: failed to fetch registry release JSON: {exc}"]
    artifacts: list[dict[str, Any]] = []
    for row in payload.get("urls") or []:
        digests = row.get("digests") if isinstance(row, dict) else None
        artifacts.append(
            {
                "filename": row.get("filename"),
                "packagetype": row.get("packagetype"),
                "python_version": row.get("python_version"),
                "size": row.get("size"),
                "upload_time_iso_8601": row.get("upload_time_iso_8601"),
                "url": row.get("url"),
                "sha256": digests.get("sha256") if isinstance(digests, dict) else None,
            }
        )
    return artifacts, []


def installed_registry_artifact_filename(steps: list[dict[str, Any]]) -> str | None:
    for step in steps:
        if step.get("name") not in {
            "download_registry_artifact",
            "install_downloaded_registry_artifact",
            "install_registry_package",
        }:
            continue
        output = f"{step.get('stdout', '')}\n{step.get('stderr', '')}"
        for line in output.splitlines():
            line = line.strip()
            if line.startswith("Downloading "):
                candidate = line.removeprefix("Downloading ").split(" ", 1)[0]
            elif line.startswith("Using cached "):
                candidate = line.removeprefix("Using cached ").split(" ", 1)[0]
            elif line.startswith("Processing "):
                candidate = line.removeprefix("Processing ").split(" ", 1)[0]
            else:
                continue
            if candidate.endswith((".whl", ".tar.gz")):
                return Path(candidate).name
    return None


def registry_download_isolated(steps: list[dict[str, Any]]) -> bool:
    for step in steps:
        if step.get("name") == "download_registry_artifact":
            command = step.get("command")
            return isinstance(command, list) and "--isolated" in command
    return False


def registry_download_cache_disabled(steps: list[dict[str, Any]]) -> bool:
    for step in steps:
        if step.get("name") == "download_registry_artifact":
            command = step.get("command")
            return isinstance(command, list) and "--no-cache-dir" in command
    return False


def registry_install_cache_disabled(steps: list[dict[str, Any]]) -> bool:
    for step in steps:
        if step.get("name") in {"download_registry_artifact", "install_registry_package"}:
            command = step.get("command")
            return isinstance(command, list) and "--no-cache-dir" in command
    return False


def registry_install_cache_hit_detected(steps: list[dict[str, Any]]) -> bool:
    for step in steps:
        if step.get("name") not in {"download_registry_artifact", "install_registry_package"}:
            continue
        output = f"{step.get('stdout', '')}\n{step.get('stderr', '')}"
        if any(line.strip().startswith("Using cached ") for line in output.splitlines()):
            return True
    return False


def locate_downloaded_registry_artifact(download_dir: Path) -> Path | None:
    candidates = sorted(
        [
            path
            for path in download_dir.glob(f"{PACKAGE_NAME}-*")
            if path.name.endswith((".whl", ".tar.gz"))
        ]
    )
    return candidates[0] if len(candidates) == 1 else None


def sha256_file(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def registry_artifact_proof(
    channel: RegistryChannel,
    version: str,
    steps: list[dict[str, Any]],
    *,
    repo_root: Path,
    downloaded_artifact: Path | None,
) -> tuple[dict[str, Any], list[str]]:
    artifacts, blockers = registry_release_artifacts(channel, version)
    downloaded_filename = downloaded_artifact.name if downloaded_artifact is not None else None
    installed_filename = downloaded_filename or installed_registry_artifact_filename(steps)
    downloaded_sha256 = sha256_file(downloaded_artifact)
    download_isolated = registry_download_isolated(steps)
    download_cache_disabled = registry_download_cache_disabled(steps)
    install_cache_disabled = registry_install_cache_disabled(steps)
    install_cache_hit_detected = registry_install_cache_hit_detected(steps)
    installed_artifact = next(
        (row for row in artifacts if row.get("filename") == installed_filename),
        None,
    )
    if not download_isolated:
        blockers.append(f"{channel.channel_id}: registry download proof must use pip --isolated")
    if not download_cache_disabled:
        blockers.append(f"{channel.channel_id}: registry download proof must disable pip cache")
    if not install_cache_disabled:
        blockers.append(f"{channel.channel_id}: registry install proof must disable pip cache")
    if install_cache_hit_detected:
        blockers.append(f"{channel.channel_id}: registry install proof used pip cache")
    if downloaded_artifact is None:
        blockers.append(f"{channel.channel_id}: downloaded registry artifact missing")
    elif downloaded_sha256 is None:
        blockers.append(
            f"{channel.channel_id}: downloaded registry artifact SHA256 missing: "
            f"{downloaded_artifact.name}"
        )
    if installed_filename is None:
        blockers.append(f"{channel.channel_id}: installed registry artifact filename missing")
    elif installed_artifact is None:
        blockers.append(
            f"{channel.channel_id}: installed registry artifact not found in registry JSON: "
            f"{installed_filename}"
        )
    elif not installed_artifact.get("sha256"):
        blockers.append(
            f"{channel.channel_id}: installed registry artifact missing SHA256: {installed_filename}"
        )
    elif downloaded_sha256 != installed_artifact.get("sha256"):
        blockers.append(
            f"{channel.channel_id}: downloaded registry artifact SHA256 mismatch: "
            f"{installed_filename}"
        )
    proof = {
        "registry_release_json_url": registry_release_json_url(channel, version),
        "registry_release_artifact_count": len(artifacts),
        "registry_release_artifacts": artifacts,
        "downloaded_registry_artifact_ref": rel(repo_root, downloaded_artifact),
        "downloaded_registry_artifact_filename": downloaded_filename,
        "downloaded_registry_artifact_sha256": downloaded_sha256,
        "registry_download_isolated": download_isolated,
        "registry_download_cache_disabled": download_cache_disabled,
        "installed_registry_artifact_filename": installed_filename,
        "installed_registry_artifact": installed_artifact,
        "installed_registry_artifact_sha256": downloaded_sha256,
        "registry_install_from_downloaded_artifact": downloaded_artifact is not None,
        "registry_install_cache_disabled": install_cache_disabled,
        "registry_install_cache_hit_detected": install_cache_hit_detected,
        "registry_artifact_digest_binding_status": "passed" if not blockers else "blocked",
    }
    return proof, blockers


def resolve_shardloom_bin(repo_root: Path, explicit: Path | None) -> tuple[Path | None, list[str]]:
    raw = explicit or (Path(os.environ[ENV_BINARY]) if os.environ.get(ENV_BINARY) else None)
    if raw is None:
        return None, [f"registry proof requires --shardloom-bin or {ENV_BINARY}"]
    binary = raw if raw.is_absolute() else repo_root / raw
    binary = binary.resolve()
    blockers: list[str] = []
    if not binary.exists():
        blockers.append(f"registry proof CLI binary does not exist: {rel(repo_root, binary)}")
    elif not binary.is_file():
        blockers.append(f"registry proof CLI binary is not a file: {rel(repo_root, binary)}")
    elif os.name != "nt" and not os.access(binary, os.X_OK):
        blockers.append(f"registry proof CLI binary is not executable: {rel(repo_root, binary)}")
    return binary, blockers


def smoke_env(cli_binary: Path) -> dict[str, str]:
    env = os.environ.copy()
    env[ENV_BINARY] = str(cli_binary)
    return env


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
    shardloom_bin: Path | None,
    downloaded_artifact: Path | None = None,
    setup_blockers: list[str] | None = None,
) -> int:
    downloaded = step_status(steps, "download_registry_artifact") == "passed"
    installed = step_status(steps, "install_downloaded_registry_artifact") == "passed"
    smoked = step_status(steps, "registry_package_client_smoke") == "passed"
    uninstalled = step_status(steps, "uninstall_registry_package") == "passed"
    smoke_stdout = "\n".join(
        step.get("stdout", "") for step in steps if step["name"] == "registry_package_client_smoke"
    )
    fallback_attempted = "fallback_attempted=True" in smoke_stdout
    external_engine_invoked = "external_engine_invoked=True" in smoke_stdout
    setup_blockers = setup_blockers or []
    registry_artifacts, registry_artifact_blockers = (
        registry_artifact_proof(
            channel,
            version,
            steps,
            repo_root=repo_root,
            downloaded_artifact=downloaded_artifact,
        )
        if downloaded and installed
        else ({}, [])
    )
    proof_passed = (
        installed
        and smoked
        and uninstalled
        and not fallback_attempted
        and not external_engine_invoked
        and not setup_blockers
        and not registry_artifact_blockers
        and shardloom_bin is not None
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
        "download_transcript_status": step_status(steps, "download_registry_artifact"),
        "install_transcript_status": step_status(steps, "install_downloaded_registry_artifact"),
        "smoke_check_status": step_status(steps, "registry_package_client_smoke"),
        "uninstall_transcript_status": step_status(steps, "uninstall_registry_package"),
        "fallback_attempted": fallback_attempted,
        "external_engine_invoked": external_engine_invoked,
        "testpypi_proof_ref": testpypi_proof_ref,
        "testpypi_proof_required": channel.channel_id == "pypi",
        "cli_binary_required_for_clean_registry_smoke": True,
        "cli_binary_available": shardloom_bin is not None and not setup_blockers,
        "cli_binary_ref": rel(repo_root, shardloom_bin),
        "cli_binary_env_var": ENV_BINARY,
        "cli_binary_smoke_source": "approved_release_or_local_artifact",
        "registry_upload_attempted_by_this_tool": False,
        "publication_attempted_by_this_tool": False,
        "tag_created": False,
        "secrets_required": False,
        "package_channel_submission_attempted_by_this_tool": False,
        "steps": steps,
        **registry_artifacts,
    }
    if channel.channel_id == "pypi" and not testpypi_proof_ref:
        report["proof_status"] = "failed"
        report["blockers"] = ["pypi proof requires a prior TestPyPI proof reference"]
    else:
        blockers = list(setup_blockers)
        blockers.extend(registry_artifact_blockers)
        if report["proof_status"] != "passed":
            blockers.append("registry proof step failed")
        if shardloom_bin is None:
            blockers.append("registry proof requires an approved ShardLoom CLI binary")
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
    download_dir = resolve_under_repo(repo_root, args.download_dir)
    output = resolve_under_repo(repo_root, args.output)
    shardloom_bin, setup_blockers = resolve_shardloom_bin(repo_root, args.shardloom_bin)
    if channel.channel_id == "pypi" and not args.testpypi_proof_ref:
        return write_transcript(
            repo_root=repo_root,
            output=output,
            channel=channel,
            version=args.version,
            venv_dir=venv_dir,
            steps=[],
            testpypi_proof_ref=None,
            shardloom_bin=shardloom_bin,
            downloaded_artifact=None,
            setup_blockers=setup_blockers,
        )
    if setup_blockers:
        return write_transcript(
            repo_root=repo_root,
            output=output,
            channel=channel,
            version=args.version,
            venv_dir=venv_dir,
            steps=[],
            testpypi_proof_ref=args.testpypi_proof_ref,
            shardloom_bin=shardloom_bin,
            downloaded_artifact=None,
            setup_blockers=setup_blockers,
        )

    if venv_dir.exists():
        remove_tree_under_repo(repo_root, venv_dir)
    if download_dir.exists():
        remove_tree_under_repo(repo_root, download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
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
                name="download_registry_artifact",
                command=download_command(clean_python, channel, args.version, download_dir),
                cwd=repo_root,
            )
        )
    downloaded_artifact = (
        locate_downloaded_registry_artifact(download_dir)
        if step_status(steps, "download_registry_artifact") == "passed"
        else None
    )
    if downloaded_artifact is not None:
        steps.append(
            run_step(
                repo_root=repo_root,
                name="install_downloaded_registry_artifact",
                command=install_downloaded_artifact_command(clean_python, downloaded_artifact),
                cwd=repo_root,
            )
        )
    if step_status(steps, "install_downloaded_registry_artifact") == "passed":
        steps.append(
            run_step(
                repo_root=repo_root,
                name="registry_package_client_smoke",
                command=smoke_command(clean_python),
                cwd=repo_root,
                env=smoke_env(shardloom_bin),
            )
        )
    if step_status(steps, "install_downloaded_registry_artifact") == "passed":
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
        shardloom_bin=shardloom_bin,
        downloaded_artifact=downloaded_artifact,
        setup_blockers=setup_blockers,
    )


if __name__ == "__main__":
    raise SystemExit(main())
