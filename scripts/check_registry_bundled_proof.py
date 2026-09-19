#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate recorded isolated bundled-CLI proof without executing its commands."""

from email.parser import Parser
import hashlib
import json
from pathlib import PurePosixPath
import re
from typing import Any

from release_channel_contract import PUBLISHED_REGISTRY_BUNDLED_SMOKE_SHA256


STEP_NAMES = (
    "create_fresh_venv", "verify_clean_venv", "install_verified_wheel",
    "bundled_cli_complete_values", "uninstall_verified_wheel", "verify_uninstalled",
)


def bundled_registry_proof_blockers(
    proof: dict[str, Any], *, channel_id: str, package_version: str,
    runtime_source_commit: str | None,
    smoke_stdout: bytes | None = None,
) -> list[str]:
    prefix = f"{channel_id}: bundled CLI proof "
    errors: list[str] = []

    def require(condition: bool, message: str) -> None:
        if not condition:
            errors.append(prefix + message)

    supplement = proof.get("bundled_cli_supplemental_proof")
    if not isinstance(supplement, dict):
        return [prefix + "is required"]
    for field in ("proof_status", "status", "uninstall_transcript_status"):
        require(supplement.get(field) == "passed", f"{field} must be passed")
    require(supplement.get("blockers") == [], "must have no blockers")
    require(supplement.get("channel_id") == channel_id, "must match the registry channel")
    require(runtime_source_commit is not None and supplement.get("source_commit") == runtime_source_commit,
            "must match the approved runtime source")
    for field in (
        "external_cli_override", "source_python_path_override", "fallback_attempted",
        "external_engine_invoked", "secrets_required", "publication_attempted_by_this_tool",
        "registry_upload_attempted_by_this_tool", "package_upload_attempted_by_this_tool",
        "package_channel_submission_attempted_by_this_tool",
    ):
        require(supplement.get(field) is False, f"{field} must be false")

    wheel = supplement.get("wheel_identity")
    wheel = wheel if isinstance(wheel, dict) else {}
    filename = proof.get("downloaded_registry_artifact_filename")
    digest = proof.get("downloaded_registry_artifact_sha256")
    require(isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) is not None
            and wheel.get("sha256") == digest == proof.get("installed_registry_artifact_sha256"),
            "wheel digest must match the downloaded and installed artifact")
    wheel_path = wheel.get("path")
    require(isinstance(filename, str) and isinstance(wheel_path, str)
            and PurePosixPath(wheel_path).name == filename == proof.get("installed_registry_artifact_filename"),
            "wheel filename must match the downloaded and installed artifact")
    metadata = supplement.get("wheel_metadata")
    if isinstance(metadata, str) and isinstance(filename, str):
        headers = Parser().parsestr(metadata)
        tag = filename.removeprefix(f"shardloom-{package_version}-").removesuffix(".whl")
        require(headers.get("Root-Is-Purelib") == "false" and headers.get_all("Tag") == [tag],
                "wheel metadata must match the platform wheel")
    else:
        require(False, "wheel metadata is required")

    steps = supplement.get("steps")
    if not isinstance(steps, list) or [s.get("name") if isinstance(s, dict) else None for s in steps] != list(STEP_NAMES):
        require(False, "requires all six ordered installation, execution and uninstall steps")
        steps = []
    for step in steps:
        require(type(step.get("returncode")) is int and step["returncode"] == 0,
                f"{step['name']} must exit successfully")
        for field in ("direct_child_reaped", "process_group_drained"):
            require(step.get(field) is True, f"{step['name']} {field} must be true")
        for field in ("timed_out", "group_absence_unproved_at_cleanup"):
            require(step.get(field) is False, f"{step['name']} {field} must be false")
        require("interrupted_signal" in step and step["interrupted_signal"] is None,
                f"{step['name']} must not be interrupted")
        for field in ("stdout_sha256", "stderr_sha256"):
            value = step.get(field)
            require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None,
                    f"{step['name']} requires {field}")
        command = step.get("command")
        require(isinstance(command, list) and all(isinstance(arg, str) for arg in command)
                and "-I" in command, f"{step['name']} requires an isolated Python command")
    if steps:
        command = steps[2].get("command")
        command = command if isinstance(command, list) else []
        require(all(arg in command for arg in ("-m", "pip", "--isolated", "install", "--no-index", "--no-cache-dir", "--no-deps"))
                and bool(command) and command[-1] == wheel_path,
                "install command must use the verified wheel in isolation")
        uninstall = steps[4].get("command")
        uninstall = uninstall if isinstance(uninstall, list) else []
        require(all(arg in uninstall for arg in ("-m", "pip", "--isolated", "uninstall", "-y", "shardloom")),
                "uninstall command must remove the verified package")

        smoke_command = steps[3].get("command")
        approved_program = PUBLISHED_REGISTRY_BUNDLED_SMOKE_SHA256.get(package_version)
        require(isinstance(smoke_command, list) and len(smoke_command) == 4
                and smoke_command[1:3] == ["-I", "-c"]
                and isinstance(smoke_command[3], str) and approved_program is not None
                and hashlib.sha256(smoke_command[3].encode()).hexdigest() == approved_program,
                "must execute the approved complete-value smoke program")
        require(isinstance(smoke_stdout, bytes) and len(smoke_stdout) <= 65536
                and hashlib.sha256(smoke_stdout).hexdigest() == steps[3].get("stdout_sha256"),
                "captured smoke stdout must match the recorded digest")
        try:
            captured_result = json.loads(smoke_stdout) if isinstance(smoke_stdout, bytes) else None
        except (ValueError, UnicodeError):
            captured_result = None
        require(isinstance(captured_result, dict) and captured_result == supplement.get("result"),
                "result must equal the captured smoke stdout")

    result = supplement.get("result")
    result = result if isinstance(result, dict) else {}
    require(result.get("cli_version") == package_version, "CLI version must match the selected release")
    for field in ("fallback_attempted", "external_engine_invoked"):
        require(result.get(field) is False, f"result {field} must be false")
    expected = [{"id": 2, "label": "βeta 雪", "amount": 15},
                {"id": 3, "label": "gamma 🧵", "amount": 27}]
    require(result.get("exact_results") == [expected, expected, expected],
            "must contain the complete DataFrame and two SQL results")
    require(result.get("unsupported_blocker") == "cg21.workflow.to_pandas.decoded_dataframe_unsupported",
            "must record the expected unsupported diagnostic")
    directory = supplement.get("proof_directory")
    package = result.get("package_path")
    cli = result.get("resolved_cli_path")
    paths_valid = all(isinstance(value, str) and value.startswith("/")
                      and ".." not in PurePosixPath(value).parts for value in (directory, package, cli))
    if paths_valid:
        package_path, cli_path = PurePosixPath(package), PurePosixPath(cli)
        paths_valid = (package_path.is_relative_to(PurePosixPath(directory) / "venv")
                       and package_path.parts[-3:] == ("site-packages", "shardloom", "__init__.py")
                       and cli_path.is_relative_to(package_path.parent / "bin")
                       and cli_path.name in ("shardloom", "shardloom.exe"))
    require(paths_valid, "must resolve the package and bundled CLI inside the same clean venv")
    if paths_valid and steps:
        venv = PurePosixPath(directory) / "venv"
        create = steps[0].get("command")
        require(isinstance(create, list) and create[1:] == ["-I", "-m", "venv", str(venv)],
                "must create the recorded clean venv")
        for step in steps[1:]:
            command = step.get("command")
            require(isinstance(command, list) and bool(command) and command[0] == str(venv / "bin/python"),
                    f"{step['name']} must use the recorded clean venv interpreter")
        clean_program = "import importlib.util; assert importlib.util.find_spec('shardloom') is None"
        for index in (1, 5):
            require(steps[index].get("command") == [str(venv / "bin/python"), "-I", "-c", clean_program],
                    f"{steps[index]['name']} must verify the package is absent")
        require(steps[2].get("command") == [str(venv / "bin/python"), "-I", "-m", "pip", "--isolated",
                "install", "--no-index", "--no-cache-dir", "--no-deps", wheel_path],
                "install command must contain only the verified isolated wheel installation")
        require(steps[4].get("command") == [str(venv / "bin/python"), "-I", "-m", "pip", "--isolated",
                "uninstall", "-y", "shardloom"],
                "uninstall command must contain only the verified isolated package removal")
    return errors
