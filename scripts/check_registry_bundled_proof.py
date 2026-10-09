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


def same_json(actual: Any, expected: Any) -> bool:
    """Compare complete JSON values without accepting bool/int or int/float drift."""
    try:
        return json.dumps(actual, sort_keys=True, allow_nan=False) == json.dumps(
            expected, sort_keys=True, allow_nan=False
        )
    except (TypeError, ValueError):
        return False


def expanded_workflow_blockers(result: dict[str, Any]) -> list[str]:
    """Validate the additional complete-result contract of the approved v0.5.1 smoke."""
    errors: list[str] = []
    rows = [
        {"id": 1, "label": "café", "amount": 8},
        {"id": 2, "label": "東京", "amount": 15},
        {"id": 3, "label": "naïve", "amount": 27},
        {"id": 4, "label": "雪", "amount": 27},
    ]
    expected = {
        "native_vortex_roundtrip_values": rows,
        "analytic_frame_values": [
            {"id": 1, "label": "café", "n": 1, "first_label": "café"},
            {"id": 2, "label": "東京", "n": 2, "first_label": "café"},
            {"id": 3, "label": "naïve", "n": 2, "first_label": "東京"},
            {"id": 4, "label": "雪", "n": 2, "first_label": "naïve"},
        ],
        "correlated_scalar_subquery_values": [
            {"value": 0, "scalar": None}, {"value": 1, "scalar": 1},
            {"value": 2, "scalar": 2}, {"value": 3, "scalar": 3},
        ],
        "repeated_sql_executions": 2,
        "actual_disk_pressure_claimed_by_smoke": False,
    }
    for field, value in expected.items():
        if not same_json(result.get(field), value):
            errors.append(f"must contain the approved complete {field}")
    digest = result.get("native_vortex_output_sha256")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        errors.append("must identify the native Vortex output digest")
    if (result.get("cli_distribution") != "bundled_wheel"
            or result.get("explicit_homebrew_cli_binding") is not False
            or not isinstance(result.get("bundled_cli"), str)
            or result.get("bundled_cli") != result.get("verified_native_cli")):
        errors.append("must identify the same bundled wheel CLI without a Homebrew override")
    workflows = {
        "vortex_incremental_results": rows,
        "streamed_general_aggregate": [
            {"team": "blue", "total": 1.0, "distinct_amounts": 1},
            {"team": "red", "total": 10.0, "distinct_amounts": 2},
        ],
        "streamed_left_join": [
            {"amount": 3, "customer": "Ada"}, {"amount": 5, "customer": None},
            {"amount": 4, "customer": "Ada"},
        ],
        "streamed_analytic_window": [
            {"sequence": 1, "team": "red", "previous_two_total": 3.0, "distinct_amounts": 1},
            {"sequence": 2, "team": "blue", "previous_two_total": 1.0, "distinct_amounts": 1},
            {"sequence": 3, "team": "red", "previous_two_total": 7.0, "distinct_amounts": 2},
            {"sequence": 4, "team": "red", "previous_two_total": 7.0, "distinct_amounts": 2},
        ],
        "sparse_pivot": [
            {"entity": 1, "pivot_a": 7.0, "pivot_b": None},
            {"entity": 2, "pivot_a": None, "pivot_b": 5.0},
        ],
    }
    incremental = result.get("incremental_complete_values")
    if not isinstance(incremental, dict) or set(incremental) != set(workflows):
        return errors + ["must contain all five complete incremental workflows"]
    for name, values in workflows.items():
        item = incremental[name]
        if not isinstance(item, dict) or not same_json(item.get("rows"), values):
            errors.append(f"{name} must contain the complete typed rows")
        if not isinstance(item, dict):
            continue
        count = item.get("batches")
        if type(count) is not int or not 0 < count <= len(values):
            errors.append(f"{name} must record a valid acknowledged batch count")
        for field in ("final_report_success", "owned_workspace_empty"):
            if item.get(field) is not True:
                errors.append(f"{name} {field} must be true")
        if item.get("explicit_spill_policy") is not (name != "vortex_incremental_results"):
            errors.append(f"{name} must record its approved spill policy")
    return errors


def bundled_registry_proof_blockers(
    proof: dict[str, Any], *, channel_id: str, package_version: str,
    runtime_source_commit: str | None,
    smoke_stdout: bytes | None = None,
) -> list[str]:
    prefix = f"{channel_id}: bundled CLI proof "
    errors: list[str] = []
    expanded_workflows = package_version == "0.5.1"

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
    source_field = "source_commit" if package_version == "0.2.4" else "release_source_commit"
    require(runtime_source_commit is not None and supplement.get(source_field) == runtime_source_commit,
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
        require(isinstance(smoke_command, list) and len(smoke_command) == (5 if expanded_workflows else 4)
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
        require(isinstance(captured_result, dict) and same_json(captured_result, supplement.get("result")),
                "result must equal the captured smoke stdout")
        if expanded_workflows:
            cli_digest = captured_result.get("cli_sha256") if isinstance(captured_result, dict) else None
            require(isinstance(cli_digest, str) and re.fullmatch(r"[0-9a-f]{64}", cli_digest) is not None
                    and isinstance(smoke_command, list) and len(smoke_command) == 5
                    and smoke_command[4] == cli_digest,
                    "must bind the bundled CLI SHA256 argument to its captured result")
            require(supplement.get("shared_smoke_sha256") == approved_program,
                    "shared smoke digest must match the approved program")

    result = supplement.get("result")
    result = result if isinstance(result, dict) else {}
    require(result.get("version" if expanded_workflows else "cli_version") == package_version,
            "CLI version must match the selected release")
    for field in ("fallback_attempted", "external_engine_invoked"):
        require(result.get(field) is False, f"result {field} must be false")
    expected = ([{"id": 2, "label": "東京", "amount": 15},
                 {"id": 3, "label": "naïve", "amount": 27}] if expanded_workflows else
                [{"id": 2, "label": "βeta 雪", "amount": 15},
                 {"id": 3, "label": "gamma 🧵", "amount": 27}])
    require(same_json(result.get("exact_results"), [expected, expected, expected]),
            "must contain the complete DataFrame and two SQL results")
    if expanded_workflows:
        errors.extend(prefix + error for error in expanded_workflow_blockers(result))
    require(result.get("unsupported_blocker") == "cg21.workflow.to_pandas.decoded_dataframe_unsupported",
            "must record the expected unsupported diagnostic")
    directory = supplement.get("proof_directory")
    package = result.get("package_path")
    cli = result.get("bundled_cli" if expanded_workflows else "resolved_cli_path")
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
