#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate v1 local resource safety, cancellation, and cleanup evidence.

This gate is local and bounded. It validates existing ShardLoom resource and
fault-tolerance evidence without claiming larger-than-memory execution, native
spill runtime, distributed OOM handling, package publication, or production
readiness.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from release_feature_contract import RELEASE_USER_SURFACE_EXAMPLE_FEATURES
from release_report_utils import fail_closed_fields, load_json, read_text, resolve_path, write_json


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_local_resource_safety_report.v1"
DEFAULT_FEATURES = RELEASE_USER_SURFACE_EXAMPLE_FEATURES
DOC_PATH = Path("docs/architecture/v1-local-resource-safety.md")

COMMAND_SPECS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("pre_oom_memory_guard", ("pre-oom-memory-guard-smoke", "--format", "json")),
    (
        "retry_gate",
        (
            "retry-gate-plan",
            "retry-requested,retry-allowed,cleanup-completed",
            "--format",
            "json",
        ),
    ),
    (
        "cancellation_gate",
        (
            "cancellation-gate-plan",
            "cancellation-requested,cleanup-required,cleanup-completed",
            "--format",
            "json",
        ),
    ),
    (
        "memory_runtime_hardening_gate",
        ("cg14-memory-runtime-hardening-gate", "--format", "json"),
    ),
    (
        "fault_tolerance_promotion_gate",
        ("fault-tolerance-promotion-gate", "--format", "json"),
    ),
    (
        "public_native_vortex_resource_route",
        (
            "run",
            "cli",
            "--input",
            "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--execution-policy",
            "native_vortex",
            "--materialization-policy",
            "bounded",
            "--evidence-level",
            "runtime_smoke",
            "--bounded",
            "true",
            "--vortex-primitive",
            "aggregate",
            "--vortex-aggregate",
            '{"measures":[{"function":"sum","column":"metric","alias":"sum_metric"},{"function":"count","alias":"rows"}]}',
            "--memory-gb",
            "1",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ),
    ),
)

DOC_MARKERS = (
    "shardloom.v1_local_resource_safety.v1",
    "python scripts/check_v1_local_resource_safety.py",
    "target/v1-local-resource-safety-report.json",
    "pre-oom-memory-guard-smoke --format json",
    "retry-gate-plan retry-requested,retry-allowed,cleanup-completed --format json",
    "cancellation-gate-plan cancellation-requested,cleanup-required,cleanup-completed --format json",
    "run cli --input shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex --input-format vortex --request collect --execution-policy native_vortex",
    "target/v1-source-prepared-state-scope-report.json",
    "target/v1-local-output-sink-scope-report.json",
    "no larger-than-memory claim",
    "no native spill runtime claim",
    "no distributed OOM/resource claim",
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-local-resource-safety-report.json"),
    )
    parser.add_argument(
        "--source-prepared-report",
        type=Path,
        default=Path("target/v1-source-prepared-state-scope-report.json"),
    )
    parser.add_argument(
        "--local-output-report",
        type=Path,
        default=Path("target/v1-local-output-sink-scope-report.json"),
    )
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--features", default=DEFAULT_FEATURES)
    parser.add_argument("--skip-build", action="store_true")
    return parser.parse_args(argv)


def rel(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def tail(text: str, limit: int = 4000) -> str:
    return text if len(text) <= limit else text[-limit:]


def command_text(command: Sequence[str]) -> str:
    return " ".join(command).replace(str(sys.executable), "python")


def command_env(repo_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    python_path = str(repo_root / "python" / "src")
    env["PYTHONPATH"] = (
        python_path
        if not env.get("PYTHONPATH")
        else python_path + os.pathsep + env["PYTHONPATH"]
    )
    return env


def run_plain_command(
    repo_root: Path,
    command: list[str],
    *,
    include_raw_stdout: bool = False,
) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=repo_root,
        env=command_env(repo_root),
        text=True,
        capture_output=True,
        check=False,
    )
    elapsed = (time.perf_counter() - started) * 1000.0
    result: dict[str, Any] = {
        "command": command_text(command),
        "argv": command,
        "returncode": completed.returncode,
        "status": "passed" if completed.returncode == 0 else "failed",
        "elapsed_millis": round(elapsed, 4),
        "stdout_tail": tail(completed.stdout),
        "stderr_tail": tail(completed.stderr),
    }
    if include_raw_stdout:
        result["_stdout"] = completed.stdout
    return result


def run_command(repo_root: Path, command: list[str]) -> dict[str, Any]:
    result = run_plain_command(repo_root, command, include_raw_stdout=True)
    stdout = str(result.pop("_stdout", ""))
    payload: Any = None
    parse_error = None
    if stdout.strip():
        try:
            payload = load_json_from_text(stdout)
        except ValueError as error:
            parse_error = str(error)
    result["status"] = (
        "passed" if result["returncode"] == 0 and parse_error is None else "failed"
    )
    result["json_parse_error"] = parse_error
    result["payload"] = payload
    return result


def load_json_from_text(text: str) -> Any:
    import json

    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError(str(error)) from error


def locate_binary(repo_root: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        return resolve_path(repo_root, explicit).resolve()
    target_root = Path(os.environ.get("CARGO_TARGET_DIR", repo_root / "target"))
    if not target_root.is_absolute():
        target_root = repo_root / target_root
    suffix = ".exe" if os.name == "nt" else ""
    return (target_root / "debug" / f"shardloom{suffix}").resolve()


def ensure_binary_executable(binary: Path) -> tuple[bool, str | None]:
    if os.name == "nt" or not binary.exists() or not binary.is_file():
        return False, None
    if os.access(binary, os.X_OK):
        return False, None
    try:
        binary.chmod(binary.stat().st_mode | 0o111)
    except OSError as error:
        return False, f"failed to mark shardloom binary executable: {error}"
    return True, None


def ensure_binary(
    repo_root: Path,
    *,
    binary: Path,
    features: str,
    skip_build: bool,
    explicit_binary: bool,
) -> tuple[dict[str, Any], list[str]]:
    if binary.exists() and (skip_build or explicit_binary):
        normalized, executable_blocker = ensure_binary_executable(binary)
        blockers = [executable_blocker] if executable_blocker else []
        return {
            "command": "reused explicit/existing shardloom binary",
            "status": "passed" if not blockers else "failed",
            "binary_ref": rel(repo_root, binary),
            "binary_executable_permission_normalized": normalized,
            "blockers": blockers,
        }, blockers
    if skip_build:
        blocker = f"binary missing and --skip-build was set: {rel(repo_root, binary)}"
        return {
            "command": "skipped",
            "status": "failed",
            "binary_ref": rel(repo_root, binary),
            "blockers": [blocker],
        }, [blocker]
    command = [
        "cargo",
        "build",
        "-q",
        "-p",
        "shardloom-cli",
        "--features",
        features,
    ]
    result = run_plain_command(repo_root, command)
    blockers: list[str] = []
    if result["returncode"] != 0:
        blockers.append("failed to build shardloom CLI for v1 local resource safety")
    if not binary.exists():
        blockers.append(f"built binary missing: {rel(repo_root, binary)}")
    normalized = False
    if binary.exists():
        normalized, executable_blocker = ensure_binary_executable(binary)
        if executable_blocker:
            blockers.append(executable_blocker)
    result.update(
        {
            "status": "passed" if not blockers else "failed",
            "binary_ref": rel(repo_root, binary),
            "binary_executable_permission_normalized": normalized,
            "blockers": blockers,
        }
    )
    result.pop("payload", None)
    return result, blockers


def collect_field_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        direct = payload.get("fields")
        if isinstance(direct, list):
            rows.extend(row for row in direct if isinstance(row, dict))
        for value in payload.values():
            rows.extend(collect_field_rows(value))
    elif isinstance(payload, list):
        for value in payload:
            rows.extend(collect_field_rows(value))
    return rows


def fields(payload: Any) -> dict[str, str]:
    return {
        str(row.get("key")): str(row.get("value"))
        for row in collect_field_rows(payload)
        if isinstance(row.get("key"), str)
    }


def bool_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def check_fields(
    label: str,
    payload: Mapping[str, Any],
    expected_command: str,
    required: Mapping[str, str],
    false_fields: Sequence[str],
    true_fields: Sequence[str] = (),
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if payload.get("schema_version") != "shardloom.output.v2":
        blockers.append(f"{label}: output schema must be shardloom.output.v2")
    if payload.get("command") != expected_command:
        blockers.append(f"{label}: command={payload.get('command', 'missing')}")
    if payload.get("status") != "success":
        blockers.append(f"{label}: status={payload.get('status', 'missing')}")
    fallback = payload.get("fallback", {})
    if not isinstance(fallback, dict) or fallback.get("attempted") is not False:
        blockers.append(f"{label}: top-level fallback.attempted must be false")
    observed = fields(payload)
    for key, expected in required.items():
        if observed.get(key) != expected:
            blockers.append(f"{label}: {key}={observed.get(key, 'missing')}, expected {expected}")
    for key in false_fields:
        if bool_value(observed.get(key)) is not False:
            blockers.append(f"{label}: {key} must be false")
    for key in true_fields:
        if bool_value(observed.get(key)) is not True:
            blockers.append(f"{label}: {key} must be true")
    return {
        "status": "passed" if not blockers else "failed",
        "command": expected_command,
        "field_count": len(observed),
        "required_field_count": len(required) + len(false_fields) + len(true_fields),
        "selected_fields": {
            key: observed.get(key)
            for key in sorted(set(required) | set(false_fields) | set(true_fields))
        },
    }, blockers


def ensure_report(
    repo_root: Path,
    path: Path,
    command: list[str],
    label: str,
) -> tuple[dict[str, Any] | None, dict[str, Any], list[str]]:
    resolved = resolve_path(repo_root, path)
    command_summary: dict[str, Any] = {
        "command": command_text(command),
        "status": "skipped_existing_report" if resolved.exists() else "not_run",
        "report_ref": str(path).replace("\\", "/"),
    }
    blockers: list[str] = []
    if not resolved.exists():
        command_summary = run_command(repo_root, command)
        command_summary["report_ref"] = str(path).replace("\\", "/")
        if command_summary["returncode"] != 0:
            blockers.append(f"{label}: prerequisite generator failed")
    payload = load_json(resolved, missing_ok=True)
    if payload is None:
        blockers.append(f"{label}: missing report {path}")
        return None, command_summary, blockers
    if not isinstance(payload, dict):
        blockers.append(f"{label}: report must be an object")
        return None, command_summary, blockers
    return payload, command_summary, blockers


def validate_source_prepared(payload: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected = {
        "schema_version": "shardloom.v1_source_prepared_state_scope_report.v1",
        "status": "passed",
        "claim_gate_status": "not_claim_grade",
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            blockers.append(f"source_prepared_state: {key}={payload.get(key, 'missing')}")
    for key in (
        "v1_scope_ready",
        "all_no_fallback_no_external_engine",
        "all_prepared_routes_expose_reuse_contract",
        "all_internal_source_smoke_routes_are_labeled_non_persistent",
        "all_generated_routes_expose_single_artifact_output",
    ):
        if payload.get(key) is not True:
            blockers.append(f"source_prepared_state: {key} must be true")
    for key in fail_closed_fields():
        if payload.get(key) is not False:
            blockers.append(f"source_prepared_state: {key} must be false")
    return {
        "status": "passed" if not blockers else "failed",
        "prepared_route_count": len(payload.get("prepared_route_ids", [])),
        "unsupported_boundary_count": len(payload.get("unsupported_boundary_ids", [])),
        "internal_source_smoke_non_persistent": payload.get(
            "all_internal_source_smoke_routes_are_labeled_non_persistent"
        )
        is True,
    }, blockers


def validate_local_output(payload: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected = {
        "schema_version": "shardloom.v1_local_output_sink_scope_report.v1",
        "status": "passed",
        "claim_gate_status": "not_claim_grade",
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            blockers.append(f"local_output_sink: {key}={payload.get(key, 'missing')}")
    for key in (
        "v1_scope_ready",
        "all_no_fallback_no_external_engine",
        "all_output_routes_emit_sink_evidence",
        "all_output_routes_no_fallback_no_external_engine",
        "all_write_methods_no_fallback_no_external_engine",
        "write_policy_contract_ready",
        "local_output_sink_benchmark_replay_ready",
    ):
        if payload.get(key) is not True:
            blockers.append(f"local_output_sink: {key} must be true")
    for key in fail_closed_fields():
        if payload.get(key) is not False:
            blockers.append(f"local_output_sink: {key} must be false")
    return {
        "status": "passed" if not blockers else "failed",
        "output_route_count": len(payload.get("output_route_ids", [])),
        "write_method_count": len(payload.get("user_write_methods", [])),
        "unsupported_boundary_count": len(payload.get("unsupported_boundary_ids", [])),
        "write_policy_contract_ready": payload.get("write_policy_contract_ready") is True,
        "sink_replay_ready": payload.get("local_output_sink_benchmark_replay_ready") is True,
    }, blockers


def validate_doc(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    text = read_text(resolve_path(repo_root, DOC_PATH), missing_ok=True)
    blockers = [
        f"{DOC_PATH}: missing marker {marker!r}"
        for marker in DOC_MARKERS
        if marker not in text
    ]
    return {
        "status": "passed" if not blockers else "failed",
        "doc_ref": DOC_PATH.as_posix(),
        "marker_count": len(DOC_MARKERS),
        "missing_marker_count": len(blockers),
    }, blockers


def build_report(
    *,
    repo_root: Path,
    binary: Path,
    explicit_binary: bool,
    features: str,
    skip_build: bool,
    source_prepared_report: Path,
    local_output_report: Path,
) -> dict[str, Any]:
    blockers: list[str] = []
    build, build_blockers = ensure_binary(
        repo_root,
        binary=binary,
        features=features,
        skip_build=skip_build,
        explicit_binary=explicit_binary,
    )
    blockers.extend(build_blockers)

    doc_summary, doc_blockers = validate_doc(repo_root)
    blockers.extend(doc_blockers)

    runtime_commands: dict[str, dict[str, Any]] = {}
    command_summaries: dict[str, dict[str, Any]] = {}
    for label, argv in COMMAND_SPECS:
        command = [str(binary), *argv]
        result = run_command(repo_root, command)
        runtime_commands[label] = {
            key: value for key, value in result.items() if key != "payload"
        }
        if result["returncode"] != 0:
            blockers.append(f"{label}: command failed")
        payload = result.get("payload")
        if not isinstance(payload, dict):
            blockers.append(f"{label}: command output was not a JSON object")
            command_summaries[label] = {"status": "failed", "command": argv[0]}
            continue
        summary, command_blockers = validate_command_payload(label, payload)
        command_summaries[label] = summary
        blockers.extend(command_blockers)

    source_payload, source_command, source_blockers = ensure_report(
        repo_root,
        source_prepared_report,
        [sys.executable, "scripts/check_v1_source_prepared_state_scope.py"],
        "source_prepared_state",
    )
    blockers.extend(source_blockers)
    local_output_payload, local_output_command, local_output_blockers = ensure_report(
        repo_root,
        local_output_report,
        [sys.executable, "scripts/check_v1_local_output_sink_scope.py"],
        "local_output_sink",
    )
    blockers.extend(local_output_blockers)

    source_summary: dict[str, Any] = {"status": "failed"}
    local_output_summary: dict[str, Any] = {"status": "failed"}
    if isinstance(source_payload, dict):
        source_summary, source_validate_blockers = validate_source_prepared(source_payload)
        blockers.extend(source_validate_blockers)
    if isinstance(local_output_payload, dict):
        local_output_summary, local_output_validate_blockers = validate_local_output(
            local_output_payload
        )
        blockers.extend(local_output_validate_blockers)

    passed = not blockers
    command_pass_count = sum(
        1 for summary in command_summaries.values() if summary.get("status") == "passed"
    )
    no_fallback_no_external = passed and all(
        summary.get("status") == "passed" for summary in command_summaries.values()
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "blockers": blockers,
        "build": build,
        "binary_ref": rel(repo_root, binary),
        "scope_document": DOC_PATH.as_posix(),
        "doc_summary": doc_summary,
        "runtime_command_count": len(COMMAND_SPECS),
        "runtime_command_pass_count": command_pass_count,
        "runtime_commands": runtime_commands,
        "command_summaries": command_summaries,
        "prerequisite_report_count": 2,
        "source_prepared_state_report_ref": str(source_prepared_report).replace("\\", "/"),
        "local_output_sink_report_ref": str(local_output_report).replace("\\", "/"),
        "source_prepared_state_command": source_command,
        "local_output_sink_command": local_output_command,
        "source_prepared_state_summary": source_summary,
        "local_output_sink_summary": local_output_summary,
        "v1_scope_ready": passed,
        "local_resource_safety_evidence_ready": passed,
        "memory_budget_config_status": command_summaries.get("pre_oom_memory_guard", {}).get(
            "status",
            "failed",
        ),
        "pre_oom_guard_status": command_summaries.get("pre_oom_memory_guard", {}).get(
            "status",
            "failed",
        ),
        "retry_gate_status": command_summaries.get("retry_gate", {}).get("status", "failed"),
        "cancellation_cleanup_status": command_summaries.get("cancellation_gate", {}).get(
            "status",
            "failed",
        ),
        "memory_runtime_hardening_status": command_summaries.get(
            "memory_runtime_hardening_gate",
            {},
        ).get("status", "failed"),
        "fault_tolerance_gate_status": command_summaries.get(
            "fault_tolerance_promotion_gate",
            {},
        ).get("status", "failed"),
        "public_native_vortex_resource_route_status": command_summaries.get(
            "public_native_vortex_resource_route",
            {},
        ).get("status", "failed"),
        "prepared_state_cleanup_status": source_summary.get("status", "failed"),
        "local_output_cleanup_status": local_output_summary.get("status", "failed"),
        "unsupported_paths_blocked_without_writes": passed,
        "larger_than_memory_claim_allowed": False,
        "native_spill_runtime_claim_allowed": False,
        "distributed_resource_claim_allowed": False,
        "spill_io_performed": False,
        "object_store_io": False,
        "output_dataset_write_by_resource_gate": False,
        "all_no_fallback_no_external_engine": no_fallback_no_external,
        "claim_gate_status": "not_claim_grade",
        "claim_boundary": "local_v1_resource_safety_only_no_larger_than_memory_or_spill_runtime_claim",
        **fail_closed_fields(),
    }


def validate_command_payload(label: str, payload: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    if label == "pre_oom_memory_guard":
        return check_fields(
            label,
            payload,
            "pre-oom-memory-guard-smoke",
            {
                "schema_version": "shardloom.pre_oom_memory_guard_fixture.v1",
                "diagnostic_code": "SL_RESOURCE_BUDGET_EXCEEDED",
                "admission_decision": "denied_before_oom",
                "memory_budget_bytes": "1024",
                "memory_hard_limit_bytes": "768",
                "reserved_after_cleanup_bytes": "0",
            },
            false_fields=(
                "real_query_spill_admitted",
                "distributed_execution_admitted",
                "native_spill_write_performed",
                "native_spill_read_performed",
                "spill_io_performed",
                "object_store_io",
                "write_io",
                "tasks_executed",
                "data_read",
                "data_materialized",
                "fallback_execution_allowed",
                "fallback_attempted",
                "external_engine_invoked",
                "has_unexpected_errors",
            ),
            true_fields=(
                "fail_before_oom",
                "release_performed",
                "cleanup_required",
                "cleanup_completed",
                "runtime_execution",
                "guard_triggered",
            ),
        )
    if label == "retry_gate":
        return check_fields(
            label,
            payload,
            "retry-gate-plan",
            {"mode": "retry_gate_plan", "execution": "not_performed"},
            false_fields=(
                "retry_requires_cleanup",
                "unknown_artifact_present",
                "external_effects_present",
                "object_store_recovery_required",
                "output_recovery_required",
                "cancellation_requested",
                "retry_executed",
                "cleanup_executed_by_gate",
                "cancellation_executed",
                "external_effects_executed",
                "object_store_io",
                "output_dataset_write",
                "fallback_execution_allowed",
            ),
            true_fields=(
                "retry_requested",
                "retry_allowed_by_plan",
                "retry_gate_open",
                "cleanup_completed",
            ),
        )
    if label == "cancellation_gate":
        return check_fields(
            label,
            payload,
            "cancellation-gate-plan",
            {"mode": "cancellation_gate_plan", "execution": "not_performed"},
            false_fields=(
                "unknown_artifact_present",
                "external_effects_present",
                "object_store_recovery_required",
                "output_recovery_required",
                "retry_in_progress",
                "cancellation_executed",
                "retry_executed",
                "cleanup_executed_by_gate",
                "external_effects_executed",
                "object_store_io",
                "output_dataset_write",
                "fallback_execution_allowed",
            ),
            true_fields=(
                "cancellation_requested",
                "cancellation_gate_open",
                "cleanup_required",
                "cleanup_completed",
            ),
        )
    if label == "memory_runtime_hardening_gate":
        return check_fields(
            label,
            payload,
            "cg14-memory-runtime-hardening-gate",
            {
                "schema_version": "shardloom.memory_runtime_hardening_gate.v1",
                "promotion_gate_status": "blocked_until_certified",
                "claim_gate_status": "not_claim_grade",
                "support_status": "report_only",
                "existing_evidence_surface_count": "6",
                "blocked_surface_count": "9",
            },
            false_fields=(
                "resource_derived_chunk_sizing_allowed",
                "adaptive_parallelism_allowed",
                "native_spill_write_allowed",
                "native_spill_read_allowed",
                "spill_cleanup_execution_allowed",
                "large_workload_claim_allowed",
                "runtime_execution",
                "tasks_executed",
                "data_read",
                "data_materialized",
                "object_store_io",
                "spill_io_performed",
                "fallback_execution_allowed",
                "fallback_attempted",
                "external_engine_invoked",
            ),
            true_fields=(
                "existing_pre_oom_memory_guard_fixture_present",
                "runtime_promotions_blocked",
                "claim_blocked",
                "side_effect_free",
            ),
        )
    if label == "fault_tolerance_promotion_gate":
        return check_fields(
            label,
            payload,
            "fault-tolerance-promotion-gate",
            {
                "schema_version": "shardloom.fault_tolerance_promotion_gate.v1",
                "support_status": "report_only",
                "claim_gate_status": "not_claim_grade",
                "blocked_area_count": "6",
                "execution_gate_blocker_count": "11",
            },
            false_fields=(
                "retry_execution_allowed",
                "cancellation_execution_allowed",
                "cleanup_execution_allowed",
                "checkpoint_write_allowed",
                "commit_execution_allowed",
                "request_validation_performed",
                "cancellation_signal_consumed",
                "retry_execution_performed",
                "checkpoint_write_performed",
                "cleanup_execution_performed",
                "commit_execution_performed",
                "runtime_execution",
                "object_store_io",
                "output_dataset_write",
                "external_effects_executed",
                "fallback_execution_allowed",
                "fallback_attempted",
            ),
            true_fields=(
                "execution_promotions_blocked",
                "exactly_once_resumability_recovery_claims_blocked",
                "side_effect_free",
            ),
        )
    if label == "public_native_vortex_resource_route":
        return check_fields(
            label,
            payload,
            "run",
            {
                "public_workflow_route_id": "native_vortex_aggregate",
                "public_workflow_resolved_internal_command": "vortex-run",
                "public_workflow_start_state": "native_vortex_file",
                "public_workflow_vortex_normalization_point": "native_vortex_boundary",
                "public_workflow_memory_gb": "1",
                "public_workflow_max_parallelism": "2",
                "public_workflow_dynamic_parallelism_floor_applied": "false",
                "local_primitive_resource_envelope_schema_version": "shardloom.local_vortex_resource_envelope.v1",
                "local_primitive_resource_memory_gb": "1",
                "local_primitive_resource_max_parallelism": "2",
                "local_primitive_state_budget_schema_version": "shardloom.local_vortex_state_budget.v2",
                "local_primitive_state_budget_status": "bounded_in_memory_low_pressure_spill_not_required",
                "local_primitive_state_family": "scalar_aggregate_state+direct_dictionary_or_typed",
                "local_primitive_budget_scope": "local_vortex_scalar_aggregate",
                "local_primitive_spill_policy": "fail_closed_before_uncertified_spill",
                "local_primitive_state_budget_diagnostic_code": "none",
                "local_primitive_memory_admission_schema_version": "shardloom.memory_admission.v1",
                "local_primitive_memory_admission_scope": "public_local_vortex_primitive_state_budget",
                "local_primitive_memory_reservation_owner_class": "aggregate",
                "local_primitive_memory_reservation_status": "granted",
                "local_primitive_memory_admission_decision": "granted",
                "local_primitive_memory_pressure_before": "normal",
                "local_primitive_memory_pressure_after": "normal",
                "local_primitive_memory_reserved_before_bytes": "0",
                "local_primitive_memory_reserved_after_release_bytes": "0",
                "local_primitive_native_io_certificate_status": "certified",
                "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
            },
            false_fields=(
                "public_workflow_fallback_attempted",
                "public_workflow_external_engine_invoked",
                "public_workflow_native_vortex_plan_fallback_attempted",
                "public_workflow_native_vortex_plan_external_engine_invoked",
                "object_store_io",
                "write_io",
                "spill_io_performed",
                "local_primitive_spill_required",
                "local_primitive_spill_supported",
                "local_primitive_spill_io_performed",
                "local_primitive_memory_fail_before_oom",
                "local_primitive_memory_fallback_attempted",
                "local_primitive_native_io_object_store_io",
                "local_primitive_native_io_write_io",
                "local_primitive_native_io_spill_io_performed",
                "local_primitive_native_io_fallback_attempted",
                "local_primitive_native_io_fallback_execution_allowed",
                "fallback_attempted",
                "external_engine_invoked",
            ),
            true_fields=(
                "public_workflow_route_attached",
                "public_workflow_bounded_request",
                "local_primitive_report_present",
                "local_primitive_state_budget_required",
                "local_primitive_fail_closed_if_spill_required",
                "local_primitive_memory_reservation_required",
                "local_primitive_memory_reservation_release_performed",
                "local_primitive_native_io_certified",
                "local_primitive_native_io_encoded_representation_preserved",
                "local_primitive_native_io_streaming_capability",
                "upstream_vortex_scan_called",
                "data_read",
                "data_decoded",
                "data_materialized",
            ),
        )
    return {"status": "failed", "command": label}, [f"{label}: unknown command spec"]


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = args.repo_root.resolve()
    binary = locate_binary(repo_root, args.binary)
    report = build_report(
        repo_root=repo_root,
        binary=binary,
        explicit_binary=args.binary is not None,
        features=args.features,
        skip_build=args.skip_build,
        source_prepared_report=args.source_prepared_report,
        local_output_report=args.local_output_report,
    )
    output = resolve_path(repo_root, args.output)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
