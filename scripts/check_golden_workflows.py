#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate ShardLoom's local golden runtime workflows.

The validator executes only local ShardLoom CLI/Python paths. It does not publish packages,
create tags, probe networks, invoke external engines, or authorize production/performance claims.
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
SCHEMA_VERSION = "shardloom.golden_workflow_validation_report.v1"
DEFAULT_FEATURES = "vortex-write,vortex-local-primitives"


REQUIRED_SUPPORT_ROWS: dict[str, tuple[str, tuple[str, ...]]] = {
    "cli_sql_local_source_smoke": ("executable", ("sql-local-source-smoke",)),
    "cli_vortex_ingest_smoke": ("feature_gated", ("vortex-ingest-smoke",)),
    "cli_generated_source_smokes": (
        "executable",
        ("generated-source-user-rows-smoke",),
    ),
    "output_inline_jsonl_csv": ("executable", ("inline_jsonl", "csv")),
    "output_vortex_local": ("feature_gated", ("vortex",)),
    "execution_prepared_vortex": ("executable", ("prepared_vortex",)),
    "execution_native_vortex_scoped": ("executable", ("native_vortex",)),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/golden-workflow-report.json"),
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=Path("target/golden-workflows"),
    )
    parser.add_argument(
        "--features",
        default=DEFAULT_FEATURES,
        help="Feature set used to build the CLI binary for the local workflows.",
    )
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--skip-build", action="store_true")
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def rel(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def command_text(command: list[str]) -> str:
    return " ".join(command).replace(str(sys.executable), "python")


def tail(text: str, limit: int = 4000) -> str:
    return text if len(text) <= limit else text[-limit:]


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def bool_field(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
    return None


def collect_field_rows(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows: list[dict[str, Any]] = []
    direct = payload.get("fields")
    if isinstance(direct, list):
        rows.extend(row for row in direct if isinstance(row, dict))
    for key in ("result", "policy", "lifecycle", "capability_snapshot"):
        child = payload.get(key)
        if isinstance(child, dict):
            rows.extend(collect_field_rows(child))
    for artifact in payload.get("artifacts", []) if isinstance(payload.get("artifacts"), list) else []:
        if isinstance(artifact, dict):
            rows.extend(collect_field_rows(artifact.get("payload")))
    return rows


def field_map(payload: dict[str, Any]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for row in collect_field_rows(payload):
        key = row.get("key")
        value = row.get("value")
        if isinstance(key, str):
            fields[key] = "" if value is None else str(value)
    return fields


def no_fallback_blockers(payload: dict[str, Any], label: str) -> list[str]:
    blockers: list[str] = []
    fallback = payload.get("fallback")
    if isinstance(fallback, dict):
        if fallback.get("attempted") is not False:
            blockers.append(f"{label}: envelope fallback.attempted must be false")
        if fallback.get("allowed") is not False:
            blockers.append(f"{label}: envelope fallback.allowed must be false")
    else:
        blockers.append(f"{label}: envelope fallback marker is missing")

    fields = field_map(payload)
    lowered_keys = tuple(key.lower() for key in fields)
    if not any("external_engine_invoked" in key for key in lowered_keys) and not any(
        "external_query_engine_invoked" in key for key in lowered_keys
    ):
        blockers.append(f"{label}: external engine marker is missing")
    for key, value in fields.items():
        lowered = key.lower()
        bool_value = bool_field(value)
        if "fallback_attempted" in lowered and bool_value is not False:
            blockers.append(f"{label}: {key} must be false")
        if "external_engine_invoked" in lowered and bool_value is not False:
            blockers.append(f"{label}: {key} must be false")
        if "external_query_engine_invoked" in lowered and bool_value is not False:
            blockers.append(f"{label}: {key} must be false")
        if lowered.endswith("fallback_execution_allowed") and bool_value is not False:
            blockers.append(f"{label}: {key} must be false")
    return blockers


def expect_status(payload: dict[str, Any], stage_id: str, expected: str = "success") -> list[str]:
    observed = payload.get("status")
    if observed != expected:
        return [f"{stage_id}: status={observed!r}, expected {expected!r}"]
    return []


def expect_fields(
    payload: dict[str, Any],
    stage_id: str,
    expected: dict[str, str],
) -> list[str]:
    fields = field_map(payload)
    blockers: list[str] = []
    for key, value in expected.items():
        observed = fields.get(key)
        if observed != value:
            blockers.append(f"{stage_id}: {key}={observed!r}, expected {value!r}")
    return blockers


def expect_prefix_fields(
    payload: dict[str, Any],
    stage_id: str,
    prefixes: dict[str, str],
) -> list[str]:
    fields = field_map(payload)
    blockers: list[str] = []
    for key, prefix in prefixes.items():
        observed = fields.get(key)
        if observed is None or not observed.startswith(prefix):
            blockers.append(f"{stage_id}: {key} must start with {prefix!r}")
    return blockers


def expect_existing_file(path: Path, stage_id: str) -> list[str]:
    if not path.exists():
        return [f"{stage_id}: missing artifact {path}"]
    if path.is_file() and path.stat().st_size == 0:
        return [f"{stage_id}: empty artifact {path}"]
    return []


def selected_fields(payload: dict[str, Any], keys: tuple[str, ...]) -> dict[str, str]:
    fields = field_map(payload)
    return {key: fields[key] for key in keys if key in fields}


def run_subprocess(
    *,
    repo_root: Path,
    command: list[str],
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=repo_root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def locate_binary(repo_root: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        return resolve(repo_root, explicit).resolve()
    target_root = Path(os.environ.get("CARGO_TARGET_DIR", repo_root / "target"))
    if not target_root.is_absolute():
        target_root = repo_root / target_root
    suffix = ".exe" if os.name == "nt" else ""
    return (target_root / "debug" / f"shardloom{suffix}").resolve()


def build_binary(repo_root: Path, features: str, skip_build: bool, binary: Path) -> dict[str, Any]:
    if skip_build:
        blockers = [] if binary.exists() else [f"binary does not exist: {binary}"]
        return {
            "command": "skipped",
            "status": "passed" if not blockers else "failed",
            "blockers": blockers,
        }
    command = [
        "cargo",
        "build",
        "-q",
        "-p",
        "shardloom-cli",
        "--features",
        features,
    ]
    completed = run_subprocess(repo_root=repo_root, command=command)
    blockers = []
    if completed.returncode != 0:
        blockers.append("feature-gated CLI build failed")
    if not binary.exists():
        blockers.append(f"built binary missing: {binary}")
    return {
        "command": command_text(command),
        "argv": command,
        "returncode": completed.returncode,
        "status": "passed" if not blockers else "failed",
        "stdout_tail": tail(completed.stdout),
        "stderr_tail": tail(completed.stderr),
        "features": features,
        "blockers": blockers,
    }


def run_cli_stage(
    *,
    repo_root: Path,
    binary: Path,
    stage_dir: Path,
    stage_id: str,
    args: list[str],
    expected_fields: dict[str, str],
    prefix_fields: dict[str, str] | None = None,
    artifact_paths: tuple[Path, ...] = (),
    selected_field_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    command = [str(binary), *args, "--format", "json"]
    completed = run_subprocess(repo_root=repo_root, command=command)
    payload: dict[str, Any]
    blockers: list[str] = []
    try:
        payload = json.loads(completed.stdout)
        if not isinstance(payload, dict):
            raise ValueError("envelope is not an object")
    except Exception as exc:  # noqa: BLE001 - surfaced in report.
        payload = {}
        blockers.append(f"{stage_id}: failed to parse JSON output: {exc}")

    artifact_ref = stage_dir / f"{stage_id}.json"
    write_json(
        artifact_ref,
        payload
        if payload
        else {
            "stdout_tail": tail(completed.stdout),
            "stderr_tail": tail(completed.stderr),
        },
    )

    if completed.returncode != 0:
        blockers.append(f"{stage_id}: returncode={completed.returncode}")
    if payload:
        blockers.extend(expect_status(payload, stage_id))
        blockers.extend(expect_fields(payload, stage_id, expected_fields))
        blockers.extend(expect_prefix_fields(payload, stage_id, prefix_fields or {}))
        blockers.extend(no_fallback_blockers(payload, stage_id))
    for artifact_path in artifact_paths:
        blockers.extend(expect_existing_file(artifact_path, stage_id))

    fields = selected_fields(
        payload,
        selected_field_keys
        + tuple(expected_fields.keys())
        + tuple((prefix_fields or {}).keys())
        + (
            "fallback_attempted",
            "external_engine_invoked",
            "claim_gate_status",
        ),
    )
    return {
        "stage_id": stage_id,
        "kind": "cli",
        "command": command_text(command),
        "argv": command,
        "returncode": completed.returncode,
        "envelope_status": payload.get("status") if payload else "unparsed",
        "status": "passed" if not blockers else "failed",
        "artifact_ref": rel(repo_root, artifact_ref),
        "selected_fields": fields,
        "blockers": blockers,
    }


def run_python_wrapper_stage(
    *,
    repo_root: Path,
    binary: Path,
    stage_dir: Path,
    source_path: Path,
    target_path: Path,
) -> dict[str, Any]:
    code = """
import json
import sys
from shardloom import context

repo_root, binary, source_path, target_path = sys.argv[1:5]
ctx = context(repo_root=repo_root, binary=binary, profile_order=("debug",))
report = ctx.prepare_vortex(source_path, target_path, allow_overwrite=True)
print(json.dumps({
    "schema_version": "shardloom.python_golden_workflow_prepare_vortex.v1",
    "source_format": report.source_format,
    "vortex_ingest_status": report.vortex_ingest_status,
    "prepared_state_created": report.prepared_state_created,
    "input_row_count": report.input_row_count,
    "writer_row_count": report.writer_row_count,
    "reopen_row_count": report.reopen_row_count,
    "reopen_verification_status": report.reopen_verification_status,
    "certification_status": report.certification_status,
    "claim_gate_status": report.claim_gate_status,
    "fallback_attempted": report.fallback_attempted,
    "external_engine_invoked": report.external_engine_invoked,
}, sort_keys=True))
"""
    env = os.environ.copy()
    python_src = repo_root / "python" / "src"
    env["PYTHONPATH"] = (
        str(python_src)
        if not env.get("PYTHONPATH")
        else str(python_src) + os.pathsep + env["PYTHONPATH"]
    )
    command = [
        sys.executable,
        "-c",
        code,
        str(repo_root),
        str(binary),
        str(source_path),
        str(target_path),
    ]
    completed = run_subprocess(repo_root=repo_root, command=command, env=env)
    blockers: list[str] = []
    try:
        payload = json.loads(completed.stdout)
        if not isinstance(payload, dict):
            raise ValueError("payload is not an object")
    except Exception as exc:  # noqa: BLE001 - surfaced in report.
        payload = {}
        blockers.append(f"python_prepare_vortex_wrapper: failed to parse JSON output: {exc}")

    artifact_ref = stage_dir / "python_prepare_vortex_wrapper.json"
    write_json(
        artifact_ref,
        payload
        if payload
        else {
            "stdout_tail": tail(completed.stdout),
            "stderr_tail": tail(completed.stderr),
        },
    )

    if completed.returncode != 0:
        blockers.append(f"python_prepare_vortex_wrapper: returncode={completed.returncode}")
    expected = {
        "schema_version": "shardloom.python_golden_workflow_prepare_vortex.v1",
        "source_format": "csv",
        "vortex_ingest_status": "prepared_state_created",
        "prepared_state_created": True,
        "input_row_count": 3,
        "writer_row_count": 3,
        "reopen_row_count": 3,
        "reopen_verification_status": "reopen_row_count_verified",
        "certification_status": "fixture_smoke_certified",
        "claim_gate_status": "fixture_smoke_only",
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    for key, expected_value in expected.items():
        observed = payload.get(key)
        if observed != expected_value:
            blockers.append(
                f"python_prepare_vortex_wrapper: {key}={observed!r}, expected {expected_value!r}"
            )
    blockers.extend(expect_existing_file(target_path, "python_prepare_vortex_wrapper"))
    return {
        "stage_id": "python_prepare_vortex_wrapper",
        "kind": "python_wrapper",
        "command": "python -c <ctx.prepare_vortex smoke>",
        "argv": command_text(command),
        "returncode": completed.returncode,
        "status": "passed" if not blockers else "failed",
        "artifact_ref": rel(repo_root, artifact_ref),
        "selected_fields": payload,
        "blockers": blockers,
    }


def validate_support_matrix(repo_root: Path) -> dict[str, Any]:
    paths = [
        Path("docs/status/runs-today-support-matrix.json"),
        Path("website-src/src/data/runs-today-support-matrix.json"),
        Path("website/assets/data/runs-today-support-matrix.json"),
        Path("website-public/assets/data/runs-today-support-matrix.json"),
    ]
    blockers: list[str] = []
    rows_verified: list[dict[str, Any]] = []
    for relative in paths:
        path = repo_root / relative
        if not path.exists():
            blockers.append(f"missing support matrix: {relative.as_posix()}")
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != "shardloom.runs_today_support_matrix.v1":
            blockers.append(f"{relative.as_posix()}: schema version mismatch")
        rows = {
            row.get("id"): row
            for row in payload.get("rows", [])
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }
        for row_id, (support_state, surfaces) in REQUIRED_SUPPORT_ROWS.items():
            row = rows.get(row_id)
            if row is None:
                blockers.append(f"{relative.as_posix()}: missing row {row_id}")
                continue
            if row.get("support_state") != support_state:
                blockers.append(
                    f"{relative.as_posix()}: {row_id} support_state={row.get('support_state')}"
                )
            row_surfaces = {str(item) for item in row.get("surface", [])}
            missing_surfaces = [surface for surface in surfaces if surface not in row_surfaces]
            if missing_surfaces:
                blockers.append(
                    f"{relative.as_posix()}: {row_id} missing surfaces {missing_surfaces}"
                )
            if row.get("fallback_attempted") is not False:
                blockers.append(f"{relative.as_posix()}: {row_id} fallback_attempted must be false")
            if row.get("external_engine_invoked") is not False:
                blockers.append(
                    f"{relative.as_posix()}: {row_id} external_engine_invoked must be false"
                )
            rows_verified.append(
                {
                    "matrix_ref": relative.as_posix(),
                    "row_id": row_id,
                    "support_state": row.get("support_state") if row else "missing",
                    "surfaces": sorted(row_surfaces) if row else [],
                }
            )
    return {
        "status": "passed" if not blockers else "failed",
        "matrix_refs": [path.as_posix() for path in paths],
        "required_rows": sorted(REQUIRED_SUPPORT_ROWS),
        "rows_verified": rows_verified,
        "blockers": blockers,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def write_workflow_sources(run_dir: Path) -> dict[str, Path]:
    local_source = run_dir / "local-csv-orders.csv"
    local_source.write_text(
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
        encoding="utf-8",
    )
    wrapper_source = run_dir / "python-wrapper-orders.csv"
    wrapper_source.write_text(
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
        encoding="utf-8",
    )
    return {"local_source": local_source, "wrapper_source": wrapper_source}


def workflow_local_csv_to_prepared_and_fanout(
    *,
    repo_root: Path,
    binary: Path,
    run_dir: Path,
    stage_dir: Path,
    local_source: Path,
    wrapper_source: Path,
) -> dict[str, Any]:
    target_vortex = run_dir / "local-csv-orders.vortex"
    wrapper_target_vortex = run_dir / "python-wrapper-orders.vortex"
    jsonl_output = run_dir / "local-query-output.jsonl"
    csv_output = run_dir / "local-query-output.csv"
    statement = (
        f"SELECT id,label,amount FROM '{local_source}' "
        "WHERE amount >= 10 LIMIT 2"
    )
    stages = [
        run_cli_stage(
            repo_root=repo_root,
            binary=binary,
            stage_dir=stage_dir,
            stage_id="local_csv_vortex_ingest",
            args=[
                "vortex-ingest-smoke",
                str(local_source),
                str(target_vortex),
                "--allow-overwrite",
            ],
            expected_fields={
                "schema_version": "shardloom.vortex_ingest_smoke.v1",
                "execution_mode": "prepared_vortex",
                "runtime_execution": "true",
                "source_io_performed": "true",
                "source_format": "csv",
                "source_adapter_id": "local_csv_input_adapter",
                "ingress_route": "vortex_ingest",
                "vortex_ingest_status": "prepared_state_created",
                "vortex_scout_ingress_status": "admitted_scout_ingress_clean",
                "vortex_scout_ingress_anomaly_count": "0",
                "vortex_scout_ingress_quarantine_required": "false",
                "vortex_scout_ingress_no_standalone_lane_status": (
                    "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
                ),
                "vortex_layout_write_advisor_status": (
                    "admitted_local_layout_write_strategy"
                ),
                "vortex_layout_write_advisor_strategy_admitted": "true",
                "vortex_layout_write_advisor_no_standalone_lane_status": (
                    "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
                ),
                "prepared_state_created": "true",
                "input_row_count": "3",
                "writer_row_count": "3",
                "reopen_row_count": "3",
                "reopen_verification_status": "reopen_row_count_verified",
                "upstream_vortex_write_called": "true",
                "upstream_vortex_scan_called": "true",
                "vortex_preparation_spine_status": "admitted_local_preparation_spine",
                "vortex_preparation_spine_vortex_first_decision": (
                    "implement_shardloom_kernel"
                ),
                "vortex_preparation_spine_source_split_count": "1",
                "vortex_preparation_spine_no_standalone_lane_status": (
                    "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
                ),
                "vortex_capillary_preparation_status": (
                    "applied_capillary_pulseweave_control"
                ),
                "vortex_capillary_preparation_task_count": "6",
                "vortex_capillary_preparation_native_io_certificate_status": "certified",
                "vortex_capillary_preparation_pulseweave_status": "applied",
                "vortex_capillary_preparation_pulseweave_runtime_decision_applied": "true",
                "vortex_capillary_preparation_no_standalone_lane_status": (
                    "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
                ),
                "vortex_copy_budget_status": (
                    "reported_copy_budget_with_unmeasured_segments"
                ),
                "vortex_copy_budget_buffer_reuse_status": (
                    "blocked_until_correctness_parity"
                ),
                "vortex_copy_budget_unsafe_lifetime_shortcut_status": (
                    "blocked_no_unsafe_lifetime_shortcuts"
                ),
                "vortex_copy_budget_no_standalone_lane_status": (
                    "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
                ),
                "claim_gate_status": "fixture_smoke_only",
            },
            prefix_fields={
                "prepared_state_digest": "fnv64:",
                "vortex_artifact_digest": "fnv64:",
                "vortex_scout_ingress_source_state_id": "local-csv-",
                "vortex_scout_ingress_metadata_range_refs": "local-csv-",
                "vortex_layout_write_advisor_source_state_id": "local-csv-",
                "vortex_preparation_spine_source_split_refs": "local-csv-",
                "vortex_capillary_preparation_pulseweave_decision_digest": "fnv1a64:",
                "vortex_copy_budget_source_state_id": "local-csv-",
                "vortex_copy_budget_prepared_state_id": "vortex-prepared-state-",
            },
            artifact_paths=(target_vortex,),
            selected_field_keys=(
                "prepared_state_id",
                "target_vortex_path",
                "certification_status",
                "vortex_scout_ingress_status",
                "vortex_layout_write_advisor_status",
                "vortex_preparation_spine_native_io_certificate_status",
                "vortex_copy_budget_status",
            ),
        ),
        run_cli_stage(
            repo_root=repo_root,
            binary=binary,
            stage_dir=stage_dir,
            stage_id="local_prepared_vortex_filter_project",
            args=[
                "vortex-filter-project",
                str(target_vortex),
                "gte:amount:10",
                "label",
                "--execute-local-primitive",
                "1",
                "2",
            ],
            expected_fields={
                "mode": "vortex_filter_project",
                "filter_project_local_execution_status": "executed",
                "filter_project_local_execution_mode": "vortex_scan_pushdown",
                "filter_project_local_execution_rows_selected": "2",
                "filter_project_local_execution_rows_projected": "2",
                "filter_project_local_execution_native_io_certified": "true",
                "filter_project_local_execution_fallback_attempted": "false",
                "filter_project_local_execution_external_effects_executed": "false",
                "scan_pushdown_status": "scan_pushdown_supported",
                "local_primitive_native_io_certificate_status": "certified",
            },
            selected_field_keys=(
                "local_primitive_execution_certificate_status",
                "local_primitive_native_io_certificate_id",
            ),
        ),
        run_cli_stage(
            repo_root=repo_root,
            binary=binary,
            stage_dir=stage_dir,
            stage_id="local_sql_jsonl_csv_fanout",
            args=[
                "sql-local-source-smoke",
                statement,
                "--fanout-output",
                f"jsonl={jsonl_output}",
                "--fanout-output",
                f"csv={csv_output}",
            ],
            expected_fields={
                "schema_version": "shardloom.sql_local_source_smoke.v1",
                "execution_mode": "direct_compatibility_transient",
                "runtime_execution": "true",
                "source_io_performed": "true",
                "source_format": "csv",
                "input_row_count": "3",
                "selected_row_count": "2",
                "output_route": "local_fanout",
                "fanout_output_count": "2",
                "fanout_output_formats": "jsonl,csv",
                "output_io_performed": "true",
                "write_io": "true",
                "output_native_io_certificate_status": "certified_local_fanout_sinks",
                "result_replay_verified": "true",
                "output_replay_status": "verified_local_sink_artifacts",
                "claim_gate_status": "fixture_smoke_only",
            },
            prefix_fields={
                "output_digest": "fnv64:",
                "correctness_digest": "fnv64:",
            },
            artifact_paths=(jsonl_output, csv_output),
            selected_field_keys=(
                "output_certificate_ref",
                "fanout_output_digests",
                "output_workspace_path_safety_status",
            ),
        ),
        run_python_wrapper_stage(
            repo_root=repo_root,
            binary=binary,
            stage_dir=stage_dir,
            source_path=wrapper_source,
            target_path=wrapper_target_vortex,
        ),
    ]
    blockers = [blocker for stage in stages for blocker in stage["blockers"]]
    return {
        "workflow_id": "local_csv_jsonl_to_vortex_ingest_prepared_query_jsonl_csv_output",
        "status": "passed" if not blockers else "failed",
        "source_route": "local_csv_input_adapter",
        "preparation_route": "vortex_ingest",
        "execution_route": "prepared_vortex_filter_project_and_direct_local_sql_output",
        "output_route": "local_fanout_jsonl_csv",
        "row_counts": {
            "source_rows": 3,
            "prepared_vortex_rows": 3,
            "prepared_query_rows_selected": 2,
            "output_rows": 2,
        },
        "artifact_refs": [
            rel(repo_root, target_vortex),
            rel(repo_root, wrapper_target_vortex),
            rel(repo_root, jsonl_output),
            rel(repo_root, csv_output),
        ],
        "claim_boundary": (
            "local runtime workflow proof; prepared primitive over ad hoc ingested artifact has "
            "Native I/O evidence but no fixture execution certificate"
        ),
        "stages": stages,
        "blockers": blockers,
    }


def workflow_generated_source_to_vortex(
    *,
    repo_root: Path,
    binary: Path,
    run_dir: Path,
    stage_dir: Path,
) -> dict[str, Any]:
    target_vortex = run_dir / "generated-source-output.vortex"
    stages = [
        run_cli_stage(
            repo_root=repo_root,
            binary=binary,
            stage_dir=stage_dir,
            stage_id="generated_source_vortex_output",
            args=[
                "generated-source-user-rows-smoke",
                str(target_vortex),
                "id:int64,label:utf8,score:float64",
                "id=1,label=alpha,score=1.5;id=2,label=beta,score=2.25;id=3,label=gamma,score=4.5",
                "--output-format",
                "vortex",
                "--allow-overwrite",
            ],
            expected_fields={
                "schema_version": "shardloom.generated_source_user_rows_smoke.v1",
                "execution_mode": "source_free_generated_output",
                "runtime_execution": "true",
                "generated_source_created": "true",
                "generated_source_kind": "user_rows",
                "generated_source_row_count": "3",
                "generated_source_certificate_status": "present",
                "output_format": "vortex",
                "output_io_performed": "true",
                "write_io": "true",
                "output_native_io_certificate_status": "certified_local_vortex_sink",
                "vortex_output_runtime_execution": "true",
                "vortex_output_reopen_verified": "true",
                "vortex_output_row_count": "3",
                "vortex_output_column_count": "3",
                "output_commit_status": "committed",
                "claim_gate_status": "fixture_smoke_only",
            },
            prefix_fields={
                "generated_source_schema_digest": "fnv64:",
                "generated_source_plan_digest": "fnv64:",
                "output_digest": "fnv64:",
                "vortex_artifact_digest": "fnv64:",
                "correctness_digest": "fnv64:",
            },
            artifact_paths=(target_vortex,),
            selected_field_keys=(
                "output_certificate_ref",
                "output_fidelity_report_status",
                "output_replay_status",
                "upstream_vortex_write_called",
                "upstream_vortex_scan_called",
            ),
        ),
        run_cli_stage(
            repo_root=repo_root,
            binary=binary,
            stage_dir=stage_dir,
            stage_id="generated_vortex_replay_filter_project",
            args=[
                "vortex-filter-project",
                str(target_vortex),
                "gte:id:2",
                "label",
                "--execute-local-primitive",
                "1",
                "2",
            ],
            expected_fields={
                "mode": "vortex_filter_project",
                "filter_project_local_execution_status": "executed",
                "filter_project_local_execution_rows_selected": "2",
                "filter_project_local_execution_rows_projected": "2",
                "filter_project_local_execution_native_io_certified": "true",
                "scan_pushdown_status": "scan_pushdown_supported",
                "local_primitive_native_io_certificate_status": "certified",
            },
            selected_field_keys=(
                "filter_project_local_execution_projected_columns",
                "local_primitive_execution_certificate_status",
                "local_primitive_native_io_certificate_id",
            ),
        ),
    ]
    blockers = [blocker for stage in stages for blocker in stage["blockers"]]
    return {
        "workflow_id": "generated_source_to_local_vortex_output_replay_fidelity",
        "status": "passed" if not blockers else "failed",
        "source_route": "source_free_generated_user_rows",
        "preparation_route": "generated_rows_to_vortex_sink",
        "execution_route": "vortex_reopen_and_local_filter_project_replay",
        "output_route": "local_vortex_output",
        "row_counts": {
            "generated_rows": 3,
            "vortex_reopen_rows": 3,
            "replay_rows_selected": 2,
        },
        "artifact_refs": [rel(repo_root, target_vortex)],
        "claim_boundary": (
            "source-free local Vortex output and replay proof only; not broad generated SQL, "
            "object-store, table, or production sink support"
        ),
        "stages": stages,
        "blockers": blockers,
    }


def workflow_certified_native_primitives(
    *,
    repo_root: Path,
    binary: Path,
    stage_dir: Path,
) -> dict[str, Any]:
    fixture = repo_root / "shardloom-vortex" / "tests" / "fixtures" / "local_primitive_struct_five.vortex"
    stages = [
        run_cli_stage(
            repo_root=repo_root,
            binary=binary,
            stage_dir=stage_dir,
            stage_id="fixture_vortex_count_where_certificate",
            args=[
                "vortex-count-where",
                str(fixture),
                "gte:value:3",
                "--execute-local-primitive",
                "1",
                "2",
            ],
            expected_fields={
                "mode": "vortex_count_where",
                "count": "3",
                "filtered_count_local_execution_status": "executed",
                "filtered_count_local_execution_rows_selected": "3",
                "filtered_count_local_execution_count": "3",
                "filtered_count_local_execution_correctness_certified": "true",
                "filtered_count_local_execution_native_io_certified": "true",
                "local_primitive_execution_certificate_status": "certified",
                "local_primitive_execution_certificate_correctness_passed": "true",
                "local_primitive_execution_certificate_external_query_engine_invoked": "false",
                "local_primitive_execution_certificate_fallback_attempted": "false",
                "local_primitive_native_io_certificate_status": "certified",
                "filtered_count_local_execution_filter_pushdown_applied": "true",
            },
            selected_field_keys=(
                "local_primitive_execution_certificate_id",
                "local_primitive_native_io_certificate_id",
                "local_primitive_execution_certificate_provider_kind",
                "local_primitive_execution_certificate_provider_api_surface",
            ),
        ),
        run_cli_stage(
            repo_root=repo_root,
            binary=binary,
            stage_dir=stage_dir,
            stage_id="fixture_vortex_project_certificate",
            args=[
                "vortex-project",
                str(fixture),
                "metric",
                "--execute-local-primitive",
                "1",
                "2",
            ],
            expected_fields={
                "mode": "vortex_project",
                "project_local_execution_status": "executed",
                "project_local_execution_rows_projected": "5",
                "project_local_execution_correctness_certified": "true",
                "project_local_execution_native_io_certified": "true",
                "local_primitive_execution_certificate_status": "certified",
                "local_primitive_execution_certificate_correctness_passed": "true",
                "local_primitive_execution_certificate_external_query_engine_invoked": "false",
                "local_primitive_execution_certificate_fallback_attempted": "false",
                "local_primitive_native_io_certificate_status": "certified",
                "scan_projection_pushed_down": "true",
            },
            selected_field_keys=(
                "local_primitive_execution_certificate_id",
                "local_primitive_native_io_certificate_id",
                "local_primitive_execution_certificate_provider_kind",
                "local_primitive_execution_certificate_provider_api_surface",
            ),
        ),
        run_cli_stage(
            repo_root=repo_root,
            binary=binary,
            stage_dir=stage_dir,
            stage_id="fixture_vortex_filter_project_certificate",
            args=[
                "vortex-filter-project",
                str(fixture),
                "gte:value:3",
                "metric",
                "--execute-local-primitive",
                "1",
                "2",
            ],
            expected_fields={
                "mode": "vortex_filter_project",
                "filter_project_local_execution_status": "executed",
                "filter_project_local_execution_rows_selected": "3",
                "filter_project_local_execution_rows_projected": "3",
                "filter_project_local_execution_correctness_certified": "true",
                "filter_project_local_execution_native_io_certified": "true",
                "local_primitive_execution_certificate_status": "certified",
                "local_primitive_execution_certificate_correctness_passed": "true",
                "local_primitive_execution_certificate_external_query_engine_invoked": "false",
                "local_primitive_execution_certificate_fallback_attempted": "false",
                "local_primitive_native_io_certificate_status": "certified",
                "scan_filter_pushed_down": "true",
                "scan_projection_pushed_down": "true",
            },
            selected_field_keys=(
                "local_primitive_execution_certificate_id",
                "local_primitive_native_io_certificate_id",
                "local_primitive_execution_certificate_provider_kind",
                "local_primitive_execution_certificate_provider_api_surface",
            ),
        ),
    ]
    blockers = [blocker for stage in stages for blocker in stage["blockers"]]
    return {
        "workflow_id": "prepared_native_vortex_count_filter_project_execution_certificates",
        "status": "passed" if not blockers else "failed",
        "source_route": "local_vortex_fixture",
        "preparation_route": "checked_in_prepared_vortex_fixture",
        "execution_route": "vortex_local_primitives_count_project_filter_project",
        "output_route": "inline_typed_envelope_certificates",
        "row_counts": {
            "fixture_rows": 5,
            "count_where_rows": 3,
            "project_rows": 5,
            "filter_project_rows": 3,
        },
        "artifact_refs": [rel(repo_root, fixture)],
        "claim_boundary": (
            "scoped fixture-certified native Vortex primitives only; not arbitrary operator "
            "completeness or production Vortex runtime support"
        ),
        "stages": stages,
        "blockers": blockers,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    work_dir = resolve(repo_root, args.work_dir)
    run_id = time.strftime("%Y%m%d-%H%M%S") + f"-{os.getpid()}"
    run_dir = work_dir / "runs" / run_id
    stage_dir = run_dir / "command-envelopes"
    run_dir.mkdir(parents=True, exist_ok=True)
    stage_dir.mkdir(parents=True, exist_ok=True)

    binary = locate_binary(repo_root, args.binary)
    build = build_binary(repo_root, args.features, args.skip_build, binary)
    support_matrix = validate_support_matrix(repo_root)
    sources = write_workflow_sources(run_dir)

    workflows: list[dict[str, Any]] = []
    if build["status"] == "passed":
        workflows = [
            workflow_local_csv_to_prepared_and_fanout(
                repo_root=repo_root,
                binary=binary,
                run_dir=run_dir,
                stage_dir=stage_dir,
                local_source=sources["local_source"],
                wrapper_source=sources["wrapper_source"],
            ),
            workflow_generated_source_to_vortex(
                repo_root=repo_root,
                binary=binary,
                run_dir=run_dir,
                stage_dir=stage_dir,
            ),
            workflow_certified_native_primitives(
                repo_root=repo_root,
                binary=binary,
                stage_dir=stage_dir,
            ),
        ]

    workflow_blockers = [
        f"{workflow['workflow_id']}: {blocker}"
        for workflow in workflows
        for blocker in workflow["blockers"]
    ]
    build_blockers = [f"build: {blocker}" for blocker in build["blockers"]]
    support_blockers = [
        f"support_matrix: {blocker}" for blocker in support_matrix["blockers"]
    ]
    blockers = build_blockers + support_blockers + workflow_blockers
    fallback_detected = any("fallback" in blocker and "must be false" in blocker for blocker in blockers)
    external_engine_detected = any(
        "external_engine" in blocker or "external_query_engine" in blocker for blocker in blockers
    )
    stage_count = sum(len(workflow["stages"]) for workflow in workflows)
    passed = not blockers and len(workflows) == 3 and stage_count >= 9

    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "golden_workflow_validator_status": "passed" if passed else "failed",
        "run_id": run_id,
        "run_dir": rel(repo_root, run_dir),
        "binary_ref": rel(repo_root, binary),
        "feature_set": args.features,
        "build": build,
        "support_matrix_status": support_matrix["status"],
        "support_matrix": support_matrix,
        "workflow_count": len(workflows),
        "stage_count": stage_count,
        "workflow_ids": [workflow["workflow_id"] for workflow in workflows],
        "workflows": workflows,
        "blockers": blockers,
        "runtime_support_claim": "local_runtime_workflow_proof_only",
        "claim_gate_status": "fixture_smoke_only",
        "production_claim_allowed": False,
        "performance_claim_allowed": False,
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
        "package_publication_performed": False,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "object_store_lakehouse_claim_allowed": False,
        "foundry_platform_claim_allowed": False,
        "fallback_attempted": fallback_detected,
        "external_engine_invoked": external_engine_detected,
        "unsupported_boundaries": [
            "no production workflow claim",
            "no object-store/lakehouse/Foundry production support",
            "no package publication",
            "no distributed runtime claim",
            "no performance superiority claim",
            "ad hoc ingested/generated Vortex replay has Native I/O evidence but fixture execution certificates remain limited to checked-in primitive fixtures",
        ],
    }
    write_json(output, report)
    print(output)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
