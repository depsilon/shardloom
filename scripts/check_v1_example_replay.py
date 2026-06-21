#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run and validate v1 docs/example replay evidence.

This gate is intentionally local and bounded. It executes the source-checkout
Python examples sequentially, validates golden-workflow replay/certificate
markers, and checks that README/website snippet anchors still describe the same
primary ShardLoom route. It does not publish packages, create tags, call
external query engines, or authorize production/performance claims.
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
SCHEMA_VERSION = "shardloom.v1_example_replay_report.v1"
DEFAULT_FEATURES = RELEASE_USER_SURFACE_EXAMPLE_FEATURES

EXPECTED_GOLDEN_WORKFLOWS = {
    "local_csv_jsonl_to_vortex_ingest_prepared_query_jsonl_csv_output",
    "generated_source_to_local_vortex_output_replay_fidelity",
    "prepared_native_vortex_count_filter_project_execution_certificates",
}
EXPECTED_SCENARIOS = {
    "selective_filter",
    "filter_projection_limit",
    "group_by_aggregation",
    "hash_join",
    "global_top_n",
    "clean_cast_filter_write",
    "malformed_timestamp_cast",
    "null_heavy_aggregate",
    "nested_json_field_scan",
}
EXPECTED_ERROR_SCENARIOS: set[str] = set()
EXPECTED_RUNTIME_COMMANDS = 3
EXPECTED_UNSUPPORTED_FAILURE_FIXTURES = 1

DOC_MARKERS: dict[str, tuple[str, ...]] = {
    "README.md": (
        "python examples/local-python-smoke/run.py --repo-root .",
        "python examples/local-python-benchmark-scenarios/run.py --repo-root .",
        "python examples/local-python-benchmark-scenarios/timing_review.py --repo-root .",
        "import shardloom as sl",
        "ctx = sl.context()",
        'ctx.read("orders.csv")',
        "prepared = ctx.prepare_vortex(",
        "clean/cast/filter/write",
        "scenario_selective-filter_fallback_attempted",
    ),
    "python/README.md": (
        "from shardloom import context",
        "import shardloom as sl",
        'ctx.read("target/orders.csv")',
        "print(result.prepared_vortex_path)",
        "print(result.vortex_ingest_performed)",
        ".filter(sl.col(\"amount\") >= 10)",
        ".collect()",
        "fallback_attempted",
        "external_engine_invoked",
    ),
    "docs/getting-started/examples.md": (
        "from shardloom import context",
        "import shardloom as sl",
        'ctx.read_csv("target/local-source-runtime.csv")',
        "fallback_attempted",
        "external_engine_invoked",
    ),
    "website-src/src/pages/start.astro": (
        "python examples\\local-python-smoke\\run.py --repo-root .",
        "import shardloom as sl",
        'ctx.read("data/orders.csv")',
        "print(result.fallback_attempted, result.external_engine_invoked)",
    ),
    "website-src/src/pages/benchmarks.astro": (
        "https://benchmark.clickhouse.com/",
        "Use ClickBench as the public comparison surface.",
        "old internal",
        "benchmark dashboard has been removed",
        "not present them as current public ranking evidence",
        "External engines are baselines only.",
        "no-fallback execution boundary",
    ),
    "website-src/src/content/docs/field-guide/python-surface.mdx": (
        "import shardloom as sl",
        "prepared = ctx.prepare_vortex(",
        "clean/cast/filter/write",
        "fallback execution",
    ),
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-example-replay-report.json"),
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=Path("target/v1-example-replay"),
    )
    parser.add_argument(
        "--golden-workflow-report",
        type=Path,
        default=Path("target/golden-workflow-report.json"),
    )
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--features", default=DEFAULT_FEATURES)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument(
        "--profile-order",
        default="debug,release",
        help="Comma-separated source-checkout build profiles for example contexts.",
    )
    return parser.parse_args(argv)


def split_profile_order(value: str) -> tuple[str, ...]:
    profiles = tuple(part.strip() for part in value.split(",") if part.strip())
    if not profiles:
        raise ValueError("--profile-order must include at least one profile")
    return profiles


def command_text(command: Sequence[str]) -> str:
    return " ".join(command).replace(str(sys.executable), "python")


def tail(text: str, limit: int = 4000) -> str:
    return text if len(text) <= limit else text[-limit:]


def rel(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def bool_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def run_command(repo_root: Path, command: list[str]) -> dict[str, Any]:
    started = time.perf_counter()
    env = os.environ.copy()
    python_path = str(repo_root / "python" / "src")
    env["PYTHONPATH"] = (
        python_path
        if not env.get("PYTHONPATH")
        else python_path + os.pathsep + env["PYTHONPATH"]
    )
    completed = subprocess.run(
        command,
        cwd=repo_root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    elapsed = (time.perf_counter() - started) * 1000.0
    return {
        "command": command_text(command),
        "argv": command,
        "returncode": completed.returncode,
        "status": "passed" if completed.returncode == 0 else "failed",
        "elapsed_millis": round(elapsed, 4),
        "stdout_tail": tail(completed.stdout),
        "stderr_tail": tail(completed.stderr),
    }


def locate_binary(repo_root: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        return resolve_path(repo_root, explicit).resolve()
    target_root = Path(os.environ.get("CARGO_TARGET_DIR", repo_root / "target"))
    if not target_root.is_absolute():
        target_root = repo_root / target_root
    suffix = ".exe" if os.name == "nt" else ""
    return (target_root / "debug" / f"shardloom{suffix}").resolve()


def ensure_binary(
    repo_root: Path,
    *,
    binary: Path,
    features: str,
    skip_build: bool,
    explicit_binary: bool,
) -> tuple[dict[str, Any], list[str]]:
    if binary.exists() and (skip_build or explicit_binary):
        return {
            "command": "reused explicit/existing shardloom binary",
            "status": "passed",
            "binary_ref": rel(repo_root, binary),
            "blockers": [],
        }, []
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
    result = run_command(repo_root, command)
    blockers: list[str] = []
    if result["returncode"] != 0:
        blockers.append("failed to build shardloom CLI for example replay")
    if not binary.exists():
        blockers.append(f"built binary missing: {rel(repo_root, binary)}")
    result.update(
        {
            "status": "passed" if not blockers else "failed",
            "binary_ref": rel(repo_root, binary),
            "blockers": blockers,
        }
    )
    return result, blockers


def validate_doc_markers(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    rows: list[dict[str, Any]] = []
    marker_count = 0
    passed_count = 0
    for source, markers in DOC_MARKERS.items():
        text = read_text(resolve_path(repo_root, source), missing_ok=True)
        missing = [marker for marker in markers if marker not in text]
        marker_count += len(markers)
        passed_count += len(markers) - len(missing)
        blockers.extend(f"{source}: missing marker {marker!r}" for marker in missing)
        rows.append(
            {
                "source": source,
                "marker_count": len(markers),
                "passed_marker_count": len(markers) - len(missing),
                "missing_markers": missing,
                "status": "passed" if not missing else "failed",
            }
        )
    return {
        "status": "passed" if not blockers else "failed",
        "source_count": len(DOC_MARKERS),
        "marker_count": marker_count,
        "passed_marker_count": passed_count,
        "rows": rows,
    }, blockers


def selected_fields(stage: Mapping[str, Any]) -> dict[str, Any]:
    fields = stage.get("selected_fields")
    return fields if isinstance(fields, dict) else {}


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


def artifact_fields(repo_root: Path, stage: Mapping[str, Any]) -> dict[str, Any]:
    artifact_ref = stage.get("artifact_ref")
    if not isinstance(artifact_ref, str) or not artifact_ref:
        return {}
    artifact_path = resolve_path(repo_root, artifact_ref)
    if not artifact_path.exists():
        return {}
    payload = load_json(artifact_path)
    return {
        str(row.get("key")): row.get("value")
        for row in collect_field_rows(payload)
        if isinstance(row.get("key"), str)
    }


def stage_has_false_execution_markers(
    stage: Mapping[str, Any],
    envelope_fields: Mapping[str, Any],
) -> bool:
    fields = {**dict(envelope_fields), **selected_fields(stage)}
    observed = []
    for key, value in fields.items():
        lowered = key.lower()
        if "fallback_attempted" in lowered or "external_engine_invoked" in lowered:
            observed.append(bool_value(value) is False)
        if "external_query_engine_invoked" in lowered:
            observed.append(bool_value(value) is False)
    return bool(observed) and all(observed)


def workflow_replay_verified(workflow: Mapping[str, Any]) -> bool:
    stages = workflow.get("stages", [])
    if not isinstance(stages, list):
        return False
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        fields = selected_fields(stage)
        if bool_value(fields.get("result_replay_verified")) is True:
            return True
        if fields.get("output_replay_status") == "verified_local_sink_artifacts":
            return True
        if fields.get("reopen_verification_status") in {
            "reopen_row_count_verified",
            "reopen_metadata_row_count_verified",
        }:
            return True
        if bool_value(fields.get("vortex_output_reopen_verified")) is True:
            return True
        if fields.get("local_primitive_execution_certificate_status") == "certified":
            return True
    return False


def validate_golden_report(repo_root: Path, path: Path) -> tuple[dict[str, Any], list[str]]:
    resolved = resolve_path(repo_root, path)
    blockers: list[str] = []
    if not resolved.exists():
        return {
            "status": "failed",
            "report_ref": str(path).replace("\\", "/"),
            "workflow_count": 0,
            "stage_count": 0,
            "replay_verified_workflow_count": 0,
            "stage_no_fallback_count": 0,
        }, [f"missing golden workflow report: {path}"]
    payload = load_json(resolved)
    if not isinstance(payload, dict):
        return {
            "status": "failed",
            "report_ref": str(path).replace("\\", "/"),
            "workflow_count": 0,
            "stage_count": 0,
            "replay_verified_workflow_count": 0,
            "stage_no_fallback_count": 0,
        }, [f"{path}: report is not an object"]

    if payload.get("schema_version") != "shardloom.golden_workflow_validation_report.v1":
        blockers.append("golden_workflow: invalid schema_version")
    if payload.get("status") != "passed":
        blockers.extend(payload.get("blockers", ["golden_workflow: status is not passed"]))
    if payload.get("fallback_attempted") is not False:
        blockers.append("golden_workflow: fallback_attempted must be false")
    if payload.get("external_engine_invoked") is not False:
        blockers.append("golden_workflow: external_engine_invoked must be false")
    workflow_ids = {str(value) for value in payload.get("workflow_ids", [])}
    if workflow_ids != EXPECTED_GOLDEN_WORKFLOWS:
        blockers.append(
            "golden_workflow: workflow_ids mismatch "
            + f"missing={sorted(EXPECTED_GOLDEN_WORKFLOWS - workflow_ids)} "
            + f"extra={sorted(workflow_ids - EXPECTED_GOLDEN_WORKFLOWS)}"
        )
    workflows = payload.get("workflows", [])
    if not isinstance(workflows, list):
        blockers.append("golden_workflow: workflows must be a list")
        workflows = []

    stage_count = 0
    stage_no_fallback_count = 0
    replay_verified_workflow_ids: set[str] = set()
    artifact_ref_count = 0
    existing_artifact_ref_count = 0
    for workflow in workflows:
        if not isinstance(workflow, dict):
            blockers.append("golden_workflow: workflow entry must be an object")
            continue
        workflow_id = str(workflow.get("workflow_id", "missing"))
        if workflow.get("status") != "passed":
            blockers.append(f"golden_workflow: {workflow_id} status must be passed")
        if workflow_replay_verified(workflow):
            replay_verified_workflow_ids.add(workflow_id)
        stages = workflow.get("stages", [])
        if not isinstance(stages, list):
            blockers.append(f"golden_workflow: {workflow_id} stages must be a list")
            continue
        for stage in stages:
            if not isinstance(stage, dict):
                blockers.append(f"golden_workflow: {workflow_id} stage must be an object")
                continue
            stage_count += 1
            stage_id = str(stage.get("stage_id", "missing"))
            if stage.get("status") != "passed":
                blockers.append(f"golden_workflow: {stage_id} status must be passed")
            if stage_has_false_execution_markers(stage, artifact_fields(repo_root, stage)):
                stage_no_fallback_count += 1
            else:
                blockers.append(
                    f"golden_workflow: {stage_id} missing false fallback/external markers"
                )
            artifact_ref = stage.get("artifact_ref")
            if isinstance(artifact_ref, str) and artifact_ref:
                artifact_ref_count += 1
                if resolve_path(repo_root, artifact_ref).exists():
                    existing_artifact_ref_count += 1
                else:
                    blockers.append(f"golden_workflow: missing stage artifact {artifact_ref}")
    missing_replay = EXPECTED_GOLDEN_WORKFLOWS - replay_verified_workflow_ids
    if missing_replay:
        blockers.append(
            "golden_workflow: missing replay/certificate markers for "
            + ",".join(sorted(missing_replay))
        )
    return {
        "status": "passed" if not blockers else "failed",
        "report_ref": str(path).replace("\\", "/"),
        "workflow_count": len(workflows),
        "stage_count": stage_count,
        "replay_verified_workflow_count": len(replay_verified_workflow_ids),
        "replay_verified_workflow_ids": sorted(replay_verified_workflow_ids),
        "stage_no_fallback_count": stage_no_fallback_count,
        "artifact_ref_count": artifact_ref_count,
        "existing_artifact_ref_count": existing_artifact_ref_count,
    }, blockers


def validate_quickstart(result: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    stdout = str(result.get("stdout_tail", ""))
    blockers: list[str] = []
    for marker in [
        "quickstart_user_surface_status=passed",
        "quickstart_local_file_blocker_id=none",
        "quickstart_local_file_route_status=passed",
        "quickstart_local_file_runtime_execution=true",
        "quickstart_local_file_vortex_ingest_performed=true",
        "quickstart_local_file_fallback_attempted=false",
        "quickstart_local_file_external_engine_invoked=false",
        "quickstart_generated_output_row_count=",
        "quickstart_generated_evidence_fallback_attempted=false",
        "quickstart_generated_evidence_external_engine_invoked=false",
        "quickstart_unsupported_runtime_execution=false",
        "quickstart_unsupported_data_read=false",
        "quickstart_unsupported_write_io=false",
        "quickstart_unsupported_fallback_attempted=false",
        "quickstart_unsupported_external_engine_invoked=false",
    ]:
        if marker not in stdout:
            blockers.append(f"quickstart: missing stdout marker {marker}")
    if "quickstart_unsupported_blocker_id=None" in stdout:
        blockers.append("quickstart: unsupported blocker id must be populated")
    if result.get("returncode") != 0:
        blockers.append("quickstart: command failed")
    return {
        "status": "passed" if not blockers else "failed",
        "local_file_vortex_collect_present": (
            "quickstart_local_file_route_status=passed" in stdout
            and "quickstart_local_file_vortex_ingest_performed=true" in stdout
        ),
        "unsupported_fixture_present": (
            "quickstart_unsupported_blocker_id=" in stdout
            and "quickstart_unsupported_blocker_id=None" not in stdout
        ),
    }, blockers


def validate_scenario_payload(
    repo_root: Path,
    payload_path: Path,
    *,
    label: str,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    resolved = resolve_path(repo_root, payload_path)
    if not resolved.exists():
        return {
            "status": "failed",
            "summary_ref": rel(repo_root, resolved),
            "scenario_count": 0,
            "expected_error_scenario_count": 0,
            "no_fallback_scenario_count": 0,
        }, [f"{label}: missing summary JSON {payload_path}"]
    payload = load_json(resolved)
    if not isinstance(payload, dict):
        return {
            "status": "failed",
            "summary_ref": rel(repo_root, resolved),
            "scenario_count": 0,
            "expected_error_scenario_count": 0,
            "no_fallback_scenario_count": 0,
        }, [f"{label}: summary JSON is not an object"]
    if payload.get("schema_version") != "shardloom.local_python_benchmark_scenarios.v1":
        blockers.append(f"{label}: invalid schema_version")
    if payload.get("passed") is not True:
        blockers.append(f"{label}: payload passed must be true")
    results = payload.get("results", [])
    if not isinstance(results, list):
        blockers.append(f"{label}: results must be a list")
        results = []
    observed = {str(row.get("name")) for row in results if isinstance(row, dict)}
    if observed != EXPECTED_SCENARIOS:
        blockers.append(
            f"{label}: scenario set mismatch "
            + f"missing={sorted(EXPECTED_SCENARIOS - observed)} "
            + f"extra={sorted(observed - EXPECTED_SCENARIOS)}"
        )
    expected_errors = {
        str(row.get("name"))
        for row in results
        if isinstance(row, dict) and row.get("expected_error") is True
    }
    if expected_errors != EXPECTED_ERROR_SCENARIOS:
        blockers.append(
            f"{label}: expected error set mismatch "
            + f"missing={sorted(EXPECTED_ERROR_SCENARIOS - expected_errors)} "
            + f"extra={sorted(expected_errors - EXPECTED_ERROR_SCENARIOS)}"
        )
    no_fallback_count = 0
    ok_count = 0
    for row in results:
        if not isinstance(row, dict):
            blockers.append(f"{label}: result row must be an object")
            continue
        name = str(row.get("name", "missing"))
        if row.get("ok") is not True:
            blockers.append(f"{label}: {name} ok must be true")
        else:
            ok_count += 1
        if row.get("fallback_attempted") is not False:
            blockers.append(f"{label}: {name} fallback_attempted must be false")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"{label}: {name} external_engine_invoked must be false")
        if (
            row.get("fallback_attempted") is False
            and row.get("external_engine_invoked") is False
        ):
            no_fallback_count += 1
    return {
        "status": "passed" if not blockers else "failed",
        "summary_ref": rel(repo_root, resolved),
        "scenario_count": len(results),
        "expected_error_scenario_count": len(expected_errors),
        "ok_scenario_count": ok_count,
        "no_fallback_scenario_count": no_fallback_count,
    }, blockers


def build_report(
    *,
    repo_root: Path,
    work_dir: Path,
    golden_workflow_report: Path,
    binary: Path,
    explicit_binary: bool,
    features: str,
    skip_build: bool,
    profile_order: tuple[str, ...],
) -> dict[str, Any]:
    work_dir = resolve_path(repo_root, work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    blockers: list[str] = []

    build, build_blockers = ensure_binary(
        repo_root,
        binary=binary,
        features=features,
        skip_build=skip_build,
        explicit_binary=explicit_binary,
    )
    blockers.extend(build_blockers)
    docs_summary, docs_blockers = validate_doc_markers(repo_root)
    blockers.extend(f"docs: {blocker}" for blocker in docs_blockers)
    golden_summary, golden_blockers = validate_golden_report(repo_root, golden_workflow_report)
    blockers.extend(golden_blockers)

    binary_arg = str(binary)
    profile_arg = ",".join(profile_order)
    quickstart = run_command(
        repo_root,
        [
            sys.executable,
            "examples/local-python-smoke/run.py",
            "--repo-root",
            ".",
            "--shardloom-bin",
            binary_arg,
        ],
    )
    quickstart_summary, quickstart_blockers = validate_quickstart(quickstart)
    blockers.extend(quickstart_blockers)

    scenario_run_root = work_dir / "python-benchmark-scenarios"
    scenario_run = run_command(
        repo_root,
        [
            sys.executable,
            "examples/local-python-benchmark-scenarios/run.py",
            "--repo-root",
            ".",
            "--run-root",
            rel(repo_root, scenario_run_root),
            "--run-id",
            "docs-website-etl",
            "--shardloom-bin",
            binary_arg,
            "--profile-order",
            profile_arg,
        ],
    )
    scenario_summary_path = (
        scenario_run_root / "docs-website-etl" / "scenario-summary.json"
    )
    scenario_summary, scenario_blockers = validate_scenario_payload(
        repo_root,
        scenario_summary_path,
        label="benchmark_scenario_runner",
    )
    blockers.extend(scenario_blockers)
    if scenario_run.get("returncode") != 0:
        blockers.append("benchmark_scenario_runner: command failed")

    timing_json = work_dir / "timing-components.json"
    timing_md = work_dir / "timing-components.md"
    timing_run_root = work_dir / "python-benchmark-timing"
    timing_review = run_command(
        repo_root,
        [
            sys.executable,
            "examples/local-python-benchmark-scenarios/timing_review.py",
            "--repo-root",
            ".",
            "--run-root",
            rel(repo_root, timing_run_root),
            "--run-id",
            "docs-website-timing",
            "--shardloom-bin",
            binary_arg,
            "--profile-order",
            profile_arg,
            "--output-json",
            rel(repo_root, timing_json),
            "--output-md",
            rel(repo_root, timing_md),
        ],
    )
    timing_summary, timing_blockers = validate_scenario_payload(
        repo_root,
        timing_json,
        label="timing_review",
    )
    blockers.extend(timing_blockers)
    if timing_review.get("returncode") != 0:
        blockers.append("timing_review: command failed")
    if not timing_md.exists():
        blockers.append("timing_review: missing timing markdown")

    runtime_commands = {
        "quickstart_python_smoke": quickstart,
        "benchmark_scenario_runner": scenario_run,
        "timing_review": timing_review,
    }
    runtime_command_status = (
        "passed"
        if all(row.get("status") == "passed" for row in runtime_commands.values())
        else "failed"
    )
    unsupported_failure_fixture_count = (
        int(bool(quickstart_summary.get("unsupported_fixture_present")))
        + len(EXPECTED_ERROR_SCENARIOS)
    )
    all_no_fallback = (
        not blockers
        and scenario_summary.get("no_fallback_scenario_count") == len(EXPECTED_SCENARIOS)
        and timing_summary.get("no_fallback_scenario_count") == len(EXPECTED_SCENARIOS)
    )
    passed = not blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "blockers": blockers,
        "build": build,
        "binary_ref": rel(repo_root, binary),
        "work_dir_ref": rel(repo_root, work_dir),
        "golden_workflow_report_ref": str(golden_workflow_report).replace("\\", "/"),
        "docs_marker_source_count": docs_summary["source_count"],
        "docs_marker_count": docs_summary["marker_count"],
        "docs_marker_pass_count": docs_summary["passed_marker_count"],
        "docs_marker_status": docs_summary["status"],
        "docs_marker_rows": docs_summary["rows"],
        "runtime_command_count": len(runtime_commands),
        "runtime_command_status": runtime_command_status,
        "runtime_commands": runtime_commands,
        "golden_workflow_replay_status": golden_summary["status"],
        "golden_workflow_replay_verified_count": golden_summary[
            "replay_verified_workflow_count"
        ],
        "golden_workflow_stage_count": golden_summary["stage_count"],
        "golden_workflow_stage_no_fallback_count": golden_summary[
            "stage_no_fallback_count"
        ],
        "golden_workflow_summary": golden_summary,
        "docs_example_execution_status": "passed" if passed else "blocked",
        "python_readme_example_execution_status": "passed"
        if docs_summary["status"] == "passed" and quickstart_summary["status"] == "passed"
        else "blocked",
        "website_example_execution_status": "passed"
        if docs_summary["status"] == "passed" and scenario_summary["status"] == "passed"
        else "blocked",
        "quickstart_smoke_status": quickstart_summary["status"],
        "quickstart_summary": quickstart_summary,
        "benchmark_scenario_execution_status": scenario_summary["status"],
        "benchmark_scenario_summary": scenario_summary,
        "timing_review_status": timing_summary["status"],
        "timing_review_summary": {
            **timing_summary,
            "timing_markdown_ref": rel(repo_root, timing_md),
        },
        "benchmark_scenario_count": scenario_summary["scenario_count"],
        "benchmark_expected_error_scenario_count": scenario_summary[
            "expected_error_scenario_count"
        ],
        "expected_error_scenario_ids": sorted(EXPECTED_ERROR_SCENARIOS),
        "unsupported_failure_fixture_count": unsupported_failure_fixture_count,
        "unsupported_failure_fixture_status": "passed"
        if unsupported_failure_fixture_count >= EXPECTED_UNSUPPORTED_FAILURE_FIXTURES
        and quickstart_summary["status"] == "passed"
        else "blocked",
        "all_no_fallback_no_external_engine": all_no_fallback,
        "claim_gate_status": "not_claim_grade",
        "runtime_support_claim_allowed": False,
        "correctness_claim_allowed": passed,
        **fail_closed_fields(),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = args.repo_root.resolve()
    binary = locate_binary(repo_root, args.binary)
    report = build_report(
        repo_root=repo_root,
        work_dir=args.work_dir,
        golden_workflow_report=args.golden_workflow_report,
        binary=binary,
        explicit_binary=args.binary is not None,
        features=args.features,
        skip_build=args.skip_build,
        profile_order=split_profile_order(args.profile_order),
    )
    output = resolve_path(repo_root, args.output)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
