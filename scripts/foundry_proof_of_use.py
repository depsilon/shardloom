#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Run a local Foundry-style proof-of-use without invoking Foundry."""

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
SCHEMA_VERSION = "shardloom.foundry_proof_of_use_report.v1"
FOUNDRY_GENERATED_OUTPUT_FANOUT_SCHEMA_VERSION = (
    "shardloom.foundry_generated_output_fanout_posture.v1"
)
FOUNDRY_GENERATED_OUTPUT_BOUNDARY_SCHEMA_VERSION = (
    "shardloom.foundry_generated_output_boundary.v1"
)
FOUNDRY_SCALE_PROOF_BOUNDARY_SCHEMA_VERSION = (
    "shardloom.foundry_scale_proof_boundary.v1"
)
FOUNDRY_PACKAGE_PROOF_BOUNDARY_MATRIX_SCHEMA_VERSION = (
    "shardloom.foundry_package_proof_boundary_matrix.v1"
)
FOUNDRY_DEV_STACK_STARTER_SCHEMA_VERSION = (
    "shardloom.foundry_dev_stack_starter_kit.v1"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=Path("target/foundry-proof-of-use/report.json"))
    parser.add_argument("--rows", type=int, default=64)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--skip-local-execution-smoke", action="store_true")
    return parser.parse_args()


def binary_path(repo_root: Path) -> Path:
    binary = repo_root / "target" / "debug" / "shardloom"
    return binary.with_suffix(".exe") if os.name == "nt" else binary


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def run_step(name: str, command: list[str], cwd: Path) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return {
        "name": name,
        "command": command,
        "returncode": completed.returncode,
        "elapsed_millis": round((time.perf_counter() - started) * 1000.0, 4),
        "stdout": completed.stdout[-4000:],
        "stderr": completed.stderr[-4000:],
    }


def load_json_if_present(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def payload_bool(payload: dict[str, Any], key: str, default: bool = False) -> bool:
    value = payload.get(key, default)
    return value if isinstance(value, bool) else default


def payload_int(payload: dict[str, Any], key: str, default: int = 0) -> int:
    value = payload.get(key, default)
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def foundry_generated_output_fanout_posture(transform: dict[str, Any]) -> dict[str, Any]:
    executed = payload_bool(transform, "generated_output_execution_performed")
    style_api = payload_bool(transform, "foundry_style_output_api_invoked")
    style_result_written = payload_bool(transform, "foundry_style_result_dataset_written")
    style_evidence_written = payload_bool(transform, "foundry_style_evidence_dataset_written")
    fallback_attempted = payload_bool(transform, "fallback_attempted")
    external_engine_invoked = payload_bool(transform, "external_engine_invoked")
    local_style_supported = (
        executed
        and style_api
        and style_result_written
        and style_evidence_written
        and not fallback_attempted
        and not external_engine_invoked
    )
    return {
        "schema_version": FOUNDRY_GENERATED_OUTPUT_FANOUT_SCHEMA_VERSION,
        "support_status": (
            "local_style_smoke_supported" if local_style_supported else "report_only"
        ),
        "admission_status": (
            "local_style_generated_output_and_foundry_style_output_api_evidence_written"
            if local_style_supported
            else "blocked_until_generated_source_and_foundry_output_api_evidence"
        ),
        "generated_output_execution_performed": executed,
        "no_dataset_smoke_separate_from_generated_output": True,
        "input_dataset_count": 0,
        "source_io_performed": False,
        "generated_source_created": payload_bool(transform, "generated_source_created"),
        "generated_source_kind": transform.get(
            "generated_source_kind", "planned_deterministic_literal_table"
        ),
        "generated_source_schema_digest": None,
        "generated_source_row_count": payload_int(transform, "generated_source_row_count"),
        "generated_source_plan_digest": None,
        "generated_source_seed": None,
        "generation_deterministic": True if executed else None,
        "generated_source_certificate_status": transform.get(
            "generated_source_certificate_status", "not_emitted_report_only"
        ),
        "source_native_io_certificate_status": "not_applicable_no_source_dataset",
        "output_plan_id": transform.get("generated_output_ref"),
        "output_plan_reuse_hit": payload_bool(
            transform, "generated_output_fanout_result_reuse_hit"
        ),
        "fanout_output_count": payload_int(
            transform, "generated_output_fanout_output_count"
        ),
        "output_io_performed": executed,
        "output_native_io_certificate_status": transform.get(
            "output_native_io_certificate_status", "not_emitted_report_only"
        ),
        "result_dataset_output_status": (
            "written_local_foundry_style_dataset"
            if style_result_written
            else "not_written_report_only"
        ),
        "evidence_dataset_output_status": (
            "written_local_foundry_style_dataset"
            if style_evidence_written
            else "not_written_report_only"
        ),
        "foundry_output_api_required": True,
        "foundry_style_output_api_invoked": style_api,
        "foundry_style_result_dataset_written": style_result_written,
        "foundry_style_evidence_dataset_written": style_evidence_written,
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "foundry_output_api_invoked": False,
        "direct_s3_write_invoked": False,
        "object_store_write_invoked": False,
        "fallback_attempted": fallback_attempted,
        "external_engine_invoked": external_engine_invoked,
        "claim_gate_status": (
            "fixture_smoke_only" if local_style_supported else "not_claim_grade"
        ),
        "claim_boundary": (
            "Local Foundry-style generated-output fanout posture only. This proves "
            "source-free generated rows can be written through ShardLoom and then "
            "recorded in local Foundry-style result/evidence datasets; it is not "
            "real Foundry runtime, not Foundry production support, not direct "
            "S3/object-store write support, not package publication, and not a "
            "performance claim."
        ),
        "blockers": []
        if local_style_supported
        else [
            "generated_source_certificate_not_emitted",
            "foundry_style_output_api_runtime_not_proven",
            "output_native_io_certificate_not_emitted",
            "result_dataset_not_written",
            "evidence_dataset_not_written",
        ],
        "public_claim_blockers": [
            "real_foundry_runtime_not_invoked",
            "real_foundry_output_api_not_invoked",
            "foundry_package_not_published",
        ],
    }


def foundry_generated_output_boundary(transform: dict[str, Any]) -> dict[str, Any]:
    executed = payload_bool(transform, "generated_output_execution_performed")
    style_api = payload_bool(transform, "foundry_style_output_api_invoked")
    style_result_written = payload_bool(transform, "foundry_style_result_dataset_written")
    style_evidence_written = payload_bool(transform, "foundry_style_evidence_dataset_written")
    fallback_attempted = payload_bool(transform, "fallback_attempted")
    external_engine_invoked = payload_bool(transform, "external_engine_invoked")
    local_style_supported = (
        executed
        and style_api
        and style_result_written
        and style_evidence_written
        and not fallback_attempted
        and not external_engine_invoked
    )
    return {
        "schema_version": FOUNDRY_GENERATED_OUTPUT_BOUNDARY_SCHEMA_VERSION,
        "support_status": (
            "local_style_smoke_supported" if local_style_supported else "report_only"
        ),
        "boundary_status": (
            "local_style_dataset_output_written_real_foundry_blocked"
            if local_style_supported
            else "blocked_until_real_foundry_output_api_evidence"
        ),
        "no_dataset_smoke_separate_from_generated_output": True,
        "input_dataset_count": 0,
        "source_io_performed": False,
        "generated_source_created": payload_bool(transform, "generated_source_created"),
        "generated_output_execution_performed": executed,
        "generated_source_certificate_status": transform.get(
            "generated_source_certificate_status", "not_emitted_report_only"
        ),
        "output_io_performed": executed,
        "output_native_io_certificate_status": transform.get(
            "output_native_io_certificate_status", "not_emitted_report_only"
        ),
        "foundry_output_api_required": True,
        "foundry_output_api_invoked": False,
        "foundry_result_dataset_written": False,
        "foundry_evidence_dataset_written": False,
        "foundry_style_output_api_invoked": style_api,
        "foundry_style_result_dataset_written": style_result_written,
        "foundry_style_evidence_dataset_written": style_evidence_written,
        "foundry_style_result_dataset_ref": transform.get(
            "foundry_style_result_dataset_ref"
        ),
        "foundry_style_evidence_dataset_ref": transform.get(
            "foundry_style_evidence_dataset_ref"
        ),
        "direct_s3_read_invoked": False,
        "direct_s3_write_invoked": False,
        "object_store_read_invoked": False,
        "object_store_write_invoked": False,
        "object_store_commit_invoked": False,
        "lakehouse_output_invoked": False,
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "fallback_attempted": fallback_attempted,
        "external_engine_invoked": external_engine_invoked,
        "public_foundry_generated_output_claim_allowed": False,
        "claim_gate_status": (
            "fixture_smoke_only" if local_style_supported else "not_claim_grade"
        ),
        "claim_boundary": (
            "Local Foundry-style generated-output proof only. It writes local "
            "result/evidence dataset-shaped artifacts through the dev-stack output "
            "API, not direct S3/object-store paths. Real Foundry output API, "
            "production, package, Marketplace, and platform claims remain blocked."
        ),
        "blockers": [
            "real_foundry_runtime_not_invoked",
            "real_foundry_output_api_not_invoked",
            "direct_object_store_path_blocked",
        ],
    }


def foundry_scale_proof_boundary(
    staged_input_bytes: int,
    transform: dict[str, Any],
) -> dict[str, Any]:
    staged_executed = payload_bool(transform, "staged_input_transform_execution_performed")
    evidence_written = payload_bool(transform, "output_evidence_dataset_written")
    fallback_attempted = payload_bool(transform, "fallback_attempted")
    external_engine_invoked = payload_bool(transform, "external_engine_invoked")
    local_style_supported = (
        staged_executed
        and evidence_written
        and not fallback_attempted
        and not external_engine_invoked
    )
    return {
        "schema_version": FOUNDRY_SCALE_PROOF_BOUNDARY_SCHEMA_VERSION,
        "support_status": "local_style_smoke_supported" if local_style_supported else "report_only",
        "proof_boundary_status": (
            "local_style_staged_transform_and_evidence_dataset_written_real_foundry_blocked"
            if local_style_supported
            else "blocked_until_real_foundry_runtime_and_evidence_dataset"
        ),
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "foundry_input_dataset_count": 0,
        "foundry_output_dataset_count": 0,
        "foundry_style_input_dataset_count": 1 if staged_executed else 0,
        "foundry_style_output_dataset_count": (
            int(payload_bool(transform, "foundry_style_result_dataset_written"))
            + int(payload_bool(transform, "foundry_style_evidence_dataset_written"))
        ),
        "staged_input_bytes": staged_input_bytes,
        "staged_input_transform_execution_performed": staged_executed,
        "staged_input_transform_output_row_count": payload_int(
            transform, "staged_input_transform_output_row_count"
        ),
        "shardloom_execution_mode": "local_foundry_style_generated_and_staged_transform_smoke",
        "split_count": 0,
        "memory_budget_bytes": None,
        "output_evidence_dataset_written": evidence_written,
        "fallback_attempted": fallback_attempted,
        "external_engine_invoked": external_engine_invoked,
        "public_foundry_claim_allowed": False,
        "claim_gate_status": (
            "fixture_smoke_only" if local_style_supported else "not_foundry_scale_grade"
        ),
        "claim_boundary": (
            "Local Foundry-style staged transform proof only. It proves local staged "
            "bytes can flow through ShardLoom and into local result/evidence dataset "
            "artifacts, but it is not real Foundry runtime, Foundry compute, Foundry "
            "Spark, managed-platform scale, package publication, or production "
            "Foundry support."
        ),
        "blockers": [
            "real_foundry_runtime_not_invoked",
            "foundry_compute_not_invoked",
            "foundry_input_dataset_count_not_proven",
            "resource_envelope_not_proven_in_foundry",
            "split_scale_not_proven_in_foundry",
        ],
    }


def _foundry_boundary_row(
    row_id: str,
    *,
    support_status: str,
    local_style_claim_allowed: bool,
    required_evidence: list[str],
    blocker_id: str,
) -> dict[str, Any]:
    return {
        "row_id": row_id,
        "support_status": support_status,
        "proof_status": "local_style_only" if local_style_claim_allowed else "blocked",
        "required_evidence": required_evidence,
        "blocker_id": blocker_id,
        "local_style_claim_allowed": local_style_claim_allowed,
        "public_foundry_claim_allowed": False,
        "foundry_package_publication_allowed": False,
        "artifact_repository_publication_allowed": False,
        "foundry_service_invocation_allowed": False,
        "compute_module_invoked": False,
        "virtual_table_native_execution_claimed": False,
        "dataset_transaction_runtime_allowed": False,
        "f10_deployment_certified": False,
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "foundry_output_api_invoked": False,
        "external_engine_invoked": False,
        "fallback_attempted": False,
        "claim_gate_status": "fixture_smoke_only"
        if local_style_claim_allowed
        else "not_claim_grade",
    }


def foundry_package_proof_boundary_matrix() -> dict[str, Any]:
    rows = [
        _foundry_boundary_row(
            "local_style_transform_fixture",
            support_status="smoke_supported",
            local_style_claim_allowed=True,
            required_evidence=[
                "source_checkout",
                "local_cli_build",
                "local_transform_script",
                "no_foundry_runtime_invocation",
            ],
            blocker_id="none_local_style_fixture_only",
        ),
        _foundry_boundary_row(
            "local_certificate_metrics_output",
            support_status="smoke_supported",
            local_style_claim_allowed=True,
            required_evidence=[
                "local_certificate_json",
                "local_benchmark_metrics_json",
                "local_foundry_style_result_dataset",
                "local_foundry_style_evidence_dataset",
                "no_real_foundry_dataset_write",
            ],
            blocker_id="none_local_certificate_json_only",
        ),
        _foundry_boundary_row(
            "shardloom_foundry_package",
            support_status="blocked",
            local_style_claim_allowed=False,
            required_evidence=[
                "package_build",
                "clean_foundry_repo_install",
                "import_smoke_inside_foundry",
                "release_provenance",
            ],
            blocker_id="gar-0036-a.shardloom_foundry_package_not_published",
        ),
        _foundry_boundary_row(
            "artifact_repository_publication",
            support_status="blocked",
            local_style_claim_allowed=False,
            required_evidence=[
                "artifact_repository_upload",
                "version_pin",
                "install_from_artifact_repository",
                "rollback_policy",
            ],
            blocker_id="gar-0036-a.artifact_repository_publication_missing",
        ),
        _foundry_boundary_row(
            "foundry_service_invocation",
            support_status="blocked",
            local_style_claim_allowed=False,
            required_evidence=[
                "real_foundry_runtime_context",
                "service_invocation_trace",
                "foundry_compute_false_or_external_boundary",
                "evidence_dataset_output",
            ],
            blocker_id="gar-0036-a.foundry_service_invocation_missing",
        ),
        _foundry_boundary_row(
            "compute_module_surface",
            support_status="blocked",
            local_style_claim_allowed=False,
            required_evidence=[
                "compute_module_packaging",
                "compute_module_invocation_trace",
                "no_spark_fallback_proof",
            ],
            blocker_id="gar-0036-a.compute_module_not_proven",
        ),
        _foundry_boundary_row(
            "virtual_table_native_execution",
            support_status="blocked",
            local_style_claim_allowed=False,
            required_evidence=[
                "virtual_table_ref",
                "staged_native_data",
                "execution_certificate",
                "native_io_certificate",
            ],
            blocker_id="gar-0036-a.virtual_table_native_execution_not_proven",
        ),
        _foundry_boundary_row(
            "dataset_transaction_runtime",
            support_status="blocked",
            local_style_claim_allowed=False,
            required_evidence=[
                "foundry_dataset_transaction_context",
                "result_dataset_write",
                "evidence_dataset_write",
                "idempotency_key",
            ],
            blocker_id="gar-0036-a.dataset_transaction_runtime_not_proven",
        ),
        _foundry_boundary_row(
            "f10_workload_certified_deployment",
            support_status="blocked",
            local_style_claim_allowed=False,
            required_evidence=[
                "workload_constitution",
                "foundry_runtime_proof",
                "correctness_evidence",
                "benchmark_evidence",
                "release_gate",
            ],
            blocker_id="gar-0036-a.f10_deployment_not_certified",
        ),
    ]
    return {
        "schema_version": FOUNDRY_PACKAGE_PROOF_BOUNDARY_MATRIX_SCHEMA_VERSION,
        "gar_id": "GAR-0036-A",
        "support_status": "report_only",
        "claim_gate_status": "not_claim_grade",
        "row_count": len(rows),
        "row_order": [row["row_id"] for row in rows],
        "local_style_claim_allowed_count": sum(
            1 for row in rows if row["local_style_claim_allowed"]
        ),
        "blocked_count": sum(1 for row in rows if row["support_status"] == "blocked"),
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "public_foundry_claim_allowed": False,
        "claim_boundary": (
            "Local Foundry-style smoke, local result/evidence dataset-shaped "
            "outputs, and local certificate JSON proof only. "
            "No shardloom-foundry package, Artifact Repository publication, service "
            "invocation, Compute Module, virtual-table native execution, dataset "
            "transaction runtime, F10 deployment, Spark fallback, or production "
            "Foundry claim is supported."
        ),
        "rows": rows,
    }


def staged_dataset_bytes(repo_root: Path, transform: dict[str, Any]) -> int:
    staged_path = transform.get("staged_dataset_path")
    if not isinstance(staged_path, str) or not staged_path:
        return 0
    path = Path(staged_path)
    if not path.is_absolute():
        path = repo_root / path
    if path.is_file():
        return path.stat().st_size
    if path.is_dir():
        return sum(child.stat().st_size for child in path.rglob("*") if child.is_file())
    return 0


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    binary = binary_path(repo_root)
    transform_output = output.parent / "certificate-output.json"
    generated_output = output.parent / "generated-output.jsonl"
    generated_output_csv = output.parent / "generated-output.csv"
    staged_transform_output = output.parent / "staged-transform-output.jsonl"
    result_dataset = output.parent / "result-dataset"
    evidence_dataset = output.parent / "evidence-dataset"
    benchmark_output = output.parent / "local-vortex-benchmark-smoke.json"

    steps: list[dict[str, Any]] = []
    steps.append(run_step("build_local_cli", ["cargo", "build", "-p", "shardloom-cli", "--bin", "shardloom"], repo_root))
    steps.append(
        run_step(
            "foundry_style_transform_import_smoke",
            [
                sys.executable,
                "examples/foundry-lightweight-transform/run.py",
                "--repo-root",
                str(repo_root),
                "--shardloom-bin",
                str(binary),
                "--output",
                str(transform_output),
                "--generated-output",
                str(generated_output),
                "--generated-output-csv",
                str(generated_output_csv),
                "--staged-transform-output",
                str(staged_transform_output),
                "--result-dataset",
                str(result_dataset),
                "--evidence-dataset",
                str(evidence_dataset),
            ],
            repo_root,
        )
    )
    if not args.skip_local_execution_smoke:
        steps.append(
            run_step(
                "supported_local_vortex_execution_smoke",
                [
                    sys.executable,
                    "examples/local-vortex-benchmark/run.py",
                    "--repo-root",
                    str(repo_root),
                    "--rows",
                    str(args.rows),
                    "--iterations",
                    str(args.iterations),
                    "--output",
                    str(benchmark_output),
                ],
                repo_root,
            )
        )

    transform = load_json_if_present(transform_output) or {}
    benchmark = load_json_if_present(benchmark_output) or {}
    passed = all(step["returncode"] == 0 for step in steps)
    generated_output_fanout = foundry_generated_output_fanout_posture(transform)
    generated_output_boundary = foundry_generated_output_boundary(transform)
    foundry_scale_proof = foundry_scale_proof_boundary(
        staged_dataset_bytes(repo_root, transform),
        transform,
    )
    package_boundary_matrix = foundry_package_proof_boundary_matrix()
    local_style_output_api_invoked = payload_bool(
        transform,
        "foundry_style_output_api_invoked",
    )
    local_style_result_written = payload_bool(
        transform,
        "foundry_style_result_dataset_written",
    )
    local_style_evidence_written = payload_bool(
        transform,
        "foundry_style_evidence_dataset_written",
    )
    generated_execution = payload_bool(transform, "generated_output_execution_performed")
    staged_execution = payload_bool(
        transform,
        "staged_input_transform_execution_performed",
    )
    fallback_attempted = payload_bool(transform, "fallback_attempted")
    external_engine_invoked = payload_bool(transform, "external_engine_invoked")
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "blocked",
        "package_install_mode": "local_source_or_internal_artifact",
        "conda_internal_artifact_install_status": "documented_not_published",
        "transform_import_proven": steps[1]["returncode"] == 0,
        "cli_binary_resolved": binary.exists(),
        "resolved_cli_path": str(binary),
        "no_dataset_smoke_performed": transform.get("smoke_protocol_version")
        is not None,
        "staged_dataset_path": transform.get("staged_dataset_path"),
        "staged_dataset_path_explicit": transform.get("staged_dataset_path_explicit") is True,
        "staged_input_transform_execution_performed": staged_execution,
        "staged_input_transform_output_ref": transform.get(
            "staged_input_transform_output_ref"
        ),
        "staged_input_transform_output_row_count": transform.get(
            "staged_input_transform_output_row_count"
        ),
        "supported_local_native_execution_smoke_performed": not args.skip_local_execution_smoke,
        "supported_local_native_execution_status": "passed"
        if benchmark.get("schema_version") is not None
        else "not_run_or_missing",
        "certificate_metrics_dataset_output_written": transform_output.exists()
        and local_style_evidence_written,
        "certificate_output_ref": str(transform_output),
        "result_dataset_output_ref": transform.get("foundry_style_result_dataset_ref"),
        "evidence_dataset_output_ref": transform.get("foundry_style_evidence_dataset_ref"),
        "benchmark_metrics_output_ref": str(benchmark_output) if benchmark_output.exists() else None,
        "materialization_staging_boundary_report_ref": "examples/foundry-lightweight-transform/expected-certificate-fields.json",
        "foundry_dev_stack_starter_kit_status": "local_style_runtime_proof",
        "foundry_dev_stack_starter_kit_ref": "docs/foundry/dev-stack-starter-kit.json",
        "foundry_dev_stack_starter_kit_schema_version": FOUNDRY_DEV_STACK_STARTER_SCHEMA_VERSION,
        "generated_output_execution_performed": generated_execution,
        "generated_output_ref": transform.get("generated_output_ref"),
        "generated_output_fanout_ref": transform.get("generated_output_fanout_ref"),
        "generated_source_created": payload_bool(transform, "generated_source_created"),
        "generated_source_kind": transform.get("generated_source_kind"),
        "generated_source_row_count": transform.get("generated_source_row_count"),
        "generated_source_certificate_status": transform.get(
            "generated_source_certificate_status"
        ),
        "output_native_io_certificate_status": transform.get(
            "output_native_io_certificate_status"
        ),
        "foundry_style_output_api_invoked": local_style_output_api_invoked,
        "foundry_style_result_dataset_written": local_style_result_written,
        "foundry_style_evidence_dataset_written": local_style_evidence_written,
        "foundry_generated_output_fanout_status": generated_output_fanout["support_status"],
        "foundry_generated_output_fanout_ref": "foundry_generated_output_fanout_posture",
        "foundry_generated_output_fanout_posture": generated_output_fanout,
        "foundry_generated_output_boundary_status": generated_output_boundary[
            "support_status"
        ],
        "foundry_generated_output_boundary_ref": "foundry_generated_output_boundary",
        "foundry_generated_output_boundary": generated_output_boundary,
        "foundry_scale_proof_boundary_status": foundry_scale_proof["support_status"],
        "foundry_scale_proof_boundary_ref": "foundry_scale_proof_boundary",
        "foundry_scale_proof_boundary": foundry_scale_proof,
        "foundry_package_proof_boundary_matrix_status": package_boundary_matrix[
            "support_status"
        ],
        "foundry_package_proof_boundary_matrix_ref": "foundry_package_proof_boundary_matrix",
        "foundry_package_proof_boundary_matrix": package_boundary_matrix,
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "foundry_input_dataset_count": foundry_scale_proof["foundry_input_dataset_count"],
        "foundry_output_dataset_count": foundry_scale_proof["foundry_output_dataset_count"],
        "foundry_style_input_dataset_count": foundry_scale_proof[
            "foundry_style_input_dataset_count"
        ],
        "foundry_style_output_dataset_count": foundry_scale_proof[
            "foundry_style_output_dataset_count"
        ],
        "staged_input_bytes": foundry_scale_proof["staged_input_bytes"],
        "shardloom_execution_mode": foundry_scale_proof["shardloom_execution_mode"],
        "split_count": foundry_scale_proof["split_count"],
        "memory_budget_bytes": foundry_scale_proof["memory_budget_bytes"],
        "output_evidence_dataset_written": foundry_scale_proof[
            "output_evidence_dataset_written"
        ],
        "direct_s3_write_invoked": False,
        "direct_s3_read_invoked": False,
        "object_store_read_invoked": False,
        "object_store_write_invoked": False,
        "object_store_commit_invoked": False,
        "foundry_output_api_invoked": False,
        "snowflake_databricks_bigquery_invoked": False,
        "virtual_tables_native_execution_claimed": False,
        "external_compute_boundary": "governed_handle_or_baseline_only",
        "fallback_attempted": fallback_attempted,
        "external_engine_invoked": external_engine_invoked,
        "public_foundry_claim_allowed": False,
        "local_foundry_style_proof_claim_allowed": (
            passed
            and generated_execution
            and staged_execution
            and local_style_output_api_invoked
            and local_style_result_written
            and local_style_evidence_written
            and not fallback_attempted
            and not external_engine_invoked
            and not args.skip_local_execution_smoke
        ),
        "claim_scope": "local_foundry_style_generated_output_and_staged_transform_smoke_only",
        "future_required_evidence": [
            "real Foundry package/import proof",
            "Foundry transform runtime proof",
            "real Foundry result dataset write proof",
            "real Foundry evidence dataset write proof",
            "Foundry Data Health/Data Expectations bridge proof",
            "governed dataset transaction proof",
        ],
        "steps": steps,
    }
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
