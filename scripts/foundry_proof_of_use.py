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


def foundry_generated_output_fanout_posture() -> dict[str, Any]:
    return {
        "schema_version": FOUNDRY_GENERATED_OUTPUT_FANOUT_SCHEMA_VERSION,
        "support_status": "report_only",
        "admission_status": "blocked_until_generated_source_and_foundry_output_api_evidence",
        "generated_output_execution_performed": False,
        "no_dataset_smoke_separate_from_generated_output": True,
        "input_dataset_count": 0,
        "source_io_performed": False,
        "generated_source_created": False,
        "generated_source_kind": "planned_deterministic_literal_table",
        "generated_source_schema_digest": None,
        "generated_source_row_count": 0,
        "generated_source_plan_digest": None,
        "generated_source_seed": None,
        "generation_deterministic": None,
        "generated_source_certificate_status": "not_emitted_report_only",
        "source_native_io_certificate_status": "not_applicable_no_source_dataset",
        "output_plan_id": None,
        "output_plan_reuse_hit": False,
        "fanout_output_count": 0,
        "output_io_performed": False,
        "output_native_io_certificate_status": "not_emitted_report_only",
        "result_dataset_output_status": "not_written_report_only",
        "evidence_dataset_output_status": "not_written_report_only",
        "foundry_output_api_required": True,
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "direct_s3_write_invoked": False,
        "object_store_write_invoked": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_gate_status": "not_claim_grade",
        "claim_boundary": (
            "Report-only Foundry generated-output fanout posture; not generated-output "
            "runtime, not Foundry production support, not direct S3/object-store write "
            "support, not package publication, and not a performance claim."
        ),
        "blockers": [
            "generated_source_certificate_not_emitted",
            "foundry_output_api_runtime_not_proven",
            "output_native_io_certificate_not_emitted",
            "result_dataset_not_written",
            "evidence_dataset_not_written",
        ],
    }


def foundry_generated_output_boundary() -> dict[str, Any]:
    return {
        "schema_version": FOUNDRY_GENERATED_OUTPUT_BOUNDARY_SCHEMA_VERSION,
        "support_status": "report_only",
        "boundary_status": "blocked_until_real_foundry_output_api_evidence",
        "no_dataset_smoke_separate_from_generated_output": True,
        "input_dataset_count": 0,
        "source_io_performed": False,
        "generated_source_created": False,
        "generated_output_execution_performed": False,
        "generated_source_certificate_status": "not_emitted_report_only",
        "output_io_performed": False,
        "output_native_io_certificate_status": "not_emitted_report_only",
        "foundry_output_api_required": True,
        "foundry_output_api_invoked": False,
        "foundry_result_dataset_written": False,
        "foundry_evidence_dataset_written": False,
        "direct_s3_read_invoked": False,
        "direct_s3_write_invoked": False,
        "object_store_read_invoked": False,
        "object_store_write_invoked": False,
        "object_store_commit_invoked": False,
        "lakehouse_output_invoked": False,
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "public_foundry_generated_output_claim_allowed": False,
        "claim_gate_status": "not_claim_grade",
        "claim_boundary": (
            "Foundry generated-output is a future validation target only. Any future "
            "admitted smoke must write result and evidence datasets through Foundry "
            "output APIs, not direct S3/object-store paths, and must preserve no "
            "fallback and no external-engine execution."
        ),
        "blockers": [
            "real_foundry_runtime_not_invoked",
            "foundry_output_api_not_invoked",
            "generated_source_certificate_not_emitted",
            "output_native_io_certificate_not_emitted",
            "result_dataset_not_written",
            "evidence_dataset_not_written",
            "direct_object_store_path_blocked",
        ],
    }


def foundry_scale_proof_boundary(staged_input_bytes: int) -> dict[str, Any]:
    return {
        "schema_version": FOUNDRY_SCALE_PROOF_BOUNDARY_SCHEMA_VERSION,
        "support_status": "report_only",
        "proof_boundary_status": "blocked_until_real_foundry_runtime_and_evidence_dataset",
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "foundry_input_dataset_count": 0,
        "foundry_output_dataset_count": 0,
        "staged_input_bytes": staged_input_bytes,
        "shardloom_execution_mode": "local_foundry_style_smoke_only",
        "split_count": 0,
        "memory_budget_bytes": None,
        "output_evidence_dataset_written": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "public_foundry_claim_allowed": False,
        "claim_gate_status": "not_foundry_scale_grade",
        "claim_boundary": (
            "Report-only Foundry scale proof boundary. Local Foundry-style smoke may "
            "prove import and local execution wiring, but it is not real Foundry runtime, "
            "Foundry compute, Foundry Spark, managed-platform scale, package publication, "
            "or production Foundry support."
        ),
        "blockers": [
            "real_foundry_runtime_not_invoked",
            "foundry_compute_not_invoked",
            "foundry_output_evidence_dataset_not_written",
            "foundry_input_dataset_count_not_proven",
            "resource_envelope_not_proven_in_foundry",
            "split_scale_not_proven_in_foundry",
        ],
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
    generated_output_fanout = foundry_generated_output_fanout_posture()
    generated_output_boundary = foundry_generated_output_boundary()
    foundry_scale_proof = foundry_scale_proof_boundary(
        staged_dataset_bytes(repo_root, transform)
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "blocked",
        "package_install_mode": "local_source_or_internal_artifact",
        "conda_internal_artifact_install_status": "documented_not_published",
        "transform_import_proven": steps[1]["returncode"] == 0,
        "cli_binary_resolved": binary.exists(),
        "resolved_cli_path": str(binary),
        "no_dataset_smoke_performed": transform.get("execution_certificate_status")
        == "not_emitted_no_dataset_smoke",
        "staged_dataset_path": transform.get("staged_dataset_path"),
        "staged_dataset_path_explicit": transform.get("staged_dataset_path_explicit") is True,
        "supported_local_native_execution_smoke_performed": not args.skip_local_execution_smoke,
        "supported_local_native_execution_status": "passed"
        if benchmark.get("schema_version") is not None
        else "not_run_or_missing",
        "certificate_metrics_dataset_output_written": transform_output.exists(),
        "certificate_output_ref": str(transform_output),
        "benchmark_metrics_output_ref": str(benchmark_output) if benchmark_output.exists() else None,
        "materialization_staging_boundary_report_ref": "examples/foundry-lightweight-transform/expected-certificate-fields.json",
        "foundry_dev_stack_starter_kit_status": "local_style_report_only",
        "foundry_dev_stack_starter_kit_ref": "docs/foundry/dev-stack-starter-kit.json",
        "foundry_dev_stack_starter_kit_schema_version": FOUNDRY_DEV_STACK_STARTER_SCHEMA_VERSION,
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
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "foundry_input_dataset_count": foundry_scale_proof["foundry_input_dataset_count"],
        "foundry_output_dataset_count": foundry_scale_proof["foundry_output_dataset_count"],
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
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "public_foundry_claim_allowed": False,
        "local_foundry_style_proof_claim_allowed": passed and not args.skip_local_execution_smoke,
        "claim_scope": "local_foundry_style_transform_and_local_vortex_execution_smoke_only",
        "future_required_evidence": [
            "real Foundry package/import proof",
            "Foundry transform runtime proof",
            "Foundry certificate dataset write proof",
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
