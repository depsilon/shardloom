#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate ShardLoom runtime-envelope evidence contracts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python" / "src"))

from shardloom import OutputEnvelope, validate_runtime_execution_fields  # noqa: E402

SCHEMA_VERSION = "shardloom.runtime_execution_envelope_validation_report.v1"
VALIDATOR_SCHEMA_VERSION = "shardloom.runtime_execution_envelope_validation.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--benchmark-artifact",
        type=Path,
        default=Path("website/assets/benchmarks/latest/benchmark-results.json"),
    )
    parser.add_argument(
        "--runs-today",
        type=Path,
        default=Path("docs/status/runs-today-support-matrix.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/runtime-execution-envelope-validation-report.json"),
    )
    return parser.parse_args()


def resolve_repo_path(path: Path, repo_root: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    return None


def envelope(
    fields: list[dict[str, str]],
    *,
    command: str = "sql-local-source-smoke",
    policy_fields: list[dict[str, str]] | None = None,
) -> OutputEnvelope:
    return OutputEnvelope.from_json(
        {
            "schema_version": "shardloom.output.v2",
            "command": command,
            "status": "success",
            "summary": "runtime envelope validation fixture",
            "human_text": "runtime envelope validation fixture",
            "fallback": {
                "attempted": False,
                "allowed": False,
                "engine": None,
                "reason": "disabled",
            },
            "diagnostics": [],
            "result": {"fields": fields},
            "result_refs": [],
            "artifacts": [],
            "artifact_refs": [],
            "certificates": [],
            "policy": {
                "fields": policy_fields
                if policy_fields is not None
                else [
                    {"key": "fallback_attempted", "value": "false"},
                    {"key": "external_engine_invoked", "value": "false"},
                    {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                ]
            },
            "lifecycle": {"fields": []},
            "capability_snapshot": {"fields": []},
            "fields": [],
        }
    )


def fixture_rows() -> list[dict[str, Any]]:
    complete = envelope(
        [
            {"key": "source_state_id", "value": "source-state-fixture"},
            {"key": "source_state_digest", "value": "fnv64:source"},
            {"key": "source_state_materialization_layout", "value": "scalar_row_map"},
            {"key": "execution_certificate_ref", "value": "sql-local-source.execution.v1"},
        ]
    ).runtime_execution_validation(surface_id="complete_sql_local_source")

    missing_certificate = envelope(
        [
            {"key": "source_state_id", "value": "source-state-fixture"},
            {"key": "source_state_materialization_layout", "value": "scalar_row_map"},
        ]
    ).runtime_execution_validation(surface_id="missing_execution_certificate")

    prepared_missing_state = envelope(
        [
            {"key": "execution_mode", "value": "prepared_vortex"},
            {"key": "source_state_materialization_layout", "value": "encoded_vortex"},
            {"key": "execution_certificate_ref", "value": "prepared-vortex.execution.v1"},
        ],
        command="traditional-analytics-vortex-batch-run",
    ).runtime_execution_validation(surface_id="prepared_vortex_missing_state")

    certified_timing_drift = envelope(
        [
            {"key": "execution_mode", "value": "compatibility_import_certified"},
            {"key": "source_state_id", "value": "source-state-fixture"},
            {"key": "source_state_materialization_layout", "value": "columnar_source_state"},
            {"key": "execution_certificate_ref", "value": "compat-certified.execution.v1"},
            {"key": "timing_scope", "value": "warm_query_only"},
            {"key": "preparation_included", "value": "false"},
        ],
        command="traditional-analytics-run",
    ).runtime_execution_validation(surface_id="compatibility_import_certified_timing_drift")

    invalid_no_fallback_flags = envelope(
        [
            {"key": "source_state_id", "value": "source-state-fixture"},
            {"key": "source_state_materialization_layout", "value": "scalar_row_map"},
            {"key": "execution_certificate_ref", "value": "sql-local-source.execution.v1"},
        ],
        policy_fields=[
            {"key": "fallback_attempted", "value": "maybe"},
            {"key": "external_engine_invoked", "value": "false"},
            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
        ],
    ).runtime_execution_validation(surface_id="invalid_no_fallback_flags")

    benchmark_field_mapping = validate_runtime_execution_fields(
        {
            "source_state_id": "source-state://benchmark-fixture",
            "source_state_digest": "fnv1a64:source",
            "prepared_state_id": "prepared-state://benchmark-fixture",
            "prepared_state_digest": "fnv1a64:prepared",
            "data_decoded": False,
            "data_materialized": False,
            "runtime_execution_certificate_id": "execution.benchmark-fixture",
            "runtime_execution_certificate_status": "certified",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "claim_gate_status": "fixture_smoke_only",
        },
        command="traditional-analytics-benchmark-row",
        surface_id="benchmark_field_mapping",
    )

    report_only_runtime = validate_runtime_execution_fields(
        {
            "runtime_execution": True,
            "support_state": "report_only",
            "source_state_id": "source-state://report-only",
            "data_decoded": False,
            "runtime_execution_certificate_id": "execution.report-only",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "claim_gate_status": "not_claim_grade",
        },
        command="runs-today-status-row",
        surface_id="report_only_runtime_masquerade",
    )

    minimal_runtime_claim_grade = validate_runtime_execution_fields(
        {
            "source_state_id": "source-state://minimal-runtime",
            "data_decoded": False,
            "runtime_execution_certificate_id": "execution.minimal-runtime",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "claim_gate_status": "claim_grade",
            "selected_evidence_level": "minimal_runtime",
        },
        command="traditional-analytics-benchmark-row",
        surface_id="minimal_runtime_claim_grade",
    )

    evidence_level_refs_only = validate_runtime_execution_fields(
        {
            "source_state_id": "source-state://refs-only",
            "data_decoded": False,
            "evidence_level_certificate_refs": "execution_certificate_status",
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "claim_gate_status": "fixture_smoke_only",
        },
        command="traditional-analytics-benchmark-row",
        surface_id="evidence_level_refs_without_execution_certificate",
    )

    return [
        complete.as_dict(),
        missing_certificate.as_dict(),
        prepared_missing_state.as_dict(),
        certified_timing_drift.as_dict(),
        invalid_no_fallback_flags.as_dict(),
        benchmark_field_mapping.as_dict(),
        report_only_runtime.as_dict(),
        minimal_runtime_claim_grade.as_dict(),
        evidence_level_refs_only.as_dict(),
    ]


def benchmark_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("published_benchmark_rows", "results", "rows"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def benchmark_field_map(row: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    evidence = row.get("shardloom_evidence")
    if isinstance(evidence, dict):
        fields.update(evidence)
    metrics = row.get("metrics")
    if isinstance(metrics, dict):
        fields.update(metrics)
    for key, value in row.items():
        if key in {
            "benchmark_constitution",
            "iteration_wall_time_millis",
            "metrics",
            "output_preview",
            "runtime_execution_validation",
            "shardloom_evidence",
        }:
            continue
        fields[key] = value
    if row.get("selected_execution_mode") == "compatibility_import_certified":
        fields["preparation_included"] = (
            row.get("compatibility_import_included") is True
            or fields.get("preparation_included_in_timing") is True
        )
    return fields


def benchmark_surface_id(row: dict[str, Any], index: int) -> str:
    scenario = str(row.get("scenario_id") or row.get("scenario_name") or index)
    scenario = scenario.lower().replace(" ", "_").replace(":", "_")
    return (
        "website_benchmark."
        f"{row.get('engine', 'unknown')}."
        f"{row.get('storage_format', 'unknown')}."
        f"{scenario}"
    )


def validate_benchmark_artifact(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    if not path.exists():
        return [], [f"benchmark artifact missing: {path}"]
    payload = load_json(path)
    if not isinstance(payload, dict):
        return [], [f"benchmark artifact must contain a JSON object: {path}"]
    rows = benchmark_rows(payload)
    if not rows:
        return [], [f"benchmark artifact has no rows: {path}"]

    reports: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        engine = str(row.get("engine", ""))
        if not engine.startswith("shardloom"):
            continue
        status = str(row.get("status", "unknown"))
        validation = validate_runtime_execution_fields(
            benchmark_field_map(row),
            command="website-published-benchmark-row",
            status=status,
            surface_id=benchmark_surface_id(row, index),
            runtime_expected=status == "success",
            execution_mode=str(row.get("selected_execution_mode") or "") or None,
        )
        report = validation.as_dict()
        reports.append(report)
        if validation.status != "passed":
            blockers.append(
                f"{report['surface_id']} runtime envelope blocked: "
                + "; ".join(validation.blockers)
            )
    if not reports:
        blockers.append(f"benchmark artifact has no ShardLoom rows: {path}")
    return reports, blockers


def validate_runs_today(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    if not path.exists():
        return [], [f"runs-today support matrix missing: {path}"]
    payload = load_json(path)
    if not isinstance(payload, dict):
        return [], [f"runs-today support matrix must contain a JSON object: {path}"]
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return [], [f"runs-today support matrix has no rows: {path}"]

    validated_rows: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            blockers.append(f"runs-today row {index} must be an object")
            continue
        row_id = str(row.get("id") or index)
        missing = [
            field
            for field in (
                "support_state",
                "runtime_execution",
                "fallback_attempted",
                "external_engine_invoked",
                "claim_gate_status",
                "evidence_refs",
            )
            if field not in row
        ]
        if missing:
            blockers.append(f"runs-today row {row_id} missing fields: {missing}")
            continue
        fallback_attempted = parse_bool(row.get("fallback_attempted"))
        external_engine_invoked = parse_bool(row.get("external_engine_invoked"))
        runtime_execution = parse_bool(row.get("runtime_execution"))
        if fallback_attempted is not False:
            blockers.append(f"runs-today row {row_id} must set fallback_attempted=false")
        if external_engine_invoked is not False:
            blockers.append(
                f"runs-today row {row_id} must set external_engine_invoked=false"
            )
        support_state = str(row.get("support_state"))
        if runtime_execution is True and support_state in {
            "report_only",
            "diagnostic_only",
            "blocked",
            "future",
        }:
            blockers.append(
                f"runs-today row {row_id} cannot mark {support_state} as runtime_execution"
            )
        if runtime_execution is True and not row.get("evidence_refs"):
            blockers.append(f"runs-today row {row_id} runtime execution lacks evidence_refs")
        validated_rows.append(
            {
                "row_id": row_id,
                "support_state": support_state,
                "runtime_execution": runtime_execution,
                "fallback_attempted": fallback_attempted,
                "external_engine_invoked": external_engine_invoked,
                "claim_gate_status": row.get("claim_gate_status"),
                "status": "passed",
            }
        )
    return validated_rows, blockers


def validate_repo(
    repo_root: Path = ROOT,
    *,
    benchmark_artifact: Path | None = None,
    runs_today: Path | None = None,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    benchmark_path = resolve_repo_path(
        benchmark_artifact or Path("website/assets/benchmarks/latest/benchmark-results.json"),
        repo_root,
    )
    runs_today_path = resolve_repo_path(
        runs_today or Path("docs/status/runs-today-support-matrix.json"),
        repo_root,
    )

    rows = fixture_rows()
    expected = {
        "complete_sql_local_source": "passed",
        "missing_execution_certificate": "blocked",
        "prepared_vortex_missing_state": "blocked",
        "compatibility_import_certified_timing_drift": "blocked",
        "invalid_no_fallback_flags": "blocked",
        "benchmark_field_mapping": "passed",
        "report_only_runtime_masquerade": "blocked",
        "minimal_runtime_claim_grade": "blocked",
        "evidence_level_refs_without_execution_certificate": "blocked",
    }
    fixture_blockers = [
        f"{row['surface_id']} status={row['status']} expected={expected[row['surface_id']]}"
        for row in rows
        if row["status"] != expected[row["surface_id"]]
    ]
    benchmark_reports, benchmark_blockers = validate_benchmark_artifact(benchmark_path)
    runs_today_rows, runs_today_blockers = validate_runs_today(runs_today_path)
    blockers = fixture_blockers + benchmark_blockers + runs_today_blockers
    benchmark_runtime_row_count = len(benchmark_reports)
    status_runtime_row_count = sum(
        1 for row in runs_today_rows if row.get("runtime_execution") is True
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "blocked",
        "validator_schema_version": VALIDATOR_SCHEMA_VERSION,
        "validated_surfaces": [
            "runtime_envelope_fixtures",
            "website_published_benchmark_rows",
            "runs_today_support_matrix",
        ],
        "fixture_row_count": len(rows),
        "fixture_passed_row_count": sum(1 for row in rows if row["status"] == "passed"),
        "fixture_blocked_row_count": sum(1 for row in rows if row["status"] == "blocked"),
        "benchmark_artifact": str(benchmark_path.relative_to(repo_root)),
        "benchmark_row_count": benchmark_runtime_row_count,
        "benchmark_passed_row_count": sum(
            1 for row in benchmark_reports if row["status"] == "passed"
        ),
        "runs_today_matrix": str(runs_today_path.relative_to(repo_root)),
        "status_row_count": len(runs_today_rows),
        "status_runtime_row_count": status_runtime_row_count,
        "rows": rows,
        "benchmark_rows": benchmark_reports,
        "runs_today_rows": runs_today_rows,
        "blockers": blockers,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_gate_status": "not_claim_grade",
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve_repo_path(args.output, repo_root)
    report = validate_repo(
        repo_root,
        benchmark_artifact=args.benchmark_artifact,
        runs_today=args.runs_today,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0 if not report["blockers"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
