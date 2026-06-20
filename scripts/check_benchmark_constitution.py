#!/usr/bin/env python3
"""Validate benchmark constitution fields without running benchmarks."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from check_benchmark_artifact_completeness import result_rows as benchmark_result_rows


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.benchmark_constitution_validation_script.v1"
CONSTITUTION_SCHEMA_VERSION = "shardloom.benchmark_constitution_validation.v1"
CLAIM_READY_STATUSES = {
    "claim_grade",
    "ready_to_publish",
    "ready_for_claim_review",
}
REQUIRED_FIELD_ORDER = (
    "benchmark_result_row",
    "route_identity",
    "route_runtime_status",
    "dataset_source_admission",
    "preparation_route",
    "execution_route",
    "output_route",
    "claim_readiness_boundary",
    "correctness_proof",
    "hardware_profile",
    "build_profile",
    "cold_warm_state",
    "stage_timings",
    "cold_lane_attribution",
    "cost_unit_fields",
    "no_fallback_proof",
    "external_baseline_boundary",
)
STAGE_TIMING_FIELDS = (
    "source_stat_millis",
    "source_read_millis",
    "source_parse_millis",
    "compatibility_parse_millis",
    "source_to_columnar_millis",
    "compatibility_to_vortex_import_millis",
    "vortex_array_build_millis",
    "vortex_write_millis",
    "vortex_digest_millis",
    "vortex_reopen_verify_millis",
    "vortex_reopen_millis",
    "vortex_scan_millis",
    "operator_compute_millis",
    "scenario_compute_millis",
    "result_sink_write_millis",
    "evidence_render_millis",
    "query_runtime_millis",
    "total_runtime_millis",
    "wall_time_millis",
    "iteration_wall_time_millis",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("website/assets/benchmarks/latest/manifest.json"),
    )
    parser.add_argument(
        "--artifact",
        type=Path,
        action="append",
        default=None,
        help="Benchmark artifact JSON to validate. Defaults to the website latest artifact.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/benchmark-constitution-report.json"),
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Verify that an overclaiming synthetic row is rejected.",
    )
    return parser.parse_args()


def resolve(path: Path) -> Path:
    return path if path.is_absolute() else ROOT / path


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def boolish(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def non_empty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip()) and value.strip().lower() not in {"unknown", "n/a"}
    if isinstance(value, list):
        return any(non_empty(item) for item in value)
    return True


def first_present(row: dict[str, Any], keys: tuple[str, ...]) -> bool:
    return any(non_empty(row.get(key)) for key in keys)


def merged_row_fields(row: dict[str, Any]) -> dict[str, Any]:
    fields = dict(row)
    evidence = row.get("shardloom_evidence")
    if isinstance(evidence, dict):
        fields.update(evidence)
    metrics = row.get("metrics")
    if isinstance(metrics, dict):
        fields.update(metrics)
    return fields


def row_label(row: dict[str, Any], index: int) -> str:
    scenario = row.get("scenario_name") or row.get("scenario") or row.get("scenario_id") or index
    engine = row.get("engine", "unknown")
    return f"{engine}:{scenario}"


def is_shardloom_row(row: dict[str, Any]) -> bool:
    return str(row.get("engine", "")).startswith("shardloom") or row.get(
        "row_classification"
    ) == "shardloom_execution_evidence"


def is_claim_bearing(row: dict[str, Any], manifest_claims_allowed: bool = False) -> bool:
    if boolish(row.get("performance_claim_allowed")) is True:
        return True
    if (
        manifest_claims_allowed
        and str(row.get("claim_gate_status", "")).strip().lower() in CLAIM_READY_STATUSES
    ):
        return True
    return (
        manifest_claims_allowed
        and str(row.get("row_claim_level", "")).strip().lower() in CLAIM_READY_STATUSES
    )


def row_reports_claim_grade(fields: dict[str, Any]) -> bool:
    return (
        boolish(fields.get("performance_claim_allowed")) is True
        or boolish(fields.get("claim_grade_requirements_met")) is True
        or str(fields.get("claim_gate_status", "")).strip().lower()
        in CLAIM_READY_STATUSES
        or str(fields.get("row_claim_level", "")).strip().lower()
        in CLAIM_READY_STATUSES
    )


def result_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return benchmark_result_rows(payload)


def artifact_environment(payload: dict[str, Any], manifest: dict[str, Any] | None) -> dict[str, Any]:
    environment = payload.get("environment")
    if isinstance(environment, dict):
        return environment
    benchmark_manifest = payload.get("benchmark_manifest")
    if isinstance(benchmark_manifest, dict) and isinstance(
        benchmark_manifest.get("environment"), dict
    ):
        return benchmark_manifest["environment"]
    if manifest and isinstance(manifest.get("environment"), dict):
        return manifest["environment"]
    return {}


def artifact_build_profile(payload: dict[str, Any]) -> dict[str, Any]:
    contract = payload.get("build_profile_contract")
    if isinstance(contract, dict):
        return contract
    manifest = payload.get("benchmark_manifest")
    if isinstance(manifest, dict):
        return manifest
    return payload


def row_missing_fields(
    row: dict[str, Any],
    environment: dict[str, Any],
    build_profile: dict[str, Any],
    claim_bearing: bool,
) -> list[str]:
    fields = merged_row_fields(row)
    fallback = boolish(fields.get("fallback_attempted"))
    external = boolish(fields.get("external_engine_invoked"))
    external_baseline = boolish(fields.get("external_baseline_only"))
    is_shardloom = is_shardloom_row(fields)
    cold_lane_status = str(fields.get("cold_lane_timing_split_status", "")).strip()
    field_present = {
        "benchmark_result_row": first_present(
            fields, ("engine", "scenario", "scenario_name", "scenario_id")
        ),
        "route_identity": first_present(
            fields,
            (
                "route_lane_id",
                "route_display_name",
                "start_state",
                "end_state",
            ),
        ),
        "route_runtime_status": first_present(fields, ("route_runtime_status",)),
        "dataset_source_admission": first_present(
            fields,
            (
                "source_kind",
                "source_format",
                "storage_format",
                "dataset_profile",
                "dataset",
            ),
        ),
        "preparation_route": first_present(
            fields,
            (
                "ingress_route",
                "vortex_ingest_status",
                "prepared_state_status",
                "requested_execution_mode",
                "selected_execution_mode",
            ),
        ),
        "execution_route": first_present(
            fields,
            (
                "execution_route_label",
                "selected_execution_mode",
                "operator_execution_class",
                "requested_execution_mode",
            ),
        ),
        "output_route": first_present(
            fields,
            (
                "output_route",
                "output_format",
                "output_plan_status",
                "result_sink_write_millis",
                "rows_materialized",
            ),
        ),
        "claim_readiness_boundary": (
            boolish(fields.get("performance_claim_allowed")) is False
            and boolish(fields.get("production_claim_allowed")) is False
            and boolish(fields.get("spark_replacement_claim_allowed")) is False
        ),
        "correctness_proof": first_present(
            fields,
            (
                "correctness_digest",
                "correctness_digest_stable",
                "native_io_certificate_status",
                "claim_gate_status",
            ),
        ),
        "hardware_profile": bool(environment),
        "build_profile": first_present(
            build_profile,
            (
                "shardloom_build_profile",
                "build_profile",
                "build_profile_kind",
                "shardloom_build_profile_kind",
            ),
        ),
        "cold_warm_state": first_present(fields, ("cache_mode", "cache_state")),
        "stage_timings": first_present(fields, STAGE_TIMING_FIELDS),
        "cold_lane_attribution": (
            cold_lane_status == "external_baseline_only"
            if not is_shardloom
            else cold_lane_status == "complete"
        ),
        "cost_unit_fields": first_present(
            fields,
            (
                "cost_proxy",
                "cost_unit",
                "cost_assumptions",
                "scale_benchmark_cost_unit",
            ),
        )
        or not claim_bearing,
        "no_fallback_proof": fallback is False and (external is False or not is_shardloom),
        "external_baseline_boundary": (not is_shardloom and external_baseline is True)
        or is_shardloom,
    }
    return [field for field in REQUIRED_FIELD_ORDER if not field_present[field]]


def validate_manifest(manifest: dict[str, Any] | None) -> list[str]:
    blockers: list[str] = []
    if manifest is None:
        blockers.append("missing benchmark manifest")
        return blockers
    for required in [
        "benchmark_constitution_schema_version",
        "benchmark_constitution_validator",
        "benchmark_constitution_required_field_order",
        "benchmark_constitution_claim_gate_status",
        "benchmark_constitution_performance_claim_allowed",
    ]:
        if required not in manifest:
            blockers.append(f"manifest missing {required}")
    if manifest.get("benchmark_constitution_schema_version") != CONSTITUTION_SCHEMA_VERSION:
        blockers.append("manifest benchmark_constitution_schema_version mismatch")
    if manifest.get("benchmark_constitution_performance_claim_allowed") is not False:
        blockers.append("manifest benchmark_constitution_performance_claim_allowed must be false")
    if manifest.get("performance_claim_allowed") is not False:
        blockers.append("manifest performance_claim_allowed must be false")
    return blockers


def is_default_public_site_manifest(path: Path) -> bool:
    try:
        return (
            path.resolve().relative_to(ROOT)
            == Path("website/assets/benchmarks/latest/manifest.json")
        )
    except ValueError:
        return False


def is_default_public_site_artifact(path: Path) -> bool:
    try:
        return (
            path.resolve().relative_to(ROOT)
            == Path("website/assets/benchmarks/latest/benchmark-results.json")
        )
    except ValueError:
        return False


def validate_artifact(
    path: Path,
    manifest: dict[str, Any] | None,
) -> tuple[list[str], list[dict[str, Any]]]:
    payload = load_json(path)
    if not isinstance(payload, dict):
        return [f"{path} must contain a JSON object"], []
    environment = artifact_environment(payload, manifest)
    build_profile = artifact_build_profile(payload)
    manifest_claims_allowed = False
    if manifest:
        manifest_claims_allowed = boolish(
            manifest.get("benchmark_constitution_performance_claim_allowed")
        ) is True or boolish(manifest.get("performance_claim_allowed")) is True
    blockers: list[str] = []
    row_reports: list[dict[str, Any]] = []
    for index, row in enumerate(result_rows(payload)):
        claim_bearing = is_claim_bearing(row, manifest_claims_allowed)
        missing = row_missing_fields(row, environment, build_profile, claim_bearing)
        if claim_bearing and missing:
            blockers.append(
                f"claim-bearing row {row_label(row, index)} missing {','.join(missing)}"
            )
        if is_shardloom_row(row):
            fields = merged_row_fields(row)
            if boolish(fields.get("fallback_attempted")) is not False:
                blockers.append(f"ShardLoom row {row_label(row, index)} lacks fallback_attempted=false")
            if boolish(fields.get("external_engine_invoked")) is not False:
                blockers.append(
                    f"ShardLoom row {row_label(row, index)} lacks external_engine_invoked=false"
                )
            if (
                str(row.get("status", "success")) == "success"
                and fields.get("cold_lane_timing_split_status") != "complete"
                and row_reports_claim_grade(fields)
            ):
                blockers.append(
                    f"claim-grade ShardLoom row {row_label(row, index)} lacks complete cold-lane timing split"
                )
        row_reports.append(
            {
                "row": row_label(row, index),
                "claim_bearing": claim_bearing,
                "missing_field_order": missing,
            }
        )
    return blockers, row_reports


def self_test() -> list[str]:
    row = {
        "engine": "shardloom",
        "scenario_name": "bad claim",
        "claim_gate_status": "claim_grade",
        "performance_claim_allowed": True,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    missing = row_missing_fields(row, {}, {}, is_claim_bearing(row, True))
    if not missing:
        return ["self-test did not reject missing claim-bearing benchmark row"]
    if "dataset_source_admission" not in missing or "stage_timings" not in missing:
        return [f"self-test rejected wrong missing fields: {missing}"]
    if "cold_lane_attribution" not in missing:
        return [f"self-test did not require cold-lane attribution: {missing}"]
    return []


def main() -> int:
    args = parse_args()
    manifest_path = resolve(args.manifest)
    manifest = load_json(manifest_path) if manifest_path.exists() else None
    artifact_paths = (
        [resolve(path) for path in args.artifact]
        if args.artifact
        else [ROOT / "website/assets/benchmarks/latest/benchmark-results.json"]
    )
    default_public_site_retired = (
        args.artifact is None
        and is_default_public_site_manifest(manifest_path)
        and all(is_default_public_site_artifact(path) for path in artifact_paths)
        and manifest is None
        and all(not path.exists() for path in artifact_paths)
    )
    blockers = (
        []
        if default_public_site_retired
        else validate_manifest(manifest if isinstance(manifest, dict) else None)
    )
    row_reports: list[dict[str, Any]] = []
    for artifact_path in artifact_paths:
        if not artifact_path.exists():
            if default_public_site_retired and is_default_public_site_artifact(artifact_path):
                continue
            blockers.append(f"missing artifact {artifact_path}")
            continue
        artifact_blockers, artifact_rows = validate_artifact(
            artifact_path,
            manifest if isinstance(manifest, dict) else None,
        )
        blockers.extend(artifact_blockers)
        row_reports.extend(artifact_rows)
    if args.self_test:
        blockers.extend(self_test())
    report = {
        "schema_version": SCHEMA_VERSION,
        "benchmark_constitution_schema_version": CONSTITUTION_SCHEMA_VERSION,
        "status": "blocked" if blockers else "passed",
        "manifest": str(manifest_path),
        "artifacts": [str(path) for path in artifact_paths],
        "required_field_order": list(REQUIRED_FIELD_ORDER),
        "row_count": len(row_reports),
        "claim_bearing_row_count": sum(1 for row in row_reports if row["claim_bearing"]),
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "artifact_status": (
            "retired_from_public_website"
            if default_public_site_retired
            else "present_or_requested"
        ),
        "public_benchmark_surface": (
            "clickbench_handoff"
            if default_public_site_retired
            else "website_published_benchmark_rows"
        ),
        "public_benchmark_url": (
            "https://benchmark.clickhouse.com/"
            if default_public_site_retired
            else None
        ),
        "blockers": blockers,
        "rows": row_reports,
    }
    output = resolve(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if blockers else 0


if __name__ == "__main__":
    raise SystemExit(main())
