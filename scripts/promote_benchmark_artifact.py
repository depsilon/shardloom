#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Promote a local benchmark execution artifact into committed website data."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "python" / "src"))

from benchmarks.traditional_analytics.benchmark_registry import (  # noqa: E402
    LANES,
    MANIFEST_SCHEMA_VERSION,
    PROFILES,
    expected_lanes_for_profile,
    lane_required_for_profile,
)
from shardloom import validate_runtime_execution_fields  # noqa: E402


SUMMARY_SCHEMA_VERSION = "shardloom.website.benchmark_evidence.v1"
DEFAULT_LATEST_DIR = ROOT / "website" / "assets" / "benchmarks" / "latest"
DEFAULT_WEBSITE_DATA = ROOT / "website" / "assets" / "data" / "benchmark-evidence.json"
DEFAULT_PUBLIC_LATEST_DIR = ROOT / "website-public" / "assets" / "benchmarks" / "latest"
DEFAULT_PUBLIC_WEBSITE_DATA = (
    ROOT / "website-public" / "assets" / "data" / "benchmark-evidence.json"
)
DEFAULT_WEBSITE_SRC_DATA = ROOT / "website-src" / "src" / "data" / "benchmark-evidence.json"
DEFAULT_WEBSITE_SRC_MANIFEST = ROOT / "website-src" / "src" / "data" / "benchmark-manifest.json"
DEFAULT_BASE_SUMMARY = DEFAULT_PUBLIC_WEBSITE_DATA
BENCHMARK_PROFILE_ROSTER = ("full_local", "full_local_plus_spark")
EXTRA_PUBLISHED_KEY_FRAGMENTS = (
    "source_state",
    "prepared_state",
    "reuse",
    "native_io",
    "coverage",
    "unsupported",
    "blocker",
    "diagnostic",
    "certificate",
    "route",
    "timing_scope",
    "claim_boundary",
    "runtime_execution_validation",
    "runtime_execution",
    "materialization",
    "decode",
    "artifact",
)
PUBLISHED_METRIC_KEYS = (
    "source_state_id",
    "source_state_digest",
    "source_location",
    "source_state_materialization_layout",
    "source_state_runtime_consumption_layout",
    "prepared_state_id",
    "prepared_state_digest",
    "prepared_artifact_ref",
    "prepared_artifact_digest",
    "vortex_artifact_ref",
    "vortex_artifact_digest",
    "output_plan_id",
    "output_plan_digest",
    "sink_artifact_ref",
    "sink_artifact_digest",
    "data_decoded",
    "data_materialized",
    "materialization_required",
    "decode_required",
    "operator_temporary_materialization_used",
    "materialization_boundary_report_emitted",
    "representation_transition_summary",
    "execution_certificate_id",
    "execution_certificate_status",
    "runtime_execution_certificate_status",
    "runtime_execution_certificate_id",
    "runtime_execution_certificate_provider_kind",
    "runtime_execution_certificate_plan_ref",
    "runtime_fallback_attempted",
    "runtime_external_query_engine_invoked",
    "execution_certificate_ref",
    "execution_certificate_refs",
    "evidence_level_certificate_refs",
    "requested_evidence_level",
    "selected_evidence_level",
    "evidence_level",
    "prepared_vortex_scale_split_execution_certificate_status",
    "prepared_vortex_scale_split_execution_certificate_id",
    "compatibility_import_included",
    "preparation_included_in_timing",
    "runtime_execution_validation_schema_version",
    "runtime_execution_validation_status",
    "runtime_execution_validation_blocker_count",
    "runtime_execution_validation_missing_fields",
    "runtime_execution_validation_invalid_fields",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--profile", choices=tuple(PROFILES), required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_LATEST_DIR)
    parser.add_argument("--website-data", type=Path, default=DEFAULT_WEBSITE_DATA)
    parser.add_argument(
        "--public-output-dir",
        type=Path,
        default=DEFAULT_PUBLIC_LATEST_DIR,
        help="Astro public-dir benchmark bundle mirrored into the static build.",
    )
    parser.add_argument(
        "--public-website-data",
        type=Path,
        default=DEFAULT_PUBLIC_WEBSITE_DATA,
        help="Astro public-dir benchmark evidence data mirrored into the static build.",
    )
    parser.add_argument(
        "--website-src-data",
        type=Path,
        default=DEFAULT_WEBSITE_SRC_DATA,
        help="Astro import-time benchmark evidence data used by the benchmark page.",
    )
    parser.add_argument(
        "--website-src-manifest",
        type=Path,
        default=DEFAULT_WEBSITE_SRC_MANIFEST,
        help="Astro import-time benchmark manifest used by the benchmark page.",
    )
    parser.add_argument(
        "--base-summary",
        type=Path,
        default=DEFAULT_BASE_SUMMARY,
        help="Existing website summary to preserve prepared/native batch evidence from.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_json_once(paths: list[Path], payload: Any) -> None:
    seen: set[Path] = set()
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        write_json(path, payload)


def repo_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def git_sha() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def iteration_values(row: dict[str, Any]) -> list[float]:
    values = row.get("iteration_wall_time_millis")
    if isinstance(values, list):
        return [
            float(value)
            for value in values
            if isinstance(value, (int, float)) and float(value) > 0
        ]
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    for key in ("total_runtime_millis", "query_runtime_millis"):
        value = metrics.get(key)
        if isinstance(value, (int, float)) and float(value) > 0:
            return [float(value)]
    return []


def geomean(values: list[float]) -> float | None:
    positives = [value for value in values if value > 0]
    if not positives:
        return None
    return math.exp(sum(math.log(value) for value in positives) / len(positives))


def fmt_ms(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f} ms"


def fmt_percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}%"


def artifact_rows(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    rows = artifact.get("results")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def coverage_rows(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    rows = artifact.get("coverage_table")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def lane_versions(artifact: dict[str, Any]) -> dict[str, Any]:
    versions = artifact.get("engine_versions")
    return versions if isinstance(versions, dict) else {}


def available_lanes(artifact: dict[str, Any], rows: list[dict[str, Any]]) -> list[str]:
    lanes = {
        name
        for name, metadata in lane_versions(artifact).items()
        if isinstance(metadata, dict) and metadata.get("available") is True
    }
    lanes.update(str(row.get("engine")) for row in rows if row.get("engine"))
    if "shardloom-vortex" in lanes:
        lanes.add("native-vortex")
    return sorted(lanes)


def missing_reason(lane: str, artifact: dict[str, Any]) -> str:
    metadata = lane_versions(artifact).get(lane)
    if isinstance(metadata, dict):
        reason = metadata.get("reason") or metadata.get("availability_reason")
        if reason:
            return str(reason)
        if metadata.get("available") is False:
            return "lane marked unavailable in benchmark artifact"
    return "not present in promoted benchmark artifact"


def lane_reason(lane: str, artifact: dict[str, Any]) -> str:
    if lane == "native-vortex":
        return "alias vocabulary for promoted shardloom-vortex/native_vortex evidence"
    metadata = lane_versions(artifact).get(lane)
    if isinstance(metadata, dict):
        version = metadata.get("version")
        if version:
            return f"available, version {version}"
    return "available in promoted benchmark artifact"


def scenario_key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row.get("storage_format", "")), str(row.get("scenario_name", "")))


def engine_timing_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_engine: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        engine = row.get("engine")
        if engine:
            by_engine[str(engine)].append(row)

    row_times: dict[tuple[str, str, str], float] = {}
    for row in rows:
        if row.get("status") != "success":
            continue
        value = geomean(iteration_values(row))
        if value is not None:
            row_times[(str(row.get("engine")), *scenario_key(row))] = value

    fastest = Counter()
    for fmt, scenario in sorted({key[1:] for key in row_times}):
        candidates = {
            engine: value
            for (engine, candidate_fmt, candidate_scenario), value in row_times.items()
            if candidate_fmt == fmt and candidate_scenario == scenario
        }
        if candidates:
            fastest[min(candidates, key=candidates.get)] += 1

    shardloom_geomean = geomean(
        [
            value
            for (engine, _fmt, _scenario), value in row_times.items()
            if engine == "shardloom"
        ]
    )
    rendered_rows: list[list[Any]] = []
    for engine, engine_rows in by_engine.items():
        successes = [row for row in engine_rows if row.get("status") == "success"]
        values = [
            value
            for (candidate, _fmt, _scenario), value in row_times.items()
            if candidate == engine
        ]
        csv_parquet_values = [
            value
            for (candidate, fmt, _scenario), value in row_times.items()
            if candidate == engine and fmt in {"csv", "parquet"}
        ]
        gm = geomean(values)
        relative = (gm / shardloom_geomean * 100.0) if gm and shardloom_geomean else None
        rendered_rows.append(
            [
                engine,
                "yes",
                f"{len(successes)}/{len(engine_rows)}",
                fmt_ms(gm),
                fmt_ms(geomean(csv_parquet_values)),
                fastest[engine],
                fmt_percent(relative),
            ]
        )
    return {
        "heading": "Local Timing Context",
        "headers": [
            "Engine",
            "Available",
            "Success / total",
            "Geomean",
            "CSV/Parquet geomean",
            "local fastest count",
            "local timing context",
        ],
        "rows": rendered_rows,
    }


def claim_gate_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(row.get("claim_gate_status", "unknown")) for row in rows)
    total = sum(counts.values()) or 1
    return {
        "heading": "Claim-Gate Distribution",
        "headers": ["Claim gate", "Rows", "Share"],
        "rows": [
            [gate, count, f"{count / total * 100.0:.1f}%"]
            for gate, count in counts.most_common()
        ],
    }


def format_coverage_table(artifact: dict[str, Any], rows: list[dict[str, Any]], profile: str) -> dict[str, Any]:
    profile_spec = PROFILES[profile]
    required = set(profile_spec.required_formats)
    optional = set(profile_spec.optional_formats)
    expected = list(dict.fromkeys([*profile_spec.required_formats, *profile_spec.optional_formats]))
    available = {
        str(value)
        for value in artifact.get("format_order", [])
        if isinstance(value, str) and value
    }
    available.update(
        str(row.get("storage_format"))
        for row in rows
        if row.get("storage_format")
    )
    counts = Counter(str(row.get("storage_format")) for row in rows if row.get("storage_format"))
    return {
        "heading": "Format Coverage",
        "headers": ["Format", "Profile role", "Status", "Rows", "Reason"],
        "rows": [
            [
                fmt,
                "required" if fmt in required else "optional",
                "available" if fmt in available else "missing_optional" if fmt in optional else "missing_required",
                counts[fmt],
                (
                    "published benchmark rows include this format"
                    if fmt in available
                    else "format is expected by the profile but absent from the promoted artifact"
                ),
            ]
            for fmt in expected
        ],
    }


def profile_lane_availability_table(
    artifact: dict[str, Any],
    rows: list[dict[str, Any]],
    active_profile: str,
) -> dict[str, Any]:
    available = set(available_lanes(artifact, rows))
    active_expected = set(expected_lanes_for_profile(active_profile))
    rendered_rows: list[list[Any]] = []
    for profile in BENCHMARK_PROFILE_ROSTER:
        profile_expected = expected_lanes_for_profile(profile)
        for lane in profile_expected:
            required = lane_required_for_profile(profile, lane)
            lane_meta = LANES.get(lane)
            if lane in available:
                status = "available"
                reason = lane_reason(lane, artifact)
            elif lane in active_expected:
                status = "missing_required" if lane_required_for_profile(active_profile, lane) else "missing_optional"
                reason = missing_reason(lane, artifact)
            else:
                status = "not_requested_by_current_profile"
                reason = f"run benchmark profile {profile} to publish this lane"
            rendered_rows.append(
                [
                    profile,
                    lane,
                    "required" if required else "optional",
                    lane_meta.group if lane_meta else "unknown",
                    status,
                    reason,
                ]
            )
    return {
        "heading": "Profile Lane Availability",
        "headers": ["Profile", "Lane", "Profile role", "Lane group", "Status", "Version / reason"],
        "rows": rendered_rows,
    }


def claim_grade_closeout_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    shardloom_rows = [
        row for row in rows if str(row.get("engine", "")).startswith("shardloom")
    ]
    counts = Counter(str(row.get("claim_gate_status", "unknown")) for row in shardloom_rows)
    blockers = counts["blocked"] + counts["unsupported"] + counts["not_claim_grade"] + counts["fixture_smoke_only"]
    return {
        "heading": "ShardLoom Claim-Grade Closeout",
        "headers": ["Scope", "Current rows", "Target", "Owning plan item"],
        "rows": [
            [
                "ShardLoom runtime rows",
                f"{len(shardloom_rows)} rows; {blockers} not claim-grade/blocked/unsupported/fixture rows",
                "claim_grade for every admitted row in the published comparative profile",
                "GAR-RUNTIME-IMPL-5J",
            ],
            [
                "External baseline rows",
                "external_baseline_only rows remain comparison context",
                "visible baseline-only rows; never fallback execution",
                "GAR-BENCH-PUB-1 / GAR-RUNTIME-IMPL-5J",
            ],
            [
                "Unsupported or blocked rows",
                f"{counts['blocked'] + counts['unsupported']} ShardLoom rows",
                "implemented, claim-gated, or moved to an explicit non-comparative gap appendix",
                "GAR-RUNTIME-IMPL-5J",
            ],
        ],
    }


def vortex_lane_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    selected = [
        row
        for row in rows
        if str(row.get("engine", "")).startswith("shardloom")
        and row.get("status") == "success"
    ]
    rendered = []
    for row in selected:
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        rendered.append(
            [
                row.get("engine"),
                row.get("storage_format"),
                row.get("scenario_name"),
                row.get("selected_execution_mode"),
                row.get("claim_gate_status"),
                fmt_ms(geomean(iteration_values(row))),
                metrics.get("vortex_scan_millis", "n/a"),
                metrics.get("operator_compute_millis", "n/a"),
                row.get("fallback_attempted", False),
                row.get("external_engine_invoked", False),
            ]
        )
    return {
        "heading": "Vortex-Oriented Lanes By Source Format",
        "headers": [
            "Engine",
            "Source format",
            "Scenario",
            "Execution mode",
            "Claim gate",
            "Local row time",
            "Vortex scan ms",
            "Operator ms",
            "Fallback",
            "External engine",
        ],
        "rows": rendered,
    }


def runtime_validation_field_map(row: dict[str, Any]) -> dict[str, Any]:
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
        fields["preparation_included"] = row.get("compatibility_import_included") is True
    return fields


def runtime_validation_surface_id(row: dict[str, Any]) -> str:
    scenario = str(row.get("scenario_id") or row.get("scenario_name") or "unknown")
    scenario = scenario.lower().replace(" ", "_").replace(":", "_")
    return (
        "promoted_benchmark."
        f"{row.get('engine', 'unknown')}."
        f"{row.get('storage_format', 'unknown')}."
        f"{scenario}"
    )


def should_validate_runtime_row(row: dict[str, Any]) -> bool:
    return str(row.get("engine", "")).startswith("shardloom")


def runtime_validation_for_row(row: dict[str, Any]) -> dict[str, Any] | None:
    if not should_validate_runtime_row(row):
        return None
    existing = row.get("runtime_execution_validation")
    if isinstance(existing, dict) and existing.get("status") == "passed":
        return existing
    status = str(row.get("status", "unknown"))
    runtime_expected = status == "success"
    validation = validate_runtime_execution_fields(
        runtime_validation_field_map(row),
        command="promoted-benchmark-row",
        status=status,
        surface_id=runtime_validation_surface_id(row),
        runtime_expected=runtime_expected,
        execution_mode=str(row.get("selected_execution_mode") or "") or None,
    )
    if validation.status != "passed":
        raise RuntimeError(
            f"{row.get('engine', 'unknown')} "
            f"{row.get('scenario_name', 'unknown')} failed runtime validation: "
            + "; ".join(validation.blockers)
        )
    return validation.as_dict()


def runtime_validation_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    reports = [
        report
        for row in rows
        for report in [runtime_validation_for_row(row)]
        if isinstance(report, dict)
    ]
    counts = Counter(str(report.get("status", "missing")) for report in reports)
    return {
        "heading": "Runtime Envelope Validation",
        "headers": ["Status", "Rows"],
        "rows": [[status, count] for status, count in sorted(counts.items())],
        "schema_version": "shardloom.website.runtime_envelope_validation.v1",
        "validator_schema_version": "shardloom.runtime_execution_envelope_validation.v1",
        "status": "passed" if counts.get("blocked", 0) == 0 else "blocked",
        "validated_row_count": len(reports),
        "validated_surfaces": [
            report.get("surface_id")
            for report in reports
            if isinstance(report.get("surface_id"), str)
        ],
    }


def published_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rendered = []
    for row in rows:
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        runtime_fields = runtime_validation_field_map(row)
        runtime_validation = runtime_validation_for_row(row)
        rendered_row = {
            "engine": row.get("engine"),
            "status": row.get("status"),
            "scenario_name": row.get("scenario_name"),
            "scenario_id": row.get("scenario_id"),
            "storage_format": row.get("storage_format"),
            "selected_execution_mode": row.get("selected_execution_mode"),
            "requested_execution_mode": row.get("requested_execution_mode"),
            "claim_gate_status": row.get("claim_gate_status"),
            "external_baseline_only": row.get("external_baseline_only"),
            "fallback_attempted": row.get("fallback_attempted", False),
            "external_engine_invoked": row.get("external_engine_invoked", False),
            "iteration_wall_time_millis": row.get("iteration_wall_time_millis"),
            "query_runtime_millis": metrics.get("query_runtime_millis"),
            "total_runtime_millis": metrics.get("total_runtime_millis"),
            "source_read_millis": metrics.get("source_read_millis"),
            "compatibility_parse_millis": metrics.get("compatibility_parse_millis"),
            "compatibility_to_vortex_import_millis": metrics.get(
                "compatibility_to_vortex_import_millis"
            ),
            "vortex_write_millis": metrics.get("vortex_write_millis"),
            "vortex_reopen_millis": metrics.get("vortex_reopen_millis"),
            "vortex_scan_millis": metrics.get("vortex_scan_millis"),
            "operator_compute_millis": metrics.get("operator_compute_millis"),
            "result_sink_write_millis": metrics.get("result_sink_write_millis"),
            "evidence_render_millis": metrics.get("evidence_render_millis"),
        }
        if runtime_validation is not None:
            rendered_row["runtime_execution_validation"] = runtime_validation
            rendered_row["runtime_execution_validation_status"] = (
                runtime_validation.get("status")
            )
            rendered_row["runtime_execution_validation_schema_version"] = (
                runtime_validation.get("schema_version")
            )
            rendered_row["runtime_claim_allowed"] = runtime_validation.get(
                "runtime_claim_allowed"
            )
        for key in PUBLISHED_METRIC_KEYS:
            if key in runtime_fields:
                rendered_row[key] = runtime_fields[key]
        for key, value in row.items():
            if key in rendered_row:
                continue
            if any(fragment in key for fragment in EXTRA_PUBLISHED_KEY_FRAGMENTS):
                rendered_row[key] = value
        for key, value in metrics.items():
            if key in rendered_row:
                continue
            if any(fragment in key for fragment in EXTRA_PUBLISHED_KEY_FRAGMENTS):
                rendered_row[key] = value
        rendered.append(rendered_row)
    return rendered


def comparative_summary(
    artifact: dict[str, Any],
    rows: list[dict[str, Any]],
    source_path: Path,
    profile: str,
) -> dict[str, Any]:
    dataset = artifact.get("dataset") if isinstance(artifact.get("dataset"), dict) else {}
    generated = artifact.get("generated_at_utc") or datetime.now(timezone.utc).isoformat()
    return {
        "source": repo_relative(source_path),
        "generated": f"{generated} from promoted local benchmark artifact.",
        "cards": [
            {"label": "Rows", "value": str(len(rows))},
            {"label": "Coverage Rows", "value": str(len(coverage_rows(artifact)))},
            {"label": "Formats", "value": str(len(artifact.get("format_order", [])))},
            {
                "label": "Performance Claim",
                "value": str(bool(artifact.get("performance_claim_allowed", False))),
            },
        ],
        "engine_timing_overview": engine_timing_table(rows),
        "vortex_oriented_lanes": vortex_lane_table(rows),
        "claim_gate_distribution": claim_gate_table(rows),
        "runtime_envelope_validation": runtime_validation_table(rows),
        "profile_lane_availability": profile_lane_availability_table(
            artifact, rows, profile
        ),
        "format_coverage": format_coverage_table(
            artifact, rows, profile
        ),
        "claim_grade_closeout": claim_grade_closeout_table(rows),
        "missing_baselines": [],
        "dataset_rows": dataset.get("rows"),
        "claim_boundary": (
            "promoted local benchmark artifact only; not public performance, "
            "superiority, Spark-displacement, or best-default evidence"
        ),
    }


def manifest_for_artifact(
    artifact: dict[str, Any],
    rows: list[dict[str, Any]],
    profile: str,
    results_path: Path,
) -> dict[str, Any]:
    expected = list(expected_lanes_for_profile(profile))
    available = available_lanes(artifact, rows)
    missing = [lane for lane in expected if lane not in available]
    missing_required = [
        lane for lane in missing if lane_required_for_profile(profile, lane)
    ]
    reasons = {lane: lane_reason(lane, artifact) for lane in available}
    for lane in missing:
        reasons[lane] = missing_reason(lane, artifact)
    versions = {}
    for lane in available:
        metadata = lane_versions(artifact).get(lane)
        if isinstance(metadata, dict) and metadata.get("version"):
            versions[lane] = metadata["version"]
        else:
            versions[lane] = "from promoted benchmark artifact"

    artifact_paths = {
        "json": repo_relative(results_path),
        "markdown": None,
        "html": None,
    }
    runtime_validation = runtime_validation_table(rows)
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": artifact.get("generated_at_utc")
        or datetime.now(timezone.utc).isoformat(),
        "benchmark_profile": profile,
        "benchmark_git_sha": git_sha(),
        "shardloom_git_sha": git_sha(),
        "artifact_status": "incomplete" if missing_required else "complete",
        "expected_lanes": expected,
        "available_lanes": available,
        "missing_lanes": missing,
        "missing_required_lanes": missing_required,
        "lane_versions": versions,
        "lane_availability_reasons": reasons,
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "artifact_environment": artifact.get("environment", {}),
            "website_promoter": "scripts/promote_benchmark_artifact.py",
        },
        "claim_boundary": PROFILES[profile].claim_boundary,
        "performance_claim_allowed": False,
        "benchmark_constitution_schema_version": "shardloom.benchmark_constitution_validation.v1",
        "benchmark_constitution_validator": "scripts/check_benchmark_constitution.py",
        "benchmark_constitution_required_field_order": [
            "benchmark_result_row",
            "dataset_source_admission",
            "preparation_route",
            "execution_route",
            "output_route",
            "correctness_proof",
            "hardware_profile",
            "build_profile",
            "cold_warm_state",
            "stage_timings",
            "cost_unit_fields",
            "no_fallback_proof",
            "external_baseline_boundary",
        ],
        "benchmark_constitution_claim_gate_status": "not_claim_grade",
        "benchmark_constitution_performance_claim_allowed": False,
        "runtime_envelope_validation": runtime_validation,
        "artifact_paths": artifact_paths,
    }


def main() -> int:
    args = parse_args()
    source_path = args.input.resolve()
    artifact = load_json(source_path)
    rows = artifact_rows(artifact)
    if not rows:
        raise SystemExit("benchmark artifact has no results rows")

    base: dict[str, Any] = {}
    if args.base_summary.exists():
        existing = load_json(args.base_summary)
        if isinstance(existing, dict):
            base = existing

    args.output_dir.mkdir(parents=True, exist_ok=True)
    results_path = args.output_dir / "benchmark-results.json"

    manifest = manifest_for_artifact(
        artifact,
        rows,
        args.profile,
        results_path,
    )
    summary = {
        **base,
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "benchmark_profile": args.profile,
        "published_benchmark_artifact": {
            "source": repo_relative(source_path),
            "generated_at_utc": artifact.get("generated_at_utc"),
            "schema_version": artifact.get("schema_version"),
            "engine_order": artifact.get("engine_order", []),
            "format_order": artifact.get("format_order", []),
            "scenario_order": artifact.get("scenario_order", []),
        },
        "published_benchmark_rows": published_rows(rows),
        "comparative_dashboard": comparative_summary(artifact, rows, source_path, args.profile),
        "benchmark_manifest": manifest,
        "claim_boundary": {
            "performance_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
            "production_sql_dataframe_claim_allowed": False,
            "production_object_store_lakehouse_foundry_claim_allowed": False,
            "scope": "promoted local benchmark artifact evidence only",
        },
    }
    write_json_once(
        [
            results_path,
            args.public_output_dir / "benchmark-results.json",
            args.website_data,
            args.public_website_data,
            args.website_src_data,
        ],
        summary,
    )
    write_json_once(
        [
            args.output_dir / "manifest.json",
            args.public_output_dir / "manifest.json",
            args.website_src_manifest,
        ],
        manifest,
    )
    print(args.output_dir / "manifest.json")
    return 0 if manifest["artifact_status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
