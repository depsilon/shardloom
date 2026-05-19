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

from benchmarks.traditional_analytics.benchmark_registry import (  # noqa: E402
    MANIFEST_SCHEMA_VERSION,
    PROFILES,
    expected_lanes_for_profile,
    lane_required_for_profile,
)


SUMMARY_SCHEMA_VERSION = "shardloom.website.benchmark_evidence.v1"
DEFAULT_LATEST_DIR = ROOT / "website" / "assets" / "benchmarks" / "latest"
DEFAULT_WEBSITE_DATA = ROOT / "website" / "assets" / "data" / "benchmark-evidence.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--profile", choices=tuple(PROFILES), required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_LATEST_DIR)
    parser.add_argument("--website-data", type=Path, default=DEFAULT_WEBSITE_DATA)
    parser.add_argument(
        "--base-summary",
        type=Path,
        default=DEFAULT_WEBSITE_DATA,
        help="Existing website summary to preserve prepared/native batch evidence from.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def vortex_lane_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    selected = [
        row
        for row in rows
        if str(row.get("engine", "")).startswith("shardloom")
        and row.get("status") == "success"
    ]
    rendered = []
    for row in selected[:40]:
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


def published_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rendered = []
    for row in rows:
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        rendered.append(
            {
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
        )
    return rendered


def comparative_summary(
    artifact: dict[str, Any],
    rows: list[dict[str, Any]],
    source_path: Path,
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
        "comparative_dashboard": comparative_summary(artifact, rows, source_path),
        "benchmark_manifest": manifest,
        "claim_boundary": {
            "performance_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
            "production_sql_dataframe_claim_allowed": False,
            "production_object_store_lakehouse_foundry_claim_allowed": False,
            "scope": "promoted local benchmark artifact evidence only",
        },
    }
    write_json(results_path, summary)
    write_json(args.output_dir / "manifest.json", manifest)
    write_json(args.website_data, summary)
    print(args.output_dir / "manifest.json")
    return 0 if manifest["artifact_status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
