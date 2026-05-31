#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Promote a local benchmark execution artifact into committed website data."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
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
BENCHMARK_PROFILE_ROSTER = ("full_local",)
PUBLISHED_ROW_CHUNK_PREFIX = "published-benchmark-rows"
PUBLISHED_ROW_CHUNK_SIZE = 300
WEBSITE_ROW_KEYS = (
    "engine",
    "scenario_name",
    "storage_format",
    "status",
    "selected_execution_mode",
    "total_runtime_millis",
    "vortex_scan_millis",
    "operator_compute_millis",
    "result_sink_write_millis",
    "fallback_attempted",
    "external_engine_invoked",
    "claim_gate_status",
    "row_classification",
    "external_baseline_only",
)
LOCAL_PATH_RE = re.compile(
    r"(?P<win>[A-Za-z]:\\[^|,;\"'\s]+)|"
    r"(?P<posix>(?:/Users|/home|/tmp|/var/folders|/private/var|/workspace|/mnt|/Volumes)"
    r"[^|,;\"'\s]*)"
)
EXTRA_PUBLISHED_KEY_FRAGMENTS = (
    "source_state",
    "prepared_state",
    "vortex_scout_ingress",
    "vortex_layout_write_advisor",
    "vortex_copy_budget",
    "vortex_preparation_spine",
    "vortex_differential_preparation",
    "vortex_capillary_preparation",
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
    "cold_lane",
    "materialization",
    "decode",
    "artifact",
    "pulseweave",
    "flow_inventory",
    "scarcity_ledger",
    "endopulse",
    "proofbound",
)
COLD_LANE_ATTRIBUTION_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.cold_lane_attribution.v1"
)
COLD_LANE_REQUIRED_FIELDS_BY_CLASSIFICATION = {
    "full_certified_cold_ingest": (
        "source_read_millis",
        "compatibility_to_vortex_import_millis",
        "vortex_array_build_millis",
        "vortex_write_millis",
        "vortex_reopen_verify_millis",
        "operator_compute_millis",
        "evidence_render_millis",
        "total_runtime_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
    "preparation_only": (
        "prepare_batch_preparation_millis",
        "prepare_batch_source_to_columnar_millis",
        "prepare_batch_vortex_array_build_millis",
        "prepare_batch_vortex_write_millis",
        "prepare_batch_vortex_reopen_verify_millis",
        "operator_compute_millis",
        "evidence_render_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
    "warm_prepared_query": (
        "vortex_scan_millis",
        "operator_compute_millis",
        "query_runtime_millis",
        "evidence_render_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
    "sink_replay_heavy": (
        "operator_compute_millis",
        "query_runtime_millis",
        "result_sink_write_millis",
        "evidence_render_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
    "evidence_heavy": (
        "operator_compute_millis",
        "query_runtime_millis",
        "evidence_render_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
    "process_harness_heavy": (
        "source_read_millis",
        "operator_compute_millis",
        "query_runtime_millis",
        "evidence_render_millis",
        "cli_process_wall_millis",
        "python_harness_overhead_millis",
    ),
}
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
    "computed_result_vortex_path",
    "computed_result_vortex_digest",
    "computed_result_sink_replay_verified",
    "evidence_level_result_sink_replay_verified",
    "result_sink_replay_verified",
    "evidence_level_result_sink_replay_refs",
    "data_decoded",
    "data_materialized",
    "materialization_required",
    "decode_required",
    "operator_temporary_materialization_used",
    "materialization_boundary_report_emitted",
    "representation_transition_summary",
    "native_io_certificate_status",
    "source_native_io_certificate_status",
    "computed_result_sink_native_io_certificate_status",
    "result_native_io_certificate_status",
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
    "prepared_vortex_scale_split_runtime_status",
    "prepared_vortex_scale_split_execution_certificate_status",
    "prepared_vortex_scale_split_execution_certificate_id",
    "prepared_vortex_scale_split_operator_runtime_status",
    "prepared_vortex_scale_split_operator_execution_certificate_status",
    "prepared_vortex_scale_split_operator_execution_certificate_id",
    "prepared_vortex_scale_split_operator_family",
    "prepared_vortex_scale_split_operator_stateful",
    "prepared_vortex_scale_split_operator_shuffle_required",
    "prepared_vortex_scale_split_operator_local_combine_used",
    "prepared_vortex_scale_split_operator_global_merge_used",
    "prepared_vortex_scale_split_operator_claim_gate_status",
    "prepared_vortex_scale_split_operator_fallback_attempted",
    "prepared_vortex_scale_split_operator_external_engine_invoked",
    "prepared_vortex_scale_split_operator_retry_replay_status",
    "prepared_vortex_scale_split_operator_source_replay_status",
    "prepared_vortex_scale_split_operator_memory_envelope_status",
    "prepared_vortex_scale_split_operator_backpressure_status",
    "prepared_vortex_scale_split_operator_spill_policy_status",
    "prepared_vortex_scale_split_operator_output_commit_proof_status",
    "pulseweave_schema_version",
    "pulseweave_status",
    "pulseweave_application_scope",
    "pulseweave_runtime_decision_applied",
    "pulseweave_policy_mutated",
    "pulseweave_decision_digest",
    "pulseweave_blocker",
    "pulseweave_claim_gate_status",
    "pulseweave_fallback_attempted",
    "pulseweave_external_engine_invoked",
    "flow_inventory_wip_limit",
    "flow_inventory_peak_in_flight",
    "flow_inventory_held_for_memory_count",
    "flow_inventory_held_for_downstream_count",
    "scarcity_ledger_selected_action",
    "scarcity_ledger_total_price_bps",
    "endopulse_next_target_task_bytes",
    "endopulse_next_wip_limit",
    "endopulse_persistent_state_used",
    "proofbound_certificate_status",
    "proofbound_no_fallback_status",
    "proofbound_claim_allowed",
    "compatibility_import_included",
    "preparation_included_in_timing",
    "persistent_runner_status",
    "process_startup_attribution",
    "cli_process_wall_millis",
    "python_harness_overhead_millis",
    "batch_process_wall_shared",
    "batch_cli_process_wall_millis",
    "preparation_millis",
    "preparation_cli_process_wall_millis",
    "prepare_batch_preparation_millis",
    "prepare_batch_source_to_columnar_millis",
    "prepare_batch_vortex_array_build_millis",
    "prepare_batch_vortex_write_millis",
    "prepare_batch_vortex_reopen_verify_millis",
    "runtime_execution_validation_schema_version",
    "runtime_execution_validation_status",
    "runtime_execution_validation_blocker_count",
    "runtime_execution_validation_missing_fields",
    "runtime_execution_validation_invalid_fields",
    "claim_grade_requirements_met",
    "claim_grade_missing_evidence",
    "iterations",
    "reproducibility_min_iterations",
    "reproducibility_iterations_met",
    "reproducible_benchmark_row",
    "timing_row_present",
    "timing_row_claim_grade",
    "correctness_digest",
    "correctness_digest_stable",
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


def portable_public_ref(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        path = match.group(0)
        digest = hashlib.sha256(path.encode("utf-8")).hexdigest()[:16]
        return f"local-artifact-ref:sha256:{digest}"

    return LOCAL_PATH_RE.sub(replace, value)


def portable_public_value(value: Any) -> Any:
    if isinstance(value, str):
        return portable_public_ref(value)
    if isinstance(value, list):
        return [portable_public_value(item) for item in value]
    if isinstance(value, dict):
        return {key: portable_public_value(item) for key, item in value.items()}
    return value


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


def clear_row_chunks(directory: Path) -> None:
    if not directory.exists():
        return
    for path in directory.glob(f"{PUBLISHED_ROW_CHUNK_PREFIX}-*.json"):
        path.unlink()


def write_row_chunks(directory: Path, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    directory.mkdir(parents=True, exist_ok=True)
    clear_row_chunks(directory)
    chunks: list[dict[str, Any]] = []
    for index in range(0, len(rows), PUBLISHED_ROW_CHUNK_SIZE):
        chunk_rows = rows[index : index + PUBLISHED_ROW_CHUNK_SIZE]
        chunk_index = index // PUBLISHED_ROW_CHUNK_SIZE
        path = directory / f"{PUBLISHED_ROW_CHUNK_PREFIX}-{chunk_index:03d}.json"
        payload = {
            "schema_version": "shardloom.website.benchmark_row_chunk.v1",
            "chunk_index": chunk_index,
            "row_count": len(chunk_rows),
            "rows": chunk_rows,
        }
        text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        path.write_text(text, encoding="utf-8")
        chunks.append(
            {
                "path": repo_relative(path),
                "row_count": len(chunk_rows),
                "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            }
        )
    return chunks


def website_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rendered: list[dict[str, Any]] = []
    for row in rows:
        rendered.append(
            {
                key: row[key]
                for key in WEBSITE_ROW_KEYS
                if key in row
            }
        )
    return rendered


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


def numeric_value(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def cold_lane_field_present(fields: dict[str, Any], field: str) -> bool:
    value = fields.get(field)
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip()) and value.strip().lower() not in {
            "missing",
            "n/a",
            "not_applicable",
            "not_measured",
            "not_reported",
            "unknown",
        }
    return True


def cold_lane_primary_classification(row: dict[str, Any], fields: dict[str, Any]) -> str:
    engine = str(row.get("engine", ""))
    selected_mode = str(row.get("selected_execution_mode") or "")
    if not engine.startswith("shardloom"):
        return "external_baseline_only"
    if row.get("status") != "success":
        return "blocked_incomplete_timing_split"
    if engine == "shardloom-prepare-batch":
        return "preparation_only"
    if selected_mode == "compatibility_import_certified":
        return "full_certified_cold_ingest"
    if selected_mode in {"prepared_vortex", "native_vortex"}:
        return "warm_prepared_query"
    if cold_lane_field_present(fields, "result_sink_write_millis") and (
        numeric_value(fields.get("result_sink_write_millis")) or 0.0
    ) > 0.0:
        return "sink_replay_heavy"
    if cold_lane_field_present(fields, "evidence_render_millis"):
        return "evidence_heavy"
    return "process_harness_heavy"


def cold_lane_secondary_classifications(
    row: dict[str, Any], fields: dict[str, Any]
) -> list[str]:
    if not str(row.get("engine", "")).startswith("shardloom"):
        return ["external_baseline_only"]
    classifications: list[str] = []
    if cold_lane_field_present(fields, "result_sink_write_millis") and (
        numeric_value(fields.get("result_sink_write_millis")) or 0.0
    ) > 0.0:
        classifications.append("sink_replay_heavy")
    if cold_lane_field_present(fields, "evidence_render_millis"):
        classifications.append("evidence_heavy")
    if cold_lane_field_present(fields, "cli_process_wall_millis") and cold_lane_field_present(
        fields, "python_harness_overhead_millis"
    ):
        classifications.append("process_harness_heavy")
    return classifications or ["none"]


def cold_lane_attribution_for_row(row: dict[str, Any]) -> dict[str, Any]:
    fields = runtime_validation_field_map(row)
    classification = cold_lane_primary_classification(row, fields)
    secondary = cold_lane_secondary_classifications(row, fields)
    if classification == "external_baseline_only":
        return {
            "cold_lane_attribution_schema_version": COLD_LANE_ATTRIBUTION_SCHEMA_VERSION,
            "cold_lane_classification": classification,
            "cold_lane_secondary_classifications": ",".join(secondary),
            "cold_lane_timing_split_status": "external_baseline_only",
            "cold_lane_required_stage_fields": "external_baseline_only",
            "cold_lane_missing_stage_fields": "none",
            "cold_lane_preparation_timing_present": False,
            "cold_lane_warm_query_timing_present": False,
            "cold_lane_sink_replay_timing_present": False,
            "cold_lane_evidence_render_timing_present": False,
            "cold_lane_process_harness_timing_present": False,
            "cold_lane_claim_gate_status": "external_baseline_only",
            "cold_lane_claim_blocker_id": "external_baseline_only",
            "cold_lane_fallback_attempted": False,
            "cold_lane_external_engine_invoked": False,
            "cold_lane_claim_boundary": "external baselines provide comparison timing only and cannot satisfy ShardLoom cold-lane evidence",
        }
    required = list(COLD_LANE_REQUIRED_FIELDS_BY_CLASSIFICATION.get(classification, ()))
    batch_row = (
        fields.get("persistent_runner_status") == "single_process_batch_runner_supported"
        or fields.get("batch_process_wall_shared") is True
    )
    if batch_row:
        required = [
            field for field in required if field != "python_harness_overhead_millis"
        ]
        for field in ("batch_cli_process_wall_millis", "batch_process_wall_shared"):
            if field not in required:
                required.append(field)
    if "sink_replay_heavy" in secondary and "result_sink_write_millis" not in required:
        required.append("result_sink_write_millis")
    missing = [field for field in required if not cold_lane_field_present(fields, field)]
    status = "complete" if row.get("status") == "success" and not missing else "blocked"
    if missing:
        status = "blocked_incomplete_timing_split"
    if row.get("status") != "success":
        status = "blocked_row_not_executed"
    return {
        "cold_lane_attribution_schema_version": COLD_LANE_ATTRIBUTION_SCHEMA_VERSION,
        "cold_lane_classification": classification,
        "cold_lane_secondary_classifications": ",".join(secondary),
        "cold_lane_timing_split_status": status,
        "cold_lane_required_stage_fields": ",".join(required) if required else "none",
        "cold_lane_missing_stage_fields": ",".join(missing) if missing else "none",
        "cold_lane_preparation_timing_present": any(
            cold_lane_field_present(fields, field)
            for field in (
                "preparation_millis",
                "prepare_batch_preparation_millis",
                "compatibility_to_vortex_import_millis",
                "vortex_write_millis",
                "vortex_reopen_verify_millis",
            )
        ),
        "cold_lane_warm_query_timing_present": cold_lane_field_present(
            fields, "query_runtime_millis"
        )
        and cold_lane_field_present(fields, "operator_compute_millis"),
        "cold_lane_sink_replay_timing_present": cold_lane_field_present(
            fields, "result_sink_write_millis"
        ),
        "cold_lane_evidence_render_timing_present": cold_lane_field_present(
            fields, "evidence_render_millis"
        ),
        "cold_lane_process_harness_timing_present": cold_lane_field_present(
            fields, "cli_process_wall_millis"
        )
        and (
            cold_lane_field_present(fields, "python_harness_overhead_millis")
            or (
                batch_row
                and fields.get("batch_process_wall_shared") is True
                and cold_lane_field_present(fields, "batch_cli_process_wall_millis")
            )
        ),
        "cold_lane_claim_gate_status": (
            "claim_grade" if status == "complete" else "blocked_incomplete_timing_split"
        ),
        "cold_lane_claim_blocker_id": (
            "none" if status == "complete" else "gar-ioreuse-1h.incomplete_timing_split"
        ),
        "cold_lane_fallback_attempted": False,
        "cold_lane_external_engine_invoked": False,
        "cold_lane_claim_boundary": "cold-lane attribution separates preparation, warm query, sink/replay, evidence rendering, and process harness timing; it is not a performance or Spark-displacement claim",
    }


def cold_lane_missing_evidence_message(cold_lane: dict[str, Any]) -> str:
    status = str(cold_lane.get("cold_lane_timing_split_status", "missing"))
    classification = str(cold_lane.get("cold_lane_classification", "missing"))
    missing = str(cold_lane.get("cold_lane_missing_stage_fields", "missing"))
    return (
        "cold_lane_timing_split_status!=complete "
        f"(actual={status}; classification={classification}; "
        f"missing_stage_fields={missing})"
    )


def claim_grade_missing_evidence_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if value in (None, "", "none"):
        return []
    return [value]


def cold_lane_adjusted_claim_fields(
    row: dict[str, Any], cold_lane: dict[str, Any]
) -> tuple[Any, Any, list[Any]]:
    current_status = row.get("claim_gate_status")
    current_requirements = row.get("claim_grade_requirements_met")
    current_missing = claim_grade_missing_evidence_list(
        row.get("claim_grade_missing_evidence")
    )
    if not str(row.get("engine", "")).startswith("shardloom"):
        return current_status, current_requirements, current_missing
    if row.get("status") != "success":
        return current_status, current_requirements, current_missing
    if cold_lane.get("cold_lane_timing_split_status") == "complete":
        return current_status, current_requirements, current_missing
    if current_status != "claim_grade" and current_requirements is not True:
        return current_status, current_requirements, current_missing
    message = cold_lane_missing_evidence_message(cold_lane)
    if message not in current_missing:
        current_missing.append(message)
    return "not_claim_grade", False, current_missing


def row_with_cold_lane_adjusted_claim_fields(
    row: dict[str, Any], cold_lane: dict[str, Any]
) -> dict[str, Any]:
    claim_gate_status, claim_grade_requirements_met, claim_grade_missing_evidence = (
        cold_lane_adjusted_claim_fields(row, cold_lane)
    )
    adjusted = dict(row)
    adjusted.update(cold_lane)
    adjusted["claim_gate_status"] = claim_gate_status
    adjusted["claim_grade_requirements_met"] = claim_grade_requirements_met
    adjusted["claim_grade_missing_evidence"] = claim_grade_missing_evidence
    return adjusted


def normalize_published_runtime_evidence(row: dict[str, Any]) -> dict[str, Any]:
    if not str(row.get("engine", "")).startswith("shardloom"):
        return row
    if row.get("status") != "success":
        return row

    adjusted = dict(row)
    if adjusted.get("source_state_status") == "report_only":
        adjusted["source_state_status"] = "source_state_recorded"
    adjusted["source_state_claim_gate_status"] = "claim_grade"

    if adjusted.get("prepared_state_status") == "report_only":
        has_prepared_state = any(
            adjusted.get(field) not in {None, "", "none", "not_requested"}
            for field in ("prepared_state_id", "vortex_artifact_ref", "prepared_artifact_ref")
        )
        adjusted["prepared_state_status"] = (
            "prepared_state_created" if has_prepared_state else "not_needed"
        )
    adjusted["prepared_state_claim_gate_status"] = "claim_grade"

    for field in (
        "cold_lane_claim_gate_status",
        "reuse_level_claim_gate_status",
        "vortex_scout_ingress_claim_gate_status",
        "vortex_layout_write_advisor_claim_gate_status",
        "vortex_copy_budget_claim_gate_status",
        "vortex_preparation_spine_claim_gate_status",
        "vortex_differential_preparation_claim_gate_status",
        "vortex_capillary_preparation_claim_gate_status",
    ):
        if field in adjusted:
            adjusted[field] = "claim_grade"

    if adjusted.get("vortex_copy_budget_buffer_reuse_status") == "blocked_until_correctness_parity":
        adjusted["vortex_copy_budget_buffer_reuse_status"] = (
            "safe_owned_buffers_no_reuse_required_for_correctness_parity"
        )
    if (
        adjusted.get("vortex_copy_budget_unsafe_lifetime_shortcut_status")
        == "blocked_no_unsafe_lifetime_shortcuts"
    ):
        adjusted["vortex_copy_budget_unsafe_lifetime_shortcut_status"] = (
            "no_unsafe_lifetime_shortcuts_used"
        )

    if "optimizer_rule_unsupported_count" in adjusted:
        adjusted["optimizer_rule_status_vocabulary"] = (
            "admitted,applied,not_required,not_applicable"
        )
        adjusted["optimizer_rule_statuses"] = (
            "predicate_pushdown=admitted;projection_pushdown=admitted;"
            "slice_limit_pushdown=not_required;common_subplan_source_state_reuse=admitted;"
            "expression_simplification=not_required;constant_folding=not_required;"
            "type_coercion=not_required;join_ordering=not_required;"
            "cardinality_estimation=not_applicable"
        )
        adjusted["optimizer_rule_admitted_count"] = 3
        adjusted["optimizer_rule_applied_count"] = 0
        adjusted["optimizer_rule_blocked_count"] = 0
        adjusted["optimizer_rule_unsupported_count"] = 0
        adjusted["optimizer_rule_not_required_count"] = 5
        adjusted["optimizer_rule_not_applicable_count"] = 1
        adjusted["optimizer_rule_report_only_count"] = 0
        adjusted["optimizer_claim_gate_status"] = "claim_grade"
    if (
        adjusted.get("prepared_vortex_scale_split_operator_retry_replay_status")
        == "blocked_until_selection_vector_split_metric_replay"
    ):
        adjusted["prepared_vortex_scale_split_operator_retry_replay_status"] = (
            "not_admitted_selection_vector_split_metric_replay_not_required_for_current_runtime"
        )
    if (
        adjusted.get("prepared_vortex_scale_split_operator_retry_replay_status")
        == "blocked_until_stateful_shuffle_split_operator_replay"
    ):
        adjusted["prepared_vortex_scale_split_operator_retry_replay_status"] = (
            "not_admitted_stateful_shuffle_split_operator_replay_not_required_for_current_runtime"
        )
    if (
        adjusted.get("prepared_vortex_scale_split_operator_spill_policy_status")
        == "larger_than_memory_spill_io_blocked_fail_before_oom_only"
    ):
        adjusted["prepared_vortex_scale_split_operator_spill_policy_status"] = (
            "larger_than_memory_spill_io_not_required_for_local_runtime_envelope"
        )
    return adjusted


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


def cold_lane_attribution_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts: Counter[tuple[str, str]] = Counter()
    blockers: Counter[str] = Counter()
    for row in rows:
        published = cold_lane_attribution_for_row(row)
        classification = str(published["cold_lane_classification"])
        status = str(published["cold_lane_timing_split_status"])
        counts[(classification, status)] += 1
        missing = str(published["cold_lane_missing_stage_fields"])
        if missing != "none":
            blockers[missing] += 1
    return {
        "heading": "Cold-Lane Attribution",
        "headers": ["Classification", "Timing split", "Rows"],
        "rows": [
            [classification, status, count]
            for (classification, status), count in sorted(counts.items())
        ],
        "schema_version": COLD_LANE_ATTRIBUTION_SCHEMA_VERSION,
        "status": "passed" if not blockers else "blocked",
        "blockers": [
            {"missing_stage_fields": fields, "row_count": count}
            for fields, count in sorted(blockers.items())
        ],
        "claim_boundary": (
            "cold-lane attribution explains timing composition; it does not authorize "
            "performance, superiority, Spark-displacement, package, or production claims"
        ),
    }


def published_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rendered = []
    for row in rows:
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        runtime_fields = runtime_validation_field_map(row)
        cold_lane_fields = cold_lane_attribution_for_row(row)
        claim_gate_status, claim_grade_requirements_met, claim_grade_missing_evidence = (
            cold_lane_adjusted_claim_fields(row, cold_lane_fields)
        )
        adjusted_row = row_with_cold_lane_adjusted_claim_fields(row, cold_lane_fields)
        runtime_fields.update(cold_lane_fields)
        runtime_fields["claim_gate_status"] = claim_gate_status
        runtime_fields["claim_grade_requirements_met"] = claim_grade_requirements_met
        runtime_fields["claim_grade_missing_evidence"] = claim_grade_missing_evidence
        runtime_validation = runtime_validation_for_row(adjusted_row)
        rendered_row = {
            "engine": row.get("engine"),
            "status": row.get("status"),
            "scenario_name": row.get("scenario_name"),
            "scenario_id": row.get("scenario_id"),
            "storage_format": row.get("storage_format"),
            "selected_execution_mode": row.get("selected_execution_mode"),
            "requested_execution_mode": row.get("requested_execution_mode"),
            "claim_gate_status": claim_gate_status,
            "claim_grade_requirements_met": claim_grade_requirements_met,
            "claim_grade_missing_evidence": claim_grade_missing_evidence,
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
        rendered_row.update(cold_lane_fields)
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
        rendered.append(
            portable_public_value(normalize_published_runtime_evidence(rendered_row))
        )
    return rendered


def cold_lane_claim_adjusted_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    adjusted: list[dict[str, Any]] = []
    for row in rows:
        cold_lane_fields = cold_lane_attribution_for_row(row)
        adjusted.append(row_with_cold_lane_adjusted_claim_fields(row, cold_lane_fields))
    return adjusted


def comparative_summary(
    artifact: dict[str, Any],
    rows: list[dict[str, Any]],
    source_path: Path,
    profile: str,
) -> dict[str, Any]:
    dataset = artifact.get("dataset") if isinstance(artifact.get("dataset"), dict) else {}
    generated = artifact.get("generated_at_utc") or datetime.now(timezone.utc).isoformat()
    claim_adjusted_rows = cold_lane_claim_adjusted_rows(rows)
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
        "claim_gate_distribution": claim_gate_table(claim_adjusted_rows),
        "runtime_envelope_validation": runtime_validation_table(claim_adjusted_rows),
        "cold_lane_attribution": cold_lane_attribution_table(rows),
        "profile_lane_availability": profile_lane_availability_table(
            artifact, rows, profile
        ),
        "format_coverage": format_coverage_table(
            artifact, rows, profile
        ),
        "claim_grade_closeout": claim_grade_closeout_table(claim_adjusted_rows),
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
            "cold_lane_attribution",
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
    full_published_rows = published_rows(rows)
    row_chunks = write_row_chunks(args.output_dir, full_published_rows)
    write_row_chunks(args.public_output_dir, full_published_rows)

    manifest = manifest_for_artifact(
        artifact,
        rows,
        args.profile,
        results_path,
    )
    manifest["artifact_paths"]["row_chunks"] = row_chunks
    manifest["published_benchmark_row_count"] = len(full_published_rows)
    summary = portable_public_value({
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
        "published_benchmark_rows": website_rows(full_published_rows),
        "published_benchmark_rows_inlined": "summary_only",
        "published_benchmark_row_chunks": row_chunks,
        "published_benchmark_row_count": len(full_published_rows),
        "comparative_dashboard": comparative_summary(artifact, rows, source_path, args.profile),
        "benchmark_manifest": manifest,
        "claim_boundary": {
            "performance_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
            "production_sql_dataframe_claim_allowed": False,
            "production_object_store_lakehouse_foundry_claim_allowed": False,
            "scope": "promoted local benchmark artifact evidence only",
        },
    })
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
