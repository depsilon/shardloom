#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate ShardLoom website benchmark artifact completeness manifests."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
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


REQUIRED_MANIFEST_FIELDS = {
    "schema_version",
    "generated_at_utc",
    "benchmark_profile",
    "expected_lanes",
    "available_lanes",
    "missing_lanes",
    "lane_versions",
    "lane_availability_reasons",
    "environment",
    "claim_boundary",
    "performance_claim_allowed",
    "artifact_paths",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="Allow missing required lanes if the manifest is explicitly marked incomplete.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def repo_path(path_text: str, manifest_path: Path) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    root_candidate = ROOT / path
    if root_candidate.exists():
        return root_candidate
    return manifest_path.parent / path


def result_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("results")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    rows = payload.get("published_benchmark_rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    rows = payload.get("rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def lane_evidence_counts(payload: dict[str, Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in result_rows(payload):
        engine = row.get("engine")
        if engine:
            lane = str(engine)
            counts[lane] += 1
            if lane == "shardloom-vortex":
                counts["native-vortex"] += 1
    for row in payload.get("batch_rows", []):
        if not isinstance(row, dict):
            continue
        requested = str(row.get("requested_execution_mode") or "")
        selected = str(row.get("selected_execution_modes") or "")
        if requested == "prepared_vortex" or "prepared_vortex" in selected:
            counts["shardloom-prepared-vortex"] += 1
        if requested == "native_vortex" or "native_vortex" in selected:
            counts["shardloom-vortex"] += 1
            counts["native-vortex"] += 1
    return counts


def recursive_text_contains(value: Any, needle: str) -> bool:
    if isinstance(value, str):
        return needle in value
    if isinstance(value, list):
        return any(recursive_text_contains(item, needle) for item in value)
    if isinstance(value, dict):
        return any(recursive_text_contains(item, needle) for item in value.values())
    return False


def validate_rows(payload: dict[str, Any], blockers: list[str]) -> None:
    for index, row in enumerate(result_rows(payload)):
        engine = str(row.get("engine", ""))
        if engine.startswith("shardloom"):
            if "fallback_attempted" not in row:
                blockers.append(f"ShardLoom row {index} is missing fallback_attempted")
            elif row.get("fallback_attempted") is not False:
                blockers.append(
                    f"ShardLoom row {index} must set fallback_attempted=false"
                )
            if "external_engine_invoked" not in row:
                blockers.append(f"ShardLoom row {index} is missing external_engine_invoked")
            elif row.get("external_engine_invoked") is not False:
                blockers.append(
                    f"ShardLoom row {index} must set external_engine_invoked=false"
                )
        elif engine:
            if (
                row.get("external_baseline_only") is not True
                and row.get("row_classification") != "external_baseline_only"
            ):
                blockers.append(
                    f"external row {index} ({engine}) is missing external_baseline_only marker"
                )


def validate_manifest(manifest_path: Path, allow_incomplete: bool) -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    manifest = load_json(manifest_path)
    missing_fields = REQUIRED_MANIFEST_FIELDS - set(manifest)
    if missing_fields:
        blockers.append(f"manifest missing fields: {sorted(missing_fields)}")

    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        blockers.append(
            f"manifest schema_version must be {MANIFEST_SCHEMA_VERSION}, got {manifest.get('schema_version')}"
        )
    if manifest.get("performance_claim_allowed") is not False:
        blockers.append("performance_claim_allowed must be false")
    profile = manifest.get("benchmark_profile")
    if profile not in PROFILES:
        blockers.append(f"unknown benchmark_profile: {profile}")
        return blockers, manifest

    expected = set(manifest.get("expected_lanes") or [])
    available = set(manifest.get("available_lanes") or [])
    missing = set(manifest.get("missing_lanes") or [])
    required_expected = set(expected_lanes_for_profile(profile))
    if not required_expected.issubset(expected):
        blockers.append(
            f"expected_lanes missing profile lanes: {sorted(required_expected - expected)}"
        )
    unresolved = expected - available - missing
    if unresolved:
        blockers.append(f"expected lanes with no availability status: {sorted(unresolved)}")
    overlap = available & missing
    if overlap:
        blockers.append(f"lanes marked both available and missing: {sorted(overlap)}")

    reasons = manifest.get("lane_availability_reasons") or {}
    for lane in missing:
        if not reasons.get(lane):
            blockers.append(f"missing lane lacks availability reason: {lane}")
    versions = manifest.get("lane_versions") or {}
    for lane in available:
        if not versions.get(lane):
            blockers.append(f"available lane lacks version metadata: {lane}")

    missing_required = [
        lane for lane in missing if lane_required_for_profile(profile, lane)
    ]
    artifact_status = str(manifest.get("artifact_status", "complete"))
    if missing_required and not (allow_incomplete and artifact_status == "incomplete"):
        blockers.append(
            "required lanes missing for profile "
            f"{profile}: {sorted(missing_required)}"
        )

    artifact_paths = manifest.get("artifact_paths") or {}
    json_path_text = artifact_paths.get("json")
    if not json_path_text:
        blockers.append("artifact_paths.json is required")
    else:
        json_path = repo_path(str(json_path_text), manifest_path)
        if not json_path.exists():
            blockers.append(f"artifact_paths.json does not exist: {json_path_text}")
        else:
            payload = load_json(json_path)
            if isinstance(payload, dict):
                validate_rows(payload, blockers)
                if recursive_text_contains(payload, "spark-retire"):
                    blockers.append(
                        "published benchmark artifact must not reference spark-retire"
                    )
                lane_counts = lane_evidence_counts(payload)
                for lane in sorted(expected & available):
                    if lane_counts[lane] == 0:
                        blockers.append(
                            f"available expected lane has no published row evidence: {lane}"
                        )
                if profile in {"full_local", "full_local_plus_spark"}:
                    if "polars" in expected or "polars" in available:
                        blockers.append(
                            "full benchmark profiles must use polars-eager and "
                            "polars-lazy, not collapsed polars"
                        )
                    for lane in ("polars-eager", "polars-lazy"):
                        if lane not in expected:
                            blockers.append(f"full benchmark profile missing {lane}")
            else:
                blockers.append("artifact_paths.json must contain an object")

    return blockers, manifest


def main() -> int:
    args = parse_args()
    blockers, manifest = validate_manifest(args.manifest, args.allow_incomplete)
    report = {
        "manifest": str(args.manifest),
        "benchmark_profile": manifest.get("benchmark_profile"),
        "artifact_status": manifest.get("artifact_status"),
        "blockers": blockers,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if blockers else 0


if __name__ == "__main__":
    raise SystemExit(main())
