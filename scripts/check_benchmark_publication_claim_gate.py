#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate that the public benchmark bundle is safe to use for claim-grade publishing.

This script is a static artifact gate. It reads committed benchmark JSON plus the pre-5J
dependency freshness report and does not execute benchmarks, import external engines, or refresh
timing data.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "python" / "src"))

from benchmarks.traditional_analytics.benchmark_registry import (  # noqa: E402
    PROFILES,
    expected_lanes_for_profile,
)
from check_benchmark_artifact_completeness import (  # noqa: E402
    load_json,
    repo_path,
    result_rows,
    runtime_validation_field_map,
    validate_manifest,
)
from shardloom import validate_runtime_execution_fields  # noqa: E402


SCHEMA_VERSION = "shardloom.benchmark_publication_claim_gate.v1"
DEFAULT_MANIFEST = ROOT / "website" / "assets" / "benchmarks" / "latest" / "manifest.json"
DEFAULT_OUTPUT = ROOT / "target" / "benchmark-publication-claim-gate-report.json"
DEFAULT_PRE_5J_DEPENDENCY_REPORT = ROOT / "target" / "pre-5j-dependency-freshness-gate.json"
PUBLIC_BENCHMARK_PAYLOAD_REFS = (
    "website/assets/benchmarks/latest/benchmark-results.json",
    "website/assets/data/benchmark-evidence.json",
    "website-public/assets/benchmarks/latest/benchmark-results.json",
    "website-public/assets/data/benchmark-evidence.json",
    "website-src/src/data/benchmark-evidence.json",
)
PUBLIC_BENCHMARK_MANIFEST_REFS = (
    "website/assets/benchmarks/latest/manifest.json",
    "website-public/assets/benchmarks/latest/manifest.json",
    "website-src/src/data/benchmark-manifest.json",
)
DEFAULT_MAX_AGE_DAYS = 14
FUTURE_CLOCK_SKEW = timedelta(minutes=5)
REQUIRED_PUBLICATION_FORMATS = ("csv", "parquet", "jsonl", "arrow-ipc", "avro", "orc")
REQUIRED_SHARDLOOM_PUBLICATION_ENGINES = (
    "shardloom",
    "shardloom-prepared-vortex",
    "shardloom-prepare-batch",
    "shardloom-vortex",
)
REQUIRED_SPARK_BASELINE_LANES = ("pyspark", "spark-default", "spark-local-tuned")
REQUIRED_CAPILLARY_ACTIVATION_FIELDS = (
    "vortex_capillary_preparation_activation_policy",
    "vortex_capillary_preparation_activation_result",
    "vortex_capillary_preparation_activation_reason",
    "vortex_capillary_preparation_activation_observed_bytes",
    "vortex_capillary_preparation_activation_observed_rows",
    "vortex_capillary_preparation_activation_observed_columns",
    "vortex_capillary_preparation_activation_observed_split_count",
)
BLOCKING_SHARDLOOM_STATUSES = {"blocked", "unsupported", "failed", "error"}
MIN_CLAIM_GRADE_ITERATIONS = 3
RESULT_SINK_REPLAY_VERIFIED_FIELDS = (
    "computed_result_sink_replay_verified",
    "result_sink_replay_verified",
    "evidence_level_result_sink_replay_verified",
)
LOCAL_PATH_RE = re.compile(
    r"(?P<win>[A-Za-z]:\\[^|,;\"'\s]+)|"
    r"(?P<posix>(?:/Users|/home|/tmp|/var/folders|/private/var|/workspace|/mnt|/Volumes)"
    r"[^|,;\"'\s]*)"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--pre-5j-dependency-report",
        type=Path,
        default=DEFAULT_PRE_5J_DEPENDENCY_REPORT,
    )
    parser.add_argument("--allow-incomplete", action="store_true")
    parser.add_argument(
        "--allow-stale-git",
        action="store_true",
        help="Inspect historical artifacts without requiring manifest SHAs to match HEAD.",
    )
    parser.add_argument(
        "--allow-dirty-worktree",
        action="store_true",
        help="Inspect historical artifacts without blocking on local dirty worktree state.",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=DEFAULT_MAX_AGE_DAYS,
        help="Maximum public benchmark artifact age before it is considered stale.",
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def git_text(repo_root: Path, args: list[str]) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(stderr or f"git {' '.join(args)} failed")
    return completed.stdout.strip()


def parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def artifact_payload(manifest: dict[str, Any], manifest_path: Path) -> dict[str, Any] | None:
    artifact_paths = manifest.get("artifact_paths")
    if not isinstance(artifact_paths, dict):
        return None
    json_path_text = artifact_paths.get("json")
    if not json_path_text:
        return None
    json_path = repo_path(str(json_path_text), manifest_path)
    if not json_path.exists():
        return None
    payload = load_json(json_path)
    return payload if isinstance(payload, dict) else None


def validate_pre_5j_dependency_report(path: Path, blockers: list[str]) -> dict[str, Any]:
    if not path.exists():
        blockers.append("missing pre-5J dependency freshness report")
        return {"status": "missing", "benchmark_refresh_allowed": False}
    payload = load_json(path)
    if not isinstance(payload, dict):
        blockers.append("pre-5J dependency freshness report is invalid")
        return {"status": "invalid", "benchmark_refresh_allowed": False}
    if payload.get("schema_version") != "shardloom.pre_5j_dependency_freshness_gate.v1":
        blockers.append("pre-5J dependency freshness schema mismatch")
    if payload.get("status") != "passed":
        blockers.extend(
            f"pre-5J dependency freshness: {blocker}"
            for blocker in payload.get("blockers", ["gate blocked"])
        )
    if payload.get("benchmark_refresh_allowed") is not True:
        blockers.append(
            "pre-5J dependency freshness must be live-checked before benchmark publication"
        )
    for field in [
        "benchmark_run_performed",
        "publication_attempted",
        "tag_created",
        "secrets_required",
        "fallback_attempted",
        "external_engine_invoked",
    ]:
        if payload.get(field) is not False:
            blockers.append(f"pre-5J dependency freshness {field} must be false")
    return {
        "status": payload.get("status"),
        "open_dependabot_check_status": payload.get("open_dependabot_check_status"),
        "open_dependabot_pr_count": payload.get("open_dependabot_pr_count"),
        "benchmark_refresh_dependency_gate_status": payload.get(
            "benchmark_refresh_dependency_gate_status"
        ),
        "benchmark_refresh_allowed": payload.get("benchmark_refresh_allowed"),
    }


def declared_publication_formats(payload: dict[str, Any]) -> set[str]:
    metadata = payload.get("published_benchmark_artifact")
    if not isinstance(metadata, dict):
        metadata = payload
    return {
        str(item)
        for item in metadata.get("format_order", [])
        if isinstance(item, str) and item
    }


def row_publication_formats(rows: list[dict[str, Any]]) -> set[str]:
    return {
        str(row.get("storage_format"))
        for row in rows
        if row.get("storage_format")
    }


def field_value(row: dict[str, Any], key: str) -> Any:
    fields = runtime_validation_field_map(row)
    if key in fields:
        return fields[key]
    metrics = row.get("metrics")
    if isinstance(metrics, dict) and key in metrics:
        return metrics[key]
    evidence = row.get("shardloom_evidence")
    if isinstance(evidence, dict) and key in evidence:
        return evidence[key]
    return None


def bool_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    return None


def int_value(value: Any) -> int | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def finite_non_negative_number(value: Any) -> bool:
    if isinstance(value, bool) or value in (None, ""):
        return False
    try:
        parsed = float(str(value).strip())
    except ValueError:
        return False
    return math.isfinite(parsed) and parsed >= 0


def meaningful_value(value: Any) -> bool:
    return str(value or "").strip().lower() not in {
        "",
        "none",
        "missing",
        "not_available",
        "not_applicable",
        "[]",
        "{}",
    }


def local_path_occurrences(value: Any, *, path: str = "$") -> list[str]:
    if isinstance(value, str):
        return [path] if LOCAL_PATH_RE.search(value) is not None else []
    if isinstance(value, list):
        occurrences: list[str] = []
        for index, item in enumerate(value):
            occurrences.extend(local_path_occurrences(item, path=f"{path}[{index}]"))
        return occurrences
    if isinstance(value, dict):
        occurrences: list[str] = []
        for key, item in value.items():
            occurrences.extend(local_path_occurrences(item, path=f"{path}.{key}"))
        return occurrences
    return []


def result_sink_replay_verified(row: dict[str, Any]) -> bool:
    for field in RESULT_SINK_REPLAY_VERIFIED_FIELDS:
        if bool_value(field_value(row, field)) is True:
            return True
    commit_status = str(
        field_value(row, "prepared_vortex_scale_split_operator_output_commit_proof_status")
        or ""
    ).lower()
    return "replay_verified" in commit_status


def shardloom_claim_gate(row: dict[str, Any]) -> str:
    for key in ("claim_gate_status", "claim_gate"):
        value = field_value(row, key)
        if value:
            return str(value)
    return "missing"


def independent_claim_grade_missing_evidence(row: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    iterations = int_value(field_value(row, "iterations"))
    min_iterations = int_value(field_value(row, "reproducibility_min_iterations"))
    if min_iterations is None:
        missing.append("reproducibility_min_iterations missing")
        min_iterations = MIN_CLAIM_GRADE_ITERATIONS
    if iterations is None:
        missing.append("iterations missing")
    elif iterations < max(min_iterations, MIN_CLAIM_GRADE_ITERATIONS):
        missing.append(
            "iterations below claim-grade minimum "
            f"(actual={iterations}; required={max(min_iterations, MIN_CLAIM_GRADE_ITERATIONS)})"
        )
    if bool_value(field_value(row, "reproducibility_iterations_met")) is not True:
        missing.append("reproducibility_iterations_met!=true")
    if bool_value(field_value(row, "correctness_digest_stable")) is not True:
        missing.append("correctness_digest_stable!=true")
    if not meaningful_value(field_value(row, "correctness_digest")):
        missing.append("correctness_digest missing")
    if not finite_non_negative_number(field_value(row, "query_runtime_millis")):
        missing.append("query_runtime_millis missing_or_invalid")
    if str(field_value(row, "cold_lane_timing_split_status") or "") != "complete":
        missing.append("cold_lane_timing_split_status!=complete")
    if not result_sink_replay_verified(row):
        missing.append("result_sink_replay_verified proof missing")
    return missing


def validate_freshness(
    manifest: dict[str, Any],
    repo_root: Path,
    blockers: list[str],
    *,
    now: datetime | None,
    max_age_days: int,
    require_current_git: bool,
    allow_dirty_worktree: bool,
    current_git_sha: str | None,
    worktree_status: str | None,
) -> dict[str, Any]:
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    generated_at = parse_utc(manifest.get("generated_at_utc"))
    age_days: float | None = None
    if generated_at is None:
        blockers.append("benchmark manifest generated_at_utc is missing or unparsable")
    else:
        age = now_utc - generated_at
        age_days = age.total_seconds() / 86400
        if generated_at > now_utc + FUTURE_CLOCK_SKEW:
            blockers.append("benchmark manifest generated_at_utc is in the future")
        if max_age_days >= 0 and age > timedelta(days=max_age_days):
            blockers.append(
                "benchmark artifact age exceeds freshness limit: "
                f"{age_days:.2f} days > {max_age_days} days"
            )

    resolved_git_sha = current_git_sha
    resolved_status = worktree_status
    if require_current_git and resolved_git_sha is None:
        try:
            resolved_git_sha = git_text(repo_root, ["rev-parse", "HEAD"])
        except RuntimeError as exc:
            blockers.append(f"current git sha unavailable: {exc}")
    if require_current_git and resolved_status is None:
        try:
            resolved_status = git_text(repo_root, ["status", "--short"])
        except RuntimeError as exc:
            blockers.append(f"git worktree status unavailable: {exc}")

    if require_current_git and resolved_git_sha:
        for key in ("benchmark_git_sha", "shardloom_git_sha"):
            recorded = manifest.get(key)
            if recorded != resolved_git_sha:
                blockers.append(
                    f"benchmark manifest {key}={recorded!r} does not match current HEAD "
                    f"{resolved_git_sha}"
                )

    worktree_dirty = bool(resolved_status)
    if require_current_git and worktree_dirty and not allow_dirty_worktree:
        blockers.append("benchmark artifact cannot be current while the worktree is dirty")

    return {
        "generated_at_utc": manifest.get("generated_at_utc"),
        "artifact_age_days": age_days,
        "max_age_days": max_age_days,
        "current_git_sha": resolved_git_sha,
        "benchmark_git_sha": manifest.get("benchmark_git_sha"),
        "shardloom_git_sha": manifest.get("shardloom_git_sha"),
        "worktree_dirty": worktree_dirty,
    }


def validate_profile_and_rows(
    manifest: dict[str, Any],
    payload: dict[str, Any] | None,
    blockers: list[str],
) -> dict[str, Any]:
    profile = str(manifest.get("benchmark_profile") or "")
    expected_lanes = set(manifest.get("expected_lanes") or [])
    available_lanes = set(manifest.get("available_lanes") or [])
    missing_required_formats: list[str] = []
    shardloom_status_counts: Counter[str] = Counter()
    shardloom_claim_counts: Counter[str] = Counter()
    shardloom_engine_counts: Counter[str] = Counter()
    shardloom_format_counts: Counter[str] = Counter()
    all_engine_counts: Counter[str] = Counter()
    external_engine_counts: Counter[str] = Counter()
    shardloom_engine_format_counts: dict[str, Counter[str]] = defaultdict(Counter)
    runtime_validation_counts: Counter[str] = Counter()
    missing_capillary_count = 0
    non_success_examples: list[str] = []
    non_claim_examples: list[str] = []
    requirements_examples: list[str] = []
    fallback_examples: list[str] = []
    external_examples: list[str] = []
    external_baseline_boundary_examples: list[str] = []
    external_claim_examples: list[str] = []
    runtime_validation_examples: list[str] = []
    runtime_claim_examples: list[str] = []
    independent_claim_examples: list[str] = []
    missing_independent_claim_proof_count = 0

    if profile not in PROFILES:
        blockers.append(f"unknown benchmark profile for publication gate: {profile}")
    else:
        required = set(expected_lanes_for_profile(profile))
        missing_expected = sorted(required - expected_lanes)
        missing_available = sorted(required - available_lanes)
        if missing_expected:
            blockers.append(
                f"publication profile expected_lanes missing required lanes: {missing_expected}"
            )
        if missing_available:
            blockers.append(
                f"publication profile available_lanes missing required lanes: {missing_available}"
            )
        if profile == "full_local_plus_spark":
            missing_spark = [
                lane for lane in REQUIRED_SPARK_BASELINE_LANES if lane not in available_lanes
            ]
            if missing_spark:
                blockers.append(
                    f"full_local_plus_spark missing required Spark baseline lanes: {missing_spark}"
                )

    if payload is None:
        blockers.append("benchmark publication payload is missing or invalid")
        declared_formats = set()
        row_formats = set()
        rows: list[dict[str, Any]] = []
        payload_local_path_occurrences: list[str] = []
    else:
        rows = result_rows(payload)
        declared_formats = declared_publication_formats(payload)
        row_formats = row_publication_formats(rows)
        payload_local_path_occurrences = local_path_occurrences(payload)
    local_path_occurrence_paths = (
        local_path_occurrences(manifest) + payload_local_path_occurrences
    )
    nonportable_ref_examples = local_path_occurrence_paths[:5]
    nonportable_ref_count = len(local_path_occurrence_paths)

    missing_declared_formats = sorted(set(REQUIRED_PUBLICATION_FORMATS) - declared_formats)
    if missing_declared_formats:
        blockers.append(
            "published benchmark manifest missing public-format declarations: "
            f"{missing_declared_formats}"
        )
    missing_required_formats = sorted(set(REQUIRED_PUBLICATION_FORMATS) - row_formats)
    if missing_required_formats:
        blockers.append(
            "published benchmark rows missing public-format coverage: "
            f"{missing_required_formats}"
        )

    for index, row in enumerate(rows):
        engine = str(row.get("engine") or "")
        storage_format = str(row.get("storage_format") or "")
        if engine:
            all_engine_counts[engine] += 1
        if not engine.startswith("shardloom"):
            if engine:
                external_engine_counts[engine] += 1
            if field_value(row, "external_baseline_only") is not True:
                if len(external_baseline_boundary_examples) < 5:
                    external_baseline_boundary_examples.append(f"{index}:{engine}")
            if field_value(row, "claim_grade_requirements_met") is True:
                if len(external_claim_examples) < 5:
                    external_claim_examples.append(f"{index}:{engine}")
            if field_value(row, "fallback_attempted") is not False:
                if len(fallback_examples) < 5:
                    fallback_examples.append(f"{index}:{engine}")
            if field_value(row, "external_engine_invoked") is not False:
                if len(external_examples) < 5:
                    external_examples.append(f"{index}:{engine}")
            continue
        shardloom_engine_counts[engine] += 1
        if storage_format:
            shardloom_format_counts[storage_format] += 1
            shardloom_engine_format_counts[engine][storage_format] += 1
        status = str(row.get("status") or "missing")
        shardloom_status_counts[status] += 1
        claim_gate = shardloom_claim_gate(row)
        shardloom_claim_counts[claim_gate] += 1
        runtime_validation = validate_runtime_execution_fields(
            runtime_validation_field_map(row),
            command="published-benchmark-row",
            status=status,
            surface_id=(
                "benchmark_publication."
                f"{engine}.{storage_format or 'unknown'}.{index}"
            ),
            runtime_expected=status == "success",
            execution_mode=str(row.get("selected_execution_mode") or "") or None,
        )
        runtime_validation_counts[runtime_validation.status] += 1
        if runtime_validation.status != "passed" and len(runtime_validation_examples) < 5:
            runtime_validation_examples.append(
                f"{index}:{engine}:{storage_format}:"
                + "; ".join(runtime_validation.blockers)
            )
        if claim_gate == "claim_grade" and not runtime_validation.runtime_claim_allowed:
            if len(runtime_claim_examples) < 5:
                runtime_claim_examples.append(f"{index}:{engine}:{storage_format}")
        if status == "success":
            independent_missing = independent_claim_grade_missing_evidence(row)
            if independent_missing:
                missing_independent_claim_proof_count += 1
                if len(independent_claim_examples) < 5:
                    independent_claim_examples.append(
                        f"{index}:{engine}:{storage_format}:{','.join(independent_missing)}"
                    )

        if status in BLOCKING_SHARDLOOM_STATUSES or status != "success":
            if len(non_success_examples) < 5:
                non_success_examples.append(f"{index}:{engine}:{status}")
        if claim_gate != "claim_grade":
            if len(non_claim_examples) < 5:
                non_claim_examples.append(f"{index}:{engine}:claim_gate_status={claim_gate}")
        if field_value(row, "claim_grade_requirements_met") is not True:
            if len(requirements_examples) < 5:
                requirements_examples.append(f"{index}:{engine}")
        if field_value(row, "fallback_attempted") is not False:
            if len(fallback_examples) < 5:
                fallback_examples.append(f"{index}:{engine}")
        if field_value(row, "external_engine_invoked") is not False:
            if len(external_examples) < 5:
                external_examples.append(f"{index}:{engine}")
        missing_capillary = []
        for key in REQUIRED_CAPILLARY_ACTIVATION_FIELDS:
            value = field_value(row, key)
            if value in (None, "", "unknown", "not_reported"):
                missing_capillary.append(key)
                continue
            if key == "vortex_capillary_preparation_activation_policy" and (
                value != "dynamic_size_complexity_gate.v1"
            ):
                missing_capillary.append(key)
            elif key == "vortex_capillary_preparation_activation_result" and (
                value not in {"activated", "skipped"}
            ):
                missing_capillary.append(key)
            elif key.startswith(
                "vortex_capillary_preparation_activation_observed_"
            ) and int_value(value) is None:
                missing_capillary.append(key)
        if missing_capillary:
            missing_capillary_count += 1

    missing_profile_row_lanes = sorted(
        lane for lane in expected_lanes if all_engine_counts[lane] == 0
    )
    if missing_profile_row_lanes:
        blockers.append(
            "publication profile lanes have no published rows: "
            f"{missing_profile_row_lanes}"
        )

    missing_engines = sorted(
        engine
        for engine in REQUIRED_SHARDLOOM_PUBLICATION_ENGINES
        if shardloom_engine_counts[engine] == 0
    )
    if missing_engines:
        blockers.append(f"missing ShardLoom publication engines: {missing_engines}")
    missing_shardloom_formats = sorted(
        set(REQUIRED_PUBLICATION_FORMATS) - set(shardloom_format_counts)
    )
    if missing_shardloom_formats:
        blockers.append(
            "ShardLoom publication rows missing public-format coverage: "
            f"{missing_shardloom_formats}"
        )
    missing_shardloom_engine_format_cells = [
        f"{engine}:{storage_format}"
        for engine in REQUIRED_SHARDLOOM_PUBLICATION_ENGINES
        for storage_format in REQUIRED_PUBLICATION_FORMATS
        if shardloom_engine_format_counts[engine][storage_format] == 0
    ]
    if missing_shardloom_engine_format_cells:
        blockers.append(
            "ShardLoom publication rows missing engine-format coverage: "
            f"{len(missing_shardloom_engine_format_cells)} cells; "
            f"examples={missing_shardloom_engine_format_cells[:12]}"
        )
    non_success_statuses = {
        status: count
        for status, count in shardloom_status_counts.items()
        if status != "success" or status in BLOCKING_SHARDLOOM_STATUSES
    }
    if non_success_statuses:
        blockers.append(
            "ShardLoom publication rows with non-success status blocked: "
            f"{dict(sorted(non_success_statuses.items()))}; examples={non_success_examples}"
        )
    non_claim_grade = {
        claim_gate: count
        for claim_gate, count in shardloom_claim_counts.items()
        if claim_gate != "claim_grade"
    }
    if non_claim_grade:
        blockers.append(
            "ShardLoom publication rows with non-claim-grade claim gate: "
            f"{dict(sorted(non_claim_grade.items()))}; examples={non_claim_examples}"
        )
    if requirements_examples:
        blockers.append(
            "ShardLoom publication rows missing claim_grade_requirements_met=true; "
            f"examples={requirements_examples}"
        )
    if fallback_examples:
        blockers.append(
            "benchmark publication rows must set fallback_attempted=false; "
            f"examples={fallback_examples}"
        )
    if external_examples:
        blockers.append(
            "benchmark publication rows must set external_engine_invoked=false; "
            f"examples={external_examples}"
        )
    if external_baseline_boundary_examples:
        blockers.append(
            "external benchmark rows must set external_baseline_only=true; "
            f"examples={external_baseline_boundary_examples}"
        )
    if external_claim_examples:
        blockers.append(
            "external benchmark rows must not satisfy ShardLoom claim-grade requirements; "
            f"examples={external_claim_examples}"
        )
    failed_runtime_validations = {
        status: count
        for status, count in runtime_validation_counts.items()
        if status != "passed"
    }
    if failed_runtime_validations:
        blockers.append(
            "ShardLoom publication rows failed runtime envelope validation: "
            f"{dict(sorted(failed_runtime_validations.items()))}; "
            f"examples={runtime_validation_examples}"
        )
    if runtime_claim_examples:
        blockers.append(
            "claim-grade ShardLoom publication rows must satisfy runtime_claim_allowed=true; "
            f"examples={runtime_claim_examples}"
        )
    if missing_independent_claim_proof_count:
        blockers.append(
            "successful ShardLoom publication rows missing independent claim-grade proof: "
            f"{missing_independent_claim_proof_count}; examples={independent_claim_examples}"
        )
    if nonportable_ref_count:
        blockers.append(
            "published benchmark rows contain non-portable local artifact paths: "
            f"{nonportable_ref_count}; examples={nonportable_ref_examples}"
        )
    if missing_capillary_count:
        blockers.append(
            "ShardLoom publication rows missing capillary activation evidence fields: "
            f"{missing_capillary_count}"
        )

    return {
        "required_publication_formats": list(REQUIRED_PUBLICATION_FORMATS),
        "declared_publication_formats": sorted(declared_formats),
        "published_formats": sorted(row_formats),
        "missing_publication_formats": missing_required_formats,
        "all_engine_counts": dict(sorted(all_engine_counts.items())),
        "external_engine_counts": dict(sorted(external_engine_counts.items())),
        "shardloom_row_count": sum(shardloom_engine_counts.values()),
        "shardloom_engine_counts": dict(sorted(shardloom_engine_counts.items())),
        "shardloom_format_counts": dict(sorted(shardloom_format_counts.items())),
        "shardloom_engine_format_counts": {
            engine: dict(sorted(counts.items()))
            for engine, counts in sorted(shardloom_engine_format_counts.items())
        },
        "missing_shardloom_engine_format_cell_count": len(
            missing_shardloom_engine_format_cells
        ),
        "shardloom_runtime_validation_counts": dict(
            sorted(runtime_validation_counts.items())
        ),
        "missing_independent_claim_proof_row_count": missing_independent_claim_proof_count,
        "nonportable_public_ref_count": nonportable_ref_count,
        "shardloom_status_counts": dict(sorted(shardloom_status_counts.items())),
        "shardloom_claim_gate_counts": dict(sorted(shardloom_claim_counts.items())),
        "missing_capillary_activation_row_count": missing_capillary_count,
    }


def validate_publication_claim_gate(
    manifest_path: Path,
    *,
    repo_root: Path = ROOT,
    pre_5j_dependency_report_path: Path = DEFAULT_PRE_5J_DEPENDENCY_REPORT,
    allow_incomplete: bool = False,
    require_current_git: bool = True,
    allow_dirty_worktree: bool = False,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    now: datetime | None = None,
    current_git_sha: str | None = None,
    worktree_status: str | None = None,
) -> dict[str, Any]:
    blockers, manifest = validate_manifest(manifest_path, allow_incomplete)
    blockers = [f"artifact completeness: {blocker}" for blocker in blockers]
    pre_5j_dependency = validate_pre_5j_dependency_report(
        resolve(repo_root, pre_5j_dependency_report_path),
        blockers,
    )
    payload = artifact_payload(manifest, manifest_path)
    freshness = validate_freshness(
        manifest,
        repo_root,
        blockers,
        now=now,
        max_age_days=max_age_days,
        require_current_git=require_current_git,
        allow_dirty_worktree=allow_dirty_worktree,
        current_git_sha=current_git_sha,
        worktree_status=worktree_status,
    )
    row_report = validate_profile_and_rows(manifest, payload, blockers)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "blocked",
        "manifest": str(manifest_path),
        "benchmark_profile": manifest.get("benchmark_profile"),
        "artifact_status": manifest.get("artifact_status"),
        "pre_5j_dependency_freshness": pre_5j_dependency,
        "freshness": freshness,
        **row_report,
        "blockers": blockers,
        "benchmark_run_performed": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def main() -> int:
    args = parse_args()
    report = validate_publication_claim_gate(
        args.manifest,
        repo_root=args.repo_root,
        pre_5j_dependency_report_path=args.pre_5j_dependency_report,
        allow_incomplete=args.allow_incomplete,
        require_current_git=not args.allow_stale_git,
        allow_dirty_worktree=args.allow_dirty_worktree,
        max_age_days=args.max_age_days,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
