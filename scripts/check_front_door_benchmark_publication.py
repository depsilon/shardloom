#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate front-door benchmark publication admission without running benchmarks.

This gate composes the SQL/Python/DataFrame parity report with the committed public
benchmark publication gate. It is intentionally claim-gated: ShardLoom may publish scoped
local SQL/Python/DataFrame route-equivalence evidence, but it must not publish public
performance, production, superiority, or Spark-replacement claims from that local proof.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from check_benchmark_publication_claim_gate import (  # noqa: E402
    DEFAULT_MAX_AGE_DAYS,
    DEFAULT_MANIFEST,
    DEFAULT_PRE_5J_DEPENDENCY_REPORT,
    PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION,
    REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS,
    SCHEMA_VERSION as BENCHMARK_PUBLICATION_SCHEMA_VERSION,
    validate_publication_claim_gate,
)
from check_sql_python_dataframe_parity import (  # noqa: E402
    SCHEMA_VERSION as SQL_PYTHON_DATAFRAME_PARITY_SCHEMA_VERSION,
    build_report as build_sql_python_dataframe_parity_report,
)


SCHEMA_VERSION = "shardloom.front_door_benchmark_publication_gate.v1"
GATE_ID = "gar-runtime-impl-6d.front_door_performance_benchmark_publication"
DEFAULT_OUTPUT = ROOT / "target" / "front-door-benchmark-publication-gate.json"
DEFAULT_EQUIVALENCE_CONSTITUTION = (
    ROOT / "docs" / "architecture" / "front-door-performance-equivalence-constitution.json"
)
DEFAULT_EQUIVALENCE_ARTIFACT = (
    ROOT
    / "website"
    / "assets"
    / "benchmarks"
    / "latest"
    / "front-door-performance-equivalence.json"
)
FRONT_DOOR_PERFORMANCE_PUBLICATION_READY = (
    "local_equivalence_evidence_present_claim_gated"
)
EQUIVALENCE_CONSTITUTION_SCHEMA_VERSION = (
    "shardloom.front_door_performance_equivalence_constitution.v1"
)
EQUIVALENCE_ARTIFACT_SCHEMA_VERSION = (
    "shardloom.front_door_performance_equivalence_artifact.v1"
)
REQUIRED_EQUIVALENCE_SCENARIOS = {
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
REQUIRED_EQUIVALENCE_FRONT_DOORS = {"SQL", "Python", "DataFrame"}
REQUIRED_EQUIVALENCE_TIMING_FIELDS = {
    "front_door_id",
    "scenario_id",
    "route_id",
    "route_lane_id",
    "timing_surface",
    "actual_evidence_tier",
    "preparation_millis",
    "query_runtime_millis",
    "result_sink_millis",
    "evidence_render_millis",
    "front_door_lowering_overhead_millis",
    "route_total_ms",
    "route_total_formula",
    "fallback_attempted",
    "external_engine_invoked",
}
REQUIRED_EQUIVALENCE_EVIDENCE_FIELDS = {
    "vortex_input_normalization_boundary",
    "native_vortex_unified_plan_contract",
    "runtime_execution_certificate_id",
    "native_io_certificate_id",
    "correctness_digest",
    "fallback_attempted",
    "external_engine_invoked",
}
REQUIRED_MISSING_EVIDENCE = (
    "public_claim_review_for_front_door_performance_equivalence",
    "independent_claim_grade_benchmark_rerun",
    "public_release_notes_claim_approval",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--equivalence-constitution",
        type=Path,
        default=DEFAULT_EQUIVALENCE_CONSTITUTION,
    )
    parser.add_argument(
        "--equivalence-artifact",
        type=Path,
        default=DEFAULT_EQUIVALENCE_ARTIFACT,
    )
    parser.add_argument(
        "--pre-5j-dependency-report",
        type=Path,
        default=DEFAULT_PRE_5J_DEPENDENCY_REPORT,
    )
    parser.add_argument("--allow-incomplete", action="store_true")
    parser.add_argument("--allow-stale-git", action="store_true")
    parser.add_argument("--allow-dirty-worktree", action="store_true")
    parser.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS)
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def row_by_id(rows: Any, row_id: str) -> dict[str, Any]:
    if not isinstance(rows, list):
        return {}
    for row in rows:
        if isinstance(row, dict) and row.get("row_id") == row_id:
            return row
    return {}


def public_front_door_summary(publication_claim_gate: dict[str, Any]) -> dict[str, Any]:
    summary = publication_claim_gate.get("public_front_door_benchmark_rows")
    return summary if isinstance(summary, dict) else {}


def load_json_object(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def validate_equivalence_constitution(constitution: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if constitution.get("schema_version") != EQUIVALENCE_CONSTITUTION_SCHEMA_VERSION:
        blockers.append(
            "front-door equivalence constitution schema mismatch: "
            + str(constitution.get("schema_version", "missing"))
        )
    if constitution.get("status") != "local_constitution_ready":
        blockers.append("front-door equivalence constitution must be local_constitution_ready")
    for field in (
        "benchmark_run_performed",
        "performance_claim_allowed",
        "front_door_performance_equivalence_claim_allowed",
        "fallback_attempted",
        "external_engine_invoked",
    ):
        if constitution.get(field) is not False:
            blockers.append(f"front-door equivalence constitution {field} must be false")
    if constitution.get("claim_gate_status") != "not_claim_grade":
        blockers.append(
            "front-door equivalence constitution claim_gate_status="
            + str(constitution.get("claim_gate_status", "missing"))
        )
    if constitution.get("default_timing_surface") != "hot_runtime":
        blockers.append("front-door equivalence constitution must default to hot_runtime")
    if constitution.get("proof_surfaces_separated") is not True:
        blockers.append("front-door equivalence constitution must separate proof surfaces")
    if constitution.get("sequential_local_device_default") is not True:
        blockers.append("front-door equivalence constitution must default to sequential local runs")
    front_doors = {
        str(value)
        for value in constitution.get("required_front_doors", [])
        if isinstance(value, str)
    }
    if front_doors != REQUIRED_EQUIVALENCE_FRONT_DOORS:
        blockers.append(
            "front-door equivalence constitution required_front_doors mismatch: "
            + ",".join(sorted(front_doors))
        )
    timing_fields = {
        str(value)
        for value in constitution.get("required_timing_fields", [])
        if isinstance(value, str)
    }
    missing_timing_fields = sorted(REQUIRED_EQUIVALENCE_TIMING_FIELDS - timing_fields)
    if missing_timing_fields:
        blockers.append(
            "front-door equivalence constitution missing timing fields: "
            + ",".join(missing_timing_fields)
        )
    evidence_fields = {
        str(value)
        for value in constitution.get("required_evidence_fields", [])
        if isinstance(value, str)
    }
    missing_evidence_fields = sorted(REQUIRED_EQUIVALENCE_EVIDENCE_FIELDS - evidence_fields)
    if missing_evidence_fields:
        blockers.append(
            "front-door equivalence constitution missing evidence fields: "
            + ",".join(missing_evidence_fields)
        )
    workloads = constitution.get("equivalence_workloads")
    if not isinstance(workloads, list):
        blockers.append("front-door equivalence constitution workloads must be a list")
        return blockers
    scenario_ids = {
        str(row.get("scenario_id"))
        for row in workloads
        if isinstance(row, dict) and row.get("scenario_id")
    }
    if scenario_ids != REQUIRED_EQUIVALENCE_SCENARIOS:
        blockers.append(
            "front-door equivalence constitution scenario ids mismatch: "
            + json.dumps(
                {
                    "expected": sorted(REQUIRED_EQUIVALENCE_SCENARIOS),
                    "actual": sorted(scenario_ids),
                },
                sort_keys=True,
            )
        )
    for row in workloads:
        if not isinstance(row, dict):
            blockers.append("front-door equivalence constitution workload rows must be objects")
            continue
        scenario_id = str(row.get("scenario_id", "missing"))
        row_front_doors = {
            str(value)
            for value in row.get("required_front_doors", [])
            if isinstance(value, str)
        }
        if row_front_doors != REQUIRED_EQUIVALENCE_FRONT_DOORS:
            blockers.append(f"{scenario_id}: workload front-door set mismatch")
        if row.get("runtime_family") != "native_vortex_unified_plan":
            blockers.append(f"{scenario_id}: workload must use native_vortex_unified_plan")
        if row.get("timing_surface") != "hot_runtime":
            blockers.append(f"{scenario_id}: workload must use hot_runtime timing surface")
    return blockers


def validate_structure(
    *,
    parity_report: dict[str, Any],
    publication_claim_gate: dict[str, Any],
    equivalence_constitution: dict[str, Any],
    equivalence_artifact_report: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    blockers.extend(validate_equivalence_constitution(equivalence_constitution))
    if parity_report.get("schema_version") != SQL_PYTHON_DATAFRAME_PARITY_SCHEMA_VERSION:
        blockers.append(
            "SQL/Python/DataFrame parity schema mismatch: "
            + str(parity_report.get("schema_version", "missing"))
        )
    if parity_report.get("status") != "passed":
        blockers.extend(
            "SQL/Python/DataFrame parity: " + str(blocker)
            for blocker in parity_report.get("blockers", ["gate blocked"])
        )
    if parity_report.get("scoped_local_front_door_parity_supported") is not True:
        blockers.append("scoped local front-door parity must be supported")
    if parity_report.get("all_no_fallback_no_external_engine") is not True:
        blockers.append("front-door parity must preserve no fallback and no external engine")
    for field in ("flexible_anything_claim_allowed", "performance_equivalence_claim_allowed"):
        if parity_report.get(field) is not False:
            blockers.append(f"SQL/Python/DataFrame parity {field} must be false")
    if parity_report.get("claim_gate_status") != "not_claim_grade":
        blockers.append(
            "SQL/Python/DataFrame parity claim_gate_status="
            + str(parity_report.get("claim_gate_status", "missing"))
        )

    performance_row = row_by_id(parity_report.get("rows"), "performance_equivalence")
    if not performance_row:
        blockers.append("missing performance_equivalence parity row")
    else:
        if performance_row.get("runtime_gap_status") != "admitted_scope":
            blockers.append("performance_equivalence row must be admitted after local artifact")
        if performance_row.get("parity_status") != "equivalent_admitted_scope":
            blockers.append("performance_equivalence row must be equivalent_admitted_scope")
        if performance_row.get("blocker_id") is not None:
            blockers.append("performance_equivalence row must not carry blocker_id")
        if "no_benchmark_claim" not in str(
            performance_row.get("performance_equivalence_status", "")
        ):
            blockers.append("performance_equivalence row must avoid public benchmark claim")
        if performance_row.get("fallback_attempted") is not False:
            blockers.append("performance_equivalence row fallback_attempted must be false")
        if performance_row.get("external_engine_invoked") is not False:
            blockers.append("performance_equivalence row external_engine_invoked must be false")

    if publication_claim_gate.get("schema_version") != BENCHMARK_PUBLICATION_SCHEMA_VERSION:
        blockers.append(
            "benchmark publication claim gate schema mismatch: "
            + str(publication_claim_gate.get("schema_version", "missing"))
        )
    front_door_rows = public_front_door_summary(publication_claim_gate)
    if front_door_rows.get("schema_version") != PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION:
        blockers.append("public front-door benchmark row schema mismatch")
    ids = {
        str(item)
        for item in front_door_rows.get("front_door_ids", [])
        if isinstance(item, str)
    }
    if ids != REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS:
        blockers.append(
            "public front-door benchmark rows must expose exactly: "
            + ",".join(sorted(REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS))
        )
    if front_door_rows.get("missing_front_door_ids"):
        blockers.append("public front-door benchmark rows have missing ids")
    if int(front_door_rows.get("invalid_example_count", 0) or 0) != 0:
        blockers.append("public front-door benchmark rows have invalid examples")
    blockers.extend(equivalence_artifact_report.get("blockers", []))
    return blockers


def validate_equivalence_artifact(path: Path) -> dict[str, Any]:
    blockers: list[str] = []
    if not path.exists():
        return {
            "present": False,
            "status": "missing",
            "row_count": 0,
            "scenario_ids": [],
            "front_door_ids": [],
            "blockers": [f"missing front-door equivalence artifact: {path}"],
        }
    artifact = load_json_object(path)
    if artifact.get("schema_version") != EQUIVALENCE_ARTIFACT_SCHEMA_VERSION:
        blockers.append(
            "front-door equivalence artifact schema mismatch: "
            + str(artifact.get("schema_version", "missing"))
        )
    if artifact.get("status") != "passed":
        blockers.append("front-door equivalence artifact status must be passed")
    for field in (
        "front_door_performance_equivalence_claim_allowed",
        "performance_claim_allowed",
        "production_claim_allowed",
        "spark_replacement_claim_allowed",
        "fallback_attempted",
        "external_engine_invoked",
    ):
        if artifact.get(field) is not False:
            blockers.append(f"front-door equivalence artifact {field} must be false")
    if artifact.get("benchmark_run_performed") is not True:
        blockers.append("front-door equivalence artifact benchmark_run_performed must be true")
    if artifact.get("sequential_local_device_default") is not True:
        blockers.append("front-door equivalence artifact must record sequential local execution")
    if artifact.get("timing_surface") != "hot_runtime":
        blockers.append("front-door equivalence artifact timing_surface must be hot_runtime")
    if artifact.get("actual_evidence_tier") != "metadata_sink":
        blockers.append("front-door equivalence artifact evidence tier must be metadata_sink")
    rows = artifact.get("rows")
    if not isinstance(rows, list):
        rows = []
        blockers.append("front-door equivalence artifact rows must be a list")
    scenario_ids = {
        str(row.get("scenario_id"))
        for row in rows
        if isinstance(row, dict) and row.get("scenario_id")
    }
    front_door_ids = {
        str(row.get("front_door_id"))
        for row in rows
        if isinstance(row, dict) and row.get("front_door_id")
    }
    if scenario_ids != REQUIRED_EQUIVALENCE_SCENARIOS:
        blockers.append(
            "front-door equivalence artifact scenario ids mismatch: "
            + json.dumps(
                {
                    "expected": sorted(REQUIRED_EQUIVALENCE_SCENARIOS),
                    "actual": sorted(scenario_ids),
                },
                sort_keys=True,
            )
        )
    if front_door_ids != REQUIRED_EQUIVALENCE_FRONT_DOORS:
        blockers.append(
            "front-door equivalence artifact front doors mismatch: "
            + ",".join(sorted(front_door_ids))
        )
    expected_row_count = len(REQUIRED_EQUIVALENCE_SCENARIOS) * len(
        REQUIRED_EQUIVALENCE_FRONT_DOORS
    )
    if len(rows) != expected_row_count or artifact.get("row_count") != expected_row_count:
        blockers.append(
            f"front-door equivalence artifact must contain {expected_row_count} rows"
        )
    required_row_fields = REQUIRED_EQUIVALENCE_TIMING_FIELDS.union(
        REQUIRED_EQUIVALENCE_EVIDENCE_FIELDS
    )
    by_scenario_digest: dict[str, set[str]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            blockers.append(f"front-door equivalence row {index} must be an object")
            continue
        missing = sorted(field for field in required_row_fields if field not in row)
        if missing:
            blockers.append(
                f"front-door equivalence row {index} missing fields: "
                + ",".join(missing[:8])
            )
        if row.get("timing_surface") != "hot_runtime":
            blockers.append(f"front-door equivalence row {index} timing_surface")
        if row.get("actual_evidence_tier") != "metadata_sink":
            blockers.append(f"front-door equivalence row {index} actual_evidence_tier")
        if row.get("fallback_attempted") is not False:
            blockers.append(f"front-door equivalence row {index} fallback_attempted")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"front-door equivalence row {index} external_engine_invoked")
        if row.get("route_id") != "native_vortex_unified_plan":
            blockers.append(f"front-door equivalence row {index} route_id")
        scenario = str(row.get("scenario_id", ""))
        digest = str(row.get("correctness_digest", ""))
        if scenario and digest:
            by_scenario_digest.setdefault(scenario, set()).add(digest)
    divergent = sorted(
        scenario for scenario, digests in by_scenario_digest.items() if len(digests) != 1
    )
    if divergent:
        blockers.append(
            "front-door equivalence correctness digest mismatch: " + ",".join(divergent)
        )
    return {
        "present": True,
        "status": "passed" if not blockers else "blocked",
        "schema_version": artifact.get("schema_version"),
        "artifact": str(path),
        "row_count": len(rows),
        "scenario_ids": sorted(scenario_ids),
        "front_door_ids": sorted(front_door_ids),
        "blockers": blockers,
    }


def build_report(
    repo_root: Path = ROOT,
    *,
    manifest_path: Path = DEFAULT_MANIFEST,
    pre_5j_dependency_report_path: Path = DEFAULT_PRE_5J_DEPENDENCY_REPORT,
    equivalence_constitution_path: Path = DEFAULT_EQUIVALENCE_CONSTITUTION,
    equivalence_artifact_path: Path = DEFAULT_EQUIVALENCE_ARTIFACT,
    allow_incomplete: bool = False,
    require_current_git: bool = True,
    allow_dirty_worktree: bool = False,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    parity_report: dict[str, Any] | None = None,
    publication_claim_gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    resolved_manifest = resolve(repo_root, manifest_path)
    resolved_pre_5j = resolve(repo_root, pre_5j_dependency_report_path)
    resolved_constitution = resolve(repo_root, equivalence_constitution_path)
    resolved_equivalence_artifact = resolve(repo_root, equivalence_artifact_path)
    parity = parity_report or build_sql_python_dataframe_parity_report(repo_root)
    constitution = load_json_object(resolved_constitution)
    equivalence_artifact = validate_equivalence_artifact(resolved_equivalence_artifact)
    publication = publication_claim_gate or validate_publication_claim_gate(
        resolved_manifest,
        repo_root=repo_root,
        pre_5j_dependency_report_path=resolved_pre_5j,
        allow_incomplete=allow_incomplete,
        require_current_git=require_current_git,
        allow_dirty_worktree=allow_dirty_worktree,
        max_age_days=max_age_days,
    )
    structural_blockers = validate_structure(
        parity_report=parity,
        publication_claim_gate=publication,
        equivalence_constitution=constitution,
        equivalence_artifact_report=equivalence_artifact,
    )
    publication_claim_blockers = list(publication.get("blockers", []))
    publication_admission_blockers = [
        "front-door performance equivalence remains local technical-preview evidence",
        "public claim-grade benchmark review is not approved",
        *[
            f"benchmark publication claim gate: {blocker}"
            for blocker in publication_claim_blockers
        ],
    ]
    front_door_rows = public_front_door_summary(publication)
    passed = not structural_blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "status": "passed" if passed else "blocked",
        "front_door_performance_publication_status": (
            FRONT_DOOR_PERFORMANCE_PUBLICATION_READY
        ),
        "claim_gate_status": "not_claim_grade",
        "front_door_performance_equivalence_claim_allowed": False,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "spark_replacement_claim_allowed": False,
        "benchmark_run_performed": bool(equivalence_artifact.get("present")),
        "benchmark_rerun_approved": True,
        "laptop_safe_sequential_controls_confirmed": True,
        "measured_front_door_equivalence_artifact_present": bool(
            equivalence_artifact.get("present")
        ),
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "manifest": str(resolved_manifest),
        "pre_5j_dependency_report": str(resolved_pre_5j),
        "front_door_equivalence_constitution": str(resolved_constitution),
        "front_door_equivalence_artifact": str(resolved_equivalence_artifact),
        "front_door_equivalence_artifact_status": equivalence_artifact.get("status"),
        "front_door_equivalence_artifact_row_count": equivalence_artifact.get(
            "row_count", 0
        ),
        "front_door_equivalence_artifact_front_door_ids": equivalence_artifact.get(
            "front_door_ids", []
        ),
        "front_door_equivalence_artifact_scenario_ids": equivalence_artifact.get(
            "scenario_ids", []
        ),
        "front_door_equivalence_constitution_status": constitution.get("status"),
        "front_door_equivalence_constitution_workload_count": len(
            constitution.get("equivalence_workloads", [])
        )
        if isinstance(constitution.get("equivalence_workloads"), list)
        else 0,
        "front_door_equivalence_constitution_timing_fields": constitution.get(
            "required_timing_fields", []
        ),
        "front_door_equivalence_constitution_evidence_fields": constitution.get(
            "required_evidence_fields", []
        ),
        "sql_python_dataframe_parity_status": parity.get("status"),
        "scoped_local_front_door_parity_supported": parity.get(
            "scoped_local_front_door_parity_supported"
        ),
        "parity_remaining_gap_row_ids": parity.get("remaining_gap_row_ids", []),
        "benchmark_publication_claim_gate_status": publication.get("status"),
        "benchmark_publication_claim_gate_blocker_count": len(publication_claim_blockers),
        "benchmark_publication_claim_gate_blockers": publication_claim_blockers,
        "public_front_door_benchmark_schema_version": front_door_rows.get(
            "schema_version"
        ),
        "public_front_door_benchmark_row_count": front_door_rows.get("row_count", 0),
        "public_front_door_benchmark_row_ids": front_door_rows.get("front_door_ids", []),
        "public_front_door_benchmark_invalid_example_count": front_door_rows.get(
            "invalid_example_count", 0
        ),
        "missing_claim_grade_evidence": list(REQUIRED_MISSING_EVIDENCE),
        "publication_admission_blockers": publication_admission_blockers,
        "claim_boundary": (
            "This gate closes the current front-door benchmark publication phase as scoped "
            "local equivalence evidence. It permits publication of local route-equivalence "
            "rows, but it does not permit public performance-equivalence, superiority, "
            "production, package, or Spark-replacement claims."
        ),
        "fallback_boundary": (
            "External engines remain benchmark baselines only; ShardLoom front-door rows must "
            "preserve fallback_attempted=false and external_engine_invoked=false."
        ),
        "blockers": structural_blockers,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(
        repo_root,
        manifest_path=args.manifest,
        pre_5j_dependency_report_path=args.pre_5j_dependency_report,
        equivalence_constitution_path=args.equivalence_constitution,
        equivalence_artifact_path=args.equivalence_artifact,
        allow_incomplete=args.allow_incomplete,
        require_current_git=not args.allow_stale_git,
        allow_dirty_worktree=args.allow_dirty_worktree,
        max_age_days=args.max_age_days,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
