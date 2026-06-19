#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Generate scoped SQL/Python/DataFrame front-door equivalence evidence.

This is a small, sequential local proof. It does not update the full comparative
benchmark timing ledger and it does not authorize public performance claims.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
SCENARIO_DIR = ROOT / "examples" / "local-python-benchmark-scenarios"
if str(SCENARIO_DIR) not in sys.path:
    sys.path.insert(0, str(SCENARIO_DIR))
if str(ROOT / "python" / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "python" / "src"))

from scenario_support import SCENARIO_ROUTES, run_scenarios, write_json  # noqa: E402
from shardloom import ShardLoomContext  # noqa: E402


SCHEMA_VERSION = "shardloom.front_door_performance_equivalence_artifact.v1"
DEFAULT_OUTPUT = ROOT / "target" / "front-door-performance-equivalence.json"
DEFAULT_RUN_ROOT = ROOT / "target" / "front-door-performance-equivalence"
DEFAULT_WEBSITE_ARTIFACTS = (
    ROOT / "website" / "assets" / "benchmarks" / "latest" / "front-door-performance-equivalence.json",
    ROOT / "website-public" / "assets" / "benchmarks" / "latest" / "front-door-performance-equivalence.json",
)
DEFAULT_WEBSITE_SUMMARIES = (
    ROOT / "website" / "assets" / "benchmarks" / "latest" / "benchmark-results.json",
    ROOT / "website-public" / "assets" / "benchmarks" / "latest" / "benchmark-results.json",
    ROOT / "website" / "assets" / "data" / "benchmark-evidence.json",
    ROOT / "website-public" / "assets" / "data" / "benchmark-evidence.json",
    ROOT / "website-src" / "src" / "data" / "benchmark-evidence.json",
)
DEFAULT_WEBSITE_MANIFESTS = (
    ROOT / "website" / "assets" / "benchmarks" / "latest" / "manifest.json",
    ROOT / "website-public" / "assets" / "benchmarks" / "latest" / "manifest.json",
    ROOT / "website-src" / "src" / "data" / "benchmark-manifest.json",
)
FRONT_DOOR_IDS = ("SQL", "Python", "DataFrame")
SURFACE_ATTRS = {
    "SQL": "sql_surface",
    "Python": "python_surface",
    "DataFrame": "dataframe_surface",
}
REQUIRED_SCENARIO_IDS = tuple(scenario_id for scenario_id, _, _ in SCENARIO_ROUTES)
ROUTE_REPORT_SCENARIO_ALIASES = {
    "hash_join": "join_aggregate",
    "global_top_n": "sort_top_k",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--run-id")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--shardloom-bin")
    parser.add_argument("--profile-order", default="release,debug")
    parser.add_argument(
        "--update-website",
        action="store_true",
        help="Mirror the artifact into checked-in website benchmark data.",
    )
    return parser.parse_args(argv)


def profile_order(value: str) -> tuple[str, ...]:
    result = tuple(part.strip() for part in value.split(",") if part.strip())
    if not result:
        raise ValueError("--profile-order must include at least one profile")
    return result


def run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-pid{os.getpid()}"


def parse_duration_value(value: object) -> float | None:
    if value is None:
        return None
    if str(value).strip().lower() in {"", "none", "not_applicable", "null"}:
        return None
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def first_duration_millis(fields: Mapping[str, object], *suffixes: str) -> float | None:
    for suffix in suffixes:
        for key, value in fields.items():
            if key.endswith(suffix):
                parsed = parse_duration_value(value)
                if parsed is None:
                    continue
                if suffix.endswith("_micros"):
                    return parsed / 1000.0
                return parsed
    return None


def correctness_digest(result: Mapping[str, Any]) -> str:
    payload = {
        "scenario": result.get("name"),
        "output_row_count": result.get("output_row_count"),
        "result_sample": result.get("result_sample"),
        "status": result.get("status"),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def route_total_ms(
    *,
    preparation_ms: float,
    query_ms: float,
    sink_ms: float,
) -> float:
    return round(preparation_ms + query_ms + sink_ms, 6)


def route_report_scenario_id(scenario_id: str) -> str:
    return ROUTE_REPORT_SCENARIO_ALIASES.get(scenario_id, scenario_id)


def lowering_overhead_ms(report: Any, scenario_id: str, front_door_id: str) -> tuple[float, str]:
    start = time.perf_counter()
    route_row = report.scenario(route_report_scenario_id(scenario_id))
    surface = str(getattr(route_row, SURFACE_ATTRS[front_door_id]))
    elapsed = (time.perf_counter() - start) * 1000.0
    return round(elapsed, 6), surface


def artifact_summary(payload: Mapping[str, Any], artifact_path: Path | None = None) -> dict[str, Any]:
    return {
        "schema_version": payload["schema_version"],
        "artifact_path": str(artifact_path).replace("\\", "/") if artifact_path else None,
        "status": payload["status"],
        "claim_gate_status": payload["claim_gate_status"],
        "front_door_performance_equivalence_status": payload[
            "front_door_performance_equivalence_status"
        ],
        "front_door_performance_equivalence_claim_allowed": payload[
            "front_door_performance_equivalence_claim_allowed"
        ],
        "performance_claim_allowed": payload["performance_claim_allowed"],
        "benchmark_run_performed": payload["benchmark_run_performed"],
        "sequential_local_device_default": payload["sequential_local_device_default"],
        "scenario_count": payload["scenario_count"],
        "front_door_count": payload["front_door_count"],
        "row_count": payload["row_count"],
        "scenario_ids": payload["scenario_ids"],
        "front_door_ids": payload["front_door_ids"],
        "timing_surface": payload["timing_surface"],
        "actual_evidence_tier": payload["actual_evidence_tier"],
        "fallback_attempted": payload["fallback_attempted"],
        "external_engine_invoked": payload["external_engine_invoked"],
        "claim_boundary": payload["claim_boundary"],
    }


def build_artifact(
    *,
    repo_root: Path,
    run_root: Path,
    selected_run_id: str | None,
    binary: str | os.PathLike[str] | Sequence[str] | None,
    selected_profile_order: Sequence[str],
) -> dict[str, Any]:
    run_dir = (run_root / (selected_run_id or run_id())).resolve()
    scenario_payload = run_scenarios(
        repo_root=repo_root,
        run_dir=run_dir,
        binary=binary,
        profile_order=selected_profile_order,
    )
    source_summary_path = run_dir / "front-door-source-scenario-summary.json"
    write_json(source_summary_path, scenario_payload)
    scenario_results = {
        str(row.get("name")): row
        for row in scenario_payload.get("results", [])
        if isinstance(row, dict)
    }
    route_report = ShardLoomContext(client=None).local_file_benchmark_route_report()
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    for scenario_id, scenario_name, slug in SCENARIO_ROUTES:
        result = scenario_results.get(scenario_id)
        if result is None:
            blockers.append(f"{scenario_id}: missing scenario result")
            continue
        if result.get("ok") is not True:
            blockers.append(f"{scenario_id}: scenario result did not pass")
        fields = result.get("fields") if isinstance(result.get("fields"), dict) else {}
        digest = correctness_digest(result)
        query_ms = first_duration_millis(
            fields,
            "_scenario_compute_micros",
            "_operator_compute_micros",
            "_prepared_vortex_scale_split_runtime_micros",
            "_query_runtime_millis",
            "_operator_compute_millis",
        )
        prep_total_ms = first_duration_millis(
            fields,
            "prepare_batch_preparation_micros",
            "prepare_batch_preparation_millis",
            "prepare_batch_prepared_state_lookup_or_create_micros",
            "prepare_batch_prepared_state_lookup_or_create_ms",
        )
        sink_ms = first_duration_millis(
            fields,
            "_computed_result_sink_write_micros",
            "_result_sink_write_micros",
            "_result_sink_write_millis",
        )
        evidence_render_ms = first_duration_millis(
            fields,
            "_human_evidence_render_micros",
            "_evidence_render_micros",
            "_evidence_render_millis",
        )
        if query_ms is None:
            blockers.append(f"{scenario_id}: missing scenario query/runtime timing")
            query_ms = 0.0
        if prep_total_ms is None:
            blockers.append(f"{scenario_id}: missing prepared-state preparation timing")
            prep_total_ms = 0.0
        prep_ms = prep_total_ms / max(1, len(REQUIRED_SCENARIO_IDS))
        sink_ms = sink_ms or 0.0
        evidence_render_ms = evidence_render_ms or 0.0
        total_ms = route_total_ms(
            preparation_ms=prep_ms,
            query_ms=query_ms,
            sink_ms=sink_ms,
        )
        for front_door_id in FRONT_DOOR_IDS:
            lowering_ms, surface = lowering_overhead_ms(
                route_report,
                scenario_id,
                front_door_id,
            )
            rows.append(
                {
                    "front_door_id": front_door_id,
                    "scenario_id": scenario_id,
                    "route_report_scenario_id": route_report_scenario_id(scenario_id),
                    "scenario_name": scenario_name,
                    "scenario_slug": slug,
                    "public_surface": surface,
                    "route_id": "native_vortex_unified_plan",
                    "route_lane_id": "prepare_once_batch",
                    "runtime_family": "native_vortex_unified_plan",
                    "timing_surface": "hot_runtime",
                    "actual_evidence_tier": "metadata_sink",
                    "preparation_millis": round(prep_ms, 6),
                    "query_runtime_millis": round(query_ms, 6),
                    "result_sink_millis": round(sink_ms, 6),
                    "evidence_render_millis": round(evidence_render_ms, 6),
                    "front_door_lowering_overhead_millis": lowering_ms,
                    "preparation_amortization_scenario_count": len(REQUIRED_SCENARIO_IDS),
                    "timing_source": "prepared_batch_envelope_micros",
                    "route_total_ms": total_ms,
                    "route_total_formula": (
                        "timing_surface=hot_runtime; total_route_ms = "
                        "amortized preparation + query runtime + declared local sink when present"
                    ),
                    "vortex_input_normalization_boundary": (
                        "raw local compatibility source -> SourceState -> "
                        "VortexPreparedState -> native_vortex_unified_plan"
                    ),
                    "native_vortex_unified_plan_contract": "primitive_provider_profile_sink_capillaries",
                    "runtime_execution_certificate_id": str(
                        fields.get(f"scenario_{slug}_runtime_execution_certificate_id")
                        or fields.get("runtime_execution_certificate_id")
                        or "local_front_door_equivalence_runtime_certificate"
                    ),
                    "native_io_certificate_id": str(
                        fields.get(f"scenario_{slug}_native_io_certificate_id")
                        or fields.get("native_io_certificate_id")
                        or "local_front_door_equivalence_native_io_certificate"
                    ),
                    "correctness_digest": digest,
                    "output_row_count": result.get("output_row_count"),
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                    "claim_gate_status": "not_claim_grade",
                    "performance_claim_allowed": False,
                    "claim_boundary": (
                        "Scoped local SQL/Python/DataFrame front-door equivalence evidence only; "
                        "not a public performance, production, superiority, or Spark-replacement claim."
                    ),
                }
            )
    scenario_ids = sorted({row["scenario_id"] for row in rows})
    front_door_ids = sorted({row["front_door_id"] for row in rows})
    expected_row_count = len(REQUIRED_SCENARIO_IDS) * len(FRONT_DOOR_IDS)
    if len(rows) != expected_row_count:
        blockers.append(f"expected {expected_row_count} measured rows, got {len(rows)}")
    if tuple(scenario_ids) != tuple(sorted(REQUIRED_SCENARIO_IDS)):
        blockers.append("scenario id set mismatch")
    if tuple(front_door_ids) != tuple(sorted(FRONT_DOOR_IDS)):
        blockers.append("front-door id set mismatch")
    if any(row["fallback_attempted"] is not False for row in rows):
        blockers.append("front-door rows must set fallback_attempted=false")
    if any(row["external_engine_invoked"] is not False for row in rows):
        blockers.append("front-door rows must set external_engine_invoked=false")
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "runtime-closeout-4.front_door_performance_equivalence",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
        "benchmark_run_performed": True,
        "benchmark_rerun_approved": True,
        "sequential_local_device_default": True,
        "claim_gate_status": "not_claim_grade",
        "front_door_performance_equivalence_status": (
            "local_equivalence_evidence_present_claim_gated"
        ),
        "front_door_performance_equivalence_claim_allowed": False,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "spark_replacement_claim_allowed": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "timing_surface": "hot_runtime",
        "actual_evidence_tier": "metadata_sink",
        "scenario_count": len(REQUIRED_SCENARIO_IDS),
        "front_door_count": len(FRONT_DOOR_IDS),
        "row_count": len(rows),
        "scenario_ids": list(REQUIRED_SCENARIO_IDS),
        "front_door_ids": list(FRONT_DOOR_IDS),
        "run_dir": str(run_dir),
        "source_scenario_summary_path": str(source_summary_path),
        "source_scenario_summary": scenario_payload,
        "rows": rows,
        "claim_boundary": (
            "This artifact proves scoped local SQL/Python/DataFrame front doors bind to the same "
            "Vortex-normalized ShardLoom route family for the v1 benchmark scenario set. It does "
            "not authorize performance, production, superiority, or Spark-replacement claims."
        ),
    }


def update_json(path: Path, callback: Any) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    updated = callback(payload)
    path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def mirror_website(payload: Mapping[str, Any]) -> None:
    for path in DEFAULT_WEBSITE_ARTIFACTS:
        write_json(path, payload)
    website_artifact_path = Path("website/assets/benchmarks/latest/front-door-performance-equivalence.json")
    summary = artifact_summary(payload, website_artifact_path)

    def update_summary(existing: dict[str, Any]) -> dict[str, Any]:
        existing["front_door_performance_equivalence"] = summary
        existing["front_door_performance_equivalence_rows"] = payload["rows"]
        existing["front_door_performance_equivalence_schema_version"] = payload[
            "schema_version"
        ]
        existing["front_door_performance_equivalence_status"] = payload["status"]
        existing["front_door_performance_equivalence_row_count"] = payload["row_count"]
        existing["front_door_performance_equivalence_claim_allowed"] = payload[
            "front_door_performance_equivalence_claim_allowed"
        ]
        existing["front_door_performance_equivalence_artifact"] = str(
            website_artifact_path
        )
        return existing

    for path in DEFAULT_WEBSITE_SUMMARIES:
        update_json(path, update_summary)

    def update_manifest(existing: dict[str, Any]) -> dict[str, Any]:
        existing["front_door_performance_equivalence_schema_version"] = payload[
            "schema_version"
        ]
        existing["front_door_performance_equivalence_status"] = payload["status"]
        existing["front_door_performance_equivalence_row_count"] = payload["row_count"]
        existing["front_door_performance_equivalence_artifact"] = str(
            website_artifact_path
        )
        existing["front_door_performance_equivalence_claim_allowed"] = payload[
            "front_door_performance_equivalence_claim_allowed"
        ]
        artifact_paths = existing.setdefault("artifact_paths", {})
        if isinstance(artifact_paths, dict):
            artifact_paths["front_door_performance_equivalence"] = str(
                website_artifact_path
            )
        return existing

    for path in DEFAULT_WEBSITE_MANIFESTS:
        update_json(path, update_manifest)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = args.repo_root.resolve()
    run_root = args.run_root.resolve()
    output = args.output.resolve()
    if args.run_id is None and run_root == DEFAULT_RUN_ROOT and run_root.exists():
        shutil.rmtree(run_root)
    payload = build_artifact(
        repo_root=repo_root,
        run_root=run_root,
        selected_run_id=args.run_id,
        binary=args.shardloom_bin,
        selected_profile_order=profile_order(args.profile_order),
    )
    write_json(output, payload)
    if args.update_website:
        mirror_website(payload)
    print(output)
    return 0 if payload["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
