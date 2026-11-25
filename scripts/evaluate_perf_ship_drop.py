#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Evaluate baseline-vs-candidate performance summaries under PERF-01 or binary complete-cohort policy."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "shardloom.perf_ship_drop_decision.v1"
POLICIES = ("perf01_thresholds", "binary_complete_cohort")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _select_lane(summary: dict[str, Any], lane: str | None) -> dict[str, Any]:
    if lane is None:
        return summary
    lanes = summary.get("lanes")
    if isinstance(lanes, dict) and lane in lanes and isinstance(lanes[lane], dict):
        return lanes[lane]
    if summary.get("timing_surface") == lane or (
        lane == "cold_process_cli" and "timing_surface" not in summary
    ):
        return summary
    raise ValueError(f"summary does not contain requested lane {lane!r}")


def _pct_delta(baseline: float, candidate: float) -> float | None:
    if baseline == 0.0:
        return 0.0 if candidate == 0.0 else None
    return ((candidate - baseline) / baseline) * 100.0


def _best_by_query(records: list[dict[str, Any]], key: str) -> dict[int, float]:
    best: dict[int, float] = {}
    for record in records:
        value = record.get(key)
        query = record.get("query")
        if type(query) is not int or type(value) not in (int, float) or not math.isfinite(value):
            continue
        best[query] = min(best[query], float(value)) if query in best else float(value)
    return best


def _cohort_observations(records: list[dict[str, Any]], key: str) -> dict[tuple[int, int], float] | None:
    observed: dict[tuple[int, int], float] = {}
    for record in records:
        query = record.get("query")
        run = record.get("run")
        value = record.get(key)
        if (
            type(query) is not int
            or type(run) is not int
            or type(value) not in (int, float)
            or not math.isfinite(value)
            or float(value) < 0.0
        ):
            return None
        obs_key = (query, run)
        if obs_key in observed:
            return None
        observed[obs_key] = float(value)
    return observed or None


def _metric_aggregate(records: list[dict[str, Any]], key: str, mode: str) -> float | None:
    if not records:
        return None
    values = [
        float(record[key])
        for record in records
        if type(record.get(key)) in (int, float)
        and math.isfinite(record[key])
        and float(record[key]) >= 0.0
    ]
    if len(values) != len(records):
        return None
    return max(values) if mode == "max" else sum(values)


def _summary_resource_metric(
    summary: dict[str, Any],
    records: list[dict[str, Any]],
    key: str,
    mode: str,
) -> tuple[float | None, str | None]:
    if not records:
        return None, None
    if any(key in record for record in records):
        value = _metric_aggregate(records, key, mode)
        return (value, "per_query_records") if value is not None else (None, None)
    worker_usage = summary.get("worker_resource_usage")
    if isinstance(worker_usage, dict):
        rc = worker_usage.get("returncode", 0)
        value = worker_usage.get(key)
        if (
            type(rc) is int
            and rc == 0
            and type(value) in (int, float)
            and math.isfinite(value)
            and float(value) >= 0.0
        ):
            return float(value), "worker_resource_usage"
    return None, None


def _check_safety(summary: dict[str, Any], label: str, reasons: list[str]) -> bool:
    if not summary.get("complete"):
        reasons.append(f"{label} run is incomplete")
        return False
    if summary.get("runs_completed") != summary.get("runs_passed"):
        reasons.append(f"{label} run has failing queries")
        return False
    if summary.get("zero_answer_cache_verified") is False:
        reasons.append(f"{label} failed zero_answer_cache_verified check")
        return False
    return True


def evaluate_ship_drop(
    baseline_raw: dict[str, Any],
    candidate_raw: dict[str, Any],
    *,
    lane: str | None = None,
    policy: str = "perf01_thresholds",
    min_total_improvement_pct: float = 2.0,
    max_tail_regression_pct: float = 5.0,
    max_rss_growth_pct: float = 10.0,
    max_sys_cpu_growth_pct: float = 10.0,
    max_page_fault_growth_pct: float = 15.0,
) -> dict[str, Any]:
    if policy not in POLICIES:
        return {
            "schema_version": SCHEMA_VERSION,
            "policy": policy,
            "decision": "blocked",
            "reasons": [f"unsupported policy {policy!r}; expected one of {POLICIES}"],
            "claim_gate_status": "not_claim_grade",
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }

    blockers: list[str] = []
    drop_reasons: list[str] = []

    try:
        baseline = _select_lane(baseline_raw, lane)
        candidate = _select_lane(candidate_raw, lane)
    except ValueError as error:
        return {
            "schema_version": SCHEMA_VERSION,
            "policy": policy,
            "decision": "blocked",
            "reasons": [str(error)],
            "claim_gate_status": "not_claim_grade",
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }

    base_surface = baseline.get("timing_surface", "cold_process_cli")
    cand_surface = candidate.get("timing_surface", "cold_process_cli")
    if base_surface != cand_surface:
        blockers.append(f"timing_surface mismatch: {base_surface!r} != {cand_surface!r}")
    if baseline.get("input_format") != candidate.get("input_format"):
        blockers.append(
            f"input_format mismatch: {baseline.get('input_format')!r} != {candidate.get('input_format')!r}"
        )
    if baseline.get("selected_query_ids") != candidate.get("selected_query_ids"):
        blockers.append("selected_query_ids mismatch between baseline and candidate")

    for summary, label in ((baseline, "baseline"), (candidate, "candidate")):
        records = summary.get("records", [])
        for record in records:
            report = record.get("execution_report", {})
            if report.get("fallback_attempted") is True or report.get("public_workflow_fallback_attempted") == "true":
                drop_reasons.append(f"{label} query {record.get('query')} reported fallback_attempted=true")
            if report.get("external_engine_invoked") is True or report.get("public_workflow_external_engine_invoked") == "true":
                drop_reasons.append(f"{label} query {record.get('query')} reported external_engine_invoked=true")

    if not _check_safety(baseline, "baseline", blockers):
        pass
    if not _check_safety(candidate, "candidate", drop_reasons):
        pass

    base_records = baseline.get("records", [])
    cand_records = candidate.get("records", [])
    base_hashes = {
        (r.get("query"), r.get("run")): r.get("result_sha256")
        for r in base_records
        if "result_sha256" in r
    }
    cand_hashes = {
        (r.get("query"), r.get("run")): r.get("result_sha256")
        for r in cand_records
        if "result_sha256" in r
    }
    for key in sorted(set(base_hashes) & set(cand_hashes)):
        if base_hashes[key] != cand_hashes[key]:
            drop_reasons.append(f"result_sha256 mismatch at query={key[0]} run={key[1]}")
    if policy == "binary_complete_cohort" and (base_hashes or cand_hashes) and set(base_hashes) != set(cand_hashes):
        drop_reasons.append("result_sha256 coverage mismatch across (query, run) observations")

    if policy == "binary_complete_cohort":
        timing_key = "engine_execution_seconds"
        if baseline.get("timing_metric") != "engine_execution_seconds":
            blockers.append("baseline timing_metric must be 'engine_execution_seconds' for binary_complete_cohort")
        if candidate.get("timing_metric") != "engine_execution_seconds":
            blockers.append("candidate timing_metric must be 'engine_execution_seconds' for binary_complete_cohort")
        if baseline.get("timing_available") is False:
            blockers.append("baseline timing_available is false")
        if candidate.get("timing_available") is False:
            blockers.append("candidate timing_available is false")
    else:
        timing_key = (
            "engine_execution_seconds"
            if baseline.get("timing_metric") == "engine_execution_seconds"
            and candidate.get("timing_metric") == "engine_execution_seconds"
            else "seconds"
        )
    total_delta_pct = None
    max_query_regression_pct = None
    if policy == "binary_complete_cohort":
        expected_repeats: int | None
        if "repeats" in baseline or "repeats" in candidate:
            base_repeats = baseline.get("repeats")
            cand_repeats = candidate.get("repeats")
            if (
                "repeats" not in baseline
                or "repeats" not in candidate
                or type(base_repeats) is not int
                or type(cand_repeats) is not int
                or base_repeats <= 0
                or cand_repeats <= 0
                or base_repeats != cand_repeats
            ):
                blockers.append(f"repeats mismatch or invalid: {base_repeats!r} != {cand_repeats!r}")
                expected_repeats = None
            else:
                expected_repeats = base_repeats
        else:
            expected_repeats = 1

        base_selected = baseline.get("selected_query_ids")
        cand_selected = candidate.get("selected_query_ids")
        base_valid_selected = (
            isinstance(base_selected, list)
            and bool(base_selected)
            and all(type(q) is int for q in base_selected)
            and len(set(base_selected)) == len(base_selected)
        )
        cand_valid_selected = (
            isinstance(cand_selected, list)
            and bool(cand_selected)
            and all(type(q) is int for q in cand_selected)
            and len(set(cand_selected)) == len(cand_selected)
        )
        if not base_valid_selected:
            blockers.append("baseline selected_query_ids must be a non-empty list of unique integers")
        if not cand_valid_selected:
            blockers.append("candidate selected_query_ids must be a non-empty list of unique integers")

        base_obs = _cohort_observations(base_records, timing_key)
        cand_obs = _cohort_observations(cand_records, timing_key)
        if base_obs is None:
            blockers.append("baseline is missing complete engine_execution_seconds (query, run) observations")
        if cand_obs is None:
            blockers.append("candidate is missing complete engine_execution_seconds (query, run) observations")
        if base_obs is not None and cand_obs is not None and set(base_obs) != set(cand_obs):
            blockers.append("observation cohort (query, run) keys differ between baseline and candidate")
        if expected_repeats is not None:
            if base_obs is not None and base_valid_selected:
                expected_base_keys = {(q, r) for q in base_selected for r in range(1, expected_repeats + 1)}
                if set(base_obs) != expected_base_keys:
                    blockers.append("baseline timing observations do not cover complete (selected_query_ids, repeats) cohort")
            if cand_obs is not None and cand_valid_selected:
                expected_cand_keys = {(q, r) for q in cand_selected for r in range(1, expected_repeats + 1)}
                if set(cand_obs) != expected_cand_keys:
                    blockers.append("candidate timing observations do not cover complete (selected_query_ids, repeats) cohort")
        shared_queries = sorted({q for q, _ in base_obs} & {q for q, _ in cand_obs}) if (base_obs and cand_obs) else []
        if base_obs and cand_obs and not blockers:
            base_total = sum(base_obs.values())
            cand_total = sum(cand_obs.values())
            total_delta_pct = _pct_delta(base_total, cand_total)
            base_q_sum = {q: 0.0 for q in shared_queries}
            cand_q_sum = {q: 0.0 for q in shared_queries}
            for (q, _), val in base_obs.items():
                if q in base_q_sum:
                    base_q_sum[q] += val
            for (q, _), val in cand_obs.items():
                if q in cand_q_sum:
                    cand_q_sum[q] += val
            query_deltas = []
            for q in shared_queries:
                delta = _pct_delta(base_q_sum[q], cand_q_sum[q])
                if delta is None:
                    drop_reasons.append(f"query {q} baseline timing is zero while candidate is positive")
                else:
                    query_deltas.append((q, delta))
            if query_deltas:
                _, max_query_regression_pct = max(query_deltas, key=lambda item: item[1])
            if total_delta_pct is None or cand_total >= base_total:
                observed = "n/a" if total_delta_pct is None else f"{total_delta_pct:+.2f}%"
                drop_reasons.append(
                    f"complete-cohort total did not decrease ({base_total:.9f}s -> {cand_total:.9f}s, delta {observed})"
                )
    else:
        base_best = _best_by_query(base_records, timing_key)
        cand_best = _best_by_query(cand_records, timing_key)
        shared_queries = sorted(set(base_best) & set(cand_best))

        if not shared_queries or blockers:
            if not shared_queries:
                blockers.append("no shared timing measurements available")
        else:
            base_total = sum(base_best[q] for q in shared_queries)
            cand_total = sum(cand_best[q] for q in shared_queries)
            total_delta_pct = _pct_delta(base_total, cand_total)
            query_deltas = []
            for q in shared_queries:
                delta = _pct_delta(base_best[q], cand_best[q])
                if delta is None:
                    drop_reasons.append(f"query {q} baseline timing is zero while candidate is positive")
                else:
                    query_deltas.append((q, delta))
            if query_deltas:
                worst_q, max_query_regression_pct = max(query_deltas, key=lambda item: item[1])
            else:
                worst_q = None

            if total_delta_pct is None or -total_delta_pct < min_total_improvement_pct:
                observed = "n/a" if total_delta_pct is None else f"{-total_delta_pct:.2f}%"
                drop_reasons.append(
                    f"total improvement {observed} below required {min_total_improvement_pct:.2f}%"
                )
            if (
                max_query_regression_pct is not None
                and worst_q is not None
                and max_query_regression_pct > max_tail_regression_pct
            ):
                drop_reasons.append(
                    f"query {worst_q} regressed {max_query_regression_pct:.2f}% > max {max_tail_regression_pct:.2f}%"
                )

    resource_deltas: dict[str, float | None] = {}
    for metric_key, agg_mode, limit_pct, label in (
        ("native_peak_rss_bytes", "max", max_rss_growth_pct, "peak_rss"),
        ("system_cpu_seconds", "sum", max_sys_cpu_growth_pct, "system_cpu"),
        ("minor_page_faults", "sum", max_page_fault_growth_pct, "minor_page_faults"),
        ("major_page_faults", "sum", max_page_fault_growth_pct, "major_page_faults"),
    ):
        base_val, base_scope = _summary_resource_metric(baseline, base_records, metric_key, agg_mode)
        cand_val, cand_scope = _summary_resource_metric(candidate, cand_records, metric_key, agg_mode)
        if base_val is None or cand_val is None:
            blockers.append(f"missing required OS resource measurement {label} ({metric_key})")
            continue
        if base_scope != cand_scope:
            blockers.append(f"OS resource scope mismatch for {label}: {base_scope!r} != {cand_scope!r}")
            continue
        delta = _pct_delta(base_val, cand_val)
        resource_deltas[label] = delta
        if delta is None or delta > limit_pct:
            drop_reasons.append(f"{label} grew {delta if delta is not None else 'inf'}% > max {limit_pct:.2f}%")

    if blockers:
        decision = "blocked"
        reasons = blockers + drop_reasons
    elif drop_reasons:
        decision = "drop"
        reasons = drop_reasons
    else:
        decision = "retain"
        if policy == "binary_complete_cohort":
            reasons = ["all correctness, no-fallback, OS resource, and binary complete-cohort engine-time gates passed"]
        else:
            reasons = ["all correctness, total speedup, tail, and OS resource gates passed"]

    return {
        "schema_version": SCHEMA_VERSION,
        "policy": policy,
        "decision": decision,
        "timing_surface": base_surface,
        "timing_key": timing_key,
        "shared_queries": len(shared_queries),
        "total_delta_pct": total_delta_pct,
        "max_query_regression_pct": max_query_regression_pct,
        "resource_deltas_pct": resource_deltas,
        "reasons": reasons,
        "claim_gate_status": "not_claim_grade",
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--lane")
    parser.add_argument("--policy", choices=POLICIES, default="perf01_thresholds")
    parser.add_argument("--min-total-improvement-pct", type=float, default=2.0)
    parser.add_argument("--max-tail-regression-pct", type=float, default=5.0)
    parser.add_argument("--max-rss-growth-pct", type=float, default=10.0)
    parser.add_argument("--max-sys-cpu-growth-pct", type=float, default=10.0)
    parser.add_argument("--max-page-fault-growth-pct", type=float, default=15.0)
    parser.add_argument("--require-retain", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    result = evaluate_ship_drop(
        _load_json(args.baseline),
        _load_json(args.candidate),
        lane=args.lane,
        policy=args.policy,
        min_total_improvement_pct=args.min_total_improvement_pct,
        max_tail_regression_pct=args.max_tail_regression_pct,
        max_rss_growth_pct=args.max_rss_growth_pct,
        max_sys_cpu_growth_pct=args.max_sys_cpu_growth_pct,
        max_page_fault_growth_pct=args.max_page_fault_growth_pct,
    )
    encoded = json.dumps(result, indent=2, allow_nan=False) + "\n"
    if args.output is not None:
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    if result["decision"] == "blocked":
        return 2
    if args.require_retain and result["decision"] != "retain":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
