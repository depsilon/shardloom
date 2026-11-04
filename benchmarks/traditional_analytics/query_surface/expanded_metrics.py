# SPDX-License-Identifier: Apache-2.0
"""Deterministic cohort metrics for expanded native query observations."""

from __future__ import annotations

from collections import Counter
import math


def _clock(value):
    """Return a usable duration, preserving zero and treating bad data as unavailable."""
    if type(value) not in (int, float):
        return None
    value = float(value)
    return value if math.isfinite(value) and value >= 0 else None


def _metric(values, denominator):
    return {
        "count": len(values),
        "denominator": denominator,
        "sum_seconds": math.fsum(values) if values else None,
        "mean_seconds": math.fsum(values) / len(values) if values else None,
    }


def _valid_pair(row):
    native = row["native"]
    polars = row["polars"]
    if native["status"] != "passed" or polars["status"] != "passed":
        return None
    native_time = _clock(native.get("workflow_seconds"))
    polars_time = _clock(polars.get("workflow_seconds"))
    if native_time is None or polars_time is None:
        return None
    return native_time, polars_time


def _pair_stratum(row):
    """Classify only explicitly comparable source-format pairs."""
    native_format = row["native_format"]
    polars_format = row["polars_format"]
    compatible = {"parquet", "arrow-ipc"}
    if native_format == "vortex" and polars_format in compatible:
        return "native_prepared_vortex"
    if native_format == polars_format and native_format in compatible:
        return "same_source_format"
    return "unmatched_format"


def summarize(rows):
    """Summarize one observation per unique case without imputing absent measurements."""
    rows = list(rows)
    if not rows:
        raise ValueError("cannot summarize an empty cohort")

    seen = set()
    normalized = []
    for row in rows:
        case_id = row.get("id")
        if not isinstance(case_id, str) or not case_id:
            raise ValueError("case id must be a nonempty string")
        if case_id in seen:
            raise ValueError(f"duplicate case id: {case_id}")
        seen.add(case_id)
        for key in ("family", "archetype", "profile", "surface", "native_format", "polars_format"):
            if not isinstance(row.get(key), str) or not row[key]:
                raise ValueError(f"{key} must be a nonempty string")
        for engine in ("native", "polars"):
            value = row.get(engine)
            if not isinstance(value, dict) or not isinstance(value.get("status"), str):
                raise ValueError(f"{engine} observation requires a status")
        normalized.append(row)

    native_statuses = Counter(row["native"]["status"] for row in normalized)
    polars_statuses = Counter(row["polars"]["status"] for row in normalized)
    native_engine = []
    native_engine_by_family = {}
    for row in normalized:
        if row["native"]["status"] == "passed":
            value = _clock(row["native"].get("engine_seconds"))
            if value is not None:
                native_engine.append(value)
                native_engine_by_family.setdefault(row["family"], []).append(value)
    complete_engine = len(native_engine) == len(normalized)

    family_rows = {}
    for family in sorted({row["family"] for row in normalized}):
        members = [row for row in normalized if row["family"] == family]
        values = native_engine_by_family.get(family, [])
        complete = len(values) == len(members)
        family_rows[family] = {
            "denominator": len(members),
            "native_status_counts": dict(sorted(Counter(row["native"]["status"] for row in members).items())),
            "polars_status_counts": dict(sorted(Counter(row["polars"]["status"] for row in members).items())),
            "native_engine_time_valid": _metric(values, len(members)),
            "complete_total_engine_seconds": math.fsum(values) if complete else None,
            "complete_total_available": complete,
        }

    family_means = [
        entry["native_engine_time_valid"]["mean_seconds"]
        for entry in family_rows.values()
        if entry["native_engine_time_valid"]["mean_seconds"] is not None
    ]
    pairs = [(row, _valid_pair(row)) for row in normalized]
    paired = [(row, clocks) for row, clocks in pairs if clocks is not None]
    same_source = [(row, clocks) for row, clocks in paired if _pair_stratum(row) == "same_source_format"]
    prepared_vortex = [
        (row, clocks) for row, clocks in paired if _pair_stratum(row) == "native_prepared_vortex"
    ]
    unmatched = [(row, clocks) for row, clocks in paired if _pair_stratum(row) == "unmatched_format"]

    def paired_summary(cohort):
        native_times = [clocks[0] for _, clocks in cohort]
        polars_times = [clocks[1] for _, clocks in cohort]
        native_total = math.fsum(native_times) if native_times else None
        polars_total = math.fsum(polars_times) if polars_times else None
        ratio = (
            native_total / polars_total
            if native_total is not None and polars_total not in (None, 0)
            else None
        )
        return {
            "count": len(cohort),
            "denominator": len(normalized),
            "native_workflow_seconds": native_total,
            "polars_workflow_seconds": polars_total,
            "native_to_polars_workflow_ratio": ratio,
            "ratio_available": ratio is not None,
        }

    engine_contributions = []
    for row in normalized:
        if row["native"]["status"] != "passed":
            continue
        seconds = _clock(row["native"].get("engine_seconds"))
        if seconds is not None:
            engine_contributions.append(
                {
                    "id": row["id"],
                    "archetype": row["archetype"],
                    "native_format": row["native_format"],
                    "surface": row["surface"],
                    "engine_seconds": seconds,
                }
            )
    engine_contributions.sort(key=lambda item: (-item["engine_seconds"], item["id"]))

    latency_gaps = []
    for row, clocks in paired:
        native_time, polars_time = clocks
        latency_gaps.append(
            {
                "id": row["id"],
                "archetype": row["archetype"],
                "native_format": row["native_format"],
                "polars_format": row["polars_format"],
                "surface": row["surface"],
                "absolute_workflow_gap_seconds": abs(native_time - polars_time),
                "native_workflow_seconds": native_time,
                "polars_workflow_seconds": polars_time,
            }
        )
    latency_gaps.sort(key=lambda item: (-item["absolute_workflow_gap_seconds"], item["id"]))

    return {
        "denominator": len(normalized),
        "native_status_counts": dict(sorted(native_statuses.items())),
        "polars_status_counts": dict(sorted(polars_statuses.items())),
        "native_engine_time_valid": _metric(native_engine, len(normalized)),
        "complete_total_engine_seconds": math.fsum(native_engine) if complete_engine else None,
        "complete_total_available": complete_engine,
        "family_balanced_mean_native_engine_seconds": math.fsum(family_means) / len(family_means)
        if family_means
        else None,
        "family_balanced_coverage": {
            "families_with_valid_means": len(family_means),
            "family_denominator": len(family_rows),
        },
        "families": family_rows,
        "paired_declarations_through_complete_delivery": paired_summary(paired),
        "paired_same_source": paired_summary(same_source),
        "paired_native_prepared_vortex": paired_summary(prepared_vortex),
        "paired_unmatched_format": paired_summary(unmatched),
        "native_top_20_engine_contributions": engine_contributions[:20],
        "top_20_matched_api_absolute_latency_gaps": latency_gaps[:20],
    }
