# SPDX-License-Identifier: Apache-2.0
"""Comparison-only normalization shared by independent benchmark adapters."""
from __future__ import annotations
import math
from typing import Any

CORRECTNESS_FLOAT_DIGITS = 4

def round_float(value: Any) -> float:
    if value is None:
        return 0.0
    number = float(value)
    if math.isnan(number):
        return 0.0
    return round(number, CORRECTNESS_FLOAT_DIGITS)

def normalize_scalar_result(row_count: Any, metric_sum: Any) -> dict[str, Any]:
    return {"row_count": int(row_count), "metric_sum": round_float(metric_sum)}

def normalize_group_rows(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        normalized.append(
            {
                key: str(row[key]) if key == "dim_label" else int(row[key]),
                "row_count": int(row["row_count"]),
                "metric_sum": round_float(row["metric_sum"]),
            }
        )
    return sorted(normalized, key=lambda row: row[key])

def normalize_top_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {"id": int(row["id"]), "metric": round_float(row["metric"])} for row in rows
    ]
    return sorted(normalized, key=lambda row: (-row["metric"], row["id"]))[:10]

def normalize_multi_group_rows(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        normalized_row = {
            key: str(row[key]) if key in {"category", "dim_label"} else int(row[key])
            for key in keys
        }
        normalized_row["row_count"] = int(row["row_count"])
        normalized_row["metric_sum"] = round_float(row["metric_sum"])
        normalized.append(normalized_row)
    return sorted(normalized, key=lambda row: tuple(row[key] for key in keys))

def normalize_rank_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {
            "group_key": int(row["group_key"]),
            "id": int(row["id"]),
            "metric": round_float(row["metric"]),
            "rank": int(row.get("rank", row.get("row_number", 1))),
        }
        for row in rows
    ]
    return sorted(normalized, key=lambda row: (row["group_key"], row["rank"], row["id"]))

def normalize_top_group_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {
            "group_key": int(row["group_key"]),
            "id": int(row["id"]),
            "metric": round_float(row["metric"]),
            "rank": int(row["rank"]),
        }
        for row in rows
    ]
    return sorted(normalized, key=lambda row: (row["group_key"], row["rank"], row["id"]))

def normalize_complex_etl_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {
            "dim_label": str(row["dim_label"]),
            "bucket": int(row["bucket"]),
            "row_count": int(row["row_count"]),
            "metric_sum": round_float(row["metric_sum"]),
            "weighted_sum": round_float(row["weighted_sum"]),
        }
        for row in rows
    ]
    return sorted(
        normalized,
        key=lambda row: (-row["weighted_sum"], row["dim_label"], row["bucket"]),
    )[:20]
