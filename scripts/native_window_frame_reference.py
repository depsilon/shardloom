# SPDX-License-Identifier: Apache-2.0
"""Brute-force window-frame oracle independent of the moving runtime."""

from __future__ import annotations

from typing import Any


_FIXTURE_VALUES = (
    (0, 3, 7),
    (1, 1, 4),
    (0, 1, None),
    (0, 1, 7),
    (0, 5, -2),
    (1, None, None),
    (0, None, 3),
    (1, 2, 4),
    (1, 2, 9),
    (0, 4, 1),
)

_OUTPUT_KEYS = (
    "id",
    "count_all",
    "nonnull",
    "distinct",
    "total",
    "mean",
    "minimum",
    "maximum",
    "first",
    "last",
    "nth",
)


def fixture_rows() -> list[dict[str, Any]]:
    """Return the fixed ten-row input with stable source ids."""
    return [
        {"id": row_id, "cohort": "東京" if cohort == 0 else None,
         "priority": priority, "value": value}
        for row_id, (cohort, priority, value) in enumerate(_FIXTURE_VALUES)
    ]


def frame_rows(
    unit: str,
    exclusion: str,
    descending: bool,
    nulls_first: bool,
) -> list[dict[str, Any]]:
    """Evaluate one explicit frame specification, preserving original id order."""
    if unit not in {"ROWS", "GROUPS", "RANGE"}:
        raise ValueError(f"unsupported frame unit: {unit!r}")
    if exclusion not in {"NO OTHERS", "CURRENT ROW", "GROUP", "TIES"}:
        raise ValueError(f"unsupported frame exclusion: {exclusion!r}")

    rows = fixture_rows()
    selected_by_id: dict[int, list[dict[str, Any]]] = {}
    for current in rows:
        cohort_rows = [row for row in rows if row["cohort"] == current["cohort"]]

        def order_key(row: dict[str, Any]) -> tuple[int, int, int]:
            priority = row["priority"]
            null_rank = 0 if nulls_first else 1
            value_rank = 1 - null_rank
            if priority is None:
                return (null_rank, 0, row["id"])
            numeric_priority = -priority if descending else priority
            return (value_rank, numeric_priority, row["id"])

        ordered = sorted(cohort_rows, key=order_key)
        current_position = next(
            index for index, row in enumerate(ordered) if row["id"] == current["id"]
        )

        peer_rank_by_id: dict[int, int] = {}
        peer_rank = -1
        previous_priority: int | None = None
        first_priority = True
        for row in ordered:
            if first_priority or row["priority"] != previous_priority:
                peer_rank += 1
                previous_priority = row["priority"]
                first_priority = False
            peer_rank_by_id[row["id"]] = peer_rank

        current_rank = peer_rank_by_id[current["id"]]
        frame: list[dict[str, Any]] = []
        for index, candidate in enumerate(ordered):
            if unit == "ROWS":
                in_frame = abs(index - current_position) <= 1
            elif unit == "GROUPS":
                in_frame = abs(peer_rank_by_id[candidate["id"]] - current_rank) <= 1
            elif current["priority"] is None:
                in_frame = candidate["priority"] is None
            else:
                in_frame = (
                    candidate["priority"] is not None
                    and abs(candidate["priority"] - current["priority"]) <= 1
                )

            if not in_frame:
                continue

            same_peer = candidate["priority"] == current["priority"]
            if exclusion == "CURRENT ROW" and candidate["id"] == current["id"]:
                continue
            if exclusion == "GROUP" and same_peer:
                continue
            if exclusion == "TIES" and same_peer and candidate["id"] != current["id"]:
                continue
            frame.append(candidate)
        selected_by_id[current["id"]] = frame

    result: list[dict[str, Any]] = []
    for row in rows:
        values = [candidate["value"] for candidate in selected_by_id[row["id"]]]
        nonnull_values = [value for value in values if value is not None]
        total = float(sum(nonnull_values)) if nonnull_values else None
        result.append(
            {
                "id": row["id"],
                "count_all": len(values),
                "nonnull": len(nonnull_values),
                "distinct": len(set(nonnull_values)),
                "total": total,
                "mean": total / len(nonnull_values) if total is not None else None,
                "minimum": min(nonnull_values) if nonnull_values else None,
                "maximum": max(nonnull_values) if nonnull_values else None,
                "first": values[0] if values else None,
                "last": values[-1] if values else None,
                "nth": values[1] if len(values) > 1 else None,
            }
        )

    assert all(tuple(result_row) == _OUTPUT_KEYS for result_row in result)
    return result
