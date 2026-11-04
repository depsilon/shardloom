"""Deferred implementations for client report values.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import client as _api


def _split_field_list(value):

    if not value or value == "not_applicable":
        return ()
    parts: list[str] = []
    start = 0
    depth = 0
    for index, char in enumerate(value):
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            part = value[start:index]
            if part:
                parts.append(part)
            start = index + 1
    tail = value[start:]
    if tail:
        parts.append(tail)
    return tuple(parts)


def _jsonl_object_rows(value, *, field_name):
    rows: list[Mapping[str, Any]] = []
    for line_number, line in enumerate(value.splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        try:
            payload = _api.json.loads(text)
        except _api.json.JSONDecodeError as exc:
            raise _api.ShardLoomProtocolError(
                f"ShardLoom field {field_name} contains invalid JSONL at line {line_number}"
            ) from exc
        if not isinstance(payload, _api.Mapping):
            raise _api.ShardLoomProtocolError(
                f"ShardLoom field {field_name} line {line_number} is not a JSON object"
            )
        rows.append(dict(payload))
    return tuple(rows)


def _csv_output_plan_required_columns(value):
    return tuple(
        part
        for part in _api._csv_values(value)
        if part.strip().lower()
        not in {
            *_api.ABSENT_REFERENCE_VALUES,
            "not_applicable_inline_result",
        }
    )
