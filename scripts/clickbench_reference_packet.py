#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Load a frozen complete-values reference for the pinned ClickBench query set."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from native_workflow_protocol import strict_json


SCHEMA_VERSION = "shardloom.clickbench.reference_values.v1"
REFERENCE_KINDS = frozenset({"retained_native_regression", "independent_reference"})
QUERY_COUNT = 43


def query_statements(query_path: Path) -> tuple[bytes, list[str]]:
    """Return the exact pinned-file bytes and its 43 executable statements."""
    raw = query_path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("query file must be UTF-8") from error
    statements = [
        statement.strip()
        for statement in "\n".join(
            line for line in text.splitlines() if not line.lstrip().startswith("--")
        ).split(";")
        if statement.strip()
    ]
    if len(statements) != QUERY_COUNT:
        raise ValueError(f"expected exactly {QUERY_COUNT} pinned ClickBench queries")
    return raw, statements


def canonical_values_sha256(values: list[dict[str, Any]]) -> str:
    try:
        encoded = json.dumps(values, sort_keys=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ValueError("reference values are not finite JSON values") from error
    return hashlib.sha256(encoded).hexdigest()


def load_reference_packet(packet_path: Path, query_path: Path) -> dict[str, Any]:
    """Validate a complete immutable reference packet against exact query bytes.

    The returned ``values`` mapping is keyed by integer query ID. ``packet`` is
    a compact receipt identity suitable for embedding in a UAT summary and for
    verifying that the file remains unchanged when the run finishes.
    """
    packet_path = Path(packet_path).resolve()
    query_path = Path(query_path)
    packet_bytes = packet_path.read_bytes()
    try:
        packet = strict_json(packet_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ValueError(f"invalid reference packet JSON: {error}") from error
    if not isinstance(packet, dict):
        raise ValueError("reference packet must be a JSON object")
    if packet.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("reference packet schema_version mismatch")
    reference_kind = packet.get("reference_kind")
    if not isinstance(reference_kind, str) or reference_kind not in REFERENCE_KINDS:
        raise ValueError("reference_kind must be retained_native_regression or independent_reference")

    query_bytes, statements = query_statements(query_path)
    queries_sha256 = hashlib.sha256(query_bytes).hexdigest()
    if packet.get("queries_sha256") != queries_sha256:
        raise ValueError("reference packet queries_sha256 does not match the supplied query file")

    records = packet.get("records")
    if not isinstance(records, list) or len(records) != QUERY_COUNT:
        raise ValueError(f"reference packet must contain exactly {QUERY_COUNT} query records")
    values: dict[int, list[dict[str, Any]]] = {}
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("reference record must be a JSON object")
        query_id = record.get("query_id")
        if type(query_id) is not int or not 1 <= query_id <= QUERY_COUNT:
            raise ValueError("reference query_id must be an integer from 1 through 43")
        if query_id in values:
            raise ValueError(f"duplicate reference query_id: {query_id}")
        if record.get("statement") != statements[query_id - 1]:
            raise ValueError(f"reference statement does not match pinned query {query_id}")
        result_values = record.get("values")
        if not isinstance(result_values, list) or any(not isinstance(row, dict) for row in result_values):
            raise ValueError(f"reference values for query {query_id} must be a list of object rows")
        expected_hash = canonical_values_sha256(result_values)
        if record.get("values_sha256") != expected_hash:
            raise ValueError(f"reference values_sha256 mismatch for query {query_id}")
        values[query_id] = result_values

    if set(values) != set(range(1, QUERY_COUNT + 1)):
        missing = sorted(set(range(1, QUERY_COUNT + 1)) - set(values))
        raise ValueError(f"reference packet is missing query IDs: {missing}")

    return {
        "values": values,
        "queries": statements,
        "packet": {
            "path": str(packet_path),
            "sha256": hashlib.sha256(packet_bytes).hexdigest(),
            "reference_kind": reference_kind,
            "queries_sha256": queries_sha256,
        },
    }
