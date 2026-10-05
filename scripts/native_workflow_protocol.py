# SPDX-License-Identifier: Apache-2.0
"""Public native workflow requests and complete result readers for test harnesses.

Workloads supply declarations and independent expectations. This module never
chooses an operator, evaluates SQL, prepares data, or substitutes an engine.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
import re
import sys
from typing import Any, Mapping

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python" / "src"))

from shardloom._result_schema import python_rows, schema_fields
from shardloom.errors import ShardLoomProtocolError


def strict_json(text: str):
    def invalid(value):
        raise ValueError(f"nonfinite JSON value: {value}")

    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            invalid(value)
        return number

    def unique_object(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON object key: {key}")
            result[key] = value
        return result

    return json.loads(text, parse_constant=invalid, parse_float=finite_float,
                      object_pairs_hook=unique_object)


def read_json_output(path: Path, output_format: str) -> list[dict[str, Any]]:
    """Decode a complete text output, including schema-free empty JSON files.

    This is file validation only. It does not execute a workload or infer a
    schema that JSON/JSONL did not persist.
    """
    content = path.read_text(encoding="utf-8")
    if output_format == "json":
        rows = strict_json(content)
    elif output_format == "jsonl":
        rows = [strict_json(line) for line in content.splitlines()]
    else:
        raise ValueError("JSON output reader requires json or jsonl")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("JSON output must contain complete object rows")
    return rows


def report_fields(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Require unambiguous execution evidence before reading a result."""
    if envelope.get("status") != "success":
        raise ValueError("public operation did not report success")
    if "fallback" in envelope:
        fallback = envelope["fallback"]
        if (not isinstance(fallback, dict) or fallback.get("attempted") is not False
                or fallback.get("engine") is not None
                or fallback.get("allowed", False) is not False):
            raise ValueError("unsafe envelope fallback evidence")
    fields = {}
    entries = envelope.get("fields")
    if not isinstance(entries, list):
        raise ValueError("report fields must be an array")
    for field in entries:
        if not isinstance(field, dict) or not isinstance(field.get("key"), str):
            raise ValueError("invalid report field")
        key = field["key"]
        if key in fields:
            raise ValueError(f"duplicate report field: {key}")
        fields[key] = field.get("value")
    for key in ("public_workflow_fallback_attempted", "public_workflow_external_engine_invoked"):
        if not (fields.get(key) is False or fields.get(key) == "false"):
            raise ValueError(f"missing or unsafe execution evidence: {key}")
    for key, value in fields.items():
        if key.endswith(("fallback_attempted", "external_engine_invoked")):
            if not (value is False or value == "false"):
                raise ValueError(f"unsafe execution evidence: {key}")
    return fields


def _unsigned(value: Any, name: str) -> int:
    if type(value) not in (int, str) or re.fullmatch(r"[0-9]+", str(value)) is None:
        raise ValueError(f"invalid {name}")
    return int(value)


def extract_result(envelope: Mapping[str, Any]):
    """Read a complete native result; diagnostic text is never a payload."""
    fields = report_fields(envelope)
    payload_keys = [key for key in ("result_jsonl", "result_values_json") if key in fields]
    if payload_keys or "result_payload_complete" in fields:
        complete = fields.get("result_payload_complete")
        if len(payload_keys) != 1 or not (complete is True or complete == "true"):
            raise ValueError("native rows require exactly one complete structured payload")
        count = _unsigned(fields.get("output_row_count"), "output row count")
        key = payload_keys[0]
        payload = fields[key]
        if not isinstance(payload, str):
            raise ValueError(f"invalid {key}")
        rows = ([strict_json(line) for line in payload.splitlines()]
                if key == "result_jsonl" else strict_json(payload))
        if (not isinstance(rows, list) or len(rows) != count
                or any(not isinstance(row, dict) for row in rows)):
            raise ValueError("native rows are truncated or have an invalid shape")
        schema_json = fields.get("result_schema_json")
        schema_format = fields.get("result_schema_format")
        if not isinstance(schema_json, str) or not isinstance(schema_format, str):
            raise ValueError("native result schema is absent")
        try:
            strict_json(schema_json)
            schema = schema_fields(schema_json, schema_format)
            python_rows(rows, schema, temporal_objects=False)
        except ShardLoomProtocolError as error:
            raise ValueError(str(error)) from error
        return rows
    raise ValueError("complete structured native result is absent")


def public_workflow_command(
    binary: Path | str, statement: str, *, input_path: Path | str | None = None,
    input_format: str | None = None, source_bindings: Mapping[str, Any] | None = None,
    surface: str = "sql", memory_gb: int = 1, max_parallelism: int = 1,
    requested_output: str = "collect", output: Path | str | None = None,
    allow_overwrite: bool = False,
) -> list[str]:
    """Build one public request, independent of workload, source and operator."""
    if surface not in ("sql", "python", "dataframe", "cli"):
        raise ValueError("unknown public workflow surface")
    if not statement.strip():
        raise ValueError("a complete SQL declaration is required")
    if type(memory_gb) is not int or memory_gb <= 0:
        raise ValueError("memory_gb must be a positive integer")
    if type(max_parallelism) is not int or max_parallelism <= 0:
        raise ValueError("max_parallelism must be a positive integer")
    if (input_path is None) != (input_format is None):
        raise ValueError("input path and format must be supplied together")
    if (requested_output == "collect") != (output is None):
        raise ValueError("a writer requires an output path; collect does not")
    args = [str(binary), "run", surface, "--sql", statement, "--request", requested_output,
            "--bounded", "true", "--memory-gb", str(memory_gb),
            "--max-parallelism", str(max_parallelism), "--format", "json"]
    if input_path is not None:
        args.extend(["--input", str(input_path), "--input-format", str(input_format)])
    if source_bindings is not None:
        args.extend(["--source-bindings", json.dumps(source_bindings, allow_nan=False)])
    if output is not None:
        args.extend(["--output", str(output)])
    if allow_overwrite:
        if output is None:
            raise ValueError("overwrite requires an output path")
        args.append("--allow-overwrite")
    return args
