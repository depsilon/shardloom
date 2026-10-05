# SPDX-License-Identifier: Apache-2.0
"""Shared complete-output checks for declared native workflows and local formats.

Workload modules supply independent values and format denials. This module only
invokes public writers, reopens their output through the native engine, and
checks complete values; it contains no workload-specific execution decisions.
"""

from __future__ import annotations

import csv
import json

from native_workflow_protocol import read_json_output
from run_clickbench_query_uat import strict_json
from run_native_unary_uat import csv_cell

LOCAL_FORMATS = ("vortex", "parquet", "arrow_ipc", "avro", "orc", "json", "jsonl", "csv")


def verify_output(context, destination, extension, expected, columns, *, name,
                  accepted, complete, csv_json_columns=()):
    """Compare a completed file with the oracle, including empty field order."""

    if extension == "csv":
        with destination.open(newline="") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != list(columns):
                raise ValueError(f"{name}: CSV column order differs: {reader.fieldnames!r}")
            actual = list(reader)
        expected_text = [
            {key: value if isinstance(value, (list, dict))
             else json.dumps(value) if key in csv_json_columns and value is not None
             else csv_cell(value) for key, value in row.items()}
            for row in expected
        ]
        for actual_row, expected_row in zip(actual, expected_text):
            for key, value in expected_row.items():
                if isinstance(value, (list, dict)):
                    actual_row[key] = strict_json(actual_row[key])
        complete(name, actual, expected_text, destination)
        return
    if extension in ("json", "jsonl"):
        actual = read_json_output(destination, extension)
    else:
        decoded = destination
        if extension != "jsonl":
            native = destination
            if extension != "vortex":
                native = destination.with_name(f"{name}-normalized.vortex")
                accepted(f"{name}-prepare", getattr(context, f"read_{extension}")(
                    destination).prepare(native, check=False))
            literal = "'" + str(native).replace("'", "''") + "'"
            schema = accepted(f"{name}-schema", context.sql(
                f"SELECT * FROM (SELECT * FROM {literal}) AS reopened LIMIT 0"
            ).collect(check=False))
            if schema.field("output_columns") != ",".join(columns):
                raise ValueError(f"{name}: reopened schema column order differs")
            decoded = destination.with_name(f"{name}-reopened.jsonl")
            accepted(f"{name}-reopen", context.read_vortex(native).write_jsonl(decoded, check=False))
        actual = read_json_output(decoded, "jsonl")
    complete(name, actual, expected, destination)


def write_outputs(context, output, workflow, expected, columns, *, name, guard,
                  accepted, complete, formats=LOCAL_FORMATS, execution=None,
                  csv_json_columns=(), denied_formats=None, denied=None, written=None):
    """Run the format matrix without changing the workload or its expected rows."""

    destinations = {}
    for extension in formats:
        guard()
        label = f"{name}-{extension}"
        destination = output / f"{label}.{extension}"
        report = getattr(workflow, f"write_{extension}")(
            destination, check=False, **(execution or {}))
        if denied_formats and extension in denied_formats:
            if denied is None:
                raise ValueError("an expected format denial requires a denial verifier")
            denied(label, report, destination, denied_formats[extension])
            continue
        (written or accepted)(label, report)
        verify_output(context, destination, extension, expected, columns, name=label,
                      accepted=accepted, complete=complete, csv_json_columns=csv_json_columns)
        destinations[extension] = destination
    return destinations
