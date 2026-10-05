# SPDX-License-Identifier: Apache-2.0
"""Structured report fixtures for harness unit tests; never an execution provider."""

import json


def schema_payload(rows, dtypes=None, names=None):
    names = list(rows[0]) if names is None and rows else (names or [])
    if dtypes is None:
        dtypes = []
        for name in names:
            values = [row[name] for row in rows]
            present = next((value for value in values if value is not None), None)
            nullable = any(value is None for value in values)
            if present is None:
                dtype = "Null"
            elif type(present) is bool:
                dtype = {"Bool": nullable}
            elif type(present) is int:
                kind = "u64" if present > (1 << 63) - 1 else "i64"
                dtype = {"Primitive": [kind, nullable]}
            elif type(present) is float:
                dtype = {"Primitive": ["f64", nullable]}
            elif isinstance(present, str):
                dtype = {"Utf8": nullable}
            else:
                raise AssertionError(f"test schema needs an explicit dtype for {name}")
            dtypes.append(dtype)
    schema = {"Struct": [{"names": names, "dtypes": dtypes}, False]}
    return [
        {"key": "result_schema_json", "value": json.dumps(schema)},
        {"key": "result_schema_format", "value": "vortex.dtype.serde.v1"},
    ]


def structured_envelope(rows, dtypes=None, names=None):
    return {
        "status": "success", "human_text": "native result",
        "fallback": {"attempted": False},
        "fields": [
            {"key": "public_workflow_fallback_attempted", "value": "false"},
            {"key": "public_workflow_external_engine_invoked", "value": "false"},
            {"key": "result_values_json", "value": json.dumps(rows, allow_nan=False)},
            {"key": "result_payload_complete", "value": "true"},
            {"key": "output_row_count", "value": str(len(rows))},
            *schema_payload(rows, dtypes, names),
        ],
    }
