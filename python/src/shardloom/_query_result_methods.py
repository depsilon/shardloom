"""Deferred implementations for query result methods.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def Result_result_rows(self):

    if self.native_result is not None:
        return tuple(self.native_result.row_export(wire_values=True)[0])
    values = self.envelope.field("result_values_json")
    if values is None:
        return _api._jsonl_object_rows(self.result_jsonl, field_name="result_jsonl")
    try:
        rows = _api.json.loads(values)
    except ValueError as error:
        raise _api.ShardLoomProtocolError("invalid native result_values_json") from error
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise _api.ShardLoomProtocolError("native result_values_json requires an array of row objects")
    return tuple(rows)


def Result_observations(self):
    if self.envelope.status != "success" or not self.envelope.field_bool(
        "relational_observation_data_completed", False
    ):
        return {}
    count = self.envelope.field_int("relational_observation_count", 0)
    observations = {}
    for index in _api.builtins.range(count):
        prefix = f"relational_observation_{index}"
        name = _api._required_field(self.envelope, prefix + "_name")
        schema = _api.schema_fields(_api._required_field(self.envelope, prefix + "_schema_json"), "vortex.dtype.serde.v1")
        rows = _api._jsonl_object_rows(_api._required_field(self.envelope, prefix + "_result_jsonl"), field_name=prefix)
        if len(rows) != 1 or name in observations:
            raise _api.ShardLoomProtocolError("observations require unique names and one completed metric row")
        observations[name] = tuple(_api.python_rows(rows, schema))[0]
    return observations


def Result_result_known(self):

    return self.envelope.field_bool("result_known", False) is True or _api._any_true_field(
        self.envelope,
        (
            "filtered_count_local_execution_result_known",
            "project_local_execution_result_known",
            "filter_project_local_execution_result_known",
            "filter_local_execution_result_known",
        ),
    )


def Result_rows_scanned(self):

    return _api._first_int_field(
        self.envelope,
        (
            "local_primitive_rows_scanned",
            "filtered_count_local_execution_rows_scanned",
            "filter_local_execution_rows_scanned",
            "project_local_execution_rows_scanned",
            "filter_project_local_execution_rows_scanned",
        ),
    )


def Result_rows_selected(self):

    return _api._first_int_field(
        self.envelope,
        (
            "rows_selected",
            "local_primitive_rows_selected",
            "filtered_count_local_execution_rows_selected",
            "filter_local_execution_rows_selected",
            "filter_project_local_execution_rows_selected",
        ),
    )


def Result_projected_columns(self):

    value = _api._first_string_field(
        self.envelope,
        (
            "local_primitive_projected_columns",
            "project_local_execution_projected_columns",
            "filter_project_local_execution_projected_columns",
            "columns",
        ),
    )
    if not value:
        return ()
    return tuple(part.strip() for part in value.split(",") if part.strip())


def Result_fallback_attempted(self):

    if self.envelope.fallback.attempted:
        return True
    return _api._any_true_field(
        self.envelope,
        (
            "fallback_attempted",
            "local_primitive_native_io_fallback_attempted",
            "local_primitive_execution_certificate_fallback_attempted",
            "filtered_count_local_execution_fallback_attempted",
            "filter_local_execution_fallback_attempted",
            "project_local_execution_fallback_attempted",
            "filter_project_local_execution_fallback_attempted",
        ),
    )


def Result_runtime_execution(self):

    return _api._any_true_field(
        self.envelope,
        (
            "runtime_execution",
            "local_primitive_report_present",
            "filtered_count_local_execution_result_known",
            "project_local_execution_result_known",
            "filter_project_local_execution_result_known",
            "filter_local_execution_result_known",
        ),
    )


def Result_data_read(self):

    return _api._any_true_field(
        self.envelope,
        (
            "data_read",
            "data_io_performed",
            "filtered_count_local_execution_data_read",
            "filter_local_execution_data_read",
            "project_local_execution_data_read",
            "filter_project_local_execution_data_read",
        ),
    )


def Result_claim_gate_status(self):

    return _api._first_string_field(
        self.envelope,
        (
            "filter_project_local_execution_claim_gate_status",
            "filter_local_execution_claim_gate_status",
            "project_local_execution_claim_gate_status",
            "why_claim_gate_status",
            "claim_gate_status",
        ),
    )
