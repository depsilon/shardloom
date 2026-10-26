# SPDX-License-Identifier: Apache-2.0
"""Expose existing native measurements without inventing whole-query totals."""
from __future__ import annotations

import re


# The original field names deliberately remain visible. These scopes are
# different and must not be added together as physical query-byte totals.
COUNTERS = {
    "local_primitive_rows_scanned": ("rows", "selected primitive's rows_scanned; may include metadata counts"),
    "local_primitive_rows_selected": ("rows", "selected primitive's selection"),
    "relational_scan_rows_delivered": ("rows", "relational scan delivery"),
    "relational_output_rows": ("rows", "relational output"),
    "local_primitive_arrays_read_count": ("arrays", "selected primitive's array-read counter"),
    "local_primitive_aggregate_native_numeric_accessor_native_decode_calls": ("calls", "aggregate native numeric accessor"),
    "local_primitive_aggregate_native_numeric_accessor_rows": ("rows", "aggregate native numeric accessor"),
    "local_primitive_aggregate_native_numeric_accessor_source_array_nbytes_estimate": ("estimated_bytes", "accessor source arrays; overlapping estimates possible"),
    "local_primitive_aggregate_native_numeric_accessor_canonical_array_nbytes_estimate": ("estimated_bytes", "accessor canonical arrays; overlapping estimates possible"),
    "local_primitive_aggregate_native_numeric_accessor_typed_value_bytes_copied": ("bytes", "accessor typed-value copies only"),
    "execution_resource_observed_spill_bytes": ("bytes", "native spill payload written; inspect execution_resource_spill_observation_scope"),
}
STRATEGIES = (
    "relational_provider_selection_reason", "local_primitive_group_output_strategy",
    "local_primitive_group_admission_strategy", "local_primitive_aggregate_update_strategy",
    "local_primitive_expression_fusion_strategy", "local_primitive_predicate_strategy",
    "local_primitive_sort_predicate_strategy", "local_primitive_physical_policy_rejected_alternatives",
    "resident_decoded_byte_accounting", "execution_resource_spill_observation_scope",
)
CERTIFICATES = tuple(prefix + suffix for prefix in
    ("local_primitive_execution_certificate_", "local_primitive_native_io_certificate_")
    for suffix in ("id", "path_id", "status", "certified"))


def extract(fields):
    counters = {}
    for name, (unit, scope) in COUNTERS.items():
        raw = fields.get(name)
        value = int(raw) if type(raw) in (str, int) and re.fullmatch(r"[0-9]+", str(raw)) else None
        counters[name] = {"value": value, "reported_value": raw, "unit": unit, "scope": scope,
                          "availability": "reported" if value is not None else "unavailable"}
    return {"schema_version": 1,
        "strategies": {name: fields.get(name) for name in STRATEGIES},
        "certificates": {name: fields.get(name) for name in CERTIFICATES},
        "counters": counters,
        "whole_query_bytes": {name: None for name in ("physical_read", "decoded", "copied", "materialized", "output")},
        "physical_plan": None,
        "limitation": "No uniform complete physical-plan or whole-query byte instrumentation across the five surfaces. "
                      "Specialized counter scopes are retained separately; missing is not zero."}
