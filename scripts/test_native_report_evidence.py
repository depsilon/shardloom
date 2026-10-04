# SPDX-License-Identifier: Apache-2.0
"""Regression checks for ambiguous retained native report fields."""
import unittest
from types import SimpleNamespace

from run_native_unary_uat import require_unique_report_fields
from native_typed_payload_cases import require_native_resource_admission


class NativeReportEvidenceTests(unittest.TestCase):
    def test_repeated_report_fields_are_denied_even_when_values_agree(self):
        for key, values in [
            ("fallback_attempted", ["false", "false"]),
            ("external_engine_invoked", ["false", "false"]),
            ("local_primitive_no_query_answer_cache", ["true", "false"]),
        ]:
            with self.subTest(key=key):
                raw = {"fields": [{"key": key, "value": value} for value in values]}
                with self.assertRaisesRegex(ValueError, key):
                    require_unique_report_fields(raw)

    def test_distinct_field_names_can_share_values_without_being_modified(self):
        raw = {"fields": [{"key": key, "value": "false"} for key in
                          ["fallback_attempted", "external_engine_invoked"]]}
        before = repr(raw)
        require_unique_report_fields(raw)
        self.assertEqual(repr(raw), before)

    def test_native_resource_peak_uses_the_declared_execution_route(self):
        common = {"public_workflow_memory_gb": "1",
                  "public_workflow_native_vortex_provider_scenario": "none"}
        for fields in [
            dict(common, public_workflow_route_id="native_vortex_relational_write",
                 resident_peak_reserved_buffer_bytes="1024"),
            dict(common, public_workflow_route_id="native_vortex_primitive_row_export",
                 native_vortex_result_export_kind="owned_native_array_stream",
                 native_vortex_array_sink_source_generation_validated="true",
                 native_vortex_array_sink_peak_reserved_bytes="1073741824"),
        ]:
            require_native_resource_admission("test", SimpleNamespace(field=fields.get))

    def test_native_resource_peak_never_accepts_missing_malformed_or_oversized_proof(self):
        base = {"public_workflow_memory_gb": "1",
                "public_workflow_native_vortex_provider_scenario": "none",
                "public_workflow_route_id": "native_vortex_primitive_row_export",
                "native_vortex_result_export_kind": "owned_native_array_stream",
                "native_vortex_array_sink_source_generation_validated": "true",
                "native_vortex_array_sink_peak_reserved_bytes": "1024"}
        changes = [
            {"native_vortex_array_sink_peak_reserved_bytes": value}
            for value in [None, "", "-1", "1.5", "NaN", "١", "1073741825"]
        ] + [
            {"public_workflow_memory_gb": "2"},
            {"public_workflow_native_vortex_provider_scenario": "external"},
            {"public_workflow_route_id": "native_vortex_relational_write"},
            {"native_vortex_result_export_kind": "primitive_row_stream"},
            {"native_vortex_array_sink_source_generation_validated": "false"},
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                fields = base | change
                require_native_resource_admission("test", SimpleNamespace(field=fields.get))


if __name__ == "__main__":
    unittest.main()
