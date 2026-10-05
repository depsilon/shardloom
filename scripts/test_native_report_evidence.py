# SPDX-License-Identifier: Apache-2.0
"""Regression checks for ambiguous retained native report fields."""
import unittest
from types import SimpleNamespace

from run_native_unary_uat import require_unique_report_fields
from native_report_evidence import has_diagnostic_detail, require_native_resource_admission


class NativeReportEvidenceTests(unittest.TestCase):
    def test_specific_diagnostic_details_survive_either_standard_constructor(self):
        detail = "COUNT(DISTINCT <argument>) only"
        for key in ("message", "reason"):
            raw = {"diagnostics": [{key: detail}]}
            self.assertTrue(has_diagnostic_detail(SimpleNamespace(raw=raw), detail))
        for raw in ({}, {"diagnostics": []}, {"diagnostics": [
            {"message": "unsupported", "reason": None, "suggested_next_step": detail}
        ]}):
            self.assertFalse(has_diagnostic_detail(SimpleNamespace(raw=raw), detail))

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

    def test_collect_and_writers_share_the_admitted_native_resource_pool(self):
        common = {"public_workflow_memory_gb": "1",
                  "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
                  "resident_provider_crate": "vortex"}
        for fields in [
            dict(common, public_workflow_route_id="native_vortex_relational_collect",
                 resident_peak_reserved_buffer_bytes="1024"),
            dict(common, public_workflow_route_id="native_vortex_relational_write",
                 resident_peak_reserved_buffer_bytes="1073741824"),
        ]:
            require_native_resource_admission("test", SimpleNamespace(field=fields.get))

    def test_native_array_writer_uses_its_session_allocator_evidence(self):
        fields = {
            "public_workflow_memory_gb": "1",
            "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
            "public_workflow_route_id": "native_vortex_primitive_row_export",
            "native_vortex_result_export_kind": "owned_native_array_stream",
            "native_vortex_array_sink_source_generation_validated": "true",
            "local_primitive_resource_memory_budget_bytes": "1073741824",
            "native_vortex_array_sink_peak_reserved_bytes": "279803",
        }
        require_native_resource_admission("test", SimpleNamespace(field=fields.get))
        changes = [
            {"native_vortex_array_sink_peak_reserved_bytes": value}
            for value in [None, "", "-1", "1.5", "NaN", "١", "1073741825"]
        ] + [
            {"native_vortex_result_export_kind": "primitive_row_stream"},
            {"native_vortex_array_sink_source_generation_validated": "false"},
            {"local_primitive_resource_memory_budget_bytes": "2147483648"},
            {"public_workflow_native_vortex_plan_route_family": "scenario_dispatch"},
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                require_native_resource_admission("test", SimpleNamespace(field=(fields | change).get))

    def test_native_resource_peak_never_accepts_missing_malformed_or_oversized_proof(self):
        base = {"public_workflow_memory_gb": "1",
                "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
                "resident_provider_crate": "vortex",
                "resident_peak_reserved_buffer_bytes": "1024"}
        changes = [
            {"resident_peak_reserved_buffer_bytes": value}
            for value in [None, "", "-1", "1.5", "NaN", "١", "1073741825"]
        ] + [
            {"public_workflow_memory_gb": "2"},
            {"public_workflow_native_vortex_plan_route_family": None},
            {"public_workflow_native_vortex_plan_route_family": "scenario_dispatch"},
            {"resident_provider_crate": None},
            {"resident_provider_crate": "external"},
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                fields = base | change
                require_native_resource_admission("test", SimpleNamespace(field=fields.get))


if __name__ == "__main__":
    unittest.main()
