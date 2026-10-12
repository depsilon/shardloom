# SPDX-License-Identifier: Apache-2.0
"""Regression checks for ambiguous retained native report fields."""
import unittest
from pathlib import Path
import tempfile
from types import SimpleNamespace

from run_native_unary_uat import require_unique_report_fields
from native_report_evidence import (
    has_diagnostic_detail, require_native_pivot_spill, require_native_resource_admission,
    require_native_allocation_peak, require_relational_source_evidence,
)


class NativeReportEvidenceTests(unittest.TestCase):
    @staticmethod
    def allocation_fields(budget=1 << 30, peak=1024):
        return {
            "execution_resource_admission_status": "admitted",
            "execution_resource_declared_memory_bytes": str(budget),
            "execution_resource_admitted_memory_bytes": str(budget),
            "execution_resource_observed_native_reserved_bytes": "512",
            "execution_resource_observed_native_allocation_peak_reserved_bytes": str(peak),
            "execution_resource_observed_native_peak_reserved_bytes": str(peak),
            "execution_resource_memory_observation_scope":
                "session_pool_lifetime_including_preparation_and_retained_owners;"
                "excludes_cli_envelope_formatting_provider_bypass_allocations_and_process_rss",
            "execution_resource_allocation_memory_observation_scope":
                "current_allocation_including_preparation_and_retained_owners;"
                "excludes_cli_envelope_formatting_provider_bypass_allocations_and_process_rss",
        }

    def test_current_allocation_is_checked_without_bounding_a_larger_lifetime_peak(self):
        fields = self.allocation_fields(1 << 30, 4096) | {
            "execution_resource_observed_native_peak_reserved_bytes": "1076174900"}
        self.assertEqual(require_native_allocation_peak(
            "smaller grant", SimpleNamespace(field=fields.get), 1 << 30), 4096)
        changes = [
            {key: value}
            for key in ("execution_resource_observed_native_reserved_bytes",
                        "execution_resource_observed_native_allocation_peak_reserved_bytes",
                        "execution_resource_observed_native_peak_reserved_bytes")
            for value in (None, "", "-1", "1.5", "NaN", "١", str(1 << 64))
        ] + [
            {"execution_resource_observed_native_reserved_bytes": "4097"},
            {"execution_resource_observed_native_allocation_peak_reserved_bytes": "1073741825"},
            {"execution_resource_observed_native_peak_reserved_bytes": "4095"},
            {"execution_resource_declared_memory_bytes": "4294967296"},
            {"execution_resource_admitted_memory_bytes": "4294967296"},
            {"execution_resource_admission_status": "not_observed"},
            {"execution_resource_memory_observation_scope": "current_allocation"},
            {"execution_resource_allocation_memory_observation_scope": "session_pool_lifetime"},
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                require_native_allocation_peak("test", SimpleNamespace(field=(fields | change).get), 1 << 30)

    def test_plan_sources_and_repeated_activity_are_separate_from_session_totals(self):
        fields = {
            "relational_prepared_file_source_count": "2",
            "resident_counter_scope": "shared_session_lifetime",
            "source_io_performed": "true",
            "resident_source_opens": "30",
            "resident_completed_executions": "23",
        }
        def verify(values, files=2, previous=None):
            return require_relational_source_evidence(
                "query", SimpleNamespace(field=values.get), files, previous=previous)

        self.assertEqual(verify(fields), (30, 23))
        self.assertEqual(verify(fields, previous=(30, 22)), (30, 23))
        memory = fields | {"relational_prepared_file_source_count": "0", "source_io_performed": "false"}
        self.assertEqual(verify(memory, files=0), (30, 23))
        for previous in ((29, 22), (31, 22), (30, 21), (30, 23), (30, 24)):
            with self.subTest(previous=previous), self.assertRaises(ValueError):
                verify(fields, previous=previous)
        changes = [
            {"relational_prepared_file_source_count": "1"},
            {"resident_counter_scope": "per_plan"},
            {"source_io_performed": "false"},
            {"resident_source_opens": "1"},
            {"resident_completed_executions": "0"},
        ] + [
            {key: value}
            for key in ("resident_source_opens", "resident_completed_executions")
            for value in (None, "", "-1", "1.5", "NaN", "١")
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                verify(fields | change)
        with self.assertRaises(ValueError):
            verify(memory | {"source_io_performed": "true"}, files=0)

    def test_pivot_spill_requires_exact_strategy_counters_quota_and_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            spill = {"workspace": directory, "quota_bytes": 256 << 20, "buffer_bytes": 1 << 20}
            fields = {
                "relational_dynamic_schema_stages": "2", "relational_spill_requested": "true",
                "relational_spill_strategy": "native_latest_pivot_state_and_stable_full_row_runs",
                "relational_spill_workspace": directory,
                "relational_spill_quota_bytes": str(spill["quota_bytes"]),
                "relational_spill_buffer_bytes": str(spill["buffer_bytes"]),
                "relational_spill_owned_cleanup_completed": "true",
                "relational_spill_peak_disk_bytes": "8192",
                **{f"relational_spilled_pivot_{key}": value for key, value in {
                    "stages": "2", "input_rows": "9", "index_rows": "4", "domains": "2",
                    "cells": "6", "lookup_blocks": "10", "reader_opens": "3",
                }.items()},
            }
            def verify(values, **options):
                require_native_pivot_spill("pivot", SimpleNamespace(field=values.get),
                                          spill, workspace, **options)
            verify(fields)
            verify(fields, stages=2)
            with self.assertRaises(ValueError):
                verify(fields, stages=1)
            changes = [
                {"relational_spill_requested": "false"},
                {"relational_spill_strategy": "stable_native_full_row_two_run_merge"},
                {"relational_spill_workspace": directory + "-other"},
                {"relational_spill_quota_bytes": "1"},
                {"relational_spill_buffer_bytes": "1"},
                {"relational_spill_owned_cleanup_completed": "false"},
                {"relational_spill_peak_disk_bytes": str(spill["quota_bytes"] + 1)},
                {"relational_spilled_pivot_stages": "0"},
                {"relational_spilled_pivot_stages": "1"},
                {"relational_spilled_pivot_reader_opens": "0"},
            ] + [
                {key: value} for key in ("relational_spilled_pivot_cells",
                                         "relational_spill_peak_disk_bytes")
                for value in (None, "", "-1", "NaN", "1.5", "١")
            ]
            for change in changes:
                with self.subTest(change=change), self.assertRaises(ValueError):
                    verify(fields | change)
            # Empty discovery still executes its stage but creates no payload.
            empty = fields | {f"relational_spilled_pivot_{key}": "0" for key in
                              ("input_rows", "index_rows", "domains", "cells", "lookup_blocks", "reader_opens")}
            verify(empty | {"relational_spill_peak_disk_bytes": "0"})
            (workspace / "unreleased.vortex").write_bytes(b"owned state")
            with self.assertRaisesRegex(ValueError, "retained"):
                verify(fields)

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
        common = self.allocation_fields() | {"public_workflow_memory_gb": "1",
                  "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
                  "resident_provider_crate": "vortex"}
        for fields in [
            dict(common, public_workflow_route_id="native_vortex_relational_collect",
                 resident_peak_reserved_buffer_bytes="1024"),
            dict(common | self.allocation_fields(peak=1 << 30),
                 public_workflow_route_id="native_vortex_relational_write",
                 resident_peak_reserved_buffer_bytes="1073741824"),
        ]:
            require_native_resource_admission("test", SimpleNamespace(field=fields.get))

    def test_native_array_writer_uses_its_session_allocator_evidence(self):
        fields = self.allocation_fields(peak=279803) | {
            "public_workflow_memory_gb": "1",
            "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
            "public_workflow_route_id": "native_vortex_primitive_row_export",
            "native_vortex_result_export_kind": "owned_native_array_stream",
            "native_vortex_array_sink_source_generation_validated": "true",
            "local_primitive_resource_memory_budget_bytes": "1073741824",
            "native_vortex_array_sink_peak_reserved_bytes": "1076735292",
            "execution_resource_observed_native_peak_reserved_bytes": "1076735292",
            "execution_resource_memory_observation_scope":
                "session_pool_lifetime_including_retained_owners;"
                "excludes_provider_bypass_allocations_and_process_rss",
        }
        require_native_resource_admission("test", SimpleNamespace(field=fields.get))
        changes = [
            {key: value}
            for key in ("native_vortex_array_sink_peak_reserved_bytes",
                        "execution_resource_observed_native_allocation_peak_reserved_bytes")
            for value in [None, "", "-1", "1.5", "NaN", "١", str(1 << 64)]
        ] + [
            {"native_vortex_array_sink_peak_reserved_bytes": "1076735293"},
            {"execution_resource_observed_native_allocation_peak_reserved_bytes": "1073741825"},
            {"execution_resource_observed_native_allocation_peak_reserved_bytes": "511"},
            {"execution_resource_observed_native_peak_reserved_bytes": "279802"},
            {"execution_resource_allocation_memory_observation_scope": "session_pool_lifetime"},
            {"execution_resource_memory_observation_scope": "current_allocation"},
            {"native_vortex_result_export_kind": "primitive_row_stream"},
            {"native_vortex_array_sink_source_generation_validated": "false"},
            {"local_primitive_resource_memory_budget_bytes": "2147483648"},
            {"public_workflow_native_vortex_plan_route_family": "scenario_dispatch"},
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                require_native_resource_admission("test", SimpleNamespace(field=(fields | change).get))

    def test_conversion_resource_proof_uses_its_declared_budget_not_the_reported_one(self):
        fields = self.allocation_fields(budget=4 << 30, peak=4 << 30) | {
            "public_workflow_memory_gb": "4",
            "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
            "resident_provider_crate": "vortex",
            "resident_peak_reserved_buffer_bytes": "4294967296",
        }
        require_native_resource_admission("test", SimpleNamespace(field=fields.get), memory_gb=4)
        with self.assertRaises(ValueError):
            require_native_resource_admission("test", SimpleNamespace(field=fields.get))
        for change in ({"public_workflow_memory_gb": "8"},
                       {"execution_resource_observed_native_allocation_peak_reserved_bytes": "4294967297"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                require_native_resource_admission("test", SimpleNamespace(field=(fields | change).get), memory_gb=4)

    def test_native_resource_peak_never_accepts_missing_malformed_or_oversized_proof(self):
        base = self.allocation_fields() | {"public_workflow_memory_gb": "1",
                "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
                "resident_provider_crate": "vortex",
                "resident_peak_reserved_buffer_bytes": "1024"}
        changes = [
            {key: value}
            for key in ("resident_peak_reserved_buffer_bytes",
                        "execution_resource_observed_native_allocation_peak_reserved_bytes")
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
