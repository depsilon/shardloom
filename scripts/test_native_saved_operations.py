# SPDX-License-Identifier: Apache-2.0
"""Regression controls for the saved evidence analysis."""
import lzma
import unittest

from verify_native_saved_operations import EVIDENCE, PACKET, analyze, strict_json
from native_saved_components import DECISION, decisions, metric, project
from native_saved_operations import cli_fields


class SavedOperationTests(unittest.TestCase):
    def test_complete_offline_analysis(self):
        report, table = analyze(PACKET, EVIDENCE)
        self.assertEqual(report, strict_json((PACKET / "report.json").read_bytes()))
        self.assertEqual(table, lzma.decompress((PACKET / "operations.jsonl.xz").read_bytes()))
        self.assertEqual(report["operation_count"], 406)
        self.assertEqual(report["cohorts"]["fresh347"]["coverage"]["resource_grant"], {"full": 347, "compact": 0})
        self.assertIsNone(report["cohorts"]["probe-vortex"]["intake_copied"]["sum"])

    def test_two_cli_field_views_must_agree(self):
        flat = [{"key": name, "value": "false"} for name in
                ("public_workflow_fallback_attempted", "public_workflow_external_engine_invoked")]
        envelope = {"status": "success", "fields": flat, "result": {"fields": flat[:1]}}
        self.assertEqual(cli_fields(envelope), {r["key"]: r["value"] for r in flat})
        envelope["result"]["fields"] = [{"key": flat[0]["key"], "value": "true"}]
        with self.assertRaisesRegex(ValueError, "ambiguous CLI"):
            cli_fields(envelope)

    def test_observed_zero_omission_and_unavailable_are_distinct(self):
        self.assertEqual(metric({"bytes": "0"}, "bytes", True), 0)
        self.assertEqual(metric({}, "bytes", True), "omitted")
        self.assertEqual(metric({}, "bytes", False), "inapplicable")
        self.assertEqual(metric({"bytes": "unavailable"}, "bytes", True), "unavailable")
        with self.assertRaises(ValueError):
            metric({"bytes": "-1"}, "bytes", True)

    def test_reason_set_does_not_allocate_counts_to_each_reason(self):
        prefix = DECISION + "region_exclusion_"
        fields = {prefix + k: v for k, v in {"considered": "3", "admitted": "1", "executed": "0",
            "declined": "2", "admission_reasons": "validated_row_selection",
            "decline_reasons": "exclusion_not_proved,required_value_evaluation"}.items()}
        decision = decisions(fields)["region_exclusion"]
        self.assertEqual(decision["declined"], 2)
        self.assertEqual(decision["decline_reasons"], ["exclusion_not_proved", "required_value_evaluation"])
        self.assertEqual(decision["executed"], 0)
        fields[prefix + "executed"] = "2"
        with self.assertRaises(ValueError):
            decisions(fields)

    def test_shared_lifetime_snapshot_has_no_per_query_delta(self):
        row = project({"cohort": "fresh347", "operation": "control", "grant": {"memory_bytes": 1 << 30, "parallelism": 1}, "full_fields": {
            "resident_source_opens": "59", "resident_completed_executions": "73",
            "resident_counter_scope": "shared_session_lifetime"}, "compact_fields": {}})
        self.assertEqual(row["lifetime_snapshot"]["resident_source_opens"], "59")
        self.assertIsNone(row["lifetime_delta"])
